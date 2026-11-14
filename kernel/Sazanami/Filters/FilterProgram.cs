using System.Buffers;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using static Kaminari.Sazanami.Filters.ValueTests;

namespace Kaminari.Sazanami.Filters;

/// <summary>
/// A physical row predicate over Arrow columns (<see cref="FilterNode"/>s over <see cref="FilterColumn"/>s), checked
/// and compiled once by <see cref="Create"/>, then evaluated batch by batch into a row mask of the rows where it is
/// true. Immutable and safe to evaluate from several threads at once.
/// </summary>
/// <remarks>
/// Every node yields two masks, the rows where it is true and the rows where it is false (a row in neither is null),
/// so <c>NOT</c> and <c>COALESCE</c> keep three-valued logic exact. Columns may come as dictionaries of the declared
/// type: a leaf is then tested once per dictionary entry and looked up per row.
/// </remarks>
internal sealed class FilterProgram
{
    private readonly Step _root;

    private FilterProgram(FilterColumn[] columns, Step root)
    {
        Columns = columns;
        _root = root;
    }

    /// <summary>The columns the program reads, in the order <see cref="Evaluate"/> takes them.</summary>
    public IReadOnlyList<FilterColumn> Columns { get; }

    /// <summary>Checks a predicate against its columns and compiles it.</summary>
    /// <exception cref="ArgumentException">
    /// A leaf names no column, tests a column of a type it does not apply to, or compares with a value of another kind or one the
    /// column cannot hold exactly.
    /// </exception>
    public static FilterProgram Create(IReadOnlyList<FilterColumn> columns, FilterNode root)
    {
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(root);
        FilterColumn[] all = [.. columns];
        return new(all, Compile(root, all));
    }

    /// <summary>
    /// Evaluates the program on a batch of <paramref name="length"/> rows (one array per column, each of that
    /// length) and sets bit <c>i</c> of <paramref name="selected"/> where the predicate is true on row <c>i</c>; bits
    /// past the last row are cleared. Returns the number of rows selected.
    /// </summary>
    /// <exception cref="ArgumentException">The arrays do not match the columns.</exception>
    public int Evaluate(ReadOnlySpan<IArrowArray> columns, int length, Span<ulong> selected)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (columns.Length != Columns.Count) throw new ArgumentException($"The program reads {Columns.Count} columns; {columns.Length} were given.", nameof(columns));
        for (int c = 0; c < columns.Length; c++)
        {
            if (columns[c].Length != length) throw new ArgumentException($"Column '{Columns[c]}' holds {columns[c].Length} rows, not {length}.", nameof(columns));
            if (!Matches(Columns[c].Type, columns[c].Data.DataType))
                throw new ArgumentException($"Column '{Columns[c]}' is {Columns[c].Type.Name}; a {columns[c].Data.DataType.Name} array was given.", nameof(columns));
        }
        int words = Masks.Words(length);
        if (selected.Length < words) throw new ArgumentException($"The mask holds fewer than {length} bits.", nameof(selected));
        var context = new Context(columns, length);
        ulong[] f = context.Rent();
        try
        {
            Eval(_root, ref context, selected[..words], f.AsSpan(0, words));
        }
        finally
        {
            context.Return(f);
        }
        return Masks.Count(selected[..words]);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Compilation

    private abstract class Step;

    private sealed class Leaf(int column, LeafTest test) : Step
    {
        public int Column { get; } = column;
        public LeafTest Test { get; } = test;
    }

    private sealed class Const(bool? value) : Step
    {
        public bool? Value { get; } = value;
    }

    private sealed class All(Step[] operands) : Step
    {
        public Step[] Operands { get; } = operands;
    }

    private sealed class Any(Step[] operands) : Step
    {
        public Step[] Operands { get; } = operands;
    }

    private sealed class Negate(Step operand) : Step
    {
        public Step Operand { get; } = operand;
    }

    private sealed class First(Step[] operands) : Step
    {
        public Step[] Operands { get; } = operands;
    }

    private static Step Compile(FilterNode node, FilterColumn[] columns)
    {
        return node switch
        {
            FilterNode.Constant c => new Const(c.Value),
            FilterNode.And a => new All([.. Operands(a.Operands).Select(o => Compile(o, columns))]),
            FilterNode.Or o => new Any([.. Operands(o.Operands).Select(x => Compile(x, columns))]),
            FilterNode.Not n => new Negate(Compile(n.Operand ?? throw new ArgumentException("NOT needs an operand."), columns)),
            FilterNode.Coalesce c => new First([.. Operands(c.Operands).Select(o => Compile(o, columns))]),
            FilterNode.IsNull n => new Leaf(Column(n.Column), NullTest.Instance),
            FilterNode.IsNan n => new Leaf(Column(n.Column), IsNan(columns[n.Column])),
            FilterNode.BooleanColumn b => new Leaf(Column(b.Column), Boolean(columns[b.Column], v => v)),
            FilterNode.Compare c => new Leaf(Column(c.Column), Compare(columns[c.Column], c.Op, c.Value)),
            FilterNode.InList i => new Leaf(Column(i.Column), InList(columns[i.Column], i.Values ?? throw new ArgumentException("IN needs a list."))),
            FilterNode.StartsWith s => new Leaf(Column(s.Column), StartsWith(columns[s.Column], s.Prefix ?? throw new ArgumentException("A prefix is needed."))),
            _ => throw new ArgumentException($"Unknown filter node {node}."),
        };

        int Column(int index) => (uint)index < (uint)columns.Length
            ? index
            : throw new ArgumentException($"Column {index} is not one of the program's {columns.Length} columns.");

        static FilterNode[] Operands(FilterNode[]? operands) => operands is { Length: > 0 }
            ? operands
            : throw new ArgumentException("AND, OR and COALESCE need operands.");
    }

    /// <summary>The value type of a column (a dictionary's values).</summary>
    private static IArrowType ValueType(IArrowType type) => type is DictionaryType d ? d.ValueType : type;

    private static bool IsBytes(IArrowType type) => type is StringType or BinaryType or StringViewType or BinaryViewType or FixedSizeBinaryType and not (Decimal128Type or Decimal256Type);

    private static bool IsFloating(IArrowType type) => type is HalfFloatType or FloatType or DoubleType;

    private static LeafTest IsNan(FilterColumn column) => IsFloating(ValueType(column.Type))
        ? Numeric(column, new NaNFactory())
        : throw Unfit(column, "IS NAN");

    private static LeafTest Compare(FilterColumn column, CompareOp op, FilterScalar value)
    {
        IArrowType type = ValueType(column.Type);
        if (type is BooleanType)
        {
            int b = BooleanValue(column, value);
            return Boolean(column, v => BytesTests.Holds(op, (v ? 1 : 0).CompareTo(b)));
        }
        if (IsBytes(type))
        {
            byte[] bytes = Bytes(column, value);
            return op switch
            {
                CompareOp.Eq => new BytesTest<BytesTests.Equal>(new(bytes)),
                _ => new BytesTest<BytesTests.Order>(new(op, bytes)),
            };
        }
        return Numeric(column, new CompareFactory(column, op, value));
    }

    private static LeafTest InList(FilterColumn column, FilterScalar[] values)
    {
        IArrowType type = ValueType(column.Type);
        if (type is BooleanType)
        {
            int[] bools = [.. values.Select(v => BooleanValue(column, v))];
            return Boolean(column, v => bools.Contains(v ? 1 : 0));
        }
        if (IsBytes(type))
        {
            byte[][] sorted = [.. values.Select(v => Bytes(column, v)).Distinct(ByteArrays.Instance)];
            System.Array.Sort(sorted, static (a, b) => a.AsSpan().SequenceCompareTo(b));
            return new BytesTest<BytesTests.Member>(new(sorted));
        }
        return Numeric(column, new InListFactory(column, values));
    }

    private static LeafTest StartsWith(FilterColumn column, byte[] prefix) => IsBytes(ValueType(column.Type))
        ? new BytesTest<BytesTests.Prefix>(new(prefix))
        : throw Unfit(column, "STARTS WITH");

    private static LeafTest Boolean(FilterColumn column, Func<bool, bool> outcome) => ValueType(column.Type) is BooleanType
        ? new BooleanTest(outcome(false), outcome(true))
        : throw Unfit(column, "a boolean test");

    private static int BooleanValue(FilterColumn column, FilterScalar value) =>
        value.Kind == FilterScalarKind.Integer && (value.IntegerValue == 0 || value.IntegerValue == 1)
            ? (int)value.IntegerValue
            : throw new ArgumentException($"Boolean column '{column}' compares with 0 or 1, not {value}.");

    private static byte[] Bytes(FilterColumn column, FilterScalar value) => value.Kind == FilterScalarKind.Bytes
        ? value.BytesValue!
        : throw new ArgumentException($"Column '{column}' holds bytes; {value} is no bytes value.");

    private static ArgumentException Unfit(FilterColumn column, string what) =>
        new($"Column '{column}' ({column.Type.Name}) does not take {what}.");

    /// <summary>Builds the test of a fixed-width column for its element type.</summary>
    private interface INumericFactory
    {
        LeafTest Make<T>(bool floating) where T : unmanaged, INumber<T>;
    }

    private static LeafTest Numeric<TFactory>(FilterColumn column, TFactory factory) where TFactory : INumericFactory => ValueType(column.Type) switch
    {
        Int8Type => factory.Make<sbyte>(false),
        UInt8Type => factory.Make<byte>(false),
        Int16Type => factory.Make<short>(false),
        UInt16Type => factory.Make<ushort>(false),
        Int32Type or Date32Type or Time32Type => factory.Make<int>(false),
        UInt32Type => factory.Make<uint>(false),
        Int64Type or Date64Type or Time64Type or TimestampType or DurationType => factory.Make<long>(false),
        UInt64Type => factory.Make<ulong>(false),
        Decimal128Type => factory.Make<Int128>(false),
        HalfFloatType => factory.Make<Half>(true),
        FloatType => factory.Make<float>(true),
        DoubleType => factory.Make<double>(true),
        _ => throw Unfit(column, "a numeric test"),
    };

    /// <summary>A value as the column's element type, exactly (else an error).</summary>
    private static T Scalar<T>(FilterColumn column, FilterScalar value, bool floating) where T : unmanaged, INumber<T>
    {
        if (floating)
        {
            if (value.Kind != FilterScalarKind.Float) throw new ArgumentException($"Floating column '{column}' compares with a floating value, not {value}.");
            T v = T.CreateSaturating(value.FloatValue);
            if (double.IsNaN(value.FloatValue) || double.CreateSaturating(v) == value.FloatValue) return v;
            throw new ArgumentException($"{value} is not exactly a value of column '{column}' ({column.Type.Name}).");
        }
        if (value.Kind != FilterScalarKind.Integer) throw new ArgumentException($"Column '{column}' compares with an integer value, not {value}.");
        try
        {
            return T.CreateChecked(value.IntegerValue);
        }
        catch (OverflowException)
        {
            throw new ArgumentException($"{value} is outside the values of column '{column}' ({column.Type.Name}).");
        }
    }

    private readonly struct NaNFactory : INumericFactory
    {
        public LeafTest Make<T>(bool floating) where T : unmanaged, INumber<T> => new NumericTest<T, IsNaN<T>>(T.Zero);
    }

    private readonly struct CompareFactory(FilterColumn column, CompareOp op, FilterScalar value) : INumericFactory
    {
        public LeafTest Make<T>(bool floating) where T : unmanaged, INumber<T>
        {
            T s = Scalar<T>(column, value, floating);
            if (floating && T.IsNaN(s))
            {
                // NaN is the greatest value and equals itself.
                return op switch
                {
                    CompareOp.Eq or CompareOp.GtEq => new NumericTest<T, IsNaN<T>>(s),
                    CompareOp.NotEq or CompareOp.Lt => new NumericTest<T, NotNaN<T>>(s),
                    CompareOp.LtEq => new ConstantTest(true),
                    _ => new ConstantTest(false),
                };
            }
            return op switch
            {
                CompareOp.Eq => new NumericTest<T, Eq<T>>(s),
                CompareOp.NotEq => new NumericTest<T, NotEq<T>>(s),
                CompareOp.Lt => new NumericTest<T, Lt<T>>(s),
                CompareOp.LtEq => new NumericTest<T, LtEq<T>>(s),
                CompareOp.Gt => floating ? new NumericTest<T, GtOrNaN<T>>(s) : new NumericTest<T, Gt<T>>(s),
                _ => floating ? new NumericTest<T, GtEqOrNaN<T>>(s) : new NumericTest<T, GtEq<T>>(s),
            };
        }
    }

    private readonly struct InListFactory(FilterColumn column, FilterScalar[] values) : INumericFactory
    {
        public LeafTest Make<T>(bool floating) where T : unmanaged, INumber<T>
        {
            T[] typed = new T[values.Length];
            for (int i = 0; i < typed.Length; i++) typed[i] = Scalar<T>(column, values[i], floating);
            return new NumericInListTest<T>(typed);
        }
    }

    private sealed class ByteArrays : IEqualityComparer<byte[]>
    {
        public static ByteArrays Instance { get; } = new();
        public bool Equals(byte[]? x, byte[]? y) => x.AsSpan().SequenceEqual(y);

        public int GetHashCode(byte[] obj)
        {
            var hash = new HashCode();
            hash.AddBytes(obj);
            return hash.ToHashCode();
        }
    }

    /// <summary>Whether an array of <paramref name="actual"/> type serves a column declared <paramref name="declared"/> (dictionaries by their values).</summary>
    private static bool Matches(IArrowType declared, IArrowType actual) => (ValueType(declared), ValueType(actual)) switch
    {
        (TimestampType d, TimestampType a) => d.Unit == a.Unit && d.Timezone == a.Timezone,
        (Time32Type d, Time32Type a) => d.Unit == a.Unit,
        (Time64Type d, Time64Type a) => d.Unit == a.Unit,
        (DurationType d, DurationType a) => d.Unit == a.Unit,
        (Decimal128Type d, Decimal128Type a) => d.Scale == a.Scale,
        (FixedSizeBinaryType d, FixedSizeBinaryType a) => d.ByteWidth == a.ByteWidth,
        var (d, a) => d.TypeId == a.TypeId,
    } && (actual is not DictionaryType dictionary || dictionary.IndexType is IntegerType);

    // ---------------------------------------------------------------------------------------------------------------
    // Evaluation

    private readonly ref struct Context(ReadOnlySpan<IArrowArray> columns, int length)
    {
        public readonly ReadOnlySpan<IArrowArray> Columns = columns;
        public readonly int Length = length;
        public int Words => Masks.Words(Length);

        public ulong[] Rent() => ArrayPool<ulong>.Shared.Rent(Math.Max(1, Words));

        public void Return(ulong[] array) => ArrayPool<ulong>.Shared.Return(array);
    }

    /// <summary>Writes the rows where <paramref name="step"/> is true into <paramref name="t"/>, those where it is false into <paramref name="f"/>.</summary>
    private static void Eval(Step step, ref Context context, Span<ulong> t, Span<ulong> f)
    {
        int length = context.Length;
        switch (step)
        {
            case Const c:
                Masks.Fill(t, length, c.Value == true);
                Masks.Fill(f, length, c.Value == false);
                return;

            case Leaf leaf:
                EvalLeaf(leaf.Test, context.Columns[leaf.Column].Data, length, t, f);
                return;

            case Negate n:
                Eval(n.Operand, ref context, f, t);
                return;

            case All a:
            {
                Eval(a.Operands[0], ref context, t, f);
                if (a.Operands.Length == 1) return;
                ulong[] t2 = context.Rent(), f2 = context.Rent();
                try
                {
                    for (int i = 1; i < a.Operands.Length; i++)
                    {
                        if (Masks.IsFull(f, length)) return; // false on every row: the rest cannot change that
                        Eval(a.Operands[i], ref context, t2.AsSpan(0, t.Length), f2.AsSpan(0, f.Length));
                        Masks.And(t, t2);
                        Masks.Or(f, f2);
                    }
                }
                finally
                {
                    context.Return(t2);
                    context.Return(f2);
                }
                return;
            }

            case Any a:
            {
                Eval(a.Operands[0], ref context, t, f);
                if (a.Operands.Length == 1) return;
                ulong[] t2 = context.Rent(), f2 = context.Rent();
                try
                {
                    for (int i = 1; i < a.Operands.Length; i++)
                    {
                        if (Masks.IsFull(t, length)) return; // true on every row
                        Eval(a.Operands[i], ref context, t2.AsSpan(0, t.Length), f2.AsSpan(0, f.Length));
                        Masks.Or(t, t2);
                        Masks.And(f, f2);
                    }
                }
                finally
                {
                    context.Return(t2);
                    context.Return(f2);
                }
                return;
            }

            case First c:
            {
                Eval(c.Operands[0], ref context, t, f);
                if (c.Operands.Length == 1) return;
                ulong[] t2 = context.Rent(), f2 = context.Rent(), open = context.Rent();
                try
                {
                    Span<ulong> unknown = open.AsSpan(0, t.Length);
                    for (int i = 1; i < c.Operands.Length; i++)
                    {
                        // The rows still null: neither true nor false.
                        t.CopyTo(unknown);
                        Masks.Or(unknown, f);
                        Masks.Not(unknown, length);
                        if (Masks.IsZero(unknown)) return;
                        Span<ulong> t2s = t2.AsSpan(0, t.Length), f2s = f2.AsSpan(0, f.Length);
                        Eval(c.Operands[i], ref context, t2s, f2s);
                        Masks.And(t2s, unknown);
                        Masks.And(f2s, unknown);
                        Masks.Or(t, t2s);
                        Masks.Or(f, f2s);
                    }
                }
                finally
                {
                    context.Return(t2);
                    context.Return(f2);
                    context.Return(open);
                }
                return;
            }

            default:
                throw new InvalidOperationException($"Unknown step {step}.");
        }
    }

    /// <summary>A leaf on a column: on its values, or once per dictionary entry and then per row by index.</summary>
    private static void EvalLeaf(LeafTest test, ArrayData data, int length, Span<ulong> t, Span<ulong> f)
    {
        if (data.Dictionary is not { } dictionary)
        {
            EvalValues(test, data, length, t, f);
            return;
        }
        int entries = dictionary.Length, words = Masks.Words(entries);
        ulong[] dt = ArrayPool<ulong>.Shared.Rent(Math.Max(1, words)), df = ArrayPool<ulong>.Shared.Rent(Math.Max(1, words));
        try
        {
            EvalValues(test, dictionary, entries, dt.AsSpan(0, words), df.AsSpan(0, words));
            // Rows: by their entry's outcomes; a null row as the test is on nulls.
            bool nulls = HasNulls(data);
            if (nulls) Masks.Load(data.Buffers[0].Span, data.Offset, length, f);
            else Masks.Fill(f, length, true);
            switch (((DictionaryType)data.DataType).IndexType)
            {
                case Int8Type: Lookup(Values<sbyte>(data, length), dt, df, entries, test.NullIsTrue, t, f); break;
                case UInt8Type: Lookup(Values<byte>(data, length), dt, df, entries, test.NullIsTrue, t, f); break;
                case Int16Type: Lookup(Values<short>(data, length), dt, df, entries, test.NullIsTrue, t, f); break;
                case UInt16Type: Lookup(Values<ushort>(data, length), dt, df, entries, test.NullIsTrue, t, f); break;
                case Int32Type: Lookup(Values<int>(data, length), dt, df, entries, test.NullIsTrue, t, f); break;
                case UInt32Type: Lookup(Values<uint>(data, length), dt, df, entries, test.NullIsTrue, t, f); break;
                case Int64Type: Lookup(Values<long>(data, length), dt, df, entries, test.NullIsTrue, t, f); break;
                case UInt64Type: Lookup(Values<ulong>(data, length), dt, df, entries, test.NullIsTrue, t, f); break;
                default: throw new ArgumentException($"Dictionary indices of type {((DictionaryType)data.DataType).IndexType.Name} are not supported.");
            }
        }
        finally
        {
            ArrayPool<ulong>.Shared.Return(dt);
            ArrayPool<ulong>.Shared.Return(df);
        }

        // On entry: f holds the valid rows. On exit: t and f the outcomes.
        static void Lookup<TIndex>(ReadOnlySpan<TIndex> indices, ulong[] dt, ulong[] df, int entries, bool nullIsTrue, Span<ulong> t, Span<ulong> f)
            where TIndex : unmanaged, IBinaryInteger<TIndex>
        {
            int length = indices.Length, words = Masks.Words(length);
            for (int w = 0; w < words; w++)
            {
                int first = w << 6, n = Math.Min(64, length - first);
                ulong valid = f[w], isTrue = 0, isFalse = 0;
                for (int k = 0; k < n; k++)
                {
                    if ((valid >> k & 1) == 0) continue;
                    ulong entry = ulong.CreateTruncating(indices[first + k]);
                    if (entry >= (ulong)entries) throw new InvalidDataException($"Dictionary index {indices[first + k]} is past its {entries} entries.");
                    isTrue |= (dt[entry >> 6] >> (int)(entry & 63) & 1) << k;
                    isFalse |= (df[entry >> 6] >> (int)(entry & 63) & 1) << k;
                }
                if (nullIsTrue) isTrue |= ~valid & Masks.LastWord(n);
                t[w] = isTrue;
                f[w] = isFalse;
            }
        }
    }

    /// <summary>A leaf on plain values: the test where valid, the null outcome elsewhere.</summary>
    private static void EvalValues(LeafTest test, ArrayData data, int length, Span<ulong> t, Span<ulong> f)
    {
        int words = Masks.Words(length);
        if (!HasNulls(data))
        {
            test.Run(data, length, default, t);
            t.CopyTo(f);
            Masks.Not(f, length);
            return;
        }
        Masks.Load(data.Buffers[0].Span, data.Offset, length, f); // the valid rows, until the end
        test.Run(data, length, f, t);
        ulong last = Masks.LastWord(length);
        for (int w = 0; w < words; w++)
        {
            ulong valid = f[w], holds = t[w], rows = w == words - 1 ? last : ulong.MaxValue;
            t[w] = holds & valid | (test.NullIsTrue ? ~valid & rows : 0);
            f[w] = ~holds & valid;
        }
    }

    private static bool HasNulls(ArrayData data) => data.NullCount != 0 && !data.Buffers[0].IsEmpty;

    /// <summary>The <paramref name="length"/> values of a fixed-width array from its offset.</summary>
    internal static ReadOnlySpan<T> Values<T>(ArrayData data, int length) where T : unmanaged =>
        data.Buffers[1].Span.CastTo<T>().Slice(data.Offset, length);
}
