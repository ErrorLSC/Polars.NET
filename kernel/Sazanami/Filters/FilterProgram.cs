using System.Buffers;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Expressions;
using Kaminari.Sazanami.Kernels;
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

    /// <summary>
    /// Whether column <paramref name="column"/> (strings or binaries) may come as a dictionary whose values are in
    /// another layout of the same kind (plain offsets for views): only tests of its own values read it (each tested
    /// once per entry), no expression and no predicate over several columns.
    /// </summary>
    public bool TakesDictionaries(int column)
    {
        if ((uint)column >= (uint)Columns.Count || !IsBytes(ValueType(Columns[column].Type))) return false;
        return Plain(_root);

        bool Plain(Step step) => step switch
        {
            Leaf leaf => leaf.Column != column || leaf.Test is not ExpressionTest,
            Computed computed => !Reads(computed.Predicate),
            All all => all.Operands.All(Plain),
            Any any => any.Operands.All(Plain),
            First first => first.Operands.All(Plain),
            Negate negate => Plain(negate.Operand),
            _ => true,
        };

        bool Reads(ValuePredicate predicate)
        {
            SortedSet<int> read = [];
            predicate.CollectColumns(read);
            return read.Contains(column);
        }
    }

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

    private sealed class Computed(ValuePredicate predicate) : Step
    {
        public ValuePredicate Predicate { get; } = predicate;
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
            FilterNode.EndsWith s => new Leaf(Column(s.Column), EndsWith(columns[s.Column], s.Suffix ?? throw new ArgumentException("A suffix is needed."))),
            FilterNode.Contains c => new Leaf(Column(c.Column), Contains(columns[c.Column], c.Pattern ?? throw new ArgumentException("A pattern is needed."))),
            FilterNode.CompareValues c => Values(ValuePredicate.Compare(c, [.. columns.Select(x => x.Type)])),
            FilterNode.ValueInList v => Values(ValuePredicate.InList(v, [.. columns.Select(x => x.Type)])),
            FilterNode.ValueIsNull n => Values(ValuePredicate.IsNull(n, [.. columns.Select(x => x.Type)])),
            _ => throw new ArgumentException($"Unknown filter node {node}."),
        };

        // A predicate of one column is a leaf of it (a dictionary's entries tested once each); one of several is
        // evaluated on the batch (dictionaries looked up per row). Null-safe ones decide null rows themselves.
        static Step Values(ValuePredicate predicate)
        {
            SortedSet<int> read = [];
            predicate.CollectColumns(read);
            return read.Count == 1 && !predicate.NullsEqual ? new Leaf(read.Min, new ExpressionTest(predicate)) : new Computed(predicate);
        }

        int Column(int index) => (uint)index < (uint)columns.Length
            ? index
            : throw new ArgumentException($"Column {index} is not one of the program's {columns.Length} columns.");

        static FilterNode[] Operands(FilterNode[]? operands) => operands is { Length: > 0 }
            ? operands
            : throw new ArgumentException("AND, OR and COALESCE need operands.");
    }

    private static IArrowType ValueType(IArrowType type) => ValueTypes.ValueType(type);

    private static bool IsBytes(IArrowType type) => ValueTypes.IsBytes(type);

    private static bool IsFloating(IArrowType type) => ValueTypes.IsFloating(type);

    private static LeafTest IsNan(FilterColumn column) => IsFloating(ValueType(column.Type))
        ? Numeric(column, new NaNFactory())
        : throw Unfit(column, "IS NAN");

    private static LeafTest Compare(FilterColumn column, CompareOp op, StoredScalar value)
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

    private static LeafTest InList(FilterColumn column, StoredScalar[] values)
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

    private static BytesTest<BytesTests.Prefix> StartsWith(FilterColumn column, byte[] prefix) => IsBytes(ValueType(column.Type))
        ? new BytesTest<BytesTests.Prefix>(new(prefix))
        : throw Unfit(column, "STARTS WITH");

    private static BytesTest<BytesTests.Suffix> EndsWith(FilterColumn column, byte[] suffix) => IsBytes(ValueType(column.Type))
        ? new BytesTest<BytesTests.Suffix>(new(suffix))
        : throw Unfit(column, "ENDS WITH");

    private static BytesTest<BytesTests.Substring> Contains(FilterColumn column, byte[] pattern) => IsBytes(ValueType(column.Type))
        ? new BytesTest<BytesTests.Substring>(new(pattern))
        : throw Unfit(column, "CONTAINS");

    private static BooleanTest Boolean(FilterColumn column, Func<bool, bool> outcome) => ValueType(column.Type) is BooleanType
        ? new BooleanTest(outcome(false), outcome(true))
        : throw Unfit(column, "a boolean test");

    private static int BooleanValue(FilterColumn column, StoredScalar value) =>
        value.Kind == StoredScalarKind.Integer && (value.IntegerValue == 0 || value.IntegerValue == 1)
            ? (int)value.IntegerValue
            : throw new ArgumentException($"Boolean column '{column}' compares with 0 or 1, not {value}.");

    private static byte[] Bytes(FilterColumn column, StoredScalar value) => value.Kind == StoredScalarKind.Bytes
        ? value.BytesValue!
        : throw new ArgumentException($"Column '{column}' holds bytes; {value} is no bytes value.");

    private static ArgumentException Unfit(FilterColumn column, string what) =>
        new($"Column '{column}' ({column.Type.Name}) does not take {what}.");

    private static LeafTest Numeric<TFactory>(FilterColumn column, TFactory factory) where TFactory : INumericVisitor<LeafTest> =>
        ValueTypes.IsNumeric(column.Type) ? ValueTypes.Visit<TFactory, LeafTest>(column.Type, factory) : throw Unfit(column, "a numeric test");

    /// <summary>A value as the column's element type, exactly (else an error).</summary>
    private static T Scalar<T>(FilterColumn column, StoredScalar value, bool floating) where T : unmanaged, INumber<T> =>
        ValueTypes.Exact<T>(value, floating, $"column '{column}' ({column.Type.Name})");

    private readonly struct NaNFactory : INumericVisitor<LeafTest>
    {
        public LeafTest Visit<T>(bool floating) where T : unmanaged, INumber<T> => new NumericTest<T, IsNaN<T>>(T.Zero);
    }

    private readonly struct CompareFactory(FilterColumn column, CompareOp op, StoredScalar value) : INumericVisitor<LeafTest>
    {
        public LeafTest Visit<T>(bool floating) where T : unmanaged, INumber<T> => NumericTests.Compare(op, Scalar<T>(column, value, floating), floating);
    }

    private readonly struct InListFactory(FilterColumn column, StoredScalar[] values) : INumericVisitor<LeafTest>
    {
        public LeafTest Visit<T>(bool floating) where T : unmanaged, INumber<T>
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
    private static bool Matches(IArrowType declared, IArrowType actual) =>
        (ValueTypes.Same(declared, actual) || actual is DictionaryType { ValueType: var values } && SameBytes(ValueType(declared), values))
        && (actual is not DictionaryType dictionary || dictionary.IndexType is IntegerType);

    /// <summary>Strings (or binaries) in two layouts: a dictionary's values as plain offsets for a column of views.</summary>
    private static bool SameBytes(IArrowType declared, IArrowType values) => (declared, values) switch
    {
        (StringType or StringViewType, StringType or StringViewType) => true,
        (BinaryType or BinaryViewType, BinaryType or BinaryViewType) => true,
        _ => false,
    };

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

            case Computed computed:
            {
                using var scratch = new ValueScratch();
                computed.Predicate.Evaluate(new ValueContext(context.Columns, length, scratch), t, f);
                return;
            }

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
                case Int32Type when !nulls && FewEntries(Values<int>(data, length), dt, df, entries, t, f): break;
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

        // Rows without nulls over entries that are all true or false, a few of them true (or false): the rows of those
        // entries found by comparing indices, the others the other outcome. False when the shape is not that one (or an
        // index is past the entries: the lookup reports it).
        static bool FewEntries(ReadOnlySpan<int> indices, ulong[] dt, ulong[] df, int entries, Span<ulong> t, Span<ulong> f)
        {
            const int Few = 3; // the entries compared (e0, e1, e2 below)
            int words = Masks.Words(entries), trues = 0, falses = 0;
            for (int w = 0; w < words; w++)
            {
                ulong all = Masks.LastWord(w == words - 1 ? entries - (w << 6) : 64);
                if ((dt[w] | df[w]) != all) return false; // a null entry
                trues += BitOperations.PopCount(dt[w]);
                falses += BitOperations.PopCount(df[w]);
            }
            bool onTrue = trues <= falses;
            if (Math.Min(trues, falses) > Few || indices.Length < 64 || VectorKernels.MaxUnsigned(indices) >= (uint)entries) return false;
            // The entries of the rarer outcome (-1: none), in locals: no stack buffer beside the hot loop.
            int e0 = -1, e1 = -1, e2 = -1, count = 0;
            ulong[] side = onTrue ? dt : df;
            for (int e = 0; e < entries; e++)
            {
                if ((side[e >> 6] >> (e & 63) & 1) == 0) continue;
                if (count == 0) e0 = e;
                else if (count == 1) e1 = e;
                else e2 = e;
                count++;
            }
            int length = indices.Length, full = length >> 6;
            for (int w = 0; w < full; w++)
            {
                ReadOnlySpan<int> block = indices.Slice(w << 6, 64);
                ulong hit = count == 0 ? 0 : Masks.Equal(block, e0);
                if (count > 1) hit |= Masks.Equal(block, e1);
                if (count > 2) hit |= Masks.Equal(block, e2);
                t[w] = onTrue ? hit : ~hit;
                f[w] = onTrue ? ~hit : hit;
            }
            for (int i = full << 6; i < length; i++)
            {
                int index = indices[i];
                bool listed = count > 0 && index == e0 || count > 1 && index == e1 || count > 2 && index == e2;
                ulong bit = 1UL << (i & 63);
                if (listed == onTrue) { t[i >> 6] |= bit; f[i >> 6] &= ~bit; }
                else { t[i >> 6] &= ~bit; f[i >> 6] |= bit; }
            }
            if ((length & 63) != 0)
            {
                ulong last = Masks.LastWord(length);
                t[full] &= last;
                f[full] &= last;
            }
            return true;
        }

        // On entry: f holds the valid rows. On exit: t and f the outcomes. Each entry's outcome as a byte (1 true,
        // 2 false, 0 null; 3 for an index past the entries), looked up per row 64 rows at a time, then made bits by
        // vector compares.
        static void Lookup<TIndex>(ReadOnlySpan<TIndex> indices, ulong[] dt, ulong[] df, int entries, bool nullIsTrue, Span<ulong> t, Span<ulong> f)
            where TIndex : unmanaged, IBinaryInteger<TIndex>
        {
            using var scratch = new NativeArray<byte>(entries + 64); // the entries' outcomes, then a block's rows
            Span<byte> outcomes = scratch.AsSpan(0, entries + 64);
            for (int e = 0; e < entries; e++)
                outcomes[e] = (byte)((dt[e >> 6] >> (e & 63) & 1) | (df[e >> 6] >> (e & 63) & 1) << 1);
            Span<byte> rows = outcomes.Slice(entries, 64);
            int length = indices.Length, words = Masks.Words(length);
            for (int w = 0; w < words; w++)
            {
                int first = w << 6, n = Math.Min(64, length - first);
                ReadOnlySpan<TIndex> block = indices.Slice(first, n);
                for (int k = 0; k < n; k++)
                {
                    ulong entry = ulong.CreateTruncating(block[k]);
                    rows[k] = entry < (ulong)entries ? outcomes[(int)entry] : (byte)3;
                }
                rows[n..].Clear();
                ulong valid = f[w], isTrue = Masks.Equal(rows, 1) & valid, isFalse = Masks.Equal(rows, 2) & valid;
                if ((Masks.Equal(rows, 3) & valid) != 0) throw PastEntries(block, valid, entries);
                if (nullIsTrue) isTrue |= ~valid & Masks.LastWord(n);
                t[w] = isTrue;
                f[w] = isFalse;
            }
        }

        static InvalidDataException PastEntries<TIndex>(ReadOnlySpan<TIndex> block, ulong valid, int entries) where TIndex : unmanaged, IBinaryInteger<TIndex>
        {
            for (int k = 0; k < block.Length; k++)
            {
                if ((valid >> k & 1) != 0 && ulong.CreateTruncating(block[k]) >= (ulong)entries)
                    return new InvalidDataException($"Dictionary index {block[k]} is past its {entries} entries.");
            }
            return new InvalidDataException($"A dictionary index is past its {entries} entries.");
        }
    }

    /// <summary>A leaf on plain values: the test where valid, the null outcome elsewhere.</summary>
    private static void EvalValues(LeafTest test, ArrayData data, int length, Span<ulong> t, Span<ulong> f)
    {
        if (test is ExpressionTest expression)
        {
            expression.Outcomes(data, length, t, f);
            return;
        }
        if (data.DataType is NullType)
        {
            // Null on every row (an Arrow null array has no buffers at all).
            Masks.Fill(t, length, test.NullIsTrue);
            Masks.Fill(f, length, false);
            return;
        }
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

    private static bool HasNulls(ArrayData data) => ValueTypes.HasNulls(data);

    /// <summary>The <paramref name="length"/> values of a fixed-width array from its offset.</summary>
    internal static ReadOnlySpan<T> Values<T>(ArrayData data, int length) where T : unmanaged => ValueTypes.Values<T>(data, length);
}
