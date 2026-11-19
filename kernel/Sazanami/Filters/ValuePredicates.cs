using System.Buffers.Binary;
using System.Numerics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Expressions;
using static Kaminari.Sazanami.Filters.ValueTests;

namespace Kaminari.Sazanami.Filters;

/// <summary>
/// A predicate over values computed from columns (<see cref="FilterNode.CompareValues"/>,
/// <see cref="FilterNode.ValueInList"/>), on the expression kernel (<see cref="ValueStep"/>): its outcomes on the rows
/// of a batch, in three-valued logic.
/// </summary>
internal abstract class ValuePredicate
{
    /// <summary>Whether two nulls are equal and a null differs from every value (an equality only).</summary>
    public abstract bool NullsEqual { get; }

    /// <summary>The outcome on a row whose (one) column is null: true for a null test, else null.</summary>
    public virtual bool NullIsTrue => false;

    public abstract void CollectColumns(ISet<int> columns);

    /// <summary>
    /// Writes the rows where the predicate is true into <paramref name="t"/>, those where it is false into
    /// <paramref name="f"/> (the words of the context's rows; the bits past the last row clear).
    /// </summary>
    public abstract void Evaluate(in ValueContext context, Span<ulong> t, Span<ulong> f);

    /// <summary>Compiles a comparison of two values.</summary>
    /// <exception cref="ArgumentException">The comparison is not well typed.</exception>
    public static ValuePredicate Compare(FilterNode.CompareValues node, IReadOnlyList<IArrowType> columns)
    {
        ArgumentNullException.ThrowIfNull(node.Left);
        ArgumentNullException.ThrowIfNull(node.Right);
        if (!Enum.IsDefined(node.Op)) throw new ArgumentException($"Unknown comparison {node.Op}.");
        if (node.NullsEqual && node.Op != CompareOp.Eq) throw new ArgumentException("Null-safe comparisons are equalities (a null-safe inequality is its NOT).");
        IArrowType? left = ValueCompiler.TypeOf(node.Left, columns), right = ValueCompiler.TypeOf(node.Right, columns);
        if (left is not null && right is not null && !ValueTypes.Same(left, right) && !(ValueTypes.IsBytes(left) && ValueTypes.IsBytes(right)))
            throw new ArgumentException($"A comparison of {left.Name} with {right.Name} values: both sides must be of one type.");
        IArrowType type = left ?? right ?? throw new ArgumentException("A comparison of two scalars is to be decided before (folded).");
        if (type is BooleanType || ValueTypes.IsBytes(type))
        {
            if (node.Left is not ValueNode.Column l || node.Right is not ValueNode.Column r)
                throw new ArgumentException($"{type.Name} values are compared column with column here (a column with a value is a Compare).");
            return type is BooleanType
                ? new BooleansPredicate(l.Index, node.Op, r.Index, node.NullsEqual)
                : new BytesPredicate(l.Index, node.Op, r.Index, node.NullsEqual);
        }
        if (!ValueTypes.IsNumeric(type)) throw new ArgumentException($"{type.Name} values are not compared here.");
        if (node.Right is ValueNode.Scalar s)
            return ValueTypes.Visit<ScalarCompareFactory, ValuePredicate>(type, new(ValueCompiler.Compile(node.Left, columns, type), node.Op, s.Value, node.NullsEqual, type));
        if (node.Left is ValueNode.Scalar first)
            return ValueTypes.Visit<ScalarCompareFactory, ValuePredicate>(type, new(ValueCompiler.Compile(node.Right, columns, type), Flip(node.Op), first.Value, node.NullsEqual, type));
        return ValueTypes.Visit<CompareFactory, ValuePredicate>(type,
            new(ValueCompiler.Compile(node.Left, columns, type), node.Op, ValueCompiler.Compile(node.Right, columns, type), node.NullsEqual));
    }

    /// <summary>Compiles membership of a computed number in a list.</summary>
    /// <exception cref="ArgumentException">The value is not a well-typed number, or a member is not exactly one of its values.</exception>
    public static ValuePredicate InList(FilterNode.ValueInList node, IReadOnlyList<IArrowType> columns)
    {
        ArgumentNullException.ThrowIfNull(node.Value);
        ArgumentNullException.ThrowIfNull(node.Values);
        IArrowType type = ValueCompiler.TypeOf(node.Value, columns) ?? throw new ArgumentException("A scalar in a list is to be decided before (folded).");
        if (!ValueTypes.IsNumeric(type)) throw new ArgumentException($"{type.Name} values are not tested for membership here.");
        return ValueTypes.Visit<InListFactory, ValuePredicate>(type, new(ValueCompiler.Compile(node.Value, columns, type), node.Values, type));
    }

    /// <summary>Compiles the null test of a computed number.</summary>
    /// <exception cref="ArgumentException">The value is not a well-typed number.</exception>
    public static ValuePredicate IsNull(FilterNode.ValueIsNull node, IReadOnlyList<IArrowType> columns)
    {
        ArgumentNullException.ThrowIfNull(node.Value);
        IArrowType type = ValueCompiler.TypeOf(node.Value, columns) ?? throw new ArgumentException("A scalar's null test is to be decided before (folded).");
        if (!ValueTypes.IsNumeric(type)) throw new ArgumentException($"{type.Name} values are not tested for nulls here.");
        return ValueTypes.Visit<IsNullFactory, ValuePredicate>(type, new(ValueCompiler.Compile(node.Value, columns, type)));
    }

    private static CompareOp Flip(CompareOp op) => op switch
    {
        CompareOp.Lt => CompareOp.Gt,
        CompareOp.LtEq => CompareOp.GtEq,
        CompareOp.Gt => CompareOp.Lt,
        CompareOp.GtEq => CompareOp.LtEq,
        _ => op,
    };

    /// <summary>
    /// The outcomes from where the test holds (<paramref name="t"/> on entry; any bit on a null row) and the valid rows
    /// of each side (all of them where <c>all</c> is set): true or false where both are valid; with
    /// <paramref name="nullsEqual"/>, true where both are null and false where one is; null elsewhere.
    /// </summary>
    protected static void Combine(Span<ulong> t, Span<ulong> f, ReadOnlySpan<ulong> left, bool leftAll, ReadOnlySpan<ulong> right, bool rightAll,
        bool nullsEqual, int length)
    {
        int words = Masks.Words(length);
        ulong last = Masks.LastWord(length);
        for (int w = 0; w < words; w++)
        {
            ulong rows = w == words - 1 ? last : ulong.MaxValue;
            ulong l = leftAll ? rows : left[w], r = rightAll ? rows : right[w], holds = t[w], both = l & r;
            if (nullsEqual)
            {
                t[w] = holds & both | ~l & ~r & rows;
                f[w] = ~holds & both | l ^ r;
            }
            else
            {
                t[w] = holds & both;
                f[w] = ~holds & both;
            }
        }
    }

    /// <summary>The index of a dictionary-encoded row.</summary>
    protected static ulong DictionaryIndex(ArrayData data, int row)
    {
        int at = data.Offset + row;
        ReadOnlySpan<byte> indices = data.Buffers[1].Span;
        return ((DictionaryType)data.DataType).IndexType switch
        {
            Int8Type => (ulong)(long)(sbyte)indices[at],
            UInt8Type => indices[at],
            Int16Type => (ulong)(long)BinaryPrimitives.ReadInt16LittleEndian(indices[(at * 2)..]),
            UInt16Type => BinaryPrimitives.ReadUInt16LittleEndian(indices[(at * 2)..]),
            Int32Type => (ulong)(long)BinaryPrimitives.ReadInt32LittleEndian(indices[(at * 4)..]),
            UInt32Type => BinaryPrimitives.ReadUInt32LittleEndian(indices[(at * 4)..]),
            Int64Type => (ulong)BinaryPrimitives.ReadInt64LittleEndian(indices[(at * 8)..]),
            UInt64Type => BinaryPrimitives.ReadUInt64LittleEndian(indices[(at * 8)..]),
            var other => throw new ArgumentException($"Dictionary indices of type {other.Name} are not supported."),
        };
    }

    /// <summary>
    /// The valid rows of a column (a dictionary's: valid indices to valid entries) into <paramref name="valid"/>, or
    /// true when every row is valid.
    /// </summary>
    protected static bool Validity(ArrayData data, int length, Span<ulong> valid)
    {
        if (data.DataType is NullType)
        {
            Masks.Fill(valid, length, false);
            return false;
        }
        bool all = !ValueTypes.HasNulls(data);
        if (data.Dictionary is not { } dictionary)
        {
            if (!all) Masks.Load(data.Buffers[0].Span, data.Offset, length, valid);
            return all;
        }
        if (all) Masks.Fill(valid, length, true);
        else Masks.Load(data.Buffers[0].Span, data.Offset, length, valid);
        bool entryNulls = ValueTypes.HasNulls(dictionary);
        ReadOnlySpan<byte> entryValid = entryNulls ? dictionary.Buffers[0].Span : default;
        for (int row = 0; row < length; row++)
        {
            if ((valid[row >> 6] >> (row & 63) & 1) == 0) continue;
            ulong entry = DictionaryIndex(data, row);
            if (entry >= (ulong)dictionary.Length) throw new InvalidDataException($"Dictionary index {entry} is past its {dictionary.Length} entries.");
            if (entryNulls && !Kernels.Bits.Get(entryValid, dictionary.Offset + (long)entry)) valid[row >> 6] &= ~(1UL << (row & 63));
        }
        return Masks.IsFull(valid, length);
    }

    private readonly struct ScalarCompareFactory(ValueStep value, CompareOp op, StoredScalar scalar, bool nullsEqual, IArrowType type) : INumericVisitor<ValuePredicate>
    {
        public ValuePredicate Visit<T>(bool floating) where T : unmanaged, INumber<T> => new NumberTestPredicate<T>((ValueStep<T>)value,
            NumericTests.Compare(op, ValueTypes.Exact<T>(scalar, floating, $"a {type.Name} value"), floating), nullsEqual);
    }

    private readonly struct IsNullFactory(ValueStep value) : INumericVisitor<ValuePredicate>
    {
        public ValuePredicate Visit<T>(bool floating) where T : unmanaged, INumber<T> => new NullTestPredicate<T>((ValueStep<T>)value);
    }

    private readonly struct InListFactory(ValueStep value, StoredScalar[] values, IArrowType type) : INumericVisitor<ValuePredicate>
    {
        public ValuePredicate Visit<T>(bool floating) where T : unmanaged, INumber<T>
        {
            T[] typed = new T[values.Length];
            for (int i = 0; i < typed.Length; i++) typed[i] = ValueTypes.Exact<T>(values[i], floating, $"a {type.Name} value");
            return new NumberTestPredicate<T>((ValueStep<T>)value, new NumericInListTest<T>(typed), false);
        }
    }

    private readonly struct CompareFactory(ValueStep left, CompareOp op, ValueStep right, bool nullsEqual) : INumericVisitor<ValuePredicate>
    {
        public ValuePredicate Visit<T>(bool floating) where T : unmanaged, INumber<T>
        {
            var (l, r) = ((ValueStep<T>)left, (ValueStep<T>)right);
            return (op, floating) switch
            {
                (CompareOp.Eq, false) => new NumbersPredicate<T, Eq<T>>(l, r, nullsEqual),
                (CompareOp.NotEq, false) => new NumbersPredicate<T, NotEq<T>>(l, r, nullsEqual),
                (CompareOp.Lt, false) => new NumbersPredicate<T, Lt<T>>(l, r, nullsEqual),
                (CompareOp.LtEq, false) => new NumbersPredicate<T, LtEq<T>>(l, r, nullsEqual),
                (CompareOp.Gt, false) => new NumbersPredicate<T, Gt<T>>(l, r, nullsEqual),
                (CompareOp.GtEq, false) => new NumbersPredicate<T, GtEq<T>>(l, r, nullsEqual),
                (CompareOp.Eq, true) => new NumbersPredicate<T, TotalEq<T>>(l, r, nullsEqual),
                (CompareOp.NotEq, true) => new NumbersPredicate<T, TotalNotEq<T>>(l, r, nullsEqual),
                (CompareOp.Lt, true) => new NumbersPredicate<T, TotalLt<T>>(l, r, nullsEqual),
                (CompareOp.LtEq, true) => new NumbersPredicate<T, TotalLtEq<T>>(l, r, nullsEqual),
                (CompareOp.Gt, true) => new NumbersPredicate<T, TotalGt<T>>(l, r, nullsEqual),
                _ => new NumbersPredicate<T, TotalGtEq<T>>(l, r, nullsEqual),
            };
        }
    }
}

/// <summary>A test of a computed number: a comparison with a scalar, or membership in a list.</summary>
internal sealed class NumberTestPredicate<T>(ValueStep<T> value, NumericLeafTest<T> test, bool nullsEqual) : ValuePredicate
    where T : unmanaged, INumber<T>
{
    public override bool NullsEqual => nullsEqual;

    public override void CollectColumns(ISet<int> columns) => value.CollectColumns(columns);

    public override void Evaluate(in ValueContext context, Span<ulong> t, Span<ulong> f)
    {
        Span<ulong> valid = context.Scratch.RentMask(context.Length);
        ReadOnlySpan<T> values = value.Evaluate(context, valid, out bool all);
        test.RunValues(values, t);
        // The scalar is never null: with nullsEqual a null value is false.
        Combine(t, f, valid, all, default, true, nullsEqual, context.Length);
    }
}

/// <summary>Whether a computed number is null: true or false on every row.</summary>
internal sealed class NullTestPredicate<T>(ValueStep<T> value) : ValuePredicate where T : unmanaged, INumber<T>
{
    public override bool NullsEqual => false;

    public override bool NullIsTrue => true;

    public override void CollectColumns(ISet<int> columns) => value.CollectColumns(columns);

    public override void Evaluate(in ValueContext context, Span<ulong> t, Span<ulong> f)
    {
        int length = context.Length;
        value.Evaluate(context, f, out bool all);
        if (all) Masks.Fill(f, length, true);
        f.CopyTo(t);
        Masks.Not(t, length);
    }
}

/// <summary>A comparison of two computed numbers, by one of the <see cref="ValueTests"/> (the total order for floating values).</summary>
internal sealed class NumbersPredicate<T, TTest>(ValueStep<T> left, ValueStep<T> right, bool nullsEqual) : ValuePredicate
    where T : unmanaged, INumber<T>
    where TTest : struct, IValueTest<T>
{
    public override bool NullsEqual => nullsEqual;

    public override void CollectColumns(ISet<int> columns)
    {
        left.CollectColumns(columns);
        right.CollectColumns(columns);
    }

    public override void Evaluate(in ValueContext context, Span<ulong> t, Span<ulong> f)
    {
        int length = context.Length;
        Span<ulong> leftValid = context.Scratch.RentMask(length), rightValid = context.Scratch.RentMask(length);
        ReadOnlySpan<T> a = left.Evaluate(context, leftValid, out bool leftAll);
        ReadOnlySpan<T> b = right.Evaluate(context, rightValid, out bool rightAll);
        CompareKernels.Run<T, TTest>(a, b, t);
        Combine(t, f, leftValid, leftAll, rightValid, rightAll, nullsEqual, length);
    }
}

/// <summary>A comparison of two boolean columns (false before true).</summary>
internal sealed class BooleansPredicate(int left, CompareOp op, int right, bool nullsEqual) : ValuePredicate
{
    public override bool NullsEqual => nullsEqual;

    public override void CollectColumns(ISet<int> columns)
    {
        columns.Add(left);
        columns.Add(right);
    }

    public override void Evaluate(in ValueContext context, Span<ulong> t, Span<ulong> f)
    {
        int length = context.Length;
        ValueScratch scratch = context.Scratch;
        Span<ulong> a = scratch.RentMask(length), b = scratch.RentMask(length), leftValid = scratch.RentMask(length), rightValid = scratch.RentMask(length);
        bool leftAll = Load(context.Data(left), length, a, leftValid), rightAll = Load(context.Data(right), length, b, rightValid);
        for (int w = 0; w < a.Length; w++)
        {
            ulong x = a[w], y = b[w];
            t[w] = op switch
            {
                CompareOp.Eq => ~(x ^ y),
                CompareOp.NotEq => x ^ y,
                CompareOp.Lt => ~x & y,
                CompareOp.LtEq => ~x | y,
                CompareOp.Gt => x & ~y,
                _ => x | ~y,
            };
        }
        Combine(t, f, leftValid, leftAll, rightValid, rightAll, nullsEqual, length);
    }

    /// <summary>A boolean column's values (a dictionary's looked up per row) and valid rows; true when every row is valid.</summary>
    private static bool Load(ArrayData data, int length, Span<ulong> values, Span<ulong> valid)
    {
        bool all = Validity(data, length, valid);
        if (data.DataType is NullType)
        {
            values.Clear();
            return all;
        }
        if (data.Dictionary is not { } dictionary)
        {
            Masks.Load(data.Buffers[1].Span, data.Offset, length, values);
            return all;
        }
        values.Clear();
        ReadOnlySpan<byte> entries = dictionary.Buffers[1].Span;
        for (int row = 0; row < length; row++)
        {
            if (!all && (valid[row >> 6] >> (row & 63) & 1) == 0) continue;
            if (Kernels.Bits.Get(entries, dictionary.Offset + (long)DictionaryIndex(data, row))) values[row >> 6] |= 1UL << (row & 63);
        }
        return all;
    }
}

/// <summary>A comparison of two string or binary columns by their bytes (unsigned, byte by byte), any of their layouts.</summary>
internal sealed class BytesPredicate(int left, CompareOp op, int right, bool nullsEqual) : ValuePredicate
{
    public override bool NullsEqual => nullsEqual;

    public override void CollectColumns(ISet<int> columns)
    {
        columns.Add(left);
        columns.Add(right);
    }

    public override void Evaluate(in ValueContext context, Span<ulong> t, Span<ulong> f)
    {
        int length = context.Length;
        ValueScratch scratch = context.Scratch;
        Span<ulong> leftValid = scratch.RentMask(length), rightValid = scratch.RentMask(length);
        ArrayData a = context.Data(left), b = context.Data(right);
        bool leftAll = Validity(a, length, leftValid), rightAll = Validity(b, length, rightValid);
        for (int w = 0, words = Masks.Words(length); w < words; w++)
        {
            int first = w << 6, n = Math.Min(64, length - first);
            ulong both = (leftAll ? Masks.LastWord(n) : leftValid[w]) & (rightAll ? Masks.LastWord(n) : rightValid[w]), word = 0;
            for (int k = 0; k < n; k++)
            {
                if ((both >> k & 1) == 0) continue;
                ReadOnlySpan<byte> x = Value(a, first + k), y = Value(b, first + k);
                bool holds = op switch
                {
                    CompareOp.Eq => x.SequenceEqual(y),
                    CompareOp.NotEq => !x.SequenceEqual(y),
                    _ => BytesTests.Holds(op, x.SequenceCompareTo(y)),
                };
                if (holds) word |= 1UL << k;
            }
            t[w] = word;
        }
        Combine(t, f, leftValid, leftAll, rightValid, rightAll, nullsEqual, length);
    }

    /// <summary>The bytes of a valid row (a dictionary's entry).</summary>
    private static ReadOnlySpan<byte> Value(ArrayData data, int row)
    {
        if (data.Dictionary is { } dictionary) return Value(dictionary, checked((int)DictionaryIndex(data, row)));
        int at = data.Offset + row;
        switch (data.DataType)
        {
            case StringType or BinaryType:
            {
                ReadOnlySpan<byte> offsets = data.Buffers[1].Span;
                int start = BinaryPrimitives.ReadInt32LittleEndian(offsets[(at * 4)..]), end = BinaryPrimitives.ReadInt32LittleEndian(offsets[((at + 1) * 4)..]);
                return data.Buffers[2].Span[start..end];
            }
            case StringViewType or BinaryViewType:
            {
                ReadOnlySpan<byte> view = data.Buffers[1].Span.Slice(at * 16, 16);
                int length = BytesTests.Length(view);
                if (length <= BytesTests.MaxInline) return view.Slice(4, length);
                int buffer = BinaryPrimitives.ReadInt32LittleEndian(view[8..]), offset = BinaryPrimitives.ReadInt32LittleEndian(view[12..]);
                if ((uint)(buffer + 2) >= (uint)data.Buffers.Length) throw new InvalidDataException($"A view points into data buffer {buffer} of {data.Buffers.Length - 2}.");
                return data.Buffers[buffer + 2].Span.Slice(offset, length);
            }
            case FixedSizeBinaryType fixedSize:
                return data.Buffers[1].Span.Slice(at * fixedSize.ByteWidth, fixedSize.ByteWidth);
            default:
                throw new InvalidOperationException($"No bytes in a {data.DataType.Name} column.");
        }
    }
}
