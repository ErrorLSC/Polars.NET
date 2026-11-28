using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Compute;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Sazanami.Data.Mapping;
using Kaminari.Sazanami.Kernels;

namespace Kaminari.Sazanami.Ordering;

/// <summary>One key of a sort order: its direction and where its nulls go.</summary>
/// <param name="Descending">Whether larger values come first.</param>
/// <param name="NullsFirst">Whether nulls come before the values (else after them).</param>
public readonly record struct SortKey(bool Descending = false, bool NullsFirst = false);

/// <summary>
/// A key column of a batch: a column, or a field nested in structs of it (<paramref name="Fields"/>: the child index
/// at each level). A row is null at the key when the field or a struct above it is.
/// </summary>
/// <param name="Column">The batch's column.</param>
/// <param name="Fields">The child indexes from the column down to the field; null or empty for the column itself.</param>
public readonly record struct OrderColumn(IArrowArray Column, int[]? Fields = null);

/// <summary>Where rows stop following a sort order.</summary>
/// <param name="Row">The row that sorts before the one above it.</param>
/// <param name="Key">The key that orders the two rows wrongly (the keys before it tie).</param>
/// <param name="Previous">The value of the row above at that key, as text.</param>
/// <param name="Value">The row's value at that key, as text.</param>
public readonly record struct OrderBreak(long Row, int Key, string Previous, string Value);

/// <summary>
/// The natural order of Arrow values, by which rows are compared key by key: numbers by value (NaN above every
/// number and equal to itself, -0.0 equal to 0.0), unsigned integers unsigned, decimals by value (one scale per
/// column), dates, times, timestamps and durations by their stored count (one unit per column), booleans false first,
/// strings and binary by their bytes (unsigned, code point order for UTF-8), dictionary-encoded values by their
/// values.
/// </summary>
/// <remarks>
/// Rows are checked a chunk at a time, a key at a time: each key gives two bitmaps over the chunk's pairs of
/// neighbouring rows, the pairs it orders wrongly and those it ties (fixed-width values compared a vector at a time,
/// nulls placed by whole validity words); a later key counts only on the pairs every key before it ties.
/// </remarks>
public static class ArrowOrder
{
    /// <summary>The pairs of rows a chunk compares (64 bitmap words).</summary>
    private const int ChunkPairs = 4096;

    /// <summary>The rows a helper thread checks at once.</summary>
    internal const int SegmentRows = ChunkPairs;

    /// <summary>Whether values of <paramref name="type"/> have an order here.</summary>
    public static bool CanOrder(IArrowType type) => type switch
    {
        DictionaryType d => CanOrder(d.ValueType),
        Int8Type or Int16Type or Int32Type or Int64Type or UInt8Type or UInt16Type or UInt32Type or UInt64Type => true,
        HalfFloatType or FloatType or DoubleType => true,
        Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type => true,
        Date32Type or Date64Type or Time32Type or Time64Type or TimestampType or DurationType => true,
        BooleanType => true,
        StringType or LargeStringType or StringViewType or BinaryType or LargeBinaryType or BinaryViewType or FixedSizeBinaryType => true,
        _ => false,
    };

    /// <summary>
    /// The type of the key a column's <paramref name="fields"/> reach (structs gone through); null when a level is no
    /// struct or has no such child.
    /// </summary>
    public static IArrowType? KeyType(IArrowType column, ReadOnlySpan<int> fields)
    {
        IArrowType type = column;
        foreach (int f in fields)
        {
            if (type is not StructType s || (uint)f >= (uint)s.Fields.Count) return null;
            type = s.Fields[f].DataType;
        }
        return type;
    }

    /// <summary>
    /// The first row of <paramref name="start"/> .. +<paramref name="count"/> that sorts before the row above it by
    /// <paramref name="keys"/> in <paramref name="order"/>, -1 when the rows follow the order (ties are in order).
    /// </summary>
    /// <param name="key">The key that orders that row wrongly.</param>
    /// <exception cref="ArgumentException">The keys and their order differ in number.</exception>
    /// <exception cref="NotSupportedException">A key's values have no order here (<see cref="CanOrder"/>).</exception>
    public static int FirstOutOfOrder(ReadOnlySpan<OrderColumn> keys, ReadOnlySpan<SortKey> order, int start, int count, out int key) =>
        FirstOutOfOrder(keys, order, start, count, null, out key);

    /// <summary>
    /// <see cref="FirstOutOfOrder(ReadOnlySpan{OrderColumn}, ReadOnlySpan{SortKey}, int, int, out int)"/>, segments of
    /// many rows checked side by side by helpers <paramref name="budget"/> has threads for (the caller's thread counted in it).
    /// </summary>
    internal static int FirstOutOfOrder(ReadOnlySpan<OrderColumn> keys, ReadOnlySpan<SortKey> order, int start, int count, WorkBudget? budget, out int key)
    {
        if (keys.Length != order.Length) throw new ArgumentException("Every key needs its order.", nameof(order));
        key = -1;
        if (count < 2) return -1;
        Column[] columns = Columns(keys);
        foreach (Column column in columns) column.RowsChecked = count;
        SortKey[] sortKeys = [.. order];
        if (budget is null || budget.Max == 1 || count < 2 * SegmentRows) return Chunks(columns, sortKeys, start, count, out key);
        return SideBySide(columns, sortKeys, start, count, budget, out key);
    }

    /// <summary>Segments of the rows checked by the caller and the helpers the budget allows; the first break of all.</summary>
    private static int SideBySide(Column[] columns, SortKey[] order, int start, int count, WorkBudget budget, out int key)
    {
        int segments = (count + SegmentRows - 1) / SegmentRows;
        var rows = new int[segments];
        var keys = new int[segments];
        int next = -1, first = int.MaxValue; // the next segment to take; the first segment with a break
        Exception? failure = null;
        List<Task> helpers = [];
        budget.Enter(); // the caller's thread, counted while it checks
        for (int h = 1; h < segments && budget.TryEnter(); h++)
        {
            helpers.Add(Task.Run(() =>
            {
                try
                {
                    Take();
                }
                finally
                {
                    budget.Exit();
                }
            }));
        }
        try
        {
            Take();
        }
        finally
        {
            budget.Exit();
        }
        Task.WaitAll([.. helpers]);
        if (failure is not null) System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(failure);
        if (first == int.MaxValue)
        {
            key = -1;
            return -1;
        }
        key = keys[first];
        return rows[first];

        void Take()
        {
            try
            {
                for (int s; (s = Interlocked.Increment(ref next)) < segments;)
                {
                    if (s > Volatile.Read(ref first)) return; // a break before it is found: it cannot be the first
                    // Each segment's pairs start at its first row, so the row before it is compared too.
                    int from = s == 0 ? start : start + s * SegmentRows - 1;
                    int end = Math.Min(start + count, start + (s + 1) * SegmentRows);
                    rows[s] = Chunks(columns, order, from, end - from, out keys[s]);
                    if (rows[s] < 0) continue;
                    for (int seen = Volatile.Read(ref first); s < seen; seen = Volatile.Read(ref first))
                    {
                        if (Interlocked.CompareExchange(ref first, s, seen) == seen) break;
                    }
                }
            }
            catch (Exception e)
            {
                Interlocked.CompareExchange(ref failure, e, null);
            }
        }
    }

    /// <summary>The rows checked chunk by chunk, key by key (see the remarks).</summary>
    private static int Chunks(Column[] columns, SortKey[] order, int start, int count, out int key)
    {
        Span<ulong> active = stackalloc ulong[ChunkPairs / 64], bad = stackalloc ulong[ChunkPairs / 64], tie = stackalloc ulong[ChunkPairs / 64];
        for (int a = start + 1; a < start + count; a += ChunkPairs)
        {
            int n = Math.Min(ChunkPairs, start + count - a), words = (n + 63) >> 6;
            int best = int.MaxValue, bestKey = -1;
            for (int k = 0; k < columns.Length; k++)
            {
                bool ties = k + 1 < columns.Length;
                columns[k].Pairs(a, n, order[k], bad, tie, ties);
                for (int w = 0; w < words; w++)
                {
                    ulong found = k == 0 ? bad[w] : active[w] & bad[w];
                    if (found == 0) continue;
                    int p = (w << 6) + BitOperations.TrailingZeroCount(found);
                    if (p < best) (best, bestKey) = (p, k);
                    break;
                }
                if (!ties) break;
                bool any = false;
                for (int w = 0; w < words; w++)
                {
                    active[w] = k == 0 ? tie[w] : active[w] & tie[w];
                    any |= active[w] != 0;
                }
                if (!any) break; // no pair left for later keys to order
            }
            if (best != int.MaxValue)
            {
                key = bestKey;
                return a + best;
            }
        }
        key = -1;
        return -1;
    }

    /// <summary>
    /// How row <paramref name="a"/> of keys <paramref name="x"/> compares with row <paramref name="b"/> of keys
    /// <paramref name="y"/> (columns of the same types) in <paramref name="order"/>: negative before, 0 tied, positive after.
    /// </summary>
    /// <param name="key">The first key that does not tie (-1 when all do).</param>
    public static int Compare(ReadOnlySpan<OrderColumn> x, int a, ReadOnlySpan<OrderColumn> y, int b, ReadOnlySpan<SortKey> order, out int key)
    {
        if (x.Length != order.Length || y.Length != order.Length) throw new ArgumentException("Every key needs its order.", nameof(order));
        Column[] left = Columns(x), right = Columns(y);
        for (int k = 0; k < left.Length; k++)
        {
            int sign = Compare(left[k], a, right[k], b, order[k]);
            if (sign != 0)
            {
                key = k;
                return sign;
            }
        }
        key = -1;
        return 0;
    }

    /// <summary>The value of row <paramref name="row"/> at a key, as text for messages.</summary>
    public static string Format(OrderColumn key, int row)
    {
        Column column = Column.Of(key);
        if (column.IsNull(row)) return "null";
        return column.Format(row);
    }

    /// <summary>
    /// Row <paramref name="row"/> of a key's field alone, as a one-row array of the field's type (a copy, the
    /// caller's to dispose): null where the field or a struct above it is.
    /// </summary>
    internal static IArrowArray TakeRow(OrderColumn key, int row)
    {
        Column column = Column.Of(key);
        int index = column.IsNull(row) ? ArrowTake.NullRow : column.FieldIndex(row);
        if (column.Field is DictionaryArray dictionary)
        {
            // The value alone, as a dictionary of one entry: a copy of a whole dictionary per call would cost a sink
            // writing a few rows at a time as much as its rows do.
            int entry = index == ArrowTake.NullRow ? ArrowTake.NullRow : column.Entry(index);
            IArrowArray value = ArrowTake.Take(dictionary.Dictionary, [entry], shareViewData: false);
            var indices = new Int32Array.Builder();
            if (entry == ArrowTake.NullRow) indices.AppendNull();
            else indices.Append(0);
            return new DictionaryArray(new DictionaryType(Int32Type.Default, value.Data.DataType, false), indices.Build(Arrow.Builders.PooledArrowAllocator.Instance), value);
        }
        if (key.Fields is not { Length: > 0 }) return ArrowTake.Take(key.Column, [row], shareViewData: false);
        return ArrowTake.Take(column.Field, [index], shareViewData: false);
    }

    private static Column[] Columns(ReadOnlySpan<OrderColumn> keys)
    {
        var columns = new Column[keys.Length];
        for (int k = 0; k < keys.Length; k++) columns[k] = Column.Of(keys[k]);
        return columns;
    }

    private static int Compare(Column x, int a, Column y, int b, SortKey order)
    {
        bool aNull = x.IsNull(a), bNull = y.IsNull(b);
        if (aNull || bNull)
        {
            if (aNull && bNull) return 0;
            return aNull == order.NullsFirst ? -1 : 1;
        }
        int sign = x.CompareValues(a, y, b);
        return order.Descending ? -sign : sign;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Set(Span<ulong> bits, int j) => bits[j >> 6] |= 1UL << (j & 63);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsSet(ReadOnlySpan<ulong> bits, int j) => (bits[j >> 6] >> (j & 63) & 1) != 0;

    /// <summary>A key's values in one batch: the field's array, the structs above it, and where its rows lie in it.</summary>
    private abstract class Column
    {
        private StructArray[] _parents = [];
        private int[] _parentShifts = [];
        private bool _nulls; // whether a row may be null at the key (the field's or a struct's validity, or a dictionary's null values)
        protected int Shift { get; private set; } // the field array's index of the column's row 0

        /// <summary>How many rows are checked (a dictionary's values are looked over only when they are no more).</summary>
        public int RowsChecked { get; set; } = int.MaxValue;

        /// <summary>The dictionary entry index <paramref name="i"/> of the field's array names (a dictionary's).</summary>
        public virtual int Entry(int i) => throw new InvalidOperationException("Not a dictionary.");

        public static Column Of(OrderColumn key)
        {
            IArrowArray array = key.Column;
            List<StructArray> parents = [];
            List<int> shifts = [];
            int shift = 0;
            foreach (int f in key.Fields ?? [])
            {
                if (array is not StructArray s || (uint)f >= (uint)s.Fields.Count)
                    throw new ArgumentException($"The key's field {f} does not lie in a struct.", nameof(key));
                parents.Add(s);
                shifts.Add(shift);
                shift += s.Offset; // its children's data is not sliced with it (StructArray.Fields is: not taken)
                array = ArrowArrayFactory.BuildArray(s.Data.Children[f]);
            }
            Column column = Leaf(array);
            column._parents = [.. parents];
            column._parentShifts = [.. shifts];
            column.Shift = shift;
            column._nulls = column.LeafNulls || parents.Any(p => p.NullCount > 0);
            return column;
        }

        public static Column Leaf(IArrowArray array) => array switch
        {
            Int8Array a => new Numbers<sbyte>(a),
            Int16Array a => new Numbers<short>(a),
            Int32Array a => new Numbers<int>(a),
            Int64Array a => new Numbers<long>(a),
            UInt8Array a => new Numbers<byte>(a),
            UInt16Array a => new Numbers<ushort>(a),
            UInt32Array a => new Numbers<uint>(a),
            UInt64Array a => new Numbers<ulong>(a),
            Date32Array a => new Numbers<int>(a),
            Date64Array a => new Numbers<long>(a),
            Time32Array a => new Numbers<int>(a),
            Time64Array a => new Numbers<long>(a),
            TimestampArray a => new Numbers<long>(a),
            DurationArray a => new Numbers<long>(a),
            Decimal32Array a => new Numbers<int>(a),
            Decimal64Array a => new Numbers<long>(a),
            HalfFloatArray a => new Halves(a),
            FloatArray a => new Floats<float>(a),
            DoubleArray a => new Floats<double>(a),
            Decimal128Array a => new Wide(a, 16),
            Decimal256Array a => new Wide(a, 32),
            BooleanArray a => new Booleans(a),
            BinaryArray a => new OffsetBytes(a), // StringArray too
            LargeBinaryArray a => new LargeOffsetBytes(a),
            BinaryViewArray a => new ViewBytes(a), // StringViewArray too
            FixedSizeBinaryArray a => new FixedBytes(a, ((FixedSizeBinaryType)a.Data.DataType).ByteWidth),
            DictionaryArray a => new Dictionary(a),
            _ => throw new NotSupportedException($"Values of type {array.Data.DataType.Name} have no order here."),
        };

        protected Column(IArrowArray array) => Array = array;

        protected IArrowArray Array { get; }

        /// <summary>The field's own array (a struct field's: not sliced with its structs).</summary>
        public IArrowArray Field => Array;

        /// <summary>The index of the column's row <paramref name="row"/> in <see cref="Field"/>.</summary>
        public int FieldIndex(int row) => row + Shift;

        /// <summary>Whether the field's own array may hold nulls.</summary>
        protected virtual bool LeafNulls => Array.NullCount > 0;

        /// <summary>Whether the column's row <paramref name="row"/> is null at the key.</summary>
        public bool IsNull(int row)
        {
            for (int p = 0; p < _parents.Length; p++)
            {
                if (_parents[p].IsNull(row + _parentShifts[p])) return true;
            }
            return IsNullAt(row + Shift);
        }

        /// <summary>Whether index <paramref name="i"/> of the field's array is null.</summary>
        protected virtual bool IsNullAt(int i) => Array.IsNull(i);

        /// <summary>Compares the (non-null) values of rows <paramref name="a"/> and <paramref name="b"/> of <paramref name="other"/>.</summary>
        public int CompareValues(int a, Column other, int b) => CompareAt(a + Shift, other, b + other.Shift);

        /// <summary>Compares the values at indexes of the field arrays (of a column of the same type).</summary>
        public abstract int CompareAt(int i, Column other, int j);

        public string Format(int row) => FormatAt(row + Shift);

        protected virtual string FormatAt(int i) => Array.FormatValue(i);

        /// <summary>
        /// The pairs of rows (r - 1, r) for r in <paramref name="first"/> .. +<paramref name="n"/>, bit r - first: in
        /// <paramref name="bad"/> those the key orders wrongly, in <paramref name="tie"/> (with <paramref name="ties"/>)
        /// those it ties; bits from <paramref name="n"/> on cleared.
        /// </summary>
        public void Pairs(int first, int n, SortKey key, Span<ulong> bad, Span<ulong> tie, bool ties)
        {
            int words = (n + 63) >> 6;
            bad = bad[..words];
            tie = tie[..words];
            bad.Clear();
            if (ties) tie.Clear();
            if (!_nulls)
            {
                ValuePairs(first + Shift, n, key.Descending, bad, tie, ties, default);
            }
            else
            {
                Span<ulong> current = stackalloc ulong[words], previous = stackalloc ulong[words], both = stackalloc ulong[words];
                Valid(first, current);
                Valid(first - 1, previous);
                for (int w = 0; w < words; w++) both[w] = current[w] & previous[w];
                ValuePairs(first + Shift, n, key.Descending, bad, tie, ties, both);
                for (int w = 0; w < words; w++)
                {
                    ulong c = current[w], p = previous[w], b = both[w];
                    // A value then a null is wrong with nulls first, a null then a value with nulls last; two nulls tie.
                    bad[w] = bad[w] & b | (key.NullsFirst ? p & ~c : ~p & c);
                    if (ties) tie[w] = tie[w] & b | ~p & ~c;
                }
            }
            if ((n & 63) != 0)
            {
                ulong keep = (1UL << (n & 63)) - 1;
                bad[words - 1] &= keep;
                if (ties) tie[words - 1] &= keep;
            }
        }

        /// <summary>Whether each row from <paramref name="first"/> holds a value at the key, a bit per row.</summary>
        private void Valid(int first, Span<ulong> words)
        {
            LeafValid(first + Shift, words);
            for (int p = 0; p < _parents.Length; p++)
            {
                ArrayData data = _parents[p].Data;
                if (data.NullCount == 0) continue;
                ReadOnlySpan<byte> validity = data.Buffers[0].Span;
                long bit = data.Offset + first + _parentShifts[p];
                for (int w = 0; w < words.Length; w++) words[w] &= Bits.Word(validity, bit + 64L * w);
            }
        }

        /// <summary>Whether each index of the field's array from <paramref name="first"/> holds a value, a bit per index.</summary>
        protected virtual void LeafValid(int first, Span<ulong> words)
        {
            ArrayData data = Array.Data;
            if (data.NullCount == 0 || data.Buffers[0].IsEmpty)
            {
                words.Fill(ulong.MaxValue);
                return;
            }
            ReadOnlySpan<byte> validity = data.Buffers[0].Span;
            for (int w = 0; w < words.Length; w++) words[w] = Bits.Word(validity, data.Offset + first + 64L * w);
        }

        /// <summary>
        /// The pairs of indexes (i - 1, i) of the field's array for i in <paramref name="first"/> .. +<paramref name="n"/>
        /// whose values are out of order (larger first, or with <paramref name="descending"/> smaller first) into
        /// <paramref name="bad"/> and tied into <paramref name="tie"/>; pairs not in <paramref name="both"/> (when
        /// given) are not looked at (their bits are cleared after).
        /// </summary>
        public virtual void ValuePairs(int first, int n, bool descending, Span<ulong> bad, Span<ulong> tie, bool ties, ReadOnlySpan<ulong> both)
        {
            for (int j = 0; j < n; j++)
            {
                if (!both.IsEmpty && !IsSet(both, j)) continue;
                int sign = CompareAt(first + j - 1, this, first + j);
                if (descending) sign = -sign;
                if (sign > 0) Set(bad, j);
                else if (sign == 0 && ties) Set(tie, j);
            }
        }
    }

    /// <summary>Integers, and values stored as integers (dates, times, timestamps, durations, small decimals).</summary>
    private sealed class Numbers<T>(IArrowArray array) : Column(array) where T : unmanaged, INumber<T>
    {
        internal ReadOnlySpan<T> Values => MemoryMarshal.Cast<byte, T>(Array.Data.Buffers[1].Span).Slice(Array.Data.Offset);

        public override int CompareAt(int i, Column other, int j) => Values[i].CompareTo(((Numbers<T>)other).Values[j]);

        public override void ValuePairs(int first, int n, bool descending, Span<ulong> bad, Span<ulong> tie, bool ties, ReadOnlySpan<ulong> both) =>
            NumberPairs(Values, first, n, descending, bad, tie, ties);
    }

    /// <summary>Pairs of fixed-width numbers, a vector at a time (a vector's lanes divide a bitmap word).</summary>
    private static void NumberPairs<T>(ReadOnlySpan<T> values, int first, int n, bool descending, Span<ulong> bad, Span<ulong> tie, bool ties)
        where T : unmanaged, INumber<T>
    {
        ref T v = ref MemoryMarshal.GetReference(values);
        int j = 0;
        if (Vector256.IsHardwareAccelerated && Vector256<T>.IsSupported)
        {
            for (; j + Vector256<T>.Count <= n; j += Vector256<T>.Count)
            {
                Vector256<T> x = Vector256.LoadUnsafe(ref v, (nuint)(first + j - 1)), y = Vector256.LoadUnsafe(ref v, (nuint)(first + j));
                Vector256<T> wrong = descending ? Vector256.LessThan(x, y) : Vector256.GreaterThan(x, y);
                bad[j >> 6] |= (ulong)Vector256.ExtractMostSignificantBits(wrong) << (j & 63);
                if (ties) tie[j >> 6] |= (ulong)Vector256.ExtractMostSignificantBits(Vector256.Equals(x, y)) << (j & 63);
            }
        }
        else if (Vector128.IsHardwareAccelerated && Vector128<T>.IsSupported)
        {
            for (; j + Vector128<T>.Count <= n; j += Vector128<T>.Count)
            {
                Vector128<T> x = Vector128.LoadUnsafe(ref v, (nuint)(first + j - 1)), y = Vector128.LoadUnsafe(ref v, (nuint)(first + j));
                Vector128<T> wrong = descending ? Vector128.LessThan(x, y) : Vector128.GreaterThan(x, y);
                bad[j >> 6] |= (ulong)Vector128.ExtractMostSignificantBits(wrong) << (j & 63);
                if (ties) tie[j >> 6] |= (ulong)Vector128.ExtractMostSignificantBits(Vector128.Equals(x, y)) << (j & 63);
            }
        }
        for (; j < n; j++)
        {
            T x = Unsafe.Add(ref v, first + j - 1), y = Unsafe.Add(ref v, first + j);
            if (descending ? x < y : x > y) Set(bad, j);
            else if (ties && x == y) Set(tie, j);
        }
    }

    /// <summary>FLOAT and DOUBLE: NaN above every number and equal to itself, -0.0 equal to 0.0.</summary>
    private sealed class Floats<T>(IArrowArray array) : Column(array) where T : unmanaged, IFloatingPoint<T>
    {
        private ReadOnlySpan<T> Values => MemoryMarshal.Cast<byte, T>(Array.Data.Buffers[1].Span).Slice(Array.Data.Offset);

        public override int CompareAt(int i, Column other, int j) => CompareFloats(Values[i], ((Floats<T>)other).Values[j]);

        public override void ValuePairs(int first, int n, bool descending, Span<ulong> bad, Span<ulong> tie, bool ties, ReadOnlySpan<ulong> both)
        {
            ref T v = ref MemoryMarshal.GetReference(Values);
            int j = 0;
            if (Vector256.IsHardwareAccelerated && Vector256<T>.IsSupported)
            {
                for (; j + Vector256<T>.Count <= n; j += Vector256<T>.Count)
                {
                    Vector256<T> x = Vector256.LoadUnsafe(ref v, (nuint)(first + j - 1)), y = Vector256.LoadUnsafe(ref v, (nuint)(first + j));
                    Vector256<T> xNaN = ~Vector256.Equals(x, x), yNaN = ~Vector256.Equals(y, y);
                    Vector256<T> wrong = descending ? Vector256.LessThan(x, y) | Vector256.AndNot(yNaN, xNaN) : Vector256.GreaterThan(x, y) | Vector256.AndNot(xNaN, yNaN);
                    bad[j >> 6] |= (ulong)Vector256.ExtractMostSignificantBits(wrong) << (j & 63);
                    if (ties) tie[j >> 6] |= (ulong)Vector256.ExtractMostSignificantBits(Vector256.Equals(x, y) | xNaN & yNaN) << (j & 63);
                }
            }
            else if (Vector128.IsHardwareAccelerated && Vector128<T>.IsSupported)
            {
                for (; j + Vector128<T>.Count <= n; j += Vector128<T>.Count)
                {
                    Vector128<T> x = Vector128.LoadUnsafe(ref v, (nuint)(first + j - 1)), y = Vector128.LoadUnsafe(ref v, (nuint)(first + j));
                    Vector128<T> xNaN = ~Vector128.Equals(x, x), yNaN = ~Vector128.Equals(y, y);
                    Vector128<T> wrong = descending ? Vector128.LessThan(x, y) | Vector128.AndNot(yNaN, xNaN) : Vector128.GreaterThan(x, y) | Vector128.AndNot(xNaN, yNaN);
                    bad[j >> 6] |= (ulong)Vector128.ExtractMostSignificantBits(wrong) << (j & 63);
                    if (ties) tie[j >> 6] |= (ulong)Vector128.ExtractMostSignificantBits(Vector128.Equals(x, y) | xNaN & yNaN) << (j & 63);
                }
            }
            for (; j < n; j++)
            {
                int sign = CompareFloats(Unsafe.Add(ref v, first + j - 1), Unsafe.Add(ref v, first + j));
                if (descending) sign = -sign;
                if (sign > 0) Set(bad, j);
                else if (sign == 0 && ties) Set(tie, j);
            }
        }
    }

    private static int CompareFloats<T>(T x, T y) where T : IFloatingPoint<T>
    {
        if (x < y) return -1;
        if (x > y) return 1;
        if (x == y) return 0; // -0.0 equal to 0.0
        bool xNaN = T.IsNaN(x), yNaN = T.IsNaN(y);
        return xNaN == yNaN ? 0 : xNaN ? 1 : -1; // NaN above every number
    }

    /// <summary>FLOAT16, compared one by one (no vectors of halves).</summary>
    private sealed class Halves(IArrowArray array) : Column(array)
    {
        private ReadOnlySpan<Half> Values => MemoryMarshal.Cast<byte, Half>(Array.Data.Buffers[1].Span).Slice(Array.Data.Offset);

        public override int CompareAt(int i, Column other, int j) => CompareFloats(Values[i], ((Halves)other).Values[j]);
    }

    /// <summary>Decimals of 16 or 32 bytes (little-endian two's complement).</summary>
    private sealed class Wide(IArrowArray array, int width) : Column(array)
    {
        private ReadOnlySpan<byte> Value(int i) => Array.Data.Buffers[1].Span.Slice((Array.Data.Offset + i) * width, width);

        public override int CompareAt(int i, Column other, int j)
        {
            ReadOnlySpan<byte> x = Value(i), y = ((Wide)other).Value(j);
            // The most significant half signed, the rest unsigned, from the top.
            int top = width - 16;
            int sign = BinaryPrimitives.ReadInt128LittleEndian(x[top..]).CompareTo(BinaryPrimitives.ReadInt128LittleEndian(y[top..]));
            if (sign != 0 || width == 16) return sign;
            return BinaryPrimitives.ReadUInt128LittleEndian(x).CompareTo(BinaryPrimitives.ReadUInt128LittleEndian(y));
        }
    }

    /// <summary>Booleans, false first: compared a bitmap word at a time.</summary>
    private sealed class Booleans(BooleanArray array) : Column(array)
    {
        public override int CompareAt(int i, Column other, int j) => array.GetValue(i)!.Value.CompareTo(((Booleans)other).Get(j));

        private bool Get(int j) => array.GetValue(j)!.Value;

        public override void ValuePairs(int first, int n, bool descending, Span<ulong> bad, Span<ulong> tie, bool ties, ReadOnlySpan<ulong> both)
        {
            ReadOnlySpan<byte> values = Array.Data.Buffers[1].Span;
            long bit = Array.Data.Offset + first;
            for (int w = 0; w < bad.Length; w++)
            {
                ulong p = Bits.Word(values, bit + 64L * w - 1), c = Bits.Word(values, bit + 64L * w);
                bad[w] = descending ? ~p & c : p & ~c; // true then false, ascending
                if (ties) tie[w] = ~(p ^ c);
            }
        }
    }

    /// <summary>Strings and binaries: their bytes, unsigned. Pairs holding a null are not looked at (a view there may point anywhere).</summary>
    private abstract class Bytes(IArrowArray array) : Column(array)
    {
        protected abstract ReadOnlySpan<byte> Get(int i);

        public override int CompareAt(int i, Column other, int j) => Math.Sign(Get(i).SequenceCompareTo(((Bytes)other).Get(j)));

        public override void ValuePairs(int first, int n, bool descending, Span<ulong> bad, Span<ulong> tie, bool ties, ReadOnlySpan<ulong> both)
        {
            for (int j = 0; j < n; j++)
            {
                if (!both.IsEmpty && !IsSet(both, j)) continue;
                int sign = CompareBytes(Get(first + j - 1), Get(first + j));
                if (descending) sign = -sign;
                if (sign > 0) Set(bad, j);
                else if (sign == 0 && ties) Set(tie, j);
            }
        }
    }

    private sealed class OffsetBytes(BinaryArray array) : Bytes(array)
    {
        protected override ReadOnlySpan<byte> Get(int i) => array.GetBytes(i);

        public override void ValuePairs(int first, int n, bool descending, Span<ulong> bad, Span<ulong> tie, bool ties, ReadOnlySpan<ulong> both)
        {
            ReadOnlySpan<int> offsets = array.ValueOffsets.Slice(first - 1, n + 2);
            ReadOnlySpan<byte> data = array.ValueBuffer.Span;
            // Values of at most 16 bytes whose offsets rise within the data (with 16 bytes after the last but one):
            // compared 16 bytes at a time without a check each. Others: one by one.
            if (offsets[0] < 0 || offsets[^1] > data.Length || !Rising(offsets))
            {
                Checked(offsets, data, n, descending, bad, tie, ties, both);
                return;
            }
            if (offsets[^2] > data.Length - 16 || !Short(offsets) || !Vector128.IsHardwareAccelerated)
            {
                Long(offsets, data, n, descending, bad, tie, ties);
                return;
            }
            // Every pair is compared (a null's bytes lie within the data too); the nulls' bits are cleared after.
            ref int offset = ref MemoryMarshal.GetReference(offsets);
            ref byte bytes = ref MemoryMarshal.GetReference(data);
            int direction = descending ? -1 : 1;
            int start = offset, middle = Unsafe.Add(ref offset, 1);
            ulong wrong = 0, tied = 0;
            for (int j = 0; j < n; j++)
            {
                int end = Unsafe.Add(ref offset, j + 2);
                int xLength = middle - start, yLength = end - middle, shared = Math.Min(xLength, yLength);
                ref byte x = ref Unsafe.Add(ref bytes, start), y = ref Unsafe.Add(ref bytes, middle);
                // The first byte that differs decides, else the lengths.
                uint differ = ~Vector128.ExtractMostSignificantBits(Vector128.Equals(Vector128.LoadUnsafe(ref x), Vector128.LoadUnsafe(ref y)));
                int at = BitOperations.TrailingZeroCount(differ | 1u << 16);
                int sign = (at < shared ? Unsafe.Add(ref x, at) - Unsafe.Add(ref y, at) : xLength - yLength) * direction;
                wrong |= (ulong)(sign > 0 ? 1 : 0) << j;
                tied |= (ulong)(sign == 0 ? 1 : 0) << j;
                if ((j & 63) == 63)
                {
                    bad[j >> 6] = wrong;
                    if (ties) tie[j >> 6] = tied;
                    wrong = tied = 0;
                }
                (start, middle) = (middle, end);
            }
            if ((n & 63) != 0)
            {
                bad[n >> 6] = wrong;
                if (ties) tie[n >> 6] = tied;
            }

            // Whether every value takes at most 16 bytes (falling offsets wrap above 16 too), a vector at a time.
            static bool Short(ReadOnlySpan<int> offsets)
            {
                int i = 1;
                if (Vector256.IsHardwareAccelerated)
                {
                    ref int o = ref MemoryMarshal.GetReference(offsets);
                    Vector256<uint> limit = Vector256.Create(16u);
                    for (; i + Vector256<int>.Count <= offsets.Length; i += Vector256<int>.Count)
                    {
                        Vector256<uint> lengths = (Vector256.LoadUnsafe(ref o, (nuint)i) - Vector256.LoadUnsafe(ref o, (nuint)(i - 1))).AsUInt32();
                        if (Vector256.GreaterThanAny(lengths, limit)) return false;
                    }
                }
                for (; i < offsets.Length; i++)
                {
                    if ((uint)(offsets[i] - offsets[i - 1]) > 16) return false;
                }
                return true;
            }

            static bool Rising(ReadOnlySpan<int> offsets)
            {
                for (int i = 1; i < offsets.Length; i++)
                {
                    if (offsets[i] < offsets[i - 1]) return false;
                }
                return true;
            }

            // Values of any length within the data (offsets checked): compared 16 bytes at a time where the data has them.
            static void Long(ReadOnlySpan<int> offsets, ReadOnlySpan<byte> data, int n, bool descending, Span<ulong> bad, Span<ulong> tie, bool ties)
            {
                ref byte bytes = ref MemoryMarshal.GetReference(data);
                for (int j = 0; j < n; j++)
                {
                    int x = offsets[j], y = offsets[j + 1], end = offsets[j + 2];
                    int sign = CompareWithin(ref Unsafe.Add(ref bytes, x), y - x, data.Length - x, ref Unsafe.Add(ref bytes, y), end - y, data.Length - y);
                    if (descending) sign = -sign;
                    if (sign > 0) Set(bad, j);
                    else if (sign == 0 && ties) Set(tie, j);
                }
            }

            // Any values: their bytes by checked slices (offsets out of the data throw), pairs holding a null passed over.
            static void Checked(ReadOnlySpan<int> offsets, ReadOnlySpan<byte> data, int n, bool descending, Span<ulong> bad, Span<ulong> tie, bool ties,
                ReadOnlySpan<ulong> both)
            {
                for (int j = 0; j < n; j++)
                {
                    if (!both.IsEmpty && !IsSet(both, j)) continue;
                    int sign = CompareBytes(data[offsets[j]..offsets[j + 1]], data[offsets[j + 1]..offsets[j + 2]]);
                    if (descending) sign = -sign;
                    if (sign > 0) Set(bad, j);
                    else if (sign == 0 && ties) Set(tie, j);
                }
            }
        }
    }

    /// <summary>
    /// Two byte strings in unsigned order, for neighbours of a sorted column: mostly equal (a run) or apart within
    /// their first bytes, so the first 8 bytes (as big-endian numbers) and equality are tried first.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CompareBytes(ReadOnlySpan<byte> x, ReadOnlySpan<byte> y)
    {
        int shared = Math.Min(x.Length, y.Length);
        if (shared >= 8)
        {
            ulong a = BinaryPrimitives.ReadUInt64BigEndian(x), b = BinaryPrimitives.ReadUInt64BigEndian(y);
            if (a != b) return a < b ? -1 : 1;
            if (shared <= 16)
            {
                // The shared bytes' last 8 (overlapping the first 8, which are equal) decide the rest of them.
                a = BinaryPrimitives.ReadUInt64BigEndian(x[(shared - 8)..]);
                b = BinaryPrimitives.ReadUInt64BigEndian(y[(shared - 8)..]);
                if (a != b) return a < b ? -1 : 1;
                return x.Length.CompareTo(y.Length);
            }
            return x[8..].SequenceCompareTo(y[8..]);
        }
        return x.SequenceCompareTo(y);
    }

    private sealed class LargeOffsetBytes(LargeBinaryArray array) : Bytes(array)
    {
        protected override ReadOnlySpan<byte> Get(int i) => array.GetBytes(i);
    }

    /// <summary>String and binary views: their 4-byte prefixes, inline, decide most pairs.</summary>
    /// <summary>
    /// String and binary views: inline values (12 bytes at most) compared as two big-endian numbers cut to their
    /// lengths; others by their 4-byte prefixes, then their bytes 16 at a time from the data buffers (each view's
    /// buffer, offset and length checked against them).
    /// </summary>
    private sealed class ViewBytes(BinaryViewArray array) : Bytes(array)
    {
        protected override ReadOnlySpan<byte> Get(int i) => array.GetBytes(i);

        public override unsafe void ValuePairs(int first, int n, bool descending, Span<ulong> bad, Span<ulong> tie, bool ties, ReadOnlySpan<ulong> both)
        {
            ArrowBuffer[] buffers = Array.Data.Buffers;
            int dataBuffers = buffers.Length - 2;
            if (dataBuffers > 64)
            {
                base.ValuePairs(first, n, descending, bad, tie, ties, both);
                return;
            }
            ReadOnlySpan<byte> views = buffers[1].Span.Slice((Array.Data.Offset + first - 1) * 16, (n + 1) * 16); // the rows' views, checked once
            var pins = new System.Buffers.MemoryHandle[dataBuffers];
            nint* starts = stackalloc nint[Math.Max(1, dataBuffers)];
            int* lengths = stackalloc int[Math.Max(1, dataBuffers)];
            try
            {
                for (int b = 0; b < dataBuffers; b++)
                {
                    pins[b] = buffers[2 + b].Memory.Pin();
                    starts[b] = (nint)pins[b].Pointer;
                    lengths[b] = buffers[2 + b].Length;
                }
                ref byte view = ref MemoryMarshal.GetReference(views);
                for (int j = 0; j < n; j++)
                {
                    if (!both.IsEmpty && !IsSet(both, j)) continue; // a null's view may point anywhere
                    ref byte x = ref Unsafe.Add(ref view, j * 16), y = ref Unsafe.Add(ref view, j * 16 + 16);
                    int xLength = Unsafe.ReadUnaligned<int>(ref x), yLength = Unsafe.ReadUnaligned<int>(ref y), sign;
                    if ((uint)xLength <= 12 && (uint)yLength <= 12)
                    {
                        // Inline: bytes 4..12 and 12..16 as big-endian numbers, each cut to the value's length (zeros
                        // past it, which leaves a value before the longer ones it begins), then the lengths.
                        ulong a = BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref x, 4))) & High(xLength);
                        ulong b = BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref y, 4))) & High(yLength);
                        uint c = BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref x, 12))) & Low(xLength);
                        uint d = BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref y, 12))) & Low(yLength);
                        sign = a != b ? (a < b ? -1 : 1) : c != d ? (c < d ? -1 : 1) : xLength - yLength;
                    }
                    else
                    {
                        // The prefixes, cut to the bytes both hold: where they differ, they decide.
                        int shared = Math.Min(4, Math.Min(xLength, yLength));
                        // (shared 4 apart: a shift by 32 is a shift by 0; a mask of 0 would leave every pair to Far, which is slower)
                        uint mask = shared >= 4 ? uint.MaxValue : shared <= 0 ? 0 : ~(uint.MaxValue >> (8 * shared));
                        uint xPrefix = BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref x, 4))) & mask;
                        uint yPrefix = BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref y, 4))) & mask;
                        sign = xPrefix != yPrefix ? (xPrefix < yPrefix ? -1 : 1) : Far(ref x, xLength, ref y, yLength, starts, lengths, dataBuffers);
                    }
                    if (descending) sign = -sign;
                    if (sign > 0) Set(bad, j);
                    else if (sign == 0 && ties) Set(tie, j);
                }
            }
            finally
            {
                foreach (ref System.Buffers.MemoryHandle pin in pins.AsSpan()) pin.Dispose();
            }
        }

        /// <summary>The inline bytes 4..12 of a value of <paramref name="length"/> (at most 12) as a big-endian mask.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong High(int length) => length >= 8 ? ulong.MaxValue : ~(ulong.MaxValue >> (8 * length));

        /// <summary>The inline bytes 12..16 of a value of <paramref name="length"/> (at most 12) as a big-endian mask.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static uint Low(int length) => length <= 8 ? 0 : length >= 12 ? uint.MaxValue : ~(uint.MaxValue >> (8 * (length - 8)));

        /// <summary>Two views with the same prefix, one of them not inline: their bytes (each checked to lie in its buffer).</summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static unsafe int Far(ref byte x, int xLength, ref byte y, int yLength, nint* starts, int* lengths, int buffers)
        {
            ref byte xBytes = ref Bytes(ref x, xLength, starts, lengths, buffers, out int xRoom);
            ref byte yBytes = ref Bytes(ref y, yLength, starts, lengths, buffers, out int yRoom);
            int shared = Math.Min(xLength, yLength);
            if (shared <= 16 && xRoom >= 16 && yRoom >= 16 && Vector128.IsHardwareAccelerated)
            {
                uint differ = ~Vector128.ExtractMostSignificantBits(Vector128.Equals(Vector128.LoadUnsafe(ref xBytes), Vector128.LoadUnsafe(ref yBytes)));
                int at = BitOperations.TrailingZeroCount(differ | 1u << 16);
                return at < shared ? Unsafe.Add(ref xBytes, at) - Unsafe.Add(ref yBytes, at) : xLength - yLength;
            }
            return CompareWithin(ref xBytes, xLength, xRoom, ref yBytes, yLength, yRoom);
        }

        /// <summary>A view's bytes (inline, or in its data buffer: checked to lie there) and how many may be read from them.</summary>
        private static unsafe ref byte Bytes(ref byte view, int length, nint* starts, int* lengths, int buffers, out int room)
        {
            if (length < 0) throw new InvalidDataException($"A string view has a negative length {length}.");
            if (length <= 12)
            {
                room = 12;
                return ref Unsafe.Add(ref view, 4);
            }
            int buffer = Unsafe.ReadUnaligned<int>(ref Unsafe.Add(ref view, 8)), offset = Unsafe.ReadUnaligned<int>(ref Unsafe.Add(ref view, 12));
            if ((uint)buffer >= (uint)buffers || offset < 0 || (long)offset + length > lengths[buffer])
                throw new InvalidDataException($"A string view points past its data (buffer {buffer}, offset {offset}, length {length}).");
            room = lengths[buffer] - offset;
            return ref Unsafe.AsRef<byte>((byte*)starts[buffer] + offset);
        }
    }

    /// <summary>
    /// Two byte strings in unsigned order, 16 bytes at a time while both have 16 readable bytes (<paramref name="xRoom"/>,
    /// <paramref name="yRoom"/>: the bytes that may be read from each), the rest one by one.
    /// </summary>
    private static int CompareWithin(ref byte x, int xLength, int xRoom, ref byte y, int yLength, int yRoom)
    {
        int shared = Math.Min(xLength, yLength), at = 0;
        if (Vector128.IsHardwareAccelerated)
        {
            for (; at < shared && xRoom - at >= 16 && yRoom - at >= 16; at += 16)
            {
                uint differ = ~Vector128.ExtractMostSignificantBits(Vector128.Equals(Vector128.LoadUnsafe(ref x, (nuint)at), Vector128.LoadUnsafe(ref y, (nuint)at))) & 0xFFFF;
                if (differ == 0) continue;
                int k = at + BitOperations.TrailingZeroCount(differ);
                return k < shared ? Unsafe.Add(ref x, k) - Unsafe.Add(ref y, k) : xLength - yLength;
            }
        }
        if (at >= shared) return xLength - yLength;
        return MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref x, at), xLength - at).SequenceCompareTo(MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref y, at), yLength - at));
    }

    private sealed class FixedBytes(FixedSizeBinaryArray array, int width) : Bytes(array)
    {
        protected override ReadOnlySpan<byte> Get(int i) => Array.Data.Buffers[1].Span.Slice((Array.Data.Offset + i) * width, width);

        public override void ValuePairs(int first, int n, bool descending, Span<ulong> bad, Span<ulong> tie, bool ties, ReadOnlySpan<ulong> both)
        {
            ReadOnlySpan<byte> data = Array.Data.Buffers[1].Span[(Array.Data.Offset * width)..];
            for (int j = 0; j < n; j++)
            {
                int i = first + j;
                int sign = data.Slice((i - 1) * width, width).SequenceCompareTo(data.Slice(i * width, width));
                if (descending) sign = -sign;
                if (sign > 0) Set(bad, j);
                else if (sign == 0 && ties) Set(tie, j);
            }
        }
    }

    /// <summary>
    /// Dictionary-encoded values, compared by the values their indices name: by the indices themselves (a vector at a
    /// time) when the dictionary's values are distinct, without nulls, and in ascending order.
    /// </summary>
    private sealed class Dictionary : Column
    {
        private readonly Column _values;
        private readonly Column _indices;
        private readonly bool _nullValues;
        private int _sorted; // 0 not known yet, 1 the values distinct and ascending, -1 not

        public Dictionary(DictionaryArray array) : base(array)
        {
            _values = Leaf(array.Dictionary);
            Indices = array.Indices;
            _indices = Leaf(Indices);
            _nullValues = array.Dictionary.NullCount > 0;
        }

        private IArrowArray Indices { get; }

        protected override bool LeafNulls => Indices.NullCount > 0 || _nullValues;

        private int Index(int i) => Indices switch
        {
            Int8Array a => a.Values[i],
            Int16Array a => a.Values[i],
            Int32Array a => a.Values[i],
            Int64Array a => checked((int)a.Values[i]),
            UInt8Array a => a.Values[i],
            UInt16Array a => a.Values[i],
            UInt32Array a => checked((int)a.Values[i]),
            UInt64Array a => checked((int)a.Values[i]),
            _ => throw new NotSupportedException($"Dictionary indices of type {Indices.Data.DataType.Name}."),
        };

        protected override bool IsNullAt(int i) => Indices.IsNull(i) || _values.IsNull(Index(i));

        protected override void LeafValid(int first, Span<ulong> words)
        {
            if (!_nullValues)
            {
                // The indices' validity alone.
                ArrayData data = Indices.Data;
                if (data.NullCount == 0 || data.Buffers[0].IsEmpty)
                {
                    words.Fill(ulong.MaxValue);
                    return;
                }
                ReadOnlySpan<byte> validity = data.Buffers[0].Span;
                for (int w = 0; w < words.Length; w++) words[w] = Bits.Word(validity, data.Offset + first + 64L * w);
                return;
            }
            words.Clear();
            int count = Math.Min(64 * words.Length, Indices.Length - first);
            for (int j = 0; j < count; j++)
            {
                if (first + j >= 0 && !IsNullAt(first + j)) Set(words, j);
            }
        }

        public override int CompareAt(int i, Column other, int j) => _values.CompareValues(Index(i), ((Dictionary)other)._values, ((Dictionary)other).Index(j));

        protected override string FormatAt(int i) => _values.Format(Index(i));

        public override int Entry(int i) => Index(i);

        public override void ValuePairs(int first, int n, bool descending, Span<ulong> bad, Span<ulong> tie, bool ties, ReadOnlySpan<ulong> both)
        {
            if (Sorted)
            {
                // Distinct ascending values: the indices order the rows as the values do (null indices' bits cleared after).
                _indices.ValuePairs(first, n, descending, bad, tie, ties, both);
                return;
            }
            // Neighbours naming one entry tie at once (runs, in sorted data); others are compared by value.
            for (int j = 0; j < n; j++)
            {
                if (!both.IsEmpty && !IsSet(both, j)) continue;
                int a = Index(first + j - 1), b = Index(first + j);
                int sign = a == b ? 0 : _values.CompareValues(a, _values, b);
                if (descending) sign = -sign;
                if (sign > 0) Set(bad, j);
                else if (sign == 0 && ties) Set(tie, j);
            }
        }

        /// <summary>
        /// Whether the values are distinct, without nulls, and ascending; looked at only for dictionaries no longer than
        /// the indices (a few rows checked against a large dictionary are compared by value instead).
        /// </summary>
        private bool Sorted
        {
            get
            {
                if (_sorted == 0)
                {
                    IArrowArray values = ((DictionaryArray)Array).Dictionary;
                    bool sorted = !_nullValues && values.Length <= Math.Min(Indices.Length, RowsChecked);
                    for (int v = 1; v < values.Length && sorted; v++) sorted = _values.CompareValues(v - 1, _values, v) < 0;
                    _sorted = sorted ? 1 : -1;
                }
                return _sorted > 0;
            }
        }
    }
}

/// <summary>
/// Checks that the rows of batch after batch follow a sort order, across the batches: the last row checked is kept
/// (a copy in native memory) to compare the next batch's first row with.
/// </summary>
/// <param name="order">The sort order, a key at a time.</param>
public sealed class OrderCheck(IReadOnlyList<SortKey> order) : IDisposable
{
    private readonly SortKey[] _order = [.. order];
    private IArrowArray[]? _last; // the last row checked, a row of each key's column
    private OrderColumn[] _lastKeys = [];
    private long _rows;

    /// <summary>The rows checked since the start (or the last <see cref="Restart"/>).</summary>
    public long Rows => _rows;

    /// <summary>
    /// Checks rows <paramref name="start"/> .. +<paramref name="count"/> of a batch's key columns (in the order's
    /// keys' order) after the rows checked before; false with where they stop following it (<see cref="OrderBreak.Row"/>
    /// counts from the first row checked). Rows after a break are not checked, and the check does not go on.
    /// </summary>
    public bool Check(ReadOnlySpan<OrderColumn> keys, int start, int count, out OrderBreak broken) => Check(keys, start, count, null, out broken);

    /// <summary>
    /// <see cref="Check(ReadOnlySpan{OrderColumn}, int, int, out OrderBreak)"/>, many rows checked side by side by the
    /// helpers <paramref name="budget"/> has threads for.
    /// </summary>
    internal bool Check(ReadOnlySpan<OrderColumn> keys, int start, int count, WorkBudget? budget, out OrderBreak broken)
    {
        broken = default;
        if (count == 0) return true;
        if (_last is not null && ArrowOrder.Compare(_lastKeys, 0, keys, start, _order, out int k) > 0)
        {
            broken = new OrderBreak(_rows, k, ArrowOrder.Format(_lastKeys[k], 0), ArrowOrder.Format(keys[k], start));
            return false;
        }
        int row = ArrowOrder.FirstOutOfOrder(keys, _order, start, count, budget, out int key);
        if (row >= 0)
        {
            broken = new OrderBreak(_rows + (row - start), key, ArrowOrder.Format(keys[key], row - 1), ArrowOrder.Format(keys[key], row));
            return false;
        }
        Keep(keys, start + count - 1);
        _rows += count;
        return true;
    }

    /// <summary>Starts over: the next rows are not compared with those checked before.</summary>
    public void Restart()
    {
        Free();
        _rows = 0;
    }

    private void Keep(ReadOnlySpan<OrderColumn> keys, int row)
    {
        var last = new IArrowArray[keys.Length];
        var lastKeys = new OrderColumn[keys.Length];
        try
        {
            for (int k = 0; k < keys.Length; k++)
            {
                // The key's field alone (the struct around it may hold columns a copy cannot be made of).
                last[k] = ArrowOrder.TakeRow(keys[k], row);
                lastKeys[k] = new OrderColumn(last[k]);
            }
        }
        catch
        {
            foreach (IArrowArray? array in last) array?.Dispose();
            throw;
        }
        Free();
        _last = last;
        _lastKeys = lastKeys;
    }

    private void Free()
    {
        if (_last is null) return;
        foreach (IArrowArray array in _last) array.Dispose();
        _last = null;
        _lastKeys = [];
    }

    /// <inheritdoc/>
    public void Dispose() => Free();
}
