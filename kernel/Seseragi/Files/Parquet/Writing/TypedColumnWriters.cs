using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Array = System.Array;
using Kaminari.Sazanami.Encodings;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Marshalling;
using Kaminari.Sazanami.Kernels;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>Fills <paramref name="destination"/> with the values of rows <paramref name="offset"/>.. in the physical type (anything for null rows).</summary>
internal delegate void FixedSource<T>(IArrowArray array, int offset, int count, Span<T> destination);

/// <summary>The bytes of the (non-null) value at <paramref name="index"/>, in <paramref name="scratch"/> when they must be made.</summary>
internal delegate ReadOnlySpan<byte> BytesSource(IArrowArray array, int index, Span<byte> scratch);

/// <summary>How the statistics of a fixed-width physical type order its values (Parquet's type-defined order).</summary>
internal interface IValueOrder<T> where T : unmanaged
{
    /// <summary>Whether the type has bounds at all (not INT96, nor the null type).</summary>
    bool HasBounds { get; }

    /// <summary>Whether the deprecated min/max fields are written too (signed orders, as Spark and Arrow write).</summary>
    bool Legacy { get; }

    /// <summary>Whether NaNs are counted and left out of the bounds.</summary>
    bool Floating { get; }

    bool IsNaN(T value);

    int Compare(T a, T b);

    /// <summary>The PLAIN bytes of a bound (a zero lower bound as -0, a zero upper bound as +0).</summary>
    byte[] Bound(T value, bool upper);
}

internal readonly struct SignedOrder<T>(bool legacy) : IValueOrder<T> where T : unmanaged, IBinaryInteger<T>
{
    public bool HasBounds => true;
    public bool Legacy => legacy;
    public bool Floating => false;
    public bool IsNaN(T value) => false;
    public int Compare(T a, T b) => a.CompareTo(b);
    public byte[] Bound(T value, bool upper) => MemoryMarshal.AsBytes(new ReadOnlySpan<T>(ref value)).ToArray();
}

internal readonly struct UnsignedOrder32 : IValueOrder<int>
{
    public bool HasBounds => true;
    public bool Legacy => false;
    public bool Floating => false;
    public bool IsNaN(int value) => false;
    public int Compare(int a, int b) => ((uint)a).CompareTo((uint)b);
    public byte[] Bound(int value, bool upper) => BitConverter.GetBytes(value);
}

internal readonly struct UnsignedOrder64 : IValueOrder<long>
{
    public bool HasBounds => true;
    public bool Legacy => false;
    public bool Floating => false;
    public bool IsNaN(long value) => false;
    public int Compare(long a, long b) => ((ulong)a).CompareTo((ulong)b);
    public byte[] Bound(long value, bool upper) => BitConverter.GetBytes(value);
}

internal readonly struct FloatOrder : IValueOrder<float>
{
    public bool HasBounds => true;
    public bool Legacy => true;
    public bool Floating => true;
    public bool IsNaN(float value) => float.IsNaN(value);
    public int Compare(float a, float b) => a.CompareTo(b);
    public byte[] Bound(float value, bool upper) => BitConverter.GetBytes(value == 0 ? (upper ? 0f : -0f) : value);
}

internal readonly struct DoubleOrder : IValueOrder<double>
{
    public bool HasBounds => true;
    public bool Legacy => true;
    public bool Floating => true;
    public bool IsNaN(double value) => double.IsNaN(value);
    public int Compare(double a, double b) => a.CompareTo(b);
    public byte[] Bound(double value, bool upper) => BitConverter.GetBytes(value == 0 ? (upper ? 0d : -0d) : value);
}

/// <summary>No bounds: INT96 (whose type-defined order readers ignore) and the null type.</summary>
internal readonly struct NoOrder<T> : IValueOrder<T> where T : unmanaged
{
    public bool HasBounds => false;
    public bool Legacy => false;
    public bool Floating => false;
    public bool IsNaN(T value) => false;
    public int Compare(T a, T b) => 0;
    public byte[] Bound(T value, bool upper) => [];
}

/// <summary>The bounds of a page or chunk of fixed-width values (NaNs counted apart).</summary>
internal struct FixedBounds<T> where T : unmanaged
{
    public bool Has;
    public T Min, Max;
    public long Nans;
}

/// <summary>A column of a fixed-width physical type (INT32, INT64, INT96, FLOAT, DOUBLE).</summary>
internal sealed class FixedColumnWriter<T, TOrder>(PageSettings settings, int maxDef, int maxRep, FixedSource<T> source, TOrder order, bool direct = false)
    : ColumnWriter(settings, maxDef, maxRep, dictionary: true)
    where T : unmanaged
    where TOrder : struct, IValueOrder<T>
{
    /// <summary>A page's values in native memory, its bounds (gathered as the values came) and its own dictionary.</summary>
    private sealed class Page : PageValues
    {
        public readonly NativeArray<T> Values = new(InitialBytes / Unsafe.SizeOf<T>());
        public int Length;
        public FixedBounds<T> Bounds;
        private FixedValueDictionary<T>? _local; // made when the page is first mapped

        public override int Count => Length;

        public override long PlainBytes => (long)Length * Unsafe.SizeOf<T>();

        protected override int LocalCount => _local?.Count ?? 0;

        protected override long LocalPlainBytes => (long)LocalCount * Unsafe.SizeOf<T>();

        /// <summary>The page dictionary's entries, in order of first appearance.</summary>
        public ReadOnlySpan<T> Entries => _local is null ? default : _local.Values;

        protected override void MapRange(Span<uint> indices, int first)
        {
            FixedValueDictionary<T> local = _local ??= new();
            ReadOnlySpan<T> values = Values.AsSpan(first, indices.Length);
            for (int i = 0; i < indices.Length; i++) indices[i] = (uint)local.GetOrAdd(values[i]);
        }

        protected override void ClearLocal() => _local?.Clear();

        public override void EncodePlain(IBufferWriter<byte> output) => output.Write(MemoryMarshal.AsBytes(Values.AsSpan(0, Length)));

        public override void EncodeAs(ParquetEncoding encoding, IBufferWriter<byte> output)
        {
            Span<T> values = Values.AsSpan(0, Length);
            switch (encoding)
            {
                case ParquetEncoding.DeltaBinaryPacked when typeof(T) == typeof(int):
                    DeltaBinaryPackedEncoder.Encode<int>(MemoryMarshal.Cast<T, int>(values), output);
                    break;
                case ParquetEncoding.DeltaBinaryPacked when typeof(T) == typeof(long):
                    DeltaBinaryPackedEncoder.Encode<long>(MemoryMarshal.Cast<T, long>(values), output);
                    break;
                case ParquetEncoding.ByteStreamSplit:
                {
                    ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(values);
                    ByteStreamSplit.Encode(bytes, Unsafe.SizeOf<T>(), output.GetSpan(bytes.Length));
                    output.Advance(bytes.Length);
                    break;
                }
                default:
                    throw new NotSupportedException($"{typeof(T).Name} values are not written as {encoding}.");
            }
        }

        public override void Clear()
        {
            base.Clear();
            Length = 0;
            Bounds = default;
        }

        public override long ScratchBytes => base.ScratchBytes + (long)Values.Capacity * Unsafe.SizeOf<T>() + (_local?.ScratchBytes ?? 0);

        /// <summary>Returns its memory to the pool (grown again as values come).</summary>
        public void Trim()
        {
            Values.Dispose();
            _local?.Dispose();
            _local = null;
        }

        public override void Dispose()
        {
            base.Dispose();
            Values.Dispose();
            _local?.Dispose();
        }
    }

    // The open page (its values, and the converted rows they are gathered from, in native memory).
    private Page _open = new();
    private readonly NativeArray<T> _scratch = new();
    private readonly FixedValueDictionary<T> _dictionary = new();
    private FixedBounds<T> _pageBounds, _chunkBounds;

    protected override void Append(IArrowArray array, int offset, int count, int nulls)
    {
        Page open = _open;
        open.Values.Grow(open.Length + count, open.Length);
        Span<T> page = open.Values.Span;
        int start = open.Length, at = start;
        if (nulls == 0)
        {
            if (direct) ArrowValues.Values<T>(array).Slice(offset, count).CopyTo(page[at..]);
            else source(array, offset, count, page.Slice(at, count));
            at += count;
        }
        else
        {
            // Direct values are gathered from the array itself; others converted first.
            ReadOnlySpan<T> all;
            if (direct) all = ArrowValues.Values<T>(array).Slice(offset, count);
            else
            {
                _scratch.Reserve(count);
                source(array, offset, count, _scratch.AsSpan(0, count));
                all = _scratch.AsSpan(0, count);
            }
            ReadOnlySpan<ulong> validity = Validity;
            if (validity.IsEmpty)
            {
                for (int i = 0; i < count; i++)
                {
                    if (!ArrowValues.IsNull(array, offset + i)) page[at++] = all[i];
                }
            }
            else
            {
                // The set bits of each word: whole words at once, else bit by bit.
                for (int w = 0; w < validity.Length; w++)
                {
                    ulong bits = validity[w];
                    int first = w << 6;
                    if (bits == ulong.MaxValue)
                    {
                        all.Slice(first, 64).CopyTo(page[at..]);
                        at += 64;
                        continue;
                    }
                    while (bits != 0)
                    {
                        page[at++] = all[first + BitOperations.TrailingZeroCount(bits)];
                        bits &= bits - 1;
                    }
                }
            }
        }
        open.Length = at;
        if (Settings.Statistics && order.HasBounds) Bounds(page[start..at]);
        if (Bloom is { } bloom) Hash(bloom, page[start..at]);
    }

    protected override void AppendAt(IArrowArray array, ReadOnlySpan<int> indices)
    {
        Page open = _open;
        open.Values.Grow(open.Length + indices.Length, open.Length);
        Span<T> page = open.Values.Span;
        int start = open.Length, at = start;
        // Runs of consecutive positions (the items of lists lie together) are read at once.
        for (int k = 0; k < indices.Length;)
        {
            int run = 1;
            while (k + run < indices.Length && indices[k + run] == indices[k] + run) run++;
            source(array, indices[k], run, page.Slice(at, run));
            at += run;
            k += run;
        }
        open.Length = at;
        if (Settings.Statistics && order.HasBounds) Bounds(page[start..at]);
        if (Bloom is { } bloom) Hash(bloom, page[start..at]);
    }

    private static void Hash(BloomBuilder bloom, ReadOnlySpan<T> values)
    {
        for (int i = 0; i < values.Length; i++) bloom.Add(MemoryMarshal.AsBytes(values.Slice(i, 1)));
    }

    private void Bounds(ReadOnlySpan<T> values)
    {
        if (values.Length >= 32 && VectorBounds(values)) return;
        ref FixedBounds<T> b = ref _pageBounds;
        foreach (T value in values)
        {
            if (order.Floating && order.IsNaN(value))
            {
                b.Nans++;
                continue;
            }
            if (!b.Has)
            {
                b.Min = b.Max = value;
                b.Has = true;
            }
            else if (order.Compare(value, b.Min) < 0) b.Min = value;
            else if (order.Compare(value, b.Max) > 0) b.Max = value;
        }
    }

    /// <summary>
    /// The bounds of a run of values by Sazanami's vector kernel, for the orders it knows (signed and unsigned 32 and
    /// 64-bit integers, floats and doubles), folded into the page's; false for other orders.
    /// </summary>
    private bool VectorBounds(ReadOnlySpan<T> values)
    {
        T lo, hi;
        bool found = true;
        long nans = 0;
        if (typeof(TOrder) == typeof(SignedOrder<int>) || typeof(TOrder) == typeof(SignedOrder<long>))
        {
            if (typeof(T) == typeof(int)) Pair(MemoryMarshal.Cast<T, int>(values), out lo, out hi);
            else Pair(MemoryMarshal.Cast<T, long>(values), out lo, out hi);
        }
        else if (typeof(TOrder) == typeof(UnsignedOrder32)) Pair(MemoryMarshal.Cast<T, uint>(values), out lo, out hi);
        else if (typeof(TOrder) == typeof(UnsignedOrder64)) Pair(MemoryMarshal.Cast<T, ulong>(values), out lo, out hi);
        else if (typeof(TOrder) == typeof(DoubleOrder))
        {
            found = MinMax.Floating(MemoryMarshal.Cast<T, double>(values), out double a, out double z, out nans);
            (lo, hi) = (Unsafe.As<double, T>(ref a), Unsafe.As<double, T>(ref z));
        }
        else if (typeof(TOrder) == typeof(FloatOrder))
        {
            found = MinMax.Floating(MemoryMarshal.Cast<T, float>(values), out float a, out float z, out nans);
            (lo, hi) = (Unsafe.As<float, T>(ref a), Unsafe.As<float, T>(ref z));
        }
        else return false;
        ref FixedBounds<T> b = ref _pageBounds;
        b.Nans += nans;
        if (!found) return true;
        if (!b.Has) (b.Min, b.Max, b.Has) = (lo, hi, true);
        else
        {
            if (order.Compare(lo, b.Min) < 0) b.Min = lo;
            if (order.Compare(hi, b.Max) > 0) b.Max = hi;
        }
        return true;

        static void Pair<TValue>(ReadOnlySpan<TValue> run, out T lo, out T hi) where TValue : unmanaged, IBinaryInteger<TValue>
        {
            MinMax.Integers(run, out TValue a, out TValue z);
            (lo, hi) = (Unsafe.As<TValue, T>(ref a), Unsafe.As<TValue, T>(ref z));
        }
    }

    protected override PageValues TakePage()
    {
        Page page = (Page)OpenPage();
        _open = new Page();
        return page;
    }

    protected override PageValues OpenPage()
    {
        _open.Bounds = _pageBounds;
        _pageBounds = default;
        return _open;
    }

    protected override void MergeLocal(PageValues page)
    {
        var values = (Page)page;
        Span<uint> remap = values.Remap();
        ReadOnlySpan<T> entries = values.Entries;
        for (int k = 0; k < remap.Length; k++) remap[k] = (uint)_dictionary.GetOrAdd(entries[k]);
    }

    protected override bool ClosePageBounds(PageValues pageValues, int values, out byte[]? min, out byte[]? max)
    {
        FixedBounds<T> page = ((Page)pageValues).Bounds;
        _chunkBounds.Nans += page.Nans;
        if (page.Has)
        {
            if (!_chunkBounds.Has) (_chunkBounds.Min, _chunkBounds.Max, _chunkBounds.Has) = (page.Min, page.Max, true);
            else
            {
                if (order.Compare(page.Min, _chunkBounds.Min) < 0) _chunkBounds.Min = page.Min;
                if (order.Compare(page.Max, _chunkBounds.Max) > 0) _chunkBounds.Max = page.Max;
            }
        }
        min = max = null;
        if (values == 0) return true;
        if (!order.HasBounds || page.Nans > 0 || !page.Has) return false;
        min = order.Bound(page.Min, upper: false);
        max = order.Bound(page.Max, upper: true);
        return true;
    }

    protected override int CompareBounds(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => order.Compare(MemoryMarshal.Read<T>(a), MemoryMarshal.Read<T>(b));

    protected override int PageValueCount => _open.Length;

    protected override long PagePlainBytes => _open.PlainBytes;

    protected override int DictionaryCount => _dictionary.Count;

    protected override long DictionaryPlainBytes => (long)_dictionary.Count * Unsafe.SizeOf<T>();

    protected override void EncodeDictionary(IBufferWriter<byte> output) => output.Write(MemoryMarshal.AsBytes(_dictionary.Values));

    protected override void ClearDictionary() => _dictionary.Clear();

    protected override Statistics? ChunkStatistics(long nullCount)
    {
        FixedBounds<T> b = _chunkBounds;
        if (!order.HasBounds) return new Statistics { NullCount = nullCount };
        // A chunk holding NaNs gets no bounds: the specification keeps NaN out of them, but Spark (parquet-mr, which
        // ignores nan_count) orders NaN above every number and would skip the NaN rows of `x > bound`.
        if (b.Nans > 0) return new Statistics { NullCount = nullCount, NanCount = b.Nans };
        byte[]? min = b.Has ? order.Bound(b.Min, upper: false) : null;
        byte[]? max = b.Has ? order.Bound(b.Max, upper: true) : null;
        return new Statistics
        {
            NullCount = nullCount,
            MinValue = min,
            MaxValue = max,
            IsMinValueExact = b.Has ? true : null,
            IsMaxValueExact = b.Has ? true : null,
            Min = order.Legacy ? min : null,
            Max = order.Legacy ? max : null,
            NanCount = order.Floating ? b.Nans : null,
        };
    }

    protected override void ResetStatistics()
    {
        _pageBounds = default;
        _chunkBounds = default;
    }

    protected override long ValueScratchBytes =>
        _open.ScratchBytes + (long)_scratch.Capacity * Unsafe.SizeOf<T>() + _dictionary.ScratchBytes;

    protected override void TrimValues()
    {
        _open.Trim();
        _scratch.Dispose();
        _dictionary.Trim();
    }

    public override void Dispose()
    {
        base.Dispose();
        _open.Dispose();
        _scratch.Dispose();
        _dictionary.Dispose();
    }
}

/// <summary>How byte-array statistics order their values.</summary>
internal enum BytesOrder
{
    /// <summary>No bounds (INTERVAL, whose order is undefined).</summary>
    None,
    /// <summary>Unsigned bytewise (strings, binary, fixed binary).</summary>
    Unsigned,
    /// <summary>Big-endian two's complement of one length (decimals in fixed-length byte arrays).</summary>
    SignedDecimal,
    /// <summary>Little-endian half floats (FLOAT16): NaNs counted and left out, zeros as for floats.</summary>
    Float16,
}

/// <summary>The bounds of a page or chunk of byte-array values (NaN half floats counted apart).</summary>
internal sealed class ByteBounds
{
    public byte[] MinBuffer = new byte[64], MaxBuffer = new byte[64];
    public int MinLength = -1, MaxLength = -1; // -1: no bound yet
    public long Nans;

    public bool Has => MinLength >= 0;
    public ReadOnlySpan<byte> Min => MinBuffer.AsSpan(0, MinLength);
    public ReadOnlySpan<byte> Max => MaxBuffer.AsSpan(0, MaxLength);

    public void Clear()
    {
        MinLength = MaxLength = -1;
        Nans = 0;
    }

    public static void Keep(ReadOnlySpan<byte> value, ref byte[] buffer, ref int length)
    {
        if (buffer.Length < value.Length) buffer = new byte[Math.Max(value.Length, buffer.Length * 2)];
        value.CopyTo(buffer);
        length = value.Length;
    }
}

/// <summary>
/// A column of BYTE_ARRAY (<paramref name="fixedLength"/> 0) or FIXED_LEN_BYTE_ARRAY values. With
/// <paramref name="direct"/> the values are the bytes of a binary or string array (plain, large or view) as they are:
/// runs of them are copied at once (offsets shifted; views gathered into the arena in one pass) instead of value by
/// value through <paramref name="source"/>.
/// </summary>
internal sealed class BytesColumnWriter(PageSettings settings, int maxDef, int maxRep, BytesSource source, int fixedLength, BytesOrder order, bool utf8, string column,
    bool direct = false)
    : ColumnWriter(settings, maxDef, maxRep, dictionary: true)
{
    /// <summary>
    /// A page's values back to back, value i at Offsets[i] .. [i + 1] (Offsets[0] stays 0), in native memory; its bounds
    /// (gathered when it is prepared) and its own dictionary.
    /// </summary>
    private sealed class Page(PageShape shape) : PageValues
    {
        public PageShape Shape => shape;

        public readonly NativeArray<byte> Arena = new(1 << 16);
        public int ArenaLength;
        public readonly NativeArray<int> Offsets = NewOffsets();
        private readonly NativeArray<int> _lengths = new(); // the lengths the delta encodings gather
        public int Length;
        public readonly ByteBounds Bounds = new();
        private ByteArrayDictionary? _local; // made when the page is first mapped

        public ReadOnlySpan<byte> Value(int i)
        {
            int start = Offsets.At(i);
            return Arena.AsSpan(start, Offsets.At(i + 1) - start);
        }

        public override int Count => Length;

        public override long PlainBytes => ArenaLength + (shape.FixedLength > 0 ? 0 : 4L * Length);

        protected override int LocalCount => _local?.Count ?? 0;

        protected override long LocalPlainBytes => _local is null ? 0 : _local.ByteSize + (shape.FixedLength > 0 ? 0 : 4L * _local.Count);

        /// <summary>Entry <paramref name="k"/> of the page's dictionary (entries in order of first appearance).</summary>
        public ReadOnlySpan<byte> Entry(int k) => _local![k];

        protected override void MapRange(Span<uint> indices, int first)
        {
            ByteArrayDictionary local = _local ??= new();
            for (int i = 0; i < indices.Length; i++) indices[i] = (uint)local.GetOrAdd(Value(first + i));
        }

        protected override void ClearLocal() => _local?.Clear();

        /// <summary>
        /// The page's bounds: from the distinct entries of its dictionary when every value is mapped (half floats aside,
        /// whose NaNs are counted value by value), else value by value.
        /// </summary>
        public override bool Prepares => shape.Statistics && shape.Order != BytesOrder.None;

        public override void Prepare()
        {
            if (!Prepares) return;
            if (Mapped && shape.Order != BytesOrder.Float16)
            {
                for (int k = 0; k < LocalCount; k++) Bound(shape.Order, Bounds, _local![k]);
                return;
            }
            for (int i = 0; i < Length; i++) Bound(shape.Order, Bounds, Value(i));
        }

        public override void EncodePlain(IBufferWriter<byte> output)
        {
            if (shape.FixedLength > 0)
            {
                output.Write(Arena.AsSpan(0, ArenaLength));
                return;
            }
            // Each value after its length, into one span taken at once.
            ReadOnlySpan<byte> arena = Arena.AsSpan(0, ArenaLength);
            ReadOnlySpan<int> offsets = Offsets.AsSpan(0, Length + 1);
            Span<byte> into = output.GetSpan(checked((int)PlainBytes));
            int at = 0;
            for (int i = 0; i < Length; i++)
            {
                int start = offsets[i], length = offsets[i + 1] - start;
                BinaryPrimitives.WriteInt32LittleEndian(into[at..], length);
                ShortCopy.Copy(arena.Slice(start, length), into[(at + 4)..]);
                at += 4 + length;
            }
            output.Advance(at);
        }

        public override void EncodeAs(ParquetEncoding encoding, IBufferWriter<byte> output)
        {
            _lengths.Reserve(Math.Max(1, 2 * Length));
            ReadOnlySpan<byte> bytes = Arena.AsSpan(0, ArenaLength);
            ReadOnlySpan<int> offsets = Offsets.AsSpan(0, Length + 1);
            switch (encoding)
            {
                case ParquetEncoding.DeltaLengthByteArray when shape.FixedLength == 0:
                    DeltaByteArrayEncoder.EncodeLengths(bytes, offsets, _lengths.Span, output);
                    break;
                case ParquetEncoding.DeltaByteArray:
                    DeltaByteArrayEncoder.EncodeIncremental(bytes, offsets, _lengths.Span, output);
                    break;
                case ParquetEncoding.ByteStreamSplit when shape.FixedLength > 0:
                    ByteStreamSplit.Encode(bytes, shape.FixedLength, output.GetSpan(bytes.Length));
                    output.Advance(bytes.Length);
                    break;
                default:
                    throw new NotSupportedException($"Column '{shape.Column}' is not written as {encoding}.");
            }
        }

        public override void Clear()
        {
            base.Clear();
            Length = 0;
            ArenaLength = 0;
            Bounds.Clear();
        }

        public override long ScratchBytes => base.ScratchBytes + Arena.Capacity + 4L * (Offsets.Capacity + _lengths.Capacity) + (_local?.ScratchBytes ?? 0);

        /// <summary>Returns its memory to the pool (grown again as values come).</summary>
        public void Trim()
        {
            Arena.Dispose();
            Offsets.Dispose();
            Offsets.Reserve(1);
            Offsets[0] = 0; // the first offset of every page
            _lengths.Dispose();
            _local?.Dispose();
            _local = null;
        }

        public override void Dispose()
        {
            base.Dispose();
            Arena.Dispose();
            Offsets.Dispose();
            _lengths.Dispose();
            _local?.Dispose();
        }

        /// <summary>The offsets of an empty page: the first, 0, is never written again.</summary>
        private static NativeArray<int> NewOffsets()
        {
            var offsets = new NativeArray<int>(4097);
            offsets[0] = 0;
            return offsets;
        }
    }

    /// <summary>What a page needs of its column to encode and bound its values (pages are prepared on any thread).</summary>
    private sealed record PageShape(int FixedLength, BytesOrder Order, bool Statistics, string Column);

    private Page _open = new(new PageShape(fixedLength, order, settings.Statistics, column));
    private readonly ByteArrayDictionary _dictionary = new();
    private readonly ByteBounds _chunkBounds = new();

    protected override void Append(IArrowArray array, int offset, int count, int nulls)
    {
        if (direct)
        {
            // Runs of valid rows, each copied at once.
            int end = offset + count;
            for (int i = offset; i < end;)
            {
                if (nulls > 0 && IsNullAt(array, offset, i - offset))
                {
                    i++;
                    continue;
                }
                int run = 1;
                while (i + run < end && (nulls == 0 || !IsNullAt(array, offset, i + run - offset))) run++;
                AppendRun(array, i, run);
                i += run;
            }
            return;
        }
        Span<byte> scratch = stackalloc byte[ValueBytes];
        for (int i = offset; i < offset + count; i++)
        {
            if (nulls > 0 && IsNullAt(array, offset, i - offset)) continue;
            ReadOnlySpan<byte> value = source(array, i, scratch);
            if (fixedLength > 0 && value.Length != fixedLength)
                throw new ArgumentException($"A value of column '{column}' has {value.Length} bytes; the column's values have {fixedLength}.");
            Add(value);
        }
    }

    protected override void AppendAt(IArrowArray array, ReadOnlySpan<int> indices)
    {
        if (direct)
        {
            // Runs of consecutive positions (the items of lists lie together), each copied at once.
            for (int k = 0; k < indices.Length;)
            {
                int run = 1;
                while (k + run < indices.Length && indices[k + run] == indices[k] + run) run++;
                AppendRun(array, indices[k], run);
                k += run;
            }
            return;
        }
        Span<byte> scratch = stackalloc byte[ValueBytes];
        foreach (int i in indices)
        {
            ReadOnlySpan<byte> value = source(array, i, scratch);
            if (fixedLength > 0 && value.Length != fixedLength)
                throw new ArgumentException($"A value of column '{column}' has {value.Length} bytes; the column's values have {fixedLength}.");
            Add(value);
        }
    }

    /// <summary>Appends the values of rows <paramref name="start"/> .. +<paramref name="count"/> (all valid) of a direct array at once.</summary>
    private void AppendRun(IArrowArray array, int start, int count)
    {
        Page open = _open;
        int first = open.Length;
        open.Offsets.Grow(checked(first + count + 1), first + 1);
        if (array is BinaryArray binary)
        {
            ReadOnlySpan<int> offsets = binary.ValueOffsets.Slice(start, count + 1);
            CopyBytes(open, binary.ValueBuffer.Span.Slice(offsets[0], offsets[count] - offsets[0]));
            int shift = open.ArenaLength - offsets[0];
            Span<int> into = open.Offsets.AsSpan(first + 1, count);
            for (int k = 0; k < count; k++) into[k] = offsets[k + 1] + shift;
        }
        else if (array is BinaryViewArray views)
        {
            AppendViews(open, views, start, count);
        }
        else
        {
            var large = (LargeBinaryArray)array;
            ReadOnlySpan<long> offsets = large.ValueOffsets.Slice(start, count + 1);
            CopyBytes(open, large.ValueBuffer.Span.Slice(checked((int)offsets[0]), checked((int)(offsets[count] - offsets[0]))));
            long shift = open.ArenaLength - offsets[0];
            Span<int> into = open.Offsets.AsSpan(first + 1, count);
            for (int k = 0; k < count; k++) into[k] = checked((int)(offsets[k + 1] + shift));
        }
        open.Length += count;
        open.ArenaLength = open.Offsets[open.Length];
        if (Bloom is { } bloom)
        {
            for (int v = first; v < open.Length; v++) bloom.Add(open.Value(v));
        }

        static void CopyBytes(Page open, ReadOnlySpan<byte> bytes)
        {
            open.Arena.Grow(checked(open.ArenaLength + bytes.Length), open.ArenaLength);
            bytes.CopyTo(open.Arena.AsSpan(open.ArenaLength));
        }
    }

    /// <summary>
    /// Gathers the values of view rows <paramref name="start"/> .. +<paramref name="count"/> (all valid) into the arena:
    /// their lengths summed first (one growth), then each copied from the view itself (up to 12 bytes inline) or from
    /// its data buffer (the last one looked up kept: values mostly share a buffer). Offsets are written as they go.
    /// </summary>
    private static void AppendViews(Page open, BinaryViewArray array, int start, int count)
    {
        ReadOnlySpan<ArrowStringView> views = MemoryMarshal.Cast<byte, ArrowStringView>(array.ViewsBuffer.Span).Slice(array.Offset + start, count);
        long total = 0;
        foreach (ref readonly ArrowStringView view in views) total += view.Length;
        // 12 bytes of slack: an inline value is copied as its view's whole 12 bytes (the next value overwrites the rest).
        open.Arena.Grow(checked((int)(open.ArenaLength + total + 12)), open.ArenaLength);
        Span<byte> arena = open.Arena.Span;
        Span<int> into = open.Offsets.AsSpan(open.Length + 1, count);
        int at = open.ArenaLength, current = -1;
        ReadOnlySpan<byte> data = default;
        for (int k = 0; k < views.Length; k++)
        {
            ref readonly ArrowStringView view = ref views[k];
            int length = view.Length;
            if (length <= NativeBinaryViewBuilder.MaxInline)
            {
                ref byte inline = ref Unsafe.Add(ref Unsafe.As<ArrowStringView, byte>(ref Unsafe.AsRef(in view)), 4);
                Unsafe.WriteUnaligned(ref arena[at], Unsafe.ReadUnaligned<ulong>(ref inline));
                Unsafe.WriteUnaligned(ref arena[at + 8], Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref inline, 8)));
            }
            else
            {
                if (view.BufferIndex != current)
                {
                    current = view.BufferIndex;
                    data = array.DataBuffer(current).Span;
                }
                ShortCopy.Copy(data.Slice(view.Offset, length), arena[at..]);
            }
            at += length;
            into[k] = at;
        }
    }

    private void Add(ReadOnlySpan<byte> value)
    {
        Page open = _open;
        open.Arena.Grow(checked(open.ArenaLength + value.Length), open.ArenaLength);
        open.Offsets.Grow(open.Length + 2, open.Length + 1);
        value.CopyTo(open.Arena.AsSpan(open.ArenaLength));
        open.ArenaLength += value.Length;
        open.Offsets.At(++open.Length) = open.ArenaLength;
        Bloom?.Add(value);
    }

    /// <summary>The bytes the sources may make a value in (decimals, intervals, half floats).</summary>
    private const int ValueBytes = 32;

    private static void Bound(BytesOrder order, ByteBounds b, ReadOnlySpan<byte> value)
    {
        if (order == BytesOrder.Float16 && Half.IsNaN(BinaryPrimitives.ReadHalfLittleEndian(value)))
        {
            b.Nans++;
            return;
        }
        // One value: below the lower bound, or else perhaps above the upper one (bounds kept in order).
        if (b.MinLength < 0)
        {
            ByteBounds.Keep(value, ref b.MinBuffer, ref b.MinLength);
            ByteBounds.Keep(value, ref b.MaxBuffer, ref b.MaxLength);
        }
        else if (Compare(order, value, b.MinBuffer.AsSpan(0, b.MinLength)) < 0) ByteBounds.Keep(value, ref b.MinBuffer, ref b.MinLength);
        else if (Compare(order, value, b.MaxBuffer.AsSpan(0, b.MaxLength)) > 0) ByteBounds.Keep(value, ref b.MaxBuffer, ref b.MaxLength);
    }

    /// <summary>Widens <paramref name="b"/> to hold <paramref name="min"/> and <paramref name="max"/>.</summary>
    private void Widen(ByteBounds b, ReadOnlySpan<byte> min, ReadOnlySpan<byte> max)
    {
        if (!b.Has || Compare(order, min, b.Min) < 0) ByteBounds.Keep(min, ref b.MinBuffer, ref b.MinLength);
        if (b.MaxLength < 0 || Compare(order, max, b.Max) > 0) ByteBounds.Keep(max, ref b.MaxBuffer, ref b.MaxLength);
    }

    protected override PageValues TakePage()
    {
        Page page = _open;
        _open = new Page(page.Shape);
        return page;
    }

    protected override PageValues OpenPage() => _open;

    protected override bool OpenPagePrepares => Settings.Statistics && order != BytesOrder.None;

    protected override void MergeLocal(PageValues page)
    {
        var values = (Page)page;
        Span<uint> remap = values.Remap();
        for (int k = 0; k < remap.Length; k++) remap[k] = (uint)_dictionary.GetOrAdd(values.Entry(k));
    }

    protected override bool ClosePageBounds(PageValues pageValues, int values, out byte[]? min, out byte[]? max)
    {
        ByteBounds page = ((Page)pageValues).Bounds;
        _chunkBounds.Nans += page.Nans;
        if (page.Has) Widen(_chunkBounds, page.Min, page.Max);
        min = max = null;
        bool usable = values == 0 || (order != BytesOrder.None && page.Nans == 0 && page.Has && Bounds(page, out min, out max, out _, out _));
        page.Clear();
        return usable;
    }

    protected override int CompareBounds(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => Compare(order, a, b);

    /// <summary>
    /// The bounds of <paramref name="b"/> as written: half-float zeros signed as for floats, strings and binary cut to
    /// the truncation length; false when no upper bound that short exists.
    /// </summary>
    private bool Bounds(ByteBounds b, out byte[]? min, out byte[]? max, out bool minExact, out bool maxExact)
    {
        byte[] lower = b.Min.ToArray(), upper = b.Max.ToArray();
        minExact = maxExact = true;
        if (order == BytesOrder.Float16)
        {
            if (BinaryPrimitives.ReadHalfLittleEndian(lower) == Half.Zero) BinaryPrimitives.WriteHalfLittleEndian(lower, Half.NegativeZero);
            if (BinaryPrimitives.ReadHalfLittleEndian(upper) == Half.Zero) BinaryPrimitives.WriteHalfLittleEndian(upper, Half.Zero);
        }
        else if (fixedLength == 0 && Settings.TruncateLength is int limit)
        {
            (min, minExact) = BoundTruncation.Lower(lower, limit, utf8);
            (max, maxExact) = BoundTruncation.Upper(upper, limit, utf8);
            return max is not null;
        }
        (min, max) = (lower, upper);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Compare(BytesOrder order, ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => order switch
    {
        BytesOrder.SignedDecimal => (sbyte)a[0] != (sbyte)b[0] ? ((sbyte)a[0]).CompareTo((sbyte)b[0]) : a[1..].SequenceCompareTo(b[1..]),
        BytesOrder.Float16 => BinaryPrimitives.ReadHalfLittleEndian(a).CompareTo(BinaryPrimitives.ReadHalfLittleEndian(b)),
        _ => Bytewise(a, b),
    };

    /// <summary>Unsigned bytewise order; the first 8 bytes, read as one big-endian number, decide most comparisons.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Bytewise(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        if (a.Length >= 8 && b.Length >= 8)
        {
            ulong x = BinaryPrimitives.ReadUInt64BigEndian(a), y = BinaryPrimitives.ReadUInt64BigEndian(b);
            if (x != y) return x < y ? -1 : 1;
        }
        return a.SequenceCompareTo(b);
    }

    protected override int PageValueCount => _open.Length;

    protected override long PagePlainBytes => _open.PlainBytes;

    private static void WriteWithLength(ReadOnlySpan<byte> value, IBufferWriter<byte> output)
    {
        Span<byte> span = output.GetSpan(4 + value.Length);
        BinaryPrimitives.WriteInt32LittleEndian(span, value.Length);
        value.CopyTo(span[4..]);
        output.Advance(4 + value.Length);
    }

    protected override int DictionaryCount => _dictionary.Count;

    protected override long DictionaryPlainBytes => _dictionary.ByteSize + (fixedLength > 0 ? 0 : 4L * _dictionary.Count);

    protected override void EncodeDictionary(IBufferWriter<byte> output)
    {
        for (int i = 0; i < _dictionary.Count; i++)
        {
            if (fixedLength > 0) output.Write(_dictionary[i]);
            else WriteWithLength(_dictionary[i], output);
        }
    }

    protected override void ClearDictionary() => _dictionary.Clear();

    protected override Statistics? ChunkStatistics(long nullCount)
    {
        ByteBounds b = _chunkBounds;
        // No bounds without values, nor (as for floats) for half floats among which NaNs are.
        if (order == BytesOrder.None || !b.Has || b.Nans > 0)
            return new Statistics { NullCount = nullCount, NanCount = order == BytesOrder.Float16 ? b.Nans : null };
        if (!Bounds(b, out byte[]? min, out byte[]? max, out bool minExact, out bool maxExact))
            return new Statistics { NullCount = nullCount, MinValue = min, IsMinValueExact = minExact };
        return new Statistics
        {
            NullCount = nullCount,
            MinValue = min,
            MaxValue = max,
            IsMinValueExact = minExact,
            IsMaxValueExact = maxExact,
            NanCount = order == BytesOrder.Float16 ? b.Nans : null,
        };
    }

    protected override void ResetStatistics()
    {
        _open.Bounds.Clear();
        _chunkBounds.Clear();
    }

    protected override bool IsByteArray => fixedLength == 0;

    protected override long PageUnencodedBytes => _open.ArenaLength;

    protected override long ValueScratchBytes => _open.ScratchBytes + _dictionary.ScratchBytes;

    protected override void TrimValues()
    {
        _open.Trim();
        _dictionary.Trim();
    }

    public override void Dispose()
    {
        base.Dispose();
        _open.Dispose();
        _dictionary.Dispose();
    }
}

/// <summary>A BOOLEAN column: values bit-packed, never dictionary-encoded.</summary>
internal sealed class BooleanColumnWriter(PageSettings settings, int maxDef, int maxRep) : ColumnWriter(settings, maxDef, maxRep, dictionary: false)
{
    /// <summary>A page's values and bounds (gathered as the values came).</summary>
    private sealed class Page : PageValues
    {
        public readonly NativeArray<bool> Values = new(InitialBytes);
        public int Length;
        public bool Any, Min = true, Max;
        private readonly NativeBufferWriter _rle = new(256);
        private readonly NativeArray<uint> _bits = new();

        public override int Count => Length;

        public override long PlainBytes => (Length + 7) / 8;

        protected override int LocalCount => 0;

        protected override long LocalPlainBytes => 0;

        protected override void MapRange(Span<uint> indices, int first) => throw new NotSupportedException("Booleans are never dictionary-encoded.");

        protected override void ClearLocal() { }

        public override void EncodePlain(IBufferWriter<byte> output)
        {
            int bytes = (Length + 7) / 8;
            Span<byte> span = output.GetSpan(bytes)[..bytes];
            span.Clear();
            ReadOnlySpan<bool> page = Values.AsSpan(0, Length);
            for (int i = 0; i < page.Length; i++)
            {
                if (page[i]) span[i >> 3] |= (byte)(1 << (i & 7));
            }
            output.Advance(bytes);
        }

        /// <summary>RLE: the values run-length / bit-packed one bit wide, after their byte length (4 bytes).</summary>
        public override void EncodeAs(ParquetEncoding encoding, IBufferWriter<byte> output)
        {
            if (encoding != ParquetEncoding.Rle) throw new NotSupportedException($"Booleans are not written as {encoding}.");
            _bits.Reserve(Length);
            Span<uint> bits = _bits.AsSpan(0, Length);
            ReadOnlySpan<bool> page = Values.AsSpan(0, Length);
            for (int i = 0; i < bits.Length; i++) bits[i] = page[i] ? 1u : 0u;
            _rle.ResetWrittenCount();
            RleBitPackedEncoder.Encode(bits, 1, _rle);
            BinaryPrimitives.WriteInt32LittleEndian(output.GetSpan(4), _rle.WrittenCount);
            output.Advance(4);
            output.Write(_rle.WrittenSpan);
        }

        public override void Clear()
        {
            base.Clear();
            Length = 0;
            Any = Max = false;
            Min = true;
        }

        public override long ScratchBytes => base.ScratchBytes + Values.Capacity + _rle.Capacity + 4L * _bits.Capacity;

        /// <summary>Returns its memory to the pool (grown again as values come).</summary>
        public void Trim()
        {
            Values.Dispose();
            _rle.Trim();
            _bits.Dispose();
        }

        public override void Dispose()
        {
            base.Dispose();
            Values.Dispose();
            _rle.Dispose();
            _bits.Dispose();
        }
    }

    private Page _open = new();
    private bool _any, _min = true, _max; // the chunk's

    protected override void Append(IArrowArray array, int offset, int count, int nulls)
    {
        Page open = _open;
        open.Values.Grow(open.Length + count, open.Length);
        var booleans = (BooleanArray)array;
        for (int i = offset; i < offset + count; i++)
        {
            if (nulls > 0 && IsNullAt(array, offset, i - offset)) continue;
            Add(open, booleans.GetValue(i) == true);
        }
    }

    protected override void AppendAt(IArrowArray array, ReadOnlySpan<int> indices)
    {
        Page open = _open;
        open.Values.Grow(open.Length + indices.Length, open.Length);
        var booleans = (BooleanArray)array;
        foreach (int i in indices) Add(open, booleans.GetValue(i) == true);
    }

    private static void Add(Page open, bool value)
    {
        open.Values.At(open.Length++) = value;
        open.Any = true;
        open.Min &= value;
        open.Max |= value;
    }

    protected override PageValues TakePage()
    {
        Page page = _open;
        _open = new Page();
        return page;
    }

    protected override PageValues OpenPage() => _open;

    protected override bool ClosePageBounds(PageValues pageValues, int values, out byte[]? min, out byte[]? max)
    {
        var page = (Page)pageValues;
        min = max = null;
        if (page.Any)
        {
            _any = true;
            _min &= page.Min;
            _max |= page.Max;
            min = [page.Min ? (byte)1 : (byte)0];
            max = [page.Max ? (byte)1 : (byte)0];
        }
        return true;
    }

    protected override int CompareBounds(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a[0].CompareTo(b[0]);

    protected override int PageValueCount => _open.Length;

    protected override long PagePlainBytes => _open.PlainBytes;

    protected override Statistics? ChunkStatistics(long nullCount)
    {
        if (!_any) return new Statistics { NullCount = nullCount };
        byte[] min = [_min ? (byte)1 : (byte)0], max = [_max ? (byte)1 : (byte)0];
        return new Statistics
        {
            NullCount = nullCount, MinValue = min, MaxValue = max, Min = min, Max = max,
            IsMinValueExact = true, IsMaxValueExact = true,
        };
    }

    protected override void ResetStatistics()
    {
        _any = _max = false;
        _min = true;
    }

    protected override long ValueScratchBytes => _open.ScratchBytes;

    protected override void TrimValues() => _open.Trim();

    public override void Dispose()
    {
        base.Dispose();
        _open.Dispose();
    }
}

/// <summary>Cuts long string and binary bounds (Statistics: a bound may be a shorter value still bounding the column).</summary>
internal static class BoundTruncation
{
    /// <summary>A lower bound of at most <paramref name="limit"/> bytes: a prefix (on a character boundary for UTF-8).</summary>
    public static (byte[] Bound, bool Exact) Lower(byte[] value, int limit, bool utf8) =>
        value.Length <= limit ? (value, true) : (value[..Cut(value, limit, utf8)], false);

    /// <summary>
    /// An upper bound of at most <paramref name="limit"/> bytes: a prefix with its last byte (character, for UTF-8)
    /// incremented; null when no shorter value bounds it (all bytes 0xFF, all characters U+10FFFF).
    /// </summary>
    public static (byte[]? Bound, bool Exact) Upper(byte[] value, int limit, bool utf8)
    {
        if (value.Length <= limit) return (value, true);
        if (!utf8 || !Utf8Text(value))
        {
            byte[] prefix = value[..limit];
            for (int k = prefix.Length - 1; k >= 0; k--)
            {
                if (prefix[k] == 0xFF) continue;
                prefix[k]++;
                return (prefix[..(k + 1)], false);
            }
            return (null, false);
        }
        List<Rune> runes = [.. Encoding.UTF8.GetString(value, 0, Cut(value, limit, true)).EnumerateRunes()];
        while (runes.Count > 0)
        {
            int last = runes[^1].Value;
            runes.RemoveAt(runes.Count - 1);
            int next = last == 0xD7FF ? 0xE000 : last + 1;
            if (next > 0x10FFFF) continue;
            runes.Add(new Rune(next));
            var text = new StringBuilder();
            foreach (Rune r in runes) text.Append(r.ToString());
            return (Encoding.UTF8.GetBytes(text.ToString()), false);
        }
        return (null, false);
    }

    /// <summary>The prefix length at most <paramref name="limit"/>, not splitting a UTF-8 sequence.</summary>
    private static int Cut(byte[] value, int limit, bool utf8)
    {
        int cut = Math.Min(limit, value.Length);
        if (utf8 && Utf8Text(value))
        {
            while (cut > 0 && cut < value.Length && (value[cut] & 0xC0) == 0x80) cut--;
        }
        return cut;
    }

    private static bool Utf8Text(byte[] value) => System.Text.Unicode.Utf8.IsValid(value);
}
