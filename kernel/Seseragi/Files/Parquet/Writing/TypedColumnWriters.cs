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
    // The open page's values (and the converted rows they are gathered from) in native memory.
    private readonly NativeArray<T> _page = new(InitialBytes / Unsafe.SizeOf<T>()), _scratch = new();
    private int _count;
    private readonly FixedValueDictionary<T> _dictionary = new();
    private FixedBounds<T> _pageBounds, _chunkBounds;

    protected override void Append(IArrowArray array, int offset, int count, int nulls)
    {
        _page.Grow(_count + count, _count);
        Span<T> page = _page.Span;
        int start = _count;
        if (nulls == 0)
        {
            if (direct) ArrowValues.Values<T>(array).Slice(offset, count).CopyTo(page[_count..]);
            else source(array, offset, count, page.Slice(_count, count));
            _count += count;
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
                    if (!ArrowValues.IsNull(array, offset + i)) page[_count++] = all[i];
                }
            }
            else
            {
                // The set bits of each word: whole words at once, else bit by bit.
                for (int w = 0; w < validity.Length; w++)
                {
                    ulong bits = validity[w];
                    int at = w << 6;
                    if (bits == ulong.MaxValue)
                    {
                        all.Slice(at, 64).CopyTo(page[_count..]);
                        _count += 64;
                        continue;
                    }
                    while (bits != 0)
                    {
                        page[_count++] = all[at + BitOperations.TrailingZeroCount(bits)];
                        bits &= bits - 1;
                    }
                }
            }
        }
        if (Settings.Statistics && order.HasBounds) Bounds(page[start.._count]);
        if (Bloom is { } bloom) Hash(bloom, start);
    }

    protected override void AppendAt(IArrowArray array, ReadOnlySpan<int> indices)
    {
        _page.Grow(_count + indices.Length, _count);
        Span<T> page = _page.Span;
        int start = _count;
        // Runs of consecutive positions (the items of lists lie together) are read at once.
        for (int k = 0; k < indices.Length;)
        {
            int run = 1;
            while (k + run < indices.Length && indices[k + run] == indices[k] + run) run++;
            source(array, indices[k], run, page.Slice(_count, run));
            _count += run;
            k += run;
        }
        if (Settings.Statistics && order.HasBounds) Bounds(page[start.._count]);
        if (Bloom is { } bloom) Hash(bloom, start);
    }

    private void Hash(BloomBuilder bloom, int start)
    {
        ReadOnlySpan<T> page = _page.AsSpan(0, _count);
        for (int i = start; i < _count; i++) bloom.Add(MemoryMarshal.AsBytes(page.Slice(i, 1)));
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

    protected override bool ClosePageBounds(int values, out byte[]? min, out byte[]? max)
    {
        FixedBounds<T> page = _pageBounds;
        _pageBounds = default;
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

    protected override int PageValueCount => _count;

    protected override long PagePlainBytes => (long)_count * Unsafe.SizeOf<T>();

    protected override void EncodePlain(IBufferWriter<byte> output) => output.Write(MemoryMarshal.AsBytes(_page.AsSpan(0, _count)));

    protected override void EncodeAs(ParquetEncoding encoding, IBufferWriter<byte> output)
    {
        Span<T> values = _page.AsSpan(0, _count);
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

    protected override void ClearPage() => _count = 0;

    protected override void MapToDictionary(Span<uint> indices, int first)
    {
        ReadOnlySpan<T> page = _page.AsSpan(first, indices.Length);
        for (int i = 0; i < indices.Length; i++) indices[i] = (uint)_dictionary.GetOrAdd(page[i]);
    }

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
        (long)(_page.Capacity + _scratch.Capacity) * Unsafe.SizeOf<T>() + _dictionary.ScratchBytes;

    protected override void TrimValues()
    {
        _page.Dispose();
        _scratch.Dispose();
        _dictionary.Trim();
    }

    public override void Dispose()
    {
        base.Dispose();
        _page.Dispose();
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
    // The open page's values back to back, value i at _offsets[i] .. [i + 1] (_offsets[0] stays 0), in native memory;
    // the lengths the delta encodings gather.
    private readonly NativeArray<byte> _arena = new(1 << 16);
    private int _arenaLength;
    private readonly NativeArray<int> _offsets = Offsets();
    private readonly NativeArray<int> _lengths = new();
    private int _count;
    private readonly ByteArrayDictionary _dictionary = new();
    private readonly ByteBounds _pageBounds = new(), _chunkBounds = new();

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
        int first = _count;
        _offsets.Grow(checked(_count + count + 1), _count + 1);
        if (array is BinaryArray binary)
        {
            ReadOnlySpan<int> offsets = binary.ValueOffsets.Slice(start, count + 1);
            CopyBytes(binary.ValueBuffer.Span.Slice(offsets[0], offsets[count] - offsets[0]));
            int shift = _arenaLength - offsets[0];
            Span<int> into = _offsets.AsSpan(_count + 1, count);
            for (int k = 0; k < count; k++) into[k] = offsets[k + 1] + shift;
        }
        else if (array is BinaryViewArray views)
        {
            AppendViews(views, start, count);
        }
        else
        {
            var large = (LargeBinaryArray)array;
            ReadOnlySpan<long> offsets = large.ValueOffsets.Slice(start, count + 1);
            CopyBytes(large.ValueBuffer.Span.Slice(checked((int)offsets[0]), checked((int)(offsets[count] - offsets[0]))));
            long shift = _arenaLength - offsets[0];
            Span<int> into = _offsets.AsSpan(_count + 1, count);
            for (int k = 0; k < count; k++) into[k] = checked((int)(offsets[k + 1] + shift));
        }
        _count += count;
        _arenaLength = _offsets[_count];
        if (Bloom is { } bloom)
        {
            for (int v = first; v < _count; v++) bloom.Add(Value(v));
        }

        void CopyBytes(ReadOnlySpan<byte> bytes)
        {
            _arena.Grow(checked(_arenaLength + bytes.Length), _arenaLength);
            bytes.CopyTo(_arena.AsSpan(_arenaLength));
        }
    }

    /// <summary>
    /// Gathers the values of view rows <paramref name="start"/> .. +<paramref name="count"/> (all valid) into the arena:
    /// their lengths summed first (one growth), then each copied from the view itself (up to 12 bytes inline) or from
    /// its data buffer (the last one looked up kept: values mostly share a buffer). Offsets are written as they go.
    /// </summary>
    private void AppendViews(BinaryViewArray array, int start, int count)
    {
        ReadOnlySpan<ArrowStringView> views = MemoryMarshal.Cast<byte, ArrowStringView>(array.ViewsBuffer.Span).Slice(array.Offset + start, count);
        long total = 0;
        foreach (ref readonly ArrowStringView view in views) total += view.Length;
        // 12 bytes of slack: an inline value is copied as its view's whole 12 bytes (the next value overwrites the rest).
        _arena.Grow(checked((int)(_arenaLength + total + 12)), _arenaLength);
        Span<byte> arena = _arena.Span;
        Span<int> into = _offsets.AsSpan(_count + 1, count);
        int at = _arenaLength, current = -1;
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
        _arena.Grow(checked(_arenaLength + value.Length), _arenaLength);
        _offsets.Grow(_count + 2, _count + 1);
        value.CopyTo(_arena.AsSpan(_arenaLength));
        _arenaLength += value.Length;
        _offsets.At(++_count) = _arenaLength;
        Bloom?.Add(value);
    }

    /// <summary>The bytes the sources may make a value in (decimals, intervals, half floats).</summary>
    private const int ValueBytes = 32;

    /// <summary>The offsets of an empty page: the first, 0, is never written again.</summary>
    private static NativeArray<int> Offsets()
    {
        var offsets = new NativeArray<int>(4097);
        offsets[0] = 0;
        return offsets;
    }

    private ReadOnlySpan<byte> Value(int i)
    {
        int start = _offsets.At(i);
        return _arena.AsSpan(start, _offsets.At(i + 1) - start);
    }

    private void Bound(ReadOnlySpan<byte> value)
    {
        ByteBounds b = _pageBounds;
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
        else if (Compare(value, b.MinBuffer.AsSpan(0, b.MinLength)) < 0) ByteBounds.Keep(value, ref b.MinBuffer, ref b.MinLength);
        else if (Compare(value, b.MaxBuffer.AsSpan(0, b.MaxLength)) > 0) ByteBounds.Keep(value, ref b.MaxBuffer, ref b.MaxLength);
    }

    /// <summary>Widens <paramref name="b"/> to hold <paramref name="min"/> and <paramref name="max"/>.</summary>
    private void Widen(ByteBounds b, ReadOnlySpan<byte> min, ReadOnlySpan<byte> max)
    {
        if (!b.Has || Compare(min, b.Min) < 0) ByteBounds.Keep(min, ref b.MinBuffer, ref b.MinLength);
        if (b.MaxLength < 0 || Compare(max, b.Max) > 0) ByteBounds.Keep(max, ref b.MaxBuffer, ref b.MaxLength);
    }

    private readonly NativeArray<int> _seen = new(); // the page stamp of each dictionary entry bounded (0: none)
    private int _stamp;

    /// <summary>
    /// The page's bounds, gathered at its end: from the distinct dictionary entries of a dictionary-encoded page (half
    /// floats aside, whose NaNs are counted value by value), else value by value.
    /// </summary>
    protected override void FinishPageBounds(ReadOnlySpan<uint> dictionaryIndices)
    {
        if (order == BytesOrder.None) return;
        if (!dictionaryIndices.IsEmpty && order != BytesOrder.Float16)
        {
            if (_seen.Capacity < _dictionary.Count)
            {
                // Entries new to the stamps were never bounded: zero, below every stamp.
                int had = _seen.Capacity;
                _seen.Grow(_dictionary.Count, had);
                _seen.AsSpan(had).Clear();
            }
            if (++_stamp == int.MaxValue)
            {
                _seen.Clear();
                _stamp = 1;
            }
            Span<int> seen = _seen.Span;
            foreach (uint index in dictionaryIndices)
            {
                if (seen[(int)index] == _stamp) continue;
                seen[(int)index] = _stamp;
                Bound(_dictionary[(int)index]);
            }
            return;
        }
        for (int i = 0; i < _count; i++) Bound(Value(i));
    }

    protected override bool ClosePageBounds(int values, out byte[]? min, out byte[]? max)
    {
        ByteBounds page = _pageBounds;
        _chunkBounds.Nans += page.Nans;
        if (page.Has) Widen(_chunkBounds, page.Min, page.Max);
        min = max = null;
        bool usable = values == 0 || (order != BytesOrder.None && page.Nans == 0 && page.Has && Bounds(page, out min, out max, out _, out _));
        page.Clear();
        return usable;
    }

    protected override int CompareBounds(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => Compare(a, b);

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
    private int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => order switch
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

    protected override int PageValueCount => _count;

    protected override long PagePlainBytes => _arenaLength + (fixedLength > 0 ? 0 : 4L * _count);

    protected override void EncodePlain(IBufferWriter<byte> output)
    {
        if (fixedLength > 0)
        {
            output.Write(_arena.AsSpan(0, _arenaLength));
            return;
        }
        // Each value after its length, into one span taken at once.
        ReadOnlySpan<byte> arena = _arena.AsSpan(0, _arenaLength);
        ReadOnlySpan<int> offsets = _offsets.AsSpan(0, _count + 1);
        Span<byte> into = output.GetSpan(checked((int)PagePlainBytes));
        int at = 0;
        for (int i = 0; i < _count; i++)
        {
            int start = offsets[i], length = offsets[i + 1] - start;
            BinaryPrimitives.WriteInt32LittleEndian(into[at..], length);
            ShortCopy.Copy(arena.Slice(start, length), into[(at + 4)..]);
            at += 4 + length;
        }
        output.Advance(at);
    }

    protected override void EncodeAs(ParquetEncoding encoding, IBufferWriter<byte> output)
    {
        _lengths.Reserve(Math.Max(1, 2 * _count));
        ReadOnlySpan<byte> bytes = _arena.AsSpan(0, _arenaLength);
        ReadOnlySpan<int> offsets = _offsets.AsSpan(0, _count + 1);
        switch (encoding)
        {
            case ParquetEncoding.DeltaLengthByteArray when fixedLength == 0:
                DeltaByteArrayEncoder.EncodeLengths(bytes, offsets, _lengths.Span, output);
                break;
            case ParquetEncoding.DeltaByteArray:
                DeltaByteArrayEncoder.EncodeIncremental(bytes, offsets, _lengths.Span, output);
                break;
            case ParquetEncoding.ByteStreamSplit when fixedLength > 0:
                ByteStreamSplit.Encode(bytes, fixedLength, output.GetSpan(bytes.Length));
                output.Advance(bytes.Length);
                break;
            default:
                throw new NotSupportedException($"Column '{column}' is not written as {encoding}.");
        }
    }

    private static void WriteWithLength(ReadOnlySpan<byte> value, IBufferWriter<byte> output)
    {
        Span<byte> span = output.GetSpan(4 + value.Length);
        BinaryPrimitives.WriteInt32LittleEndian(span, value.Length);
        value.CopyTo(span[4..]);
        output.Advance(4 + value.Length);
    }

    protected override void ClearPage()
    {
        _count = 0;
        _arenaLength = 0;
    }

    protected override void MapToDictionary(Span<uint> indices, int first)
    {
        for (int i = 0; i < indices.Length; i++) indices[i] = (uint)_dictionary.GetOrAdd(Value(first + i));
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
        _pageBounds.Clear();
        _chunkBounds.Clear();
    }

    protected override bool IsByteArray => fixedLength == 0;

    protected override long PageUnencodedBytes => _arenaLength;

    protected override long ValueScratchBytes =>
        _arena.Capacity + 4L * (_offsets.Capacity + _lengths.Capacity + _seen.Capacity) + _dictionary.ScratchBytes;

    protected override void TrimValues()
    {
        _arena.Dispose();
        _offsets.Dispose();
        _offsets.Reserve(1);
        _offsets[0] = 0; // the first offset of every page
        _lengths.Dispose();
        _seen.Dispose();
        _dictionary.Trim();
    }

    public override void Dispose()
    {
        base.Dispose();
        _arena.Dispose();
        _offsets.Dispose();
        _lengths.Dispose();
        _seen.Dispose();
        _dictionary.Dispose();
    }
}

/// <summary>A BOOLEAN column: values bit-packed, never dictionary-encoded.</summary>
internal sealed class BooleanColumnWriter(PageSettings settings, int maxDef, int maxRep) : ColumnWriter(settings, maxDef, maxRep, dictionary: false)
{
    private readonly NativeArray<bool> _page = new(InitialBytes);
    private int _count;
    private bool _any, _min = true, _max;          // the chunk's
    private bool _pageAny, _pageMin = true, _pageMax; // the open page's

    protected override void Append(IArrowArray array, int offset, int count, int nulls)
    {
        _page.Grow(_count + count, _count);
        var booleans = (BooleanArray)array;
        for (int i = offset; i < offset + count; i++)
        {
            if (nulls > 0 && IsNullAt(array, offset, i - offset)) continue;
            Add(booleans.GetValue(i) == true);
        }
    }

    protected override void AppendAt(IArrowArray array, ReadOnlySpan<int> indices)
    {
        _page.Grow(_count + indices.Length, _count);
        var booleans = (BooleanArray)array;
        foreach (int i in indices) Add(booleans.GetValue(i) == true);
    }

    private void Add(bool value)
    {
        _page.At(_count++) = value;
        _pageAny = true;
        _pageMin &= value;
        _pageMax |= value;
    }

    protected override bool ClosePageBounds(int values, out byte[]? min, out byte[]? max)
    {
        min = max = null;
        if (_pageAny)
        {
            _any = true;
            _min &= _pageMin;
            _max |= _pageMax;
            min = [_pageMin ? (byte)1 : (byte)0];
            max = [_pageMax ? (byte)1 : (byte)0];
        }
        _pageAny = _pageMax = false;
        _pageMin = true;
        return true;
    }

    protected override int CompareBounds(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => a[0].CompareTo(b[0]);

    protected override int PageValueCount => _count;

    protected override long PagePlainBytes => (_count + 7) / 8;

    protected override void EncodePlain(IBufferWriter<byte> output)
    {
        int bytes = (_count + 7) / 8;
        Span<byte> span = output.GetSpan(bytes)[..bytes];
        span.Clear();
        ReadOnlySpan<bool> page = _page.AsSpan(0, _count);
        for (int i = 0; i < page.Length; i++)
        {
            if (page[i]) span[i >> 3] |= (byte)(1 << (i & 7));
        }
        output.Advance(bytes);
    }

    protected override void ClearPage() => _count = 0;

    private readonly NativeBufferWriter _rle = new(256);
    private readonly NativeArray<uint> _bits = new();

    /// <summary>RLE: the values run-length / bit-packed one bit wide, after their byte length (4 bytes).</summary>
    protected override void EncodeAs(ParquetEncoding encoding, IBufferWriter<byte> output)
    {
        if (encoding != ParquetEncoding.Rle) throw new NotSupportedException($"Booleans are not written as {encoding}.");
        _bits.Reserve(_count);
        Span<uint> bits = _bits.AsSpan(0, _count);
        ReadOnlySpan<bool> page = _page.AsSpan(0, _count);
        for (int i = 0; i < bits.Length; i++) bits[i] = page[i] ? 1u : 0u;
        _rle.ResetWrittenCount();
        RleBitPackedEncoder.Encode(bits, 1, _rle);
        BinaryPrimitives.WriteInt32LittleEndian(output.GetSpan(4), _rle.WrittenCount);
        output.Advance(4);
        output.Write(_rle.WrittenSpan);
    }

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
        _any = _max = _pageAny = _pageMax = false;
        _min = _pageMin = true;
    }

    protected override long ValueScratchBytes => _page.Capacity + _rle.Capacity + 4L * _bits.Capacity;

    protected override void TrimValues()
    {
        _page.Dispose();
        _rle.Trim();
        _bits.Dispose();
    }

    public override void Dispose()
    {
        base.Dispose();
        _page.Dispose();
        _rle.Dispose();
        _bits.Dispose();
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
