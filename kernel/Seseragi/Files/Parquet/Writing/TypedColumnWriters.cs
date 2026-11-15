using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Array = System.Array;
using Kaminari.Sazanami.Encodings;
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

/// <summary>A column of a fixed-width physical type (INT32, INT64, INT96, FLOAT, DOUBLE).</summary>
internal sealed class FixedColumnWriter<T, TOrder>(PageSettings settings, int maxDef, int maxRep, FixedSource<T> source, TOrder order)
    : ColumnWriter(settings, maxDef, maxRep, dictionary: true)
    where T : unmanaged
    where TOrder : struct, IValueOrder<T>
{
    private T[] _page = new T[1024];
    private T[] _scratch = new T[1024];
    private int _count;
    private readonly FixedValueDictionary<T> _dictionary = new();
    private bool _hasBounds;
    private T _min, _max;
    private long _nans;

    protected override void Append(IArrowArray array, int offset, int count, int nulls)
    {
        if (_page.Length < _count + count) Array.Resize(ref _page, Math.Max(_page.Length * 2, _count + count));
        int start = _count;
        if (nulls == 0)
        {
            source(array, offset, count, _page.AsSpan(_count, count));
            _count += count;
        }
        else
        {
            if (_scratch.Length < count) _scratch = new T[Math.Max(count, _scratch.Length * 2)];
            Span<T> all = _scratch.AsSpan(0, count);
            source(array, offset, count, all);
            for (int i = 0; i < count; i++)
            {
                if (!ArrowValues.IsNull(array, offset + i)) _page[_count++] = all[i];
            }
        }
        if (Settings.Statistics && order.HasBounds) Bounds(_page.AsSpan(start, _count - start));
    }

    protected override void AppendAt(IArrowArray array, ReadOnlySpan<int> indices)
    {
        if (_page.Length < _count + indices.Length) Array.Resize(ref _page, Math.Max(_page.Length * 2, _count + indices.Length));
        int start = _count;
        // Runs of consecutive positions (the items of lists lie together) are read at once.
        for (int k = 0; k < indices.Length;)
        {
            int run = 1;
            while (k + run < indices.Length && indices[k + run] == indices[k] + run) run++;
            source(array, indices[k], run, _page.AsSpan(_count, run));
            _count += run;
            k += run;
        }
        if (Settings.Statistics && order.HasBounds) Bounds(_page.AsSpan(start, _count - start));
    }

    private void Bounds(ReadOnlySpan<T> values)
    {
        foreach (T value in values)
        {
            if (order.Floating && order.IsNaN(value))
            {
                _nans++;
                continue;
            }
            if (!_hasBounds)
            {
                _min = _max = value;
                _hasBounds = true;
            }
            else if (order.Compare(value, _min) < 0) _min = value;
            else if (order.Compare(value, _max) > 0) _max = value;
        }
    }

    protected override int PageValueCount => _count;

    protected override long PagePlainBytes => (long)_count * Unsafe.SizeOf<T>();

    protected override void EncodePlain(IBufferWriter<byte> output) => output.Write(MemoryMarshal.AsBytes(_page.AsSpan(0, _count)));

    protected override void ClearPage() => _count = 0;

    protected override void MapToDictionary(Span<uint> indices)
    {
        for (int i = 0; i < indices.Length; i++) indices[i] = (uint)_dictionary.GetOrAdd(_page[i]);
    }

    protected override int DictionaryCount => _dictionary.Count;

    protected override long DictionaryPlainBytes => (long)_dictionary.Count * Unsafe.SizeOf<T>();

    protected override void EncodeDictionary(IBufferWriter<byte> output) => output.Write(MemoryMarshal.AsBytes(_dictionary.Values));

    protected override void ClearDictionary() => _dictionary.Clear();

    protected override Statistics? ChunkStatistics(long nullCount)
    {
        if (!order.HasBounds) return new Statistics { NullCount = nullCount };
        // A chunk holding NaNs gets no bounds: the specification keeps NaN out of them, but Spark (parquet-mr, which
        // ignores nan_count) orders NaN above every number and would skip the NaN rows of `x > bound`.
        if (_nans > 0) return new Statistics { NullCount = nullCount, NanCount = _nans };
        byte[]? min = _hasBounds ? order.Bound(_min, upper: false) : null;
        byte[]? max = _hasBounds ? order.Bound(_max, upper: true) : null;
        return new Statistics
        {
            NullCount = nullCount,
            MinValue = min,
            MaxValue = max,
            IsMinValueExact = _hasBounds ? true : null,
            IsMaxValueExact = _hasBounds ? true : null,
            Min = order.Legacy ? min : null,
            Max = order.Legacy ? max : null,
            NanCount = order.Floating ? _nans : null,
        };
    }

    protected override void ResetStatistics()
    {
        _hasBounds = false;
        _nans = 0;
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

/// <summary>A column of BYTE_ARRAY (<paramref name="fixedLength"/> 0) or FIXED_LEN_BYTE_ARRAY values.</summary>
internal sealed class BytesColumnWriter(PageSettings settings, int maxDef, int maxRep, BytesSource source, int fixedLength, BytesOrder order, bool utf8, string column)
    : ColumnWriter(settings, maxDef, maxRep, dictionary: true)
{
    private byte[] _arena = new byte[1 << 16];
    private int _arenaLength;
    private int[] _offsets = new int[1025];
    private int _count;
    private readonly byte[] _scratch = new byte[32];
    private readonly ByteArrayDictionary _dictionary = new();
    private byte[] _min = new byte[64], _max = new byte[64];
    private int _minLength = -1, _maxLength = -1; // -1: no bound yet
    private long _nans;

    protected override void Append(IArrowArray array, int offset, int count, int nulls)
    {
        for (int i = offset; i < offset + count; i++)
        {
            if (nulls > 0 && ArrowValues.IsNull(array, i)) continue;
            ReadOnlySpan<byte> value = source(array, i, _scratch);
            if (fixedLength > 0 && value.Length != fixedLength)
                throw new ArgumentException($"A value of column '{column}' has {value.Length} bytes; the column's values have {fixedLength}.");
            Add(value);
            if (Settings.Statistics && order != BytesOrder.None) Bound(value);
        }
    }

    protected override void AppendAt(IArrowArray array, ReadOnlySpan<int> indices)
    {
        foreach (int i in indices)
        {
            ReadOnlySpan<byte> value = source(array, i, _scratch);
            if (fixedLength > 0 && value.Length != fixedLength)
                throw new ArgumentException($"A value of column '{column}' has {value.Length} bytes; the column's values have {fixedLength}.");
            Add(value);
            if (Settings.Statistics && order != BytesOrder.None) Bound(value);
        }
    }

    private void Add(ReadOnlySpan<byte> value)
    {
        if (_arenaLength + value.Length > _arena.Length)
            Array.Resize(ref _arena, (int)Math.Min(Array.MaxLength, Math.Max(_arena.Length * 2L, (long)_arenaLength + value.Length)));
        if (_count + 1 == _offsets.Length) Array.Resize(ref _offsets, _offsets.Length * 2);
        value.CopyTo(_arena.AsSpan(_arenaLength));
        _arenaLength += value.Length;
        _offsets[++_count] = _arenaLength;
    }

    private ReadOnlySpan<byte> Value(int i) => _arena.AsSpan(_offsets[i], _offsets[i + 1] - _offsets[i]);

    private void Bound(ReadOnlySpan<byte> value)
    {
        if (order == BytesOrder.Float16 && Half.IsNaN(BinaryPrimitives.ReadHalfLittleEndian(value)))
        {
            _nans++;
            return;
        }
        if (_minLength < 0 || Compare(value, _min.AsSpan(0, _minLength)) < 0) Keep(value, ref _min, ref _minLength);
        if (_maxLength < 0 || Compare(value, _max.AsSpan(0, _maxLength)) > 0) Keep(value, ref _max, ref _maxLength);
    }

    private static void Keep(ReadOnlySpan<byte> value, ref byte[] buffer, ref int length)
    {
        if (buffer.Length < value.Length) buffer = new byte[Math.Max(value.Length, buffer.Length * 2)];
        value.CopyTo(buffer);
        length = value.Length;
    }

    private int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b) => order switch
    {
        BytesOrder.SignedDecimal => (sbyte)a[0] != (sbyte)b[0] ? ((sbyte)a[0]).CompareTo((sbyte)b[0]) : a[1..].SequenceCompareTo(b[1..]),
        BytesOrder.Float16 => BinaryPrimitives.ReadHalfLittleEndian(a).CompareTo(BinaryPrimitives.ReadHalfLittleEndian(b)),
        _ => a.SequenceCompareTo(b),
    };

    protected override int PageValueCount => _count;

    protected override long PagePlainBytes => _arenaLength + (fixedLength > 0 ? 0 : 4L * _count);

    protected override void EncodePlain(IBufferWriter<byte> output)
    {
        if (fixedLength > 0)
        {
            output.Write(_arena.AsSpan(0, _arenaLength));
            return;
        }
        for (int i = 0; i < _count; i++) WriteWithLength(Value(i), output);
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

    protected override void MapToDictionary(Span<uint> indices)
    {
        for (int i = 0; i < indices.Length; i++) indices[i] = (uint)_dictionary.GetOrAdd(Value(i));
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
        // No bounds without values, nor (as for floats) for half floats among which NaNs are.
        if (order == BytesOrder.None || _minLength < 0 || _nans > 0)
            return new Statistics { NullCount = nullCount, NanCount = order == BytesOrder.Float16 ? _nans : null };
        byte[] min = _min.AsSpan(0, _minLength).ToArray(), max = _max.AsSpan(0, _maxLength).ToArray();
        bool minExact = true, maxExact = true;
        if (order == BytesOrder.Float16)
        {
            if (BinaryPrimitives.ReadHalfLittleEndian(min) == Half.Zero) BinaryPrimitives.WriteHalfLittleEndian(min, Half.NegativeZero);
            if (BinaryPrimitives.ReadHalfLittleEndian(max) == Half.Zero) BinaryPrimitives.WriteHalfLittleEndian(max, Half.Zero);
        }
        else if (fixedLength == 0 && Settings.TruncateLength is int limit)
        {
            (min, minExact) = BoundTruncation.Lower(min, limit, utf8);
            byte[]? upper;
            (upper, maxExact) = BoundTruncation.Upper(max, limit, utf8);
            if (upper is null) return new Statistics { NullCount = nullCount, MinValue = min, IsMinValueExact = minExact };
            max = upper;
        }
        return new Statistics
        {
            NullCount = nullCount,
            MinValue = min,
            MaxValue = max,
            IsMinValueExact = minExact,
            IsMaxValueExact = maxExact,
            NanCount = order == BytesOrder.Float16 ? _nans : null,
        };
    }

    protected override void ResetStatistics()
    {
        _minLength = _maxLength = -1;
        _nans = 0;
    }
}

/// <summary>A BOOLEAN column: values bit-packed, never dictionary-encoded.</summary>
internal sealed class BooleanColumnWriter(PageSettings settings, int maxDef, int maxRep) : ColumnWriter(settings, maxDef, maxRep, dictionary: false)
{
    private bool[] _page = new bool[1024];
    private int _count;
    private bool _any, _min = true, _max;

    protected override void Append(IArrowArray array, int offset, int count, int nulls)
    {
        if (_page.Length < _count + count) Array.Resize(ref _page, Math.Max(_page.Length * 2, _count + count));
        var booleans = (BooleanArray)array;
        for (int i = offset; i < offset + count; i++)
        {
            if (nulls > 0 && booleans.IsNull(i)) continue;
            Add(booleans.GetValue(i) == true);
        }
    }

    protected override void AppendAt(IArrowArray array, ReadOnlySpan<int> indices)
    {
        if (_page.Length < _count + indices.Length) Array.Resize(ref _page, Math.Max(_page.Length * 2, _count + indices.Length));
        var booleans = (BooleanArray)array;
        foreach (int i in indices) Add(booleans.GetValue(i) == true);
    }

    private void Add(bool value)
    {
        _page[_count++] = value;
        _any = true;
        _min &= value;
        _max |= value;
    }

    protected override int PageValueCount => _count;

    protected override long PagePlainBytes => (_count + 7) / 8;

    protected override void EncodePlain(IBufferWriter<byte> output)
    {
        int bytes = (_count + 7) / 8;
        Span<byte> span = output.GetSpan(bytes)[..bytes];
        span.Clear();
        for (int i = 0; i < _count; i++)
        {
            if (_page[i]) span[i >> 3] |= (byte)(1 << (i & 7));
        }
        output.Advance(bytes);
    }

    protected override void ClearPage() => _count = 0;

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
