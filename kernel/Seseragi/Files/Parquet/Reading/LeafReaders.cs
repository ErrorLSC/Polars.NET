using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Unicode;
using Kaminari.Sazanami.Encodings;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Helpers;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet.Reading;

/// <summary>Creates the reader of a flat leaf by its value kind.</summary>
internal static class LeafReaders
{
    public static LeafReader Create(PageReader pages, SchemaNode leaf, LeafType type, long valueCount) => type.Kind switch
    {
        ValueKind.Null => new NullLeafReader(pages, leaf, type, valueCount),
        ValueKind.Boolean => new BooleanLeafReader(pages, leaf, type, valueCount),
        ValueKind.Int32 when type.ArrowType is UInt32Type => new FixedLeafReader<uint, SameBits<uint>>(pages, leaf, type, valueCount),
        ValueKind.Int32 => new FixedLeafReader<int, SameBits<int>>(pages, leaf, type, valueCount),
        ValueKind.Int8 => new FixedLeafReader<sbyte, NarrowInt32<sbyte>>(pages, leaf, type, valueCount),
        ValueKind.Int16 => new FixedLeafReader<short, NarrowInt32<short>>(pages, leaf, type, valueCount),
        ValueKind.UInt8 => new FixedLeafReader<byte, NarrowInt32<byte>>(pages, leaf, type, valueCount),
        ValueKind.UInt16 => new FixedLeafReader<ushort, NarrowInt32<ushort>>(pages, leaf, type, valueCount),
        ValueKind.Int64 when type.ArrowType is UInt64Type => new FixedLeafReader<ulong, SameBits<ulong>>(pages, leaf, type, valueCount),
        ValueKind.Int64 => new FixedLeafReader<long, SameBits<long>>(pages, leaf, type, valueCount),
        ValueKind.RescaledInt64 => new FixedLeafReader<long, RescaledInt64>(pages, leaf, type, valueCount),
        ValueKind.NarrowInt64 => type.ArrowType switch
        {
            Int8Type => new FixedLeafReader<sbyte, NarrowInt64<sbyte>>(pages, leaf, type, valueCount),
            Int16Type => new FixedLeafReader<short, NarrowInt64<short>>(pages, leaf, type, valueCount),
            Int32Type => new FixedLeafReader<int, NarrowInt64<int>>(pages, leaf, type, valueCount),
            UInt8Type => new FixedLeafReader<byte, NarrowInt64<byte>>(pages, leaf, type, valueCount),
            UInt16Type => new FixedLeafReader<ushort, NarrowInt64<ushort>>(pages, leaf, type, valueCount),
            _ => new FixedLeafReader<uint, NarrowInt64<uint>>(pages, leaf, type, valueCount),
        },
        ValueKind.Float => new FixedLeafReader<float, SameBits<float>>(pages, leaf, type, valueCount),
        ValueKind.Double => new FixedLeafReader<double, SameBits<double>>(pages, leaf, type, valueCount),
        ValueKind.Float16 => new FixedLeafReader<Half, SameBits<Half>>(pages, leaf, type, valueCount),
        ValueKind.DecimalFromInt32 or ValueKind.DecimalFromInt64 => new FixedLeafReader<Int128, IntegerDecimal>(pages, leaf, type, valueCount),
        ValueKind.DecimalFromBytes when leaf.Element.Type == ParquetType.ByteArray => type.ArrowType is Decimal128Type
            ? new ByteArrayDecimalLeafReader<Int128>(pages, leaf, type, valueCount, Decimals.ToInt128)
            : new ByteArrayDecimalLeafReader<Int256Bits>(pages, leaf, type, valueCount, Decimals.ToInt256),
        ValueKind.DecimalFromBytes => type.ArrowType is Decimal128Type
            ? new FixedLeafReader<Int128, FixedBytesDecimal128>(pages, leaf, type, valueCount)
            : new FixedLeafReader<Int256Bits, FixedBytesDecimal256>(pages, leaf, type, valueCount),
        ValueKind.Int96Timestamp => new FixedLeafReader<long, Int96Timestamp>(pages, leaf, type, valueCount),
        ValueKind.FlbaTimestamp => new FixedLeafReader<long, WideTimestamp>(pages, leaf, type, valueCount),
        ValueKind.Interval => new FixedLeafReader<MonthDayNanos, IntervalValue>(pages, leaf, type, valueCount),
        ValueKind.FixedBinary => new FixedBinaryLeafReader(pages, leaf, type, valueCount),
        ValueKind.Utf8 or ValueKind.Binary when type.Dictionary => new DictionaryOutputLeafReader(pages, leaf, type, valueCount),
        ValueKind.Utf8 or ValueKind.Binary when type.ArrowType is StringViewType or BinaryViewType => new BinaryViewLeafReader(pages, leaf, type, valueCount),
        ValueKind.Utf8 or ValueKind.Binary => new BinaryLeafReader(pages, leaf, type, valueCount),
        _ => throw new ParquetFormatException($"No reader for {type.Kind}."),
    };

    /// <summary>The PLAIN BYTE_ARRAY value at <paramref name="position"/> (a 4-byte length, then the bytes).</summary>
    public static ReadOnlySpan<byte> NextByteArray(ReadOnlySpan<byte> values, ref int position)
    {
        if (values.Length - position < 4) throw new ParquetFormatException("a BYTE_ARRAY value runs past the page.");
        int length = BinaryPrimitives.ReadInt32LittleEndian(values[position..]);
        if (length < 0 || length > values.Length - position - 4) throw new ParquetFormatException($"a BYTE_ARRAY value of {length} bytes runs past the page.");
        ReadOnlySpan<byte> value = values.Slice(position + 4, length);
        position += 4 + length;
        return value;
    }

    /// <summary>The PLAIN BYTE_ARRAY values of a dictionary page, copied: offsets (count + 1) and bytes.</summary>
    public static (int[] Offsets, byte[] Data) ByteArrayDictionary(ReadOnlySpan<byte> plain, int count, bool utf8)
    {
        if (count > plain.Length / 4) throw new ParquetFormatException($"a dictionary page of {plain.Length} bytes cannot hold {count} BYTE_ARRAY values.");
        var offsets = new int[count + 1];
        var data = new byte[Math.Max(0, plain.Length - 4 * count)];
        // One check for the page when all of it is ASCII (length prefixes of values under 128 bytes included).
        bool check = utf8 && !System.Text.Ascii.IsValid(plain);
        int position = 0, at = 0;
        for (int i = 0; i < count; i++)
        {
            ReadOnlySpan<byte> value = NextByteArray(plain, ref position);
            if (check && !Utf8.IsValid(value)) throw InvalidUtf8();
            // The room was sized by the declared count: values longer than that leaves mean fewer values than declared.
            if (value.Length > data.Length - at) throw FewerEntries(plain.Length, count);
            value.CopyTo(data.AsSpan(at));
            at += value.Length;
            offsets[i + 1] = at;
        }
        return (offsets, data);
    }

    public static ParquetFormatException InvalidUtf8() => new("a STRING value is not valid UTF-8.");

    public static ParquetFormatException FewerEntries(int bytes, int count) => new($"a dictionary page of {bytes} bytes holds fewer than its {count} BYTE_ARRAY values.");

    /// <summary>
    /// Reads <paramref name="starts"/>.Length PLAIN BYTE_ARRAY values from <paramref name="position"/>: their starts and
    /// lengths go to the spans; returns the total length and that of the values longer than <paramref name="inline"/>.
    /// </summary>
    public static (long Total, long Long) ParsePlain(ReadOnlySpan<byte> values, ref int position, Span<int> starts, Span<int> lengths, int inline)
    {
        long total = 0, longBytes = 0;
        int at = position;
        for (int j = 0; j < starts.Length; j++)
        {
            if (values.Length - at < 4) throw new ParquetFormatException("a BYTE_ARRAY value runs past the page.");
            int length = BinaryPrimitives.ReadInt32LittleEndian(values[at..]);
            if ((uint)length > (uint)(values.Length - at - 4)) throw new ParquetFormatException($"a BYTE_ARRAY value of {length} bytes runs past the page.");
            starts[j] = at + 4;
            lengths[j] = length;
            total += length;
            if (length > inline) longBytes += length;
            at += 4 + length;
        }
        position = at;
        return (total, longBytes);
    }

    /// <summary>
    /// Checks that each value is UTF-8: at once when the whole region they lie in (length prefixes included) is ASCII,
    /// as it is for text with values shorter than 128 bytes; value by value otherwise.
    /// </summary>
    public static void ValidateUtf8(ReadOnlySpan<byte> values, int from, int to, ReadOnlySpan<int> starts, ReadOnlySpan<int> lengths)
    {
        if (System.Text.Ascii.IsValid(values[from..to])) return;
        for (int j = 0; j < starts.Length; j++)
        {
            if (!Utf8.IsValid(values.Slice(starts[j], lengths[j]))) throw InvalidUtf8();
        }
    }
}

/// <summary>Pooled scratch for the starts and lengths of PLAIN BYTE_ARRAY values.</summary>
internal static class PlainScratch
{
    public static void Ensure(ref int[] starts, ref int[] lengths, int count)
    {
        if (starts.Length >= count) return;
        Return(ref starts, ref lengths);
        starts = ArrayPool<int>.Shared.Rent(count);
        lengths = ArrayPool<int>.Shared.Rent(count);
    }

    public static void Return(ref int[] starts, ref int[] lengths)
    {
        if (starts.Length > 0) ArrayPool<int>.Shared.Return(starts);
        if (lengths.Length > 0) ArrayPool<int>.Shared.Return(lengths);
        starts = lengths = [];
    }
}

/// <summary>Fixed-width Arrow values converted from fixed-width PLAIN values by <typeparamref name="TConvert"/>.</summary>
internal sealed class FixedLeafReader<T, TConvert>(PageReader pages, SchemaNode leaf, LeafType type, long valueCount)
    : LeafReader(pages, leaf, type, valueCount) where T : unmanaged where TConvert : IValueConverter<T>
{
    private readonly NativeFixedWidthBuilder<T> _builder = (NativeFixedWidthBuilder<T>)NativeColumnBuilder.Create(type.ArrowType);
    private T[] _dictionary = [];
    private int _dictionaryCount;

    public override NativeColumnBuilder Builder => _builder;
    protected override int DictionaryCount => _dictionaryCount;

    protected override void LoadDictionary(ReadOnlySpan<byte> plain, int count)
    {
        int width = TConvert.Width(Type);
        if (count > plain.Length / width) throw new ParquetFormatException($"a dictionary page of {plain.Length} bytes cannot hold {count} values of {width} bytes.");
        _dictionary = count == 0 ? [] : ArrayPool<T>.Shared.Rent(count);
        _dictionaryCount = count;
        TConvert.Convert(plain, _dictionary.AsSpan(0, count), Type);
    }

    protected override void AppendPlain(int rows, SlotMask mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        int bytes = valid * TConvert.Width(Type);
        if (bytes > values.Length - position) throw new ParquetFormatException($"the page holds fewer values than its {valid} present ones.");
        ReadOnlySpan<byte> plain = values.Slice(position, bytes);
        if (mask.IsEmpty)
        {
            // Output buffers outgrow the caches: written past them (non-temporal), they cost no read before the write.
            if (typeof(TConvert) == typeof(SameBits<T>)) NonTemporal.Copy(plain, MemoryMarshal.AsBytes(_builder.AppendSlots(rows)));
            else TConvert.Convert(plain, _builder.AppendSlots(rows), Type);
        }
        else if (typeof(TConvert) == typeof(SameBits<T>))
            MaskExpand.Scatter(MemoryMarshal.Cast<byte, T>(plain), _builder.AppendUninitializedSlots(mask), mask);
        else
        {
            Span<T> slots = _builder.AppendUninitializedSlots(mask);
            TConvert.Convert(plain, slots[..valid], Type);
            MaskExpand.InPlace(slots, valid, mask);
        }
        position += bytes;
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        int bytes = valid * TConvert.Width(Type);
        if (bytes > values.Length - position) throw new ParquetFormatException($"the page holds fewer values than its {valid} present ones.");
        position += bytes;
    }

    protected override void AppendFromDictionary(int rows, SlotMask mask, ReadOnlySpan<int> indices)
    {
        // The indices were checked against the dictionary when the page was entered.
        if (!mask.IsEmpty)
        {
            MaskExpand.Gather(_dictionary.AsSpan(0, _dictionaryCount), indices, _builder.AppendUninitializedSlots(mask), mask);
            return;
        }
        NonTemporal.Gather<T>(_dictionary.AsSpan(0, _dictionaryCount), indices, _builder.AppendSlots(rows));
    }

    public override void Dispose()
    {
        if (_dictionary.Length > 0) ArrayPool<T>.Shared.Return(_dictionary);
        _dictionary = [];
        base.Dispose();
    }
}

/// <summary>
/// Booleans: PLAIN (bit-packed, LSB first), RLE (a 4-byte length, then the hybrid at bit width 1), or a dictionary.
/// PLAIN and RLE values stay bits: appended as they are, or laid over the valid slots when there are nulls.
/// </summary>
internal sealed class BooleanLeafReader(PageReader pages, SchemaNode leaf, LeafType type, long valueCount)
    : LeafReader(pages, leaf, type, valueCount)
{
    private readonly NativeBooleanBuilder _builder = new();
    private byte[] _pageBits = [];   // an RLE page's present values, as bits
    private int _at;                 // the next of them
    private ulong[] _slotBits = [];  // values laid over a run of slots
    private byte[] _dictionary = [];
    private byte[] _page = [];       // dictionary values of a run, one byte each
    private byte[] _expanded = [];

    public override NativeColumnBuilder Builder => _builder;
    protected override int DictionaryCount => _dictionary.Length;

    protected override void LoadDictionary(ReadOnlySpan<byte> plain, int count)
    {
        if (count > plain.Length * 8L) throw new ParquetFormatException($"a dictionary page of {plain.Length} bytes cannot hold {count} booleans.");
        _dictionary = new byte[count];
        BitUnpacking.Unpack(plain, 0, 1, _dictionary.AsSpan());
    }

    protected override bool BeginOtherEncoding(ParquetEncoding encoding, ReadOnlySpan<byte> values, int valid)
    {
        if (encoding != ParquetEncoding.Rle) return false;
        if (values.Length < 4) throw new ParquetFormatException("an RLE boolean page is too short for its length.");
        int length = BinaryPrimitives.ReadInt32LittleEndian(values);
        if (length < 0 || length > values.Length - 4) throw new ParquetFormatException($"RLE booleans of {length} bytes run past the page.");
        // Slack after the bits: they are read 8 bytes at a time (Bits.Word's fast path).
        Ensure(ref _pageBits, (valid + 7) / 8 + 8);
        if (RleBitPackedDecoder.ReadAllBits(values.Slice(4, length), _pageBits, valid, out _) != valid)
            throw new ParquetFormatException($"the page has fewer booleans than its {valid} present values.");
        _at = 0;
        return true;
    }

    protected override void AppendOther(int rows, SlotMask mask, int valid)
    {
        AppendBits(rows, mask, _pageBits, _at);
        _at += valid;
    }

    protected override void SkipOther(int valid) => _at += valid;

    protected override void AppendPlain(int rows, SlotMask mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        // position counts bits here: PLAIN booleans are bit-packed.
        if (BitUnpacking.Bytes((long)position + valid, 1) > values.Length) throw new ParquetFormatException($"the page holds fewer booleans than its {valid} present ones.");
        AppendBits(rows, mask, values, position);
        position += valid;
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        if (BitUnpacking.Bytes((long)position + valid, 1) > values.Length) throw new ParquetFormatException($"the page holds fewer booleans than its {valid} present ones.");
        position += valid;
    }

    /// <summary>Appends <paramref name="rows"/> slots whose valid ones take the bits from <paramref name="first"/> on (already an LSB-first bitmap).</summary>
    private void AppendBits(int rows, SlotMask mask, ReadOnlySpan<byte> bits, long first)
    {
        if (mask.IsEmpty)
        {
            _builder.AppendBits(bits, first, rows);
            return;
        }
        int words = (rows + 63) / 64;
        if (_slotBits.Length < words)
        {
            if (_slotBits.Length > 0) ArrayPool<ulong>.Shared.Return(_slotBits);
            _slotBits = ArrayPool<ulong>.Shared.Rent(words);
        }
        Span<ulong> slots = _slotBits.AsSpan(0, words);
        MaskExpand.Bits(bits, first, slots, mask);
        _builder.AppendBits(MemoryMarshal.AsBytes(slots), 0, mask);
    }

    protected override void AppendFromDictionary(int rows, SlotMask mask, ReadOnlySpan<int> indices)
    {
        if (!mask.IsEmpty)
        {
            Ensure(ref _expanded, rows);
            Span<byte> values = _expanded.AsSpan(0, rows);
            MaskExpand.Gather(_dictionary, indices, values, mask);
            _builder.AppendValues(values, mask);
            return;
        }
        Ensure(ref _page, indices.Length);
        for (int i = 0; i < indices.Length; i++) _page[i] = _dictionary[indices[i]];
        _builder.AppendValues(_page.AsSpan(0, indices.Length));
    }

    private static void Ensure(ref byte[] buffer, int length)
    {
        if (buffer.Length >= length) return;
        if (buffer.Length > 0) ArrayPool<byte>.Shared.Return(buffer);
        buffer = ArrayPool<byte>.Shared.Rent(length);
    }

    public override void Dispose()
    {
        foreach (byte[] buffer in (byte[][])[_page, _expanded, _pageBits])
        {
            if (buffer.Length > 0) ArrayPool<byte>.Shared.Return(buffer);
        }
        if (_slotBits.Length > 0) ArrayPool<ulong>.Shared.Return(_slotBits);
        _page = _expanded = _pageBits = [];
        _slotBits = [];
        base.Dispose();
    }
}

/// <summary>BYTE_ARRAY values as Utf8 (validated) or Binary.</summary>
internal sealed class BinaryLeafReader(PageReader pages, SchemaNode leaf, LeafType type, long valueCount)
    : LeafReader(pages, leaf, type, valueCount)
{
    private readonly NativeBinaryBuilder _builder = new(type.ArrowType);
    private readonly bool _utf8 = type.Kind == ValueKind.Utf8;
    private int[] _offsets = [0];
    private byte[] _data = [];

    public override NativeColumnBuilder Builder => _builder;
    protected override int DictionaryCount => _offsets.Length - 1;

    protected override void LoadDictionary(ReadOnlySpan<byte> plain, int count) =>
        (_offsets, _data) = LeafReaders.ByteArrayDictionary(plain, count, _utf8);

    // Dictionary-encoded: the average entry; PLAIN: the chunk's bytes per value (lengths and headers included).
    protected override long DataBytes(long slots) => DictionaryCount > 0
        ? (long)Math.Min(Math.Ceiling((double)_data.Length * slots / DictionaryCount), 1L << 30)
        : ChunkBytesPerValue(slots);

    private int[] _starts = [], _lengths = [];

    protected override void AppendPlain(int rows, SlotMask mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        PlainScratch.Ensure(ref _starts, ref _lengths, valid);
        int from = position;
        var (total, _) = LeafReaders.ParsePlain(values, ref position, _starts.AsSpan(0, valid), _lengths.AsSpan(0, valid), int.MaxValue);
        if (_utf8) LeafReaders.ValidateUtf8(values, from, position, _starts.AsSpan(0, valid), _lengths.AsSpan(0, valid));
        _builder.AppendValues(values, _starts.AsSpan(0, valid), _lengths.AsSpan(0, valid), MaskBytes(mask), total);
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        for (int i = 0; i < valid; i++) LeafReaders.NextByteArray(values, ref position);
    }

    public override void Dispose()
    {
        PlainScratch.Return(ref _starts, ref _lengths);
        base.Dispose();
    }

    protected override void AppendFromDictionary(int rows, SlotMask mask, ReadOnlySpan<int> indices)
    {
        for (int i = 0, j = 0; i < rows; i++)
        {
            if (!mask.IsEmpty && !mask.IsValid(i))
            {
                _builder.AppendNull();
                continue;
            }
            int index = indices[j++];
            _builder.Append(_data.AsSpan(_offsets[index], _offsets[index + 1] - _offsets[index]));
        }
    }
}

/// <summary>
/// BYTE_ARRAY values as Utf8View (validated) or BinaryView. A dictionary-encoded page costs a 16-byte copy per value:
/// the entries' views are made once per dictionary, over the dictionary's bytes added to each batch as one buffer.
/// </summary>
internal sealed class BinaryViewLeafReader(PageReader pages, SchemaNode leaf, LeafType type, long valueCount)
    : LeafReader(pages, leaf, type, valueCount)
{
    private readonly NativeBinaryViewBuilder _builder = new(type.ArrowType);
    private readonly bool _utf8 = type.Kind == ValueKind.Utf8;
    private byte[] _data = [];          // the bytes of the dictionary's entries that are not inline (pooled)
    private int _dataLength;
    private Int128[] _views = [];       // the dictionary's entries, the long ones pointing into buffer 0 (pooled)
    private int _viewCount;
    private int _batchBuffer = -1;      // the index of the dictionary's bytes in the current batch

    public override NativeColumnBuilder Builder => _builder;
    protected override int DictionaryCount => _viewCount;

    /// <summary>
    /// The entries' views, made in one pass over the page: short values inline, long ones pointing into the copied
    /// bytes of the long values only (a batch that uses the dictionary gets those bytes as a buffer).
    /// </summary>
    protected override void LoadDictionary(ReadOnlySpan<byte> plain, int count)
    {
        if (count > plain.Length / 4) throw new ParquetFormatException($"a dictionary page of {plain.Length} bytes cannot hold {count} BYTE_ARRAY values.");
        ReturnDictionary();
        _views = count == 0 ? [] : ArrayPool<Int128>.Shared.Rent(count);
        int bytes = plain.Length - 4 * count;
        _data = bytes == 0 ? [] : ArrayPool<byte>.Shared.Rent(bytes);
        // One check for the page when all of it is ASCII (length prefixes of values under 128 bytes included).
        bool check = _utf8 && !System.Text.Ascii.IsValid(plain);
        int position = 0, at = 0;
        for (int i = 0; i < count; i++)
        {
            ReadOnlySpan<byte> value = LeafReaders.NextByteArray(plain, ref position);
            if (check && !Utf8.IsValid(value)) throw LeafReaders.InvalidUtf8();
            _views[i] = NativeBinaryViewBuilder.View(value, 0, at);
            if (value.Length <= NativeBinaryViewBuilder.MaxInline) continue;
            if (value.Length > bytes - at) throw LeafReaders.FewerEntries(plain.Length, count);
            value.CopyTo(_data.AsSpan(at));
            at += value.Length;
        }
        _viewCount = count;
        _dataLength = at;
        _batchBuffer = -1;
    }

    private void ReturnDictionary()
    {
        if (_views.Length > 0) ArrayPool<Int128>.Shared.Return(_views);
        if (_data.Length > 0) ArrayPool<byte>.Shared.Return(_data);
        _views = [];
        _data = [];
        _viewCount = _dataLength = 0;
    }

    protected override void OnBatchExported() => _batchBuffer = -1;

    // Dictionary pages copy no values; PLAIN ones about the chunk's bytes per value.
    protected override long DataBytes(long slots) => DictionaryCount > 0 ? 0 : ChunkBytesPerValue(slots);

    private int[] _starts = [], _lengths = [];

    protected override void AppendPlain(int rows, SlotMask mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        PlainScratch.Ensure(ref _starts, ref _lengths, valid);
        int from = position;
        var (_, longBytes) = LeafReaders.ParsePlain(values, ref position, _starts.AsSpan(0, valid), _lengths.AsSpan(0, valid), NativeBinaryViewBuilder.MaxInline);
        if (_utf8) LeafReaders.ValidateUtf8(values, from, position, _starts.AsSpan(0, valid), _lengths.AsSpan(0, valid));
        _builder.AppendValues(values, _starts.AsSpan(0, valid), _lengths.AsSpan(0, valid), mask, longBytes);
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        for (int i = 0; i < valid; i++) LeafReaders.NextByteArray(values, ref position);
    }

    public override void Dispose()
    {
        PlainScratch.Return(ref _starts, ref _lengths);
        ReturnDictionary();
        base.Dispose();
    }

    protected override void AppendFromDictionary(int rows, SlotMask mask, ReadOnlySpan<int> indices)
    {
        if (_dataLength > 0 && _batchBuffer < 0) _batchBuffer = _builder.AddBuffer(_data.AsSpan(0, _dataLength));
        // The indices were checked against the dictionary when the page was entered.
        Span<Int128> slots;
        if (mask.IsEmpty)
        {
            slots = _builder.AppendViewSlots(rows);
            NonTemporal.Gather<Int128>(_views, indices, slots);
        }
        else
        {
            slots = _builder.AppendUninitializedViewSlots(mask);
            MaskExpand.Gather(_views, indices, slots, mask);
        }
        if (_batchBuffer > 0)
        {
            // The views point into buffer 0; this batch holds the dictionary elsewhere (null views are empty).
            Span<int> words = MemoryMarshal.Cast<Int128, int>(slots);
            for (int i = 0; i < words.Length; i += 4)
            {
                if (words[i] > NativeBinaryViewBuilder.MaxInline) words[i + 2] = _batchBuffer;
            }
        }
    }
}

/// <summary>
/// BYTE_ARRAY values output dictionary-encoded (the file's Arrow schema marks the column so): each batch gets its
/// own dictionary of distinct values, in first-seen order.
/// </summary>
internal sealed class DictionaryOutputLeafReader(PageReader pages, SchemaNode leaf, LeafType type, long valueCount)
    : LeafReader(pages, leaf, type, valueCount)
{
    private readonly NativeDictionaryBuilder _builder = new((DictionaryType)type.ArrowType);
    private readonly bool _utf8 = type.Kind == ValueKind.Utf8;
    private readonly Dictionary<byte[], int> _batchEntries = new(ByteArrayComparer.Instance);
    private int[] _offsets = [0];
    private byte[] _data = [];
    private int[] _fileToBatch = [];  // dictionary page entry → batch entry, -1 until seen in this batch

    public override NativeColumnBuilder Builder => _builder;
    protected override int DictionaryCount => _offsets.Length - 1;

    protected override void LoadDictionary(ReadOnlySpan<byte> plain, int count)
    {
        (_offsets, _data) = LeafReaders.ByteArrayDictionary(plain, count, _utf8);
        _fileToBatch = new int[count];
        _fileToBatch.AsSpan().Fill(-1);
    }

    protected override void OnBatchExported()
    {
        _batchEntries.Clear();
        _fileToBatch.AsSpan().Fill(-1);
    }

    private int Entry(ReadOnlySpan<byte> value)
    {
        byte[] key = value.ToArray();
        if (_batchEntries.TryGetValue(key, out int index)) return index;
        index = _batchEntries.Count;
        _batchEntries.Add(key, index);
        _builder.Entries.Append(value);
        return index;
    }

    protected override void AppendPlain(int rows, SlotMask mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        ReadOnlySpan<byte> present = mask.IsEmpty ? Ones(rows) : MaskBytes(mask);
        Span<int> slots = _builder.AppendSlots(present);
        for (int i = 0; i < rows; i++)
        {
            if (present[i] == 0) continue;
            ReadOnlySpan<byte> value = LeafReaders.NextByteArray(values, ref position);
            if (_utf8 && !Utf8.IsValid(value)) throw LeafReaders.InvalidUtf8();
            slots[i] = Entry(value);
        }
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        for (int i = 0; i < valid; i++) LeafReaders.NextByteArray(values, ref position);
    }

    protected override void AppendFromDictionary(int rows, SlotMask mask, ReadOnlySpan<int> indices)
    {
        ReadOnlySpan<byte> present = mask.IsEmpty ? Ones(rows) : MaskBytes(mask);
        Span<int> slots = _builder.AppendSlots(present);
        for (int i = 0, j = 0; i < rows; i++)
        {
            if (present[i] == 0) continue;
            int file = indices[j++];
            int batch = _fileToBatch[file];
            if (batch < 0) _fileToBatch[file] = batch = Entry(_data.AsSpan(_offsets[file], _offsets[file + 1] - _offsets[file]));
            slots[i] = batch;
        }
    }

    private byte[] _ones = [];

    private ReadOnlySpan<byte> Ones(int rows)
    {
        if (_ones.Length < rows)
        {
            _ones = new byte[rows];
            _ones.AsSpan().Fill(1);
        }
        return _ones.AsSpan(0, rows);
    }

    private sealed class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();
        public bool Equals(byte[]? x, byte[]? y) => x.AsSpan().SequenceEqual(y);
        public int GetHashCode(byte[] obj) => (int)System.IO.Hashing.XxHash3.HashToUInt64(obj);
    }
}

/// <summary>FIXED_LEN_BYTE_ARRAY values as FixedSizeBinary.</summary>
internal sealed class FixedBinaryLeafReader(PageReader pages, SchemaNode leaf, LeafType type, long valueCount)
    : LeafReader(pages, leaf, type, valueCount)
{
    private readonly NativeFixedSizeBinaryBuilder _builder = new((FixedSizeBinaryType)type.ArrowType);
    private readonly int _width = type.TypeLength;
    private byte[] _dictionary = [];

    public override NativeColumnBuilder Builder => _builder;
    protected override int DictionaryCount => _dictionary.Length / _width;

    protected override void LoadDictionary(ReadOnlySpan<byte> plain, int count)
    {
        if (count > plain.Length / _width) throw new ParquetFormatException($"a dictionary page of {plain.Length} bytes cannot hold {count} values of {_width} bytes.");
        _dictionary = plain[..(count * _width)].ToArray();
    }

    protected override void AppendPlain(int rows, SlotMask mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        int bytes = valid * _width;
        if (bytes > values.Length - position) throw new ParquetFormatException($"the page holds fewer values than its {valid} present ones.");
        ReadOnlySpan<byte> present = MaskBytes(mask);
        Span<byte> slots = _builder.AppendSlots(rows, present);
        values.Slice(position, bytes).CopyTo(slots);
        position += bytes;
        SpreadBytes(slots, valid, present);
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        int bytes = valid * _width;
        if (bytes > values.Length - position) throw new ParquetFormatException($"the page holds fewer values than its {valid} present ones.");
        position += bytes;
    }

    protected override void AppendFromDictionary(int rows, SlotMask mask, ReadOnlySpan<int> indices)
    {
        ReadOnlySpan<byte> present = MaskBytes(mask);
        Span<byte> slots = _builder.AppendSlots(rows, present);
        for (int i = 0; i < indices.Length; i++) _dictionary.AsSpan(indices[i] * _width, _width).CopyTo(slots[(i * _width)..]);
        SpreadBytes(slots, indices.Length, present);
    }

    private void SpreadBytes(Span<byte> slots, int valid, ReadOnlySpan<byte> mask)
    {
        if (mask.IsEmpty || valid == mask.Length) return;
        int j = valid - 1;
        for (int i = mask.Length - 1; i >= 0; i--)
        {
            Span<byte> slot = slots.Slice(i * _width, _width);
            if (mask[i] != 0) slots.Slice(j-- * _width, _width).CopyTo(slot);
            else slot.Clear();
        }
    }
}

/// <summary>DECIMAL in BYTE_ARRAY (variable-length big-endian two's complement).</summary>
internal sealed class ByteArrayDecimalLeafReader<T>(PageReader pages, SchemaNode leaf, LeafType type, long valueCount, ByteArrayDecimalLeafReader<T>.Converter convert)
    : LeafReader(pages, leaf, type, valueCount) where T : unmanaged
{
    public delegate T Converter(ReadOnlySpan<byte> bigEndian, LeafType type);

    private readonly NativeFixedWidthBuilder<T> _builder = (NativeFixedWidthBuilder<T>)NativeColumnBuilder.Create(type.ArrowType);
    private T[] _dictionary = [];

    public override NativeColumnBuilder Builder => _builder;
    protected override int DictionaryCount => _dictionary.Length;

    protected override void LoadDictionary(ReadOnlySpan<byte> plain, int count)
    {
        if (count > plain.Length / 4) throw new ParquetFormatException($"a dictionary page of {plain.Length} bytes cannot hold {count} BYTE_ARRAY values.");
        _dictionary = new T[count];
        int position = 0;
        for (int i = 0; i < count; i++) _dictionary[i] = convert(LeafReaders.NextByteArray(plain, ref position), Type);
    }

    protected override void AppendPlain(int rows, SlotMask mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        ReadOnlySpan<byte> present = MaskBytes(mask);
        Span<T> slots = mask.IsEmpty ? _builder.AppendSlots(rows) : _builder.AppendSlots(present);
        for (int i = 0; i < rows; i++)
        {
            if (!mask.IsEmpty && present[i] == 0) continue;
            slots[i] = convert(LeafReaders.NextByteArray(values, ref position), Type);
        }
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        for (int i = 0; i < valid; i++) LeafReaders.NextByteArray(values, ref position);
    }

    protected override void AppendFromDictionary(int rows, SlotMask mask, ReadOnlySpan<int> indices)
    {
        if (!mask.IsEmpty)
        {
            MaskExpand.Gather(_dictionary, indices, _builder.AppendUninitializedSlots(mask), mask);
            return;
        }
        Span<T> slots = _builder.AppendSlots(rows);
        for (int i = 0; i < indices.Length; i++) slots[i] = _dictionary[indices[i]];
    }
}

/// <summary>The UNKNOWN annotation: always null; levels are read, values ignored.</summary>
internal sealed class NullLeafReader(PageReader pages, SchemaNode leaf, LeafType type, long valueCount)
    : LeafReader(pages, leaf, type, valueCount)
{
    private readonly NativeNullBuilder _builder = new();

    public override NativeColumnBuilder Builder => _builder;
    protected override int DictionaryCount => int.MaxValue;
    protected override void LoadDictionary(ReadOnlySpan<byte> plain, int count) { }
    protected override bool BeginOtherEncoding(ParquetEncoding encoding, ReadOnlySpan<byte> values, int valid) => true;

    protected override void AppendPlain(int rows, SlotMask mask, int valid, ReadOnlySpan<byte> values, ref int position) => AppendOther(rows, mask, valid);

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position) { }

    protected override void AppendFromDictionary(int rows, SlotMask mask, ReadOnlySpan<int> indices) => AppendOther(rows, mask, indices.Length);

    protected override void AppendOther(int rows, SlotMask mask, int valid)
    {
        for (int i = 0; i < rows; i++) _builder.AppendNull();
    }

    protected override void SkipOther(int valid) { }
}
