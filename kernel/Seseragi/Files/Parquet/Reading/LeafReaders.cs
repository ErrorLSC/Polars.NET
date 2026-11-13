using System.Buffers;
using System.Buffers.Binary;
using System.Text.Unicode;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
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
        int position = 0, at = 0;
        for (int i = 0; i < count; i++)
        {
            ReadOnlySpan<byte> value = NextByteArray(plain, ref position);
            if (utf8 && !Utf8.IsValid(value)) throw InvalidUtf8();
            value.CopyTo(data.AsSpan(at));
            at += value.Length;
            offsets[i + 1] = at;
        }
        return (offsets, data);
    }

    public static ParquetFormatException InvalidUtf8() => new("a STRING value is not valid UTF-8.");
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

    protected override void AppendPlain(int rows, ReadOnlySpan<byte> mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        int bytes = valid * TConvert.Width(Type);
        if (bytes > values.Length - position) throw new ParquetFormatException($"the page holds fewer values than its {valid} present ones.");
        Span<T> slots = mask.IsEmpty ? _builder.AppendSlots(rows) : _builder.AppendSlots(mask);
        TConvert.Convert(values.Slice(position, bytes), slots[..valid], Type);
        position += bytes;
        Spread(slots, valid, mask);
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        int bytes = valid * TConvert.Width(Type);
        if (bytes > values.Length - position) throw new ParquetFormatException($"the page holds fewer values than its {valid} present ones.");
        position += bytes;
    }

    protected override void AppendFromDictionary(int rows, ReadOnlySpan<byte> mask, ReadOnlySpan<int> indices)
    {
        Span<T> slots = mask.IsEmpty ? _builder.AppendSlots(rows) : _builder.AppendSlots(mask);
        ReadOnlySpan<T> dictionary = _dictionary;
        for (int i = 0; i < indices.Length; i++) slots[i] = dictionary[indices[i]];
        Spread(slots, indices.Length, mask);
    }

    public override void Dispose()
    {
        if (_dictionary.Length > 0) ArrayPool<T>.Shared.Return(_dictionary);
        _dictionary = [];
        base.Dispose();
    }
}

/// <summary>Booleans: PLAIN (bit-packed, LSB first), RLE (a 4-byte length, then the hybrid at bit width 1), or a dictionary.</summary>
internal sealed class BooleanLeafReader(PageReader pages, SchemaNode leaf, LeafType type, long valueCount)
    : LeafReader(pages, leaf, type, valueCount)
{
    private readonly NativeBooleanBuilder _builder = new();
    private byte[] _page = [];   // the page's present values, one byte each
    private int _at;
    private byte[] _dictionary = [];
    private byte[] _expanded = [];

    public override NativeColumnBuilder Builder => _builder;
    protected override int DictionaryCount => _dictionary.Length;

    protected override void LoadDictionary(ReadOnlySpan<byte> plain, int count)
    {
        if (count > plain.Length * 8L) throw new ParquetFormatException($"a dictionary page of {plain.Length} bytes cannot hold {count} booleans.");
        _dictionary = new byte[count];
        BitPacking.Unpack(plain, 0, 1, _dictionary.AsSpan());
    }

    protected override bool BeginOtherEncoding(ParquetEncoding encoding, ReadOnlySpan<byte> values, int valid)
    {
        if (encoding != ParquetEncoding.Rle) return false;
        if (values.Length < 4) throw new ParquetFormatException("an RLE boolean page is too short for its length.");
        int length = BinaryPrimitives.ReadInt32LittleEndian(values);
        if (length < 0 || length > values.Length - 4) throw new ParquetFormatException($"RLE booleans of {length} bytes run past the page.");
        Ensure(ref _page, valid);
        var decoder = new RleBitPackedDecoder(values.Slice(4, length), 1);
        if (decoder.Read(_page.AsSpan(0, valid)) != valid) throw new ParquetFormatException($"the page has fewer booleans than its {valid} present values.");
        _at = 0;
        return true;
    }

    protected override void AppendOther(int rows, ReadOnlySpan<byte> mask, int valid)
    {
        Append(rows, mask, _page.AsSpan(_at, valid));
        _at += valid;
    }

    protected override void SkipOther(int valid) => _at += valid;

    protected override void AppendPlain(int rows, ReadOnlySpan<byte> mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        // position counts bits here: PLAIN booleans are bit-packed.
        if (BitPacking.Bytes((long)position + valid, 1) > values.Length) throw new ParquetFormatException($"the page holds fewer booleans than its {valid} present ones.");
        Ensure(ref _page, valid);
        BitPacking.Unpack(values, position, 1, _page.AsSpan(0, valid));
        position += valid;
        Append(rows, mask, _page.AsSpan(0, valid));
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        if (BitPacking.Bytes((long)position + valid, 1) > values.Length) throw new ParquetFormatException($"the page holds fewer booleans than its {valid} present ones.");
        position += valid;
    }

    protected override void AppendFromDictionary(int rows, ReadOnlySpan<byte> mask, ReadOnlySpan<int> indices)
    {
        Ensure(ref _page, indices.Length);
        for (int i = 0; i < indices.Length; i++) _page[i] = _dictionary[indices[i]];
        Append(rows, mask, _page.AsSpan(0, indices.Length));
    }

    private void Append(int rows, ReadOnlySpan<byte> mask, ReadOnlySpan<byte> present)
    {
        if (mask.IsEmpty)
        {
            _builder.AppendValues(present);
            return;
        }
        Ensure(ref _expanded, rows);
        Span<byte> values = _expanded.AsSpan(0, rows);
        for (int i = 0, j = 0; i < rows; i++) values[i] = mask[i] != 0 ? present[j++] : (byte)0;
        _builder.AppendValues(values, mask);
    }

    private static void Ensure(ref byte[] buffer, int length)
    {
        if (buffer.Length >= length) return;
        if (buffer.Length > 0) ArrayPool<byte>.Shared.Return(buffer);
        buffer = ArrayPool<byte>.Shared.Rent(length);
    }

    public override void Dispose()
    {
        if (_page.Length > 0) ArrayPool<byte>.Shared.Return(_page);
        if (_expanded.Length > 0) ArrayPool<byte>.Shared.Return(_expanded);
        _page = _expanded = [];
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

    protected override void AppendPlain(int rows, ReadOnlySpan<byte> mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        for (int i = 0; i < rows; i++)
        {
            if (!mask.IsEmpty && mask[i] == 0)
            {
                _builder.AppendNull();
                continue;
            }
            ReadOnlySpan<byte> value = LeafReaders.NextByteArray(values, ref position);
            if (_utf8 && !Utf8.IsValid(value)) throw LeafReaders.InvalidUtf8();
            _builder.Append(value);
        }
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        for (int i = 0; i < valid; i++) LeafReaders.NextByteArray(values, ref position);
    }

    protected override void AppendFromDictionary(int rows, ReadOnlySpan<byte> mask, ReadOnlySpan<int> indices)
    {
        for (int i = 0, j = 0; i < rows; i++)
        {
            if (!mask.IsEmpty && mask[i] == 0)
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

    public override void BatchExported()
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

    protected override void AppendPlain(int rows, ReadOnlySpan<byte> mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        Span<int> slots = _builder.AppendSlots(mask.IsEmpty ? Ones(rows) : mask);
        for (int i = 0; i < rows; i++)
        {
            if (!mask.IsEmpty && mask[i] == 0) continue;
            ReadOnlySpan<byte> value = LeafReaders.NextByteArray(values, ref position);
            if (_utf8 && !Utf8.IsValid(value)) throw LeafReaders.InvalidUtf8();
            slots[i] = Entry(value);
        }
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        for (int i = 0; i < valid; i++) LeafReaders.NextByteArray(values, ref position);
    }

    protected override void AppendFromDictionary(int rows, ReadOnlySpan<byte> mask, ReadOnlySpan<int> indices)
    {
        Span<int> slots = _builder.AppendSlots(mask.IsEmpty ? Ones(rows) : mask);
        for (int i = 0, j = 0; i < rows; i++)
        {
            if (!mask.IsEmpty && mask[i] == 0) continue;
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

    protected override void AppendPlain(int rows, ReadOnlySpan<byte> mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        int bytes = valid * _width;
        if (bytes > values.Length - position) throw new ParquetFormatException($"the page holds fewer values than its {valid} present ones.");
        Span<byte> slots = _builder.AppendSlots(rows, mask);
        values.Slice(position, bytes).CopyTo(slots);
        position += bytes;
        SpreadBytes(slots, valid, mask);
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        int bytes = valid * _width;
        if (bytes > values.Length - position) throw new ParquetFormatException($"the page holds fewer values than its {valid} present ones.");
        position += bytes;
    }

    protected override void AppendFromDictionary(int rows, ReadOnlySpan<byte> mask, ReadOnlySpan<int> indices)
    {
        Span<byte> slots = _builder.AppendSlots(rows, mask);
        for (int i = 0; i < indices.Length; i++) _dictionary.AsSpan(indices[i] * _width, _width).CopyTo(slots[(i * _width)..]);
        SpreadBytes(slots, indices.Length, mask);
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

    protected override void AppendPlain(int rows, ReadOnlySpan<byte> mask, int valid, ReadOnlySpan<byte> values, ref int position)
    {
        Span<T> slots = mask.IsEmpty ? _builder.AppendSlots(rows) : _builder.AppendSlots(mask);
        for (int i = 0; i < rows; i++)
        {
            if (!mask.IsEmpty && mask[i] == 0) continue;
            slots[i] = convert(LeafReaders.NextByteArray(values, ref position), Type);
        }
    }

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position)
    {
        for (int i = 0; i < valid; i++) LeafReaders.NextByteArray(values, ref position);
    }

    protected override void AppendFromDictionary(int rows, ReadOnlySpan<byte> mask, ReadOnlySpan<int> indices)
    {
        Span<T> slots = mask.IsEmpty ? _builder.AppendSlots(rows) : _builder.AppendSlots(mask);
        for (int i = 0; i < indices.Length; i++) slots[i] = _dictionary[indices[i]];
        Spread(slots, indices.Length, mask);
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

    protected override void AppendPlain(int rows, ReadOnlySpan<byte> mask, int valid, ReadOnlySpan<byte> values, ref int position) => AppendOther(rows, mask, valid);

    protected override void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position) { }

    protected override void AppendFromDictionary(int rows, ReadOnlySpan<byte> mask, ReadOnlySpan<int> indices) => AppendOther(rows, mask, indices.Length);

    protected override void AppendOther(int rows, ReadOnlySpan<byte> mask, int valid)
    {
        for (int i = 0; i < rows; i++) _builder.AppendNull();
    }

    protected override void SkipOther(int valid) { }
}
