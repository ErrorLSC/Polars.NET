using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;
using Kaminari.Sazanami.Arrow;
using Kaminari.Sazanami.Encodings;
using Kaminari.Sazanami.Helpers;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet.Reading;

/// <summary>
/// Reads one column chunk into an Arrow builder, a batch of records at a time. The base class walks the pages,
/// decodes repetition and definition levels, counts records (a record starts at repetition level 0), keeps the
/// batch's levels for the assembler of nested columns, and works out which entries are slots of the leaf's array
/// and which of those are null; the kinds below decode the values. A page's levels (and dictionary indices) are
/// decoded whole when the page is entered, so records can be read or skipped from anywhere in it.
/// </summary>
internal abstract class LeafReader : IDisposable
{
    private readonly PageReader _pages;
    private readonly int _maxDefinition, _maxRepetition;
    private readonly int _definitionBits, _repetitionBits;
    private long _valuesLeft;           // in the chunk, from its metadata
    private readonly long _chunkValues;

    // How entries become slots of this leaf's array (see ReadNode); a flat leaf: every entry.
    private int _slotDef;
    private bool _rejectNulls;
    private int _fixedSlotDef = -1, _fixedElementDef, _fixedSize; // a fixed-size list parent: placeholders for null lists
    private bool _keepLevels;
    private bool _bitLevels; // a flat optional leaf: the page's definition levels are kept as validity bits (_defBits)

    private byte[] _def = [], _rep = [];   // the page's levels
    private byte[] _defBits = [];          // the page's definition levels as an LSB-first bitmap (with slack for word reads)
    private int _defBitsSet;               // how many of them are 1
    private byte[] _maskBytes = [];        // a bit mask unpacked for the kinds that take bytes
    private int _pageValues, _pageAt;
    private ReadOnlyMemory<byte> _values;
    private int _valuePosition;
    private bool _fromDictionary, _other, _hasDictionary;
    private int[] _indices = [];
    private int _indexAt;

    private NativeByteBuffer? _transcoded;    // the current page's values decoded into PLAIN layout (other encodings)
    private ulong[]? _deltaScratch;
    private int[] _lengths = [], _prefixes = [];  // a page's byte array lengths and prefix lengths (delta encodings)

    private byte[] _batchDef = [], _batchRep = [];
    private int _batchCount;
    private byte[] _mask = [];

    protected LeafReader(PageReader pages, SchemaNode leaf, LeafType type, long valueCount)
    {
        _pages = pages;
        Leaf = leaf;
        Type = type;
        _maxDefinition = leaf.MaxDefinition;
        _maxRepetition = leaf.MaxRepetition;
        _definitionBits = 32 - BitOperations.LeadingZeroCount((uint)_maxDefinition);
        _repetitionBits = 32 - BitOperations.LeadingZeroCount((uint)_maxRepetition);
        _valuesLeft = valueCount;
        _chunkValues = valueCount;
    }

    /// <summary>Whether a flat optional leaf may keep its definition levels as validity bits (else as bytes).</summary>
    public bool LevelsAsBits { get; set; } = true;

    /// <summary>The column chunk's uncompressed size (its metadata), for estimating value bytes per slot.</summary>
    public long UncompressedBytes { get; set; }

    /// <summary>
    /// Lets the builder grow once for the batch about to be read: <paramref name="records"/> records out of the row
    /// group's <paramref name="rowGroupRows"/> (for a repeated leaf, its share of the chunk's values).
    /// </summary>
    public void Reserve(int records, long rowGroupRows)
    {
        // Hints from the footer, which nothing bounds: capped, since a hint too small only means growing later.
        long slots = _maxRepetition == 0
            ? records
            : (long)Math.Min(Math.Ceiling((double)_chunkValues * records / Math.Max(1, rowGroupRows)), records * 16.0);
        Builder.Reserve(slots, DataBytes(slots));
    }

    /// <summary>About how many value bytes <paramref name="slots"/> slots take (variable-length values only; at most 1 GiB).</summary>
    protected virtual long DataBytes(long slots) => 0;

    /// <summary>The chunk's uncompressed bytes per value: an estimate of the bytes of PLAIN variable-length values.</summary>
    protected long ChunkBytesPerValue(long slots) =>
        _chunkValues <= 0 || UncompressedBytes <= 0 ? 0 : (long)Math.Min(Math.Ceiling((double)UncompressedBytes * slots / _chunkValues), 1L << 30);

    public SchemaNode Leaf { get; }
    public LeafType Type { get; }
    public abstract NativeColumnBuilder Builder { get; }
    protected string Name => string.Join('.', Leaf.Path);

    /// <summary>
    /// Sets how entries become slots (from the leaf's read node), whether the batch's levels are kept for the
    /// assembler, and the fixed-size list the leaf is the item of, if any.
    /// </summary>
    public void Configure(ReadNode node, bool keepLevels, ReadNode? fixedSizeList)
    {
        _slotDef = node.SlotDef;
        _rejectNulls = node.RejectNulls;
        _keepLevels = keepLevels;
        // Levels 0 and 1 of a flat optional leaf are its validity: kept as bits, a bit-packed run of them is used as is.
        _bitLevels = LevelsAsBits && _maxDefinition == 1 && _maxRepetition == 0 && !keepLevels && node.SlotDef == 0 && fixedSizeList is null && !node.RejectNulls;
        if (fixedSizeList is not null)
        {
            _fixedSlotDef = fixedSizeList.SlotDef;
            _fixedElementDef = fixedSizeList.ElementDef;
            _fixedSize = fixedSizeList.ListSize;
        }
    }

    /// <summary>The definition levels of the entries read into the current batch.</summary>
    public ReadOnlySpan<byte> BatchDefinitions => _batchDef.AsSpan(0, _batchCount);

    /// <summary>The repetition levels of the entries read into the current batch.</summary>
    public ReadOnlySpan<byte> BatchRepetitions => _batchRep.AsSpan(0, _batchCount);

    /// <summary>The number of entries of the dictionary page, once read.</summary>
    protected abstract int DictionaryCount { get; }

    /// <summary>Reads a dictionary page (PLAIN values).</summary>
    protected abstract void LoadDictionary(ReadOnlySpan<byte> plain, int count);

    /// <summary>Appends <paramref name="rows"/> slots (null where <paramref name="mask"/> says so; all valid when it is empty) from PLAIN values.</summary>
    protected abstract void AppendPlain(int rows, SlotMask mask, int valid, ReadOnlySpan<byte> values, ref int position);

    /// <summary>Skips <paramref name="valid"/> PLAIN values.</summary>
    protected abstract void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position);

    /// <summary>Appends slots from dictionary entries (the indices of the valid ones, checked against the dictionary).</summary>
    protected abstract void AppendFromDictionary(int rows, SlotMask mask, ReadOnlySpan<int> indices);

    /// <summary>Lets a kind take a page whose encoding the base does not decode (RLE booleans); false refuses it.</summary>
    protected virtual bool BeginOtherEncoding(ParquetEncoding encoding, ReadOnlySpan<byte> values, int valid) => false;

    /// <summary>Appends values of a page taken by <see cref="BeginOtherEncoding"/>.</summary>
    protected virtual void AppendOther(int rows, SlotMask mask, int valid) => throw new InvalidOperationException();

    /// <summary>The mask as one byte per slot, for the kinds that take bytes (a bit mask is unpacked into scratch).</summary>
    protected ReadOnlySpan<byte> MaskBytes(SlotMask mask)
    {
        if (!mask.IsBits) return mask.Bytes;
        Ensure(ref _maskBytes, mask.Length);
        return mask.ToBytes(_maskBytes);
    }

    protected virtual void SkipOther(int valid) => throw new InvalidOperationException();

    /// <summary>Called after the builder was exported (a batch ended).</summary>
    public void BatchExported()
    {
        _batchCount = 0;
        OnBatchExported();
    }

    protected virtual void OnBatchExported() { }

    /// <summary>Appends <paramref name="records"/> records to <see cref="Builder"/>.</summary>
    public void Read(int records) => Advance(records, read: true);

    /// <summary>Skips <paramref name="records"/> records (for row ranges).</summary>
    public void Skip(int records) => Advance(records, read: false);

    private void Advance(int records, bool read)
    {
        try
        {
            int starts = 0;
            bool first = true;
            while (records > 0)
            {
                if (_pageAt == _pageValues && !TryNextDataPage())
                {
                    if (starts == records) break;
                    throw new ParquetFormatException($"the column chunk ends before its row group does ({records - starts} records missing).");
                }
                int a = _pageAt, b = a;
                if (_maxRepetition == 0)
                {
                    b = a + Math.Min(records - starts, _pageValues - a);
                    starts += b - a;
                }
                else
                {
                    ReadOnlySpan<byte> rep = _rep.AsSpan(0, _pageValues);
                    if (first && rep[a] != 0) throw new ParquetFormatException($"a record starts with repetition level {rep[a]}, not 0.");
                    for (; b < _pageValues; b++)
                    {
                        if (rep[b] != 0) continue;
                        if (starts == records) break;
                        starts++;
                    }
                }
                first = false;
                if (read) Consume(a, b);
                else SkipEntries(a, b);
                _pageAt = b;
                if (b < _pageValues || (_maxRepetition == 0 && starts == records)) break;
            }
        }
        catch (ParquetFormatException e)
        {
            throw new ParquetFormatException($"Column '{Name}': {e.Message}", e);
        }
        catch (Exception e) when (e is InvalidOperationException and not ObjectDisposedException or OverflowException)
        {
            throw new ParquetFormatException($"Column '{Name}': {e.Message}", e);
        }
    }

    /// <summary>Reads the entries [<paramref name="a"/>, <paramref name="b"/>) of the page: levels kept, slots and values appended.</summary>
    private void Consume(int a, int b)
    {
        int n = b - a;
        if (n == 0) return;
        if (_bitLevels)
        {
            int present = (int)Bits.Count(_defBits, a, n);
            Append(n, present == n ? default : SlotMask.FromBits(_defBits, a, n), present);
            return;
        }
        ReadOnlySpan<byte> def = _maxDefinition == 0 ? default : _def.AsSpan(a, n);
        if (_keepLevels)
        {
            Ensure(ref _batchDef, _batchCount + n);
            Ensure(ref _batchRep, _batchCount + n);
            if (def.IsEmpty) _batchDef.AsSpan(_batchCount, n).Clear();
            else def.CopyTo(_batchDef.AsSpan(_batchCount));
            if (_maxRepetition == 0) _batchRep.AsSpan(_batchCount, n).Clear();
            else _rep.AsSpan(a, n).CopyTo(_batchRep.AsSpan(_batchCount));
            _batchCount += n;
        }

        int slots, valid;
        ReadOnlySpan<byte> mask;
        if (def.IsEmpty)
        {
            slots = valid = n;
            mask = default;
        }
        else if (_slotDef == 0 && _fixedSlotDef < 0)
        {
            // Every entry is a slot (a flat column, or the top of a nested one): count with vector compares, and
            // at a maximum of 1 the levels are the mask themselves.
            byte max = (byte)_maxDefinition;
            slots = n;
            valid = def.Count(max);
            if (valid == n) mask = default;
            else if (max == 1) mask = def;
            else
            {
                Ensure(ref _mask, n);
                for (int i = 0; i < n; i++) _mask[i] = def[i] == max ? (byte)1 : (byte)0;
                mask = _mask.AsSpan(0, n);
            }
            if (_rejectNulls && valid != slots) throw new ParquetFormatException("a map key is null.");
        }
        else
        {
            byte max = (byte)_maxDefinition;
            Ensure(ref _mask, n);
            slots = 0;
            valid = 0;
            for (int i = 0; i < n; i++)
            {
                int d = def[i];
                if (d >= _slotDef)
                {
                    byte present = d == max ? (byte)1 : (byte)0;
                    _mask[slots++] = present;
                    valid += present;
                }
                else if (_fixedSlotDef >= 0 && d >= _fixedSlotDef && d < _fixedElementDef)
                {
                    // A null or empty fixed-size list still holds its items: null placeholders.
                    Ensure(ref _mask, checked(slots + _fixedSize + (n - i)));
                    _mask.AsSpan(slots, _fixedSize).Clear();
                    slots += _fixedSize;
                }
            }
            mask = valid == slots ? default : _mask.AsSpan(0, slots);
            if (_rejectNulls && valid != slots) throw new ParquetFormatException("a map key is null.");
        }
        Append(slots, mask, valid);
    }

    /// <summary>Appends <paramref name="slots"/> slots, <paramref name="valid"/> of them with values, from the page's encoding.</summary>
    private void Append(int slots, SlotMask mask, int valid)
    {
        if (slots == 0) return;
        if (_other) AppendOther(slots, mask, valid);
        else if (_fromDictionary)
        {
            AppendFromDictionary(slots, mask, _indices.AsSpan(_indexAt, valid));
            _indexAt += valid;
        }
        else AppendPlain(slots, mask, valid, _values.Span, ref _valuePosition);
    }

    private void SkipEntries(int a, int b)
    {
        int n = b - a;
        int valid = _maxDefinition == 0 ? n : _bitLevels ? (int)Bits.Count(_defBits, a, n) : _def.AsSpan(a, n).Count((byte)_maxDefinition);
        if (valid == 0) return;
        if (_other) SkipOther(valid);
        else if (_fromDictionary) _indexAt += valid;
        else SkipPlain(valid, _values.Span, ref _valuePosition);
    }

    /// <summary>
    /// Checks, at the end of a row group, that the chunk's values (by its metadata) were all read. Bytes after them
    /// are not parsed: a padded chunk range (PARQUET-816) reaches into the next chunk.
    /// </summary>
    public void Finish()
    {
        if (_pageAt != _pageValues || _valuesLeft != 0)
            throw new ParquetFormatException($"Column '{Name}' holds more values than its row group has rows.");
    }

    private bool TryNextDataPage()
    {
        while (true)
        {
            if (_valuesLeft == 0 || !_pages.TryNext(out Page page)) return false;
            PageHeader header = page.Header;
            switch (header.Type)
            {
                case PageType.DictionaryPage:
                {
                    if (_hasDictionary) throw new ParquetFormatException("the column chunk has a second dictionary page.");
                    DictionaryPageHeader d = header.DictionaryPageHeader ?? throw new ParquetFormatException("a DICTIONARY_PAGE has no dictionary_page_header.");
                    if (d.Encoding is not (ParquetEncoding.Plain or ParquetEncoding.PlainDictionary))
                        throw new ParquetFormatException($"a dictionary page is encoded with {d.Encoding}, not PLAIN.");
                    if (d.NumValues < 0) throw new ParquetFormatException($"a dictionary page has {d.NumValues} values.");
                    LoadDictionary(page.Body.Span, d.NumValues);
                    _hasDictionary = true;
                    continue;
                }
                case PageType.DataPage:
                {
                    DataPageHeader h = header.DataPageHeader ?? throw new ParquetFormatException("a DATA_PAGE has no data_page_header.");
                    if (h.NumValues == 0) continue;
                    BeginPage(h.NumValues, page.Body, h.Encoding, h.RepetitionLevelEncoding, h.DefinitionLevelEncoding, v2: null);
                    return true;
                }
                case PageType.DataPageV2:
                {
                    DataPageHeaderV2 h = header.DataPageHeaderV2!;
                    if (h.NumValues == 0) continue;
                    BeginPage(h.NumValues, page.Body, h.Encoding, ParquetEncoding.Rle, ParquetEncoding.Rle, h);
                    return true;
                }
                default:
                    continue; // index pages, unknown page types
            }
        }
    }

    private void BeginPage(int count, ReadOnlyMemory<byte> body, ParquetEncoding encoding, ParquetEncoding repetitionEncoding,
        ParquetEncoding definitionEncoding, DataPageHeaderV2? v2)
    {
        if (count < 0 || count > _valuesLeft)
            throw new ParquetFormatException($"a data page holds {count} values, more than the {_valuesLeft} left in the column chunk.");
        _valuesLeft -= count;
        ReadOnlySpan<byte> span = body.Span;
        int position = 0;
        if (v2 is not null)
        {
            if (_maxRepetition > 0) DecodeLevels(span[..v2.RepetitionLevelsByteLength], ref _rep, count, _repetitionBits, _maxRepetition, "repetition");
            if (_bitLevels) DecodeLevelBits(span.Slice(v2.RepetitionLevelsByteLength, v2.DefinitionLevelsByteLength), count);
            else if (_maxDefinition > 0) DecodeLevels(span.Slice(v2.RepetitionLevelsByteLength, v2.DefinitionLevelsByteLength), ref _def, count, _definitionBits, _maxDefinition, "definition");
            position = v2.RepetitionLevelsByteLength + v2.DefinitionLevelsByteLength;
        }
        else
        {
            // Data page v1: repetition levels, then definition levels, each RLE (after a 4-byte length) or BIT_PACKED.
            if (_maxRepetition > 0) position = LevelsV1(span, position, repetitionEncoding, ref _rep, count, _repetitionBits, _maxRepetition, "repetition");
            if (_bitLevels) position = LevelBitsV1(span, position, definitionEncoding, count);
            else if (_maxDefinition > 0) position = LevelsV1(span, position, definitionEncoding, ref _def, count, _definitionBits, _maxDefinition, "definition");
        }
        int valid = _maxDefinition == 0 ? count : _bitLevels ? _defBitsSet : _def.AsSpan(0, count).Count((byte)_maxDefinition);

        _values = body[position..];
        _valuePosition = 0;
        _pageValues = count;
        _pageAt = 0;
        _other = false;
        _fromDictionary = false;
        switch (encoding)
        {
            case ParquetEncoding.Plain:
                break;
            case ParquetEncoding.PlainDictionary or ParquetEncoding.RleDictionary:
                if (!_hasDictionary) throw new ParquetFormatException("a dictionary-encoded page comes without a dictionary page.");
                DecodeIndices(_values.Span, valid);
                _fromDictionary = true;
                break;
            case ParquetEncoding.DeltaBinaryPacked or ParquetEncoding.DeltaLengthByteArray or ParquetEncoding.DeltaByteArray or ParquetEncoding.ByteStreamSplit:
                _values = Transcode(encoding, _values.Span, valid);
                break;
            default:
                if (!BeginOtherEncoding(encoding, _values.Span, valid))
                    throw new ParquetFormatException($"pages encoded with {encoding} are not supported.");
                _other = true;
                break;
        }
    }

    private static int LevelsV1(ReadOnlySpan<byte> span, int position, ParquetEncoding encoding, ref byte[] levels, int count, int bits, int max, string what)
    {
        ReadOnlySpan<byte> rest = span[position..];
        if (encoding == ParquetEncoding.Rle)
        {
            if (rest.Length < 4) throw new ParquetFormatException($"a page is too short for its {what} levels.");
            int length = BinaryPrimitives.ReadInt32LittleEndian(rest);
            if (length < 0 || length > rest.Length - 4) throw new ParquetFormatException($"{what} levels of {length} bytes run past the page.");
            DecodeLevels(rest.Slice(4, length), ref levels, count, bits, max, what);
            return position + 4 + length;
        }
        if (encoding == ParquetEncoding.BitPacked)
        {
            long length = BitUnpacking.Bytes(count, bits);
            if (length > rest.Length) throw new ParquetFormatException($"BIT_PACKED {what} levels run past the page.");
            Ensure(ref levels, count);
            BitPacking.UnpackMsbFirst(rest, 0, bits, levels.AsSpan(0, count));
            CheckLevels(levels.AsSpan(0, count), max, what);
            return position + (int)length;
        }
        throw new ParquetFormatException($"{what} levels are encoded with {encoding}.");
    }

    /// <summary>Definition levels (0 or 1, at bit width 1) into <see cref="_defBits"/>.</summary>
    private void DecodeLevelBits(ReadOnlySpan<byte> encoded, int count)
    {
        // Slack after the bits: words are read 8 bytes at a time.
        EnsureBits(count);
        if (RleBitPackedDecoder.ReadAllBits(encoded, _defBits, count, out long set) != count)
            throw new ParquetFormatException($"the page has fewer definition levels than its {count} values.");
        _defBitsSet = (int)set;
    }

    private void EnsureBits(int count)
    {
        int bytes = (count + 7) / 8 + 16;
        if (_defBits.Length >= bytes) return;
        if (_defBits.Length > 0) ArrayPool<byte>.Shared.Return(_defBits);
        _defBits = ArrayPool<byte>.Shared.Rent(bytes);
    }

    /// <summary>Data page v1 definition levels of a flat optional leaf, as bits: RLE (after a 4-byte length) or the deprecated BIT_PACKED.</summary>
    private int LevelBitsV1(ReadOnlySpan<byte> span, int position, ParquetEncoding encoding, int count)
    {
        if (encoding == ParquetEncoding.Rle)
        {
            ReadOnlySpan<byte> rest = span[position..];
            if (rest.Length < 4) throw new ParquetFormatException("a page is too short for its definition levels.");
            int length = BinaryPrimitives.ReadInt32LittleEndian(rest);
            if (length < 0 || length > rest.Length - 4) throw new ParquetFormatException($"definition levels of {length} bytes run past the page.");
            DecodeLevelBits(rest.Slice(4, length), count);
            return position + 4 + length;
        }
        // BIT_PACKED (most significant bit first, rare): as bytes, then into bits.
        int end = LevelsV1(span, position, encoding, ref _def, count, 1, 1, "definition");
        EnsureBits(count);
        _defBits.AsSpan(0, (count + 7) / 8).Clear();
        for (int i = 0; i < count; i++) _defBits[i >> 3] |= (byte)(_def[i] << (i & 7));
        _defBitsSet = (int)Bits.Count(_defBits, 0, count);
        return end;
    }

    private static void DecodeLevels(ReadOnlySpan<byte> encoded, ref byte[] levels, int count, int bits, int max, string what)
    {
        Ensure(ref levels, count);
        var decoder = new RleBitPackedDecoder(encoded, bits);
        if (decoder.Read(levels.AsSpan(0, count)) != count) throw new ParquetFormatException($"the page has fewer {what} levels than its {count} values.");
        CheckLevels(levels.AsSpan(0, count), max, what);
    }

    private static void CheckLevels(ReadOnlySpan<byte> levels, int max, string what)
    {
        if (max >= byte.MaxValue) return;
        int at = levels.IndexOfAnyInRange((byte)(max + 1), byte.MaxValue);
        if (at >= 0) throw new ParquetFormatException($"a {what} level {levels[at]} exceeds the maximum {max}.");
    }

    private static void Ensure(ref byte[] buffer, int length)
    {
        if (buffer.Length >= length) return;
        byte[] grown = ArrayPool<byte>.Shared.Rent(Math.Max(length, buffer.Length * 2));
        buffer.CopyTo(grown, 0);
        if (buffer.Length > 0) ArrayPool<byte>.Shared.Return(buffer);
        buffer = grown;
    }

    /// <summary>
    /// The values of a page in one of the encodings decoded here, whole and into PLAIN layout (a buffer the next
    /// page reuses), so that every kind reads them as PLAIN: nulls, conversions, views, skips alike.
    /// </summary>
    private ReadOnlyMemory<byte> Transcode(ParquetEncoding encoding, ReadOnlySpan<byte> encoded, int valid)
    {
        ParquetType? physical = Leaf.Element.Type;
        switch (encoding, physical)
        {
            case (ParquetEncoding.DeltaBinaryPacked, ParquetType.Int32):
            {
                Span<byte> plain = TranscodeBuffer(valid, 4);
                DeltaBinaryPacked.Decode(encoded, MemoryMarshal.Cast<byte, int>(plain), DeltaScratch, toEnd: false);
                return _transcoded!.Memory[..plain.Length];
            }
            case (ParquetEncoding.DeltaBinaryPacked, ParquetType.Int64):
            {
                Span<byte> plain = TranscodeBuffer(valid, 8);
                DeltaBinaryPacked.Decode(encoded, MemoryMarshal.Cast<byte, long>(plain), DeltaScratch, toEnd: false);
                return _transcoded!.Memory[..plain.Length];
            }
            case (ParquetEncoding.DeltaLengthByteArray, ParquetType.ByteArray):
                return ByteArrays(encoded, valid, prefixed: false, fixedLength: 0);
            case (ParquetEncoding.DeltaByteArray, ParquetType.ByteArray):
                return ByteArrays(encoded, valid, prefixed: true, fixedLength: 0);
            case (ParquetEncoding.DeltaByteArray, ParquetType.FixedLenByteArray):
                return ByteArrays(encoded, valid, prefixed: true, fixedLength: Leaf.Element.TypeLength ?? 0);
            case (ParquetEncoding.ByteStreamSplit, ParquetType.Float or ParquetType.Int32):
                return Split(encoded, valid, 4);
            case (ParquetEncoding.ByteStreamSplit, ParquetType.Double or ParquetType.Int64):
                return Split(encoded, valid, 8);
            case (ParquetEncoding.ByteStreamSplit, ParquetType.FixedLenByteArray):
                return Split(encoded, valid, Leaf.Element.TypeLength ?? 0);
            default:
                throw new ParquetFormatException($"{physical} values cannot be encoded with {encoding}.");
        }
    }

    /// <summary>
    /// BYTE_STREAM_SPLIT values of <paramref name="width"/> bytes as PLAIN: the page's bytes are <paramref name="width"/>
    /// streams of equal length (no padding), as many values as that length, at least the present ones.
    /// </summary>
    private ReadOnlyMemory<byte> Split(ReadOnlySpan<byte> encoded, int valid, int width)
    {
        if (width <= 0 || encoded.Length % width != 0)
            throw new ParquetFormatException($"a BYTE_STREAM_SPLIT page of {encoded.Length} bytes does not split into streams of {width}-byte values.");
        if (encoded.Length / width < valid)
            throw new ParquetFormatException($"a BYTE_STREAM_SPLIT page holds {encoded.Length / width} values, fewer than the {valid} present.");
        Span<byte> plain = TranscodeBuffer(valid, width);
        ByteStreamSplit.Decode(encoded, width, valid, plain);
        return _transcoded!.Memory[..plain.Length];
    }

    /// <summary>DELTA_LENGTH_BYTE_ARRAY or DELTA_BYTE_ARRAY values as PLAIN BYTE_ARRAY (or FIXED_LEN_BYTE_ARRAY when <paramref name="fixedLength"/> is set).</summary>
    private ReadOnlyMemory<byte> ByteArrays(ReadOnlySpan<byte> encoded, int valid, bool prefixed, int fixedLength)
    {
        EnsureInts(ref _lengths, valid);
        if (prefixed) EnsureInts(ref _prefixes, valid);
        Span<int> lengths = _lengths.AsSpan(0, valid), prefixes = prefixed ? _prefixes.AsSpan(0, valid) : default;
        DeltaByteArrays.Sections sections = DeltaByteArrays.Read(encoded, prefixed, prefixes, lengths, DeltaScratch);
        Span<byte> plain = TranscodeBuffer(DeltaByteArrays.PlainLength(prefixes, lengths, fixedLength), 1);
        DeltaByteArrays.WritePlain(encoded.Slice(sections.DataStart, (int)sections.DataBytes), prefixes, lengths, plain, fixedLength);
        return _transcoded!.Memory[..plain.Length];
    }

    private static void EnsureInts(ref int[] buffer, int length)
    {
        if (buffer.Length >= length) return;
        if (buffer.Length > 0) ArrayPool<int>.Shared.Return(buffer);
        buffer = ArrayPool<int>.Shared.Rent(length);
    }

    /// <summary>The transcoding buffer, room for <paramref name="count"/> values of <paramref name="width"/> bytes.</summary>
    private Span<byte> TranscodeBuffer(long count, int width)
    {
        long bytes = count * width;
        if (bytes > Array.MaxLength) throw new ParquetFormatException($"a page of {count} values is too large to decode.");
        _transcoded ??= new NativeByteBuffer(0, pooled: true);
        _transcoded.EnsureCapacity(bytes);
        return _transcoded.Span[..(int)bytes];
    }

    private Span<ulong> DeltaScratch => _deltaScratch ??= new ulong[DeltaBinaryPacked.Chunk];

    private void DecodeIndices(ReadOnlySpan<byte> values, int valid)
    {
        if (_indices.Length < valid)
        {
            if (_indices.Length > 0) ArrayPool<int>.Shared.Return(_indices);
            _indices = ArrayPool<int>.Shared.Rent(valid);
        }
        _indexAt = 0;
        if (valid == 0) return;
        if (values.IsEmpty) throw new ParquetFormatException("a dictionary-encoded page has no bit width.");
        var decoder = new RleBitPackedDecoder(values[1..], values[0]);
        Span<int> indices = _indices.AsSpan(0, valid);
        if (decoder.Read(indices) != valid) throw new ParquetFormatException($"the page has fewer dictionary indices than its {valid} values.");
        uint count = (uint)DictionaryCount;
        if (VectorKernels.MaxUnsigned(indices) >= count)
        {
            foreach (int index in indices)
            {
                if ((uint)index >= count) throw new ParquetFormatException($"a dictionary index {index} is beyond the {count} entries.");
            }
        }
    }

    /// <summary>
    /// Gives the page reader's buffers back once the chunk is read, rather than when the row group is done: buffers
    /// held by finished columns of every row group in flight outgrow the last-level cache, and the units still
    /// decoding then get cold ones (measured: flat 16 threads −9 %).
    /// </summary>
    public void ReleasePages()
    {
        _pages.Dispose();
        _transcoded?.Release();
        _transcoded = null;
        if (_lengths.Length > 0) ArrayPool<int>.Shared.Return(_lengths);
        if (_prefixes.Length > 0) ArrayPool<int>.Shared.Return(_prefixes);
        _lengths = _prefixes = [];
    }

    public virtual void Dispose()
    {
        ReleasePages();
        foreach (byte[] buffer in (byte[][])[_mask, _def, _rep, _batchDef, _batchRep, _defBits, _maskBytes])
        {
            if (buffer.Length > 0) ArrayPool<byte>.Shared.Return(buffer);
        }
        if (_indices.Length > 0) ArrayPool<int>.Shared.Return(_indices);
        _mask = _def = _rep = _batchDef = _batchRep = _defBits = _maskBytes = [];
        _indices = [];
        Builder.Dispose();
        GC.SuppressFinalize(this);
    }
}
