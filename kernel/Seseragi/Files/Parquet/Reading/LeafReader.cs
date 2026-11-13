using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;

namespace Kaminari.Seseragi.Files.Parquet.Reading;

/// <summary>
/// Reads one flat column chunk (a leaf directly under the root) into an Arrow builder, a batch at a time. The base
/// class walks the pages and decodes definition levels into a validity mask; the kinds below decode the values.
/// A page's levels (and dictionary indices) are decoded whole when the page is entered, so rows can be read or
/// skipped from anywhere in it.
/// </summary>
internal abstract class LeafReader : IDisposable
{
    private readonly PageReader _pages;
    private readonly int _maxDefinition;
    private readonly int _definitionBits;
    private long _valuesLeft;           // in the chunk, from its metadata

    private byte[] _mask = [];          // per value of the page: 1 where present
    private int _pageValues, _pageAt;
    private ReadOnlyMemory<byte> _values;
    private int _valuePosition;
    private bool _fromDictionary;
    private int[] _indices = [];
    private int _indexAt;
    private bool _hasDictionary;

    protected LeafReader(PageReader pages, SchemaNode leaf, LeafType type, long valueCount)
    {
        _pages = pages;
        Leaf = leaf;
        Type = type;
        _maxDefinition = leaf.MaxDefinition;
        _definitionBits = 32 - BitOperations.LeadingZeroCount((uint)_maxDefinition);
        _valuesLeft = valueCount;
    }

    public SchemaNode Leaf { get; }
    public LeafType Type { get; }
    public abstract NativeColumnBuilder Builder { get; }
    protected string Name => string.Join('.', Leaf.Path);

    /// <summary>The number of entries of the dictionary page, once read.</summary>
    protected abstract int DictionaryCount { get; }

    /// <summary>Reads a dictionary page (PLAIN values).</summary>
    protected abstract void LoadDictionary(ReadOnlySpan<byte> plain, int count);

    /// <summary>Appends <paramref name="rows"/> slots (null where <paramref name="mask"/> is 0; all valid when it is empty) from PLAIN values.</summary>
    protected abstract void AppendPlain(int rows, ReadOnlySpan<byte> mask, int valid, ReadOnlySpan<byte> values, ref int position);

    /// <summary>Skips <paramref name="valid"/> PLAIN values.</summary>
    protected abstract void SkipPlain(int valid, ReadOnlySpan<byte> values, ref int position);

    /// <summary>Appends slots from dictionary entries (the indices of the valid ones, checked against the dictionary).</summary>
    protected abstract void AppendFromDictionary(int rows, ReadOnlySpan<byte> mask, ReadOnlySpan<int> indices);

    /// <summary>Lets a kind take a page whose encoding the base does not decode (RLE booleans); false refuses it.</summary>
    protected virtual bool BeginOtherEncoding(ParquetEncoding encoding, ReadOnlySpan<byte> values, int valid) => false;

    /// <summary>Appends values of a page taken by <see cref="BeginOtherEncoding"/>.</summary>
    protected virtual void AppendOther(int rows, ReadOnlySpan<byte> mask, int valid) => throw new InvalidOperationException();

    protected virtual void SkipOther(int valid) => throw new InvalidOperationException();

    /// <summary>Called after the builder was exported (a batch ended).</summary>
    public virtual void BatchExported() { }

    private bool _other;

    /// <summary>Appends <paramref name="rows"/> rows to <see cref="Builder"/>.</summary>
    public void Read(int rows)
    {
        try
        {
            while (rows > 0)
            {
                if (_pageAt == _pageValues) NextDataPage();
                int n = Math.Min(rows, _pageValues - _pageAt);
                ReadOnlySpan<byte> mask = _maxDefinition == 0 ? default : _mask.AsSpan(_pageAt, n);
                int valid = mask.IsEmpty ? n : n - mask.Count((byte)0);
                if (_other) AppendOther(n, mask, valid);
                else if (_fromDictionary)
                {
                    AppendFromDictionary(n, mask, _indices.AsSpan(_indexAt, valid));
                    _indexAt += valid;
                }
                else AppendPlain(n, mask, valid, _values.Span, ref _valuePosition);
                _pageAt += n;
                rows -= n;
            }
        }
        catch (ParquetFormatException e)
        {
            throw new ParquetFormatException($"Column '{Name}': {e.Message}", e);
        }
        catch (InvalidOperationException e)
        {
            throw new ParquetFormatException($"Column '{Name}': {e.Message}", e);
        }
    }

    /// <summary>Skips <paramref name="rows"/> rows (for row ranges).</summary>
    public void Skip(int rows)
    {
        try
        {
            while (rows > 0)
            {
                if (_pageAt == _pageValues) NextDataPage();
                int n = Math.Min(rows, _pageValues - _pageAt);
                int valid = _maxDefinition == 0 ? n : n - _mask.AsSpan(_pageAt, n).Count((byte)0);
                if (_other) SkipOther(valid);
                else if (_fromDictionary) _indexAt += valid;
                else SkipPlain(valid, _values.Span, ref _valuePosition);
                _pageAt += n;
                rows -= n;
            }
        }
        catch (ParquetFormatException e)
        {
            throw new ParquetFormatException($"Column '{Name}': {e.Message}", e);
        }
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

    private void NextDataPage()
    {
        while (true)
        {
            if (!_pages.TryNext(out Page page))
                throw new ParquetFormatException($"the column chunk ends before its row group does ({_valuesLeft} values missing).");
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
                    BeginPage(h.NumValues, page.Body, h.Encoding, h.DefinitionLevelEncoding, v2: null);
                    return;
                }
                case PageType.DataPageV2:
                {
                    DataPageHeaderV2 h = header.DataPageHeaderV2!;
                    if (h.NumValues == 0) continue;
                    BeginPage(h.NumValues, page.Body, h.Encoding, ParquetEncoding.Rle, h);
                    return;
                }
                default:
                    continue; // index pages, unknown page types
            }
        }
    }

    private void BeginPage(int count, ReadOnlyMemory<byte> body, ParquetEncoding encoding, ParquetEncoding levelEncoding, DataPageHeaderV2? v2)
    {
        if (count < 0 || count > _valuesLeft)
            throw new ParquetFormatException($"a data page holds {count} values, more than the {_valuesLeft} left in the column chunk.");
        _valuesLeft -= count;
        ReadOnlySpan<byte> span = body.Span;
        int position = 0;
        int valid = count;
        if (_maxDefinition > 0)
        {
            if (_mask.Length < count)
            {
                if (_mask.Length > 0) ArrayPool<byte>.Shared.Return(_mask);
                _mask = ArrayPool<byte>.Shared.Rent(count);
            }
            Span<byte> levels = _mask.AsSpan(0, count);
            ReadOnlySpan<byte> encoded;
            if (v2 is not null)
            {
                encoded = span.Slice(v2.RepetitionLevelsByteLength, v2.DefinitionLevelsByteLength);
                position = v2.RepetitionLevelsByteLength + v2.DefinitionLevelsByteLength;
                DecodeRle(encoded, levels);
            }
            else if (levelEncoding == ParquetEncoding.Rle)
            {
                if (span.Length < 4) throw new ParquetFormatException("a page is too short for its definition levels.");
                int length = BinaryPrimitives.ReadInt32LittleEndian(span);
                if (length < 0 || length > span.Length - 4) throw new ParquetFormatException($"definition levels of {length} bytes run past the page.");
                DecodeRle(span.Slice(4, length), levels);
                position = 4 + length;
            }
            else if (levelEncoding == ParquetEncoding.BitPacked)
            {
                long length = BitPacking.Bytes(count, _definitionBits);
                if (length > span.Length) throw new ParquetFormatException("BIT_PACKED definition levels run past the page.");
                BitPacking.UnpackMsbFirst(span, 0, _definitionBits, levels);
                position = (int)length;
            }
            else throw new ParquetFormatException($"definition levels are encoded with {levelEncoding}.");

            byte max = (byte)_maxDefinition;
            for (int i = 0; i < levels.Length; i++)
            {
                byte level = levels[i];
                if (level > max) throw new ParquetFormatException($"a definition level {level} exceeds the maximum {max}.");
                levels[i] = level == max ? (byte)1 : (byte)0;
            }
            valid = count - levels.Count((byte)0);
        }
        else if (v2 is not null) position = v2.RepetitionLevelsByteLength + v2.DefinitionLevelsByteLength;

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
            default:
                if (!BeginOtherEncoding(encoding, _values.Span, valid))
                    throw new ParquetFormatException($"pages encoded with {encoding} are not supported{(encoding is >= ParquetEncoding.DeltaBinaryPacked and <= ParquetEncoding.ByteStreamSplit ? " yet" : "")}.");
                _other = true;
                break;
        }
    }

    private void DecodeRle(ReadOnlySpan<byte> encoded, Span<byte> levels)
    {
        var decoder = new RleBitPackedDecoder(encoded, _definitionBits);
        if (decoder.Read(levels) != levels.Length) throw new ParquetFormatException($"the page has fewer definition levels than its {levels.Length} values.");
    }

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
        foreach (int index in indices)
        {
            if ((uint)index >= count) throw new ParquetFormatException($"a dictionary index {index} is beyond the {count} entries.");
        }
    }

    /// <summary>Moves the first <paramref name="valid"/> slots to the positions where <paramref name="mask"/> is 1, clearing the others.</summary>
    protected static void Spread<T>(Span<T> slots, int valid, ReadOnlySpan<byte> mask)
    {
        if (mask.IsEmpty || valid == slots.Length) return;
        int j = valid - 1;
        for (int i = slots.Length - 1; i >= 0; i--) slots[i] = mask[i] != 0 ? slots[j--] : default!;
    }

    public virtual void Dispose()
    {
        _pages.Dispose();
        if (_mask.Length > 0) ArrayPool<byte>.Shared.Return(_mask);
        if (_indices.Length > 0) ArrayPool<int>.Shared.Return(_indices);
        _mask = [];
        _indices = [];
        Builder.Dispose();
        GC.SuppressFinalize(this);
    }
}
