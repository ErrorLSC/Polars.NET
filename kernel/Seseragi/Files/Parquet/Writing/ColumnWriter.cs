using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Numerics;
using Apache.Arrow;
using Array = System.Array;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>What every column writer of a file shares: the options as the page layer needs them.</summary>
internal sealed record PageSettings(
    CompressionCodec Codec, int Level, ParquetPageVersion Version, int PageBytes, int PageRows, bool Dictionary,
    int DictionaryBytes, bool Statistics, int? TruncateLength, bool Checksums);

/// <summary>A column chunk ready to be written: its pages (the dictionary page first) and what its metadata needs.</summary>
internal readonly record struct FinishedChunk(
    ReadOnlyMemory<byte> DictionaryPage, ReadOnlyMemory<byte> DataPages, long UncompressedSize, ParquetEncoding[] Encodings,
    PageEncodingStats[] EncodingStats, Statistics? Statistics, long Values)
{
    public long CompressedSize => DictionaryPage.Length + DataPages.Length;
}

/// <summary>
/// Writes one leaf column of a row group: definition levels and values gathered page by page, each page encoded
/// (dictionary indices or plain values), compressed and checksummed as soon as it is full; the chunk is finished at
/// the end of the row group (the dictionary page, built meanwhile, goes first). Subclasses hold the values of the
/// page in their physical type, map them to a dictionary, encode them and keep the chunk's statistics.
/// </summary>
internal abstract class ColumnWriter
{
    private enum DictionaryState { Off, Building, Full }

    private readonly PageSettings _settings;
    private readonly bool _optional;
    private readonly ArrayBufferWriter<byte> _pages = new(1 << 16);
    private readonly ArrayBufferWriter<byte> _values = new(1 << 16);
    private readonly ArrayBufferWriter<byte> _levels = new(1 << 12);
    private readonly ArrayBufferWriter<byte> _body = new(1 << 16);
    private readonly ArrayBufferWriter<byte> _dictionaryPage = new(256);
    private readonly ThriftCompactWriter _header = new();
    private uint[] _pageLevels = new uint[1024];
    private uint[] _indices = new uint[1024];
    private int _pageRows, _pageNulls;
    private long _chunkRows, _chunkNulls, _uncompressed;
    private int _dictionaryPages, _plainPages;
    private DictionaryState _dictionary;
    private bool _dictionaryJudged;

    protected ColumnWriter(PageSettings settings, bool optional, bool dictionary)
    {
        _settings = settings;
        _optional = optional;
        DictionaryCapable = dictionary && settings.Dictionary;
        _dictionary = DictionaryCapable ? DictionaryState.Building : DictionaryState.Off;
    }

    private bool DictionaryCapable { get; }

    protected PageSettings Settings => _settings;

    /// <summary>The bytes of the chunk so far, with the open page and the dictionary as they would be written plainly.</summary>
    public long BufferedBytes => _pages.WrittenCount + PagePlainBytes + (_dictionary != DictionaryState.Off ? DictionaryPlainBytes : 0);

    /// <summary>Adds the rows <paramref name="offset"/> .. +<paramref name="count"/> of <paramref name="array"/>.</summary>
    public void Write(IArrowArray array, int offset, int count)
    {
        while (count > 0)
        {
            int take = Math.Min(Math.Min(count, _settings.PageRows - _pageRows), 1024);
            if (_pageLevels.Length < _pageRows + take) Array.Resize(ref _pageLevels, Math.Max(_pageLevels.Length * 2, _pageRows + take));
            int nulls = 0;
            if (_optional)
            {
                Span<uint> levels = _pageLevels.AsSpan(_pageRows, take);
                if (ArrowValues.NullCount(array) == 0)
                {
                    levels.Fill(1);
                }
                else
                {
                    for (int i = 0; i < take; i++)
                    {
                        bool valid = !ArrowValues.IsNull(array, offset + i);
                        levels[i] = valid ? 1u : 0u;
                        if (!valid) nulls++;
                    }
                }
            }
            Append(array, offset, take, nulls);
            _pageRows += take;
            _pageNulls += nulls;
            offset += take;
            count -= take;
            if (_pageRows >= _settings.PageRows || PagePlainBytes >= _settings.PageBytes) FlushPage();
        }
    }

    /// <summary>Closes the chunk: its last page, the dictionary page; the writer is ready for the next row group after <see cref="Reset"/>.</summary>
    public FinishedChunk Finish()
    {
        FlushPage();
        if (_dictionaryPages > 0) WriteDictionaryPage();
        List<ParquetEncoding> encodings = [];
        List<PageEncodingStats> stats = [];
        if (_dictionaryPages > 0)
        {
            stats.Add(new(PageType.DictionaryPage, ParquetEncoding.Plain, 1));
            stats.Add(new(DataPageType, ParquetEncoding.RleDictionary, _dictionaryPages));
        }
        if (_plainPages > 0) stats.Add(new(DataPageType, ParquetEncoding.Plain, _plainPages));
        if (_dictionaryPages > 0 || _plainPages > 0) encodings.Add(ParquetEncoding.Plain);
        encodings.Add(ParquetEncoding.Rle);
        if (_dictionaryPages > 0) encodings.Add(ParquetEncoding.RleDictionary);
        Statistics? statistics = _settings.Statistics ? ChunkStatistics(_chunkNulls) : null;
        return new FinishedChunk(_dictionaryPage.WrittenMemory, _pages.WrittenMemory, _uncompressed, [.. encodings], [.. stats], statistics, _chunkRows);
    }

    /// <summary>Forgets the finished chunk (its buffers are reused).</summary>
    public void Reset()
    {
        _pages.ResetWrittenCount();
        _dictionaryPage.ResetWrittenCount();
        _chunkRows = _chunkNulls = _uncompressed = 0;
        _dictionaryPages = _plainPages = 0;
        _dictionary = DictionaryCapable ? DictionaryState.Building : DictionaryState.Off;
        _dictionaryJudged = false;
        ClearDictionary();
        ResetStatistics();
    }

    private PageType DataPageType => _settings.Version == ParquetPageVersion.V2 ? PageType.DataPageV2 : PageType.DataPage;

    private void FlushPage()
    {
        if (_pageRows == 0) return;
        int count = PageValueCount;
        _values.ResetWrittenCount();
        ParquetEncoding encoding = ParquetEncoding.Plain;
        if (_dictionary == DictionaryState.Building)
        {
            if (_indices.Length < count) _indices = new uint[Math.Max(count, _indices.Length * 2)];
            Span<uint> indices = _indices.AsSpan(0, count);
            MapToDictionary(indices);
            int bitWidth = DictionaryCount <= 1 ? 0 : 32 - BitOperations.LeadingZeroCount((uint)DictionaryCount - 1);
            if (!_dictionaryJudged)
            {
                // The first page decides: a dictionary that does not make it smaller is dropped for the whole chunk.
                _dictionaryJudged = true;
                if (DictionaryPlainBytes + 1 + RleBitPackedEncoder.MaxBytes(count, bitWidth) >= PagePlainBytes)
                {
                    _dictionary = DictionaryState.Off;
                    ClearDictionary();
                }
            }
            if (_dictionary == DictionaryState.Building)
            {
                _values.GetSpan(1)[0] = (byte)bitWidth;
                _values.Advance(1);
                RleBitPackedEncoder.Encode(indices, bitWidth, _values);
                encoding = ParquetEncoding.RleDictionary;
                if (DictionaryPlainBytes > _settings.DictionaryBytes) _dictionary = DictionaryState.Full;
            }
        }
        if (encoding == ParquetEncoding.Plain) EncodePlain(_values);

        _levels.ResetWrittenCount();
        if (_optional) RleBitPackedEncoder.Encode(_pageLevels.AsSpan(0, _pageRows), 1, _levels);

        _body.ResetWrittenCount();
        int uncompressed;
        PageHeader header;
        if (_settings.Version == ParquetPageVersion.V1)
        {
            // Levels (with their 4-byte length) and values, compressed together.
            int levels = _optional ? 4 + _levels.WrittenCount : 0;
            uncompressed = levels + _values.WrittenCount;
            byte[] raw = ArrayPool<byte>.Shared.Rent(uncompressed);
            try
            {
                if (_optional)
                {
                    BinaryPrimitives.WriteInt32LittleEndian(raw, _levels.WrittenCount);
                    _levels.WrittenSpan.CopyTo(raw.AsSpan(4));
                }
                _values.WrittenSpan.CopyTo(raw.AsSpan(levels));
                Compress(raw.AsSpan(0, uncompressed), _body);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(raw);
            }
            header = new PageHeader
            {
                Type = PageType.DataPage,
                UncompressedPageSize = uncompressed,
                CompressedPageSize = _body.WrittenCount,
                DataPageHeader = new DataPageHeader
                {
                    NumValues = _pageRows,
                    Encoding = encoding,
                    DefinitionLevelEncoding = ParquetEncoding.Rle,
                    RepetitionLevelEncoding = ParquetEncoding.Rle,
                },
            };
        }
        else
        {
            // Levels uncompressed and without a length, then the values compressed.
            int levels = _levels.WrittenCount;
            _levels.WrittenSpan.CopyTo(_body.GetSpan(levels));
            _body.Advance(levels);
            bool compressed = _settings.Codec != CompressionCodec.Uncompressed;
            if (compressed) Compress(_values.WrittenSpan, _body);
            else
            {
                _values.WrittenSpan.CopyTo(_body.GetSpan(_values.WrittenCount));
                _body.Advance(_values.WrittenCount);
            }
            uncompressed = levels + _values.WrittenCount;
            header = new PageHeader
            {
                Type = PageType.DataPageV2,
                UncompressedPageSize = uncompressed,
                CompressedPageSize = _body.WrittenCount,
                DataPageHeaderV2 = new DataPageHeaderV2
                {
                    NumValues = _pageRows,
                    NumNulls = _pageNulls,
                    NumRows = _pageRows,
                    Encoding = encoding,
                    DefinitionLevelsByteLength = levels,
                    RepetitionLevelsByteLength = 0,
                    IsCompressedField = compressed,
                },
            };
        }
        WritePage(header, _pages);
        if (encoding == ParquetEncoding.RleDictionary) _dictionaryPages++;
        else _plainPages++;
        _chunkRows += _pageRows;
        _chunkNulls += _pageNulls;
        _pageRows = _pageNulls = 0;
        ClearPage();
    }

    private void WriteDictionaryPage()
    {
        _values.ResetWrittenCount();
        EncodeDictionary(_values);
        _body.ResetWrittenCount();
        Compress(_values.WrittenSpan, _body);
        var header = new PageHeader
        {
            Type = PageType.DictionaryPage,
            UncompressedPageSize = _values.WrittenCount,
            CompressedPageSize = _body.WrittenCount,
            DictionaryPageHeader = new DictionaryPageHeader { NumValues = DictionaryCount, Encoding = ParquetEncoding.Plain },
        };
        WritePage(header, _dictionaryPage);
    }

    /// <summary>Writes the header (with the body's checksum) and the body.</summary>
    private void WritePage(PageHeader header, ArrayBufferWriter<byte> output)
    {
        if (_settings.Checksums) header = WithCrc(header, (int)Crc32.HashToUInt32(_body.WrittenSpan));
        _header.Clear();
        header.Write(_header);
        output.Write(_header.WrittenSpan);
        output.Write(_body.WrittenSpan);
        _uncompressed += _header.WrittenSpan.Length + header.UncompressedPageSize;
    }

    private static PageHeader WithCrc(PageHeader h, int crc) => new()
    {
        Type = h.Type,
        UncompressedPageSize = h.UncompressedPageSize,
        CompressedPageSize = h.CompressedPageSize,
        Crc = crc,
        DataPageHeader = h.DataPageHeader,
        DictionaryPageHeader = h.DictionaryPageHeader,
        DataPageHeaderV2 = h.DataPageHeaderV2,
    };

    private void Compress(ReadOnlySpan<byte> source, ArrayBufferWriter<byte> output)
    {
        Span<byte> destination = output.GetSpan(PageCodecs.MaxCompressedLength(_settings.Codec, source.Length));
        output.Advance(PageCodecs.Compress(_settings.Codec, source, destination, _settings.Level));
    }

    // ------------------------------------------------------------------
    // The values of a physical type
    // ------------------------------------------------------------------

    /// <summary>Adds the non-null values of the rows (the page's statistics updated); <paramref name="nulls"/> of them are null.</summary>
    protected abstract void Append(IArrowArray array, int offset, int count, int nulls);

    /// <summary>The non-null values in the open page.</summary>
    protected abstract int PageValueCount { get; }

    /// <summary>The bytes the open page's values take plainly encoded.</summary>
    protected abstract long PagePlainBytes { get; }

    protected abstract void EncodePlain(IBufferWriter<byte> output);

    protected abstract void ClearPage();

    /// <summary>Adds the page's values to the dictionary and gives their indices.</summary>
    protected virtual void MapToDictionary(Span<uint> indices) => throw new NotSupportedException();

    protected virtual int DictionaryCount => 0;

    /// <summary>The bytes of the dictionary page's values (plainly encoded).</summary>
    protected virtual long DictionaryPlainBytes => 0;

    protected virtual void EncodeDictionary(IBufferWriter<byte> output) => throw new NotSupportedException();

    protected virtual void ClearDictionary() { }

    protected abstract Statistics? ChunkStatistics(long nullCount);

    protected abstract void ResetStatistics();
}
