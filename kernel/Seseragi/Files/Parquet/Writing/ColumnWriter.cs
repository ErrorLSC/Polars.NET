using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Numerics;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Array = System.Array;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Encryption;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;
using Kaminari.Sazanami.Concurrency;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>What every column writer of a file shares: the options as the page layer needs them.</summary>
internal sealed record PageSettings(
    CompressionCodec Codec, int Level, ParquetPageVersion Version, int PageBytes, int PageRows, bool Dictionary,
    int DictionaryBytes, bool Statistics, int? TruncateLength, bool Checksums, bool PageIndex, bool SizeStatistics,
    ParquetValueEncoding Encoding = ParquetValueEncoding.Plain, ParquetBloomFilterOptions? Bloom = null, WorkBudget? Budget = null);

/// <summary>
/// A column chunk ready to be written: its pages (the dictionary page first; the data pages as header and body
/// pieces, <see cref="DataPagesLength"/> bytes in all) and what its metadata needs. The offset index's page offsets
/// count from the start of the data pages. The chunk owns its native buffers: the pages and the dictionary page until
/// written (<see cref="ReleasePages"/>), the Bloom filter's bitset until the writer takes it, at layout. The pages of
/// an encrypted column are sealed (modules), for the row group of ordinal <see cref="SealedGroup"/> (-1: not sealed).
/// </summary>
internal readonly record struct FinishedChunk(
    ReadOnlyMemory<byte> DictionaryPage, NativeBufferWriter? DictionaryOwner, IReadOnlyList<ReadOnlyMemory<byte>> DataPages, long DataPagesLength, long UncompressedSize,
    ParquetEncoding[] Encodings, PageEncodingStats[] EncodingStats, Statistics? Statistics, long Values, ColumnIndex? ColumnIndex,
    OffsetIndex? OffsetIndex, SizeStatistics? SizeStatistics, NativeBufferWriter? BloomFilter, PageJob[] Pages, int SealedGroup)
{
    public long CompressedSize => DictionaryPage.Length + DataPagesLength;

    /// <summary>Returns the pages' buffers to the pool, once they are in the file (the chunk owns them; the writer moved on).</summary>
    public void ReleasePages()
    {
        foreach (PageJob page in Pages) page.Release();
        DictionaryOwner?.Dispose();
    }

    /// <summary>Returns every buffer to the pool, the Bloom filter's too: a chunk that is never laid out.</summary>
    public void Release()
    {
        ReleasePages();
        BloomFilter?.Dispose();
    }
}

/// <summary>
/// A data page of the open chunk, as the page index describes it; its bounds in the column writer's bounds arena from
/// <see cref="Bounds"/>, the lower then the upper (a length of -1: none).
/// </summary>
internal readonly record struct PageRecord(int Offset, int Size, long FirstRow, long Nulls, bool NullPage, int Bounds, int MinLength, int MaxLength, long Unencoded);

/// <summary>
/// Writes one leaf column of a row group: definition levels and values gathered page by page, each page encoded
/// (dictionary indices or plain values) as soon as it is full, then compressed and checksummed (by a helper thread
/// when the budget leaves one, a few pages in flight at most; the pages are kept in order whoever compressed them);
/// the chunk is finished at the end of the row group (the dictionary page, built meanwhile, goes first). Subclasses hold the values of the
/// page in their physical type, map them to a dictionary, encode them and keep the chunk's statistics.
/// </summary>
internal abstract class ColumnWriter : IDisposable
{
    private enum DictionaryState { Off, Building, Full }

    private readonly PageSettings _settings;
    private readonly int _maxDef, _maxRep, _defWidth, _repWidth;
    // The chunk's data pages in order: the first _committed compressed and counted, the rest in flight (encoded,
    // compressed by now or soon). One list: a column writer lives as long as its file, its objects in the oldest
    // generation, and every object it holds costs the garbage collector on each collection.
    private readonly List<PageJob> _pages = [];
    private int _committed;
    private long _committedBytes, _inFlightBound;
    // Scratch and the dictionary page in native memory (the page is handed to the finished chunk, which frees it).
    private readonly NativeBufferWriter _values = new(1 << 16);
    private readonly NativeBufferWriter _levelScratch = new(1 << 12);
    private readonly NativeBufferWriter _body = new(1 << 16);
    private NativeBufferWriter _dictionaryPage = new(256);
    // The open page's levels and dictionary indices, in native memory too.
    // A byte per level (a level is at most the nesting depth: 255 at most, checked): a partition's open page holds its
    // rows long, levels and all.
    private readonly NativeArray<byte> _pageDef = new(InitialLevels), _pageRep; // repetition levels only below a list
    private ValidityWords _validity; // the validity of the rows of a flat step (1024 at most)
    private int _validityWords;
    private readonly NativeArray<uint> _indices = new(); // made room for when a page is dictionary-encoded
    private int _pageRows, _pageLevels, _pageNulls;
    private long _chunkLevels, _chunkNulls, _uncompressed;
    private PagesByEncoding _pagesBy; // data pages by ParquetEncoding
    private DictionaryState _dictionary;
    private bool _dictionaryJudged;
    private readonly NativeArray<byte> _bounds = new(); // the pages' bounds (PageRecord), native: no array per page
    private int _boundsLength;
    // Per page, maxDef + 1 / maxRep + 1 counts each; only when written (size statistics, and for the definition
    // levels only when they say more than the null counts).
    private readonly NativeArray<long>? _defHistograms, _repHistograms;
    private int _defHistogramLength, _repHistogramLength;
    private long _chunkRows, _chunkUnencoded;
    private bool _indexable = true; // every page so far has bounds a column index can hold
    private FileEncryptor? _encryptor; // an encrypted column: its pages are sealed as they are compressed
    private string _path = "";
    private int _column;

    /// <summary>The ordinal of the row group this writer fills (an encrypted column's modules name it).</summary>
    public int RowGroup { get; set; }

    /// <summary>Seals this column's pages from now on with <paramref name="encryptor"/>, as column <paramref name="column"/> (<paramref name="path"/>).</summary>
    public void UseEncryption(FileEncryptor encryptor, string path, int column) => (_encryptor, _path, _column) = (encryptor, path, column);

    /// <summary>The highest definition or repetition level a column may have (levels are kept a byte each).</summary>
    internal const int MaxLevel = byte.MaxValue;

    /// <summary>The bytes a page buffer (levels, indices, values) takes at first.</summary>
    protected const int InitialBytes = 16 * 1024;

    private const int InitialLevels = InitialBytes / sizeof(uint); // levels and dictionary indices (a byte per level: a quarter)

    [InlineArray(16)]
    private struct ValidityWords
    {
        private ulong _word;
    }

    [InlineArray(10)]
    private struct PagesByEncoding
    {
        private int _count;
    }

    /// <param name="settings">The page options.</param>
    /// <param name="maxDef">The leaf's maximum definition level (0 for a required top-level column).</param>
    /// <param name="maxRep">The leaf's maximum repetition level (0 outside lists and maps).</param>
    /// <param name="dictionary">Whether the type may be dictionary-encoded.</param>
    /// <exception cref="NotSupportedException">A level beyond 255 (more than 255 nested optional or repeated fields).</exception>
    protected ColumnWriter(PageSettings settings, int maxDef, int maxRep, bool dictionary)
    {
        if (maxDef > MaxLevel || maxRep > MaxLevel)
            throw new NotSupportedException($"Columns nested deeper than {MaxLevel} optional or repeated levels are not written (definition level {maxDef}, repetition level {maxRep}).");
        _settings = settings;
        _maxDef = maxDef;
        _maxRep = maxRep;
        _defWidth = 32 - BitOperations.LeadingZeroCount((uint)maxDef);
        _repWidth = 32 - BitOperations.LeadingZeroCount((uint)maxRep);
        _pageRep = new(maxRep > 0 ? InitialLevels : 0);
        if (settings.SizeStatistics && (maxDef > 1 || (maxDef == 1 && !settings.Statistics))) _defHistograms = new();
        if (settings.SizeStatistics && maxRep > 0) _repHistograms = new();
        DictionaryCapable = dictionary && settings.Dictionary;
        _dictionary = DictionaryCapable ? DictionaryState.Building : DictionaryState.Off;
        Bloom = settings.Bloom is { } bloom ? new BloomBuilder(bloom, settings.Budget) : null;
        FallbackEncoding = settings.Encoding switch
        {
            ParquetValueEncoding.DeltaBinaryPacked => ParquetEncoding.DeltaBinaryPacked,
            ParquetValueEncoding.DeltaLengthByteArray => ParquetEncoding.DeltaLengthByteArray,
            ParquetValueEncoding.DeltaByteArray => ParquetEncoding.DeltaByteArray,
            ParquetValueEncoding.ByteStreamSplit => ParquetEncoding.ByteStreamSplit,
            ParquetValueEncoding.Rle => ParquetEncoding.Rle,
            _ => ParquetEncoding.Plain,
        };
    }

    /// <summary>The chunk's Bloom filter, when the column has one: subclasses add each value as it enters a page.</summary>
    protected BloomBuilder? Bloom { get; }

    /// <summary>The encoding of data pages written without the dictionary.</summary>
    private ParquetEncoding FallbackEncoding { get; }

    private bool DictionaryCapable { get; }

    protected PageSettings Settings => _settings;

    /// <summary>The pages a column keeps in flight when helpers compress them.</summary>
    private const int MaxInFlight = 4;

    private long OpenBytes => PagePlainBytes + (_dictionary != DictionaryState.Off ? DictionaryPlainBytes : 0);

    /// <summary>
    /// The bytes of the chunk so far (its pages compressed: waits for those in flight), with the open page and the
    /// dictionary as they would be written plainly.
    /// </summary>
    public long BufferedBytes
    {
        get
        {
            Drain();
            return _committedBytes + OpenBytes;
        }
    }

    /// <summary>At least <see cref="BufferedBytes"/>, without waiting: pages in flight at their largest compressed size.</summary>
    public long BufferedBytesBound => _committedBytes + _inFlightBound + OpenBytes;

    /// <summary>Adds the rows <paramref name="offset"/> .. +<paramref name="count"/> of a flat column (no repetition, at most one optional level).</summary>
    public void Write(IArrowArray array, int offset, int count)
    {
        while (count > 0)
        {
            int take = Math.Min(Math.Min(count, _settings.PageRows - _pageRows), 1024);
            EnsureLevels(take);
            int nulls = 0;
            _validityWords = 0;
            if (_maxDef > 0)
            {
                Span<byte> levels = _pageDef.AsSpan(_pageLevels, take);
                Span<ulong> validityWords = _validity;
                if (ArrowValues.NullCount(array) == 0)
                {
                    levels.Fill(1);
                }
                else if (array is not DictionaryArray && array.Data.Buffers is [{ Length: > 0 } validity, ..])
                {
                    // The validity bits, word by word: the levels, and (through Validity) which values to take.
                    int words = Masks.Words(take);
                    Masks.Load(validity.Span, (long)array.Offset + offset, take, validityWords[..words]);
                    _validityWords = words;
                    nulls = take - Masks.Count(validityWords[..words]);
                    for (int i = 0; i < take; i++) levels[i] = (byte)((validityWords[i >> 6] >> (i & 63)) & 1);
                }
                else
                {
                    for (int i = 0; i < take; i++)
                    {
                        bool valid = !ArrowValues.IsNull(array, offset + i);
                        levels[i] = valid ? (byte)1 : (byte)0;
                        if (!valid) nulls++;
                    }
                }
            }
            Append(array, offset, take, nulls);
            _pageRows += take;
            _pageLevels += take;
            _pageNulls += nulls;
            offset += take;
            count -= take;
            if (_pageRows >= _settings.PageRows || PagePlainBytes >= _settings.PageBytes) FlushPage();
        }
    }

    /// <summary>
    /// Adds shredded rows: their levels (a row starts at each repetition level 0) and the positions in
    /// <paramref name="leaf"/> of the values present. Pages close only between rows.
    /// </summary>
    public void Write(ReadOnlySpan<byte> def, ReadOnlySpan<byte> rep, IArrowArray? leaf, ReadOnlySpan<int> indices)
    {
        while (!def.IsEmpty)
        {
            // Whole rows, as many as the page has room for and a few thousand levels at most (the page's bytes are
            // checked after each step, as the flat path checks them every 1024 rows).
            int room = _settings.PageRows - _pageRows, levels = 0, rows = 0, values = 0, nulls = 0;
            while (levels < def.Length && (rep[levels] != 0 || (rows < room && levels < LevelStep)))
            {
                if (rep[levels] == 0) rows++;
                if (def[levels] == _maxDef) values++;
                else nulls++;
                levels++;
            }
            EnsureLevels(levels);
            def[..levels].CopyTo(_pageDef.AsSpan(_pageLevels));
            if (_maxRep > 0) rep[..levels].CopyTo(_pageRep.AsSpan(_pageLevels));
            if (values > 0) AppendAt(leaf!, indices[..values]);
            _pageRows += rows;
            _pageLevels += levels;
            _pageNulls += nulls;
            def = def[levels..];
            rep = rep[levels..];
            indices = indices[values..];
            if (_pageRows >= _settings.PageRows || PagePlainBytes >= _settings.PageBytes) FlushPage();
        }
    }

    /// <summary>The levels a step of <see cref="Write(ReadOnlySpan{byte}, ReadOnlySpan{byte}, IArrowArray?, ReadOnlySpan{int})"/> takes before the page's size is checked (a row is never split).</summary>
    private const int LevelStep = 4096;

    private void EnsureLevels(int more)
    {
        int size = checked(_pageLevels + more);
        _pageDef.Grow(size, _pageLevels);
        if (_maxRep > 0) _pageRep.Grow(size, _pageLevels);
    }

    /// <summary>Closes the chunk: its last page, the dictionary page; the writer is ready for the next row group after <see cref="Reset"/>.</summary>
    public FinishedChunk Finish()
    {
        FlushPage();
        Drain();
        int dictionaryPages = _pagesBy[(int)ParquetEncoding.RleDictionary];
        if (dictionaryPages > 0) WriteDictionaryPage();
        // The encodings: of the dictionary page (PLAIN), the levels (RLE) and the data pages, in that order.
        List<ParquetEncoding> encodings = [];
        List<PageEncodingStats> stats = [];
        if (dictionaryPages > 0) stats.Add(new(PageType.DictionaryPage, ParquetEncoding.Plain, 1));
        foreach (ParquetEncoding e in (ParquetEncoding[])[ParquetEncoding.RleDictionary, ParquetEncoding.Plain, FallbackEncoding])
        {
            if (_pagesBy[(int)e] > 0 && !Listed(stats, DataPageType, e)) stats.Add(new(DataPageType, e, _pagesBy[(int)e]));
        }
        if (dictionaryPages > 0 || _pagesBy[(int)ParquetEncoding.Plain] > 0) encodings.Add(ParquetEncoding.Plain);
        encodings.Add(ParquetEncoding.Rle);
        if (dictionaryPages > 0) encodings.Add(ParquetEncoding.RleDictionary);
        if (FallbackEncoding is not (ParquetEncoding.Plain or ParquetEncoding.Rle) && _pagesBy[(int)FallbackEncoding] > 0) encodings.Add(FallbackEncoding);
        Statistics? statistics = _settings.Statistics ? ChunkStatistics(_chunkNulls) : null;

        ColumnIndex? columnIndex = BuildColumnIndex();
        OffsetIndex? offsetIndex = BuildOffsetIndex();
        SizeStatistics? sizeStatistics = BuildSizeStatistics();
        List<ReadOnlyMemory<byte>> pages = new(_pages.Count * 2);
        foreach (PageJob page in _pages)
        {
            pages.Add(page.HeaderBytes);
            pages.Add(page.Body.WrittenMemory);
        }
        PageJob[] owned = [.. _pages];
        _pages.Clear(); // the chunk owns its pages now: the writer may go on while they are written
        _committed = 0;
        NativeBufferWriter? dictionary = null;
        if (_dictionaryPage.WrittenCount > 0)
        {
            dictionary = _dictionaryPage; // the chunk owns it too; the next chunk gets a buffer of its own
            _dictionaryPage = new NativeBufferWriter(256);
        }
        return new FinishedChunk(dictionary?.WrittenMemory ?? default, dictionary, pages, _committedBytes, _uncompressed, [.. encodings], [.. stats], statistics, _chunkLevels,
            columnIndex, offsetIndex, sizeStatistics, Bloom?.Finish(), owned, _encryptor is null ? -1 : RowGroup);

        static bool Listed(List<PageEncodingStats> stats, PageType type, ParquetEncoding encoding)
        {
            foreach (PageEncodingStats s in stats)
            {
                if (s.PageType == type && s.Encoding == encoding) return true;
            }
            return false;
        }
    }

    /// <summary>Whether a definition level histogram says more than the null count: deeper levels, or no statistics to hold the count.</summary>
    private bool DefinitionHistogramWanted => _maxDef > 1 || (_maxDef == 1 && !_settings.Statistics);

    private OffsetIndex? BuildOffsetIndex()
    {
        if (!_settings.PageIndex) return null;
        bool unencoded = _settings.SizeStatistics && IsByteArray;
        // Loops, not LINQ: an index per column chunk (a sink's thousand files hold thousands).
        var locations = new PageLocation[_pages.Count];
        long[]? bytes = unencoded ? new long[_pages.Count] : null;
        for (int i = 0; i < locations.Length; i++)
        {
            PageRecord page = _pages[i].Record;
            locations[i] = new PageLocation(page.Offset, page.Size, page.FirstRow);
            if (bytes is not null) bytes[i] = page.Unencoded;
        }
        return new OffsetIndex { PageLocations = locations, UnencodedByteArrayDataBytes = bytes };
    }

    /// <summary>
    /// The pages' bounds and null counts; none without statistics, or when a page holds values whose bounds an index
    /// cannot hold (a type without order, NaNs, a string bound that cannot be cut short).
    /// </summary>
    private ColumnIndex? BuildColumnIndex()
    {
        if (!_settings.PageIndex || !_settings.Statistics || !_indexable || _pages.Count == 0) return null;
        // Ascending when no non-null page's bounds fall below the previous one's, descending when none rise; both
        // (pages alike, or fewer than two) count as ascending.
        bool ascending = true, descending = true;
        PageRecord? previous = null;
        foreach (PageJob job in _pages)
        {
            PageRecord page = job.Record;
            if (page.NullPage) continue;
            if (previous is { } p)
            {
                int min = CompareBounds(Min(page), Min(p)), max = CompareBounds(Max(page), Max(p));
                if (min < 0 || max < 0) ascending = false;
                if (min > 0 || max > 0) descending = false;
            }
            previous = page;
        }
        int pages = _pages.Count;
        var nullPages = new bool[pages];
        var mins = new byte[pages][];
        var maxes = new byte[pages][];
        var nullCounts = new long[pages];
        for (int i = 0; i < pages; i++)
        {
            PageRecord page = _pages[i].Record;
            (nullPages[i], mins[i], maxes[i], nullCounts[i]) = (page.NullPage, Min(page).ToArray(), Max(page).ToArray(), page.Nulls);
        }
        return new ColumnIndex
        {
            NullPages = nullPages,
            MinValues = mins,
            MaxValues = maxes,
            BoundaryOrder = ascending ? BoundaryOrder.Ascending : descending ? BoundaryOrder.Descending : BoundaryOrder.Unordered,
            NullCounts = nullCounts,
            RepetitionLevelHistograms = _repHistograms is not null ? _repHistograms.AsSpan(0, _repHistogramLength).ToArray() : null,
            DefinitionLevelHistograms = _defHistograms is not null && _maxDef > 1 ? _defHistograms.AsSpan(0, _defHistogramLength).ToArray() : null,
        };
    }

    /// <summary>A page's lower bound (empty: none).</summary>
    private ReadOnlySpan<byte> Min(PageRecord page) => page.MinLength < 0 ? [] : _bounds.AsSpan(page.Bounds, page.MinLength);

    /// <summary>A page's upper bound (empty: none).</summary>
    private ReadOnlySpan<byte> Max(PageRecord page) =>
        page.MaxLength < 0 ? [] : _bounds.AsSpan(page.Bounds + Math.Max(page.MinLength, 0), page.MaxLength);

    private SizeStatistics? BuildSizeStatistics()
    {
        if (!_settings.SizeStatistics) return null;
        long[]? rep = _repHistograms is not null ? Sum(_repHistograms.AsSpan(0, _repHistogramLength), _maxRep + 1) : null;
        long[]? def = DefinitionHistogramWanted ? Sum(_defHistograms!.AsSpan(0, _defHistogramLength), _maxDef + 1) : null;
        long? unencoded = IsByteArray ? _chunkUnencoded : null;
        return rep is null && def is null && unencoded is null
            ? null
            : new SizeStatistics { UnencodedByteArrayDataBytes = unencoded, RepetitionLevelHistogram = rep, DefinitionLevelHistogram = def };

        static long[] Sum(ReadOnlySpan<long> pages, int width)
        {
            var total = new long[width];
            for (int i = 0; i < pages.Length; i++) total[i % width] += pages[i];
            return total;
        }
    }

    /// <summary>Frees the writer's native buffers (pages of an unfinished chunk included); the writer is not used after.</summary>
    public virtual void Dispose()
    {
        // Every page waited for, one by one past a failure (helpers may still compress the pages after it), before
        // any page's buffers go back to the pool.
        while (_committed < _pages.Count)
        {
            try
            {
                CommitOldest();
            }
            catch (Exception)
            {
                // A page that failed to compress: its failure was reported where the chunk was finished.
            }
        }
        foreach (PageJob page in _pages) page.Release();
        _pages.Clear();
        _committed = 0;
        _values.Dispose();
        _levelScratch.Dispose();
        _body.Dispose();
        _dictionaryPage.Dispose();
        _pageDef.Dispose();
        _pageRep.Dispose();
        _indices.Dispose();
        _bounds.Dispose();
        _defHistograms?.Dispose();
        _repHistograms?.Dispose();
        Bloom?.Dispose();
    }

    /// <summary>
    /// The native bytes its scratch holds (page values, levels, dictionary indices, encoding buffers, the dictionary):
    /// what it keeps between row groups for reuse.
    /// </summary>
    internal long ScratchBytes =>
        _values.Capacity + _levelScratch.Capacity + _body.Capacity + _dictionaryPage.Capacity
        + _pageDef.Capacity + _pageRep.Capacity + 4L * _indices.Capacity + ValueScratchBytes;

    /// <summary>The native bytes of a subclass's scratch (its page values, dictionary).</summary>
    protected virtual long ValueScratchBytes => 0;

    /// <summary>
    /// Returns its scratch memory to the pool between row groups (after <see cref="Reset"/>; grown again as rows come):
    /// for writers that stay open long with little to write (one per partition).
    /// </summary>
    /// <exception cref="InvalidOperationException">The writer holds rows or pages.</exception>
    internal void Trim()
    {
        if (_pageRows != 0 || _pages.Count != 0)
            throw new InvalidOperationException("Only a column writer between row groups is trimmed.");
        _values.Trim();
        _levelScratch.Trim();
        _body.Trim();
        _dictionaryPage.Trim(); // handed out only by Finish, which makes a new one
        _pageDef.Dispose();
        _pageRep.Dispose();
        _indices.Dispose();
        _bounds.Dispose();
        _defHistograms?.Dispose();
        _repHistograms?.Dispose();
        TrimValues();
    }

    /// <summary>Returns a subclass's scratch memory (see <see cref="Trim"/>).</summary>
    protected virtual void TrimValues() { }

    /// <summary>Forgets the finished chunk (its buffers are reused).</summary>
    public void Reset()
    {
        Drain();
        foreach (PageJob page in _pages) page.Release(); // pages of a chunk never finished
        _pages.Clear();
        _committed = 0;
        _committedBytes = 0;
        _dictionaryPage.ResetWrittenCount();
        _boundsLength = _defHistogramLength = _repHistogramLength = 0;
        _chunkRows = _chunkUnencoded = 0;
        _indexable = true;
        _chunkLevels = _chunkNulls = _uncompressed = 0;
        _pagesBy = default;
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
        long unencoded = IsByteArray ? PageUnencodedBytes : 0;

        // The dictionary first (indices only, no output yet): it is judged on the first page holding values (pages of
        // nulls before it go without; they hold nothing to encode either way).
        Span<uint> indices = default;
        int bitWidth = 0;
        if (_dictionary == DictionaryState.Building && (count > 0 || _dictionaryJudged))
        {
            _indices.Reserve(count);
            if (_dictionaryJudged) MapToDictionary(_indices.AsSpan(0, count), 0);
            else
            {
                // The first page with values decides: a dictionary that does not make it smaller is dropped for the whole
                // chunk; for fixed-width values compressed afterwards, one that does not make it a quarter smaller
                // (bit-packed indices hardly compress, plain values do: measured, plain then wrote smaller files and read
                // faster). Mapped in steps: the dictionary and its indices' width only grow, so one that does not pay
                // part way never will (keys and measurements are dropped without mapping the rest of the page).
                _dictionaryJudged = true;
                long worth = IsByteArray || _settings.Codec == CompressionCodec.Uncompressed ? PagePlainBytes : PagePlainBytes * 3 / 4;
                for (int first = 0; first < count; first += JudgeStep)
                {
                    MapToDictionary(_indices.AsSpan(first, Math.Min(JudgeStep, count - first)), first);
                    if (DictionaryPlainBytes + 1 + RleBitPackedEncoder.MaxBytes(count, IndexWidth) >= worth)
                    {
                        _dictionary = DictionaryState.Off;
                        ClearDictionary();
                        break;
                    }
                }
            }
            if (_dictionary == DictionaryState.Building)
            {
                indices = _indices.AsSpan(0, count);
                bitWidth = IndexWidth;
            }
        }
        bool dictionaryPage = _dictionary == DictionaryState.Building && indices.Length == count && (count > 0 || _dictionaryJudged);

        // The page's bounds (from its distinct dictionary entries when it has them), then the index entry.
        if (_settings.Statistics) FinishPageBounds(dictionaryPage ? indices : default);
        byte[]? pageMin = null, pageMax = null;
        if (!ClosePageBounds(count, out pageMin, out pageMax) && _settings.Statistics) _indexable = false;
        if (_settings.SizeStatistics) Histograms();
        bool v1 = _settings.Version == ParquetPageVersion.V1;
        int bounds = _boundsLength;
        AppendBound(pageMin);
        AppendBound(pageMax);
        var job = new PageJob(_settings)
        {
            Record = new PageRecord(0, 0, _chunkRows, _pageNulls, count == 0, bounds, pageMin?.Length ?? -1, pageMax?.Length ?? -1, unencoded),
            Seal = _encryptor?.ForChunk(_path, RowGroup, _column),
            Ordinal = _pages.Count, // the chunk's data pages so far
        };
        NativeBufferWriter raw = job.Raw;
        raw.Reserve((int)Math.Min(Array.MaxLength / 2, PagePlainBytes + 2L * RleBitPackedEncoder.MaxBytes(_pageLevels, Math.Max(_defWidth, _repWidth)) + 16));

        // Repetition levels, then definition levels (each RLE / bit-packed; none when the maximum is 0; format 1 puts
        // each after its 4-byte length), then the values.
        int repBytes = 0, defBytes = 0;
        if (_maxRep > 0) repBytes = Levels(_pageRep.AsSpan(0, _pageLevels), _repWidth, v1, raw);
        if (_maxDef > 0) defBytes = Levels(_pageDef.AsSpan(0, _pageLevels), _defWidth, v1, raw);
        job.LevelsLength = raw.WrittenCount;

        ParquetEncoding encoding = ParquetEncoding.Plain;
        if (dictionaryPage)
        {
            raw.GetSpan(1)[0] = (byte)bitWidth;
            raw.Advance(1);
            RleBitPackedEncoder.Encode(indices, bitWidth, raw);
            encoding = ParquetEncoding.RleDictionary;
            if (DictionaryPlainBytes > _settings.DictionaryBytes) _dictionary = DictionaryState.Full;
        }
        if (encoding == ParquetEncoding.Plain)
        {
            encoding = FallbackEncoding;
            if (encoding == ParquetEncoding.Plain) EncodePlain(raw);
            else EncodeAs(encoding, raw);
        }
        job.Header = v1
            ? new PageHeader
            {
                Type = PageType.DataPage,
                UncompressedPageSize = 0, // the sizes and the checksum come with the compressed body
                CompressedPageSize = 0,
                DataPageHeader = new DataPageHeader
                {
                    NumValues = _pageLevels,
                    Encoding = encoding,
                    DefinitionLevelEncoding = ParquetEncoding.Rle,
                    RepetitionLevelEncoding = ParquetEncoding.Rle,
                },
            }
            : new PageHeader
            {
                Type = PageType.DataPageV2,
                UncompressedPageSize = 0, // the sizes and the checksum come with the compressed body
                CompressedPageSize = 0,
                DataPageHeaderV2 = new DataPageHeaderV2
                {
                    NumValues = _pageLevels,
                    NumNulls = _pageNulls,
                    NumRows = _pageRows,
                    Encoding = encoding,
                    DefinitionLevelsByteLength = defBytes,
                    RepetitionLevelsByteLength = repBytes,
                    IsCompressedField = _settings.Codec != CompressionCodec.Uncompressed,
                },
            };
        _pagesBy[(int)encoding]++;
        _chunkLevels += _pageLevels;
        _chunkNulls += _pageNulls;
        _chunkRows += _pageRows;
        _chunkUnencoded += unencoded;
        _pageRows = _pageLevels = _pageNulls = 0;
        ClearPage();
        Submit(job);
    }

    /// <summary>Queues an encoded page for compression (a helper takes it when the budget allows) and keeps the queue short.</summary>
    private void Submit(PageJob job)
    {
        job.Bound = (job.Seal is null ? 64 : 128) + PageCodecs.MaxCompressedLength(_settings.Codec, job.Raw.WrittenCount); // header (and modules)
        _pages.Add(job);
        _inFlightBound += job.Bound;
        if (_settings.Budget is { Max: > 1 } budget && budget.TryEnter()) job.Help(budget);
        int keep = _settings.Budget is { Max: > 1 } ? MaxInFlight : 0;
        while (_pages.Count - _committed > keep) CommitOldest();
    }

    /// <summary>Compresses (or waits for) the oldest page in flight and adds it to the chunk.</summary>
    private void CommitOldest()
    {
        PageJob job = _pages[_committed++];
        _inFlightBound -= job.Bound;
        job.Complete();
        int size = job.HeaderBytes.Length + job.BodyLength;
        job.Record = job.Record with { Offset = checked((int)_committedBytes), Size = size };
        _committedBytes += size;
        _uncompressed += job.HeaderBytes.Length + job.Uncompressed;
    }

    /// <summary>Keeps a page bound in the bounds arena (none: nothing).</summary>
    private void AppendBound(byte[]? bound)
    {
        if (bound is null) return;
        _bounds.Grow(_boundsLength + bound.Length, _boundsLength);
        bound.CopyTo(_bounds.AsSpan(_boundsLength, bound.Length));
        _boundsLength += bound.Length;
    }

    /// <summary>Adds every page in flight to the chunk.</summary>
    private void Drain()
    {
        while (_committed < _pages.Count) CommitOldest();
    }

    /// <summary>Counts the open page's levels by value (definition levels, when there are; repetition levels, when there are).</summary>
    private void Histograms()
    {
        if (_defHistograms is not null)
        {
            _defHistograms.Grow(_defHistogramLength + _maxDef + 1, _defHistogramLength);
            Span<long> counts = _defHistograms.AsSpan(_defHistogramLength, _maxDef + 1);
            _defHistogramLength += _maxDef + 1;
            if (_maxDef == 1)
            {
                counts[0] = _pageNulls;
                counts[1] = _pageLevels - _pageNulls;
            }
            else
            {
                counts.Clear();
                foreach (byte level in _pageDef.AsSpan(0, _pageLevels)) counts[level]++;
            }
        }
        if (_repHistograms is not null)
        {
            _repHistograms.Grow(_repHistogramLength + _maxRep + 1, _repHistogramLength);
            Span<long> counts = _repHistograms.AsSpan(_repHistogramLength, _maxRep + 1);
            _repHistogramLength += _maxRep + 1;
            counts.Clear();
            foreach (byte level in _pageRep.AsSpan(0, _pageLevels)) counts[level]++;
        }
    }

    /// <summary>Appends levels to <paramref name="output"/> (format 1: after a 4-byte length); returns the bytes appended.</summary>
    private int Levels(ReadOnlySpan<byte> levels, int width, bool withLength, IBufferWriter<byte> output)
    {
        _levelScratch.ResetWrittenCount();
        RleBitPackedEncoder.Encode(levels, width, _levelScratch);
        int length = _levelScratch.WrittenCount;
        if (withLength)
        {
            BinaryPrimitives.WriteInt32LittleEndian(output.GetSpan(4), length);
            output.Advance(4);
        }
        output.Write(_levelScratch.WrittenSpan);
        return (withLength ? 4 : 0) + length;
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

    /// <summary>
    /// Writes the header (with the body's checksum) and the body; an encrypted column's dictionary page as two modules
    /// (the body sealed into scratch first: the header holds its size and checksum).
    /// </summary>
    private void WritePage(PageHeader header, NativeBufferWriter output)
    {
        using ColumnEncryptor? seal = _encryptor?.ForChunk(_path, RowGroup, _column);
        ReadOnlySpan<byte> body = _body.WrittenSpan;
        if (seal is not null)
        {
            int length = seal.ModuleLength(ParquetModule.DictionaryPage, body.Length);
            _values.ResetWrittenCount(); // the dictionary's plain values, compressed by now
            seal.Encrypt(ParquetModule.DictionaryPage, -1, body, _values.GetSpan(length)[..length]);
            _values.Advance(length);
            body = _values.WrittenSpan;
        }
        header = With(header, body.Length, _settings.Checksums ? (int)Crc32.HashToUInt32(body) : null);
        ThriftCompactWriter thrift = ThriftCompactWriter.ThreadScratch(); // a writer of its own per column would live as long as the column
        header.Write(thrift);
        int headerLength = thrift.WrittenSpan.Length;
        if (seal is null) output.Write(thrift.WrittenSpan);
        else
        {
            byte[] module = seal.Encrypt(ParquetModule.DictionaryPageHeader, -1, thrift.WrittenSpan);
            output.Write(module);
            headerLength = module.Length;
        }
        output.Write(body);
        _uncompressed += headerLength + header.UncompressedPageSize;
    }

    private static PageHeader With(PageHeader h, int compressed, int? crc) => new()
    {
        Type = h.Type,
        UncompressedPageSize = h.UncompressedPageSize,
        CompressedPageSize = compressed,
        Crc = crc,
        DataPageHeader = h.DataPageHeader,
        DictionaryPageHeader = h.DictionaryPageHeader,
        DataPageHeaderV2 = h.DataPageHeaderV2,
    };

    private void Compress(ReadOnlySpan<byte> source, NativeBufferWriter output)
    {
        Span<byte> destination = output.GetSpan(PageCodecs.MaxCompressedLength(_settings.Codec, source.Length));
        output.Advance(PageCodecs.Compress(_settings.Codec, source, destination, _settings.Level));
    }

    // ------------------------------------------------------------------
    // The values of a physical type
    // ------------------------------------------------------------------

    /// <summary>
    /// The validity of the rows <see cref="Append"/> takes (bit k: row offset + k), when it was read from the bitmap;
    /// empty otherwise (no nulls, or a dictionary whose values may be null: ask <see cref="ArrowValues.IsNull"/>).
    /// </summary>
    protected ReadOnlySpan<ulong> Validity => ((ReadOnlySpan<ulong>)_validity)[.._validityWords];

    /// <summary>Whether row <paramref name="offset"/> + <paramref name="k"/> of a step is null.</summary>
    protected bool IsNullAt(IArrowArray array, int offset, int k) =>
        _validityWords > 0 ? (_validity[k >> 6] >> (k & 63) & 1) == 0 : ArrowValues.IsNull(array, offset + k);

    /// <summary>Adds the non-null values of the rows (the page's statistics updated); <paramref name="nulls"/> of them are null.</summary>
    protected abstract void Append(IArrowArray array, int offset, int count, int nulls);

    /// <summary>Adds the values at <paramref name="indices"/> of <paramref name="array"/> (all present; the page's statistics updated).</summary>
    protected abstract void AppendAt(IArrowArray array, ReadOnlySpan<int> indices);

    /// <summary>The non-null values in the open page.</summary>
    protected abstract int PageValueCount { get; }

    /// <summary>The bytes the open page's values take plainly encoded.</summary>
    protected abstract long PagePlainBytes { get; }

    protected abstract void EncodePlain(IBufferWriter<byte> output);

    /// <summary>Encodes the open page's values in <paramref name="encoding"/> (one the type takes: <see cref="ColumnSettings.Takes"/>).</summary>
    protected abstract void EncodeAs(ParquetEncoding encoding, IBufferWriter<byte> output);

    protected abstract void ClearPage();

    /// <summary>Adds the page's values <paramref name="first"/> .. + indices' length to the dictionary and gives their indices.</summary>
    protected virtual void MapToDictionary(Span<uint> indices, int first) => throw new NotSupportedException();

    /// <summary>The values of the page judging the dictionary mapped before it is judged again.</summary>
    private const int JudgeStep = 1024;

    /// <summary>The bits of a dictionary index (none for one entry).</summary>
    private int IndexWidth => DictionaryCount <= 1 ? 0 : 32 - BitOperations.LeadingZeroCount((uint)DictionaryCount - 1);

    protected virtual int DictionaryCount => 0;

    /// <summary>The bytes of the dictionary page's values (plainly encoded).</summary>
    protected virtual long DictionaryPlainBytes => 0;

    protected virtual void EncodeDictionary(IBufferWriter<byte> output) => throw new NotSupportedException();

    protected virtual void ClearDictionary() { }

    /// <summary>
    /// Completes the open page's bounds before they are closed, for writers that gather them at the page's end;
    /// <paramref name="dictionaryIndices"/> are the page's values as dictionary indices when the page is so encoded
    /// (empty otherwise).
    /// </summary>
    protected virtual void FinishPageBounds(ReadOnlySpan<uint> dictionaryIndices) { }

    /// <summary>
    /// Closes the open page's bounds (folding them into the chunk's) and gives them as the page index stores them:
    /// none for a page of <paramref name="values"/> 0; false when the page holds values an index cannot bound (a type
    /// without order, NaNs, a bound that cannot be cut short). Without statistics nothing was gathered.
    /// </summary>
    protected abstract bool ClosePageBounds(int values, out byte[]? min, out byte[]? max);

    /// <summary>Orders two bounds given by <see cref="ClosePageBounds"/>.</summary>
    protected abstract int CompareBounds(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b);

    /// <summary>Whether the physical type is BYTE_ARRAY (whose value bytes size statistics count).</summary>
    protected virtual bool IsByteArray => false;

    /// <summary>The bytes of the open page's values without their lengths (BYTE_ARRAY).</summary>
    protected virtual long PageUnencodedBytes => 0;

    protected abstract Statistics? ChunkStatistics(long nullCount);

    protected abstract void ResetStatistics();
}
