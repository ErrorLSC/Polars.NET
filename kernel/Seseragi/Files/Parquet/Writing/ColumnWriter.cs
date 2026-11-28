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
/// Writes one leaf column of a row group: definition levels and values gathered page by page. A full page's levels are
/// encoded at once; its values (<see cref="PageValues"/>) are prepared on any thread (mapped to a dictionary of their
/// own while the chunk may be dictionary-encoded; bounds), laid out in page order on the writer's thread (the
/// dictionary judged, or merged with the page's in order of first appearance: the file is the one a single thread
/// mapping each value would write), then encoded and compressed on any thread (helpers when the budget leaves them, a
/// few pages in flight at most; the pages kept in order whoever ran them). The chunk is finished at the end of the
/// row group (the dictionary page, built meanwhile, goes first). Subclasses hold the values of the page in their
/// physical type, their dictionaries, and the chunk's statistics.
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
    private int _resolved, _committed; // the first _resolved laid out (encoding chosen, merged into the dictionary)
    private int _judging = -1;         // the page judging the dictionary, until laid out
    private long _committedBytes, _inFlightBound, _unresolvedDictionaryBound;
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

    // Page values once encoded (by whichever thread compressed the page) go back to the pool: kept for the next
    // pages, they held measurably more memory with many writers open (30 partitions: +9 % peak RSS for two pages a
    // column), for no speed.
    private readonly Action<PageValues> _returnPage = static page => page.Dispose();

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
    public long BufferedBytesBound => _committedBytes + _inFlightBound + OpenBytes + _unresolvedDictionaryBound;

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
            // checked after each step, as the flat path checks them every 1024 rows): rows start at repetition level
            // 0, counted (and found) a vector at a time.
            var (levels, rows) = Step(rep, _settings.PageRows - _pageRows);
            int values = def[..levels].Count((byte)_maxDef), nulls = levels - values;
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
    internal const int LevelStep = 4096;

    /// <summary>
    /// The levels (and rows) a step takes from <paramref name="rep"/>, which starts a row: the rows starting within
    /// <see cref="LevelStep"/> levels, and the rest of the last one, unless that makes more than <paramref name="room"/>
    /// rows, then up to the row past the room. A row with the levels left and no repetition is no row at all only for
    /// a flat column, which never comes here.
    /// </summary>
    internal static (int Levels, int Rows) Step(ReadOnlySpan<byte> rep, int room)
    {
        int end = Math.Min(rep.Length, LevelStep);
        int rows = rep[..end].Count((byte)0);
        if (rows <= room)
        {
            // The last row goes on past the step's end until the next one starts.
            int more = end == rep.Length ? -1 : rep[end..].IndexOf((byte)0);
            return (more < 0 ? rep.Length : end + more, rows);
        }
        // The page fills within the step: up to the start of row room + 1.
        int at = 0;
        for (int row = 0; ; row++)
        {
            int next = rep[(at + 1)..].IndexOf((byte)0);
            if (row + 1 == room || next < 0) return (next < 0 ? rep.Length : at + 1 + next, row + 1);
            at += 1 + next;
        }
    }

    private void EnsureLevels(int more)
    {
        int size = checked(_pageLevels + more);
        _pageDef.Grow(size, _pageLevels);
        if (_maxRep > 0) _pageRep.Grow(size, _pageLevels);
    }

    /// <summary>Closes the chunk: its last page, the dictionary page; the writer is ready for the next row group after <see cref="Reset"/>.</summary>
    public FinishedChunk Finish()
    {
        FlushPage(last: true);
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
        _resolved = _committed = 0;
        _judging = -1;
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
        // Every page waited for, one by one past a failure (helpers may still prepare or compress the pages after it),
        // before any page's buffers go back to the pool.
        foreach (PageJob page in _pages) page.Release();
        _pages.Clear();
        _resolved = _committed = 0;
        _judging = -1;
        _values.Dispose();
        _levelScratch.Dispose();
        _body.Dispose();
        _dictionaryPage.Dispose();
        _pageDef.Dispose();
        _pageRep.Dispose();
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
        + _pageDef.Capacity + _pageRep.Capacity + ValueScratchBytes;

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
        _resolved = _committed = 0;
        _judging = -1;
        _committedBytes = _unresolvedDictionaryBound = 0;
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

    /// <summary>
    /// Closes the open page: its levels encoded, its values taken (<see cref="TakePage"/>) and queued in a page job to be
    /// prepared (mapped to a dictionary of their own while the chunk may still be dictionary-encoded) on any thread,
    /// then laid out in order (<see cref="ResolveOldest"/>), then encoded and compressed on any thread.
    /// </summary>
    private void FlushPage(bool last = false)
    {
        if (_pageRows == 0) return;
        int count = PageValueCount;
        long unencoded = IsByteArray ? PageUnencodedBytes : 0, plain = PagePlainBytes;
        if (_settings.SizeStatistics) Histograms();
        bool v1 = _settings.Version == ParquetPageVersion.V1;
        // The dictionary is judged on the first page holding values (pages of nulls before it go without; they hold
        // nothing to encode either way); pages are mapped while the chunk may still be dictionary-encoded (its state
        // as far as laid out: a page laid out later may still end it, and the page is written plainly then).
        // A page judging the dictionary is laid out before the next page is mapped: a dictionary dropped on the first
        // page leaves the rest unmapped, as one thread would.
        if (_judging >= _resolved)
        {
            while (_resolved <= _judging) ResolveOldest();
        }
        _judging = -1;
        bool judges = _dictionary == DictionaryState.Building && !_dictionaryJudged && count > 0, afterJudging = _dictionaryJudged;
        if (judges)
        {
            _dictionaryJudged = true;
            _judging = _pages.Count;
        }
        bool map = _dictionary == DictionaryState.Building && count > 0;
        // A page with nothing to prepare, and the chunk's last (nothing is written meanwhile), are prepared, laid out
        // and encoded at once, from the open page itself (kept open, emptied): no page values change hands (after the
        // pages before it, laid out first).
        bool inline = last || !map && !OpenPagePrepares;
        if (inline)
        {
            while (_resolved < _pages.Count) ResolveOldest();
        }
        PageValues values = inline ? OpenPage() : TakePage();
        // The values' bytes at most: plain, or bit-packed indices of 32 bits at the widest.
        long valueBound = Math.Max(plain, (FallbackEncoding == ParquetEncoding.Plain ? 0 : plain) + 1L + RleBitPackedEncoder.MaxBytes(count, 32));
        var job = new PageJob(_settings)
        {
            Record = new PageRecord(0, 0, _chunkRows, _pageNulls, count == 0, 0, -1, -1, unencoded),
            Seal = _encryptor?.ForChunk(_path, RowGroup, _column),
            Ordinal = _pages.Count, // the chunk's data pages so far
            Return = _returnPage,
            Judges = judges,
            AfterJudging = afterJudging,
            Values = values,
            Inline = inline,
        };
        NativeBufferWriter raw = job.Raw;
        // The levels' room only: the values' comes as they are encoded (the page values hold them meanwhile).
        raw.Reserve((int)Math.Min(Array.MaxLength / 2, 2L * RleBitPackedEncoder.MaxBytes(_pageLevels, Math.Max(_defWidth, _repWidth)) + 16));

        // Repetition levels, then definition levels (each RLE / bit-packed; none when the maximum is 0; format 1 puts
        // each after its 4-byte length); the values follow once the page is laid out.
        int repBytes = 0, defBytes = 0;
        if (_maxRep > 0) repBytes = Levels(_pageRep.AsSpan(0, _pageLevels), _repWidth, v1, raw);
        if (_maxDef > 0) defBytes = Levels(_pageDef.AsSpan(0, _pageLevels), _defWidth, v1, raw);
        job.LevelsLength = raw.WrittenCount;
        job.Shape = (_pageLevels, _pageNulls, _pageRows, defBytes, repBytes);
        long worth = IsByteArray || _settings.Codec == CompressionCodec.Uncompressed ? plain : plain * 3 / 4;
        job.Preparation = new PagePreparation(values, map, judges ? worth : null);
        if (inline) job.Preparation.Complete(); // on this thread, before the open page takes rows again
        job.Bound = (job.Seal is null ? 64 : 128) + PageCodecs.MaxCompressedLength(_settings.Codec, checked((int)Math.Min(Array.MaxLength, raw.WrittenCount + valueBound)));
        job.DictionaryBound = map ? plain : 0;
        _chunkLevels += _pageLevels;
        _chunkNulls += _pageNulls;
        _chunkRows += _pageRows;
        _chunkUnencoded += unencoded;
        _pageRows = _pageLevels = _pageNulls = 0;
        Submit(job);
    }

    /// <summary>
    /// Queues a page: prepared by a helper when the budget leaves one, laid out in order as soon as it is prepared (or
    /// once too many wait), compressed by a helper; a few pages in flight at most. Columns encoding pages otherwise
    /// than plainly or as indices lay each out at once: the encoding comes with the layout.
    /// </summary>
    private void Submit(PageJob job)
    {
        _pages.Add(job);
        _inFlightBound += job.Bound;
        _unresolvedDictionaryBound += job.DictionaryBound;
        bool helped = _settings.Budget is { Max: > 1 } && FallbackEncoding == ParquetEncoding.Plain;
        PagePreparation preparation = job.Preparation!;
        if (helped && preparation.HasWork && _settings.Budget!.TryEnter()) preparation.Help(_settings.Budget);
        int keep = helped ? MaxPreparing : 0;
        // In order: the pages prepared (or with nothing to prepare), then the oldest until few wait.
        while (_resolved < _pages.Count && (_pages[_resolved].Preparation is { } p && (p.IsDone || !p.HasWork) || _pages.Count - _resolved > keep)) ResolveOldest();
        int inFlight = _settings.Budget is { Max: > 1 } ? MaxInFlight : 0;
        while (_pages.Count - _committed > inFlight) CommitOldest();
    }

    /// <summary>The pages a column keeps waiting to be laid out while helpers prepare them.</summary>
    private const int MaxPreparing = 2;

    /// <summary>
    /// Lays out the oldest page not laid out yet, once prepared (by this thread when no helper took it), as one thread
    /// writing the pages in order would: the dictionary judged on it or merged with its own (the chunk's indices given
    /// in order of first appearance, as when mapped directly), its encoding and bounds, its header; then queued to be
    /// compressed.
    /// </summary>
    private void ResolveOldest()
    {
        PageJob job = _pages[_resolved++];
        job.Preparation!.Complete();
        _unresolvedDictionaryBound -= job.DictionaryBound;
        PageValues values = job.Values!;
        int count = values.Count;
        bool dictionaryPage = false;
        int bitWidth = 0;
        if (_dictionary == DictionaryState.Building && (count > 0 || job.AfterJudging))
        {
            if (job.Judges && !values.Pays)
            {
                // A dictionary that does not make the first page smaller is dropped for the whole chunk; for fixed-width
                // values compressed afterwards, one that does not make it a quarter smaller (bit-packed indices hardly
                // compress, plain values do: measured, plain then wrote smaller files and read faster).
                _dictionary = DictionaryState.Off;
                ClearDictionary();
            }
            else
            {
                if (count > 0) MergeLocal(values);
                dictionaryPage = true;
                bitWidth = IndexWidth;
            }
        }

        // The page's bounds, then the index entry.
        if (!ClosePageBounds(values, count, out byte[]? pageMin, out byte[]? pageMax) && _settings.Statistics) _indexable = false;
        int bounds = _boundsLength;
        AppendBound(pageMin);
        AppendBound(pageMax);
        job.Record = job.Record with { Bounds = bounds, MinLength = pageMin?.Length ?? -1, MaxLength = pageMax?.Length ?? -1 };

        ParquetEncoding encoding;
        if (dictionaryPage)
        {
            encoding = ParquetEncoding.RleDictionary;
            job.BitWidth = bitWidth;
            if (DictionaryPlainBytes > _settings.DictionaryBytes) _dictionary = DictionaryState.Full;
        }
        else
        {
            encoding = FallbackEncoding;
            if (encoding != ParquetEncoding.Plain)
            {
                values.EncodeAs(encoding, job.Raw);
                job.Values = null;
                if (!job.Inline) _returnPage(values);
            }
        }
        if (job.Inline && job.Values is not null)
        {
            // The open page itself: encoded now, then emptied for the rows to come.
            if (dictionaryPage) values.EncodeIndices(bitWidth, job.Raw);
            else values.EncodePlain(job.Raw);
            job.Values = null;
        }
        if (job.Inline) values.Clear();
        var (levels, nulls, rows, defBytes, repBytes) = job.Shape;
        job.Header = _settings.Version == ParquetPageVersion.V1
            ? new PageHeader
            {
                Type = PageType.DataPage,
                UncompressedPageSize = 0, // the sizes and the checksum come with the compressed body
                CompressedPageSize = 0,
                DataPageHeader = new DataPageHeader
                {
                    NumValues = levels,
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
                    NumValues = levels,
                    NumNulls = nulls,
                    NumRows = rows,
                    Encoding = encoding,
                    DefinitionLevelsByteLength = defBytes,
                    RepetitionLevelsByteLength = repBytes,
                    IsCompressedField = _settings.Codec != CompressionCodec.Uncompressed,
                },
            };
        _pagesBy[(int)encoding]++;
        // The page's bound now that its encoding is known (never above the one it was queued with).
        long valueBytes = job.Values is null ? 0 : dictionaryPage ? 1L + RleBitPackedEncoder.MaxBytes(count, bitWidth) : values.PlainBytes;
        long bound = Math.Min(job.Bound, (job.Seal is null ? 64 : 128) + PageCodecs.MaxCompressedLength(_settings.Codec, checked((int)Math.Min(Array.MaxLength, job.Raw.WrittenCount + valueBytes))));
        _inFlightBound += bound - job.Bound;
        job.Bound = bound;
        if (_settings.Budget is { Max: > 1 } budget && budget.TryEnter()) job.Help(budget);
    }

    /// <summary>Compresses (or waits for) the oldest page in flight and adds it to the chunk (laid out first when it was not).</summary>
    private void CommitOldest()
    {
        if (_committed == _resolved) ResolveOldest();
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
            else Count(_pageDef.AsSpan(0, _pageLevels), counts);
        }
        if (_repHistograms is not null)
        {
            _repHistograms.Grow(_repHistogramLength + _maxRep + 1, _repHistogramLength);
            Span<long> counts = _repHistograms.AsSpan(_repHistogramLength, _maxRep + 1);
            _repHistogramLength += _maxRep + 1;
            Count(_pageRep.AsSpan(0, _pageLevels), counts);
        }

        // How many levels have each value: a vector pass per value for the few levels of most columns, else one pass.
        static void Count(ReadOnlySpan<byte> levels, Span<long> counts)
        {
            if (counts.Length <= 8)
            {
                long rest = levels.Length;
                for (int k = 1; k < counts.Length; k++) rest -= counts[k] = levels.Count((byte)k);
                counts[0] = rest;
                return;
            }
            counts.Clear();
            foreach (byte level in levels) counts[level]++;
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

    /// <summary>
    /// Takes the open page's values (with its bounds as far as gathered on the way) and opens an empty page.
    /// </summary>
    protected abstract PageValues TakePage();

    /// <summary>The open page's values (with its bounds as far as gathered on the way), left open: encoded and emptied in place.</summary>
    protected abstract PageValues OpenPage();

    /// <summary>Whether the open page's values have anything to prepare beyond a dictionary (<see cref="PageValues.Prepares"/>).</summary>
    protected virtual bool OpenPagePrepares => false;

    /// <summary>
    /// Merges a page's dictionary into the chunk's, in order of first appearance, filling the page's
    /// <see cref="PageValues.Remap"/> with each entry's index in the chunk's.
    /// </summary>
    protected virtual void MergeLocal(PageValues page) => throw new NotSupportedException();

    /// <summary>The bits of a dictionary index (none for one entry).</summary>
    private int IndexWidth => PageValues.Width(DictionaryCount);

    protected virtual int DictionaryCount => 0;

    /// <summary>The bytes of the dictionary page's values (plainly encoded).</summary>
    protected virtual long DictionaryPlainBytes => 0;

    protected virtual void EncodeDictionary(IBufferWriter<byte> output) => throw new NotSupportedException();

    protected virtual void ClearDictionary() { }

    /// <summary>
    /// Closes a page's bounds (folding them into the chunk's, pages in order) and gives them as the page index stores
    /// them: none for a page of <paramref name="values"/> 0; false when the page holds values an index cannot bound (a
    /// type without order, NaNs, a bound that cannot be cut short). Without statistics nothing was gathered.
    /// </summary>
    protected abstract bool ClosePageBounds(PageValues page, int values, out byte[]? min, out byte[]? max);

    /// <summary>Orders two bounds given by <see cref="ClosePageBounds"/>.</summary>
    protected abstract int CompareBounds(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b);

    /// <summary>Whether the physical type is BYTE_ARRAY (whose value bytes size statistics count).</summary>
    protected virtual bool IsByteArray => false;

    /// <summary>The bytes of the open page's values without their lengths (BYTE_ARRAY).</summary>
    protected virtual long PageUnencodedBytes => 0;

    protected abstract Statistics? ChunkStatistics(long nullCount);

    protected abstract void ResetStatistics();
}
