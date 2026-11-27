using System.IO.Hashing;
using System.Runtime.ExceptionServices;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Concurrency;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Encryption;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet.Reading;

/// <summary>A page of a column chunk, uncompressed: for a data page v2, its levels followed by its values.</summary>
internal readonly record struct Page(PageHeader Header, ReadOnlyMemory<byte> Body);

/// <summary>Memory for a page's decompressed body, valid until the next page.</summary>
internal interface IPageMemory
{
    Memory<byte> Take(int bytes);
}

/// <summary>
/// Walks the pages of one column chunk: parses each header, checks sizes and, when asked, the CRC, and decompresses
/// the page into a scratch buffer that the next page reuses. The chunk is either in memory (fetched, or a slice of a
/// source that lives in memory) or read from a local file as it goes, through a window of about a megabyte that the
/// pages reuse: each page is then read once from the page cache into memory the caches still hold when it is decoded,
/// instead of the whole row group going through main memory twice. A page's body is valid until the next page.
/// <para>
/// A chunk read whole, in order, may read ahead (<see cref="UseReadAhead"/>): the next pages' headers are parsed as
/// before, and their bodies read, checked, decrypted and decompressed by helpers on threads the budget leaves, while
/// the column decodes the page before; the pages are taken in order, and a failure surfaces at the page it concerns.
/// </para>
/// </summary>
internal sealed class PageReader : IDisposable, IPageMemory
{
    /// <summary>The pages read ahead at most (each one's stored and decompressed bytes held until taken).</summary>
    internal const int AheadPages = 4;

    /// <summary>The bytes read ahead from a file at once (more when a page is larger).</summary>
    internal const int ReadAhead = 1 << 20;

    private readonly ReadOnlyMemory<byte> _chunk;
    private readonly CompressionCodec _codec;
    private readonly bool _verifyChecksums;
    private readonly NativeBufferPool? _pool;
    private readonly IRandomAccess? _source;           // reading from a file: the source and the chunk's range in it
    private readonly long _sourceStart;
    private readonly long _length;
    private NativeByteBuffer? _scratch;
    private NativeByteBuffer? _window;                // file reads: chunk bytes [_windowStart, _windowEnd)
    private long _windowStart, _windowEnd;
    private long _position;
    private GuardedMemory.Block? _guardedStored, _guardedBody; // fuzzing: the page's bytes, each ending at a guard page
    private readonly (long Start, ReadOnlyMemory<byte> Bytes)[]? _segments; // a chunk fetched in pieces
    private ChunkPageIndex? _index;     // reading by the offset index: pages at its offsets
    private long _chunkStart;           // the chunk's offset in the file (the index's offsets are the file's)
    private bool[]? _needed;            // the pages to be read (others are skipped): bounds reads ahead
    private int _nextPage;              // the next data page by the index
    private bool _dictionaryPending;    // a dictionary page lies before the first indexed page and was not read
    private ColumnDecryptor? _crypto;   // an encrypted chunk: each header and page is a module of its own
    private int _dataOrdinal;           // read in order: the ordinal of the next data page (in the AAD of its modules)
    private int _headerOrdinal;         // the ordinal of the page whose header was parsed last
    private NativeByteBuffer? _plainHeader, _plainPage; // decrypted header and page body
    private WorkBudget? _aheadBudget;   // reading ahead: the threads helpers may take
    private Queue<Ahead>? _ahead;       // the pages read ahead, in order
    private readonly Stack<Ahead> _spareAhead = [];
    private Ahead? _current;            // the page read ahead that TryNext returned last (its body is valid until the next call)
    private bool _aheadStopped;         // a header could not be parsed ahead: its failure waits in the last page queued
    private bool _headerAhead;          // parsing the header of a page read ahead (the window then holds no more)

    /// <summary>
    /// Reads ahead from now on, with helpers on the thread pool holding threads of <paramref name="budget"/> (shared
    /// with the decodes: never more busy than it allows): for a chunk read whole and in order (never by the offset
    /// index, nor skipping pages); not while fuzzing (pages then end at guard pages).
    /// </summary>
    public void UseReadAhead(WorkBudget budget)
    {
        if (_index is not null || GuardedMemory.Enabled || budget.Max <= 1) return;
        _aheadBudget = budget;
        _ahead = new Queue<Ahead>(AheadPages);
    }

    /// <summary>Whether pages are read ahead (for tests).</summary>
    internal bool ReadsAhead => _ahead is not null;

    /// <summary>The pages read ahead by helpers or by the reader itself (for tests).</summary>
    internal int PagesReadAhead { get; private set; }

    /// <summary>
    /// Decrypts the chunk's headers and pages with <paramref name="crypto"/> from now on (Parquet modular encryption);
    /// the reader disposes it.
    /// </summary>
    public void UseDecryption(ColumnDecryptor crypto) => _crypto = crypto;

    /// <summary>
    /// Reads by the chunk's offset index from now on: data pages are found at their offsets (skipped ones passed over
    /// by <see cref="TrySkipByIndex"/> without a read), a dictionary page before the first one is read before the first
    /// data page read. Each page read must agree with the index. <paramref name="needed"/> marks the pages that will be
    /// read: reads ahead stop where they end.
    /// </summary>
    public void UseIndex(ChunkPageIndex index, long chunkStart, bool[]? needed)
    {
        _index = index;
        _chunkStart = chunkStart;
        _needed = needed;
        _nextPage = 0;
        _dictionaryPending = index.Offsets[0] > chunkStart;
    }

    /// <summary>Whether pages are read by the offset index.</summary>
    public bool Indexed => _index is not null;

    /// <summary>The rows of the data page <see cref="TryNext"/> returned last, by the offset index (null when not reading by it).</summary>
    public long? IndexedRows { get; private set; }

    /// <summary>
    /// Passes over the next data page by the offset index, without reading anything, when it holds at most
    /// <paramref name="most"/> rows (<paramref name="rows"/>).
    /// </summary>
    public bool TrySkipByIndex(long most, out long rows)
    {
        rows = 0;
        if (_index is null || _nextPage >= _index.PageCount) return false;
        rows = _index.RowsOf(_nextPage);
        if (rows > most) return false;
        _nextPage++;
        SkippedPages++;
        return true;
    }

    /// <summary>Where the bytes that may be read from <paramref name="from"/> on end: the run of pages to read there (the chunk's end when not known).</summary>
    private long ReadLimit(long from)
    {
        if (_index is not { } index || _needed is not { } needed) return _length;
        long first = index.Offsets[0] - _chunkStart, end;
        int k;
        if (from < first)
        {
            k = 0;
            end = first;
        }
        else
        {
            k = Math.Max(0, Array.BinarySearch(index.Offsets, from + _chunkStart) is var i && i >= 0 ? i : ~i - 1);
            end = index.Offsets[k] - _chunkStart + index.Sizes[k];
            k++;
        }
        for (; k < index.PageCount && needed[k] && index.Offsets[k] - _chunkStart == end; k++) end += index.Sizes[k];
        return Math.Min(end, _length);
    }

    /// <summary>A chunk whose bytes are in memory.</summary>
    public PageReader(ReadOnlyMemory<byte> chunk, CompressionCodec codec, bool verifyChecksums, NativeBufferPool? pool = null)
    {
        _chunk = chunk;
        _length = chunk.Length;
        _codec = codec;
        _verifyChecksums = verifyChecksums;
        _pool = pool;
    }

    /// <summary>
    /// A chunk of <paramref name="length"/> bytes of which only some were fetched: <paramref name="segments"/>, each at
    /// its offset in the chunk (the dictionary page, the pages to read). Reading elsewhere is an error.
    /// </summary>
    public PageReader((long Start, ReadOnlyMemory<byte> Bytes)[] segments, long length, CompressionCodec codec, bool verifyChecksums,
        NativeBufferPool? pool = null)
    {
        _segments = segments;
        _length = length;
        _codec = codec;
        _verifyChecksums = verifyChecksums;
        _pool = pool;
    }

    /// <summary>A chunk of <paramref name="length"/> bytes at <paramref name="start"/> of a local file, read as the pages are.</summary>
    public PageReader(IRandomAccess source, long start, long length, CompressionCodec codec, bool verifyChecksums, NativeBufferPool? pool = null)
    {
        _source = source;
        _sourceStart = start;
        _length = length;
        _codec = codec;
        _verifyChecksums = verifyChecksums;
        _pool = pool;
    }

    /// <summary>The scratch buffer, at least <paramref name="bytes"/> long (taken from the pool on first use).</summary>
    private NativeByteBuffer Scratch(int bytes)
    {
        if (_scratch is null) _scratch = _pool?.Rent(bytes) ?? new NativeByteBuffer(0, pooled: true);
        _scratch.EnsureCapacity(bytes);
        return _scratch;
    }

    /// <summary>Whether bytes remain after the pages read so far.</summary>
    public bool HasMore => _ahead?.Count > 0 || _position < _length;

    /// <summary>
    /// Chunk bytes [<paramref name="from"/>, + <paramref name="count"/>), clipped to the chunk: a slice of the chunk
    /// in memory, or of the window, which is refilled from <paramref name="from"/> on (at least <see cref="ReadAhead"/>
    /// bytes) when it does not hold them.
    /// </summary>
    private ReadOnlyMemory<byte> Bytes(long from, long count)
    {
        count = Math.Min(count, _length - from);
        if (_segments is not null) return Segment(from, count);
        if (_source is null) return _chunk.Slice((int)from, (int)count);
        if (from < _windowStart || from + count > _windowEnd)
        {
            // A header parsed for a page read ahead: its helper reads the body itself, so only the header is read.
            long length = Math.Min(Math.Max(count, _headerAhead ? 0 : ReadAhead), Math.Max(ReadLimit(from), from + count) - from);
            _window ??= _pool?.Rent(length) ?? new NativeByteBuffer(0, pooled: true);
            _window.EnsureCapacity(length);
            if (_source.ReadAt(_sourceStart + from, _window.Span[..(int)length]) != length)
                throw new ParquetFormatException("The file ends inside a column chunk.");
            _windowStart = from;
            _windowEnd = from + length;
        }
        return _window!.Memory.Slice((int)(from - _windowStart), (int)count);
    }

    private PageHeader? _peeked;       // the next page's header, parsed ahead by TryPeek
    private int _peekedLength;

    /// <summary>The data pages returned by <see cref="TryNext"/> (read and decompressed).</summary>
    public int DataPagesRead { get; private set; }

    /// <summary>The pages passed over by <see cref="SkipPeeked"/> (their bodies never read nor decompressed).</summary>
    public int SkippedPages { get; private set; }

    /// <summary>The header of the next page, its body not yet read; false at the end of the chunk.</summary>
    /// <exception cref="ParquetFormatException">The header is damaged.</exception>
    public bool TryPeek(out PageHeader header)
    {
        if (_ahead is not null)
        {
            // Skipping pages reads in order from here on: pages already read ahead cannot be peeked at.
            if (_ahead.Count > 0) throw new InvalidOperationException("Pages read ahead are taken in order, not peeked at.");
            Recycle();
            _ahead = null;
        }
        if (_peeked is null)
        {
            if (_position >= _length)
            {
                header = null!;
                return false;
            }
            _peeked = ParseHeader(out _peekedLength);
        }
        header = _peeked;
        return true;
    }

    /// <summary>Passes over the page <see cref="TryPeek"/> returned, without reading its body.</summary>
    /// <exception cref="ParquetFormatException">The page's size runs past the column chunk.</exception>
    public void SkipPeeked()
    {
        PageHeader header = _peeked ?? throw new InvalidOperationException("No page was peeked.");
        int compressed = CompressedSize(header, _peekedLength);
        _position += _peekedLength + compressed;
        _peeked = null;
        Passed(header);
        SkippedPages++;
    }

    /// <summary>
    /// Positions on the next page by the offset index (the dictionary page first, when one lies before the indexed
    /// pages) and parses its header, checking its size against the index.
    /// </summary>
    private bool TryNextIndexed(out PageHeader header, out int headerLength)
    {
        ChunkPageIndex index = _index!;
        if (_dictionaryPending)
        {
            _dictionaryPending = false;
            _position = 0;
            header = ParseHeader(out headerLength, index.Offsets[0] - _chunkStart);
            if (header.Type == PageType.DictionaryPage)
            {
                IndexedRows = null;
                return true;
            }
        }
        if (_nextPage >= index.PageCount)
        {
            (header, headerLength) = (null!, 0);
            return false;
        }
        _position = index.Offsets[_nextPage] - _chunkStart;
        header = ParseHeader(out headerLength, index.Sizes[_nextPage]);
        if ((long)headerLength + header.CompressedPageSize != index.Sizes[_nextPage])
            throw new ParquetFormatException($"Data page {_nextPage} takes {(long)headerLength + header.CompressedPageSize} bytes; the offset index says {index.Sizes[_nextPage]}.");
        IndexedRows = index.RowsOf(_nextPage);
        _nextPage++;
        return true;
    }

    /// <summary>
    /// Parses the header at the current position (from a file: the first 64 KiB, then more for large statistics), within
    /// the next <paramref name="within"/> bytes (the page's size, when the offset index gives it).
    /// </summary>
    private PageHeader ParseHeader(out int headerLength, long within = long.MaxValue)
    {
        if (_crypto is not null) return ParseEncryptedHeader(out headerLength, within);
        // From a file, the header is parsed from the first 64 KiB, then (headers with large statistics) from more,
        // up to the rest of the chunk, which is what a chunk in memory offers at once.
        long rest = Math.Min(_length - _position, within), offered = _source is null ? rest : Math.Min(rest, _headerAhead ? 1024 : 64 * 1024);
        while (true)
        {
            try
            {
                return PageHeader.Parse(Bytes(_position, offered).Span, out headerLength, skipStatistics: true);
            }
            catch (ParquetFormatException) when (offered < rest)
            {
                offered = Math.Min(rest, offered * 16);
            }
        }
    }

    /// <summary>
    /// Decrypts and parses the header module at the current position: the first one of a chunk is its dictionary
    /// page's when it has one (tried first: the AAD tells them apart), any other a data page's of the next ordinal.
    /// </summary>
    private PageHeader ParseEncryptedHeader(out int headerLength, long within)
    {
        ColumnDecryptor crypto = _crypto!;
        long rest = Math.Min(_length - _position, within);
        int stored = ModuleCipher.StoredLength(Bytes(_position, 4).Span, rest, $"a page header of column '{crypto.Name}'");
        ReadOnlySpan<byte> module = Bytes(_position, stored).Span;
        int length = crypto.PlainLength(ParquetModule.DataPageHeader, module);
        _plainHeader ??= _pool?.Rent(length) ?? new NativeByteBuffer(0, pooled: true);
        _plainHeader.EnsureCapacity(length);
        Span<byte> plain = _plainHeader.Span[..length];
        int ordinal = _index is null ? _dataOrdinal : _nextPage;
        bool opened = _position == 0 && crypto.TryDecrypt(ParquetModule.DictionaryPageHeader, -1, module, plain)
                      || crypto.TryDecrypt(ParquetModule.DataPageHeader, ordinal, module, plain);
        if (!opened) throw crypto.WrongKey();
        _headerOrdinal = ordinal;
        headerLength = stored;
        return PageHeader.Parse(plain, out _, skipStatistics: true);
    }

    /// <summary>The plaintext of an encrypted page body (after its stored bytes were checked against the CRC).</summary>
    private ReadOnlyMemory<byte> DecryptBody(PageHeader header, ReadOnlyMemory<byte> stored)
    {
        ColumnDecryptor crypto = _crypto!;
        ParquetModule type = header.Type == PageType.DictionaryPage ? ParquetModule.DictionaryPage : ParquetModule.DataPage;
        int length = crypto.PlainLength(type, stored.Span);
        _plainPage ??= _pool?.Rent(length) ?? new NativeByteBuffer(0, pooled: true);
        _plainPage.EnsureCapacity(length);
        if (!crypto.TryDecrypt(type, _headerOrdinal, stored.Span, _plainPage.Span[..length])) throw crypto.WrongKey();
        return _plainPage.Memory[..length];
    }

    /// <summary>Counts a data page passed (read or skipped) when reading in order: its ordinal names the next one's modules.</summary>
    private void Passed(PageHeader header)
    {
        if (header.Type is PageType.DataPage or PageType.DataPageV2) _dataOrdinal++;
    }

    /// <summary>The stored size of a page's body, checked against its header and the rest of the chunk.</summary>
    private int CompressedSize(PageHeader header, int headerLength)
    {
        int compressed = header.CompressedPageSize, uncompressed = header.UncompressedPageSize;
        if (compressed < 0 || uncompressed < 0) throw new ParquetFormatException($"A page header gives negative sizes ({compressed}, {uncompressed}).");
        long left = _length - _position - headerLength;
        if (compressed > left)
            throw new ParquetFormatException($"A page of {compressed} bytes runs past the end of its column chunk ({left} bytes left).");
        return compressed;
    }

    /// <summary>Chunk bytes from the fetched piece holding <paramref name="from"/>, up to <paramref name="count"/> (clipped to the piece).</summary>
    private ReadOnlyMemory<byte> Segment(long from, long count)
    {
        foreach (var (start, bytes) in _segments!)
        {
            if (from >= start && from < start + bytes.Length) return bytes.Slice((int)(from - start), (int)Math.Min(count, start + bytes.Length - from));
        }
        throw new ParquetFormatException($"A page lies outside the bytes fetched for its column chunk (at {from}).");
    }

    /// <summary>The next page, or false at the end of the chunk.</summary>
    /// <exception cref="ParquetFormatException">A header or size is damaged, a CRC does not match, or decompression fails.</exception>
    public bool TryNext(out Page page)
    {
        if (_ahead is not null && _peeked is null)
        {
            Recycle();
            TopUp();
            if (_ahead.TryDequeue(out Ahead? next))
            {
                _current = next; // its body is the page's until the next call
                next.Complete();
                if (next.Header.Type is PageType.DataPage or PageType.DataPageV2) DataPagesRead++;
                page = new Page(next.Header, next.Body);
                return true;
            }
        }
        page = default;
        PageHeader header;
        int headerLength;
        if (_index is null)
        {
            if (!TryPeek(out header)) return false;
            headerLength = _peekedLength;
            _peeked = null;
        }
        else if (!TryNextIndexed(out header, out headerLength)) return false;
        int compressed = CompressedSize(header, headerLength), uncompressed = header.UncompressedPageSize;
        ReadOnlyMemory<byte> stored = Bytes(_position, headerLength + compressed)[headerLength..];
        _position += headerLength + compressed;
        // Fuzzing: the page's bytes end at a guard page, so that a decoder reading past them faults.
        if (GuardedMemory.Enabled) stored = Guard(ref _guardedStored, GuardedMemory.Block.Copy(stored.Span)).Memory;

        // The CRC of an encrypted page covers its stored module (measured: pyarrow, both algorithms).
        if (_verifyChecksums && header.Crc is int crc && Crc32.HashToUInt32(stored.Span) != unchecked((uint)crc))
            throw new ParquetFormatException($"A {header.Type} page fails its CRC check.");
        if (_crypto is not null)
        {
            if (header.Type is PageType.DataPage or PageType.DataPageV2 or PageType.DictionaryPage)
            {
                stored = DecryptBody(header, stored);
                if (GuardedMemory.Enabled) stored = Guard(ref _guardedStored, GuardedMemory.Block.Copy(stored.Span)).Memory;
            }
            if (_index is null) Passed(header);
        }

        if (header.Type is PageType.DataPage or PageType.DataPageV2) DataPagesRead++;
        page = new Page(header, BodyOf(header, stored, _codec, this));
        return true;
    }

    /// <summary>The scratch buffer; while fuzzing, a block ending at a guard page.</summary>
    Memory<byte> IPageMemory.Take(int bytes) =>
        GuardedMemory.Enabled ? Guard(ref _guardedBody, new GuardedMemory.Block(bytes)).Memory : Scratch(bytes).Memory[..bytes];

    /// <summary>A page's body as decoders take it (decompressed, a v2 page's levels first) from its stored bytes.</summary>
    private static ReadOnlyMemory<byte> BodyOf(PageHeader header, ReadOnlyMemory<byte> stored, CompressionCodec codec, IPageMemory memory) => header.Type switch
    {
        PageType.DataPageV2 => BodyV2(header, stored, codec, memory),
        PageType.DataPage or PageType.DictionaryPage => Decompress(stored, header.UncompressedPageSize, codec, memory),
        _ => ReadOnlyMemory<byte>.Empty, // index pages and page types from the future: skipped by the caller
    };

    private static ReadOnlyMemory<byte> Decompress(ReadOnlyMemory<byte> stored, int uncompressed, CompressionCodec codec, IPageMemory memory)
    {
        if (stored.IsEmpty && uncompressed == 0) return ReadOnlyMemory<byte>.Empty; // an empty page: nothing to decompress
        if (codec == CompressionCodec.Uncompressed)
        {
            if (stored.Length != uncompressed)
                throw new ParquetFormatException($"An uncompressed page holds {stored.Length} bytes; its header says {uncompressed}.");
            return stored;
        }
        Memory<byte> body = memory.Take(uncompressed);
        PageCodecs.Decompress(codec, stored.Span, body.Span);
        return body;
    }

    /// <summary>Puts <paramref name="block"/> in <paramref name="slot"/>, unmapping the block it held.</summary>
    private static GuardedMemory.Block Guard(ref GuardedMemory.Block? slot, GuardedMemory.Block block)
    {
        ((IDisposable?)slot)?.Dispose();
        slot = block;
        return block;
    }

    /// <summary>A data page v2: levels are never compressed; values are unless <c>is_compressed</c> is false or there are none.</summary>
    private static ReadOnlyMemory<byte> BodyV2(PageHeader header, ReadOnlyMemory<byte> stored, CompressionCodec codec, IPageMemory memory)
    {
        DataPageHeaderV2 v2 = header.DataPageHeaderV2 ?? throw new ParquetFormatException("A DATA_PAGE_V2 has no data_page_header_v2.");
        int levels = v2.RepetitionLevelsByteLength + v2.DefinitionLevelsByteLength;
        if (v2.RepetitionLevelsByteLength < 0 || v2.DefinitionLevelsByteLength < 0 || levels > stored.Length || levels > header.UncompressedPageSize)
            throw new ParquetFormatException("A DATA_PAGE_V2 gives level lengths beyond the page.");
        int values = header.UncompressedPageSize - levels;
        ReadOnlyMemory<byte> storedValues = stored[levels..];
        if (codec == CompressionCodec.Uncompressed || !v2.IsCompressed || (storedValues.IsEmpty && values == 0))
        {
            // Some writers flag compression on pages without value bytes; there is nothing to decompress then.
            if (storedValues.Length != values)
                throw new ParquetFormatException($"A DATA_PAGE_V2 holds {storedValues.Length} value bytes; its header says {values}.");
            return stored;
        }
        Memory<byte> body = memory.Take(header.UncompressedPageSize);
        stored.Span[..levels].CopyTo(body.Span);
        PageCodecs.Decompress(codec, storedValues.Span, body.Span[levels..]);
        return body;
    }

    /// <summary>The page read ahead that was returned last goes back to the spares (its body is no longer the caller's).</summary>
    private void Recycle()
    {
        if (_current is null) return;
        _spareAhead.Push(_current);
        _current = null;
    }

    /// <summary>
    /// Queues the next pages, up to <see cref="AheadPages"/>, each with a helper while the budget leaves threads: its
    /// header parsed here (the next one's position depends on it), the rest by the helper (or by the reader, when it
    /// needs the page before a helper took it).
    /// </summary>
    private void TopUp()
    {
        Queue<Ahead> ahead = _ahead!;
        WorkBudget budget = _aheadBudget!;
        while (ahead.Count < AheadPages && !_aheadStopped && _position < _length && budget.TryEnter())
        {
            Ahead next = _spareAhead.TryPop(out Ahead? spare) ? spare : new Ahead(this);
            ahead.Enqueue(next);
            PagesReadAhead++;
            try
            {
                _headerAhead = true;
                PageHeader header = ParseHeader(out int headerLength);
                _headerAhead = false;
                int compressed = CompressedSize(header, headerLength);
                long start = _position + headerLength;
                next.Prepare(header, _headerOrdinal, start, compressed, _source is null ? Bytes(start, compressed) : default);
                _position = start + compressed;
                if (_crypto is not null) Passed(header);
            }
            catch (Exception e)
            {
                _headerAhead = false;
                // Thrown where the page is taken, as reading in order would: a chunk read to its last value never
                // reaches what lies past it.
                next.Prepare(null!, 0, 0, 0, default, e);
                _aheadStopped = true;
                budget.Exit();
                return;
            }
            next.Help(budget);
        }
    }

    /// <summary>
    /// A page read ahead: its stored bytes (in memory, or read from the file by whoever runs it), checked against the
    /// CRC, decrypted (with a decryptor of its own: helpers run side by side) and decompressed into buffers of its own,
    /// reused by the next page it holds.
    /// </summary>
    private sealed class Ahead(PageReader reader) : ClaimedWork, IPageMemory
    {
        private long _start;
        private int _length, _ordinal;
        private ReadOnlyMemory<byte> _stored; // in memory; empty: read from the file
        private Exception? _failure;
        private NativeByteBuffer? _input, _plain, _output;
        private ColumnDecryptor? _crypto;

        public PageHeader Header { get; private set; } = null!;

        /// <summary>The page's body, once done.</summary>
        public ReadOnlyMemory<byte> Body { get; private set; }

        /// <summary>The page to read next (or, with <paramref name="failure"/>, one that failed before its header was known: taking it throws).</summary>
        public void Prepare(PageHeader header, int ordinal, long start, int length, ReadOnlyMemory<byte> stored, Exception? failure = null)
        {
            (Header, _ordinal, _start, _length, _stored, _failure, Body) = (header, ordinal, start, length, stored, failure, default);
            Reset(); // last: from here on a helper may run it
        }

        protected override void Execute()
        {
            if (_failure is { } failure) ExceptionDispatchInfo.Throw(failure);
            ReadOnlyMemory<byte> stored = _stored;
            bool own = false;
            if (reader._source is { } source)
            {
                _input = Buffer(_input, _length);
                if (source.ReadAt(reader._sourceStart + _start, _input.Span[.._length]) != _length)
                    throw new ParquetFormatException("The file ends inside a column chunk.");
                stored = _input.Memory[.._length];
                own = true;
            }
            if (reader._verifyChecksums && Header.Crc is int crc && Crc32.HashToUInt32(stored.Span) != unchecked((uint)crc))
                throw new ParquetFormatException($"A {Header.Type} page fails its CRC check.");
            if (reader._crypto is { } shared && Header.Type is PageType.DataPage or PageType.DataPageV2 or PageType.DictionaryPage)
            {
                _crypto ??= shared.Fork();
                ParquetModule type = Header.Type == PageType.DictionaryPage ? ParquetModule.DictionaryPage : ParquetModule.DataPage;
                int length = _crypto.PlainLength(type, stored.Span);
                // Decrypted in place in a buffer of its own (the plaintext where the ciphertext lies), else into another.
                Memory<byte> plain = own ? _input!.Memory.Slice(ModuleCipher.PlainOffset, length) : (_plain = Buffer(_plain, length)).Memory[..length];
                if (!_crypto.TryDecrypt(type, _ordinal, stored.Span, plain.Span)) throw _crypto.WrongKey();
                stored = plain;
            }
            Body = BodyOf(Header, stored, reader._codec, this);
        }

        Memory<byte> IPageMemory.Take(int bytes) => (_output = Buffer(_output, bytes)).Memory[..bytes];

        private NativeByteBuffer Buffer(NativeByteBuffer? buffer, int bytes)
        {
            buffer ??= reader._pool?.Rent(bytes) ?? new NativeByteBuffer(0, pooled: true);
            buffer.EnsureCapacity(Math.Max(bytes, 1));
            return buffer;
        }

        /// <summary>Lets the page go (never run when no helper took it, else waited for), then its buffers.</summary>
        public void Release()
        {
            Drop();
            reader.Return(ref _input);
            reader.Return(ref _plain);
            reader.Return(ref _output);
            _crypto?.Dispose();
            _crypto = null;
        }
    }

    public void Dispose()
    {
        // The pages read ahead first: helpers may still be reading into their buffers.
        while (_ahead?.TryDequeue(out Ahead? ahead) == true) ahead.Release();
        _current?.Release();
        _current = null;
        while (_spareAhead.TryPop(out Ahead? spare)) spare.Release();
        Return(ref _scratch);
        Return(ref _window);
        Return(ref _plainHeader);
        Return(ref _plainPage);
        _crypto?.Dispose();
        _crypto = null;
        ((IDisposable?)_guardedStored)?.Dispose();
        ((IDisposable?)_guardedBody)?.Dispose();
        _guardedStored = _guardedBody = null;
    }

    private void Return(ref NativeByteBuffer? buffer)
    {
        if (buffer is null) return;
        if (_pool is null) buffer.Release();
        else _pool.Return(buffer);
        buffer = null;
    }
}
