using System.IO.Hashing;
using Kaminari.Sazanami.Helpers;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet.Reading;

/// <summary>A page of a column chunk, uncompressed: for a data page v2, its levels followed by its values.</summary>
internal readonly record struct Page(PageHeader Header, ReadOnlyMemory<byte> Body);

/// <summary>
/// Walks the pages of one column chunk: parses each header, checks sizes and, when asked, the CRC, and decompresses
/// the page into a scratch buffer that the next page reuses. The chunk is either in memory (fetched, or a slice of a
/// source that lives in memory) or read from a local file as it goes, through a window of about a megabyte that the
/// pages reuse: each page is then read once from the page cache into memory the caches still hold when it is decoded,
/// instead of the whole row group going through main memory twice. A page's body is valid until the next page.
/// </summary>
internal sealed class PageReader : IDisposable
{
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
    public bool HasMore => _position < _length;

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
            long length = Math.Min(Math.Max(count, ReadAhead), Math.Max(ReadLimit(from), from + count) - from);
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
        // From a file, the header is parsed from the first 64 KiB, then (headers with large statistics) from more,
        // up to the rest of the chunk, which is what a chunk in memory offers at once.
        long rest = Math.Min(_length - _position, within), offered = _source is null ? rest : Math.Min(rest, 64 * 1024);
        while (true)
        {
            try
            {
                return PageHeader.Parse(Bytes(_position, offered).Span, out headerLength);
            }
            catch (ParquetFormatException) when (offered < rest)
            {
                offered = Math.Min(rest, offered * 16);
            }
        }
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

        if (_verifyChecksums && header.Crc is int crc && Crc32.HashToUInt32(stored.Span) != unchecked((uint)crc))
            throw new ParquetFormatException($"A {header.Type} page fails its CRC check.");

        if (header.Type is PageType.DataPage or PageType.DataPageV2) DataPagesRead++;
        page = new Page(header, header.Type switch
        {
            PageType.DataPageV2 => BodyV2(header, stored),
            PageType.DataPage or PageType.DictionaryPage => Decompress(stored, uncompressed),
            _ => ReadOnlyMemory<byte>.Empty, // index pages and page types from the future: skipped by the caller
        });
        return true;
    }

    private ReadOnlyMemory<byte> Decompress(ReadOnlyMemory<byte> stored, int uncompressed)
    {
        if (stored.IsEmpty && uncompressed == 0) return ReadOnlyMemory<byte>.Empty; // an empty page: nothing to decompress
        if (_codec == CompressionCodec.Uncompressed)
        {
            if (stored.Length != uncompressed)
                throw new ParquetFormatException($"An uncompressed page holds {stored.Length} bytes; its header says {uncompressed}.");
            return stored;
        }
        if (GuardedMemory.Enabled)
        {
            GuardedMemory.Block body = Guard(ref _guardedBody, new GuardedMemory.Block(uncompressed));
            PageCodecs.Decompress(_codec, stored.Span, body.GetSpan());
            return body.Memory;
        }
        NativeByteBuffer scratch = Scratch(uncompressed);
        PageCodecs.Decompress(_codec, stored.Span, scratch.Span[..uncompressed]);
        return scratch.Memory[..uncompressed];
    }

    /// <summary>Puts <paramref name="block"/> in <paramref name="slot"/>, unmapping the block it held.</summary>
    private static GuardedMemory.Block Guard(ref GuardedMemory.Block? slot, GuardedMemory.Block block)
    {
        ((IDisposable?)slot)?.Dispose();
        slot = block;
        return block;
    }

    /// <summary>A data page v2: levels are never compressed; values are unless <c>is_compressed</c> is false or there are none.</summary>
    private ReadOnlyMemory<byte> BodyV2(PageHeader header, ReadOnlyMemory<byte> stored)
    {
        DataPageHeaderV2 v2 = header.DataPageHeaderV2 ?? throw new ParquetFormatException("A DATA_PAGE_V2 has no data_page_header_v2.");
        int levels = v2.RepetitionLevelsByteLength + v2.DefinitionLevelsByteLength;
        if (v2.RepetitionLevelsByteLength < 0 || v2.DefinitionLevelsByteLength < 0 || levels > stored.Length || levels > header.UncompressedPageSize)
            throw new ParquetFormatException("A DATA_PAGE_V2 gives level lengths beyond the page.");
        int values = header.UncompressedPageSize - levels;
        ReadOnlyMemory<byte> storedValues = stored[levels..];
        if (_codec == CompressionCodec.Uncompressed || !v2.IsCompressed || (storedValues.IsEmpty && values == 0))
        {
            // Some writers flag compression on pages without value bytes; there is nothing to decompress then.
            if (storedValues.Length != values)
                throw new ParquetFormatException($"A DATA_PAGE_V2 holds {storedValues.Length} value bytes; its header says {values}.");
            return stored;
        }
        if (GuardedMemory.Enabled)
        {
            GuardedMemory.Block guarded = Guard(ref _guardedBody, new GuardedMemory.Block(header.UncompressedPageSize));
            stored.Span[..levels].CopyTo(guarded.GetSpan());
            PageCodecs.Decompress(_codec, storedValues.Span, guarded.GetSpan()[levels..]);
            return guarded.Memory;
        }
        NativeByteBuffer scratch = Scratch(header.UncompressedPageSize);
        Span<byte> body = scratch.Span[..header.UncompressedPageSize];
        stored.Span[..levels].CopyTo(body);
        PageCodecs.Decompress(_codec, storedValues.Span, body[levels..]);
        return scratch.Memory[..header.UncompressedPageSize];
    }

    public void Dispose()
    {
        Return(ref _scratch);
        Return(ref _window);
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
