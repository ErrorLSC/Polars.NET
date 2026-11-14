using System.IO.Hashing;
using Kaminari.Sazanami.Helpers;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet.Codecs;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet.Reading;

/// <summary>A page of a column chunk, uncompressed: for a data page v2, its levels followed by its values.</summary>
internal readonly record struct Page(PageHeader Header, ReadOnlyMemory<byte> Body);

/// <summary>
/// Walks the pages of one column chunk (its bytes in memory): parses each header, checks sizes and, when asked,
/// the CRC, and decompresses the page into a scratch buffer that the next page reuses.
/// </summary>
internal sealed class PageReader(ReadOnlyMemory<byte> chunk, CompressionCodec codec, bool verifyChecksums, NativeBufferPool? pool = null) : IDisposable
{
    private NativeByteBuffer? _scratch;
    private int _position;

    /// <summary>The scratch buffer, at least <paramref name="bytes"/> long (taken from the pool on first use).</summary>
    private NativeByteBuffer Scratch(int bytes)
    {
        if (_scratch is null) _scratch = pool?.Rent(bytes) ?? new NativeByteBuffer(0, pooled: true);
        _scratch.EnsureCapacity(bytes);
        return _scratch;
    }

    /// <summary>Whether bytes remain after the pages read so far.</summary>
    public bool HasMore => _position < chunk.Length;

    /// <summary>The next page, or false at the end of the chunk.</summary>
    /// <exception cref="ParquetFormatException">A header or size is damaged, a CRC does not match, or decompression fails.</exception>
    public bool TryNext(out Page page)
    {
        page = default;
        if (_position >= chunk.Length) return false;
        ReadOnlySpan<byte> rest = chunk.Span[_position..];
        PageHeader header = PageHeader.Parse(rest, out int headerLength);
        int compressed = header.CompressedPageSize, uncompressed = header.UncompressedPageSize;
        if (compressed < 0 || uncompressed < 0) throw new ParquetFormatException($"A page header gives negative sizes ({compressed}, {uncompressed}).");
        if (compressed > rest.Length - headerLength)
            throw new ParquetFormatException($"A page of {compressed} bytes runs past the end of its column chunk ({rest.Length - headerLength} bytes left).");
        ReadOnlyMemory<byte> stored = chunk.Slice(_position + headerLength, compressed);
        _position += headerLength + compressed;

        if (verifyChecksums && header.Crc is int crc && Crc32.HashToUInt32(stored.Span) != unchecked((uint)crc))
            throw new ParquetFormatException($"A {header.Type} page fails its CRC check.");

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
        if (codec == CompressionCodec.Uncompressed)
        {
            if (stored.Length != uncompressed)
                throw new ParquetFormatException($"An uncompressed page holds {stored.Length} bytes; its header says {uncompressed}.");
            return stored;
        }
        NativeByteBuffer scratch = Scratch(uncompressed);
        PageCodecs.Decompress(codec, stored.Span, scratch.Span[..uncompressed]);
        return scratch.Memory[..uncompressed];
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
        if (codec == CompressionCodec.Uncompressed || !v2.IsCompressed || (storedValues.IsEmpty && values == 0))
        {
            // Some writers flag compression on pages without value bytes; there is nothing to decompress then.
            if (storedValues.Length != values)
                throw new ParquetFormatException($"A DATA_PAGE_V2 holds {storedValues.Length} value bytes; its header says {values}.");
            return stored;
        }
        NativeByteBuffer scratch = Scratch(header.UncompressedPageSize);
        Span<byte> body = scratch.Span[..header.UncompressedPageSize];
        stored.Span[..levels].CopyTo(body);
        PageCodecs.Decompress(codec, storedValues.Span, body[levels..]);
        return scratch.Memory[..header.UncompressedPageSize];
    }

    public void Dispose()
    {
        if (_scratch is null) return;
        if (pool is null) _scratch.Release();
        else pool.Return(_scratch);
        _scratch = null;
    }
}
