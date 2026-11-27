using System.Collections.Concurrent;
using System.IO.Hashing;
using Kaminari.Sazanami.Filters;
using Kaminari.Seseragi.Files.Parquet.Encryption;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// The Bloom filters of a file's column chunks, read when first asked for (nearby ones in one read) and kept for every
/// reader of the file. A filter that is missing, lies outside the file, is not a split block filter of XXH64 hashes
/// over uncompressed bits, or does not parse, is taken as absent: it then rules nothing out.
/// </summary>
internal sealed class BloomFilters(ParquetFile file)
{
    private const long CoalesceGap = 1 << 20;
    private const int MaxBytes = 128 << 20;
    private const int HeaderGuess = 64; // a header takes about 15 bytes; writers that give no length are read in two steps

    private readonly ConcurrentDictionary<(int Group, int Leaf), SplitBlockBloomFilter?> _filters = new();

    /// <summary>The filter of a chunk loaded before (null when it has none or it is unusable).</summary>
    public SplitBlockBloomFilter? Get(int group, int leaf) => _filters.TryGetValue((group, leaf), out SplitBlockBloomFilter? f) ? f : null;

    /// <summary>Whether a chunk's metadata points at a Bloom filter.</summary>
    public bool Has(int group, int leaf) => file.Thrift.RowGroups[group].Columns[leaf].MetaData?.BloomFilterOffset is long offset && offset >= 0;

    /// <summary>The hash a value's PLAIN bytes have in a Parquet Bloom filter (XXH64, seed 0).</summary>
    public static ulong Hash(ReadOnlySpan<byte> plain) => XxHash64.HashToUInt64(plain);

    /// <summary>Loads the filters of <paramref name="chunks"/> not loaded yet.</summary>
    public async ValueTask LoadAsync(IEnumerable<(int Group, int Leaf)> chunks, bool useAsync, CancellationToken ct)
    {
        List<(long Start, int Length, (int, int) Key)> wanted = [];
        foreach (var key in chunks.Distinct())
        {
            if (_filters.ContainsKey(key)) continue;
            ColumnMetaData? meta = file.Thrift.RowGroups[key.Group].Columns[key.Leaf].MetaData;
            long length = file.Source.Length;
            if (meta?.BloomFilterOffset is long offset && offset >= 0 && offset < length)
            {
                int bytes = meta.BloomFilterLength is int l && l > 0 && l <= MaxBytes + 1024 && offset <= length - l ? l : (int)Math.Min(HeaderGuess, length - offset);
                wanted.Add((offset, bytes, key));
            }
            else _filters.TryAdd(key, null);
        }
        if (wanted.Count == 0) return;
        wanted.Sort((a, b) => a.Start.CompareTo(b.Start));
        int i = 0;
        while (i < wanted.Count)
        {
            long start = wanted[i].Start, end = start + wanted[i].Length;
            int j = i + 1;
            while (j < wanted.Count && wanted[j].Start - end <= CoalesceGap && Math.Max(end, wanted[j].Start + wanted[j].Length) - start <= 64L << 20)
                end = Math.Max(end, wanted[j].Start + wanted[j++].Length);
            byte[] bytes = new byte[end - start];
            await ParquetFile.ReadExactlyAsync(file.Source, start, bytes, useAsync, ct).ConfigureAwait(false);
            for (; i < j; i++)
            {
                var (at, length, key) = wanted[i];
                _filters.TryAdd(key, await LoadAsync(key, bytes.AsMemory((int)(at - start), length), at, useAsync, ct).ConfigureAwait(false));
            }
        }
    }

    /// <summary>The filter of a chunk from the bytes read at its offset: in plain, or (an encrypted chunk) as two modules.</summary>
    private async ValueTask<SplitBlockBloomFilter?> LoadAsync((int Group, int Leaf) key, ReadOnlyMemory<byte> bytes, long offset, bool useAsync, CancellationToken ct)
    {
        if (file.Decryptor is not { } decryptor) return await ParseAsync(bytes, offset, useAsync, ct).ConfigureAwait(false);
        RowGroup group = file.Thrift.RowGroups[key.Group];
        using ColumnDecryptor? crypto = decryptor.TryForChunk(group.Columns[key.Leaf], group.Ordinal ?? key.Group, key.Leaf, out bool missing);
        if (missing) return null; // without the chunk's key its filter rules nothing out
        if (crypto is null) return await ParseAsync(bytes, offset, useAsync, ct).ConfigureAwait(false);
        try
        {
            byte[] headerModule = await ModuleAtAsync(bytes, offset, useAsync, ct).ConfigureAwait(false);
            BloomFilterHeader header = BloomFilterHeader.Parse(crypto.Decrypt(ParquetModule.BloomFilterHeader, -1, headerModule), out _);
            if (header.Algorithm != 1 || header.Hash != 1 || header.Compression != 1) return null;
            int n = header.NumBytes;
            if (n <= 0 || n > MaxBytes || n % SplitBlockBloomFilter.BlockBytes != 0) return null;
            long bitsetAt = offset + headerModule.Length;
            ReadOnlyMemory<byte> rest = bytes.Length > headerModule.Length ? bytes[headerModule.Length..] : ReadOnlyMemory<byte>.Empty;
            byte[] bitset = crypto.Decrypt(ParquetModule.BloomFilterBitset, -1, await ModuleAtAsync(rest, bitsetAt, useAsync, ct).ConfigureAwait(false));
            return bitset.Length == n ? new SplitBlockBloomFilter(bitset) : null;
        }
        catch (ParquetFormatException)
        {
            return null; // a damaged filter is no filter
        }
    }

    /// <summary>The encrypted module at <paramref name="offset"/> (its length first), from <paramref name="bytes"/> read there when they hold it.</summary>
    private async ValueTask<byte[]> ModuleAtAsync(ReadOnlyMemory<byte> bytes, long offset, bool useAsync, CancellationToken ct)
    {
        long available = file.Source.Length - offset;
        if (bytes.Length < 4)
        {
            if (available < 4) throw new ParquetFormatException("An encrypted Bloom filter runs past the end of the file.");
            byte[] start = new byte[4];
            await ParquetFile.ReadExactlyAsync(file.Source, offset, start, useAsync, ct).ConfigureAwait(false);
            bytes = start;
        }
        int stored = ModuleCipher.StoredLength(bytes.Span, Math.Min(available, MaxBytes + 1024), "a Bloom filter");
        if (bytes.Length >= stored) return bytes[..stored].ToArray();
        byte[] module = new byte[stored];
        await ParquetFile.ReadExactlyAsync(file.Source, offset, module, useAsync, ct).ConfigureAwait(false);
        return module;
    }

    /// <summary>A filter from its header and bitset at <paramref name="offset"/>; reads the bitset when only the header came.</summary>
    private async ValueTask<SplitBlockBloomFilter?> ParseAsync(ReadOnlyMemory<byte> bytes, long offset, bool useAsync, CancellationToken ct)
    {
        BloomFilterHeader header;
        int headerLength;
        try
        {
            header = BloomFilterHeader.Parse(bytes.Span, out headerLength);
        }
        catch (ParquetFormatException)
        {
            return null;
        }
        // Members by field id: algorithm BLOCK = 1, hash XXHASH = 1, compression UNCOMPRESSED = 1.
        if (header.Algorithm != 1 || header.Hash != 1 || header.Compression != 1) return null;
        int n = header.NumBytes;
        if (n <= 0 || n > MaxBytes || n % SplitBlockBloomFilter.BlockBytes != 0 || offset + headerLength > file.Source.Length - n) return null;
        byte[] bitset = new byte[n];
        if (bytes.Length >= headerLength + n) bytes.Slice(headerLength, n).CopyTo(bitset);
        else await ParquetFile.ReadExactlyAsync(file.Source, offset + headerLength, bitset, useAsync, ct).ConfigureAwait(false);
        return new SplitBlockBloomFilter(bitset);
    }
}
