using System.IO.Hashing;
using System.Numerics;
using Kaminari.Sazanami.Encodings;
using Kaminari.Sazanami.Filters;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>
/// The Bloom filter of a column chunk being written: each value hashed as Parquet's filters hash it (XXH64, seed 0, of
/// its PLAIN bytes, byte arrays without their length) as it enters a page, whatever the page's encoding turns out to
/// be. The distinct hashes are kept (Sazanami's open-addressing table) and the filter sized for them when the chunk is
/// finished, at most for <see cref="ParquetBloomFilterOptions.DistinctValues"/>, cut to
/// <see cref="ParquetBloomFilterOptions.MaxBytes"/>.
/// </summary>
internal sealed class BloomBuilder(ParquetBloomFilterOptions options)
{
    private readonly FixedValueDictionary<ulong> _distinct = new();

    /// <summary>The most blocks a filter takes: <see cref="ParquetBloomFilterOptions.MaxBytes"/>, rounded down to a power of two.</summary>
    private int MaxBlocks => (int)BitOperations.RoundUpToPowerOf2((uint)(options.MaxBytes / SplitBlockBloomFilter.BlockBytes) + 1) / 2;

    public void Add(ReadOnlySpan<byte> plain) => _distinct.GetOrAdd(XxHash64.HashToUInt64(plain));

    /// <summary>The chunk's filter bitset; the builder is then ready for the next chunk.</summary>
    public byte[] Finish()
    {
        long distinct = Math.Max(1, _distinct.Count);
        if (options.DistinctValues is long most) distinct = Math.Min(distinct, most);
        var filter = SplitBlockBloomFilter.Empty(Math.Min(SplitBlockBloomFilter.BlocksFor(distinct, options.FalsePositiveRate), MaxBlocks));
        foreach (ulong hash in _distinct.Values) filter.Insert(hash);
        _distinct.Clear();
        return filter.Bitset.ToArray();
    }
}
