using System.Buffers.Binary;
using System.Numerics;

namespace Kaminari.Sazanami.Filters;

/// <summary>
/// A split block Bloom filter (Parquet's BloomFilter.md, as Impala and Kudu define it): blocks of 256 bits in eight
/// 32-bit words; a 64-bit hash picks a block with its upper half and sets or tests one bit per word from its lower half
/// times eight salts. The caller hashes (Parquet: XXH64 with seed 0 of the value's PLAIN encoding).
/// </summary>
public sealed class SplitBlockBloomFilter
{
    /// <summary>The bytes of a block.</summary>
    public const int BlockBytes = 32;

    private static readonly uint[] Salt = [0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU, 0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U];

    private readonly byte[] _bitset;

    /// <summary>A filter over <paramref name="bitset"/> (little-endian words), its length a positive multiple of <see cref="BlockBytes"/>.</summary>
    /// <exception cref="ArgumentException">The bitset is empty or not whole blocks.</exception>
    public SplitBlockBloomFilter(byte[] bitset)
    {
        ArgumentNullException.ThrowIfNull(bitset);
        if (bitset.Length == 0 || bitset.Length % BlockBytes != 0)
            throw new ArgumentException($"A split block Bloom filter is whole blocks of {BlockBytes} bytes, not {bitset.Length} bytes.", nameof(bitset));
        _bitset = bitset;
    }

    /// <summary>An empty filter of <paramref name="blocks"/> blocks.</summary>
    public static SplitBlockBloomFilter Empty(int blocks) => new(new byte[checked(blocks * BlockBytes)]);

    /// <summary>The bitset (for writing it out).</summary>
    public ReadOnlySpan<byte> Bitset => _bitset;

    private int BlockOf(ulong hash) => (int)(((hash >> 32) * (ulong)(_bitset.Length / BlockBytes)) >> 32);

    /// <summary>Records a value by its hash.</summary>
    public void Insert(ulong hash)
    {
        Span<byte> block = _bitset.AsSpan(BlockOf(hash) * BlockBytes, BlockBytes);
        uint key = (uint)hash;
        for (int i = 0; i < 8; i++)
        {
            Span<byte> word = block.Slice(i * 4, 4);
            BinaryPrimitives.WriteUInt32LittleEndian(word, BinaryPrimitives.ReadUInt32LittleEndian(word) | Bit(key, i));
        }
    }

    /// <summary>False only when no value with this hash was inserted.</summary>
    public bool MightContain(ulong hash)
    {
        ReadOnlySpan<byte> block = _bitset.AsSpan(BlockOf(hash) * BlockBytes, BlockBytes);
        uint key = (uint)hash;
        for (int i = 0; i < 8; i++)
        {
            uint bit = Bit(key, i);
            if ((BinaryPrimitives.ReadUInt32LittleEndian(block.Slice(i * 4, 4)) & bit) == 0) return false;
        }
        return true;
    }

    /// <summary>The bit of word <paramref name="i"/>: the top five bits of the key times the word's salt.</summary>
    private static uint Bit(uint key, int i) => 1U << (int)((key * Salt[i]) >> 27);

    /// <summary>The number of blocks for <paramref name="distinct"/> values at a false positive rate (rounded up to a power of two).</summary>
    public static int BlocksFor(long distinct, double falsePositiveRate)
    {
        // Parquet's formula: bits m = -8 n / ln(1 - p^(1/8)).
        double bits = -8.0 * Math.Max(1, distinct) / Math.Log(1 - Math.Pow(falsePositiveRate, 1.0 / 8));
        long blocks = (long)Math.Ceiling(bits / (BlockBytes * 8));
        return (int)Math.Min(BitOperations.RoundUpToPowerOf2((ulong)Math.Max(1, blocks)), 1 << 22);
    }
}
