using System.Buffers.Binary;
using System.Numerics;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// Bit ranges of LSB-first bitmaps (Arrow validity, Parquet's bit-packed booleans and levels): read 64 bits from any
/// offset, copy, fill and count ranges. Whole 64-bit words where the range allows, single bits only at its ends;
/// reads never go past the source.
/// </summary>
internal static class Bits
{
    /// <summary>Bit <paramref name="bit"/>.</summary>
    public static bool Get(ReadOnlySpan<byte> bitmap, long bit) => (bitmap[(int)(bit >> 3)] >> (int)(bit & 7) & 1) != 0;

    /// <summary>Up to 64 bits from <paramref name="bit"/> (fewer when the bitmap ends; the missing ones read as 0).</summary>
    public static ulong Word(ReadOnlySpan<byte> bitmap, long bit)
    {
        int at = (int)(bit >> 3), shift = (int)(bit & 7);
        if (at + 9 <= bitmap.Length)
        {
            ulong word = BinaryPrimitives.ReadUInt64LittleEndian(bitmap[at..]);
            return shift == 0 ? word : word >> shift | (ulong)bitmap[at + 8] << (64 - shift);
        }
        ulong value = 0;
        // Fewer than 9 bytes left: as many as there are.
        for (int k = 0; k < 9 && at + k < bitmap.Length; k++)
        {
            int position = 8 * k - shift;
            value |= position >= 0 ? (ulong)bitmap[at + k] << position : (ulong)bitmap[at + k] >> -position;
        }
        return value;
    }

    /// <summary>The number of set bits among <paramref name="count"/> bits from <paramref name="bit"/>.</summary>
    public static long Count(ReadOnlySpan<byte> bitmap, long bit, long count)
    {
        long set = 0;
        for (; count >= 64; bit += 64, count -= 64) set += BitOperations.PopCount(Word(bitmap, bit));
        if (count > 0) set += BitOperations.PopCount(Word(bitmap, bit) & ((1UL << (int)count) - 1));
        return set;
    }

    /// <summary>Copies <paramref name="count"/> bits from <paramref name="sourceBit"/> of <paramref name="source"/> to <paramref name="destinationBit"/> of <paramref name="destination"/>.</summary>
    public static void Copy(ReadOnlySpan<byte> source, long sourceBit, Span<byte> destination, long destinationBit, long count)
    {
        if (count <= 0) return;
        if (((sourceBit + count + 7) >> 3) > source.Length || ((destinationBit + count + 7) >> 3) > destination.Length)
            throw new ArgumentOutOfRangeException(nameof(count));
        // 56 bits at a time: shifted by at most 7 they stay within the 8 bytes read, modified and written back.
        while (count > 0)
        {
            int n = (int)Math.Min(count, Chunk);
            Merge(destination, destinationBit, n, Word(source, sourceBit));
            sourceBit += n;
            destinationBit += n;
            count -= n;
        }
    }

    /// <summary>Sets (or clears) <paramref name="count"/> bits from <paramref name="bit"/>.</summary>
    public static void Fill(Span<byte> bitmap, long bit, long count, bool value)
    {
        if (count <= 0) return;
        if (((bit + count + 7) >> 3) > bitmap.Length) throw new ArgumentOutOfRangeException(nameof(count));
        ulong bits = value ? ulong.MaxValue : 0;
        while (count > 0)
        {
            int n = (int)Math.Min(count, Chunk);
            Merge(bitmap, bit, n, bits);
            bit += n;
            count -= n;
        }
    }

    private const int Chunk = 56;

    /// <summary>Writes the low <paramref name="count"/> (at most 56) bits of <paramref name="bits"/> at <paramref name="bit"/>, leaving the bits around them.</summary>
    private static void Merge(Span<byte> bitmap, long bit, int count, ulong bits)
    {
        int at = (int)(bit >> 3), shift = (int)(bit & 7);
        ulong mask = ((1UL << count) - 1) << shift;
        if (at + 8 <= bitmap.Length)
        {
            Span<byte> word = bitmap[at..];
            ulong old = BinaryPrimitives.ReadUInt64LittleEndian(word);
            BinaryPrimitives.WriteUInt64LittleEndian(word, (old & ~mask) | ((bits << shift) & mask));
            return;
        }
        for (int k = 0; k < count; k++) Set(bitmap, bit + k, (bits >> k & 1) != 0);
    }

    private static void Set(Span<byte> bitmap, long bit, bool value)
    {
        ref byte b = ref bitmap[(int)(bit >> 3)];
        int flag = 1 << (int)(bit & 7);
        b = value ? (byte)(b | flag) : (byte)(b & ~flag);
    }
}
