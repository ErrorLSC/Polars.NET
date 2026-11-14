using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Kaminari.Sazanami.Helpers;

namespace Kaminari.Sazanami.Filters;

/// <summary>
/// Row masks as 64-bit words (row <c>i</c> is bit <c>i % 64</c> of word <c>i / 64</c>, Arrow's LSB-first order): the
/// algebra filters combine them with. Masks of <c>length</c> rows keep the bits past the last row clear.
/// </summary>
internal static class Masks
{
    /// <summary>The words a mask of <paramref name="length"/> rows takes.</summary>
    public static int Words(int length) => (int)(((uint)length + 63) >> 6);

    /// <summary>The bits of the last word that are rows.</summary>
    public static ulong LastWord(int length) => (length & 63) == 0 ? ulong.MaxValue : (1UL << (length & 63)) - 1;

    /// <summary>Sets every row of the mask (or clears them), clearing the bits past the last row.</summary>
    public static void Fill(Span<ulong> mask, int length, bool value)
    {
        int words = Words(length);
        if (!value)
        {
            mask[..words].Clear();
            return;
        }
        mask[..words].Fill(ulong.MaxValue);
        if (words > 0) mask[words - 1] = LastWord(length);
    }

    /// <summary><c>target &amp;= other</c>.</summary>
    public static void And(Span<ulong> target, ReadOnlySpan<ulong> other)
    {
        if (other.Length < target.Length) throw new ArgumentException("The masks differ in length.", nameof(other));
        ref ulong t = ref MemoryMarshal.GetReference(target);
        ref ulong o = ref MemoryMarshal.GetReference(other);
        int i = 0;
        if (Vector256.IsHardwareAccelerated)
        {
            for (; i + Vector256<ulong>.Count <= target.Length; i += Vector256<ulong>.Count)
                (Vector256.LoadUnsafe(ref t, (nuint)i) & Vector256.LoadUnsafe(ref o, (nuint)i)).StoreUnsafe(ref t, (nuint)i);
        }
        for (; i < target.Length; i++) Unsafe.Add(ref t, i) &= Unsafe.Add(ref o, i);
    }

    /// <summary><c>target |= other</c>.</summary>
    public static void Or(Span<ulong> target, ReadOnlySpan<ulong> other)
    {
        if (other.Length < target.Length) throw new ArgumentException("The masks differ in length.", nameof(other));
        ref ulong t = ref MemoryMarshal.GetReference(target);
        ref ulong o = ref MemoryMarshal.GetReference(other);
        int i = 0;
        if (Vector256.IsHardwareAccelerated)
        {
            for (; i + Vector256<ulong>.Count <= target.Length; i += Vector256<ulong>.Count)
                (Vector256.LoadUnsafe(ref t, (nuint)i) | Vector256.LoadUnsafe(ref o, (nuint)i)).StoreUnsafe(ref t, (nuint)i);
        }
        for (; i < target.Length; i++) Unsafe.Add(ref t, i) |= Unsafe.Add(ref o, i);
    }

    /// <summary><c>target &amp;= ~other</c>.</summary>
    public static void AndNot(Span<ulong> target, ReadOnlySpan<ulong> other)
    {
        if (other.Length < target.Length) throw new ArgumentException("The masks differ in length.", nameof(other));
        ref ulong t = ref MemoryMarshal.GetReference(target);
        ref ulong o = ref MemoryMarshal.GetReference(other);
        int i = 0;
        if (Vector256.IsHardwareAccelerated)
        {
            for (; i + Vector256<ulong>.Count <= target.Length; i += Vector256<ulong>.Count)
                Vector256.AndNot(Vector256.LoadUnsafe(ref t, (nuint)i), Vector256.LoadUnsafe(ref o, (nuint)i)).StoreUnsafe(ref t, (nuint)i);
        }
        for (; i < target.Length; i++) Unsafe.Add(ref t, i) &= ~Unsafe.Add(ref o, i);
    }

    /// <summary>Flips every row of a mask of <paramref name="length"/> rows (the bits past the last row stay clear).</summary>
    public static void Not(Span<ulong> mask, int length)
    {
        int words = Words(length);
        for (int i = 0; i < words; i++) mask[i] = ~mask[i];
        if (words > 0) mask[words - 1] &= LastWord(length);
    }

    /// <summary>The number of set bits.</summary>
    public static int Count(ReadOnlySpan<ulong> mask)
    {
        int set = 0;
        foreach (ulong word in mask) set += BitOperations.PopCount(word);
        return set;
    }

    /// <summary>Whether no bit is set.</summary>
    public static bool IsZero(ReadOnlySpan<ulong> mask) => mask.IndexOfAnyExcept(0UL) < 0;

    /// <summary>Whether every row of a mask of <paramref name="length"/> rows is set.</summary>
    public static bool IsFull(ReadOnlySpan<ulong> mask, int length)
    {
        int words = Words(length);
        if (words == 0) return true;
        return mask[..(words - 1)].IndexOfAnyExcept(ulong.MaxValue) < 0 && (mask[words - 1] & LastWord(length)) == LastWord(length);
    }

    /// <summary>
    /// Reads <paramref name="length"/> bits of an LSB-first bitmap from bit <paramref name="firstBit"/> (an Arrow
    /// validity or boolean buffer at an array's offset) into a mask, clearing the bits past the last row.
    /// </summary>
    public static void Load(ReadOnlySpan<byte> bitmap, long firstBit, int length, Span<ulong> mask)
    {
        if (firstBit < 0 || (firstBit + length + 7) >> 3 > bitmap.Length) throw new ArgumentOutOfRangeException(nameof(length), "The bitmap holds fewer bits.");
        int words = Words(length);
        if ((firstBit & 7) == 0)
        {
            int bytes = (length + 7) >> 3;
            Span<byte> target = MemoryMarshal.AsBytes(mask[..words]);
            bitmap.Slice((int)(firstBit >> 3), bytes).CopyTo(target);
            target[bytes..].Clear();
        }
        else
        {
            for (int w = 0; w < words; w++) mask[w] = Bits.Word(bitmap, firstBit + 64L * w);
        }
        if (words > 0) mask[words - 1] &= LastWord(length);
    }
}
