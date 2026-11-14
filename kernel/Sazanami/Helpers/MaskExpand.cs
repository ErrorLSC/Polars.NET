using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// Null expansion: values stored densely (one per valid slot) laid out over slots by a mask (<see cref="SlotMask"/>:
/// bytes, zero = null, or the bits of a validity bitmap), with the null slots zeroed. The mask is read 64 slots at a
/// time as a word of bits (vector compares of bytes, or the bitmap's bits as they are); a word all valid is one block
/// copy, a word all null one clear, and a mixed word visits its set bits and then its clear ones, so no branch depends
/// on a single slot. Every slot is written, so the slots may start uninitialized. The number of values must equal the
/// mask's valid slots (checked first).
/// </summary>
internal static class MaskExpand
{
    /// <summary>
    /// Moves the first <paramref name="valid"/> slots of <paramref name="slots"/> to the valid positions of
    /// <paramref name="mask"/> (in place, from the end) and zeroes the others.
    /// </summary>
    public static void InPlace<T>(Span<T> slots, int valid, SlotMask mask)
    {
        int n = mask.Length;
        if (slots.Length < n) throw new ArgumentOutOfRangeException(nameof(slots));
        Check(mask, valid);
        if (valid == n) return;
        ref T s = ref MemoryMarshal.GetReference(slots);
        int j = valid - 1; // the next value to place, moving down
        int b = n & ~63;
        if (b < n)
        {
            int length = n - b;
            Backward(ref s, b, mask.PartialWord(b, length), Range(length), ref j);
        }
        for (b -= 64; b >= 0; b -= 64)
        {
            // Everything from here down is valid: the values already sit where they belong.
            if (j == b + 63) return;
            ulong word = mask.Word(b);
            if (word == ulong.MaxValue)
            {
                MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref s, j - 63), 64).CopyTo(MemoryMarshal.CreateSpan(ref Unsafe.Add(ref s, b), 64));
                j -= 64;
            }
            else if (word == 0) MemoryMarshal.CreateSpan(ref Unsafe.Add(ref s, b), 64).Clear();
            else Backward(ref s, b, word, ulong.MaxValue, ref j);
        }
    }

    /// <summary>
    /// Lays <paramref name="values"/> (one per valid slot) over the first <paramref name="mask"/>.Length slots of
    /// <paramref name="slots"/>, zeroing the null ones.
    /// </summary>
    public static void Scatter<T>(ReadOnlySpan<T> values, Span<T> slots, SlotMask mask)
    {
        int n = mask.Length;
        if (slots.Length < n) throw new ArgumentOutOfRangeException(nameof(slots));
        Check(mask, values.Length);
        ref T s = ref MemoryMarshal.GetReference(slots);
        ref T v = ref MemoryMarshal.GetReference(values);
        int j = 0;
        int b = 0;
        for (; b + 64 <= n; b += 64)
        {
            ulong word = mask.Word(b);
            if (word == ulong.MaxValue) MemoryMarshal.CreateReadOnlySpan(ref Unsafe.Add(ref v, j), 64).CopyTo(MemoryMarshal.CreateSpan(ref Unsafe.Add(ref s, b), 64));
            else if (word == 0) MemoryMarshal.CreateSpan(ref Unsafe.Add(ref s, b), 64).Clear();
            else Forward(ref Unsafe.Add(ref s, b), word, ulong.MaxValue, ref Unsafe.Add(ref v, j));
            j += BitOperations.PopCount(word);
        }
        if (b < n)
        {
            int length = n - b;
            Forward(ref Unsafe.Add(ref s, b), mask.PartialWord(b, length), Range(length), ref Unsafe.Add(ref v, j));
        }
    }

    /// <summary>
    /// Lays the dictionary entries <paramref name="indices"/> name (one per valid slot, each already checked to be
    /// within <paramref name="dictionary"/>) over the first <paramref name="mask"/>.Length slots, zeroing the null ones.
    /// </summary>
    public static void Gather<T>(ReadOnlySpan<T> dictionary, ReadOnlySpan<int> indices, Span<T> slots, SlotMask mask)
    {
        int n = mask.Length;
        if (slots.Length < n) throw new ArgumentOutOfRangeException(nameof(slots));
        Check(mask, indices.Length);
        ref T s = ref MemoryMarshal.GetReference(slots);
        ref T d = ref MemoryMarshal.GetReference(dictionary);
        ref int x = ref MemoryMarshal.GetReference(indices);
        int j = 0;
        int b = 0;
        for (; b + 64 <= n; b += 64)
        {
            ulong word = mask.Word(b);
            GatherWord(ref Unsafe.Add(ref s, b), word, ulong.MaxValue, ref d, ref Unsafe.Add(ref x, j));
            j += BitOperations.PopCount(word);
        }
        if (b < n) GatherWord(ref Unsafe.Add(ref s, b), mask.PartialWord(b, n - b), Range(n - b), ref d, ref Unsafe.Add(ref x, j));
    }

    // The valid slots of one word take the values from j down (set bits high to low), then its null slots are
    // zeroed: what is still to be placed lies below the word, so zeroing it clobbers nothing.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Backward<T>(ref T s, int b, ulong word, ulong range, ref int j)
    {
        ref T slot = ref Unsafe.Add(ref s, b);
        for (ulong set = word; set != 0;)
        {
            int k = 63 - BitOperations.LeadingZeroCount(set);
            Unsafe.Add(ref slot, k) = Unsafe.Add(ref s, j--);
            set ^= 1UL << k;
        }
        for (ulong clear = ~word & range; clear != 0; clear &= clear - 1) Unsafe.Add(ref slot, BitOperations.TrailingZeroCount(clear)) = default!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Forward<T>(ref T slot, ulong word, ulong range, ref T values)
    {
        int j = 0;
        for (ulong set = word; set != 0; set &= set - 1) Unsafe.Add(ref slot, BitOperations.TrailingZeroCount(set)) = Unsafe.Add(ref values, j++);
        for (ulong clear = ~word & range; clear != 0; clear &= clear - 1) Unsafe.Add(ref slot, BitOperations.TrailingZeroCount(clear)) = default!;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void GatherWord<T>(ref T slot, ulong word, ulong range, ref T dictionary, ref int indices)
    {
        if (word == ulong.MaxValue)
        {
            for (int k = 0; k < 64; k++) Unsafe.Add(ref slot, k) = Unsafe.Add(ref dictionary, Unsafe.Add(ref indices, k));
            return;
        }
        int j = 0;
        for (ulong set = word; set != 0; set &= set - 1)
            Unsafe.Add(ref slot, BitOperations.TrailingZeroCount(set)) = Unsafe.Add(ref dictionary, Unsafe.Add(ref indices, j++));
        for (ulong clear = ~word & range; clear != 0; clear &= clear - 1) Unsafe.Add(ref slot, BitOperations.TrailingZeroCount(clear)) = default!;
    }

    private static ulong Range(int length) => length == 64 ? ulong.MaxValue : (1UL << length) - 1;

    /// <summary>64 mask bytes as a word: bit k set where byte k is not zero.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong Word(ref byte p)
    {
        if (Vector256.IsHardwareAccelerated)
        {
            ulong lo = Vector256.ExtractMostSignificantBits(Vector256.Equals(Vector256.LoadUnsafe(ref p), Vector256<byte>.Zero));
            ulong hi = Vector256.ExtractMostSignificantBits(Vector256.Equals(Vector256.LoadUnsafe(ref p, 32), Vector256<byte>.Zero));
            return ~(lo | hi << 32);
        }
        if (Vector128.IsHardwareAccelerated)
        {
            ulong w0 = Vector128.ExtractMostSignificantBits(Vector128.Equals(Vector128.LoadUnsafe(ref p), Vector128<byte>.Zero));
            ulong w1 = Vector128.ExtractMostSignificantBits(Vector128.Equals(Vector128.LoadUnsafe(ref p, 16), Vector128<byte>.Zero));
            ulong w2 = Vector128.ExtractMostSignificantBits(Vector128.Equals(Vector128.LoadUnsafe(ref p, 32), Vector128<byte>.Zero));
            ulong w3 = Vector128.ExtractMostSignificantBits(Vector128.Equals(Vector128.LoadUnsafe(ref p, 48), Vector128<byte>.Zero));
            return ~(w0 | w1 << 16 | w2 << 32 | w3 << 48);
        }
        return PartialWord(ref p, 64);
    }

    /// <summary>The first <paramref name="length"/> (at most 64) mask bytes as a word.</summary>
    internal static ulong PartialWord(ref byte p, int length)
    {
        ulong word = 0;
        for (int k = 0; k < length; k++) word |= (Unsafe.Add(ref p, k) != 0 ? 1UL : 0) << k;
        return word;
    }

    /// <summary>Checks that <paramref name="mask"/> has exactly <paramref name="values"/> valid slots.</summary>
    private static void Check(SlotMask mask, int values)
    {
        int valid = mask.CountValid();
        if (valid != values) throw new ArgumentException($"The mask has {valid} valid slots for {values} values.");
    }
}
