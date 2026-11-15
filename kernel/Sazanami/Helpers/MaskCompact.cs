using System.Numerics;
using System.Runtime.InteropServices;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// Row filtering inside decoding, the counterpart of <see cref="MaskExpand"/>: of <c>n</c> slots whose values are
/// stored densely (one per valid slot), the values of the slots a keep mask selects, in order. Masks are 64-bit words
/// (slot i is bit i % 64 of word i / 64, LSB first, bits past the last slot clear), as the filters make them. A word
/// of kept slots all valid is one block copy; otherwise each kept valid slot is found by its bit and its value by its
/// rank among the valid slots.
/// </summary>
internal static class MaskCompact
{
    /// <summary>
    /// Copies the values of the kept valid slots of <paramref name="n"/> slots to <paramref name="target"/>, in order;
    /// returns how many. <paramref name="values"/> holds a value per valid slot (<paramref name="valid"/> empty: every
    /// slot is valid).
    /// </summary>
    /// <exception cref="ArgumentException">The masks are shorter than the slots, or the values or the target too short.</exception>
    public static int Values<T>(ReadOnlySpan<T> values, ReadOnlySpan<ulong> keep, ReadOnlySpan<ulong> valid, int n, Span<T> target)
    {
        int words = Check(keep, valid, n);
        if (values.Length < (valid.IsEmpty ? n : Count(valid, words))) throw new ArgumentException("Fewer values than valid slots.", nameof(values));
        if (target.Length < Kept(keep, valid, words)) throw new ArgumentException("The target is shorter than the values kept.", nameof(target));
        int j = 0, rank = 0;
        for (int w = 0; w < words; w++)
        {
            ulong kept = keep[w];
            if (valid.IsEmpty)
            {
                if (kept == ulong.MaxValue)
                {
                    values.Slice(w << 6, 64).CopyTo(target[j..]);
                    j += 64;
                    continue;
                }
                for (; kept != 0; kept &= kept - 1) target[j++] = values[(w << 6) + BitOperations.TrailingZeroCount(kept)];
                continue;
            }
            ulong present = valid[w];
            if (kept == ulong.MaxValue && present == ulong.MaxValue)
            {
                values.Slice(rank, 64).CopyTo(target[j..]);
                j += 64;
            }
            else
            {
                for (kept &= present; kept != 0; kept &= kept - 1)
                    target[j++] = values[rank + BitOperations.PopCount(present & ((1UL << BitOperations.TrailingZeroCount(kept)) - 1))];
            }
            rank += BitOperations.PopCount(present);
        }
        return j;
    }

    /// <summary>
    /// As <see cref="Values{T}"/> for values of <paramref name="width"/> bytes each (PLAIN fixed-width values of any
    /// width): copies <paramref name="width"/> bytes per kept valid slot; returns how many values.
    /// </summary>
    public static int Values(ReadOnlySpan<byte> values, int width, ReadOnlySpan<ulong> keep, ReadOnlySpan<ulong> valid, int n, Span<byte> target)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(width);
        switch (width)
        {
            case 1: return Values(values, keep, valid, n, target);
            case 2: return Values(MemoryMarshal.Cast<byte, ushort>(values), keep, valid, n, MemoryMarshal.Cast<byte, ushort>(target));
            case 4: return Values(MemoryMarshal.Cast<byte, uint>(values), keep, valid, n, MemoryMarshal.Cast<byte, uint>(target));
            case 8: return Values(MemoryMarshal.Cast<byte, ulong>(values), keep, valid, n, MemoryMarshal.Cast<byte, ulong>(target));
            case 16: return Values(MemoryMarshal.Cast<byte, Int128>(values), keep, valid, n, MemoryMarshal.Cast<byte, Int128>(target));
        }
        int words = Check(keep, valid, n);
        if (values.Length / width < (valid.IsEmpty ? n : Count(valid, words))) throw new ArgumentException("Fewer values than valid slots.", nameof(values));
        if (target.Length / width < Kept(keep, valid, words)) throw new ArgumentException("The target is shorter than the values kept.", nameof(target));
        int j = 0, rank = 0;
        for (int w = 0; w < words; w++)
        {
            ulong present = valid.IsEmpty ? ulong.MaxValue : valid[w];
            for (ulong kept = keep[w] & present; kept != 0; kept &= kept - 1, j++)
            {
                int bit = BitOperations.TrailingZeroCount(kept);
                int at = valid.IsEmpty ? (w << 6) + bit : rank + BitOperations.PopCount(present & ((1UL << bit) - 1));
                values.Slice(at * width, width).CopyTo(target[(j * width)..]);
            }
            if (!valid.IsEmpty) rank += BitOperations.PopCount(present);
        }
        return j;
    }

    /// <summary>
    /// The bits of the kept slots, packed: bit j of <paramref name="target"/> is the bit of <paramref name="bits"/> at
    /// the j-th kept slot (a validity of the slots kept); bits past them are 0. Returns how many are set.
    /// </summary>
    public static int Bits(ReadOnlySpan<ulong> bits, ReadOnlySpan<ulong> keep, int n, Span<ulong> target)
    {
        int words = Check(keep, default, n);
        if (bits.Length < words) throw new ArgumentException("The bits are fewer than the slots.", nameof(bits));
        int kept = Kept(keep, default, words);
        if (target.Length < (kept + 63) >> 6) throw new ArgumentException("The target is shorter than the bits kept.", nameof(target));
        target[..((kept + 63) >> 6)].Clear();
        int j = 0, set = 0;
        for (int w = 0; w < words; w++)
        {
            ulong selected = keep[w], source = bits[w];
            if (selected == ulong.MaxValue)
            {
                // A whole word lands across at most two target words.
                target[j >> 6] |= source << (j & 63);
                if ((j & 63) != 0) target[(j >> 6) + 1] |= source >> (64 - (j & 63));
                set += BitOperations.PopCount(source);
                j += 64;
                continue;
            }
            for (; selected != 0; selected &= selected - 1, j++)
            {
                ulong bit = source >> BitOperations.TrailingZeroCount(selected) & 1;
                target[j >> 6] |= bit << (j & 63);
                set += (int)bit;
            }
        }
        return set;
    }

    // The words of n slots, checked against the masks (a validity may be empty).
    private static int Check(ReadOnlySpan<ulong> keep, ReadOnlySpan<ulong> valid, int n)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(n);
        int words = (int)(((uint)n + 63) >> 6);
        if (keep.Length < words) throw new ArgumentException("The keep mask is shorter than the slots.", nameof(keep));
        if (!valid.IsEmpty && valid.Length < words) throw new ArgumentException("The validity is shorter than the slots.", nameof(valid));
        return words;
    }

    private static int Count(ReadOnlySpan<ulong> mask, int words)
    {
        int count = 0;
        foreach (ulong word in mask[..words]) count += BitOperations.PopCount(word);
        return count;
    }

    private static int Kept(ReadOnlySpan<ulong> keep, ReadOnlySpan<ulong> valid, int words)
    {
        int count = 0;
        for (int w = 0; w < words; w++) count += BitOperations.PopCount(valid.IsEmpty ? keep[w] : keep[w] & valid[w]);
        return count;
    }
}
