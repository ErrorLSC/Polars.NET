using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Kaminari.Sazanami.Text;

/// <summary>ASCII decimal digits read eight at a time (SWAR: one 64-bit word, the digits combined pairwise).</summary>
internal static class AsciiDigits
{
    /// <summary>The value of eight ASCII digits in <paramref name="v"/>, the first (most significant) in the lowest byte.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Eight(ulong v)
    {
        v -= 0x3030_3030_3030_3030;
        v = v * 10 + (v >> 8);
        return (uint)(((v & 0x0000_00FF_0000_00FF) * (100 + (1_000_000UL << 32))
            + ((v >> 16) & 0x0000_00FF_0000_00FF) * (1 + (10_000UL << 32))) >> 32);
    }

    /// <summary>
    /// The value of the <paramref name="count"/> (1–8) bytes at <paramref name="at"/> as decimal digits, most significant
    /// first; false when one is not a digit. Reads eight bytes from <paramref name="at"/> (the caller makes sure they are
    /// readable); the ones past <paramref name="count"/> take no part.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryUpToEight(ref byte at, int count, out uint value)
    {
        ulong bytes = Unsafe.ReadUnaligned<ulong>(ref at);
        if (!BitConverter.IsLittleEndian) bytes = BinaryPrimitives.ReverseEndianness(bytes);
        // The digits moved to the top of the word, '0's (leading zeros) below them.
        int shift = (8 - count) * 8;
        ulong v = (bytes << shift) | (0x3030_3030_3030_3030UL & ~(ulong.MaxValue << shift));
        // Every byte 0x30–0x39: its high nibble 3, and adding 6 to it does not carry into the high nibble.
        if (((v & 0xF0F0_F0F0_F0F0_F0F0UL) | (((v + 0x0606_0606_0606_0606UL) & 0xF0F0_F0F0_F0F0_F0F0UL) >> 4)) != 0x3333_3333_3333_3333UL)
        {
            value = 0;
            return false;
        }
        value = Eight(v);
        return true;
    }
}
