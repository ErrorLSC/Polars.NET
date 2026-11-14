using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Kaminari.Sazanami.Encodings;

/// <summary>
/// Unpacking of values of 0–64 bits (above 32 into 64-bit integers only) packed least significant bit first (Parquet's hybrid encoding, Arrow bitmaps
/// and others): whole groups of 8 by the per-width kernels of <see cref="BitUnpack"/>, the rest bounds-checked.
/// </summary>
internal static class BitUnpacking
{
    /// <summary>
    /// Unpacks <paramref name="destination"/>.Length values of <paramref name="bitWidth"/> bits (at most 32, or 64 into
    /// 64-bit integers), the
    /// first at value index <paramref name="first"/> of <paramref name="packed"/>, packed least significant bit first.
    /// </summary>
    public static unsafe void Unpack<T>(ReadOnlySpan<byte> packed, long first, int bitWidth, Span<T> destination) where T : unmanaged, IBinaryInteger<T>
    {
        if (bitWidth == 0)
        {
            destination.Clear();
            return;
        }
        if ((uint)bitWidth > 8u * (uint)Unsafe.SizeOf<T>()) throw new ArgumentOutOfRangeException(nameof(bitWidth), $"{bitWidth}-bit values do not fit {typeof(T).Name}.");
        int done = 0;
        // Up to a group boundary value by value, then whole groups by the per-width kernel, then the rest.
        while (done < destination.Length && ((first + done) & 7) != 0) done++;
        UnpackChecked(packed, first, bitWidth, destination[..done]);
        if (BitConverter.IsLittleEndian && done < destination.Length)
        {
            long startByte = (first + done) / 8 * bitWidth;
            long groups = Math.Min((destination.Length - done) / 8, BitUnpack.SafeGroups(packed.Length, startByte, bitWidth));
            if (groups > 0)
            {
                fixed (byte* source = packed)
                fixed (T* target = destination)
                    BitUnpack.Groups(source + startByte, target + done, (int)groups, bitWidth);
                done += (int)groups * 8;
            }
        }
        UnpackChecked(packed, first + done, bitWidth, destination[done..]);
    }

    /// <summary>The bounds-checked path (and the reference the kernels are tested against).</summary>
    internal static void UnpackChecked<T>(ReadOnlySpan<byte> packed, long first, int bitWidth, Span<T> destination) where T : unmanaged, IBinaryInteger<T>
    {
        if (bitWidth == 0)
        {
            destination.Clear();
            return;
        }
        ulong mask = bitWidth == 64 ? ulong.MaxValue : (1UL << bitWidth) - 1;
        long bit = first * bitWidth;
        for (int i = 0; i < destination.Length; i++, bit += bitWidth)
        {
            int at = (int)(bit >> 3), shift = (int)(bit & 7);
            ulong word = at + 8 <= packed.Length ? BinaryPrimitives.ReadUInt64LittleEndian(packed[at..]) : Tail(packed, at);
            ulong value = word >> shift;
            // Wider than 56 bits from mid-byte: the ninth byte holds the top (missing past the end: zeros).
            if (shift + bitWidth > 64 && at + 8 < packed.Length) value |= (ulong)packed[at + 8] << (64 - shift);
            destination[i] = T.CreateTruncating(value & mask);
        }
    }

    /// <summary>The bytes from <paramref name="at"/> on, little-endian, when fewer than 8 remain.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static ulong Tail(ReadOnlySpan<byte> packed, int at)
    {
        ulong word = 0;
        for (int k = 0; at + k < packed.Length && k < 8; k++) word |= (ulong)packed[at + k] << (8 * k);
        return word;
    }

    /// <summary>The bytes <paramref name="count"/> values of <paramref name="bitWidth"/> bits take.</summary>
    public static long Bytes(long count, int bitWidth) => (count * bitWidth + 7) >> 3;
}
