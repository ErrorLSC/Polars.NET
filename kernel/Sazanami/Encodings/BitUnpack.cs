using System.Numerics;
using System.Runtime.CompilerServices;

namespace Kaminari.Sazanami.Encodings;

/// <summary>
/// The fast path of LSB-first bit unpacking: whole groups of 8 values (a group of width w takes exactly w bytes),
/// with one kernel per bit width so the shifts and offsets are constants and the group is unrolled. Each value is
/// read with an unaligned 8-byte load, so a group needs 8 readable bytes past its own (the caller leaves the last
/// groups of a buffer to the bounds-checked path). Portable scalar code: the same on x64 and arm64.
/// </summary>
internal static unsafe class BitUnpack
{
    private interface IBits
    {
        static abstract int Value { get; }
    }

    private readonly struct B1 : IBits { public static int Value => 1; }
    private readonly struct B2 : IBits { public static int Value => 2; }
    private readonly struct B3 : IBits { public static int Value => 3; }
    private readonly struct B4 : IBits { public static int Value => 4; }
    private readonly struct B5 : IBits { public static int Value => 5; }
    private readonly struct B6 : IBits { public static int Value => 6; }
    private readonly struct B7 : IBits { public static int Value => 7; }
    private readonly struct B8 : IBits { public static int Value => 8; }
    private readonly struct B9 : IBits { public static int Value => 9; }
    private readonly struct B10 : IBits { public static int Value => 10; }
    private readonly struct B11 : IBits { public static int Value => 11; }
    private readonly struct B12 : IBits { public static int Value => 12; }
    private readonly struct B13 : IBits { public static int Value => 13; }
    private readonly struct B14 : IBits { public static int Value => 14; }
    private readonly struct B15 : IBits { public static int Value => 15; }
    private readonly struct B16 : IBits { public static int Value => 16; }
    private readonly struct B17 : IBits { public static int Value => 17; }
    private readonly struct B18 : IBits { public static int Value => 18; }
    private readonly struct B19 : IBits { public static int Value => 19; }
    private readonly struct B20 : IBits { public static int Value => 20; }
    private readonly struct B21 : IBits { public static int Value => 21; }
    private readonly struct B22 : IBits { public static int Value => 22; }
    private readonly struct B23 : IBits { public static int Value => 23; }
    private readonly struct B24 : IBits { public static int Value => 24; }
    private readonly struct B25 : IBits { public static int Value => 25; }
    private readonly struct B26 : IBits { public static int Value => 26; }
    private readonly struct B27 : IBits { public static int Value => 27; }
    private readonly struct B28 : IBits { public static int Value => 28; }
    private readonly struct B29 : IBits { public static int Value => 29; }
    private readonly struct B30 : IBits { public static int Value => 30; }
    private readonly struct B31 : IBits { public static int Value => 31; }
    private readonly struct B32 : IBits { public static int Value => 32; }

    /// <summary>
    /// How many whole groups of 8 values, starting at byte <paramref name="startByte"/>, the fast path may read from
    /// <paramref name="length"/> bytes (each needs <paramref name="bitWidth"/> bytes plus 8 readable after it).
    /// </summary>
    public static long SafeGroups(long length, long startByte, int bitWidth) =>
        bitWidth == 0 ? long.MaxValue : Math.Max(0, (length - startByte - 8) / bitWidth);

    /// <summary>Unpacks <paramref name="groups"/> groups of 8 values of <paramref name="bitWidth"/> (1–32) bits.</summary>
    public static void Groups<T>(byte* source, T* destination, int groups, int bitWidth) where T : unmanaged, IBinaryInteger<T>
    {
        delegate*<byte*, T*, int, void> kernel = bitWidth switch
        {
            1 => &Groups<B1, T>,
            2 => &Groups<B2, T>,
            3 => &Groups<B3, T>,
            4 => &Groups<B4, T>,
            5 => &Groups<B5, T>,
            6 => &Groups<B6, T>,
            7 => &Groups<B7, T>,
            8 => &Groups<B8, T>,
            9 => &Groups<B9, T>,
            10 => &Groups<B10, T>,
            11 => &Groups<B11, T>,
            12 => &Groups<B12, T>,
            13 => &Groups<B13, T>,
            14 => &Groups<B14, T>,
            15 => &Groups<B15, T>,
            16 => &Groups<B16, T>,
            17 => &Groups<B17, T>,
            18 => &Groups<B18, T>,
            19 => &Groups<B19, T>,
            20 => &Groups<B20, T>,
            21 => &Groups<B21, T>,
            22 => &Groups<B22, T>,
            23 => &Groups<B23, T>,
            24 => &Groups<B24, T>,
            25 => &Groups<B25, T>,
            26 => &Groups<B26, T>,
            27 => &Groups<B27, T>,
            28 => &Groups<B28, T>,
            29 => &Groups<B29, T>,
            30 => &Groups<B30, T>,
            31 => &Groups<B31, T>,
            32 => &Groups<B32, T>,
            _ => throw new ArgumentOutOfRangeException(nameof(bitWidth)),
        };
        kernel(source, destination, groups);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static void Groups<TBits, T>(byte* src, T* dst, int groups) where TBits : IBits where T : unmanaged, IBinaryInteger<T>
    {
        int w = TBits.Value;
        ulong mask = (1UL << w) - 1;
        for (int g = 0; g < groups; g++, src += w, dst += 8)
        {
            dst[0] = T.CreateTruncating(Unsafe.ReadUnaligned<ulong>(src) & mask);
            dst[1] = T.CreateTruncating((Unsafe.ReadUnaligned<ulong>(src + (w >> 3)) >> (w & 7)) & mask);
            dst[2] = T.CreateTruncating((Unsafe.ReadUnaligned<ulong>(src + (2 * w >> 3)) >> (2 * w & 7)) & mask);
            dst[3] = T.CreateTruncating((Unsafe.ReadUnaligned<ulong>(src + (3 * w >> 3)) >> (3 * w & 7)) & mask);
            dst[4] = T.CreateTruncating((Unsafe.ReadUnaligned<ulong>(src + (4 * w >> 3)) >> (4 * w & 7)) & mask);
            dst[5] = T.CreateTruncating((Unsafe.ReadUnaligned<ulong>(src + (5 * w >> 3)) >> (5 * w & 7)) & mask);
            dst[6] = T.CreateTruncating((Unsafe.ReadUnaligned<ulong>(src + (6 * w >> 3)) >> (6 * w & 7)) & mask);
            dst[7] = T.CreateTruncating((Unsafe.ReadUnaligned<ulong>(src + (7 * w >> 3)) >> (7 * w & 7)) & mask);
        }
    }
}
