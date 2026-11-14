using System.Numerics;
using System.Runtime.CompilerServices;

namespace Kaminari.Sazanami.Encodings;

/// <summary>
/// The fast path of LSB-first bit unpacking: whole groups of 8 values (a group of width w takes exactly w bytes; widths
/// above 32 only into 64-bit integers),
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
    private readonly struct B33 : IBits { public static int Value => 33; }
    private readonly struct B34 : IBits { public static int Value => 34; }
    private readonly struct B35 : IBits { public static int Value => 35; }
    private readonly struct B36 : IBits { public static int Value => 36; }
    private readonly struct B37 : IBits { public static int Value => 37; }
    private readonly struct B38 : IBits { public static int Value => 38; }
    private readonly struct B39 : IBits { public static int Value => 39; }
    private readonly struct B40 : IBits { public static int Value => 40; }
    private readonly struct B41 : IBits { public static int Value => 41; }
    private readonly struct B42 : IBits { public static int Value => 42; }
    private readonly struct B43 : IBits { public static int Value => 43; }
    private readonly struct B44 : IBits { public static int Value => 44; }
    private readonly struct B45 : IBits { public static int Value => 45; }
    private readonly struct B46 : IBits { public static int Value => 46; }
    private readonly struct B47 : IBits { public static int Value => 47; }
    private readonly struct B48 : IBits { public static int Value => 48; }
    private readonly struct B49 : IBits { public static int Value => 49; }
    private readonly struct B50 : IBits { public static int Value => 50; }
    private readonly struct B51 : IBits { public static int Value => 51; }
    private readonly struct B52 : IBits { public static int Value => 52; }
    private readonly struct B53 : IBits { public static int Value => 53; }
    private readonly struct B54 : IBits { public static int Value => 54; }
    private readonly struct B55 : IBits { public static int Value => 55; }
    private readonly struct B56 : IBits { public static int Value => 56; }
    private readonly struct B57 : IBits { public static int Value => 57; }
    private readonly struct B58 : IBits { public static int Value => 58; }
    private readonly struct B59 : IBits { public static int Value => 59; }
    private readonly struct B60 : IBits { public static int Value => 60; }
    private readonly struct B61 : IBits { public static int Value => 61; }
    private readonly struct B62 : IBits { public static int Value => 62; }
    private readonly struct B63 : IBits { public static int Value => 63; }
    private readonly struct B64 : IBits { public static int Value => 64; }

    /// <summary>
    /// How many whole groups of 8 values, starting at byte <paramref name="startByte"/>, the fast path may read from
    /// <paramref name="length"/> bytes (each needs <paramref name="bitWidth"/> bytes plus 8 readable after it).
    /// </summary>
    public static long SafeGroups(long length, long startByte, int bitWidth) =>
        bitWidth == 0 ? long.MaxValue : Math.Max(0, (length - startByte - 8) / bitWidth);

    /// <summary>
    /// Unpacks <paramref name="groups"/> groups of 8 values of <paramref name="bitWidth"/> bits: 1–32, or up to 64
    /// into 64-bit integers.
    /// </summary>
    public static void Groups<T>(byte* source, T* destination, int groups, int bitWidth) where T : unmanaged, IBinaryInteger<T>
    {
        if (bitWidth > 8 * sizeof(T)) throw new ArgumentOutOfRangeException(nameof(bitWidth), $"{bitWidth}-bit values do not fit {typeof(T).Name}.");
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
            33 => &Groups<B33, T>,
            34 => &Groups<B34, T>,
            35 => &Groups<B35, T>,
            36 => &Groups<B36, T>,
            37 => &Groups<B37, T>,
            38 => &Groups<B38, T>,
            39 => &Groups<B39, T>,
            40 => &Groups<B40, T>,
            41 => &Groups<B41, T>,
            42 => &Groups<B42, T>,
            43 => &Groups<B43, T>,
            44 => &Groups<B44, T>,
            45 => &Groups<B45, T>,
            46 => &Groups<B46, T>,
            47 => &Groups<B47, T>,
            48 => &Groups<B48, T>,
            49 => &Groups<B49, T>,
            50 => &Groups<B50, T>,
            51 => &Groups<B51, T>,
            52 => &Groups<B52, T>,
            53 => &Groups<B53, T>,
            54 => &Groups<B54, T>,
            55 => &Groups<B55, T>,
            56 => &Groups<B56, T>,
            57 => &Groups<B57, T>,
            58 => &Groups<B58, T>,
            59 => &Groups<B59, T>,
            60 => &Groups<B60, T>,
            61 => &Groups<B61, T>,
            62 => &Groups<B62, T>,
            63 => &Groups<B63, T>,
            64 => &Groups<B64, T>,
            _ => throw new ArgumentOutOfRangeException(nameof(bitWidth)),
        };
        kernel(source, destination, groups);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static void Groups<TBits, T>(byte* src, T* dst, int groups) where TBits : IBits where T : unmanaged, IBinaryInteger<T>
    {
        int w = TBits.Value;
        for (int g = 0; g < groups; g++, src += w, dst += 8)
        {
            dst[0] = Value<TBits, T>(src, 0);
            dst[1] = Value<TBits, T>(src, 1);
            dst[2] = Value<TBits, T>(src, 2);
            dst[3] = Value<TBits, T>(src, 3);
            dst[4] = Value<TBits, T>(src, 4);
            dst[5] = Value<TBits, T>(src, 5);
            dst[6] = Value<TBits, T>(src, 6);
            dst[7] = Value<TBits, T>(src, 7);
        }
    }

    /// <summary>
    /// Value <paramref name="k"/> of a group (constants fold: one unaligned load and a shift; values wider than 56
    /// bits that start mid-byte take a ninth byte, which lies within the 8 readable bytes after the group).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T Value<TBits, T>(byte* src, int k) where TBits : IBits where T : unmanaged, IBinaryInteger<T>
    {
        int w = TBits.Value, bit = k * w, shift = bit & 7;
        ulong mask = w == 64 ? ulong.MaxValue : (1UL << w) - 1;
        ulong value = Unsafe.ReadUnaligned<ulong>(src + (bit >> 3)) >> shift;
        if (shift + w > 64) value |= (ulong)src[(bit >> 3) + 8] << (64 - shift);
        return T.CreateTruncating(value & mask);
    }
}
