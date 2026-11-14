using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Kaminari.Sazanami.Encodings;

/// <summary>
/// Byte stream split (Parquet's BYTE_STREAM_SPLIT): values of <c>K</c> bytes stored as K streams of N bytes, stream j
/// holding byte j of every value. Joining them back, 2-, 4- and 8-byte values interleave 16 values at a time with
/// portable widening (byte streams into 16-bit pairs, into 32-bit quads, into 64-bit octets: shifts and ors only);
/// other widths, the tail and machines without vector support go value by value.
/// </summary>
internal static class ByteStreamSplit
{
    /// <summary>
    /// Writes the first <paramref name="count"/> values of <paramref name="streams"/> (K = <paramref name="width"/>
    /// streams of <paramref name="streams"/>.Length / K bytes each) to <paramref name="destination"/>, K bytes each.
    /// </summary>
    public static void Decode(ReadOnlySpan<byte> streams, int width, int count, Span<byte> destination)
    {
        if (width <= 0 || streams.Length % width != 0) throw new ArgumentException($"{streams.Length} bytes are not {width} streams.", nameof(streams));
        int stride = streams.Length / width;
        if ((uint)count > (uint)stride) throw new ArgumentOutOfRangeException(nameof(count), $"{count} values asked of streams of {stride}.");
        if (destination.Length < (long)count * width) throw new ArgumentException("The destination is too short.", nameof(destination));
        ref byte s = ref MemoryMarshal.GetReference(streams);
        ref byte d = ref MemoryMarshal.GetReference(destination);
        int done = Vector128.IsHardwareAccelerated ? width switch
        {
            2 => Two(ref s, stride, count, ref d),
            4 => Four(ref s, stride, count, ref d),
            8 => Eight(ref s, stride, count, ref d),
            _ => 0,
        } : 0;
        for (int i = done; i < count; i++)
        {
            for (int j = 0; j < width; j++) Unsafe.Add(ref d, i * width + j) = Unsafe.Add(ref s, j * stride + i);
        }
    }

    private static int Two(ref byte s, int stride, int count, ref byte d)
    {
        int i = 0;
        for (; i + 16 <= count; i += 16)
        {
            Vector128<byte> b0 = Load(ref s, i), b1 = Load(ref s, stride + i);
            Pairs(b0, b1, out Vector128<ushort> lo, out Vector128<ushort> hi);
            lo.AsByte().StoreUnsafe(ref d, (nuint)(2 * i));
            hi.AsByte().StoreUnsafe(ref d, (nuint)(2 * i + 16));
        }
        return i;
    }

    private static int Four(ref byte s, int stride, int count, ref byte d)
    {
        int i = 0;
        for (; i + 16 <= count; i += 16)
        {
            Pairs(Load(ref s, i), Load(ref s, stride + i), out Vector128<ushort> lo01, out Vector128<ushort> hi01);
            Pairs(Load(ref s, 2 * stride + i), Load(ref s, 3 * stride + i), out Vector128<ushort> lo23, out Vector128<ushort> hi23);
            nuint at = (nuint)(4 * i);
            Quads(lo01, lo23, 0).StoreUnsafe(ref d, at);
            Quads(lo01, lo23, 1).StoreUnsafe(ref d, at + 16);
            Quads(hi01, hi23, 0).StoreUnsafe(ref d, at + 32);
            Quads(hi01, hi23, 1).StoreUnsafe(ref d, at + 48);
        }
        return i;
    }

    private static int Eight(ref byte s, int stride, int count, ref byte d)
    {
        int i = 0;
        for (; i + 16 <= count; i += 16)
        {
            Pairs(Load(ref s, i), Load(ref s, stride + i), out Vector128<ushort> lo01, out Vector128<ushort> hi01);
            Pairs(Load(ref s, 2 * stride + i), Load(ref s, 3 * stride + i), out Vector128<ushort> lo23, out Vector128<ushort> hi23);
            Pairs(Load(ref s, 4 * stride + i), Load(ref s, 5 * stride + i), out Vector128<ushort> lo45, out Vector128<ushort> hi45);
            Pairs(Load(ref s, 6 * stride + i), Load(ref s, 7 * stride + i), out Vector128<ushort> lo67, out Vector128<ushort> hi67);
            nuint at = (nuint)(8 * i);
            // Values i..i+7 come from the low pairs, i+8..i+15 from the high ones; each quad vector holds 4 values.
            for (int half = 0; half < 2; half++)
            {
                Vector128<ushort> p01 = half == 0 ? lo01 : hi01, p23 = half == 0 ? lo23 : hi23, p45 = half == 0 ? lo45 : hi45, p67 = half == 0 ? lo67 : hi67;
                for (int q = 0; q < 2; q++)
                {
                    Vector128<uint> low = Quads(p01, p23, q).AsUInt32(), high = Quads(p45, p67, q).AsUInt32();
                    (Vector128.WidenLower(low) | Vector128.WidenLower(high) << 32).AsByte().StoreUnsafe(ref d, at);
                    (Vector128.WidenUpper(low) | Vector128.WidenUpper(high) << 32).AsByte().StoreUnsafe(ref d, at + 16);
                    at += 32;
                }
            }
        }
        return i;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<byte> Load(ref byte s, int at) => Vector128.LoadUnsafe(ref s, (nuint)at);

    /// <summary>Bytes of two streams as 16-bit pairs: values 0–7 in <paramref name="lo"/>, 8–15 in <paramref name="hi"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Pairs(Vector128<byte> first, Vector128<byte> second, out Vector128<ushort> lo, out Vector128<ushort> hi)
    {
        lo = Vector128.WidenLower(first) | Vector128.WidenLower(second) << 8;
        hi = Vector128.WidenUpper(first) | Vector128.WidenUpper(second) << 8;
    }

    /// <summary>Pairs of streams 0–1 and 2–3 as 32-bit values: the lower four values (<paramref name="half"/> 0) or the upper four.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Vector128<byte> Quads(Vector128<ushort> pairs01, Vector128<ushort> pairs23, int half) => half == 0
        ? (Vector128.WidenLower(pairs01) | Vector128.WidenLower(pairs23) << 16).AsByte()
        : (Vector128.WidenUpper(pairs01) | Vector128.WidenUpper(pairs23) << 16).AsByte();
}
