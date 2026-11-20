using System.Buffers;
using System.Numerics;
using System.Runtime.InteropServices;
using Kaminari.Sazanami.Encodings;

namespace Kaminari.Seseragi.Files.Parquet.Encodings;

/// <summary>
/// Writes the RLE / bit-packing hybrid (Encodings.md): runs of 8 or more equal values as RLE runs, the rest as
/// bit-packed groups of 8 (the last group padded with zeros). The inverse of <see cref="RleBitPackedDecoder"/>.
/// </summary>
internal static class RleBitPackedEncoder
{
    /// <summary>
    /// Appends <paramref name="values"/> (each below 2^<paramref name="bitWidth"/>: at most 32 bits for <see cref="uint"/>,
    /// 8 for <see cref="byte"/>, such as levels) to <paramref name="output"/>.
    /// </summary>
    public static void Encode<T>(ReadOnlySpan<T> values, int bitWidth, IBufferWriter<byte> output) where T : unmanaged, IUnsignedNumber<T>, IBinaryInteger<T>
    {
        if (typeof(T) != typeof(uint) && typeof(T) != typeof(byte)) throw new NotSupportedException($"Values of {typeof(T)} are not encoded.");
        int n = values.Length, i = 0, literals = 0; // literals: values[literals..i] wait to be bit-packed
        while (i < n)
        {
            T value = values[i];
            int run = 1;
            while (i + run < n && values[i + run] == value) run++;
            if (run >= 8)
            {
                // Bit-packed groups hold 8 values: fill the pending group from the run first.
                int fill = (8 - (i - literals) % 8) % 8;
                if (fill > 0)
                {
                    if (run - fill < 8)
                    {
                        i += run;
                        continue;
                    }
                    i += fill;
                    run -= fill;
                }
                BitPacked(values[literals..i], bitWidth, output);
                Rle(uint.CreateTruncating(value), run, bitWidth, output);
                i += run;
                literals = i;
            }
            else
            {
                i += run;
            }
        }
        BitPacked(values[literals..n], bitWidth, output);
    }

    /// <summary>The bytes <see cref="Encode"/> writes at most for <paramref name="count"/> values.</summary>
    public static int MaxBytes(int count, int bitWidth) => (int)(BitPacker.Bytes(count + 7, bitWidth) + (count / 8 + 1) * 5 + 8);

    private static void BitPacked<T>(ReadOnlySpan<T> values, int bitWidth, IBufferWriter<byte> output) where T : unmanaged
    {
        if (values.IsEmpty) return;
        int groups = (values.Length + 7) / 8;
        int bytes = groups * bitWidth;
        Span<byte> span = output.GetSpan(5 + bytes);
        int o = Varint(((uint)groups << 1) | 1, span);
        Span<byte> packed = span.Slice(o, bytes);
        packed.Clear();
        if (typeof(T) == typeof(byte)) BitPacker.Pack(MemoryMarshal.Cast<T, byte>(values), bitWidth, packed);
        else BitPacker.Pack(MemoryMarshal.Cast<T, uint>(values), bitWidth, packed);
        output.Advance(o + bytes);
    }

    private static void Rle(uint value, int run, int bitWidth, IBufferWriter<byte> output)
    {
        int valueBytes = (bitWidth + 7) / 8;
        Span<byte> span = output.GetSpan(5 + valueBytes);
        int o = Varint((uint)run << 1, span);
        for (int b = 0; b < valueBytes; b++) span[o++] = (byte)(value >> (8 * b));
        output.Advance(o);
    }

    private static int Varint(uint value, Span<byte> span)
    {
        int o = 0;
        while (value >= 0x80)
        {
            span[o++] = (byte)(value | 0x80);
            value >>= 7;
        }
        span[o++] = (byte)value;
        return o;
    }
}
