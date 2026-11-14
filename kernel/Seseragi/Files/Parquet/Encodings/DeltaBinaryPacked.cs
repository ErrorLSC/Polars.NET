using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Kaminari.Sazanami.Encodings;

namespace Kaminari.Seseragi.Files.Parquet.Encodings;

/// <summary>
/// DELTA_BINARY_PACKED (Encodings.md): a header (block size, miniblocks per block, value count, first value), then
/// blocks of a minimum delta, one bit width per miniblock and the miniblocks, each the block's deltas minus the
/// minimum, bit-packed least significant bit first. Arithmetic wraps (two's complement), done in 64 bits and truncated:
/// the same result for INT32. Miniblocks are padded to their full size; in the last block, the bit widths of unused
/// miniblocks are present but arbitrary and their data absent.
/// </summary>
internal static class DeltaBinaryPacked
{
    /// <summary>The deltas unpacked at once (a scratch of this many values is passed in).</summary>
    public const int Chunk = 64;

    /// <summary>
    /// Decodes the first <paramref name="destination"/>.Length values of the stream at the start of
    /// <paramref name="data"/> (INT32 for 4-byte <typeparamref name="T"/>, INT64 for 8-byte). With
    /// <paramref name="toEnd"/>, walks the rest of the stream too (all the values its header counts) and returns the
    /// position after it: what follows is another section, as in DELTA_LENGTH_BYTE_ARRAY. Without, stops after the
    /// values wanted (a header counting more than the page holds is read as pyarrow and DuckDB read it).
    /// <paramref name="scratch"/> holds at least <see cref="Chunk"/> values.
    /// </summary>
    /// <exception cref="ParquetFormatException">The header is invalid, the stream holds fewer values than wanted, a bit width is too wide, or the data is cut short.</exception>
    public static int Decode<T>(ReadOnlySpan<byte> data, Span<T> destination, Span<ulong> scratch, bool toEnd) where T : unmanaged, IBinaryInteger<T>
    {
        int position = 0;
        long blockSize = Uleb(data, ref position, "block size");
        long miniblocks = Uleb(data, ref position, "miniblock count");
        long total = Uleb(data, ref position, "value count");
        ulong last = Zigzag(Uleb(data, ref position, "first value", 64));
        if (blockSize <= 0 || blockSize % 128 != 0 || blockSize > int.MaxValue)
            throw new ParquetFormatException($"a DELTA_BINARY_PACKED block holds {blockSize} values, not a positive multiple of 128.");
        if (miniblocks <= 0 || blockSize % miniblocks != 0 || blockSize / miniblocks % 32 != 0)
            throw new ParquetFormatException($"a DELTA_BINARY_PACKED block of {blockSize} values has {miniblocks} miniblocks (their size must be a multiple of 32).");
        if (total > int.MaxValue) throw new ParquetFormatException($"a DELTA_BINARY_PACKED stream counts {total} values.");
        if (total < destination.Length)
            throw new ParquetFormatException($"a DELTA_BINARY_PACKED stream holds {total} values, fewer than the {destination.Length} present.");
        if (total == 0) return position;

        int perMiniblock = (int)(blockSize / miniblocks);
        int written = 0;
        if (destination.Length > 0) destination[written++] = T.CreateTruncating(last);
        long remaining = total - 1;
        while (remaining > 0 && (toEnd || written < destination.Length))
        {
            ulong minDelta = Zigzag(Uleb(data, ref position, "minimum delta", 64));
            if (miniblocks > data.Length - position) throw CutShort();
            ReadOnlySpan<byte> widths = data.Slice(position, (int)miniblocks);
            position += (int)miniblocks;
            for (int m = 0; m < miniblocks && remaining > 0 && (toEnd || written < destination.Length); m++)
            {
                int width = widths[m];
                // Writers must not go past the type's width (Encodings.md), but DuckDB writes INT32 deltas up to 33
                // bits wide (not wrapped to 32 bits); DuckDB and Polars read them, pyarrow refuses. Unpacked and summed
                // in 64 bits and truncated, they give the values written.
                if (width > 64) throw new ParquetFormatException($"a DELTA_BINARY_PACKED miniblock is {width} bits wide.");
                int n = (int)Math.Min(perMiniblock, remaining);
                // A miniblock takes its full size; the stream's last one may stop at its last value's byte.
                long full = (long)perMiniblock * width / 8, needed = BitUnpacking.Bytes(n, width);
                if (needed > data.Length - position) throw CutShort();
                int take = Math.Min(n, destination.Length - written);
                // Within bounds: take values fit the destination from written on (no check per value).
                ref T target = ref Unsafe.Add(ref MemoryMarshal.GetReference(destination), written);
                if (width == 0)
                {
                    // All deltas are the minimum: no bits to unpack.
                    for (int k = 0; k < take; k++)
                    {
                        last += minDelta;
                        Unsafe.Add(ref target, k) = T.CreateTruncating(last);
                    }
                }
                else
                {
                    ReadOnlySpan<byte> packed = data[position..];
                    for (int at = 0; at < take; at += Chunk)
                    {
                        Span<ulong> deltas = scratch[..Math.Min(Chunk, take - at)];
                        BitUnpacking.Unpack(packed, at, width, deltas);
                        ref ulong delta = ref MemoryMarshal.GetReference(deltas);
                        for (int k = 0; k < deltas.Length; k++)
                        {
                            last += minDelta + Unsafe.Add(ref delta, k);
                            Unsafe.Add(ref target, at + k) = T.CreateTruncating(last);
                        }
                    }
                }
                written += take;
                position += (int)Math.Min(full, data.Length - position);
                remaining -= n;
            }
        }
        return position;
    }

    /// <summary>An unsigned LEB128 value (at most <paramref name="bits"/> bits).</summary>
    private static long Uleb(ReadOnlySpan<byte> data, ref int position, string what, int bits = 63)
    {
        ulong value = 0;
        for (int shift = 0; ; shift += 7)
        {
            if (position >= data.Length) throw new ParquetFormatException($"a DELTA_BINARY_PACKED {what} is cut short.");
            if (shift > 63) throw new ParquetFormatException($"a DELTA_BINARY_PACKED {what} is too long.");
            byte b = data[position++];
            value |= (ulong)(b & 0x7F) << shift;
            if (b < 0x80) break;
        }
        if (bits < 64 && value > long.MaxValue) throw new ParquetFormatException($"a DELTA_BINARY_PACKED {what} is too large.");
        return (long)value;
    }

    private static ulong Zigzag(long encoded) => ((ulong)encoded >> 1) ^ (0UL - ((ulong)encoded & 1));

    private static ParquetFormatException CutShort() => new("a DELTA_BINARY_PACKED stream is cut short.");
}
