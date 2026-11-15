using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using Kaminari.Sazanami.Encodings;

namespace Kaminari.Seseragi.Files.Parquet.Encodings;

/// <summary>
/// Writes DELTA_BINARY_PACKED (the inverse of <see cref="DeltaBinaryPacked"/>): the header (block size, miniblocks,
/// value count, first value), then blocks of 128 deltas in four miniblocks of 32, as parquet-mr and Arrow write them.
/// Deltas wrap in the values' own width, so INT32 miniblocks are never wider than 32 bits (pyarrow refuses wider
/// ones, which DuckDB writes); the last miniblock is padded with zeros, the unused ones of the last block absent with
/// a bit width of 0.
/// </summary>
internal static class DeltaBinaryPackedEncoder
{
    private const int BlockSize = 128, Miniblocks = 4, MiniblockSize = BlockSize / Miniblocks;

    /// <summary>Appends the encoding of <paramref name="values"/> (INT32 for 4-byte <typeparamref name="T"/>, INT64 for 8-byte).</summary>
    public static void Encode<T>(ReadOnlySpan<T> values, IBufferWriter<byte> output) where T : unmanaged, IBinaryInteger<T>
    {
        ulong mask = Unsafe.SizeOf<T>() == 4 ? uint.MaxValue : ulong.MaxValue;
        Uleb(output, BlockSize);
        Uleb(output, Miniblocks);
        Uleb(output, (ulong)values.Length);
        Uleb(output, Zigzag(values.IsEmpty ? 0 : long.CreateTruncating(values[0])));
        Span<T> deltas = stackalloc T[BlockSize];
        Span<ulong> packed = stackalloc ulong[BlockSize];
        Span<byte> width = stackalloc byte[Miniblocks];
        for (int i = 1; i < values.Length; i += BlockSize)
        {
            int n = Math.Min(BlockSize, values.Length - i);
            T min = values[i] - values[i - 1];
            for (int k = 0; k < n; k++)
            {
                T delta = values[i + k] - values[i + k - 1]; // wraps, as the reader's sum does
                deltas[k] = delta;
                if (delta < min) min = delta;
            }
            packed.Clear();
            for (int k = 0; k < n; k++) packed[k] = ulong.CreateTruncating(deltas[k] - min) & mask;
            Uleb(output, Zigzag(long.CreateTruncating(min)));
            int used = (n + MiniblockSize - 1) / MiniblockSize;
            Span<byte> widths = output.GetSpan(Miniblocks)[..Miniblocks];
            widths.Clear();
            for (int m = 0; m < used; m++)
            {
                ulong any = 0;
                foreach (ulong p in packed.Slice(m * MiniblockSize, MiniblockSize)) any |= p;
                widths[m] = (byte)(64 - BitOperations.LeadingZeroCount(any));
            }
            widths.CopyTo(width);
            output.Advance(Miniblocks);
            for (int m = 0; m < used; m++)
            {
                int bytes = MiniblockSize * width[m] / 8;
                Span<byte> destination = output.GetSpan(bytes)[..bytes];
                BitPacker.Pack(packed.Slice(m * MiniblockSize, MiniblockSize), width[m], destination);
                output.Advance(bytes);
            }
        }
    }

    private static ulong Zigzag(long v) => (ulong)((v << 1) ^ (v >> 63));

    private static void Uleb(IBufferWriter<byte> output, ulong value)
    {
        Span<byte> span = output.GetSpan(10);
        int n = 0;
        while (value >= 0x80)
        {
            span[n++] = (byte)(value | 0x80);
            value >>= 7;
        }
        span[n++] = (byte)value;
        output.Advance(n);
    }
}

/// <summary>
/// Writes DELTA_LENGTH_BYTE_ARRAY (the lengths as DELTA_BINARY_PACKED, then the bytes back to back) and
/// DELTA_BYTE_ARRAY (each value's prefix shared with the one before, as DELTA_BINARY_PACKED, then the rest as
/// DELTA_LENGTH_BYTE_ARRAY), from values back to back in <c>bytes</c> with their <c>offsets</c> (one more than values).
/// </summary>
internal static class DeltaByteArrayEncoder
{
    public static void EncodeLengths(ReadOnlySpan<byte> bytes, ReadOnlySpan<int> offsets, IBufferWriter<byte> output)
    {
        int count = offsets.Length - 1;
        int[] lengths = ArrayPool<int>.Shared.Rent(Math.Max(1, count));
        try
        {
            for (int i = 0; i < count; i++) lengths[i] = offsets[i + 1] - offsets[i];
            DeltaBinaryPackedEncoder.Encode<int>(lengths.AsSpan(0, count), output);
            output.Write(bytes[offsets[0]..offsets[count]]);
        }
        finally
        {
            ArrayPool<int>.Shared.Return(lengths);
        }
    }

    public static void EncodeIncremental(ReadOnlySpan<byte> bytes, ReadOnlySpan<int> offsets, IBufferWriter<byte> output)
    {
        int count = offsets.Length - 1;
        int[] prefixes = ArrayPool<int>.Shared.Rent(Math.Max(1, count)), suffixes = ArrayPool<int>.Shared.Rent(Math.Max(1, count));
        try
        {
            ReadOnlySpan<byte> previous = [];
            for (int i = 0; i < count; i++)
            {
                ReadOnlySpan<byte> value = bytes[offsets[i]..offsets[i + 1]];
                int prefix = value.CommonPrefixLength(previous);
                prefixes[i] = prefix;
                suffixes[i] = value.Length - prefix;
                previous = value;
            }
            DeltaBinaryPackedEncoder.Encode<int>(prefixes.AsSpan(0, count), output);
            DeltaBinaryPackedEncoder.Encode<int>(suffixes.AsSpan(0, count), output);
            for (int i = 0; i < count; i++) output.Write(bytes.Slice(offsets[i] + prefixes[i], suffixes[i]));
        }
        finally
        {
            ArrayPool<int>.Shared.Return(prefixes);
            ArrayPool<int>.Shared.Return(suffixes);
        }
    }
}
