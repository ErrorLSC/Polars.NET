namespace Kaminari.Sazanami.Encodings;

/// <summary>
/// Packing of values of 0–32 bits least significant bit first (Parquet's hybrid encoding, Arrow bitmaps and others):
/// the inverse of <see cref="BitUnpacking"/>.
/// </summary>
internal static class BitPacker
{
    /// <summary>The bytes <paramref name="count"/> values of <paramref name="bitWidth"/> bits take.</summary>
    public static long Bytes(long count, int bitWidth) => (count * bitWidth + 7) >> 3;

    /// <summary>
    /// Packs <paramref name="values"/> (each below 2^<paramref name="bitWidth"/>) into the first
    /// <see cref="Bytes"/> bytes of <paramref name="destination"/>; the bits past the last value are zero.
    /// </summary>
    public static void Pack(ReadOnlySpan<uint> values, int bitWidth, Span<byte> destination)
    {
        if (bitWidth == 0) return;
        ulong pending = 0;
        int bits = 0, o = 0;
        foreach (uint value in values)
        {
            pending |= (ulong)value << bits;
            bits += bitWidth;
            while (bits >= 8)
            {
                destination[o++] = (byte)pending;
                pending >>= 8;
                bits -= 8;
            }
        }
        if (bits > 0) destination[o] = (byte)pending;
    }
}
