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

    /// <summary>Packs values of 0–8 bits (each below 2^<paramref name="bitWidth"/>), as <see cref="Pack(ReadOnlySpan{uint}, int, Span{byte})"/> does.</summary>
    public static void Pack(ReadOnlySpan<byte> values, int bitWidth, Span<byte> destination)
    {
        if (bitWidth == 0) return;
        uint pending = 0;
        int bits = 0, o = 0;
        foreach (byte value in values)
        {
            pending |= (uint)value << bits;
            bits += bitWidth;
            if (bits >= 8)
            {
                destination[o++] = (byte)pending;
                pending >>= 8;
                bits -= 8;
            }
        }
        if (bits > 0) destination[o] = (byte)pending;
    }

    /// <summary>Packs values of 0–64 bits (each below 2^<paramref name="bitWidth"/>), as <see cref="Pack(ReadOnlySpan{uint}, int, Span{byte})"/> does.</summary>
    public static void Pack(ReadOnlySpan<ulong> values, int bitWidth, Span<byte> destination)
    {
        if (bitWidth == 0) return;
        UInt128 pending = 0;
        int bits = 0, o = 0;
        foreach (ulong value in values)
        {
            pending |= (UInt128)value << bits;
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
