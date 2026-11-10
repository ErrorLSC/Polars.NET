namespace Kaminari.Sazanami.Curves;

/// <summary>
/// Space-filling curves over k dimensions: each point (one unsigned coordinate per dimension) gets a 128-bit key,
/// and sorting by the key keeps points that are close in every dimension close in the order. Used to lay out data
/// files (Z-ordering, clustering) so that filters on any of the dimensions skip files.
/// Coordinates are ordinals (for instance dense ranks), each below 2^bits; <see cref="Quantize"/> brings larger
/// ones into range. Portable code only: no instruction-set specific paths.
/// </summary>
public static class SpaceFillingCurve
{
    /// <summary>The width of a key.</summary>
    public const int KeyBits = 128;

    /// <summary>The most bits each of <paramref name="dimensions"/> coordinates can keep in a key (at most 64).</summary>
    public static int BitsPerDimension(int dimensions)
    {
        if (dimensions is < 1 or > KeyBits) throw new ArgumentOutOfRangeException(nameof(dimensions), $"Between 1 and {KeyBits} dimensions.");
        return Math.Min(64, KeyBits / dimensions);
    }

    /// <summary>
    /// Scales ordinals in [0, <paramref name="max"/>] onto [0, 2^<paramref name="bits"/>) keeping their order: unchanged
    /// when they fit, else proportionally (equal ordinals stay equal, distinct ones may merge).
    /// </summary>
    public static void Quantize(ReadOnlySpan<ulong> ordinals, ulong max, int bits, Span<ulong> output)
    {
        if (bits is < 1 or > 64) throw new ArgumentOutOfRangeException(nameof(bits));
        if (output.Length < ordinals.Length) throw new ArgumentException("The output is shorter than the input.", nameof(output));
        if (bits == 64 || max < 1UL << bits)
        {
            ordinals.CopyTo(output);
            return;
        }
        UInt128 range = (UInt128)max + 1;
        for (int i = 0; i < ordinals.Length; i++)
        {
            if (ordinals[i] > max) throw new ArgumentOutOfRangeException(nameof(ordinals), $"An ordinal is above the maximum {max}.");
            output[i] = (ulong)(((UInt128)ordinals[i] << bits) / range);
        }
    }

    /// <summary>
    /// Z-order (Morton) keys: the coordinates' bits interleaved, high bits first and, within one bit level,
    /// dimension 0 first.
    /// </summary>
    /// <param name="coordinates">One array per dimension, all as long as <paramref name="keys"/>; each value below 2^bits.</param>
    /// <param name="bits">Bits per coordinate; dimensions × bits at most 128.</param>
    /// <param name="keys">Receives one key per point.</param>
    public static void ZOrder(ReadOnlyMemory<ulong>[] coordinates, int bits, Span<UInt128> keys)
    {
        int k = Check(coordinates, bits, keys.Length);
        UInt128[] spread = SpreadTable(k);
        keys.Clear();
        int bytes = (bits + 7) / 8;
        for (int d = 0; d < k; d++)
        {
            ReadOnlySpan<ulong> values = coordinates[d].Span;
            int lane = k - 1 - d; // dimension 0 takes the highest position of each bit level
            for (int p = 0; p < keys.Length; p++)
            {
                ulong v = values[p];
                UInt128 key = keys[p];
                for (int j = 0; j < bytes; j++)
                    key |= spread[(int)((v >> (8 * j)) & 0xFF)] << (8 * j * k + lane);
                keys[p] = key;
            }
        }
    }

    /// <summary>
    /// Hilbert keys: the position of each point along a Hilbert curve through the grid of 2^bits cells per
    /// dimension. Unlike Z-order, consecutive keys are always neighbouring cells, so clusters are more compact.
    /// </summary>
    /// <param name="coordinates">One array per dimension, all as long as <paramref name="keys"/>; each value below 2^bits.</param>
    /// <param name="bits">Bits per coordinate; dimensions × bits at most 128.</param>
    /// <param name="keys">Receives one key per point.</param>
    public static void Hilbert(ReadOnlyMemory<ulong>[] coordinates, int bits, Span<UInt128> keys)
    {
        int k = Check(coordinates, bits, keys.Length);
        if (k == 1)
        {
            ReadOnlySpan<ulong> only = coordinates[0].Span;
            for (int p = 0; p < keys.Length; p++) keys[p] = only[p];
            return;
        }
        UInt128[] spread = SpreadTable(k);
        int bytes = (bits + 7) / 8;
        Span<ulong> x = k <= 32 ? stackalloc ulong[k] : new ulong[k];
        for (int p = 0; p < keys.Length; p++)
        {
            for (int d = 0; d < k; d++) x[d] = coordinates[d].Span[p];
            AxesToTranspose(x, bits);
            // The transposed form read bit level by bit level, dimension 0 first, is the Hilbert index.
            UInt128 key = 0;
            for (int d = 0; d < k; d++)
            {
                int lane = k - 1 - d;
                for (int j = 0; j < bytes; j++)
                    key |= spread[(int)((x[d] >> (8 * j)) & 0xFF)] << (8 * j * k + lane);
            }
            keys[p] = key;
        }
    }

    /// <summary>
    /// Skilling's transform (AIP Conf. Proc. 707, 2004): turns a point's coordinates in place into the "transposed"
    /// Hilbert index, whose bits interleaved give the index.
    /// </summary>
    private static void AxesToTranspose(Span<ulong> x, int bits)
    {
        int n = x.Length;
        ulong m = 1UL << (bits - 1);
        // Inverse undo excess work.
        for (ulong q = m; q > 1; q >>= 1)
        {
            ulong p = q - 1;
            for (int i = 0; i < n; i++)
            {
                if ((x[i] & q) != 0)
                {
                    x[0] ^= p;
                }
                else
                {
                    ulong t = (x[0] ^ x[i]) & p;
                    x[0] ^= t;
                    x[i] ^= t;
                }
            }
        }
        // Gray encode.
        for (int i = 1; i < n; i++) x[i] ^= x[i - 1];
        ulong flip = 0;
        for (ulong q = m; q > 1; q >>= 1)
        {
            if ((x[n - 1] & q) != 0) flip ^= q - 1;
        }
        for (int i = 0; i < n; i++) x[i] ^= flip;
    }

    private static int Check(ReadOnlyMemory<ulong>[] coordinates, int bits, int points)
    {
        ArgumentNullException.ThrowIfNull(coordinates);
        int k = coordinates.Length;
        if (k < 1) throw new ArgumentException("At least one dimension.", nameof(coordinates));
        if (bits < 1 || bits > BitsPerDimension(k))
            throw new ArgumentOutOfRangeException(nameof(bits), $"{k} dimensions keep 1 to {BitsPerDimension(k)} bits each.");
        ulong limit = bits == 64 ? ulong.MaxValue : (1UL << bits) - 1;
        foreach (ReadOnlyMemory<ulong> dimension in coordinates)
        {
            if (dimension.Length != points) throw new ArgumentException("Every dimension needs one coordinate per key.", nameof(coordinates));
            foreach (ulong v in dimension.Span)
            {
                if (v > limit) throw new ArgumentOutOfRangeException(nameof(coordinates), $"A coordinate is above 2^{bits} - 1; quantize first.");
            }
        }
        return k;
    }

    // For k dimensions: each byte value with its bit j moved to bit j * k, so that a coordinate's bits land k apart.
    private static readonly UInt128[]?[] s_spread = new UInt128[]?[KeyBits + 1];

    private static UInt128[] SpreadTable(int k)
    {
        UInt128[]? table = Volatile.Read(ref s_spread[k]);
        if (table is not null) return table;
        table = new UInt128[256];
        for (int b = 0; b < 256; b++)
        {
            UInt128 v = 0;
            for (int j = 0; j < 8; j++)
            {
                if ((b & (1 << j)) != 0 && j * k < KeyBits) v |= UInt128.One << (j * k);
            }
            table[b] = v;
        }
        Volatile.Write(ref s_spread[k], table);
        return table;
    }
}
