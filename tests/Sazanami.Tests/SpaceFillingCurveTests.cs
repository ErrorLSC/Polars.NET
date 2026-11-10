using Kaminari.Sazanami.Curves;

namespace Sazanami.Tests;

public class SpaceFillingCurveTests
{
    private static ReadOnlyMemory<ulong>[] Grid(int dimensions, int side, out int points)
    {
        points = (int)Math.Pow(side, dimensions);
        var coordinates = new ulong[dimensions][];
        for (int d = 0; d < dimensions; d++) coordinates[d] = new ulong[points];
        for (int p = 0; p < points; p++)
        {
            int rest = p;
            for (int d = 0; d < dimensions; d++)
            {
                coordinates[d][p] = (ulong)(rest % side);
                rest /= side;
            }
        }
        return [.. coordinates.Select(c => (ReadOnlyMemory<ulong>)c)];
    }

    /// <summary>Bit by bit, the obvious way: bit b of dimension d at position b * k + (k - 1 - d).</summary>
    private static UInt128 NaiveZ(ulong[] point, int bits)
    {
        int k = point.Length;
        UInt128 key = 0;
        for (int b = 0; b < bits; b++)
        {
            for (int d = 0; d < k; d++)
            {
                if (((point[d] >> b) & 1) != 0) key |= UInt128.One << (b * k + (k - 1 - d));
            }
        }
        return key;
    }

    [Theory]
    [InlineData(1, 64)]
    [InlineData(2, 64)]
    [InlineData(3, 42)]
    [InlineData(5, 25)]
    [InlineData(16, 8)]
    [InlineData(128, 1)]
    public void ZOrder_MatchesTheBitByBitDefinition(int k, int bits)
    {
        var random = new Random(k * 1000 + bits);
        const int points = 200;
        var coordinates = new ulong[k][];
        for (int d = 0; d < k; d++)
        {
            coordinates[d] = new ulong[points];
            for (int p = 0; p < points; p++)
                coordinates[d][p] = bits == 64 ? (ulong)random.NextInt64() * 2 + (ulong)random.Next(2) : (ulong)random.NextInt64(1L << bits);
        }
        var keys = new UInt128[points];

        SpaceFillingCurve.ZOrder([.. coordinates.Select(c => (ReadOnlyMemory<ulong>)c)], bits, keys);

        for (int p = 0; p < points; p++)
            Assert.Equal(NaiveZ([.. coordinates.Select(c => c[p])], bits), keys[p]);
    }

    [Fact]
    public void ZOrder_OfA4By4Grid_IsTheClassicZ()
    {
        // Dimension 0 is the more significant within each bit level: (x, y) -> x1 y1 x0 y0.
        var keys = new UInt128[4];
        SpaceFillingCurve.ZOrder([new ulong[] { 0, 0, 1, 1 }, new ulong[] { 0, 1, 0, 1 }], 2, keys);
        Assert.Equal([0, 1, 2, 3], keys.Select(k => (int)k));
        SpaceFillingCurve.ZOrder([new ulong[] { 2, 3, 3, 0 }, new ulong[] { 0, 3, 0, 2 }], 2, keys);
        Assert.Equal([8, 15, 10, 4], keys.Select(k => (int)k));
    }

    [Theory]
    [InlineData(2, 3)]
    [InlineData(2, 5)]
    [InlineData(3, 3)]
    [InlineData(4, 2)]
    public void Hilbert_VisitsEveryCellOnce_StepByNeighbours(int k, int bits)
    {
        int side = 1 << bits;
        ReadOnlyMemory<ulong>[] grid = Grid(k, side, out int points);
        var keys = new UInt128[points];

        SpaceFillingCurve.Hilbert(grid, bits, keys);

        // A bijection onto 0 .. points - 1 ...
        int[] order = [.. Enumerable.Range(0, points).OrderBy(p => keys[p])];
        Assert.Equal(Enumerable.Range(0, points).Select(i => (UInt128)i), order.Select(p => keys[p]));
        // ... where each step moves to a neighbouring cell (one coordinate changes by one).
        for (int i = 1; i < points; i++)
        {
            int a = order[i - 1], b = order[i];
            long distance = Enumerable.Range(0, k).Sum(d => Math.Abs((long)grid[d].Span[a] - (long)grid[d].Span[b]));
            Assert.True(distance == 1, $"step {i}: distance {distance}");
        }
    }

    [Fact]
    public void Quantize_KeepsOrderAndRange()
    {
        ulong[] ordinals = [0, 1, 500, 999, 1000];
        var output = new ulong[ordinals.Length];

        SpaceFillingCurve.Quantize(ordinals, 1000, 4, output);
        Assert.Equal([0UL, 0UL, 7UL, 15UL, 15UL], output); // floor(v * 16 / 1001)

        SpaceFillingCurve.Quantize(ordinals, 1000, 10, output); // fits: unchanged
        Assert.Equal(ordinals, output);
        Assert.Throws<ArgumentOutOfRangeException>(() => SpaceFillingCurve.Quantize([1001], 1000, 4, new ulong[1]));
    }

    [Fact]
    public void Inputs_AreChecked()
    {
        Assert.Equal(42, SpaceFillingCurve.BitsPerDimension(3));
        Assert.Equal(64, SpaceFillingCurve.BitsPerDimension(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => SpaceFillingCurve.ZOrder([new ulong[] { 4 }, new ulong[] { 0 }], 2, new UInt128[1]));
        Assert.Throws<ArgumentOutOfRangeException>(() => SpaceFillingCurve.Hilbert([new ulong[] { 0 }, new ulong[] { 0 }, new ulong[] { 0 }], 43, new UInt128[1]));
        Assert.Throws<ArgumentException>(() => SpaceFillingCurve.ZOrder([new ulong[] { 0, 1 }, new ulong[] { 0 }], 2, new UInt128[2]));
    }
}
