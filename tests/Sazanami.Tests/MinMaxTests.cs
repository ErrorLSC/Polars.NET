using Kaminari.Sazanami.Kernels;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="MinMax"/> against a value-by-value model: every length around the vector widths, signed and unsigned
/// integers at their extremes, floats and doubles with NaNs (skipped, counted), infinities and signed zeros.
/// </summary>
public class MinMaxTests
{
    private static readonly int[] Lengths = [1, 2, 3, 7, 8, 15, 16, 17, 31, 32, 33, 100, 1000, 4097];

    [Fact]
    public void Integers_AreBoundedAsTheirOrderSays()
    {
        var r = new Random(1);
        foreach (int n in Lengths)
        {
            int[] ints = [.. Enumerable.Range(0, n).Select(_ => r.Next(8) switch { 0 => int.MinValue, 1 => int.MaxValue, _ => r.Next(int.MinValue, int.MaxValue) })];
            Check<int>(ints);
            Check<uint>([.. ints.Select(v => (uint)v)]);
            long[] longs = [.. Enumerable.Range(0, n).Select(_ => r.Next(8) switch { 0 => long.MinValue, 1 => long.MaxValue, _ => r.NextInt64(long.MinValue, long.MaxValue) })];
            Check<long>(longs);
            Check<ulong>([.. longs.Select(v => (ulong)v)]);
            // The extreme alone, at every position.
            for (int at = 0; at < Math.Min(n, 40); at++)
            {
                long[] one = new long[n];
                one[at] = long.MinValue;
                Check<long>(one);
            }
        }

        static void Check<T>(T[] values) where T : unmanaged, System.Numerics.IBinaryInteger<T>
        {
            MinMax.Integers<T>(values, out T min, out T max);
            Assert.Equal((values.Min(), values.Max()), (min, max));
        }
    }

    [Fact]
    public void Floats_SkipAndCountNaNs()
    {
        var r = new Random(2);
        foreach (int n in Lengths)
        {
            foreach (int nanEvery in (int[])[0, 1, 3, 50])
            {
                double[] doubles = [.. Enumerable.Range(0, n).Select(i => nanEvery > 0 && i % nanEvery == 0 ? double.NaN : r.Next(10) switch
                {
                    0 => double.PositiveInfinity, 1 => double.NegativeInfinity, 2 => -0d, 3 => 0d, _ => r.NextDouble() * 2e6 - 1e6,
                })];
                Check<double>(doubles);
                Check<float>([.. doubles.Select(v => (float)v)]);
            }
        }

        static void Check<T>(T[] values) where T : unmanaged, System.Numerics.IFloatingPointIeee754<T>
        {
            T[] numbers = [.. values.Where(v => !T.IsNaN(v))];
            bool found = MinMax.Floating<T>(values, out T min, out T max, out long nans);
            Assert.Equal(values.Length - numbers.Length, nans);
            Assert.Equal(numbers.Length > 0, found);
            if (!found) return;
            // Zeros compare equal: either sign may stand for them.
            Assert.True(min == numbers.Min() && max == numbers.Max(), $"{min}..{max} for {numbers.Min()}..{numbers.Max()}");
        }
    }
}
