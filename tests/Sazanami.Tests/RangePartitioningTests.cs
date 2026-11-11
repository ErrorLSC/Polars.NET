using Apache.Arrow;
using Kaminari.Sazanami.Ordering;

namespace Sazanami.Tests;

public class RangePartitioningTests
{
    private static uint[] Ids(RangeBoundaries boundaries, IArrowArray values)
    {
        var ids = new uint[values.Length];
        boundaries.Assign(values, ids);
        return ids;
    }

    private static Int32Array Ints(params int?[] values)
    {
        var b = new Int32Array.Builder();
        foreach (int? v in values)
        {
            if (v is int i) b.Append(i);
            else b.AppendNull();
        }
        return b.Build();
    }

    [Fact]
    public void Ranges_HoldAboutEqualCounts_AndKeepTheOrder()
    {
        Int32Array sample = Ints([.. Enumerable.Range(0, 100).Select(i => (int?)i)]);
        RangeBoundaries boundaries = RangeBoundaries.FromSample(sample, 4);
        Assert.Equal(4, boundaries.RangeCount);
        uint[] ids = Ids(boundaries, sample);
        Assert.Equal([25, 25, 25, 25], ids.GroupBy(i => i).OrderBy(g => g.Key).Select(g => g.Count()));
        Assert.True(ids.Zip(ids.Skip(1)).All(p => p.First <= p.Second));
        // Values outside the sample fall into the end ranges.
        Assert.Equal([0u, 3u], Ids(boundaries, Ints(-1000, 1000)));
    }

    [Fact]
    public void EqualValues_ShareARange_AndNullsTakeTheFirst()
    {
        // Mostly one value: it is never split, so fewer ranges than asked.
        Int32Array sample = Ints([.. Enumerable.Repeat((int?)7, 90), .. Enumerable.Range(0, 10).Select(i => (int?)(100 + i)), null, null]);
        RangeBoundaries boundaries = RangeBoundaries.FromSample(sample, 10);
        uint[] ids = Ids(boundaries, Ints(7, 7, 100, 109, null));
        Assert.Equal(ids[0], ids[1]);
        Assert.True(ids[2] > ids[0] && ids[3] >= ids[2]);
        Assert.Equal(0u, ids[4]);
        Assert.True(boundaries.RangeCount <= 10);

        RangeBoundaries single = RangeBoundaries.FromSample(Ints(5, 5, 5), 8);
        Assert.Equal(1, single.RangeCount);
        Assert.Equal([0u, 0u, 0u], Ids(single, Ints(1, 5, 9)));

        RangeBoundaries empty = RangeBoundaries.FromSample(Ints(null, null), 8);
        Assert.Equal(1, empty.RangeCount);
    }

    [Fact]
    public void Strings_OrderByTheirBytes_AndDoublesPutNaNLast()
    {
        var strings = new StringArray.Builder().AppendRange(["b", "a", "ab", "c", "aa", "bb"]).Build();
        RangeBoundaries s = RangeBoundaries.FromSample(strings, 3);
        uint[] ids = Ids(s, new StringArray.Builder().AppendRange(["a", "aa", "ab", "b", "bb", "c"]).Build());
        Assert.True(ids.Zip(ids.Skip(1)).All(p => p.First <= p.Second));
        Assert.True(ids[^1] > ids[0]);

        var doubles = new DoubleArray.Builder().AppendRange([1.0, double.NaN, -2.0, 0.0, -0.0, 3.0]).Build();
        RangeBoundaries d = RangeBoundaries.FromSample(doubles, 6);
        uint[] dIds = Ids(d, new DoubleArray.Builder().AppendRange([-2.0, -0.0, 0.0, 1.0, 3.0, double.NaN]).Build());
        Assert.Equal(dIds[1], dIds[2]); // -0.0 equals 0.0
        Assert.True(dIds[5] > dIds[4]); // NaN above every number
    }

    [Fact]
    public void Decimals_OrderByValue()
    {
        var type = new Apache.Arrow.Types.Decimal128Type(20, 2);
        var b = new Decimal128Array.Builder(type);
        foreach (decimal v in new[] { 3.50m, -1.25m, 100m, 0m, 2m }) b.Append(v);
        RangeBoundaries boundaries = RangeBoundaries.FromSample(b.Build(), 5);
        var probe = new Decimal128Array.Builder(type);
        foreach (decimal v in new[] { -1.25m, 0m, 2m, 3.50m, 100m }) probe.Append(v);
        Assert.Equal([0u, 1u, 2u, 3u, 4u], Ids(boundaries, probe.Build()));
    }

    [Fact]
    public void Keys_FollowTheHilbertCurve_AndBucketsSplitBySampledKeys()
    {
        Assert.Equal(10, ClusteringKeys.BitsFor(1000));
        Assert.Equal(1, ClusteringKeys.BitsFor(1));
        Assert.Equal(1, ClusteringKeys.BitsFor(2));

        // Without noise: neighbouring cells get keys one curve step apart (the low byte is the noise byte).
        uint[] x = [0, 0, 1, 1], y = [0, 1, 1, 0];
        var keys = new ulong[4];
        ClusteringKeys.Compute([x, y], 1, null, keys);
        Assert.Equal([0UL, 1UL << 8, 2UL << 8, 3UL << 8], keys);
        Assert.Throws<ArgumentException>(() => ClusteringKeys.Compute([x, y, x, y], 15, null, keys));

        ulong[] sample = [.. Enumerable.Range(0, 1000).Select(i => (ulong)i * 7)];
        ulong[] boundaries = ClusteringKeys.BucketBoundaries(sample, 4);
        Assert.Equal(3, boundaries.Length);
        var counts = new int[4];
        foreach (ulong k in sample) counts[ClusteringKeys.Bucket(boundaries, k)]++;
        Assert.All(counts, c => Assert.Equal(250, c));
        // Keys all alike: one bucket.
        Assert.Empty(ClusteringKeys.BucketBoundaries([5, 5, 5, 5], 4));
    }
}
