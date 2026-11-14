using Kaminari.Sazanami.Filters;

namespace Sazanami.Tests;

public class SplitBlockBloomFilterTests
{
    [Fact]
    public void InsertedHashes_AreAlwaysFound_OthersRarely()
    {
        var random = new Random(11);
        int blocks = SplitBlockBloomFilter.BlocksFor(2000, 0.01);
        var filter = SplitBlockBloomFilter.Empty(blocks);
        ulong[] inserted = [.. Enumerable.Range(0, 2000).Select(_ => (ulong)random.NextInt64() ^ ((ulong)random.Next() << 63))];
        foreach (ulong h in inserted) filter.Insert(h);
        Assert.All(inserted, h => Assert.True(filter.MightContain(h)));
        int falsePositives = Enumerable.Range(0, 100_000).Count(_ => filter.MightContain((ulong)random.NextInt64()));
        Assert.InRange(falsePositives, 1, 2500); // about 1 % asked for
        Assert.Equal(blocks * SplitBlockBloomFilter.BlockBytes, filter.Bitset.Length);
        Assert.Throws<ArgumentException>(() => new SplitBlockBloomFilter(new byte[33]));
        Assert.False(SplitBlockBloomFilter.Empty(1).MightContain(12345));
    }
}
