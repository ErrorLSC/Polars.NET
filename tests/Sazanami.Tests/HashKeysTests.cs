using Kaminari.Sazanami.Ordering;

namespace Sazanami.Tests;

/// <summary><see cref="HashKeys"/>: bucketing by the top bits, then each bucket sorted and deduplicated, by ranges.</summary>
public sealed class HashKeysTests
{
    public static TheoryData<int, int> Sizes() => new() { { 0, 1 }, { 1, 1 }, { 40, 7 }, { 100_000, 1_000 }, { 300_000, 250_000 } };

    [Theory]
    [MemberData(nameof(Sizes))]
    public void BucketsSortedAndDeduplicated_GiveTheDistinctKeysInOrder(int count, int distinct)
    {
        var random = new Random(count);
        ulong[] pool = [.. Enumerable.Range(0, distinct).Select(_ => (ulong)random.NextInt64() ^ ((ulong)random.Next() << 63))];
        if (distinct > 2) pool[1] = pool[0] & 0xFFFF_0000_0000_0000UL; // keys sharing a bucket
        ulong[] keys = [.. Enumerable.Range(0, count).Select(_ => pool[random.Next(distinct)])];
        ulong[] into = new ulong[count];
        int[] starts = new int[HashKeys.Buckets + 1], kept = new int[HashKeys.Buckets];
        HashKeys.Bucket(keys, into, starts);
        Assert.Equal(count, starts[HashKeys.Buckets]);
        // In two ranges, as parts would.
        int total = HashKeys.SortDistinct(into, starts, kept, 0, 12_345) + HashKeys.SortDistinct(into, starts, kept, 12_345, HashKeys.Buckets);
        List<ulong> read = [];
        for (int b = 0; b < HashKeys.Buckets; b++) read.AddRange(into.AsSpan(starts[b], kept[b]).ToArray());
        Assert.Equal([.. keys.Distinct().Order()], read);
        Assert.Equal(read.Count, total);
    }

    [Fact]
    public void Bucket_RefusesShortDestinations()
    {
        Assert.Throws<ArgumentException>(() => HashKeys.Bucket([1UL, 2UL], new ulong[1], new int[HashKeys.Buckets + 1]));
        Assert.Throws<ArgumentException>(() => HashKeys.Bucket([1UL], new ulong[1], new int[10]));
    }
}
