using Kaminari.Sazanami.Ordering;

namespace Sazanami.Tests;

/// <summary><see cref="CountingSort"/> against a stable sort by group id.</summary>
public class CountingSortTests
{
    /// <summary>Group ids of a random shape: <paramref name="count"/> groups over <paramref name="rows"/> rows.</summary>
    private static int[] RandomGroups(Random random, int rows, int count) => random.Next(6) switch
    {
        0 => [.. Enumerable.Repeat(random.Next(count), rows)],
        1 => [.. Enumerable.Range(0, rows).Select(i => (int)((long)i * count / Math.Max(1, rows)))],           // in order
        2 => [.. Enumerable.Range(0, rows).Select(i => count - 1 - (int)((long)i * count / Math.Max(1, rows)))], // descending
        3 => [.. Enumerable.Range(0, rows).Select(i => i % count)],
        4 => [.. Enumerable.Range(0, rows).Select(_ => random.Next(Math.Min(count, 3)))],                       // a few groups used
        _ => [.. Enumerable.Range(0, rows).Select(_ => random.Next(count))],
    };

    [Fact]
    public void RandomGroups_ComeInGroupOrder_StableWithinAGroup()
    {
        var random = new Random(1123);
        int[] sizes = [0, 1, 2, 63, 64, 65, 1000, 5000, 122_880];
        int[] counts = [1, 2, 30, 1000, 1 << 20];
        for (int round = 0; round < 400; round++)
        {
            int rows = sizes[random.Next(sizes.Length)];
            int count = random.Next(8) == 0 ? Math.Max(1, rows) : counts[random.Next(counts.Length)];
            int[] groups = RandomGroups(random, rows, count);
            var starts = new int[count + 1];
            var permutation = new int[rows + random.Next(3)]; // longer is fine
            Array.Fill(starts, -7);
            Array.Fill(permutation, -7);
            bool ordered = CountingSort.Permutation(groups, starts, permutation);

            string where = $"round {round}: {rows} rows, {count} groups";
            int[] expected = [.. Enumerable.Range(0, rows).OrderBy(i => groups[i])];
            Assert.True(expected.AsSpan().SequenceEqual(permutation.AsSpan(0, rows)), where);
            Assert.All(permutation[rows..], p => Assert.Equal(-7, p));
            int[] sizesByGroup = new int[count];
            foreach (int g in groups) sizesByGroup[g]++;
            int at = 0;
            for (int g = 0; g < count; g++)
            {
                Assert.True(at == starts[g], $"{where}: start of group {g}");
                at += sizesByGroup[g];
            }
            Assert.Equal(rows, starts[count]);
            bool nonDecreasing = groups.Zip(groups.Skip(1)).All(p => p.First <= p.Second);
            Assert.True(nonDecreasing == ordered, $"{where}: in order {nonDecreasing}");
        }
    }

    [Fact]
    public void InOrderRows_AreTheIdentity()
    {
        int[] groups = [0, 0, 2, 2, 2, 5];
        var starts = new int[7];
        var permutation = new int[6];
        Assert.True(CountingSort.Permutation(groups, starts, permutation));
        Assert.Equal([0, 1, 2, 3, 4, 5], permutation);
        Assert.Equal([0, 2, 2, 5, 5, 5, 6], starts);
        Assert.False(CountingSort.Permutation([1, 0], starts.AsSpan(0, 3), permutation));
        Assert.Equal([1, 0], permutation[..2]);
        Assert.Equal([0, 1, 2], starts[..3]);
    }

    [Fact]
    public void BadArguments_Throw()
    {
        var starts = new int[4];
        var permutation = new int[4];
        Assert.Throws<ArgumentOutOfRangeException>(() => CountingSort.Permutation([0, 3], starts, permutation));
        Assert.Throws<ArgumentOutOfRangeException>(() => CountingSort.Permutation([-1, 0], starts, permutation));
        Assert.Throws<ArgumentException>(() => CountingSort.Permutation([0], [], permutation));
        Assert.Throws<ArgumentException>(() => CountingSort.Permutation([0, 1, 2, 0, 1], starts, permutation));
        // No rows: every group empty.
        starts.AsSpan().Fill(9);
        Assert.True(CountingSort.Permutation([], starts, []));
        Assert.Equal([0, 0, 0, 0], starts);
    }
}
