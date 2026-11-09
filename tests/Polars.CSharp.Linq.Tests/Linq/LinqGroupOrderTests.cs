using Polars.CSharp.Linq;
using Polars.NET.Linq;
using Kaminari.Samidare;

namespace Polars.CSharp.Tests;

public record GoRow(int Id, int A, int B, int V);

/// <summary>
/// A GroupBy keeps the order of its groups (first appearance of each key) unless that order cannot be observed:
/// beneath a sort over all of its keys. Results always agree with LINQ to Objects, including the order of groups
/// that a sort leaves tied.
/// </summary>
public class LinqGroupOrderTests
{
    // Keys first appear in the order 3, 1, 2; the counts of keys 1 and 2 tie.
    private static readonly GoRow[] Rows =
    [
        new(1, 3, 1, 10), new(2, 1, 2, 20), new(3, 2, 1, 30), new(4, 3, 2, 40),
        new(5, 1, 1, 50), new(6, 2, 2, 60), new(7, 3, 1, 70)
    ];

    private static void AssertGroupOrder<T>(Func<IQueryable<GoRow>, IQueryable<T>> query, bool expectMaintained)
    {
        using var df = DataFrame.FromRows(Rows);
        var native = query(df.AsQueryable<GoRow>());

        var plan = ((IPolarsExplainable)native).InspectPlan(true).LogicalPlan!.Value;
        Assert.Contains($"maintainOrder={(expectMaintained ? "true" : "false")}", plan);
        Assert.DoesNotContain($"maintainOrder={(expectMaintained ? "false" : "true")}", plan);
        Assert.Equal(query(Rows.AsQueryable()).ToList(), native.ToList());
    }

    [Fact]
    public void Sort_Over_The_Key_Drops_The_Group_Order()
    {
        AssertGroupOrder(q => q.GroupBy(r => r.A).Select(g => new { g.Key, S = g.Sum(r => r.V) }).OrderBy(x => x.Key), false);
        AssertGroupOrder(q => q.GroupBy(r => r.A).Select(g => new { g.Key, S = g.Sum(r => r.V) }).OrderByDescending(x => x.Key).Take(2), false);
        AssertGroupOrder(q => q.GroupBy(r => r.A).Select(g => new { g.Key, C = g.Count() }).OrderBy(x => x.C).ThenBy(x => x.Key), false);
    }

    [Fact]
    public void Sort_Over_All_Composite_Keys_Drops_The_Group_Order()
    {
        AssertGroupOrder(q => q.GroupBy(r => new { r.A, r.B }).Select(g => new { g.Key.A, g.Key.B, C = g.Count() }).OrderBy(x => x.A).ThenBy(x => x.B), false);
    }

    [Fact]
    public void Sort_That_Can_Tie_Keeps_The_Group_Order()
    {
        // Keys 1 and 2 have the same count: the stable sort keeps them in group order.
        AssertGroupOrder(q => q.GroupBy(r => r.A).Select(g => new { g.Key, C = g.Count() }).OrderBy(x => x.C), true);
        AssertGroupOrder(q => q.GroupBy(r => new { r.A, r.B }).Select(g => new { g.Key.A, g.Key.B, C = g.Count() }).OrderBy(x => x.A), true);
    }

    [Fact]
    public void Unsorted_Groups_Keep_Their_Order()
    {
        AssertGroupOrder(q => q.GroupBy(r => r.A).Select(g => new { g.Key, S = g.Sum(r => r.V) }), true);
        AssertGroupOrder(q => q.GroupBy(r => r.A).Select(g => new { g.Key, S = g.Sum(r => r.V) }).Take(2), true);
    }

    /// <summary>
    /// A slice selects groups by their order even when its own output order is not observed (an aggregate over it).
    /// Polars pushes the slice into the group_by and then drops maintain_order; the lowering must prevent that.
    /// Run several times: the wrong result is a random choice of groups.
    /// </summary>
    [Fact]
    public void Slice_Of_Groups_Under_An_Order_Free_Aggregate_Takes_The_First_Groups()
    {
        var rows = Enumerable.Range(0, 60).Select(i => new GoRow(i, i % 7, i % 3, i * 13 % 50)).ToArray();
        using var df = DataFrame.FromRows(rows);

        void Check<T>(Func<IQueryable<GoRow>, T> query)
        {
            var expected = query(rows.AsQueryable());
            for (int i = 0; i < 8; i++)
                Assert.Equal(expected, query(df.AsQueryable<GoRow>()));
        }

        Check(q => q.GroupBy(r => r.A).Select(g => g.Key).Take(3).Sum());
        Check(q => q.GroupBy(r => r.A).Select(g => g.Sum(r => r.V)).Take(2).Sum());
        Check(q => q.GroupBy(r => r.A).Select(g => g.Key).Skip(2).Take(3).Sum());
        Check(q => q.GroupBy(r => r.A).Select(g => g.Key).Skip(5).Sum());
        Check(q => q.GroupBy(r => r.A).Select(g => g.Key).Skip(100).Count());
        Check(q => q.GroupBy(r => r.A).Select(g => g.Key).TakeLast(2).Sum());
        Check(q => q.GroupBy(r => r.A).Select(g => g.Key).TakeLast(20).Sum());
    }
}
