using Polars.CSharp.Linq;
using Polars.NET.Linq;
using Kaminari.Samidare;
using Pl = Polars.CSharp.Polars;

namespace Polars.Config.Tests;

public record OsRow(int Id, int A, int B);

/// <summary>
/// LINQ orderings are stable by default: rows with equal sort keys keep their input order, as in LINQ to Objects.
/// AllowUnstableOrdering lets the sort leave ties in any order (as SQL ORDER BY does), which lets the native
/// engine drop the order of the rows beneath it.
/// </summary>
public class LinqOrderingStabilityTests
{
    // Groups first appear as (3,1), (1,2), (2,1), (3,2), (1,1), (2,2); sorting the groups by A alone leaves ties.
    private static readonly OsRow[] Rows =
    [
        new(1, 3, 1), new(2, 1, 2), new(3, 2, 1), new(4, 3, 2), new(5, 1, 1), new(6, 2, 2), new(7, 3, 1)
    ];

    private static IQueryable<(int A, int B, int C)> GroupsSortedByA(IQueryable<OsRow> q) =>
        q.GroupBy(r => new { r.A, r.B }).Select(g => new { g.Key.A, g.Key.B, C = g.Count() }).OrderBy(x => x.A)
            .Select(x => ValueTuple.Create(x.A, x.B, x.C));

    private static string NativePlan<T>(IQueryable<T> query) => ((IQueryExplainable)query).InspectPlan(true).NativePlan;

    [Fact]
    public void Orderings_Are_Stable_By_Default()
    {
        Assert.False(Pl.Config.AllowUnstableOrdering);
        using var df = DataFrame.FromRows(Rows);
        var query = GroupsSortedByA(df.AsQueryable<OsRow>());

        Assert.Contains("maintain_order: true", NativePlan(query));
        Assert.Equal(GroupsSortedByA(Rows.AsQueryable()).ToList(), [.. query]);
    }

    [Fact]
    public void Unstable_Orderings_Still_Sort_By_Their_Keys()
    {
        using var _ = Pl.Config.SetAllowUnstableOrdering().BeginScope();
        Assert.True(Pl.Config.AllowUnstableOrdering);

        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<OsRow>().OrderBy(r => r.A).Select(r => r.Id);

        Assert.DoesNotContain("maintain_order: true", NativePlan(query));
        var sorted = query.ToList();
        Assert.Equal(Rows.Select(r => r.Id).Order(), sorted.Order());
        Assert.Equal(Rows.OrderBy(r => r.A).Select(r => r.A), sorted.Select(id => Rows.Single(r => r.Id == id).A));
    }

    [Fact]
    public void The_Setting_Is_Scoped()
    {
        using (Pl.Config.SetAllowUnstableOrdering().BeginScope())
        {
            Assert.True(Pl.Config.AllowUnstableOrdering);
        }

        Assert.False(Pl.Config.AllowUnstableOrdering);
    }
}
