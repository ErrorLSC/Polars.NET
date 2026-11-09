using Polars.CSharp.Linq;
using Polars.NET.Core.Exceptions;
using Polars.NET.Linq;
using Kaminari.Samidare;

namespace Polars.Config.Tests;

public record WrEmp(string Dept, string Name, int Amount);
public record WrDept(string Dept, string Region);

/// <summary>
/// Aggregations over whole group rows (g.Distinct(), g.ToList(), ...) build a struct of the element's columns.
/// That is only valid when every element property is a column (a projection's anonymous type), not for a
/// join's pair of entities; and a grouping used inside an aggregate is its rows, never its key.
/// </summary>
public class LinqGroupWholeRowTests
{
    private static readonly WrEmp[] Emps = [new("S", "a", 1), new("S", "a", 1), new("S", "b", 2), new("E", "c", 3)];
    private static readonly WrDept[] Depts = [new("S", "EU"), new("E", "US")];

    private static List<string> Sorted<T>(IEnumerable<T> rows) => rows.Select(r => r!.ToString()!).Order().ToList();

    private static bool IsNative<T>(IQueryable<T> q) => !((IQueryExplainable)q).InspectPlan(true).IsFullFallback;

    [Fact]
    [Trait("LINQ", "GroupWholeRow")]
    public void GroupBy_Projection_Whole_Row_Distinct_Is_Native_And_Correct()
    {
        using var e = DataFrame.FromRows(Emps);
        var query = e.AsQueryable<WrEmp>().Select(x => new { x.Dept, x.Name }).GroupBy(x => x.Dept).Select(g => new { g.Key, N = g.Distinct().Count() });
        var expected = Emps.Select(x => new { x.Dept, x.Name }).GroupBy(x => x.Dept).Select(g => new { g.Key, N = g.Distinct().Count() });

        Assert.True(IsNative(query));
        Assert.Equal(Sorted(expected), Sorted(query));
    }

    [Fact]
    [Trait("LINQ", "GroupWholeRow")]
    public void RollUp_Projection_Whole_Row_Distinct_Is_Native_And_Correct()
    {
        using var e = DataFrame.FromRows(Emps);
        var query = e.AsQueryable<WrEmp>().Select(x => new { x.Dept, x.Name }).GroupBy(x => new { x.Dept }).RollUp((k, g) => new { k.Dept, N = g.Distinct().Count() });

        Assert.True(IsNative(query));
        Assert.Equal(["{ Dept = , N = 3 }", "{ Dept = E, N = 1 }", "{ Dept = S, N = 2 }"], Sorted(query));
    }

    [Fact]
    [Trait("LINQ", "GroupWholeRow")]
    public void GroupBy_Entity_Pair_Whole_Row_Is_Not_Pushed_Down()
    {
        using var e = DataFrame.FromRows(Emps);
        using var d = DataFrame.FromRows(Depts);
        IQueryable<object> Query() => e.AsQueryable<WrEmp>()
            .Join(d.AsQueryable<WrDept>(), x => x.Dept, y => y.Dept, (x, y) => new { x, y })
            .GroupBy(p => p.y.Region)
            .Select(g => new { g.Key, N = g.Distinct().Count() })
            .Cast<object>();
        // Previously translated as list operations on the key column and failed inside Polars.
        Assert.Throws<PolarsPushdownException>(() => Query().ToList());
    }

    [Fact]
    [Trait("LINQ", "GroupWholeRow")]
    public void RollUp_Entity_Pair_Whole_Row_Is_Not_Pushed_Down()
    {
        using var e = DataFrame.FromRows(Emps);
        using var d = DataFrame.FromRows(Depts);
        var query = e.AsQueryable<WrEmp>()
            .Join(d.AsQueryable<WrDept>(), x => x.Dept, y => y.Dept, (x, y) => new { x, y })
            .GroupBy(p => new { p.y.Region })
            .RollUp((k, g) => new { k.Region, N = g.Distinct().Count() });

        // Previously built a struct of the non-existent columns "x" and "y" and failed inside Polars.
        Assert.Throws<PolarsPushdownException>(() => query.ToList());
    }
}
