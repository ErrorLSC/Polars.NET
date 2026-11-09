using Polars.CSharp.Linq;
using Polars.NET.Linq;
using Kaminari.Samidare;

namespace Polars.CSharp.Tests;

/// <summary>
/// A pure subquery is grafted into the outer plan as an IR subtree (its operators visible to the optimizer)
/// rather than bound as an opaque source, wherever it is written: inline in a lambda (its root read through
/// the closure), or captured as a query variable. The right side of a semi or anti join observes no order, so
/// its GroupBy and Distinct drop their order guarantees; a Limit there still needs its input's order.
/// </summary>
public class LinqSubqueryGraftingTests
{
    private static readonly GoRow[] Rows =
        Enumerable.Range(0, 60).Select(i => new GoRow(i, i % 7, i % 3, i * 13 % 50)).ToArray();

    /// Runs the query natively and with LINQ to Objects (the subquery reading `inner`), compares the results
    /// in order and returns the native optimized logical plan.
    private static string AssertAgrees<T>(Func<IQueryable<GoRow>, IQueryable<GoRow>, IQueryable<T>> query)
    {
        using var df = DataFrame.FromRows(Rows);
        using var dfInner = DataFrame.FromRows(Rows);
        var native = query(df.AsQueryable<GoRow>(), dfInner.AsQueryable<GoRow>());

        Assert.Equal(query(Rows.AsQueryable(), Rows.AsQueryable()).ToList(), native.ToList());
        return ((IQueryExplainable)native).InspectPlan(true).LogicalPlan!.Value;
    }

    [Fact]
    public void Inline_Contains_Subquery_Is_Grafted()
    {
        var plan = AssertAgrees((q, inner) => q.Where(r => inner.Where(x => x.V > 40).Select(x => x.A).Contains(r.B)).Select(r => r.Id));

        Assert.Contains("how=Semi", plan);
        Assert.Contains("Filter: (col(\"V\") > 40)", plan);
    }

    [Fact]
    public void Captured_Subquery_Variable_Is_Grafted()
    {
        var plan = AssertAgrees((q, inner) =>
        {
            var ids = inner.OrderByDescending(x => x.V).ThenBy(x => x.Id).Take(10).Select(x => x.Id);
            return q.Where(r => !ids.Contains(r.Id)).Select(r => r.Id);
        });

        Assert.Contains("how=Anti", plan);
        Assert.Contains("Limit: offset=0 count=10", plan);
        Assert.Contains("Sort:", plan);
    }

    [Fact]
    public void Grouped_Subquery_With_A_Group_Predicate_Is_Grafted()
    {
        // The key Select is kept with its GroupBy: GroupBy(..).Where(g => ..) alone has grouping elements and
        // cannot be compiled on its own (this used to throw).
        var plan = AssertAgrees((q, inner) =>
            q.Where(r => !inner.GroupBy(x => x.A).Where(g => g.Sum(x => x.V) > 220).Select(g => g.Key).Contains(r.A)).Select(r => r.Id));

        Assert.Contains("how=Anti", plan);
        Assert.Contains("GroupBy:", plan);
    }

    [Fact]
    public void Semi_Join_Right_Side_Drops_Group_And_Distinct_Order()
    {
        var grouped = AssertAgrees((q, inner) =>
            q.Where(r => inner.GroupBy(x => x.A).Select(g => new { g.Key, T = g.Sum(x => x.V) }).Where(t => t.T > 220).Select(t => t.Key).Contains(r.B)).Select(r => r.Id));
        Assert.Contains("maintainOrder=false", grouped);
        Assert.DoesNotContain("maintainOrder=true", grouped);

        var distinct = AssertAgrees((q, inner) =>
            q.Where(r => inner.Where(x => x.V > 25).Select(x => x.A).Distinct().Contains(r.B)).Select(r => r.Id));
        Assert.Contains("Distinct: subset=None keep=First maintainOrder=false", distinct);
    }

    [Fact]
    public void Semi_Join_Right_Side_Keeps_The_Order_A_Limit_Reads()
    {
        // Which groups the Take keeps depends on group order, so the GroupBy beneath it keeps its order.
        var plan = AssertAgrees((q, inner) =>
            q.Where(r => inner.GroupBy(x => x.A).Select(g => g.Key).Take(3).Contains(r.A)).Select(r => r.Id));

        Assert.Contains("Limit:", plan);
        Assert.Contains("maintainOrder=true", plan);
    }

    [Fact]
    public void Semi_Join_Left_Side_Keeps_The_Outer_Order()
    {
        AssertAgrees((q, inner) =>
            q.GroupBy(r => r.B).Select(g => new { g.Key, N = g.Count() })
                .Where(t => inner.Where(x => x.V < 10).Select(x => x.B).Contains(t.Key)));
    }
}
