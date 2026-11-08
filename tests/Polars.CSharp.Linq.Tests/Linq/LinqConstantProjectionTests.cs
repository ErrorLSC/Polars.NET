using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests;

public record CpRow(int Id, string Name);
public enum CpShade { A = 1, B = 2 }

/// <summary>
/// A projection that reads no column (a constant) yields one row per source row, as in LINQ to Objects, although a
/// Polars select of constants alone yields a single row.
/// </summary>
public class LinqConstantProjectionTests
{
    private static readonly CpRow[] Rows = [new(1, "a"), new(2, "b"), new(3, "c")];

    private static void AssertMatches<T>(Func<IQueryable<CpRow>, IQueryable<T>> query)
    {
        using var df = DataFrame.FromRows(Rows);
        Assert.Equal(query(Rows.AsQueryable()).ToList(), query(df.AsQueryable<CpRow>()).ToList());
    }

    [Fact]
    public void Constants_Repeat_Per_Row()
    {
        AssertMatches(q => q.Select(r => 7));
        AssertMatches(q => q.Select(r => "x"));
        AssertMatches(q => q.Select(r => CpShade.B));
        AssertMatches(q => q.Select(r => 1 + 2));
        AssertMatches(q => q.Select(r => new { A = 1, B = "s" }));
    }

    [Fact]
    public void Constants_Follow_The_Rows_Left_By_Earlier_Steps()
    {
        AssertMatches(q => q.Where(r => r.Id > 1).Select(r => 7));
        AssertMatches(q => q.Where(r => r.Id > 10).Select(r => 7));
        AssertMatches(q => q.OrderBy(r => r.Id).Take(2).Select(r => 1));
        AssertMatches(q => q.GroupBy(r => r.Id % 2).Select(g => 1));
    }

    [Fact]
    public void Steps_After_A_Constant_Projection()
    {
        AssertMatches(q => q.Select(r => 7).Where(x => x > 0));
        AssertMatches(q => q.Select(r => 1).Distinct());

        using var df = DataFrame.FromRows(Rows);
        Assert.Equal(6, df.AsQueryable<CpRow>().Select(r => 2).Sum());
        Assert.Equal(3, df.AsQueryable<CpRow>().Select(r => 2).Count());
    }
}
