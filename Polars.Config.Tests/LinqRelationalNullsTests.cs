using Polars.CSharp.Linq;
using Polars.NET.Linq;
using Pl = Polars.CSharp.Polars;

namespace Polars.Config.Tests;

public class RnRow
{
    public long Id { get; set; }
    public string? A { get; set; }
    public string? B { get; set; }
    public long? V { get; set; }
}

/// <summary>
/// UseRelationalNulls switches LINQ comparisons from C# null semantics (the default, as EF Core) to relational
/// three-valued semantics (as EF Core's UseRelationalNulls()): a comparison with a null operand is null, so a
/// filter drops the row, and correlated subqueries never match null keys.
/// </summary>
public class LinqRelationalNullsTests
{
    private static readonly RnRow[] Rows =
    [
        new() { Id = 1, A = "x", B = "x", V = 10 }, new() { Id = 2, A = "x", B = "y", V = null },
        new() { Id = 3, A = null, B = "y", V = 30 }, new() { Id = 4, A = null, B = null, V = null },
    ];

    private static List<long> EqualIds(DataFrame df) =>
        df.AsQueryable<RnRow>().Where(x => x.A == x.B).Select(x => x.Id).ToList();

    [Fact]
    public void Equality_Is_Null_On_A_Null_Operand()
    {
        using var df = DataFrame.From(Rows);
        using (Pl.Config.SetUseRelationalNulls(true).BeginScope())
        {
            Assert.True(Pl.Config.UseRelationalNulls);
            Assert.Equal([1L], EqualIds(df));
            Assert.Equal([2L], df.AsQueryable<RnRow>().Where(x => x.A != x.B).Select(x => x.Id).ToList());
            Assert.Equal([3L], df.AsQueryable<RnRow>().Where(x => x.V != 10).Select(x => x.Id).ToList());
            Assert.Equal([1L], df.AsQueryable<RnRow>().Where(x => !(x.V > 15)).Select(x => x.Id).ToList());
        }

        Assert.False(Pl.Config.UseRelationalNulls);
    }

    [Fact]
    public void Lifted_Comparison_In_A_Projection_Is_Null_On_A_Null_Operand()
    {
        using var df = DataFrame.From(Rows);
        using (Pl.Config.SetUseRelationalNulls(true).BeginScope())
        {
            var query = df.AsQueryable<RnRow>().Select(x => new { x.Id, Gt = (bool?)(x.V > 15) });

            Assert.Equal([false, null, true, null], query.ToList().Select(r => r.Gt));
        }
    }

    [Fact]
    public void Correlated_Subquery_Never_Matches_Null_Keys()
    {
        using var df = DataFrame.From(Rows);
        using var other = DataFrame.From(Rows);
        var others = other.AsQueryable<RnRow>();
        List<long> Matching() =>
            df.AsQueryable<RnRow>().Where(x => others.Any(y => y.B == x.A && y.Id != x.Id)).Select(x => x.Id).ToList();

        Assert.Equal(Rows.Where(x => Rows.Any(y => y.B == x.A && y.Id != x.Id)).Select(x => x.Id), Matching());
        using (Pl.Config.SetUseRelationalNulls(true).BeginScope())
        {
            Assert.Equal([2L], Matching());
        }
    }

    [Fact]
    public void The_Setting_Is_Part_Of_The_Query_Cache_Key()
    {
        using var df = DataFrame.From(Rows);

        Assert.Equal([1L, 4L], EqualIds(df));
        Assert.Equal([1L, 4L], EqualIds(df));
        using (Pl.Config.SetUseRelationalNulls(true).BeginScope())
        {
            Assert.Equal([1L], EqualIds(df));
        }

        Assert.Equal([1L, 4L], EqualIds(df));
    }
}
