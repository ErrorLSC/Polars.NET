using Polars.CSharp.Linq;
using Polars.NET.Core.Exceptions;
using Polars.NET.Linq;
using Kaminari.Samidare;

namespace Polars.CSharp.Tests;

public record ArgRow(int Id, double? V, bool Flag);

/// <summary>
/// Method-enum and option arguments of LINQ marker methods (Rank, Diff, Interpolate, Quantile) must either be
/// honored exactly or make the call untranslatable; they are never replaced by a default.
/// </summary>
public class LinqMethodArgumentTests
{
    private static readonly ArgRow[] Rows =
    [
        new(1, 1.0, true),
        new(2, 3.0, false),
        new(3, 6.0, true),
        new(4, 10.0, false),
    ];

    private static bool IsNative<T>(IQueryable<T> query) =>
        !((IQueryExplainable)query).InspectPlan(true).IsFullFallback;

    [Fact]
    public void Diff_With_Public_NullBehavior_Pushes_Down()
    {
        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<ArgRow>().Select(r => new { r.Id, D = r.V.Diff(1, NullBehavior.Ignore) });

        Assert.True(IsNative(query));
        Assert.Equal([null, 2.0, 3.0, 4.0], query.ToList().Select(x => x.D));
    }

    [Fact]
    public void Rank_Seed_Is_Honored()
    {
        // All values tie, so Random ranking is decided by the seed alone.
        using var df = DataFrame.FromRows(Enumerable.Range(0, 40).Select(i => new ArgRow(i, 1.0, true)).ToArray());

        List<double> RanksWith(ulong seed) =>
            df.AsQueryable<ArgRow>()
                .Select(r => new { r.Id, R = r.V.Rank(RankMethod.Random, false, seed) })
                .ToList()
                .Select(x => x.R)
                .ToList();

        var first = RanksWith(42);
        Assert.Equal(first, RanksWith(42));
        Assert.Equal(first, RanksWith(42));
        Assert.NotEqual(first, RanksWith(7));
    }

    [Fact]
    public void Rank_With_Null_Seed_Translates()
    {
        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<ArgRow>().Select(r => new { r.Id, R = r.V.Rank(RankMethod.Min, true, null) });

        Assert.True(IsNative(query));
        Assert.Equal([4.0, 3.0, 2.0, 1.0], query.ToList().Select(x => x.R));
    }

    [Fact]
    public void Undefined_Rank_Method_Is_Rejected_Not_Defaulted()
    {
        using var df = DataFrame.FromRows(Rows);
        var invalid = (RankMethod)42;
        var query = df.AsQueryable<ArgRow>().Select(r => new { r.Id, R = r.V.Rank(invalid) });

        var ex = Assert.ThrowsAny<ArgumentOutOfRangeException>(() => query.ToList());
        Assert.Contains("42 is not a valid rank method", ex.Message);
    }

    private static double Scale(double v) => v * 10;

    [Fact]
    public void Undefined_Rank_Method_Inside_A_Udf_Argument_Is_Rejected()
    {
        // The invalid argument is found while translating the UDF's argument; it must not be swallowed into
        // "no UDF" and the query quietly evaluated elsewhere.
        using var df = DataFrame.FromRows(Rows);
        var invalid = (RankMethod)42;
        var query = df.AsQueryable<ArgRow>().Select(r => new { r.Id, R = Scale(r.V.Rank(invalid)) });

        var ex = Assert.ThrowsAny<ArgumentOutOfRangeException>(() => query.ToList());
        Assert.Contains("42 is not a valid rank method", ex.Message);
    }

    [Fact]
    public void Undefined_Quantile_Method_Is_Rejected_Not_Defaulted()
    {
        using var df = DataFrame.FromRows(Rows);
        var invalid = (QuantileMethod)99;
        var query = df.AsQueryable<ArgRow>()
            .GroupBy(r => r.Flag)
            .Select(g => new { g.Key, Q = g.Select(x => x.V).Quantile(0.5, invalid) });

        var ex = Assert.ThrowsAny<ArgumentOutOfRangeException>(() => query.ToList());
        Assert.Contains("99 is not a valid quantile method", ex.Message);
    }

    [Fact]
    public void Row_Dependent_Interpolation_Method_Is_Not_Defaulted_To_Linear()
    {
        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<ArgRow>()
            .Select(r => new { r.Id, I = r.V.Interpolate(r.Flag ? InterpolationMethod.Linear : InterpolationMethod.Nearest) });

        // Previously translated as Linear for every row. Now it is untranslatable, so the projection runs on
        // the client, where the marker method refuses to evaluate.
        var ex = Assert.Throws<InvalidOperationException>(() => query.ToList());
        Assert.Contains("Interpolate", ex.Message);
    }

    [Fact]
    public void Group_Dependent_Quantile_Method_Is_Not_Defaulted_To_Nearest()
    {
        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<ArgRow>()
            .GroupBy(r => r.Flag)
            .Select(g => new { g.Key, Q = g.Select(x => x.V).Quantile(0.5, g.Key ? QuantileMethod.Linear : QuantileMethod.Higher) });

        // Previously computed with Nearest for every group; a per-group method cannot be expressed natively.
        Assert.ThrowsAny<Exception>(() => query.ToList());
    }

    private static IQueryable<AsOfRow> AsOfQuery(DataFrame trades, DataFrame quotes, AsOfTolerance? tolerance, AsofStrategy strategy) =>
        trades.AsQueryable<AsOfTrade>()
            .JoinAsOf(quotes.AsQueryable<AsOfQuote>(), t => t.Time, q => q.Time, (t, q) => new AsOfRow(t.Time, q.Price), tolerance, strategy);

    private static DataFrame AsOfTrades() => DataFrame.FromRows([new AsOfTrade(1, "A"), new AsOfTrade(7, "A")]);

    private static DataFrame AsOfQuotes() => DataFrame.FromRows([new AsOfQuote(0, "A", 10), new AsOfQuote(6, "A", 60)]);

    [Fact]
    public void Undefined_AsOf_Strategy_Is_Rejected_Not_Defaulted_To_Backward()
    {
        using var trades = AsOfTrades();
        using var quotes = AsOfQuotes();

        var ex = Assert.ThrowsAny<ArgumentOutOfRangeException>(() => AsOfQuery(trades, quotes, null, (AsofStrategy)9).ToList());
        Assert.Contains("9 is not a valid AsOf strategy", ex.Message);
    }
}
