using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests;

public record AsOfTrade(long Time, string Sym);
public record AsOfQuote(long Time, string Sym, int? Price);
public record AsOfRow(long Time, int? Price);
public record StampRow(int Id, DateTime Ts);
public record TaggedRow(int Id, List<string> Tags, int[] Scores);

/// <summary>
/// Pins LINQ behaviors whose IR representation depends on type / join / strategy vocabulary:
/// AsOf join strategies, by-keys and tolerance, and casts to DateTimeOffset / decimal.
/// </summary>
public class LinqIrVocabularyTests
{
    // Outer:  1(A)  7(A)  10(B)
    // Inner:  0(A)=10  4(A)=40  6(B)=60  9(B)=90   (sorted by Time)
    private static DataFrame Trades(params long[] times) =>
        DataFrame.FromRows(times.Select((t, i) => new AsOfTrade(t, i < 2 ? "A" : "B")).ToArray());

    private static DataFrame Quotes() =>
        DataFrame.FromRows([
            new AsOfQuote(0, "A", 10),
            new AsOfQuote(4, "A", 40),
            new AsOfQuote(6, "B", 60),
            new AsOfQuote(9, "B", 90),
        ]);

    private static int?[] RunAsOf(DataFrame trades, DataFrame quotes, AsofStrategy strategy, AsOfTolerance? tolerance = null)
    {
        var query = trades.AsQueryable<AsOfTrade>()
            .JoinAsOf(
                quotes.AsQueryable<AsOfQuote>(),
                t => t.Time,
                q => q.Time,
                (t, q) => new AsOfRow(t.Time, q.Price),
                tolerance,
                strategy)
            .OrderBy(r => r.Time)
            .Select(r => r.Price);

        Assert.Contains("100% Native Pushdown", query.Explain());
        return query.ToArray();
    }

    [Theory]
    [InlineData(AsofStrategy.Backward, new[] { 10, 60, 90 })]
    [InlineData(AsofStrategy.Nearest, new[] { 10, 60, 90 })]
    [Trait("LINQ", "IrVocabulary")]
    public void JoinAsOf_Strategy_Matches_Expected(AsofStrategy strategy, int[] expected)
    {
        using var trades = Trades(1, 7, 10);
        using var quotes = Quotes();

        Assert.Equal(expected.Select(v => (int?)v), RunAsOf(trades, quotes, strategy));
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void JoinAsOf_Forward_Leaves_Unmatched_As_Null()
    {
        using var trades = Trades(1, 7, 10);
        using var quotes = Quotes();

        Assert.Equal([40, 90, null], RunAsOf(trades, quotes, AsofStrategy.Forward));
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void JoinAsOf_Backward_With_Integer_Tolerance()
    {
        using var trades = Trades(2, 7, 10);
        using var quotes = Quotes();

        // 2 -> 0 is 2 apart (> tolerance 1) -> null; 7 -> 6 and 10 -> 9 are within tolerance.
        Assert.Equal([null, 60, 90], RunAsOf(trades, quotes, AsofStrategy.Backward, 1L));
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void JoinAsOf_Backward_By_Group_Key()
    {
        using var trades = Trades(1, 7, 10);
        using var quotes = Quotes();

        var prices = trades.AsQueryable<AsOfTrade>()
            .JoinAsOf(
                quotes.AsQueryable<AsOfQuote>(),
                t => t.Time,
                q => q.Time,
                t => t.Sym,
                q => q.Sym,
                (t, q) => new AsOfRow(t.Time, q.Price))
            .OrderBy(r => r.Time)
            .Select(r => r.Price)
            .ToArray();

        // 7(A) must not see 6(B): latest A quote at or before 7 is 4 -> 40.
        Assert.Equal([10, 40, 90], prices);
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void Cast_DateTime_To_DateTimeOffset_Is_Utc()
    {
        var ts = new DateTime(2025, 3, 4, 5, 6, 7);
        using var df = DataFrame.FromRows([new StampRow(1, ts)]);

        var result = df.AsQueryable<StampRow>()
            .Select(s => new { s.Id, Dto = (DateTimeOffset)s.Ts })
            .ToList();

        Assert.Single(result);
        Assert.Equal(TimeSpan.Zero, result[0].Dto.Offset);
        Assert.Equal(new DateTimeOffset(ts, TimeSpan.Zero), result[0].Dto);
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void Cast_Int_To_Decimal()
    {
        using var df = DataFrame.FromRows([new StampRow(7, DateTime.UnixEpoch)]);

        var result = df.AsQueryable<StampRow>()
            .Select(s => new { Value = (decimal)s.Id })
            .ToList();

        Assert.Equal(7m, result[0].Value);
    }

    private static DataFrame TaggedRows() =>
        DataFrame.FromRows([
            new TaggedRow(1, ["a", "b"], [10, 20, 30]),
            new TaggedRow(2, [], [5]),
            new TaggedRow(3, ["c"], []),
        ]);

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void Cast_Collection_Upcast_To_Interface_Preserves_Elements()
    {
        using var df = TaggedRows();

        var query = df.AsQueryable<TaggedRow>()
            .Select(r => new
            {
                r.Id,
                Tags = (IEnumerable<string>)r.Tags,
                Scores = (IReadOnlyList<int>)r.Scores
            })
            .OrderBy(r => r.Id);

        // The upcasts must reach the IR as casts (not be dropped or evaluated client-side).
        Assert.Contains("CAST(", query.ExplainLogicalPlan());
        Assert.Contains("100% Native Pushdown", query.Explain());

        var result = query.ToList();

        Assert.Equal(3, result.Count);
        Assert.Equal(["a", "b"], result[0].Tags);
        Assert.Empty(result[1].Tags);
        Assert.Equal(["c"], result[2].Tags);
        Assert.Equal([10, 20, 30], result[0].Scores);
        Assert.Equal([5], result[1].Scores);
        Assert.Empty(result[2].Scores);
    }

    [Fact]
    [Trait("LINQ", "IrVocabulary")]
    public void Cast_Collection_Upcast_Then_Count()
    {
        using var df = TaggedRows();

        var counts = df.AsQueryable<TaggedRow>()
            .OrderBy(r => r.Id)
            .Select(r => ((IEnumerable<string>)r.Tags).Count())
            .ToList();

        Assert.Equal([2, 0, 1], counts);
    }
}
