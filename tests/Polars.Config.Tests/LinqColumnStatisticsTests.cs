using Polars.CSharp;
using Polars.CSharp.Linq;
using Polars.NET.Core.Config;

namespace Polars.Config.Tests;

/// <summary>Column statistics change plan choices only: results stay the same with them on.</summary>
public class LinqColumnStatisticsTests
{
    public record Sale(int ItemKey, int StoreKey, int DayKey, long Amount);
    public record Item(int ItemId, int ItemTag);
    public record Store(int StoreId, int StoreTag);
    public record Day(int DayId, int Year);

    private static readonly Sale[] Sales = [.. Enumerable.Range(0, 500).Select(i => new Sale(i % 20, i % 7, i % 30, i))];
    private static readonly Item[] Items = [.. Enumerable.Range(0, 20).Select(i => new Item(i, i % 4))];
    private static readonly Store[] Stores = [.. Enumerable.Range(0, 7).Select(i => new Store(i, i % 2))];
    private static readonly Day[] Days = [.. Enumerable.Range(0, 30).Select(i => new Day(i, 2000 + i % 3))];

    [Fact]
    public void A_registered_feedback_store_receives_reports_until_disposed()
    {
        var schema = Polars.NET.Linq.IR.LogicalSchema.OfFields(
            Microsoft.FSharp.Collections.ListModule.OfArray(new[] { Tuple.Create("k", Polars.NET.Linq.IR.LogicalTypes.Int32) }));
        var plan = Polars.NET.Linq.IR.LogicalPlan.NewScan(Polars.NET.Linq.IR.LogicalPlanSource.NewSlot(0), schema);
        var sources = new[] { Polars.NET.Linq.IR.SourceBinding.NewTable(new IdentifiedSource(schema)) };
        var key = Polars.NET.Linq.IR.CardinalityFeedback.TryKey(sources, plan).Value;
        var store = new Polars.NET.Linq.IR.InMemoryCardinalityFeedback();
        var feedback = (Polars.NET.Linq.IR.ICardinalityFeedback)store;

        using (Polars.NET.Linq.IR.QueryStatistics.RegisterFeedback(store))
        {
            Polars.NET.Linq.IR.CardinalityFeedback.Report(sources, plan, 42);
            Assert.Equal(42.0, feedback.TryGetObserved(key).Value.Rows);
        }

        Polars.NET.Linq.IR.CardinalityFeedback.Report(sources, plan, 7);
        Assert.Equal(42.0, feedback.TryGetObserved(key).Value.Rows);
    }

    private sealed class IdentifiedSource(Polars.NET.Linq.IR.LogicalSchema schema)
        : Polars.NET.Linq.IR.ILogicalTableSource, Polars.NET.Linq.IR.ISourceIdentity
    {
        public string Name => "file";
        public Polars.NET.Linq.IR.LogicalSchema Schema => schema;
        public Microsoft.FSharp.Core.FSharpOption<string> SourceIdentity => Microsoft.FSharp.Core.FSharpOption<string>.Some("file-a");
    }

    [Fact]
    public void Scalar_terminals_over_joins_match_LINQ_to_Objects()
    {
        using var sales = DataFrame.FromRows(Sales);
        using var items = DataFrame.FromRows(Items);
        using var stores = DataFrame.FromRows(Stores);
        using var days = DataFrame.FromRows(Days);

        IQueryable<long> Native() =>
            from s in sales.AsQueryable<Sale>()
            join i in items.AsQueryable<Item>().Where(i => i.ItemTag == 1) on s.ItemKey equals i.ItemId
            join t in stores.AsQueryable<Store>() on s.StoreKey equals t.StoreId
            join d in days.AsQueryable<Day>().Where(d => d.Year == 2001) on s.DayKey equals d.DayId
            select s.Amount;

        var expected = (from s in Sales
                        join i in Items.Where(i => i.ItemTag == 1) on s.ItemKey equals i.ItemId
                        join t in Stores on s.StoreKey equals t.StoreId
                        join d in Days.Where(d => d.Year == 2001) on s.DayKey equals d.DayId
                        select s.Amount).ToList();

        var original = CoreConfig.CollectColumnStatistics;
        try
        {
            CoreConfig.CollectColumnStatistics = true;
            Assert.Equal(expected.Sum(), Native().Sum());
            Assert.Equal(expected.Count, Native().Count());
            Assert.Equal(expected.Min(), Native().Min());
            Assert.Equal(expected.Max(), Native().Max());
        }
        finally
        {
            CoreConfig.CollectColumnStatistics = original;
        }
    }
}
