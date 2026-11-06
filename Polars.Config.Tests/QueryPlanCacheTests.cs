using System.Linq.Expressions;
using Microsoft.FSharp.Collections;
using Polars.CSharp.Linq;
using Polars.NET.Core.Enums;
using Polars.NET.Linq;
using Polars.NET.Linq.IR;

using Pl = Polars.CSharp.Polars;

namespace Polars.Config.Tests;

public class QueryPlanCacheTests
{
    private class Customer
    {
        public int Id { get; set; }
        public string City { get; set; } = "";
        public decimal Balance { get; set; }
    }

    private static readonly Customer[] Customers =
    [
        new Customer { Id = 1, City = "Beijing", Balance = 100m },
        new Customer { Id = 2, City = "Shanghai", Balance = 200m },
        new Customer { Id = 3, City = "Tokyo", Balance = 300m },
    ];

    private static IQueryable<Customer> FilterByCity(DataFrame df, string city) =>
        df.AsQueryable<Customer>().Where(c => c.City == city);

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheHit_Same_Query_And_Values_Reuses_The_Compiled_Pipeline()
    {
        QueryPlanCache.Clear();
        using var df = DataFrame.FromRows(Customers);

        // First sighting: compiled, not admitted.
        Assert.Equal("Beijing", Assert.Single(FilterByCity(df, "Beijing").ToList()).City);
        Assert.Equal(0, QueryPlanCache.Count);

        // Second sighting: compiled again and admitted.
        Assert.Equal("Beijing", Assert.Single(FilterByCity(df, "Beijing").ToList()).City);
        Assert.Equal(0, QueryPlanCache.Hits);
        Assert.Equal(1, QueryPlanCache.Count);

        // From then on: hits.
        Assert.Equal("Beijing", Assert.Single(FilterByCity(df, "Beijing").ToList()).City);
        Assert.Equal(1, QueryPlanCache.Hits);
        Assert.Equal(1, QueryPlanCache.Count);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void One_Off_Parameter_Values_Never_Enter_The_Cache()
    {
        QueryPlanCache.Clear();
        using var df = DataFrame.FromRows(Customers);

        for (int id = 100; id < 150; id++)
        {
            var captured = id;
            Assert.Empty(df.AsQueryable<Customer>().Where(c => c.Id == captured).ToList());
        }
        Assert.Equal(0, QueryPlanCache.Count);

        var repeated = 2;
        _ = df.AsQueryable<Customer>().Where(c => c.Id == repeated).ToList();
        _ = df.AsQueryable<Customer>().Where(c => c.Id == repeated).ToList();
        Assert.Equal(1, QueryPlanCache.Count);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheMiss_Different_Parameter_Values_Compile_Separately()
    {
        QueryPlanCache.Clear();
        using var df = DataFrame.FromRows(Customers);

        Assert.Equal("Beijing", Assert.Single(FilterByCity(df, "Beijing").ToList()).City);
        Assert.Equal("Shanghai", Assert.Single(FilterByCity(df, "Shanghai").ToList()).City);
        Assert.Equal("Tokyo", Assert.Single(FilterByCity(df, "Tokyo").ToList()).City);

        Assert.Equal(0, QueryPlanCache.Hits);
        Assert.Equal(0, QueryPlanCache.Count);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheHit_Over_Another_Frame_Binds_That_Frame()
    {
        QueryPlanCache.Clear();
        using var dfA = DataFrame.FromRows(Customers);
        using var dfB = DataFrame.FromRows(
        [
            new Customer { Id = 7, City = "Beijing", Balance = 700m },
            new Customer { Id = 8, City = "Beijing", Balance = 800m },
        ]);

        Assert.Equal([1], FilterByCity(dfA, "Beijing").Select(c => c.Id).ToList());
        Assert.Equal([1], FilterByCity(dfA, "Beijing").Select(c => c.Id).ToList());
        Assert.Equal([7, 8], FilterByCity(dfB, "Beijing").Select(c => c.Id).ToList());
        Assert.Equal(1, QueryPlanCache.Hits);
        Assert.Equal(900m, FilterByCity(dfB, "Beijing").Max(c => c.Balance) + 100m);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheMiss_Root_With_Different_Columns()
    {
        QueryPlanCache.Clear();
        using var df = DataFrame.FromRows(Customers);
        using var reordered = df.Select("City", "Id", "Balance");

        for (int i = 0; i < 2; i++)
        {
            Assert.Equal([1], FilterByCity(df, "Beijing").Select(c => c.Id).ToList());
            Assert.Equal([1], FilterByCity(reordered, "Beijing").Select(c => c.Id).ToList());
        }
        Assert.Equal(0, QueryPlanCache.Hits);
        Assert.Equal(2, QueryPlanCache.Count);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheMiss_When_Udf_Pushdown_Setting_Changes()
    {
        QueryPlanCache.Clear();
        using var df = DataFrame.FromRows(Customers);
        IQueryable<Customer> Shape() => df.AsQueryable<Customer>().Where(c => Mask(c.City).StartsWith('S'));

        var pushed = (IPolarsExplainable)Shape();
        Assert.True(pushed.InspectPlan(true).HasUdf);

        using (Pl.Config.SetDisallowUdfPushdown(true).BeginScope())
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            var inspection = ((IPolarsExplainable)Shape()).InspectPlan(true);
            Assert.False(inspection.HasUdf);
            Assert.True(inspection.HasClientPredicates);
            Assert.Equal(["Shanghai"], Shape().Select(c => c.City).ToList());
        }

        // Back to the default: the cached pushed-down compilation is reused, UDF diagnostics included.
        Assert.True(((IPolarsExplainable)Shape()).InspectPlan(true).HasUdf);
        Assert.Equal(["Shanghai"], Shape().Select(c => c.City).ToList());
    }

    private static string Mask(string city) => city.ToUpperInvariant();

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheHit_Replays_Fallback_Diagnostics()
    {
        QueryPlanCache.Clear();
        using var df = DataFrame.FromRows(Customers);
        // A client ordering key needs the full fallback.
        IQueryable<string> Shape() => df.AsQueryable<Customer>()
            .OrderBy(c => TestUdfMethods.MaskName(c.City))
            .Where(c => c.City.StartsWith("S"))
            .Select(c => c.City);

        using (Pl.Config.SetDisallowUdfPushdown(true).BeginScope())
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            var first = ((IPolarsExplainable)Shape()).InspectPlan(true);
            _ = ((IPolarsExplainable)Shape()).InspectPlan(true);
            var hitsBefore = QueryPlanCache.Hits;
            var second = ((IPolarsExplainable)Shape()).InspectPlan(true);

            Assert.True(QueryPlanCache.Hits > hitsBefore);
            Assert.True(second.IsFullFallback);
            Assert.Equal(first.FallbackReason, second.FallbackReason);
            Assert.Equal(first.FallbackBoundary, second.FallbackBoundary);
            Assert.Equal(["Shanghai"], Shape().ToList());
        }
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void Collection_Parameters_Are_Not_Cached()
    {
        QueryPlanCache.Clear();
        using var df = DataFrame.FromRows(Customers);

        var ids = new List<int> { 1, 3 };
        Assert.Equal([1, 3], df.AsQueryable<Customer>().Where(c => ids.Contains(c.Id)).Select(c => c.Id).ToList());

        // The list is mutable: a cached compilation would keep the old contents.
        ids.Add(2);
        Assert.Equal([1, 2, 3], df.AsQueryable<Customer>().Where(c => ids.Contains(c.Id)).Select(c => c.Id).ToList());
        Assert.Equal(0, QueryPlanCache.Count);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void A_Hit_Binds_Every_Root_Of_The_Query_Being_Compiled()
    {
        QueryPlanCache.Clear();
        using var dfA = DataFrame.FromRows(Customers);
        using var dfB = DataFrame.FromRows([new Customer { Id = 9, City = "Oslo", Balance = 1m }]);
        using var dfC = DataFrame.FromRows([new Customer { Id = 10, City = "Rome", Balance = 2m }]);
        using var dfD = DataFrame.FromRows([new Customer { Id = 11, City = "Lima", Balance = 3m }]);

        List<int> ConcatIds(DataFrame second) =>
            dfA.AsQueryable<Customer>().Concat(second.AsQueryable<Customer>()).Select(c => c.Id).ToList();

        // Seen twice, the two-root pipeline is admitted; the third query hits it with its own second root.
        Assert.Equal([1, 2, 3, 9], ConcatIds(dfB));
        Assert.Equal([1, 2, 3, 10], ConcatIds(dfC));
        var hitsBefore = QueryPlanCache.Hits;
        Assert.Equal([1, 2, 3, 11], ConcatIds(dfD));
        Assert.True(QueryPlanCache.Hits > hitsBefore);
    }

    private class Order
    {
        public int CustomerId { get; set; }
        public decimal Amount { get; set; }
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void Self_Join_And_Join_Of_Two_Tables_Do_Not_Share_A_Plan()
    {
        QueryPlanCache.Clear();
        using var dfA = DataFrame.FromRows(Customers);
        using var dfB = DataFrame.FromRows([new Customer { Id = 2, City = "Oslo", Balance = 1m }]);

        static List<string> Pairs(IQueryable<Customer> left, IQueryable<Customer> right) =>
            left.Join(right, l => l.Id, r => r.Id, (l, r) => l.City + "/" + r.City).ToList();

        // A self-join reads one table (one query) through both roots; it is cached after two sightings.
        var a1 = dfA.AsQueryable<Customer>();
        Assert.Equal(["Beijing/Beijing", "Shanghai/Shanghai", "Tokyo/Tokyo"], Pairs(a1, a1));
        var a2 = dfA.AsQueryable<Customer>();
        Assert.Equal(["Beijing/Beijing", "Shanghai/Shanghai", "Tokyo/Tokyo"], Pairs(a2, a2));

        // Same shape and schemas, but two tables: it must not be served the self-join's single-table binding.
        Assert.Equal(["Shanghai/Oslo"], Pairs(dfA.AsQueryable<Customer>(), dfB.AsQueryable<Customer>()));
    }

    private sealed class FakeTable(LogicalSchema schema) : ILogicalTableSource
    {
        public string Name => "fake";
        public LogicalSchema Schema => schema;
    }

    private static LogicalSchema SchemaOf(params (string Name, LogicalType Type)[] fields) =>
        new(ListModule.OfSeq(fields.Select(f => new LogicalField(f.Name, f.Type, true))));

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void Key_Includes_Root_Column_Types_And_Table_Sharing()
    {
        Expression<Func<int>> shape = () => 0;
        var none = new Dictionary<string, ExtractedParameter>();
        CompiledQueryKey Key(params ILogicalTableSource[] roots) =>
            QueryPlanCache.TryCreateKey(typeof(int), shape, none, roots)!.Value;

        var intTable = new FakeTable(SchemaOf(("Id", LogicalTypes.Int32)));
        var longTable = new FakeTable(SchemaOf(("Id", LogicalTypes.Int64)));
        var otherIntTable = new FakeTable(SchemaOf(("Id", LogicalTypes.Int32)));

        Assert.NotEqual(Key(intTable), Key(longTable));
        Assert.Equal(Key(intTable), Key(otherIntTable));
        Assert.NotEqual(Key(intTable, intTable), Key(intTable, otherIntTable));
        Assert.Equal(Key(intTable, otherIntTable), Key(otherIntTable, intTable));
    }

    /// Replaces the closure read of `inner` inside `query` with the query itself as a constant, the shape F#
    /// query expressions produce for a captured query.
    private static IQueryable<T> InlineRoot<T, TInner>(IQueryable<T> query, IQueryable<TInner> inner)
    {
        var inlined = new InlineQueryable(inner).Visit(query.Expression);
        return query.Provider.CreateQuery<T>(inlined);
    }

    private sealed class InlineQueryable(IQueryable inner) : ExpressionVisitor
    {
        protected override Expression VisitMember(MemberExpression node) =>
            node.Type == inner.GetType() || typeof(IQueryable).IsAssignableFrom(node.Type) && node.Expression is ConstantExpression
                ? Expression.Constant(inner, node.Type)
                : base.VisitMember(node);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void Root_Inside_A_Lambda_Is_Rebound_When_The_Plan_Is_Native()
    {
        QueryPlanCache.Clear();
        using var dfA = DataFrame.FromRows(Customers);

        List<int> WithOrders(Order[] orders)
        {
            using var dfO = DataFrame.FromRows(orders);
            var ordersQuery = dfO.AsQueryable<Order>();
            return InlineRoot(dfA.AsQueryable<Customer>().Where(c => ordersQuery.Any(o => o.CustomerId == c.Id)), ordersQuery)
                .Select(c => c.Id)
                .ToList();
        }

        Assert.Equal([1], WithOrders([new Order { CustomerId = 1, Amount = 5m }]));
        Assert.Equal([2], WithOrders([new Order { CustomerId = 2, Amount = 5m }]));
        var hitsBefore = QueryPlanCache.Hits;
        Assert.Equal([3], WithOrders([new Order { CustomerId = 3, Amount = 5m }]));
        Assert.True(QueryPlanCache.Hits > hitsBefore);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void Root_Inside_A_Client_Predicate_Is_Read_From_The_Query_Being_Run()
    {
        QueryPlanCache.Clear();
        using var dfA = DataFrame.FromRows(Customers);

        // The subquery's predicate cannot be translated, so the whole predicate runs in managed code and its
        // compiled delegate holds the orders root. Each run uses (and then disposes) its own orders table:
        // a cached delegate would read a disposed one.
        List<int> WithOrders(Order[] orders)
        {
            using var dfO = DataFrame.FromRows(orders);
            var ordersQuery = dfO.AsQueryable<Order>();
            return InlineRoot(
                    dfA.AsQueryable<Customer>()
                        .Where(c => ordersQuery.Any(o => o.CustomerId == c.Id && TestUdfMethods.MaskName(c.City) != "")),
                    ordersQuery)
                .Select(c => c.Id)
                .ToList();
        }

        using (Pl.Config.SetDisallowUdfPushdown(true).BeginScope())
        using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
        {
            Assert.Equal([1], WithOrders([new Order { CustomerId = 1, Amount = 5m }]));
            Assert.Equal([2], WithOrders([new Order { CustomerId = 2, Amount = 5m }]));
            Assert.Equal([3], WithOrders([new Order { CustomerId = 3, Amount = 5m }]));
        }
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void Key_Distinguishes_Values_That_Equals_Conflates()
    {
        static CompiledQueryKey? KeyOf(object value)
        {
            Expression<Func<int>> shape = () => 0;
            var parameters = new Dictionary<string, ExtractedParameter>
            {
                ["@p0"] = new ExtractedParameter("@p0", value.GetType(), value),
            };
            var key = QueryPlanCache.TryCreateKey(typeof(int), shape, parameters, []);
            return key?.Value;
        }

        var ticks = new DateTime(2024, 1, 1).Ticks;
        Assert.NotEqual(KeyOf(new DateTime(ticks, DateTimeKind.Utc)), KeyOf(new DateTime(ticks, DateTimeKind.Local)));
        Assert.NotEqual(KeyOf(1.0m), KeyOf(1.00m));
        Assert.NotEqual(KeyOf(0.0), KeyOf(-0.0));
        Assert.NotEqual(KeyOf(1), KeyOf(1L));
        Assert.Equal(KeyOf("x"), KeyOf("x"));
        Assert.Null(KeyOf(new List<int>()));
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheMiss_DifferentQueryStructure_AddsNewKeys()
    {
        using var df = DataFrame.FromRows(
        [
            new Customer { Id = 1, City = "Beijing", Balance = 100m },
            new Customer { Id = 2, City = "Shanghai", Balance = 200m },
        ]);

        int initialCount = QueryPlanCache.Count;

        var city = "Beijing";
        var minBalance = 150m;

        // Each query runs twice so that the admission rule lets it into the cache.
        for (int i = 0; i < 2; i++)
        {
            // Query 1: Where City == param
            _ = df.AsQueryable<Customer>().Where(c => c.City == city).ToList();

            // Query 2: Where Balance > param (different property & condition)
            _ = df.AsQueryable<Customer>().Where(c => c.Balance > minBalance).ToList();

            // Query 3: Where City == param and Balance > param
            _ = df.AsQueryable<Customer>().Where(c => c.City == city && c.Balance > minBalance).ToList();
        }

        Assert.True(QueryPlanCache.Count >= initialCount + 2);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheDisabled_BypassesCache()
    {
        QueryPlanCache.Enabled = false;
        QueryPlanCache.Clear();

        try
        {
            using var df = DataFrame.FromRows(
            [
                new Customer { Id = 1, City = "Beijing", Balance = 100m },
            ]);

            var city1 = "Beijing";
            var q1 = df.AsQueryable<Customer>().Where(c => c.City == city1).ToList();

            var city2 = "Tokyo";
            var q2 = df.AsQueryable<Customer>().Where(c => c.City == city2).ToList();

            Assert.Equal(0, QueryPlanCache.Hits);
            Assert.Equal(0, QueryPlanCache.Count);
        }
        finally
        {
            QueryPlanCache.Enabled = true;
        }
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void CacheCapacityAndEviction_ExceedingLimit_PrunesOldestEntries()
    {
        QueryPlanCache.Clear();
        int originalCapacity = QueryPlanCache.MaxCapacity;

        try
        {
            // Set max capacity to 5
            QueryPlanCache.MaxCapacity = 5;
            Assert.Equal(5, QueryPlanCache.MaxCapacity);

            using var df = DataFrame.FromRows(
            [
                new Customer { Id = 1, City = "Beijing", Balance = 100m },
            ]);

            // Insert 5 structurally different queries, each twice so that it is admitted.
            for (int i = 0; i < 2; i++)
            {
                _ = df.AsQueryable<Customer>().Where(c => c.Id == 1).ToList();
                _ = df.AsQueryable<Customer>().Where(c => c.Id > 1).ToList();
                _ = df.AsQueryable<Customer>().Where(c => c.Id < 1).ToList();
                _ = df.AsQueryable<Customer>().Where(c => c.Id >= 1).ToList();
                _ = df.AsQueryable<Customer>().Where(c => c.Id <= 1).ToList();
            }

            Assert.Equal(5, QueryPlanCache.Count);

            // Admitting a 6th unique query triggers prune
            _ = df.AsQueryable<Customer>().Where(c => c.Id != 1).ToList();
            _ = df.AsQueryable<Customer>().Where(c => c.Id != 1).ToList();

            // After prune of oldest 20%, cache should remain <= 5
            Assert.True(QueryPlanCache.Count <= 5);
        }
        finally
        {
            QueryPlanCache.MaxCapacity = originalCapacity;
            QueryPlanCache.Clear();
        }
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void PolarsConfig_SetQueryCacheSettings_AppliesSuccessfully()
    {
        int originalCapacity = QueryPlanCache.MaxCapacity;
        bool originalEnabled = QueryPlanCache.Enabled;

        try
        {
            using (Pl.Config.SetQueryCacheCapacity(256).BeginScope())
            {
                Assert.Equal(256, QueryPlanCache.MaxCapacity);
                Assert.Equal("256", Pl.Config["POLARS_QUERY_CACHE_CAPACITY"]);
            }
            // After scope disposal, it restores previous value
            Assert.Equal(originalCapacity, QueryPlanCache.MaxCapacity);

            _ = Pl.Config.SetQueryCacheCapacity(512);
            Assert.Equal(512, QueryPlanCache.MaxCapacity);

            _ = Pl.Config.SetQueryCacheEnabled(false);
            Assert.False(QueryPlanCache.Enabled);
            Assert.Equal("0", Pl.Config["POLARS_QUERY_CACHE_ENABLED"]);

            _ = Pl.Config.SetQueryCacheEnabled(true);
            Assert.True(QueryPlanCache.Enabled);
            Assert.Equal("1", Pl.Config["POLARS_QUERY_CACHE_ENABLED"]);
        }
        finally
        {
            QueryPlanCache.MaxCapacity = originalCapacity;
            QueryPlanCache.Enabled = originalEnabled;
        }
    }
}

