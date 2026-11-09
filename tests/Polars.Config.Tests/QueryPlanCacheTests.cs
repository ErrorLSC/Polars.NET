using System.Linq.Expressions;
using Microsoft.FSharp.Collections;
using Polars.CSharp.Linq;
using Polars.NET.Core.Config;
using Polars.NET.Core.Enums;
using Polars.NET.Linq;
using Kaminari.Samidare;
using Kaminari.Samidare.IR;

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
    public void Collection_Parameters_Key_By_Their_Contents()
    {
        QueryPlanCache.Clear();
        using var df = DataFrame.FromRows(Customers);
        var ids = new List<int> { 1, 3 };
        List<int> Run() => df.AsQueryable<Customer>().Where(c => ids.Contains(c.Id)).Select(c => c.Id).ToList();

        // Same contents: admitted on the second sighting, then hit.
        Assert.Equal([1, 3], Run());
        Assert.Equal([1, 3], Run());
        Assert.Equal(1, QueryPlanCache.Count);
        Assert.Equal([1, 3], Run());
        Assert.Equal(1, QueryPlanCache.Hits);

        // The list is mutable: new contents are a new key, never the plan compiled with the old ones.
        ids.Add(2);
        Assert.Equal([1, 2, 3], Run());
        Assert.Equal(1, QueryPlanCache.Hits);

        // An array or a collection expression keys the same way.
        int[] array = [2, 3];
        IReadOnlyList<int> readOnly = [2, 3];
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal([2, 3], df.AsQueryable<Customer>().Where(c => array.Contains(c.Id)).Select(c => c.Id).ToList());
            Assert.Equal([2, 3], df.AsQueryable<Customer>().Where(c => readOnly.Contains(c.Id)).Select(c => c.Id).ToList());
        }
        Assert.Equal(3, QueryPlanCache.Count);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void Collections_Larger_Than_The_Key_Limit_Are_Not_Cached()
    {
        QueryPlanCache.Clear();
        using var df = DataFrame.FromRows(Customers);
        var ids = Enumerable.Range(0, QueryPlanCache.MaxCollectionKeyLength + 1).ToList();

        for (int i = 0; i < 3; i++)
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
    public void Captured_Subqueries_That_Differ_Do_Not_Share_A_Plan()
    {
        QueryPlanCache.Clear();
        using var dfA = DataFrame.FromRows(Customers);
        using var dfO = DataFrame.FromRows(new[]
        {
            new Order { CustomerId = 1, Amount = 5m },
            new Order { CustomerId = 2, Amount = 50m },
            new Order { CustomerId = 3, Amount = 500m },
        });

        // The subquery is inlined as a constant (as F# captures it). Its element type and root table are the same
        // for every subquery here, so only its own expression tells the plans apart.
        List<int> CustomersIn(IQueryable<int> customerIds) =>
            InlineRoot(dfA.AsQueryable<Customer>().Where(c => customerIds.Contains(c.Id)), customerIds)
                .Select(c => c.Id)
                .ToList();

        var orders = dfO.AsQueryable<Order>();
        for (int i = 0; i < 3; i++)
            Assert.Equal([3], CustomersIn(orders.Where(o => o.Amount > 100m).Select(o => o.CustomerId)));
        Assert.Equal(2, QueryPlanCache.Count);

        Assert.Equal([1], CustomersIn(orders.Where(o => o.Amount < 10m).Select(o => o.CustomerId)));
        Assert.Equal([1, 2], CustomersIn(orders.OrderBy(o => o.Amount).Take(2).Select(o => o.CustomerId)));

        // The same subquery over another orders table still reuses the plan, bound to that table.
        using var dfO2 = DataFrame.FromRows(new[] { new Order { CustomerId = 2, Amount = 900m } });
        var hitsBefore = QueryPlanCache.Hits;
        Assert.Equal([2], CustomersIn(dfO2.AsQueryable<Order>().Where(o => o.Amount > 100m).Select(o => o.CustomerId)));
        Assert.True(QueryPlanCache.Hits > hitsBefore);
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void Closure_Captured_Subquery_Is_Cached_And_Bound_To_The_Captured_Table()
    {
        QueryPlanCache.Clear();
        using var dfA = DataFrame.FromRows(Customers);

        // The subquery is read from a closure, as C# captures a local query variable.
        List<int> CustomersWithBigOrders(Order[] orders, decimal minimum)
        {
            using var dfO = DataFrame.FromRows(orders);
            var ids = dfO.AsQueryable<Order>().Where(o => o.Amount > minimum).Select(o => o.CustomerId);
            return dfA.AsQueryable<Customer>().Where(c => ids.Contains(c.Id)).Select(c => c.Id).ToList();
        }

        Assert.Equal([1], CustomersWithBigOrders([new Order { CustomerId = 1, Amount = 50m }], 10m));
        Assert.Equal([2], CustomersWithBigOrders([new Order { CustomerId = 2, Amount = 50m }], 10m));
        Assert.Equal([3], CustomersWithBigOrders([new Order { CustomerId = 3, Amount = 50m }], 10m));
        // Two entries: the subquery (compiled when grafted) and the outer query. The subquery's own entry alone
        // would make Hits grow too, so the entry count is what shows the outer query is cached.
        Assert.Equal(2, QueryPlanCache.Count);

        // A value captured inside the subquery is part of the key.
        Assert.Empty(CustomersWithBigOrders([new Order { CustomerId = 3, Amount = 50m }], 100m));
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void Closure_Captured_Subqueries_That_Differ_Do_Not_Share_A_Plan()
    {
        QueryPlanCache.Clear();
        using var dfA = DataFrame.FromRows(Customers);
        using var dfO = DataFrame.FromRows(new[]
        {
            new Order { CustomerId = 1, Amount = 5m },
            new Order { CustomerId = 2, Amount = 50m },
            new Order { CustomerId = 3, Amount = 500m },
        });
        var orders = dfO.AsQueryable<Order>();

        // Typed as IEnumerable<int>, as a query variable often is.
        List<int> CustomersIn(IEnumerable<int> customerIds) =>
            dfA.AsQueryable<Customer>().Where(c => customerIds.Contains(c.Id)).Select(c => c.Id).ToList();

        for (int i = 0; i < 3; i++)
            Assert.Equal([3], CustomersIn(orders.Where(o => o.Amount > 100m).Select(o => o.CustomerId)));
        Assert.Equal(2, QueryPlanCache.Count);

        Assert.Equal([1], CustomersIn(orders.Where(o => o.Amount < 10m).Select(o => o.CustomerId)));
        Assert.Equal([1, 2], CustomersIn(orders.OrderBy(o => o.Amount).Take(2).Select(o => o.CustomerId)));
    }

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void Closure_Captured_Root_Inside_A_Client_Predicate_Is_Read_From_The_Query_Being_Run()
    {
        QueryPlanCache.Clear();
        using var dfA = DataFrame.FromRows(Customers);

        // As Root_Inside_A_Client_Predicate_Is_Read_From_The_Query_Being_Run, with the orders query read from the
        // closure: a cached delegate would keep reading the first (disposed) orders table.
        List<int> WithOrders(Order[] orders)
        {
            using var dfO = DataFrame.FromRows(orders);
            var ordersQuery = dfO.AsQueryable<Order>();
            return dfA.AsQueryable<Customer>()
                .Where(c => ordersQuery.Any(o => o.CustomerId == c.Id && TestUdfMethods.MaskName(c.City) != ""))
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
        // Collections key by their elements in order, and by their runtime type; non-scalar elements are not keyable.
        Assert.NotNull(KeyOf(new List<int> { 1, 2 }));
        Assert.Equal(KeyOf(new List<int> { 1, 2 }), KeyOf(new List<int> { 1, 2 }));
        Assert.NotEqual(KeyOf(new List<int> { 1, 2 }), KeyOf(new List<int> { 2, 1 }));
        Assert.NotEqual(KeyOf(new List<int> { 1, 2 }), KeyOf(new[] { 1, 2 }));
        Assert.NotEqual(KeyOf(new List<decimal> { 1.0m }), KeyOf(new List<decimal> { 1.00m }));
        Assert.Null(KeyOf(new List<object> { new object() }));
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

    public enum CachedShade { Red = 5, Green = 1, Blue = 3 }
    public record CachedShadeRow(int Id, CachedShade Shade);
    public record CachedIntShadeRow(int Id, int Shade);

    private static List<int> BlueIds(DataFrame df) =>
        df.AsQueryable<CachedShadeRow>().Where(r => r.Shade == CachedShade.Blue).Select(r => r.Id).ToList();

    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void Enum_And_Integer_Columns_Do_Not_Share_A_Cached_Plan()
    {
        // The plan compares categories on an Enum column and values on an integer column, so the column types
        // are part of the cache key.
        QueryPlanCache.Clear();
        using var enumFrame = DataFrame.FromRows([new CachedShadeRow(1, CachedShade.Blue), new CachedShadeRow(2, CachedShade.Red)]);
        using var intFrame = DataFrame.FromRows([new CachedIntShadeRow(1, 3), new CachedIntShadeRow(2, 5)]);

        for (int i = 0; i < 3; i++)
        {
            Assert.Equal([1], BlueIds(enumFrame));
            Assert.Equal([1], BlueIds(intFrame));
        }
        Assert.Equal(2, QueryPlanCache.Count);
    }

    private class CacheKv
    {
        public long K { get; set; }
        public long V { get; set; }
    }

    private static List<long> AboveSingle(DataFrame facts, DataFrame others)
    {
        var o = others.AsQueryable<CacheKv>();
        return facts.AsQueryable<CacheKv>().Where(x => x.V > o.Single(y => y.K == 7).V).Select(x => x.K).ToList();
    }

    private static List<long> InOtherKeys(DataFrame facts, DataFrame others)
    {
        var o = others.AsQueryable<CacheKv>();
        return facts.AsQueryable<CacheKv>().Where(x => o.Where(y => y.V > 0).Select(y => y.K).ToList().Contains(x.K))
            .Select(x => x.K).ToList();
    }

    // Single runs in the plan (a count assertion) unless UDF pushdown is disallowed: it is then evaluated
    // while the query compiles, and that plan must not be reused for other tables.
    [Theory]
    [Trait("LINQ", "QueryPlanCache")]
    [InlineData(false)]
    [InlineData(true)]
    public void Subquery_Evaluated_At_Compile_Time_Is_Not_Reused_For_Other_Tables(bool disallowUdfs)
    {
        using var _ = Pl.Config.SetDisallowUdfPushdown(disallowUdfs).BeginScope();
        QueryPlanCache.Clear();
        using var facts = DataFrame.FromRows(new CacheKv[] { new() { K = 1, V = 10 }, new() { K = 2, V = 50 }, new() { K = 3, V = 90 } });
        using var low = DataFrame.FromRows(new CacheKv[] { new() { K = 7, V = 20 }, new() { K = 1, V = 5 } });
        using var high = DataFrame.FromRows(new CacheKv[] { new() { K = 7, V = 80 }, new() { K = 3, V = 5 } });

        for (int i = 0; i < 3; i++)
        {
            Assert.Equal([2L, 3L], AboveSingle(facts, low));
            Assert.Equal([1L], InOtherKeys(facts, low));
        }

        Assert.Equal([3L], AboveSingle(facts, high));
        Assert.Equal([3L], InOtherKeys(facts, high));
    }

    private class Sale
    {
        public string Region { get; set; } = "";
        public string? Store { get; set; }
        public decimal Amount { get; set; }
    }

    /// A cached union or rollup plan lowered over other frames of the same schema: the schemas resolved for
    /// aligning the union's sides and typing the rollup's null keys are reused, the data is the hit's own.
    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void Union_And_Rollup_Hits_Read_The_Frames_Of_The_Query_Being_Run()
    {
        QueryPlanCache.Clear();
        Sale[][] datasets =
        [
            [new() { Region = "N", Store = "a", Amount = 1.5m }, new() { Region = "N", Store = "b", Amount = 2m }, new() { Region = "S", Store = null, Amount = 4m }],
            [new() { Region = "E", Store = "c", Amount = 10m }, new() { Region = "W", Store = "d", Amount = 20.25m }],
            [new() { Region = "N", Store = "a", Amount = 7m }],
        ];

        for (int round = 0; round < 3; round++)
        {
            foreach (var rows in datasets)
            {
                using var df = DataFrame.FromRows(rows);
                var q = df.AsQueryable<Sale>();

                // Union: the literal side is aligned to the column types of the first side.
                var union = q.Select(s => new { s.Region, s.Amount })
                             .Concat(q.Where(s => s.Store == null).Select(s => new { Region = "none", Amount = 0m }))
                             .OrderBy(x => x.Region).ThenBy(x => x.Amount)
                             .ToList();
                var expectedUnion = rows.Select(s => new { s.Region, s.Amount })
                                        .Concat(rows.Where(s => s.Store == null).Select(s => new { Region = "none", Amount = 0m }))
                                        .OrderBy(x => x.Region, StringComparer.Ordinal).ThenBy(x => x.Amount)
                                        .ToList();
                Assert.Equal(expectedUnion, union);

                // Rollup: the subtotal rows hold nulls of the keys' types.
                var rollup = q.GroupBy(s => new { s.Region, s.Store })
                              .RollUp((key, g) => new { key.Region, key.Store, Total = g.Sum(x => x.Amount) })
                              .OrderBy(r => r.Region).ThenBy(r => r.Store).ThenBy(r => r.Total)
                              .ToList();
                var expectedRollup = rows.GroupBy(s => new { s.Region, s.Store }).Select(g => new { Region = (string?)g.Key.Region, g.Key.Store, Total = g.Sum(x => x.Amount) })
                    .Concat(rows.GroupBy(s => s.Region).Select(g => new { Region = (string?)g.Key, Store = (string?)null, Total = g.Sum(x => x.Amount) }))
                    .Append(new { Region = (string?)null, Store = (string?)null, Total = rows.Sum(x => x.Amount) })
                    .OrderBy(r => r.Region, StringComparer.Ordinal).ThenBy(r => r.Store, StringComparer.Ordinal).ThenBy(r => r.Total)
                    .Select(r => (r.Region, r.Store, r.Total))
                    .ToList();
                Assert.Equal(expectedRollup, rollup.Select(r => ((string?)r.Region, r.Store, r.Total)).ToList());
            }
        }

        Assert.True(QueryPlanCache.Hits > 0);
    }

    private class SaleRow { public int ItemKey { get; set; } public int StoreKey { get; set; } public int Amount { get; set; } }
    private class ItemRow { public int Item { get; set; } public int ItemTag { get; set; } }
    private class StoreRow { public int Store { get; set; } public int StoreTag { get; set; } }

    /// AllowUnstableOrdering changes the plan (joins beneath a sort that can tie may be reordered), so it is part of
    /// the plan cache key; the rows are the same either way.
    [Fact]
    [Trait("LINQ", "QueryPlanCache")]
    public void A_Query_Used_Twice_Is_Shared_Whole_Even_When_Compiled_Alone_First()
    {
        var sales = Enumerable.Range(0, 200).Select(i => new SaleRow { ItemKey = i % 20, StoreKey = i % 10, Amount = i % 7 }).ToArray();
        var items = Enumerable.Range(0, 20).Select(i => new ItemRow { Item = i, ItemTag = 0 }).ToArray();
        using var salesDf = DataFrame.FromRows(sales);
        using var itemsDf = DataFrame.FromRows(items);

        var grouped = from s in salesDf.AsQueryable<SaleRow>()
                      join i in itemsDf.AsQueryable<ItemRow>() on s.ItemKey equals i.Item
                      group s by s.ItemKey into g
                      select new { g.Key, Total = g.Sum(x => x.Amount) };

        var query = from a in grouped.Where(x => x.Key > 2)
                    join b in grouped.Where(x => x.Key < 10) on a.Key equals b.Key
                    orderby a.Key
                    select a.Key * 1000 + b.Total;

        var groupedRows = (from s in sales
                           join i in items on s.ItemKey equals i.Item
                           group s by s.ItemKey into g
                           select new { g.Key, Total = g.Sum(x => x.Amount) }).ToList();
        var expected = (from a in groupedRows.Where(x => x.Key > 2)
                        join b in groupedRows.Where(x => x.Key < 10) on a.Key equals b.Key
                        orderby a.Key
                        select a.Key * 1000 + b.Total).ToList();

        QueryPlanCache.Clear();
        // Compiled alone first, so a grafted reference must not reuse this complete, optimized plan.
        _ = grouped.ToList();

        var lines = query.ExplainLogicalPlan().Split('\n');
        var shared = Enumerable.Range(0, lines.Length).Where(i => lines[i].Contains("Shared: id=")).ToList();
        Assert.Equal(2, shared.Count);
        // The grouping itself is shared, not only a prefix beneath it.
        Assert.All(shared, i => Assert.Contains("GroupBy", lines[i + 1]));
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Unstable_Ordering_Reorders_Joins_And_Keys_The_Cache()
    {
        var sales = Enumerable.Range(0, 200).Select(i => new SaleRow { ItemKey = i % 20, StoreKey = i % 10, Amount = i % 7 }).ToArray();
        var items = Enumerable.Range(0, 20).Select(i => new ItemRow { Item = i, ItemTag = 0 }).ToArray();
        var stores = Enumerable.Range(0, 10).Select(i => new StoreRow { Store = i, StoreTag = i % 5 }).ToArray();
        using var salesDf = DataFrame.FromRows(sales);
        using var itemsDf = DataFrame.FromRows(items);
        using var storesDf = DataFrame.FromRows(stores);

        IQueryable<int> Query() =>
            from s in salesDf.AsQueryable<SaleRow>()
            join i in itemsDf.AsQueryable<ItemRow>() on s.ItemKey equals i.Item
            join t in storesDf.AsQueryable<StoreRow>().Where(t => t.StoreTag == 1) on s.StoreKey equals t.Store
            orderby s.Amount
            select s.Amount;

        var expected = (from s in sales
                        join i in items on s.ItemKey equals i.Item
                        join t in stores.Where(t => t.StoreTag == 1) on s.StoreKey equals t.Store
                        orderby s.Amount
                        select s.Amount).ToList();

        var original = CoreConfig.AllowUnstableOrdering;
        try
        {
            QueryPlanCache.Clear();
            CoreConfig.AllowUnstableOrdering = false;
            string stable = "";
            for (int run = 0; run < 3; run++) stable = Query().ExplainLogicalPlan();
            Assert.Equal(expected, Query().ToList());

            CoreConfig.AllowUnstableOrdering = true;
            string unstable = Query().ExplainLogicalPlan();
            Assert.Equal(expected, Query().ToList());

            // The printed tree lists the last join first. Written order joins the items first (deepest); the filtered
            // stores go deepest once ties are unspecified.
            Assert.True(stable.IndexOf("col(\"StoreKey\")", StringComparison.Ordinal) < stable.IndexOf("col(\"ItemKey\")", StringComparison.Ordinal), stable);
            Assert.True(unstable.IndexOf("col(\"ItemKey\")", StringComparison.Ordinal) < unstable.IndexOf("col(\"StoreKey\")", StringComparison.Ordinal), unstable);
        }
        finally
        {
            CoreConfig.AllowUnstableOrdering = original;
        }
    }
}
