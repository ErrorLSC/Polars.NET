<Query Kind="Statements">
  <NuGetReference>Polars.NET</NuGetReference>
  <NuGetReference>Polars.NET.Native.win-x64</NuGetReference>
  <Namespace>Polars.CSharp</Namespace>
  <Namespace>Polars.CSharp.Linq</Namespace>
  <Namespace>static Polars.CSharp.Polars</Namespace>
</Query>

// ============================================================================
// 02 - C# LINQ Comprehension Syntax, Relational Joins & Aggregations
// ============================================================================
// Polars.NET provides a native LINQ provider that compiles standard C# LINQ queries
// directly into native Rust Polars LazyPlans without managed loop overhead!

record Customer(int Id, string Name, string Region);
record Order(int OrderId, int CustomerId, string Product, double Amount, int Quantity);

var customers = new List<Customer>
{
    new(1, "Alice", "North"),
    new(2, "Bob", "South"),
    new(3, "Charlie", "North"),
    new(4, "David", "East")
};

var orders = new List<Order>
{
    new(101, 1, "Laptop", 1200.0, 1),
    new(102, 1, "Mouse", 45.0, 2),
    new(103, 2, "Monitor", 350.0, 2),
    new(104, 3, "Keyboard", 120.0, 1),
    new(105, 3, "Desk Chair", 280.0, 1),
    new(106, 4, "Headphones", 150.0, 1),
    new(107, 1, "USB Cable", 25.0, 3)
};

// ----------------------------------------------------------------------------
// 1. Relational JOIN using C# LINQ Comprehension Syntax
// ----------------------------------------------------------------------------
// Compiles into a native Polars LazyJoin pushed down to Rust!
var joinQuery =
    from o in orders.AsPolarsQueryable()
    where o.Amount >= 50.0
    join c in customers.AsPolarsQueryable() on o.CustomerId equals c.Id
    orderby o.Amount descending
    select new
    {
        OrderId = o.OrderId,
        Customer = c.Name,
        Region = c.Region,
        Product = o.Product,
        Total = o.Amount * o.Quantity
    };

// Inspect the compiled Rust LazyPlan
joinQuery.Explain().Dump("Compiled Rust Join & Filter Execution Plan");

// Execute and display rich results in LINQPad
joinQuery.ToList().Dump("Relational Join Results (Amount >= $50)");

// ----------------------------------------------------------------------------
// 2. GroupBy & Multi-Metric Aggregations in LINQ
// ----------------------------------------------------------------------------
var groupQuery =
    from o in orders.AsPolarsQueryable()
    join c in customers.AsPolarsQueryable() on o.CustomerId equals c.Id
    group o by c.Region into g
    orderby g.Sum(x => x.Amount * x.Quantity) descending
    select new
    {
        Region = g.Key,
        TotalRevenue = g.Sum(x => x.Amount * x.Quantity),
        AverageOrderValue = g.Average(x => x.Amount),
        TotalItemsSold = g.Sum(x => x.Quantity),
        OrderCount = g.Count()
    };

groupQuery.Explain().Dump("Compiled Rust GroupBy Aggregation Plan");
groupQuery.ToList().Dump("Regional Sales Summary");

// ----------------------------------------------------------------------------
// 3. Querying directly over Polars DataFrames via .AsQueryable<T>()
// ----------------------------------------------------------------------------
using var dfOrders = DataFrame.From(orders);

var dfQuery =
    from o in dfOrders.AsQueryable<Order>()
    where o.Quantity > 1
    select new { o.OrderId, o.Product, o.Amount, o.Quantity };

dfQuery.ToList().Dump("Direct DataFrame Query (Bulk Orders)");
