<Query Kind="Statements">
  <NuGetReference>Polars.NET</NuGetReference>
  <NuGetReference>Polars.NET.Native.win-x64</NuGetReference>
  <Namespace>Polars.CSharp</Namespace>
  <Namespace>Polars.CSharp.Linq</Namespace>
  <Namespace>static Polars.CSharp.Polars</Namespace>
</Query>

// ============================================================================
// 02 - Native C# LINQ Queries over Polars.NET
// ============================================================================
// Polars.NET provides a native LINQ provider that compiles standard C# LINQ
// directly into optimized native Rust Polars LazyPlans without managed loop overhead!

record OrderRecord(int Id, string Customer, string Region, double Amount, int Quantity);

// In-memory collections can be seamlessly converted to Polars Queryable via .AsPolarsQueryable()
var orders = new List<OrderRecord>
{
    new(1, "Alice", "North", 150.0, 2),
    new(2, "Bob", "South", 80.0, 1),
    new(3, "Charlie", "North", 300.0, 5),
    new(4, "David", "East", 210.0, 3),
    new(5, "Emma", "North", 95.0, 1),
    new(6, "Frank", "South", 450.0, 4),
    new(7, "Grace", "East", 120.0, 2),
    new(8, "Henry", "West", 260.0, 3)
};

// 1. Fluent LINQ Query compiled to Rust engine
var query = orders.AsPolarsQueryable()
    .Where(o => o.Amount > 100.0)
    .GroupBy(o => o.Region)
    .Select(g => new
    {
        Region = g.Key,
        TotalSales = g.Sum(x => x.Amount),
        AverageSales = g.Average(x => x.Amount),
        OrderCount = g.Count()
    })
    .OrderByDescending(x => x.TotalSales);

// 2. Inspect the native Polars execution plan
query.Explain().Dump("Compiled Rust LazyPlan");

// 3. Execute the query
query.ToList().Dump("LINQ Aggregation Results");

// 4. Query directly against a Polars DataFrame
using var df = DataFrame.From(orders);
var topCustomers = df.AsQueryable<OrderRecord>()
    .Where(o => o.Amount >= 200.0)
    .OrderByDescending(o => o.Amount)
    .Select(o => new { o.Customer, o.Region, o.Amount })
    .ToList();

topCustomers.Dump("High-Value Orders from DataFrame");
