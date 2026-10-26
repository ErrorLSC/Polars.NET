<Query Kind="Statements">
  <NuGetReference>Polars.NET</NuGetReference>
  <NuGetReference>Polars.NET.Native.win-x64</NuGetReference>
  <Namespace>Polars.CSharp</Namespace>
  <Namespace>Polars.CSharp.Linq</Namespace>
  <Namespace>static Polars.CSharp.Polars</Namespace>
</Query>

// ============================================================================
// 04 - LazyFrame and Rust Query Optimizer
// ============================================================================
// LazyFrames delay execution until .Collect() is called. The Rust engine builds
// a Logical Plan and automatically applies optimizations such as:
// - Predicate Pushdown (filtering rows at source before loading all data)
// - Projection Pushdown (reading only required columns)
// - Common Subplan Elimination

using var rawDf = DataFrame.FromColumns([
    Series.From("Id", Enumerable.Range(1, 1000)),
    Series.From("Department", Enumerable.Range(1, 1000).Select(i => i % 3 == 0 ? "IT" : i % 3 == 1 ? "Sales" : "Finance")),
    Series.From("Score", Enumerable.Range(1, 1000).Select(i => (double)(i * 17 % 100))),
    Series.From("UnusedCol1", Enumerable.Range(1, 1000).Select(i => $"Data_{i}")),
    Series.From("UnusedCol2", Enumerable.Range(1, 1000).Select(i => i * 2))
]);

// 1. Build a Lazy Pipeline
using var lf = rawDf.Lazy()
    .Filter(Col("Score") > 60.0)
    .GroupBy("Department")
    .Agg(
        Col("Score").Mean().Alias("AvgScore"),
        Col("Score").Count().Alias("HighScorers")
    )
    .Sort("AvgScore", descending: true);

// 2. Compare Unoptimized vs Optimized Query Plan
lf.Explain(optimized: false).Dump("Unoptimized Logical Plan");
lf.Explain(optimized: true).Dump("Optimized Rust Execution Plan (Pushdowns Applied)");

// 3. Materialize results with .Collect()
using var result = lf.Collect();
result.Dump("Optimized Query Result");
