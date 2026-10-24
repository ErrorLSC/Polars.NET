<Query Kind="Statements">
  <NuGetReference>Polars.NET</NuGetReference>
  <NuGetReference>Polars.NET.Native.win-x64</NuGetReference>
  <Namespace>Polars.CSharp</Namespace>
  <Namespace>Polars.CSharp.Linq</Namespace>
  <Namespace>static Polars.CSharp.Polars</Namespace>
</Query>

// ============================================================================
// 03 - Column Expressions, String / Temporal Namespaces & Window Functions
// ============================================================================
// Polars expressions (Expr) are composable, lazily-evaluated AST trees executed
// by Rust using SIMD and multi-core parallelism.

using var sales = DataFrame.FromColumns([
    Series.From("Date", ["2026-01-15", "2026-01-20", "2026-02-05", "2026-02-18", "2026-03-01", "2026-03-12"]),
    Series.From("Product", ["Widget A", "widget b", "WIDGET A", "Gadget X", "Widget B", "Gadget Y"]),
    Series.From("Category", ["Hardware", "Hardware", "Hardware", "Electronics", "Hardware", "Electronics"]),
    Series.From("Revenue", [1200.0, 850.0, 1400.0, 3100.0, 920.0, 2800.0])
]);

// 1. Normalizing text with .Str namespace
using var cleaned = sales.WithColumns(
    Col("Product").Str.ToUpper().Alias("ProductClean"),
    Col("Date").Str.ToDate("%Y-%m-%d").Alias("ParsedDate")
);

Util.RawHtml(cleaned.ToHtml()).Dump("Cleaned Data with Normalized Strings & Dates");

// 2. GroupBy Aggregations with multiple metrics
using var categoryStats = cleaned.GroupBy("Category").Agg(
    Col("Revenue").Sum().Alias("TotalRevenue"),
    Col("Revenue").Mean().Alias("AvgRevenue"),
    Col("Revenue").Max().Alias("MaxRevenue"),
    Count().Alias("TransactionCount")
);

Util.RawHtml(categoryStats.ToHtml()).Dump("Category Summary Aggregations");

// 3. Window functions using .Over()
// Calculate each transaction's deviation from its category average without separate joins
using var windowed = cleaned.WithColumn(
    (Col("Revenue") - Col("Revenue").Mean().Over("Category")).Alias("CategoryDeviation")
);

Util.RawHtml(windowed.ToHtml()).Dump("Windowed Calculation (.Over(\"Category\"))");
