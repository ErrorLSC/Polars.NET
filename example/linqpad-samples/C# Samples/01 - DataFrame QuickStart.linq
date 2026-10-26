<Query Kind="Program">
  <NuGetReference>Polars.NET</NuGetReference>
  <NuGetReference>Polars.NET.Native.win-x64</NuGetReference>
  <Namespace>Polars.CSharp</Namespace>
  <Namespace>Polars.CSharp.Linq</Namespace>
  <Namespace>static Polars.CSharp.Polars</Namespace>
</Query>

// ============================================================================
// 01 - Getting Started with Polars.NET DataFrame
// ============================================================================
// Welcome to Polars.NET in LINQPad!
// Polars.NET is a lightning-fast DataFrame library powered by the Rust Polars engine.
//
// Tip: If running on Linux/macOS via LINQPad CLI (lpad),
// switch Polars.NET.Native.win-x64 to Polars.NET.Native.linux-x64 or osx-arm64.

// 1. Create a DataFrame from columnar Series
using var df = DataFrame.FromColumns([
    Series.From("Name", ["Alice", "Bob", "Charlie", "David", "Eva"]),
    Series.From("Department", ["Engineering", "HR", "Engineering", "Marketing", "Engineering"]),
    Series.From("Salary", [95000.0, 62000.0, 105000.0, 71000.0, 88000.0]),
    Series.From("Age", [29, 34, 42, 31, 26])
]);

// LINQPad's Util.RawHtml renders Polars's beautiful built-in styled HTML table
df.Dump("Original DataFrame");

// 2. High-performance filtering with Col expressions
using var filtered = df.Filter(Col("Salary") > 70000.0 & Col("Department") == "Engineering");
filtered.Dump("Engineers with Salary > 70k");

// 3. Adding computed columns
using var withBonus = df.WithColumns(
    (Col("Salary") * 0.1).Alias("Bonus")
);
withBonus.Dump("With Computed Bonus Column");

// 4. Quick schema & shape inspection
new {
    Shape = $"{df.Height} rows × {df.Shape.Width} cols",
    Columns = df.ColumnNames,
    Schema = df.Schema.ToString()
}.Dump("DataFrame Metadata");
