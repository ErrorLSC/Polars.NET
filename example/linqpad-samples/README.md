# LINQPad Samples for Polars.NET

This folder contains interactive `.linq` sample scripts for **LINQPad 7 & 8**.

## LINQPad Free Edition Compatibility

In the **Free Edition** of LINQPad, arbitrary NuGet search and restore is normally restricted. However, LINQPad provides a first-class feature for library authors:
1. When a NuGet package contains sample scripts in a root folder named `linqpad-samples`,
2. And the package includes the `linqpad-samples` tag in its NuGet metadata (`<PackageTags>`),

Users running the **Free Edition** can navigate to the **Samples** tab in LINQPad, search for `Polars.NET`, and run all interactive samples for free without needing a paid license. LINQPad automatically restores and loads the package and its dependencies.

## Included Samples

1. **`01 - Getting Started with DataFrame.linq`**: Basic DataFrame creation, filtering with `Col(...)`, computed columns, and rich HTML table rendering (`Util.RawHtml(df.ToHtml()).Dump()`).
2. **`02 - Native LINQ Queries.linq`**: Native C# LINQ provider querying in-memory collections and DataFrames, inspecting the Rust `LazyPlan` with `.Explain()`, and materializing results.
3. **`03 - Expressions and Aggregations.linq`**: String and temporal namespaces (`.Str`, `.Dt`), multi-metric `GroupBy.Agg()`, and window functions with `.Over()`.
4. **`04 - LazyFrame and Optimization.linq`**: Building lazy pipelines, comparing unoptimized vs optimized plans with predicate/projection pushdown, and materializing with `.Collect()`.
