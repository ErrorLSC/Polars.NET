# LINQPad Samples for Polars.NET & Polars.FSharp

This folder contains interactive `.linq` sample scripts for **LINQPad**.

## Structure & Included Samples

### C# Samples (`C# Samples/`)
1. **`01 - DataFrame QuickStart.linq`**: Basic DataFrame creation from series, filtering with `Col(...)`, computed columns, metadata inspection, and rich HTML table rendering (`Util.RawHtml(df.ToHtml()).Dump()`).
2. **`02 - LINQ Comprehension & Joins.linq`**: Classical C# LINQ comprehension syntax (`from ... in ... join ... on ... where ... group ... by ... select ...`), relational JOINs, multi-metric aggregations, and inspecting compiled Rust LazyPlans via `.Explain()`.
3. **`03 - Expressions and Aggregations.linq`**: String and temporal namespaces (`.Str`, `.Dt`), multi-metric `GroupBy.Agg()`, and window functions with `.Over()`.
4. **`04 - LazyFrame and Optimization.linq`**: Building lazy pipelines, comparing unoptimized vs optimized plans with predicate/projection pushdown, and materializing with `.Collect()`.

### F# Samples (`F# Samples/`)
1. **`01 - DataFrame & Functional Pipelines.linq`**: F# pipeline combinators (`|>`), `DataFrame.filter`, `DataFrame.withColumn`, `DataFrame.sort`, `DataFrame.groupBy`, and HTML table rendering.
2. **`02 - Query Computation Expressions.linq`**: Native F# `query { ... }` computation expressions over Polars, relational `join`, `where`, `groupBy`, sorting, and inspecting compiled Rust LazyPlans.
