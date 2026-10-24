# Getting Started with Polars.NET

Welcome to Polars.NET! This quickstart walks you through creating DataFrames, transforming data, computing window functions, and leveraging the Lazy engine with real-world execution plans.

---

## 0. Prerequisites & Native Packages

Unlike Python's wheel distribution, .NET projects require the managed package **plus the native runtime package** matching your OS and architecture:

::: code-group

```bash [C# (.NET CLI)]
# Core managed package
dotnet add package Polars.NET

# Required: Native runtime package matching your deployment environment
dotnet add package Polars.NET.runtime.linux-x64
# Or: Polars.NET.runtime.win-x64 / Polars.NET.runtime.osx-arm64
```

```bash [F# (.NET CLI)]
# Managed packages (includes functional extensions and pipeline helpers)
dotnet add package Polars.NET
dotnet add package Polars.FSharp

# Required: Native runtime package
dotnet add package Polars.NET.runtime.linux-x64
# Or: Polars.NET.runtime.win-x64 / Polars.NET.runtime.osx-arm64
```

:::

---

## 1. Creating a DataFrame

Polars.NET supports creating DataFrames from in-memory records, strongly-typed collections, CSV, Parquet, Arrow streams, and databases.

::: code-group

```csharp [C# (Fluent API)]
using Polars;
using Polars.CSharp;
using static Polars.CSharp.Polars;

// 1. Create from in-memory anonymous columns
var df = DataFrame.FromColumns(new 
{
    Date = new[] { "2023-01-01", "2023-01-01", "2023-01-02" },
    City = new[] { "London", "Manchester", "London" },
    Temperature = new[] { 10.5, 9.0, 12.1 },
    Rain = new[] { true, true, false }
});

// 2. Parse Date string to native Date32 type
df = df.WithColumn(Col("Date").Str.ToDate("%Y-%m-%d"));
```

```fsharp [F# (Pipeline API)]
open Polars.FSharp

type Weather = { Date: string; City: string; Temperature: float; Rain: bool }

let data = [
    { Date="2023-01-01"; City="London";     Temperature=10.5; Rain=true }
    { Date="2023-01-01"; City="Manchester"; Temperature=9.0;  Rain=true }
    { Date="2023-01-02"; City="London";     Temperature=12.1; Rain=false }
]

let df = 
    DataFrame.ofRecords data
    |> DataFrame.withColumn (pl.col("Date").Str.ToDate "%Y-%m-%d")
```

```csharp [C# 12 (Collection Expression)]
using Polars.CSharp;

Series sDate = Series.From("Date", ["2023-01-01", "2023-01-01", "2023-01-02"]);
Series sCity = Series.From("City", ["London", "Manchester", "London"]);
Series sTemp = Series.From("Temperature", [10.5, 9.0, 12.1]);
Series sRain = Series.From("Rain", [true, true, false]);

// Construct DataFrame using C# 12 [CollectionBuilder]
DataFrame df = [sDate, sCity, sTemp, sRain];
```

:::

<!--@include: ../snippets/quickstart_df.html-->

---

## 2. Filtering & Grouped Aggregations

Polars expressions (`Expr`) execute multi-threaded SIMD parallel calculations across columns.

::: code-group

```csharp [C# (Fluent API)]
var aggDf = df
    .Filter(Col("Temperature") > 9.5)
    .GroupBy("City")
    .Agg(
        Col("Temperature").Mean().Alias("Avg_Temperature"),
        Col("Temperature").Count().Alias("Record_Count")
    )
    .Sort("Avg_Temperature", descending: true);
```

```fsharp [F# (Pipeline API)]
let aggDf =
    df
    |> DataFrame.filter (pl.col "Temperature" .> pl.lit 9.5)
    |> DataFrame.groupBy [ pl.col "City" ]
    |> DataFrame.agg [
        pl.col("Temperature").Mean().Alias "Avg_Temperature"
        pl.col("Temperature").Count().Alias "Record_Count"
    ]
    |> DataFrame.sortDescending [ pl.col "Avg_Temperature" ]
```

```csharp [C# (LINQ)]
// Strongly-typed LINQ to Polars expression
var aggDf = (from r in df.AsQueryable()
             where r.Temperature > 9.5
             group r by r.City into g
             select new {
                 City = g.Key,
                 Avg_Temperature = g.Average(x => x.Temperature),
                 Record_Count = g.Count()
             })
             .ToDataFrame()
             .Sort("Avg_Temperature", descending: true);
```

:::

<!--@include: ../snippets/quickstart_filter_agg.html-->

---

## 3. Window Functions & Expr AST Visualization

Window functions (`.Over()`) evaluate group-level metrics without reducing rows. Polars.NET provides full introspection into expression syntax trees via `.ToHtml()`.

::: code-group

```csharp [C# (Fluent API)]
// Calculate temperature deviation from the city average
var diffExpr = (Col("Temperature") - Col("Temperature").Mean().Over("City"))
    .Alias("Temp_Diff");

var windowDf = df.WithColumn(diffExpr);
```

```fsharp [F# (Pipeline API)]
let diffExpr = 
    (pl.col("Temperature") - pl.col("Temperature").Mean().Over(pl.col "City"))
        .Alias("Temp_Diff")

let windowDf = df |> DataFrame.withColumn diffExpr
```

```csharp [C# (LINQ)]
// Over() extension operator in LINQ
var windowDf = df.AsQueryable()
    .Select(r => new {
        r.City,
        r.Temperature,
        Temp_Diff = r.Temperature - r.Temperature.Over(r.City).Average()
    })
    .ToDataFrame();
```

:::

<!--@include: ../snippets/quickstart_expr_ast.html-->

---

## 4. The Lazy Engine & Execution Planning

`LazyFrame` builds a declarative query graph without reading raw data into memory. When calling `.Collect()`, Polars optimizes the query graph via **predicate pushdown**, **projection pruning**, and **streaming chunking**.

::: code-group

```csharp [C# (Fluent API)]
var lf = df.Lazy()
    .Filter(Col("Temperature") > 10.0)
    .GroupBy("City")
    .Agg(Col("Temperature").Mean().Alias("Avg_Temp"));

// Inspect the optimized query plan
Console.WriteLine(lf.Explain(optimized: true));

// Execute in parallel
var result = lf.Collect();
```

```fsharp [F# (Pipeline API)]
let lf = 
    df.Lazy()
    |> LazyFrame.filter (pl.col "Temperature" .> pl.lit 10.0)
    |> LazyFrame.groupBy [ pl.col "City" ]
    |> LazyFrame.agg [
        pl.col("Temperature").Mean().Alias "Avg_Temp"
    ]

printfn "%s" (lf.Explain(optimized = true))

let result = lf.Collect()
```

:::

<!--@include: ../snippets/quickstart_lazy_plan.html-->

---

## Next Steps & Guides

* [C# Idioms & Best Practices](/articles/csharp-idioms) - Fluent chaining, Selectors, Schema dictionary, and memory safety.
* [F# Idioms & Best Practices](/articles/fsharp-idioms) - Functional pipelines, anonymous records, and typed streaming.
* [LINQ & Query Computation Expressions](/articles/linq) - 3-tier fallback strategy, LINQ to Polars, and F# `query { ... }`.
* [Ecosystem & Native Interoperability](/articles/interop) - Zero-copy Arrow C Data Interface, ADBC, ADO.NET, ML.NET, and Tensors.
- API Reference: Explore the full list of available Expressions.