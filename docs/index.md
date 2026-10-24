---
layout: home
title: Polars.NET - Blazingly Fast DataFrames for .NET
hero:
  name: Polars.NET
  text: The High-Performance DataFrame Library
  tagline: Rust Core. Arrow Memory. Native Speed.
  actions:
    - theme: brand
      text: Get Started
      link: /articles/quickstart
    - theme: alt
      text: View on GitHub
      link: https://github.com/errorlsc/polars.net
features:
  - title: Blazing Fast & Memory Efficient
    details: Powered by the Polars Rust core and Apache Arrow columnar format with multi-threaded SIMD, query optimization, and streaming execution.
  - title: Idiomatic C# & F#
    details: Fluent API, LINQ query provider, C# 12 collection expressions, alongside native F# pipeline combinators and query computation expressions.
  - title: Deep Ecosystem Interoperability
    details: Seamless bridges to Arrow C Data Interface, ADBC, ADO.NET DbDataReader, ML.NET IDataView, TorchSharp / System.Numerics Tensor, and IAsyncEnumerable.
---

# Polars.NET

**Polars.NET** brings the [Polars](https://pola.rs/) data processing engine to the .NET ecosystem.

---

## 1. QuickStart

```csharp
using Polars.CSharp;
using static Polars.CSharp.Polars; // For Col(), Lit() helpers

// 1. Create a DataFrame
var data = new[] {
    new { Name = "Alice", Age = 25, Dept = "IT" },
    new { Name = "Bob", Age = 30, Dept = "HR" },
    new { Name = "Charlie", Age = 35, Dept = "IT" }
};
var df = DataFrame.From(data);

// 2. Filter & Aggregate
var res = df.
    Lazy()
    .Filter(Col("Age") > 28)
    .GroupBy("Dept")
    .Agg(
        Col("Age").Mean().Alias("AvgAge"),
        Col("Name").Count().Alias("Count")
    )
    .Sort("AvgAge");

// 3. Output
res.CollectStreaming().Show();
///shape: (2, 3)
/// ┌──────┬────────┬───────┐
/// │ Dept ┆ AvgAge ┆ Count │
/// │ ---  ┆ ---    ┆ ---   │
/// │ str  ┆ f64    ┆ u32   │
/// ╞══════╪════════╪═══════╡
/// │ HR   ┆ 30.0   ┆ 1     │
/// │ IT   ┆ 35.0   ┆ 1     │
/// └──────┴────────┴───────┘
```
## 2.F# Support 

```fsharp
let windowDf = 
    df
    |> pl.select [
        pl.col "Date"
        pl.col "City"
        pl.col "Temperature"
        
        // Over(Date): Calculate mean value by group
        (pl.col("Temperature").Mean().Over(pl.col "Date"))
            .Alias "Daily_Avg"
            
        (pl.col "Temperature" - pl.col("Temperature").Mean().Over(pl.col "Date"))
            .Alias "Diff"
    ]
    |> pl.sort (pl.col "Date", false)

windowDf.Show()
/// result
/// shape: (3, 5)
/// ┌────────────┬────────────┬─────────────┬───────────┬───────┐
/// │ Date       ┆ City       ┆ Temperature ┆ Daily_Avg ┆ Diff  │
/// │ ---        ┆ ---        ┆ ---         ┆ ---       ┆ ---   │
/// │ date       ┆ str        ┆ f64         ┆ f64       ┆ f64   │
/// ╞════════════╪════════════╪═════════════╪═══════════╪═══════╡
/// │ 2023-01-01 ┆ London     ┆ 10.5        ┆ 9.75      ┆ 0.75  │
/// │ 2023-01-01 ┆ Manchester ┆ 9.0         ┆ 9.75      ┆ -0.75 │
/// │ 2023-01-02 ┆ London     ┆ 12.1        ┆ 12.1      ┆ 0.0   │
/// └────────────┴────────────┴─────────────┴───────────┴───────┘
```
