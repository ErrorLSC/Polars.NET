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
    - theme: alt
      text: Polars (Upstream)
      link: https://pola.rs/
features:
  - title: Blazing Fast & Memory Efficient
    details: Powered by the Polars Rust core and Apache Arrow columnar format with multi-threaded SIMD, query optimization, and streaming execution.
  - title: Idiomatic C# & F#
    details: Fluent API, LINQ query provider, C# 12 collection expressions, alongside native F# pipeline combinators and query computation expressions.
  - title: Deep Ecosystem Interoperability
    details: Seamless bridges to Arrow C Data Interface, ADBC, ADO.NET DbDataReader, ML.NET IDataView, TorchSharp / System.Numerics Tensor, and IAsyncEnumerable.
---

## Unified Across Paradigms

Write queries in the style most natural to your project. Polars.NET executes C# Fluent calls, F# functional pipelines, C# LINQ syntax, and F# computation expressions with **identical, zero-copy native performance**:

::: code-group

```csharp [C# (Fluent API)]
using Polars.CSharp;
using static Polars.CSharp.Polars;

// Ingest from strongly-typed records or anonymous objects
var df = DataFrame.FromColumns(new
{
    Date = new[] { "2023-01-01", "2023-01-01", "2023-01-02" },
    City = new[] { "London", "Manchester", "London" },
    Temperature = new[] { 10.5, 9.0, 12.1 },
    Rain = new[] { true, true, false }
}).WithColumn(Col("Date").Str.ToDate("%Y-%m-%d"));

// Filter, GroupBy & Aggregate with multi-threaded SIMD execution
var result = df.Lazy()
    .Filter(Col("Temperature") > 9.5)
    .GroupBy("City")
    .Agg(
        Col("Temperature").Mean().Alias("Avg_Temperature"),
        Col("Temperature").Count().Alias("Record_Count")
    )
    .Sort("Avg_Temperature", descending: true)
    .Collect();
```

```fsharp [F# (Pipeline API)]
open Polars.FSharp

type Weather = { Date: string; City: string; Temperature: float; Rain: bool }

let data = [
    { Date="2023-01-01"; City="London";     Temperature=10.5; Rain=true }
    { Date="2023-01-01"; City="Manchester"; Temperature=9.0;  Rain=true }
    { Date="2023-01-02"; City="London";     Temperature=12.1; Rain=false }
]

// Idiomatic functional pipelines with pl.* combinators
let result =
    DataFrame.ofRecords data
    |> DataFrame.withColumn (pl.col("Date").Str.ToDate "%Y-%m-%d")
    |> DataFrame.filter (pl.col "Temperature" .> pl.lit 9.5)
    |> DataFrame.groupBy [ pl.col "City" ]
    |> DataFrame.agg [
        pl.col("Temperature").Mean().Alias "Avg_Temperature"
        pl.col("Temperature").Count().Alias "Record_Count"
    ]
    |> DataFrame.sortDescending [ pl.col "Avg_Temperature" ]
```

```csharp [C# (LINQ)]
using Polars.CSharp;
using Polars.CSharp.Linq;

// Standard C# LINQ with 100% Native Pushdown to the Rust engine
var result = (from r in df.AsQueryable<WeatherRecord>()
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

```fsharp [F# (Query CE)]
open Polars.FSharp
open Polars.FSharp.Query

// Native F# query { } computation expression
let result =
    query {
        for r in df.AsQueryable<Weather>() do
        where (r.Temperature > 9.5)
        groupValBy r r.City into g
        select {|
            City = g.Key
            Avg_Temperature = g.Average(fun x -> x.Temperature)
            Record_Count = g.Count()
        |}
    }
    |> Query.toDataFrame
    |> DataFrame.sortDescending [ pl.col "Avg_Temperature" ]
```

:::

### Output

<!--@include: ./snippets/quickstart_filter_agg.html-->

---

## Why Polars.NET?

| Feature | Standard .NET DataFrames | Polars.NET |
| :--- | :--- | :--- |
| **Execution Core** | Interpreted managed loops | Multi-threaded vectorized Rust SIMD |
| **Memory Model** | Managed object allocations (GC pressure) | Arrow columnar layout with zero-copy FFI |
| **Query Optimization**| None (client-side execution) | Logical plan optimization, Predicate & Projection pushdown |
| **Out-of-Core Processing** | Out of Memory (OOM) on large files | Streaming engine & Chunked memory |
| **Language Paradigms** | Basic C# collection methods | C# Fluent API, F# Pipelines, C# LINQ, and F# Query CE |
| **Lakehouse & Big Data** | Requires Spark / JVM wrappers | Native Delta Lake ACID storage, ADBC, & Arrow IPC |

Ready to build blazingly fast data pipelines? Read the [Quick Start Guide](/articles/quickstart) or explore the [Core Concepts](/articles/expressions).

---

## Built on Polars

Polars.NET brings the blazing speed, Arrow memory layout, and query optimization of the world-class [Polars](https://pola.rs/) DataFrame engine to the .NET ecosystem. We express our deepest gratitude to Ritchie Vink and the entire Polars open-source community for their pioneering work on high-performance columnar data processing.

* **Official Website**: [pola.rs](https://pola.rs/)
* **Upstream User Guide**: [docs.pola.rs](https://docs.pola.rs/)
* **Python API Reference**: [Polars Python API Reference](https://docs.pola.rs/api/python/stable/reference/index.html)
* **AI & LLM Context**: [llms.txt](/llms.txt) (Index) &middot; [llms-full.txt](/llms-full.txt) (Full Context)
