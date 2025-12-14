# Polars.NET

🚀 **Blazingly fast DataFrames for .NET (F# & C#)**, powered by Rust Polars.

Polars.NET brings the power of the [Polars](https://pola.rs/) library to the .NET ecosystem using high-performance Zero-Copy FFI (Arrow Interface).

## Features

* **⚡ High Performance**: Written in Rust, exposed via C API, wrapped in .NET SafeHandles.
* **🧠 Lazy Evaluation**: Build query plans and execute them efficiently with query optimization.
* **🛡️ Type Safe**: Idiomatic F# API with strong typing for joins, aggregations, and expressions.
* **🔗 Interop**: Seamless conversion between Polars DataFrame, Apache Arrow RecordBatch, and F# Records.
* **📊 Time Series**: First-class support for `join_asof`, rolling windows, and temporal operations.
* **🧩 SQL Support**: Run SQL queries directly on your DataFrames.

## Quick Start (F#)

```fsharp
open PolarsFSharp

// 1. Scan a CSV file (Lazy)
let lf = Polars.scanCsv "data.csv" None

// 2. Build a query
let df = 
    lf
    |> Polars.filterLazy (Polars.col "age" .> Polars.lit 18)
    |> Polars.groupByLazy 
        [ Polars.col "department" ]
        [ 
            Polars.col("salary").Mean().Alias("avg_salary")
            Polars.count().Alias("count")
        ]
    |> Polars.sortLazy (Polars.col "avg_salary") true
    |> Polars.collect

// 3. Show results
df |> Polars.show
```
## Advanced Examples

Time Series Join (As-Of Join)
Match trades with the most recent quote before the trade time.

```F#

// lfTrades: time, price
// lfQuotes: time, bid, ask

let res = 
    lfTrades
    |> Polars.joinAsOf lfQuotes 
        (Polars.col "time") (Polars.col "time") // Join keys
        [] [] // Group by (optional)
        (Some "backward") // Strategy
        (Some "2m")       // Tolerance: 2 minutes
    |> Polars.collect
```
## UDF (User Defined Functions)
Run custom C# logic on columns with Zero-Copy overhead.

```F#

open PolarsFSharp.Udf

// Define a simple function
let addOne (x: int) = x + 1

// Apply it to a column
lf 
|> Polars.withColumn (
    Polars.col "value"
    |> fun e -> e.Map(mapInt32 addOne) // Auto-vectorized via Arrow
)
```
## Architecture
Polars.NET.Core: C# P/Invoke bindings (LibraryImport) handling memory safety (GC & Rust Drop).

PolarsFSharp: F# functional wrapper providing DSL and type safety.

Rust Core: A thin shim exposing Polars functionality via C ABI.