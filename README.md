<div align="center">
  <img src="assets/logo.svg" width="160" height="160" alt="Polars.NET Logo" />
  <h1>Polars.NET</h1>
  <p><b>Blazingly Fast DataFrames & LINQ Engine for .NET</b></p>
  <p><i>Powered by Rust Polars & Apache Arrow</i></p>

  <p>
    <a href="https://www.nuget.org/packages/Polars.NET"><img src="https://img.shields.io/nuget/v/Polars.NET.svg" alt="NuGet Polars.NET" /></a>
    <a href="https://www.nuget.org/packages/Polars.NET"><img src="https://img.shields.io/nuget/dt/Polars.NET.svg" alt="NuGet Downloads" /></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square" alt="License" /></a>
    <a href="https://errorlsc.github.io/Polars.NET/"><img src="https://img.shields.io/badge/docs-online-512BD4.svg?style=flat-square&logo=read-the-docs&logoColor=white" alt="Documentation" /></a>
  </p>

  <p>
    <a href="https://errorlsc.github.io/Polars.NET/"><b>Documentation Website</b></a> |
    <a href="https://errorlsc.github.io/Polars.NET/articles/quickstart"><b>Quick Start</b></a> |
    <a href="https://errorlsc.github.io/Polars.NET/articles/linq"><b>LINQ Guide</b></a> |
    <a href="https://errorlsc.github.io/Polars.NET/api/csharp_generated/DataFrame"><b>C# API</b></a> |
    <a href="https://errorlsc.github.io/Polars.NET/api/fsharp_generated/pl"><b>F# API</b></a> |
    <a href="https://errorlsc.github.io/Polars.NET/llms.txt"><b>llms.txt</b></a> |
    <a href="https://github.com/ErrorLSC/Polars.NET-Cookbook"><b>Cookbook</b></a>
  </p>
</div>

---

I really dreamed that we can play with DataFrames in .NET just like python, now we got a much more enjoyable and funny one.

## Highlights

* **Native Pushdown LINQ**: Compiles C# LINQ and F# query { } directly into Rust Polars physical plans then executed natively. Verified with all 22 TPC-H benchmark queries implemented in native LINQ without client-side fallback. Includes .Explain(), .ToDataFrame(), and joins between in-memory collections and DataFrames..
* **High Performance**: Built on the Rust Polars engine and Apache Arrow columnar memory format. Delivers vectorized SIMD execution, query optimization (predicate and projection pushdown, common subplan elimination), and out-of-core streaming execution for larger-than-memory datasets.
* **Deep .NET Ecosystem Integration**: First-class bridges to standard .NET abstractions, including `IAsyncEnumerable<T>` streaming, ADO.NET `IDataReader` / `DbDataReader` ingestion, ML.NET `IDataView` pipelines, and direct extraction into `System.Numerics.Tensors` (`ReadOnlyTensorSpan`) for numerical computing and model inference.
* **Universal I/O & Storage**: Reads and writes Parquet, CSV, JSON/NDJSON, Arrow IPC, Excel, and Avro. Native integration with Arrow Database Connectivity (ADBC) and Delta Lake (ACID transactions, time travel, and SQL MERGE), targeting both local filesystems and cloud object storage (Amazon S3, Azure Blob, Google Cloud Storage).

---

## Installation

Polars.NET targets **.NET 8.0** and requires referencing both the managed API package and the matching native runtime package for your execution platform:

### C# Users

```bash
# Core API package
dotnet add package Polars.NET

# Platform native runtime (choose matching your deployment target):
dotnet add package Polars.NET.Native.win-x64
# dotnet add package Polars.NET.Native.linux-x64
# dotnet add package Polars.NET.Native.linux-arm64
# dotnet add package Polars.NET.Native.osx-arm64

# Optional: ML.NET integration
dotnet add package Polars.NET.ML
```

### F# Users

```bash
# F# idiomatic API package
dotnet add package Polars.FSharp

# Platform native runtime:
dotnet add package Polars.NET.Native.win-x64
```

---

## Quick Tour

### 1. C# LINQ Query (`IQueryable<T>`)

Write standard C# LINQ queries that compile directly into Rust SIMD kernels without hydrating managed objects:

```csharp
using Polars.CSharp;
using Polars.CSharp.Linq;

public record Order(int Id, string Customer, string Region, double Revenue);

List<Order> orders = GetOrders();

// 1. Direct entry from in-memory collection, DataFrame, or ADO.NET IDataReader:
var query = from o in orders.AsPolarsQueryable()
            where o.Revenue >= 500.0 && o.Region != "Test"
            group o by o.Region into g
            select new {
                Region = g.Key,
                TotalRevenue = g.Sum(x => x.Revenue),
                OrderCount = g.Count()
            };

// 2. Materialize directly into a Polars DataFrame:
using DataFrame resultDf = query.ToDataFrame();
resultDf.Show();

// 3. Or inspect the execution plan:
query.InspectPlan();
```

### 2. C# Vectorized Expressions (`LazyFrame` & `Pl.Col`)

Harness the full power of the Polars DSL for complex analytics, column expressions, and Selectors:

```csharp
using Polars.CSharp;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

// Read high-throughput Parquet lazily:
using var lf = LazyFrame.ScanParquet("orders/*.parquet");

using var summaryDf = lf
    .Filter(Pl.Col("order_date") >= new DateOnly(2025, 1, 1))
    .GroupBy("region")
    .Agg(
        Pl.Col("revenue").Sum().Alias("total_revenue"),
        Pl.Col("customer_id").NUnique().Alias("unique_customers"),
        Cs.Numeric().Mean().Name.Prefix("avg_")
    )
    .Sort("total_revenue", descending: true)
    .Collect();

summaryDf.Show();
```

### 3. F# Query Computation Expression

Leverage F# strong typing, anonymous records, and pipeline combinators over native queries:

```fsharp
open Polars.FSharp
open Polars.FSharp.Query

// F# query { } compiles into polars API call:
let summaryQuery =
    query {
        for o in df.AsQueryable<Order>() do
        where (o.Revenue >= 500.0 && o.Region <> "Test")
        groupValBy o o.Region into g
        select {|
            Region = g.Key
            TotalRevenue = g.Sum(fun x -> x.Revenue)
            OrderCount = g.Count()
        |}
    }

use resultDf = summaryQuery.ToDataFrame()
resultDf |> DataFrame.show
```

### 4. Polars.NET Exclusive: Strongly-Typed SQL `Merge`

Perform ANSI SQL `MERGE` upsert operations directly on DataFrames or in-memory collections:

```csharp
List<ProductItem> inMemoryTarget = GetTargetProducts();
List<ProductItem> incomingUpdates = GetSourceProducts();

// Start merge from a List<T>:
using DataFrame mergedDf = inMemoryTarget.Merge(incomingUpdates, t => t.Id, s => s.Id)
    .WhenMatched(
        (t, s) => s.Price > t.Price,
        then => then.Update(set => set.Set(t => t.Price, (t, s) => s.Price))
    )
    .WhenNotMatchedByTarget(then => then.Insert())
    .WhenNotMatchedBySource(t => t.Category == "Seasonal", then => then.Delete())
    .ToDataFrame(); // Or materialize directly to List<ProductItem> via .ToList()
```

---

## Documentation & Guides

Comprehensive documentation, architecture references, and API catalogs are available on our documentation portal:

| Topic | Documentation Guide | Description |
| :--- | :--- | :--- |
| **Quick Start** | [Quick Start Guide](https://errorlsc.github.io/Polars.NET/articles/quickstart) | 5-minute setup, core operations, and data ingestion |
| **LINQ Provider** | [LINQ & Query CE](https://errorlsc.github.io/Polars.NET/articles/linq) | 3-tier compiler, BCL mappings, and pushdown architecture |
| **Expressions** | [Expressions & Column Ops](https://errorlsc.github.io/Polars.NET/articles/expressions) | Vectorized computations, string, date/time, and math functions |
| **Lazy Execution** | [LazyFrame & Streaming](https://errorlsc.github.io/Polars.NET/articles/lazy) | Predicate pushdown, projection pruning, and streaming engine |
| **Lakehouse** | [Delta Lake & ACID Storage](https://errorlsc.github.io/Polars.NET/articles/delta) | Parquet scanning, Delta ACID transactions, and time-travel |
| **Time Series** | [Time Series & Analytics](https://errorlsc.github.io/Polars.NET/articles/timeseries) | Dynamic groupby, rolling windows, and interpolation |
| **Ecosystem** | [Ecosystem & Native Interop](https://errorlsc.github.io/Polars.NET/articles/interop) | Apache Arrow, ADO.NET, ML.NET, Tensor, and Spans |
| **C# Idioms** | [Idiomatic C# Practices](https://errorlsc.github.io/Polars.NET/articles/csharp-idioms) | SafeHandles, collection expressions, and fast-path row cursors |
| **F# Idioms** | [Idiomatic F# Practices](https://errorlsc.github.io/Polars.NET/articles/fsharp-idioms) | Zero-allocation struct enumerators, modules, and pipeline operators |
| **AI / LLMs** | [llms.txt](https://errorlsc.github.io/Polars.NET/llms.txt) | Curated context and anti-hallucination guardrails for LLM agents |

---

## Project Architecture

Polars.NET uses a modular three-tier architecture to prevent upstream breaking changes:

```

+-------------------------------------------------------------------+
|               Polars.NET User API Layer                           |
|   - Polars.CSharp                                                 |
|   - Polars.FSharp                                                 |
+-------------------------------------------------------------------+
                                  |
                                  v
+-------------------------------------------------------------------+
|              Polars.NET.Core & Polars.NET.Linq                    |
|   - Common Infrastructure, LINQ Provider written in F#            |
|   - Apache Arrow C Data Interface                                 |
|   - P/Invoke bindings & NativeShim library loaders                |
+-------------------------------------------------------------------+
                                  |
                                  v
+-------------------------------------------------------------------+
|                 Rust polars & delta-rs Engine                     |
|   - Execution engine                                              |
|   - Physical query planner, optimizer            |
|   - delta-rs integration                                         |
+-------------------------------------------------------------------+
```

---

## Benchmarks

Polars.NET brings the blazing performance of Rust Polars directly to the .NET ecosystem:

<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/benchmark_summary_dark.png">
    <img alt="Speedup Summary vs Legacy .NET" src="assets/benchmark_summary_light.png" width="750" />
  </picture>
</div>

* Benchmark suite: [Polars.NET-Benchmark](https://github.com/ErrorLSC/Polars.NET-Benchmark)
* Recipe repository: [Polars.NET-Cookbook](https://github.com/ErrorLSC/Polars.NET-Cookbook)

---

## Acknowledgements

Polars.NET is built on:
* [Polars (pola.rs)](https://pola.rs/) - The lightning-fast DataFrame library developed by Ritchie Vink and the Polars team.
* [Apache Arrow](https://arrow.apache.org/) - The columnar in-memory data standard powering zero-copy interop.

---

## License

MIT License. See [LICENSE](LICENSE) for details.  
Third-party licenses and notices are documented in [THIRD_PARTY_LICENSES.html](THIRD_PARTY_LICENSES.html).
