# LazyFrame & Query Optimization

Polars.NET achieves its staggering analytical performance through its **Lazy Engine**. Instead of eagerly evaluating operations row-by-row or column-by-column, the engine records your pipeline into a Directed Acyclic Graph (DAG) called a **Logical Plan**. 

When you trigger materialization (`.Collect()`), the Rust query optimizer analyzes the entire pipeline, eliminating dead columns, reordering filters, and compiling an optimal physical execution plan.

---

## 1. The Mental Model: Eager vs. Lazy Execution

| Feature | Eager (`DataFrame`) | Lazy (`LazyFrame`) |
| :--- | :--- | :--- |
| **Execution Timing** | Immediately on every method call | Deferred until `.Collect()` |
| **Intermediate Memory** | Full intermediate buffers allocated per step | Zero intermediate memory allocated |
| **Optimizer Passes** | None (runs exactly as called) | Predicate & Projection Pushdown, CSE, Limit Pushdown |
| **Max Dataset Size** | Must fit comfortably in available RAM | Can exceed RAM via Streaming Engine |
| **Inspection** | Inspect data directly (`.Head()`, `.Show()`) | Inspect query plan (`.Explain()`, `.ToHtml()`) |

::: code-group

```csharp [C# (Eager vs Lazy)]
using Polars;
using Polars.CSharp;
using static Polars.CSharp.Polars;

// Eager: Allocates 3 intermediate DataFrames on heap
DataFrame eagerResult = df
    .Filter(Col("Category") == "Tech")      // Allocates intermediate DF 1
    .WithColumns((Col("Amount") * 0.1).Alias("Tax")) // Allocates intermediate DF 2
    .Sort("Total", descending: true);       // Allocates intermediate DF 3

// Lazy: Builds a single optimized execution graph
DataFrame lazyResult = df.Lazy()
    .Filter(Col("Category") == "Tech")
    .WithColumns((Col("Amount") * 0.1).Alias("Tax"))
    .Sort("Total", descending: true)
    .Collect();                             // Single optimized execution!
```

```fsharp [F# (Eager vs Lazy)]
open Polars.FSharp

// Eager: Each pipe creates a materialized intermediate DataFrame
let eagerResult =
    df
    |> DataFrame.filter (pl.col "Category" .== pl.lit "Tech")
    |> DataFrame.withColumn ((pl.col "Amount" * pl.lit 0.1).Alias "Tax")
    |> DataFrame.sortDescending [ pl.col "Total" ]

// Lazy: Zero intermediate allocation, compiled to native query graph
let lazyResult =
    df.Lazy()
    |> LazyFrame.filter (pl.col "Category" .== pl.lit "Tech")
    |> LazyFrame.withColumn ((pl.col "Amount" * pl.lit 0.1).Alias "Tax")
    |> LazyFrame.sortDescending [ pl.col "Total" ]
    |> LazyFrame.collect
```

```csharp [C# (LINQ)]
// LINQ to Polars is LAZY by default!
// AsQueryable<T>() builds an expression tree that compiles to LazyFrame
var query = from t in df.AsQueryable<Transaction>()
            where t.Category == "Tech"
            orderby t.Amount descending
            select new {
                t.Customer,
                t.Amount,
                Tax = t.Amount * 0.1
            };

// Materializes only when ToDataFrame() or enumeration is invoked:
using DataFrame result = query.ToDataFrame();
```

:::

---

## 2. The Lazy Query Lifecycle

A typical LazyFrame pipeline follows three clear phases:

<div class="pl-pipeline">
  <div class="pl-pipeline-row">
    <div class="pl-step">
      <div class="pl-step-title">1. Acquisition (Zero I/O)</div>
      <div class="pl-step-desc"><code>LazyFrame.ScanParquet()</code><br><code>LazyFrame.ScanCsv()</code><br><code>df.Lazy()</code></div>
    </div>
    <div class="pl-arrow">→</div>
    <div class="pl-step">
      <div class="pl-step-title">2. Composition (Query DAG)</div>
      <div class="pl-step-desc"><code>.Filter()</code><br><code>.WithColumns()</code><br><code>.GroupBy() / .Agg()</code><br><code>.Join()</code></div>
    </div>
    <div class="pl-arrow">→</div>
    <div class="pl-step">
      <div class="pl-step-title">3. Optimization & Compilation</div>
      <div class="pl-step-desc">Rust pushdown optimizers compile physical execution kernels</div>
    </div>
    <div class="pl-arrow">→</div>
    <div class="pl-step">
      <div class="pl-step-title">4. Materialization</div>
      <div class="pl-step-desc"><code>.Collect()</code> → DataFrame<br><code>.CollectBatchesAsync()</code> → Stream</div>
    </div>
  </div>
</div>

### 1. Zero-Cost File Scanning (`ScanParquet` / `ScanCsv` / `ScanIpc`)

Instead of loading massive files entirely into RAM with `ReadParquet` or `ReadCsv`, always prefer `ScanParquet` and `ScanCsv`. They read only the file metadata and headers:

::: code-group

```csharp [C# (Fluent API)]
using Polars;
using Polars.CSharp;

// Reads 0 data bytes from disk! Only file metadata is inspected.
LazyFrame lf = LazyFrame.ScanParquet("large_dataset.parquet");
```

```fsharp [F# (Pipeline API)]
open Polars.FSharp

// Zero memory read at invocation
let lf = LazyFrame.ScanParquet "large_dataset.parquet"
```

:::

### 2. Immutability & Safe Query Branching

In Polars.NET, a `LazyFrame` is an **immutable query description**. It is completely safe to branch, fork, or reuse a base `LazyFrame` across multiple pipelines without consuming it:

::: code-group

```csharp [C# (Fluent API)]
using var basePlan = LazyFrame.ScanParquet("sales.parquet");

// Safe branching: basePlan is NEVER consumed or mutated!
using var techSales = basePlan.Filter(Col("Category") == "Tech").Collect();
using var bookSales = basePlan.Filter(Col("Category") == "Books").Collect();
```

```fsharp [F# (Pipeline API)]
use basePlan = LazyFrame.ScanParquet "sales.parquet"

// Both branches independently optimize and execute
use techSales = basePlan |> LazyFrame.filter (pl.col "Category" .== pl.lit "Tech") |> LazyFrame.collect
use bookSales = basePlan |> LazyFrame.filter (pl.col "Category" .== pl.lit "Books") |> LazyFrame.collect
```

:::

---

## 3. Engine Optimizations Under the Hood

When you invoke `.Collect()`, the engine runs several transformative optimization passes:

### 1. Predicate Pushdown (Filter Pushdown)
Filters declared at the end of a query pipeline are pushed directly down into the file reader or before expensive operations like Joins and Window aggregations. If scanning Parquet, rows that don't match the filter are skipped directly at the storage level using Parquet page statistics (min/max).

### 2. Projection Pushdown (Column Pruning)
Columns not required by downstream expressions are pruned from the query plan. If your Parquet file contains 100 columns but your query only selects 3, **only those 3 columns are read from disk**, reducing disk I/O and memory pressure by up to 97%.

### 3. Common Subplan Elimination (CSE)
When multiple expressions share identical calculations (e.g. `Col("Price") * Col("Quantity")`), the engine identifies the common subtree and computes it exactly once.

### 4. Slice / Limit Pushdown
When `.Limit(n)` or `.Head(n)` is present, the scan operation stops decoding data as soon as the requested row count is satisfied.

---

## 4. Query Plan Inspection: `.Explain()` and `.ToHtml()`

Polars provides full transparency into what the optimizer decides to do. You can inspect both the raw unoptimized plan and the optimized execution graph:

::: code-group

```csharp [C# (Inspecting Plans)]
var lf = df.Lazy()
    .Filter(Col("Amount") > 50.0)
    .WithColumns((Col("Amount") * 0.1).Alias("Tax"))
    .Filter(Col("Category") == "Tech")
    .GroupBy("Customer")
    .Agg(
        Col("Amount").Sum().Alias("Total_Spend"),
        Col("Tax").Sum().Alias("Total_Tax"),
        Col("TxId").Count().Alias("Tx_Count")
    )
    .Sort("Total_Spend", descending: true);

// 1. Raw Logical Plan (Unoptimized)
Console.WriteLine(lf.Explain(optimized: false));

// 2. Physical Execution Plan (Optimized)
Console.WriteLine(lf.Explain(optimized: true));
```

```fsharp [F# (Inspecting Plans)]
let lf =
    df.Lazy()
    |> LazyFrame.filter (pl.col "Amount" .> pl.lit 50.0)
    |> LazyFrame.withColumn ((pl.col "Amount" * pl.lit 0.1).Alias "Tax")
    |> LazyFrame.filter (pl.col "Category" .== pl.lit "Tech")
    |> LazyFrame.groupBy [ pl.col "Customer" ]
    |> LazyFrame.agg [
        pl.col("Amount").Sum().Alias "Total_Spend"
        pl.col("Tax").Sum().Alias "Total_Tax"
        pl.col("TxId").Count().Alias "Tx_Count"
    ]
    |> LazyFrame.sortDescending [ pl.col "Total_Spend" ]

// 1. Raw Logical Plan (Unoptimized)
printfn "%s" (lf.Explain(optimized = false))

// 2. Physical Execution Plan (Optimized)
printfn "%s" (lf.Explain(optimized = true))
```

:::

### Real Engine Optimization in Action

Observe how the Polars Rust optimizer transforms the pipeline above:

#### A. Raw Logical Plan (Before Optimization)
In the raw plan, both filters are separate, and the scan projects **all 5 columns** (`PROJECT */5 COLUMNS`) including the unused `IsFlagged` column:

<!--@include: ../snippets/lazy_unoptimized_plan.html-->

#### B. Physical Execution Plan (After Optimization)
Notice the optimizer's brilliance:
1. **Predicate Merging & Pushdown**: The two filters `Amount > 50.0` and `Category == "Tech"` are combined into a single SIMD boolean expression `(col("Amount") > 50.0) & (col("Category") == "Tech")` and pushed down **before** calculating `Tax`.
2. **Projection Pushdown**: Column `IsFlagged` is completely eliminated from memory and I/O (`PROJECT 4/5 COLUMNS`).

<!--@include: ../snippets/lazy_optimized_plan.html-->

#### C. Collected Materialized Result
Calling `.Collect()` materializes the cleanly aggregated result:

<!--@include: ../snippets/lazy_collected_df.html-->

---

## 5. Streaming Engine (Out-of-Core Execution)

When datasets are larger than available physical RAM, eager engines throw an `OutOfMemoryException`. Polars.NET solves this through its **Streaming Engine**:

Instead of loading the entire table at once, the streaming engine breaks the query graph into discrete chunks that are streamed through the execution pipeline:

<div class="pl-pipeline">
  <div class="pl-pipeline-row">
    <div class="pl-step">
      <div class="pl-step-title">Dataset on Disk (100 GB)</div>
      <div class="pl-step-desc">Cold storage Parquet/CSV files exceeding physical RAM capacity</div>
    </div>
    <div class="pl-arrow">→ <i>batches</i> →</div>
    <div class="pl-step">
      <div class="pl-step-title">Streaming Engine (RAM: ~500 MB)</div>
      <div class="pl-step-desc">Bounded chunked execution; memory consumption stays strictly bounded</div>
    </div>
    <div class="pl-arrow">→</div>
    <div class="pl-step">
      <div class="pl-step-title">Output Sink</div>
      <div class="pl-step-desc">Target Parquet file, Database via <code>SinkTo</code>, or streaming consumer</div>
    </div>
  </div>
</div>

### Activating the Streaming Engine

Pass `Engine.Streaming` into `.Collect()` or use asynchronous batch iteration:

::: code-group

```csharp [C# (Streaming Execution)]
using Polars;
using Polars.CSharp;

var lf = LazyFrame.ScanParquet("giant_dataset_50gb.parquet")
    .Filter(Col("ResponseTime") > 1000)
    .GroupBy("Service")
    .Agg(Col("ResponseTime").Mean().Alias("Avg_Latency"));

// Collect using Polars native streaming execution:
using DataFrame result = lf.Collect(Engine.Streaming);
```

```fsharp [F# (Streaming Execution)]
open Polars.FSharp

let lf =
    LazyFrame.ScanParquet "giant_dataset_50gb.parquet"
    |> LazyFrame.filter (pl.col "ResponseTime" .> pl.lit 1000)
    |> LazyFrame.groupBy [ pl.col "Service" ]
    |> LazyFrame.agg [ pl.col("ResponseTime").Mean().Alias "Avg_Latency" ]

// Execute in streaming engine
use result = lf.Collect(Engine.Streaming)
```

:::

---

## 6. Asynchronous Streaming (`IAsyncEnumerable`)

Polars.NET provides first-class support for modern .NET asynchronous streams via `CollectBatchesAsync()`:

::: code-group

```csharp [C# (Asynchronous Batches)]
using Polars.CSharp;

var lf = LazyFrame.ScanParquet("large_events.parquet")
    .Filter(Col("Status") == "ERROR");

// Stream materialized DataFrame batches as soon as Rust computes them:
await foreach (DataFrame batch in lf.CollectBatchesAsync(chunkSize: 50_000))
{
    using (batch)
    {
        Console.WriteLine($"Processed batch of {batch.Height} error records.");
        await PostToAlertSystemAsync(batch);
    }
}
```

```fsharp [F# (Asynchronous Batches)]
open Polars.FSharp

let lf =
    LazyFrame.ScanParquet "large_events.parquet"
    |> LazyFrame.filter (pl.col "Status" .== pl.lit "ERROR")

// Consume asynchronous stream without blocking the main thread
task {
    for batch in lf.CollectBatchesAsync(chunkSize = 50_000L) do
        use b = batch
        printfn $"Processed batch of %d{b.Height} error records."
        do! postToAlertSystem b
}
```

:::

---

## Next Steps

* [LINQ & Query Expressions](/articles/linq) - Explore how C# LINQ and F# `query { ... }` natively translate into LazyFrame execution graphs.
* [Ecosystem & Native Interop](/articles/interop) - Stream LazyFrame results into Apache Arrow, ADBC, and ADO.NET.
* [C# Idioms & Best Practices](/articles/csharp-idioms) - Memory management, struct enumerators, and disposal patterns.

