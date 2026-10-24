# Introduction to Polars.NET

Welcome to Polars.NET. This guide provides practical advice and patterns to help you get the most out of the library.

## Core Concepts

Understanding these two modes is crucial for performance:

### 1. Lazy vs. Eager

* **Eager (`DataFrame`)**: Execution happens immediately. Data is loaded into memory. This is similar to `List<T>` or `DataTable`. Use this for debugging or small datasets.
* **Lazy (`LazyFrame`)**: No execution happens until you call `Collect()`. Polars builds a "Query Plan", optimizes it (predicate pushdown, projection pushdown), and then executes.

> **Rule of Thumb:** Always start with **Lazy** (`LazyFrame`). Only convert to **Eager** (`DataFrame`) when you need to inspect the final results or materialize data for the UI.

### 2. Expressions (`Expr`)

In Polars.NET, you don't manipulate data directly in loops. You construct `Expr` objects that tell the engine *what* to do.

```csharp
// Avoid: Traditional C# loops (Slow, data copying)
foreach(var row in df.Rows()) { ... }

// Do: Use Expressions (Fast, SIMD, Parallel)
df.Select(
    Col("A").Sum(),
    Col("B").Filter(Col("A") > 10).Mean()
);
```

## 3.Practical Advice (Best Practices)

### 1. Prefer Scan over Read

When dealing with files (CSV, Parquet, IPC), use the Scan methods provided by LazyFrame.

ReadCsv: Loads the entire file into RAM. Prone to Out-Of-Memory (OOM) on large files.

ScanCsv: Returns a LazyFrame. It only reads the headers initially. When you filter or select columns later, the reader will only read the necessary bytes from the disk.

### 2. Handle "Big Data" with Streaming

If your dataset is larger than your available RAM, standard execution might fail. Polars.NET supports an out-of-core streaming engine.

```cs
// This enables the streaming engine to process data in chunks
// keeping memory usage low and stable.
var result = lazyFrame.Collect(useStreaming:true);
```

### 3. Native Expressions > UDFs

While Polars.NET supports high-performance C# User Defined Functions (UDFs), they are opaque to the optimizer.

Native: Col("A") + Col("B") -> Runs in Rust, uses SIMD, multi-threaded.

UDF: Col("A").Map(x => x + 1) -> Calls back into C#, no SIMD, strictly row-wise or chunk-wise.

Advice: Only use Map if the logic cannot be expressed using the built-in Expr functions (e.g., calling an external API or a complex .NET library).

### 4. Memory Management (using)

Polars.NET objects (DataFrame, Series, LazyFrame) wrap native Rust memory handles. While we implement Finalizers, relying on GC for large data objects can cause memory pressure.

For critical hot paths or huge datasets, use the using statement to deterministic release native memory:

```csharp
using var df = DataFrame.ReadCsv("data.csv");
using var filtered = df.Filter(Col("id") > 100);
// Native memory is freed immediately here
```

### 5. C# vs F#

Polars.NET offers two distinct APIs tailored for each language's strengths:

* C#: Uses a fluent, method-chaining style similar to LINQ.

  * Best for: Production engineering, web services, building typed wrappers.

* F#: Uses a functional, pipeline-operator (|>) style.

  * Best for: Data exploration, scripting, notebooks, and complex transformations.

### 6. Performance Pitfalls

1. Iterating Rows: Avoid converting a DataFrame to `IEnumerable<T>` or iterating rows manually unless absolutely necessary. This breaks the zero-copy advantage and incurs heavy marshalling costs.

2. Premature Collection: Don't call .Collect() in the middle of a transformation chain. Keep the chain lazy as long as possible to let the optimizer do its job.

3. Strings are expensive: Operations on String columns are generally slower than numeric types. If you have categorical data (e.g., "US", "CN", "JP"), cast them to Categorical type for faster grouping and sorting.

```cs
// Cast string column to Categorical for performance 
df.WithColumns(Col("Country").Cast(DataType.Categorical));
```

### 7. Getting Help

API Reference: [Link to API Docs](/api/)

Polars Rust Documentation: Since this is a wrapper with C ABI layer, the core logic is relatively same to the main Polars project. Concepts from the official Polars guide often apply here.
