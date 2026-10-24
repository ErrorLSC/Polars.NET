# LINQ Provider (`IQueryable`) & F# Query CE

Polars.NET contains a production-grade LINQ expression-tree compiler (`Polars.NET.Linq`) that translates standard C# `IQueryable<T>` queries and F# `query { }` computation expressions directly into native Rust `LazyFrame` physical plans. 

When a query is fully pushed down, Polars executes multithreaded SIMD kernels in Rust and directly yields a `DataFrame` **without allocating a single intermediate .NET row object**.

<div class="pl-pipeline">
  <div class="pl-pipeline-row">
    <div class="pl-step">
      <div class="pl-step-title">1. .NET Query Frontends</div>
      <div class="pl-step-desc">
        • C# LINQ Query Syntax (<code>from ... select ...</code>)<br>
        • C# Method Syntax (<code>.Where().Select()</code>)<br>
        • F# Query CE (<code>query { for ... select ... }</code>)
      </div>
    </div>
    <div class="pl-arrow">→</div>
    <div class="pl-step">
      <div class="pl-step-title">2. Expression Tree Compiler</div>
      <div class="pl-step-desc">
        • <b>Tier 1</b>: 100% Native Pushdown (Rust DAG)<br>
        • <b>Tier 2</b>: Hybrid (Native Pushdown + Arrow Cursor)<br>
        • <b>Tier 3</b>: Strict Policy / Client Evaluation
      </div>
    </div>
    <div class="pl-arrow">→</div>
    <div class="pl-step">
      <div class="pl-step-title">3. Execution & Materialization</div>
      <div class="pl-step-desc">
        • <code>.ToDataFrame()</code>: Zero managed heap allocations<br>
        • <code>.ToLazyFrame()</code>: Escape to raw Polars DSL<br>
        • <code>.ToList() / .ToArray()</code>: Typed object hydration
      </div>
    </div>
  </div>
</div>

---

## 1. Entrypoints & Query Materialization

To query a DataFrame or LazyFrame with LINQ, call `.AsQueryable<T>()`. The query executes lazily—no computation occurs until a terminal method is called:

::: code-group

```csharp [C# (Query Syntax)]
using Polars;
using Polars.CSharp;
using Polars.CSharp.Linq;

public record OrderDto(int OrderId, string Customer, string Region, double Revenue);

// 1. Enter LINQ from DataFrame or LazyFrame:
var query = from o in df.AsQueryable<OrderDto>()
            where o.Revenue >= 500.0 && o.Region != "Test"
            group o by o.Region into g
            select new {
                Region = g.Key,
                TotalRevenue = g.Sum(x => x.Revenue),
                OrderCount = g.Count()
            };

// 2. Materialize directly into a Polars DataFrame (Zero .NET Object Allocation):
using DataFrame resultDf = query.ToDataFrame();

// 3. Or inspect the compiled native plan:
Console.WriteLine(query.Explain());
```

```csharp [C# (Method Syntax)]
using Polars;
using Polars.CSharp;
using Polars.CSharp.Linq;

public record OrderDto(int OrderId, string Customer, string Region, double Revenue);

var query = df.AsQueryable<OrderDto>()
    .Where(o => o.Revenue >= 500.0 && o.Region != "Test")
    .GroupBy(o => o.Region)
    .Select(g => new {
        Region = g.Key,
        TotalRevenue = g.Sum(x => x.Revenue),
        OrderCount = g.Count()
    });

using DataFrame resultDf = query.ToDataFrame();
```

```fsharp [F# (Query CE)]
open System.Linq
open Polars.FSharp
open Polars.FSharp.Query

[<CLIMutable>]
type OrderDto = {
    OrderId: int
    Customer: string
    Region: string
    Revenue: float
}

// 1. Enter F# query { } computation expression:
let q =
    query {
        for o in df.AsQueryable<OrderDto>() do
        where (o.Revenue >= 500.0 && o.Region <> "Test")
        groupValBy o o.Region into g
        select {|
            Region = g.Key
            TotalRevenue = g.Sum(fun x -> x.Revenue)
            OrderCount = g.Count()
        |}
    }

// 2. Materialize to DataFrame:
use resultDf = q.ToDataFrame()

// 3. Or inspect the compiled native plan:
printfn "%s" (q.Explain())
```

```fsharp [F# (Pipeline API)]
open Polars.FSharp
open Polars.FSharp.Query

// Idiomatic F# pipeline combinators over Query
let resultDf =
    df.AsQueryable<OrderDto>()
    |> Query.where (fun o -> o.Revenue >= 500.0 && o.Region <> "Test")
    |> Query.toDataFrame
```

:::

### Real Engine Plan & Output

Here is the exact logical execution plan and materialized DataFrame produced by Polars for the query above:

#### A. 100% Native Pushdown Plan (`query.Explain()`)
<!--@include: ../snippets/linq_plan_inspection.html-->

#### B. Materialized Native Output (`query.ToDataFrame()`)
<!--@include: ../snippets/linq_groupby_agg.html-->

---

### Direct Pipeline Entrypoints: Collections & ADO.NET Streams

Instead of manually creating a `DataFrame` or `LazyFrame` first, you can enter the Polars LINQ pipeline directly from in-memory collections or database streams using `AsPolarsQueryable`:

#### 1. In-Memory Collections (`IEnumerable<T>.AsPolarsQueryable()`)
Directly transposes any `List<T>`, array, or in-memory sequence into a native Polars LazyFrame and exposes it as a Polars LINQ queryable:

```csharp
using Polars.CSharp;
using Polars.CSharp.Linq;

List<OrderDto> orders = GetOrders();

// Ingest in-memory list straight into native Polars Rust execution:
using DataFrame topOrders = orders.AsPolarsQueryable()
    .Where(o => o.Revenue >= 500.0)
    .OrderByDescending(o => o.Revenue)
    .Take(100)
    .ToDataFrame();
```

#### 2. ADO.NET Database Streams (`IDataReader.AsPolarsQueryable()`)
Streams an open `IDataReader` directly into a Polars LINQ query with configurable batch size, bypassing intermediate object allocations:

```csharp
using System.Data;
using Microsoft.Data.SqlClient;
using Polars.CSharp;
using Polars.CSharp.Linq;

using var cmd = connection.CreateCommand();
cmd.CommandText = "SELECT OrderId, Customer, Region, Revenue FROM Orders";
using IDataReader reader = cmd.ExecuteReader();

// Streams database rows straight into Polars LINQ engine:
using DataFrame regionalSales = reader.AsPolarsQueryable<OrderDto>(batchSize: 50_000)
    .Where(o => o.Revenue > 100.0)
    .GroupBy(o => o.Region)
    .Select(g => new {
        Region = g.Key,
        TotalRevenue = g.Sum(x => x.Revenue),
        OrderCount = g.Count()
    })
    .ToDataFrame();
```

#### 3. Seamless In-Memory Collection Interop (Join, Concat & Union)

The Polars LINQ provider seamlessly accepts arbitrary in-memory .NET collections (`List<T>`, arrays, `IEnumerable<T>`) as operands inside active queries without manual pre-conversion:

* **In-Memory Join Pushdown**: Join a Polars DataFrame query directly with an in-memory lookup collection (`List<TInner>`). The provider transposes the in-memory sequence into native Arrow batches and executes a Tier-1 100% native pushdown join in Rust:
  ```csharp
  var inMemoryCustomers = new List<Customer>
  {
      new(1, "Alice"),
      new(2, "Bob")
  };

  // query is a Polars queryable over a high-volume DataFrame or LazyFrame:
  var enrichedQuery = ordersQuery.Join(
      inMemoryCustomers, // Arbitrary in-memory List<T>
      o => o.CustomerId,
      c => c.Id,
      (o, c) => new { o.OrderId, c.Name, o.Amount }
  );

  using DataFrame resultDf = enrichedQuery.ToDataFrame();
  ```

* **In-Memory Concat & Union Pushdown**: Append or merge in-memory rows directly into a Polars query:
  ```csharp
  var extraEmployees = new List<Employee>
  {
      new("Charlie", 35, 70000),
      new("David", 40, 80000)
  };

  // Concatenate in-memory collection straight into the native physical plan:
  var combinedQuery = query.Where(e => e.Age >= 25).Concat(extraEmployees);
  using DataFrame combinedDf = combinedQuery.ToDataFrame();
  ```

> [!NOTE] Native Transposition Guarantee
> When an in-memory collection participates in a `Join`, `Concat`, `Union`, or `Merge` operation, Polars.NET automatically transposes the managed objects into Arrow contiguous arrays, pushes execution into native Rust SIMD kernels, and preserves full query optimization.

---

## 2. Terminal Methods: What Can You Materialize?

Polars.NET LINQ queries support diverse terminal materialization pathways depending on your scenario:

| Method | Target | Performance Characteristics |
| :--- | :--- | :--- |
| **`.ToDataFrame()`** / **`.ToDataFrameAsync()`** | `DataFrame` | **Fastest**. 100% native Rust execution; zero .NET object hydration. |
| **`.ToLazyFrame()`** | `LazyFrame` | Compiles the LINQ pipeline to a native `LazyFrame` for continued raw Polars DSL chaining. |
| **`.ToSeries(name)`** | `Series` | Extracts a single column or composite struct series from a projected query. |
| **`.ToList()`** / **`.ToArray()`** | `List<T>` / `T[]` | Materializes typed .NET objects via pre-compiled stack cursors. |
| **`await foreach`** / **`.ToAsyncEnumerable()`** | `IAsyncEnumerable<T>` | Asynchronous non-blocking streaming without thread starvation. |
| **`.First()`** / **`.Single()`** / **`.Count()`** | Scalar | Scalar aggregations pushed down to single-row reduction kernels. |

---

## 3. Supported Native BCL & C# Method Mappings

The LINQ provider automatically translates standard .NET Base Class Library (BCL) calls into vectorized Polars expressions:

<div class="pl-pipeline">
  <div class="pl-pipeline-row">
    <div class="pl-step">
      <div class="pl-step-title">C# BCL Expressions</div>
      <div class="pl-step-desc"><code>s.Name.ToUpper()</code><br><code>s.Created.Year</code><br><code>Math.Round(s.Price, 2)</code><br><code>Regex.IsMatch(s.Email, pattern)</code></div>
    </div>
    <div class="pl-arrow">→ <i>Compiled to</i> →</div>
    <div class="pl-step">
      <div class="pl-step-title">Rust Vectorized Kernels</div>
      <div class="pl-step-desc"><code>col("Name").str.to_uppercase()</code><br><code>col("Created").dt.year()</code><br><code>col("Price").round(2)</code><br><code>col("Email").str.contains(pattern)</code></div>
    </div>
  </div>
</div>

| Category | Supported .NET Operations | Compiled Native Polars Expression |
| :--- | :--- | :--- |
| **Strings** | `s.Contains(x)`, `s.StartsWith(x)`, `s.EndsWith(x)`, `s.ToUpper()`, `s.ToLower()`, `s.Trim()`, `s.Substring(start, len)`, `s.Replace(a, b)`, `s.Length`, `string.IsNullOrEmpty(s)` | `.Str.Contains()`, `.Str.StartsWith()`, `.Str.ToUppercase()`, `.Str.Slice()`, `.Str.ReplaceAll()`, `.Str.LenBytes()` |
| **Mathematics** | `Math.Abs(x)`, `Math.Round(x, n)`, `Math.Floor(x)`, `Math.Ceiling(x)`, `Math.Sqrt(x)`, `Math.Pow(x, y)`, `Math.Log(x)`, `Math.Exp(x)`, `Math.Sin(x)`, `Math.Clamp(x, min, max)` | `.Abs()`, `.Round()`, `.Floor()`, `.Ceil()`, `.Sqrt()`, `.Pow()`, `.Log()`, `.Exp()`, `.Sin()`, `.Clip()` |
| **Date & Time** | `dt.Year`, `dt.Month`, `dt.Day`, `dt.Hour`, `dt.Minute`, `dt.Second`, `dt.DayOfWeek`, `dt.DayOfYear`, `dt.Date`, `dt.AddDays(n)` | `.Dt.Year()`, `.Dt.Month()`, `.Dt.Day()`, `.Dt.Hour()`, `.Dt.Minute()`, `.Dt.Second()`, `.Dt.Weekday()`, `.Dt.OffsetBy()` |
| **Regular Expressions** | `Regex.IsMatch(s, pattern)` | `.Str.Contains(pattern)` using native Rust high-performance regex engine |
| **Collections & Lists** | `item.Tags.Contains("promo")`, `item.Tags.Length`, `items.SelectMany(x => x.Tags)` | `.List.Contains()`, `.List.Len()`, Native **`.Explode()`** |
| **Conditionals** | `x ? a : b`, `a ?? b` | Native ternary `When.Then.Otherwise` and `Coalesce` kernels |

---

## 4. The 3-Tier Execution Architecture

When you call `query.Explain()`, Polars.NET reports one of three execution tiers:

### Tier 1: `[100% Native Pushdown]`
Every filtering, sorting, grouping, joining, and projection stage is translated into native Polars Rust expressions.
* **Heap Impact**: Zero managed heap allocations during execution.
* **Throughput**: Maximum SIMD vectorized throughput.

### Tier 2: `[Hybrid Execution (Partial Pushdown)]`
Filtering, sorting, and grouping run 100% in Rust. Only custom user code in the final `.Select(...)` (such as formatting or calling external helper functions) is evaluated on the client via a fast struct cursor:

```csharp
var query = df.AsQueryable<Transaction>()
    .Where(t => t.Amount > 100)                      // Native Pushdown (Rust)
    .OrderByDescending(t => t.Amount)               // Native Pushdown (Rust)
    .Select(t => new {                              // Client Evaluation on final row:
        t.Id,
        FormattedDisplay = FormatExternal(t.Amount) // Custom C# method
    });
```

### Tier 3: Client Evaluation Policy & `PolarsPushdownException`
By default, Polars.NET adopts a **Strict Policy** (`ClientEvaluationPolicy.Strict`). If an unsupported custom method is used inside an *intermediate* stage (`Where`, `GroupBy`, `Join`), the provider throws `PolarsPushdownException` to prevent silent performance degradation:

```csharp
// [Error] Throws PolarsPushdownException: CustomMethod cannot be evaluated inside Rust SIMD kernel!
var query = df.AsQueryable<Item>().Where(x => MyCustomHelper(x.Score));
```

#### How to Address Pushdown Exceptions:
1. **Best Practice**: Replace custom methods with supported BCL functions (`string`, `Math`, `Regex`).
2. **Hybrid Projection**: Move custom formatting into the terminal `.Select(...)` clause.
3. **Explicit Client Evaluation**: Call `.AsEnumerable()` (C#) or `.AsSeq()` (F#) to consciously switch to in-memory LINQ:
   ```csharp
   // Explicitly offload to client:
   var result = df.AsQueryable<Item>()
       .Where(x => x.Active)               // Pushed down to Polars
       .AsEnumerable()                     // Explicit client barrier
       .Where(x => MyCustomHelper(x.Score)); // Evaluated on client
   ```

---

## 5. Extended Analytics Operators in LINQ

Standard C# LINQ syntax lacks window functions, lag/lead operators, and time-series tools. `Polars.CSharp.Linq` fills this gap by introducing extension methods usable directly inside LINQ `.Select(...)`:

::: code-group

```csharp [C# (Window Functions & Shifts)]
using Polars.CSharp.Linq;

var query = df.AsQueryable<Employee>()
    .Select(e => new
    {
        e.Name,
        e.DepartmentId,
        e.Salary,
        // 1. Window Function: Group Average
        DeptAvg = e.Salary.Average().Over(e.DepartmentId),
        
        // 2. Window Deviation
        DiffFromAvg = e.Salary - e.Salary.Average().Over(e.DepartmentId),
        
        // 3. Dense Ranking within department
        DeptRank = e.Salary.Rank(RankMethod.Dense, descending: true).Over(e.DepartmentId),
        
        // 4. Time-series Lead / Lag Shift
        PrevSalary = e.Salary.Shift(1).Over(e.DepartmentId)
    });
```

```csharp [C# (Time-Series Interpolation)]
using Polars.CSharp.Linq;

var cleanQuery = df.AsQueryable<SensorReading>()
    .Select(r => new
    {
        r.Timestamp,
        r.DeviceId,
        // Linear interpolation over missing sensor readings partitioned by Device
        CleanVal = r.RawValue.Interpolate(InterpolationMethod.Linear).Over(r.DeviceId)
    });
```

:::

---

## 6. The Bidirectional Escape Hatch

No matter how capable a LINQ provider is, advanced analytics occasionally require specialized engine-specific features (such as regex unnesting, Parquet scan tuning, or Aho-Corasick matching). Polars.NET provides a seamless **Bidirectional Escape Hatch**:

<div class="pl-pipeline">
  <div class="pl-pipeline-row">
    <div class="pl-step" style="text-align: center;">
      <div class="pl-step-title">IQueryable&lt;T&gt; (LINQ Provider)</div>
      <div class="pl-step-desc">Typed C# queries and F# <code>query { }</code> with BCL expressions</div>
    </div>
    <div class="pl-arrow">⇄ <code>query.ToLazyFrame()</code> / <code>lf.AsQueryable&lt;T&gt;()</code> ⇄</div>
    <div class="pl-step" style="text-align: center;">
      <div class="pl-step-title">LazyFrame (Native Polars DSL)</div>
      <div class="pl-step-desc">Raw Rust SIMD kernels, Selectors, and Streaming engine</div>
    </div>
  </div>
</div>

### Pattern 1: LINQ to Native LazyFrame (`.ToLazyFrame()`)
Start with familiar LINQ for relational filtering and joining, then seamlessly switch to native Polars DSL for specialized operations:

```csharp
// 1. Build initial filtering using comfortable LINQ:
var baseQuery = df.AsQueryable<LogEntry>()
    .Where(e => e.Level == "ERROR" && e.Timestamp >= DateTime.UtcNow.AddDays(-7));

// 2. Escape hatch to native LazyFrame:
using LazyFrame lf = baseQuery.ToLazyFrame();

// 3. Apply raw Polars domain operations (Regex extraction, etc.):
using DataFrame finalDf = lf
    .WithColumns(Pl.Col("Message").Str.ExtractAll(@"\[code:(\d+)\]").Alias("ErrorCodes"))
    .Collect();
```

### Pattern 2: Native LazyFrame to LINQ (`.AsQueryable<T>()`)
Conversely, start with specialized file scanning or partition pruning, and switch back to LINQ:

```csharp
// 1. Scan with specialized low-level parquet tuning:
using LazyFrame lf = LazyFrame.ScanParquet("telemetry/*.parquet")
    .Filter(Pl.Col("DeviceType") == "IoT");

// 2. Resume querying with standard LINQ:
var summary = from r in lf.AsQueryable<TelemetryDto>()
              group r by r.Location into g
              select new {
                  Location = g.Key,
                  AvgTemp = g.Average(x => x.Temperature)
              };
```

---

## 7. Polars.NET Exclusive: Strongly-Typed LINQ Merge (`TypedMerge`)

Polars.NET brings full **ANSI SQL `MERGE`** semantics directly into the C# and F# LINQ ecosystem:

::: code-group

```csharp [C# (TypedMerge)]
using Polars.CSharp;
using Polars.CSharp.Linq;

var targetQuery = targetDf.AsQueryable<ProductItem>();
var sourceQuery = sourceDf.AsQueryable<ProductItem>();

// Perform strongly-typed SQL MERGE using C# expression trees
using DataFrame resultDf = targetQuery.Merge(sourceQuery, t => t.Id, s => s.Id)
    // 1. Matched condition -> Delete
    .WhenMatched(
        (t, s) => s.IsDiscontinued,
        then => then.Delete()
    )
    // 2. Matched condition -> Update modified fields
    .WhenMatched(
        (t, s) => s.Price != t.Price,
        then => then.Update(set => set.Set(t => t.Price, (t, s) => s.Price))
    )
    // 3. Not matched in target -> Insert new item from source
    .WhenNotMatchedByTarget(
        s => s.Price > 0.0,
        then => then.Insert()
    )
    // 4. Not matched in source -> Delete obsolete target item
    .WhenNotMatchedBySource(
        t => t.Category == "Seasonal",
        then => then.Delete()
    )
    .ToDataFrame(); // Returns compiled DataFrame directly!
```

```fsharp [F# (Pipeline Merge)]
open Polars.FSharp

// F# functional pipeline merge
let mergedDf =
    targetDf.Merge(sourceDf, pl.col "Id")
    |> Merge.whenMatched (pl.col "IsDiscontinued") Merge.delete
    |> Merge.whenMatchedUpdateAll
    |> Merge.whenNotMatchedInsertAll
    |> Merge.executeEager
```

:::

### Direct In-Memory Collection Merge (`List<T>.Merge(...)`)

The strongly-typed `Merge` builder can also be initiated directly on standard in-memory collections (`List<T>`, arrays, `IEnumerable<T>`), merging against another in-memory list or a Polars queryable:

```csharp
List<ProductItem> inMemoryTarget = GetTargetProducts();
List<ProductItem> incomingUpdates = GetSourceProducts();

// 1. Initiate full ANSI SQL MERGE directly from an in-memory List<T>:
using DataFrame mergedDf = inMemoryTarget.Merge(incomingUpdates, t => t.Id, s => s.Id)
    .WhenMatched(
        (t, s) => s.Price > t.Price,
        then => then.Update(set => set.Set(t => t.Price, (t, s) => s.Price))
    )
    .WhenNotMatchedByTarget(then => then.Insert())
    .WhenNotMatchedBySource(t => t.Category == "Seasonal", then => then.Delete())
    .ToDataFrame();

// 2. Or materialize directly back to typed entities:
List<ProductItem> updatedList = inMemoryTarget.Merge(incomingUpdates, t => t.Id, s => s.Id)
    .WhenMatched(then => then.Update(set => set.Set(t => t.Price, (t, s) => s.Price)))
    .WhenNotMatchedByTarget(then => then.Insert())
    .ToList();
```

---

## Next Steps

* [Ecosystem & Native Interop](/articles/interop) - Connect Polars to Apache Arrow C Data Interface, ADBC, ADO.NET, ML.NET, and Tensors.
* [C# Idioms & Best Practices](/articles/csharp-idioms) - Dive deeper into memory management, collection expressions, and Fast-Path mappers.
* [F# Idioms & Best Practices](/articles/fsharp-idioms) - Explore anonymous records, pipeline composition, and typed projections.
