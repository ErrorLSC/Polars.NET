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

```csharp [C# (ROLLUP / CUBE & GROUPING_ID)]
using Polars.CSharp.Linq;

var report = df.AsQueryable<Sale>()
    .GroupBy(s => new { s.Category, s.Class })
    .RollUp((key, g) => new   // also .Cube(...) and .GroupingSets(...)
    {
        key.Category,
        key.Class,
        Total = g.Sum(s => s.Amount),
        // GROUPING(col): 1 when the key is rolled up at this level, else 0
        Level = key.Category.Grouping() + key.Class.Grouping(),
        // GROUPING_ID(): one bit per key, the first key the most significant bit
        Gid = key.GroupingId(),
        ClassFirst = key.GroupingId(k => new { k.Class, k.Category }),
        // Grouping calls mix freely with keys, aggregates and window functions
        Parent = key.Class.Grouping() == 0 ? key.Category : null,
        Share = g.Sum(s => s.Amount) * 100 / g.Sum(s => s.Amount).Sum().Over(key.GroupingId()),
    });
```

:::

`Grouping()` and `GroupingId()` tell a rolled-up key (a subtotal row) apart from a key whose value is null. Both work only inside the result selector of `RollUp`, `Cube` or `GroupingSets`.

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

## 8. Query Semantics: Matching LINQ to Objects

A query should return what the same query returns over in-memory collections (LINQ to Objects). Polars, like a
SQL database, follows different rules for nulls, NaN, integer arithmetic and strings. The provider compensates
for these differences when it translates the query. This section lists what it compensates for and the few
places where results knowingly differ.

### Null comparisons follow C#

As with EF Core's default, comparisons use C# semantics, not SQL three-valued logic:

| Expression | C# / Polars.NET | SQL-style engine |
|---|---|---|
| `x.A == x.B` with both null | `true` | null (row dropped) |
| `x.V != 10` with `V` null | `true` (row kept) | null (row dropped) |
| `x.V > 5` with `V` null, in a `Select` | `false` | null |
| `ids.Contains(x.Id)` with `Id` null and null in `ids` | `true` | null |
| `others.Any(y => y.K == x.K)` with both keys null | matches | no match |

An explicit `join ... on a equals b` never matches null keys, exactly like `Enumerable.Join`. A composite key
(`on new { a.X, a.Y } equals new { b.X, b.Y }`) matches only when every component matches, so a null component never
matches, as in EF Core. LINQ to Objects compares the anonymous objects and matches null with null there.

In `LeftJoin`, `RightJoin` and `FullJoin`, an unmatched side follows LINQ to Objects: a class is `null`, and a
struct is `default(T)`, so its `int` members read as `0` and its `string` / `Nullable<T>` members as `null`. The
.NET 11 overloads without a result selector return `(Outer, Inner)` tuples with the same rules.
A side cast to `Nullable<T>` first (`depts.Select(d => (Department?)d)`) is `null` when unmatched, and
`pair.Outer.HasValue` / `pair.Outer.Value.Name` translate natively.

Aggregates over `DefaultIfEmpty` inside a group or a correlated subquery
(`g.Select(p => p.Id).DefaultIfEmpty().Max()`, `...DefaultIfEmpty(-1).Min()`, `Average`, `Sum`) run natively
and return the default value for an empty group.

Comparisons against a non-null constant (`x.V == 10`, `x.V > 5`) still reach Polars as plain comparisons inside
a `Where`, so Parquet statistics and predicate pushdown keep working.

Expression trees built by hand (or by VB) can contain comparisons lifted to null (`bool?` results); these are
not translated to Polars and are evaluated in .NET.

To use SQL null semantics instead (as EF Core's `UseRelationalNulls()`), enable the switch:

```csharp
using (Pl.Config.SetUseRelationalNulls(true).BeginScope())
{
    // x.A == x.B is null when either side is null; correlated subqueries never match null keys.
    var rows = df.AsQueryable<Pair>().Where(x => x.A == x.B).ToList();
}
```

### Floating-point values follow .NET

For `double`, `float` and `Half`:

* A comparison involving `NaN` is `false`, and `!=` is `true`. (Polars treats NaN as equal to itself and greater
  than every number.)
* `OrderBy` places `NaN` before every number, and after null, as `Comparer<double>.Default` does.
* `Min` returns `NaN` as soon as one value is `NaN`; `Max` skips `NaN` unless every value is `NaN`.

### Arithmetic

* Integer `/` and `%` truncate toward zero, as in C#: `-7 / 2 == -3` and `-7 % 2 == -1`. `%` keeps the dividend's
  sign for floating-point values too (`-7.5 % 2 == -1.5`).
* `Sum` over `int` / `long` and `Average` over `long` are checked, as in .NET: a total outside the type's range
  throws `OverflowException`. The check is on the total, so a sum whose partial sums overflow but whose total
  fits (`[int.MaxValue, 1, -1]`) returns the total. `decimal` sums are not checked.
* Other integer overflow wraps around, as in unchecked C#, and so does a narrowing cast (`(byte)300 == 44`). A
  floating value cast to an integer saturates to the target's range for every integer type, following
  .NET 11: `(int)double.NaN == 0`, `(int)double.PositiveInfinity == int.MaxValue`, `(byte)-2.7 == 0` and
  `(byte)300.5 == 255`. On .NET 10 and earlier, LINQ to Objects wraps for targets narrower than `int`
  (`(byte)-2.7 == 254`); the provider deliberately follows .NET 11 on every runtime.
* `decimal` values are fixed-point in Polars. A decimal division keeps 18 fractional digits (C# keeps up to 28
  significant digits), and a value carries its column's scale, so `ToString()` may print `2.500` for `2.5m`.

### Strings

* Concatenation (`a + b`, `string.Concat`, `string.Format`, interpolation, `string.Join`) reads null as `""`, as in
  .NET.
* **Strings sort and compare ordinally**, by Unicode code point: `"Banana" < "apple" < "äpfel"`. This applies to
  `OrderBy`, `ThenBy`, `Min` / `Max` over strings, and `CompareTo`. .NET defaults to the current culture, so orderings over mixed-case or
  accented text can differ from LINQ to Objects. `string.Compare(a, b, ...)` is evaluated exactly as in .NET.
* An operator overload that takes a comparer (`OrderBy(x => x.Name, StringComparer.CurrentCulture)`,
  `DistinctBy(..., comparer)`, `GroupBy(..., comparer)`) is not translated. The whole query is evaluated in .NET
  under the client evaluation policy: Strict raises `PolarsPushdownException`, Allow runs it with LINQ to Objects.

### Where .NET would throw

Polars does not raise per-row exceptions, so a few operations that throw in .NET produce null instead (as EF Core
does):

* indexing past the end of a list or array, `Substring` beyond the string's length;
* a member or method called on a null value (`x.Name.Length` with `Name` null);
* a failed `Parse` (`int.Parse`, `DateTime.Parse`);
* integer or decimal division or remainder by zero.

If such a null reaches a non-nullable member of your result type, materialization throws
`InvalidOperationException` naming the member and the column, so it never silently becomes `0` or `default`.
Declare the member nullable (`int?`) to receive the null.

F# record fields are strict for reference types too: a plain `string` field rejects a null (F# strings are not
expected to be null), and the error says how to declare the field instead. A field receives nulls when it is an
option (`string option`, `int option`) or, with nullness checking enabled (`<Nullable>enable</Nullable>`), a
reference type declared `| null` (`string | null`).

The LINQ operators that must fail do fail with the exceptions LINQ uses:

* `First`, `Last`, `Single` and `ElementAt` over a subquery raise `InvalidOperationException` ("Sequence contains no
  elements", "Sequence contains more than one element") or `ArgumentOutOfRangeException`. The check runs inside
  the native plan.
* Storing an undefined value into an enum column raises "Value 'x' is not a defined member of enum T".

### Short-circuit evaluation

A custom method (a managed UDF) on the right of `&&` / `||` / `??`, or in a `?:` branch, runs only on the rows
that reach it, so the usual guards protect it:

```csharp
// Parse is never called with null, and 100 / x.N never runs with N == 0.
var rows = df.AsQueryable<Row>()
    .Where(x => x.Text != null && MyParse(x.Text) > 0)
    .Select(x => x.N == 0 ? 0 : MyRatio(x.N))
    .ToList();
```

The checks behind `First` / `Single` subqueries are the exception. They run once for the whole query, even when
`&&` or an empty outer table means LINQ to Objects would never reach them.

### Dates and times

A `DateTimeOffset` is stored as a UTC instant. Its members read the UTC value: `Offset` is zero, and `Hour` and
`DateTime` give the UTC hour and time. Keep the offset in its own column if you need it.

---

## Next Steps

* [Ecosystem & Native Interop](/articles/interop) - Connect Polars to Apache Arrow C Data Interface, ADBC, ADO.NET, ML.NET, and Tensors.
* [C# Idioms & Best Practices](/articles/csharp-idioms) - Dive deeper into memory management, collection expressions, and Fast-Path mappers.
* [F# Idioms & Best Practices](/articles/fsharp-idioms) - Explore anonymous records, pipeline composition, and typed projections.
