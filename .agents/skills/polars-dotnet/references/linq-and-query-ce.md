# Polars.NET LINQ (`Polars.CSharp.Linq`) & F# Query CE (`Polars.FSharp.Query`)

Polars.NET includes a full expression-tree compiler (`Polars.NET.Linq`) that translates C# `IQueryable<T>` pipelines and F# `query { }` computation expressions into native Rust Polars `LazyFrame` plans, complete with query plan caching, subquery decorrelation, and zero-allocation Arrow row materialization.

---

## 1. Entrypoints & Terminal Conversions

### C# (`using Polars.CSharp.Linq;`)
- **Start a query**:
  - `df.AsQueryable<T>()` or `lf.AsQueryable<T>()`
  - `series.AsQueryable<T>()` (for scalar series or unnesting struct series)
  - `enumerable.AsPolarsQueryable()` (pushes in-memory `IEnumerable<T>` into Polars LazyFrame)
- **Finish a query**:
  - `.ToDataFrame()` / `.ToDataFrameAsync()` — compiles and executes directly into a `DataFrame` **without** hydrating intermediate C# objects when 100% pushed down!
  - `.ToLazyFrame()` — extracts the compiled `LazyFrame` to continue chaining native Polars DSL operations.
  - `.ToSeries(name)` — materializes single-column or struct series.
  - `.ToList()`, `.ToArray()`, `foreach`, `.First()`, `.Single()`, `.Count()`, `.Sum()`, `.Min()`, `.Max()`, `.Average()`, `.Any()`, `.All()` — executes and materializes strongly-typed `.NET` values via zero-allocation IL/cursor materializers.
- **Inspect the execution plan**:
  - `string plan = query.Explain(optimized: true);`
  - `query.InspectPlan(Console.WriteLine);`

### F# (`open Polars.FSharp.Query` + `open System.Linq`)
- **Start a query**:
  - `df.AsQueryable<'T>()` or `lf.AsQueryable<'T>()`
  - Inference helper: `df.AsQueryable(sampleSeq)` (infers `'T` from an existing sequence without explicit type annotation!)
- **Write F# `query { }` CE**:
  ```fsharp
  open System.Linq
  open Polars.FSharp
  open Polars.FSharp.Query

  let q =
      query {
          for o in dfOrders.AsQueryable<OrderDto>() do
          where (o.Revenue > 1000.0 && o.Region <> "Test")
          groupValBy o o.Region into g
          select {
              Region = g.Key
              TotalRevenue = g.Sum(fun x -> x.Revenue)
              Count = g.Count()
          }
      }
  ```
- **Finish or Inspect**:
  - `q.ToDataFrame()`, `q.ToLazyFrame()`, `q.ToSeries()`, `q.ToList()`, `q.Explain()`
  - Pipeline module `Query.*`: `q |> Query.toDataFrame`, `q |> Query.toLazyFrame`, `q |> Query.asSeq`

---

## 2. Supported Native Pushdown Capabilities

1. **Filtering, Projection & Let Bindings**:
   - Arbitrary `Where` predicates, `let` intermediate variables, nested struct field access (`u.Location.Geo.Lat`), anonymous types (`new { ... }`), F# records, F# anonymous records (`{| ... |}`), C# `record` / `readonly record struct` / ValueTuples, null-coalescing (`x.Nickname ?? x.FallbackName`), andF# `Option.defaultValue` / `.IsSome` / `.Value`.
2. **String, Math, Date/Time & Regex Mappings**:
   - `string`: `Contains`, `StartsWith`, `EndsWith`, `ToUpper`, `ToLower`, `Trim`, `TrimStart`, `TrimEnd`, `Substring`, `Replace`, `IndexOf`, `Length`, `IsNullOrEmpty`, `IsNullOrWhiteSpace`, `PadLeft`, `PadRight`.
   - `Math`: `Abs`, `Round`, `Floor`, `Ceiling`, `Sqrt`, `Pow`, `Log`, `Log10`, `Exp`, `Sin`, `Cos`, `Tan`, `Min`, `Max`, `Clamp`, `Sign`.
   - `DateTime` / `DateOnly` / `TimeOnly` / `TimeSpan`: `.Year`, `.Month`, `.Day`, `.Hour`, `.Minute`, `.Second`, `.DayOfWeek`, `.DayOfYear`, `.Date`, `.AddDays(...)`, `.AddHours(...)`, date subtraction.
   - `Regex.IsMatch(input, pattern)` -> native Rust regex crate pushdown.
   - Array/List columns: `.Contains(...)`, `.Length` / `.Count`, `.Any()`, `SelectMany` (native `Explode`!).
3. **Joins & Set Operations**:
   - Inner `Join`, multi-key `Join` (`on new { a.K1, a.K2 } equals new { b.K1, b.K2 }`), `GroupJoin` + `DefaultIfEmpty()` (compiled to native Polars **Left Outer Join**), Cross Join (multiple `from` / `for` clauses), `Zip`, `Concat`, `Union`, `Distinct`, `DistinctBy`, `Except`, `Intersect`.
   - Specialized Polars joins on `IQueryable`: `.JoinAsOf(...)`, `.SemiJoin(...)`, `.AntiJoin(...)`.
4. **GroupBy, Having, Window Functions & Correlated Subqueries**:
   - `GroupBy` followed by `.Where(g => g.Sum(...) > X)` (compiled to native Polars `Having`), `.Select(...)` with `Count`, `LongCount`, `Sum`, `Average`, `Min`, `Max`, `First`, `Last`, `MinBy`, `MaxBy`, `.ToList()` / `.ToArray()` (compiled to native Polars `Implode` list aggregation!).
   - Correlated subqueries inside `Select` or `Where` (e.g. `orders.Where(o => o.CustomerId == c.Id).Sum(o => o.Amount)`) are automatically decorrelated into native Polars joins!

---

## 3. Execution Modes & Troubleshooting `PolarsPushdownException`

When you call `query.Explain()`, Polars.NET classifies the plan into one of three modes:
1. **`[Polars.NET Query Plan - 100% Native Pushdown]`**: Every stage runs inside Rust Polars. Calling `.ToDataFrame()` never allocates a single `.NET` row object.
2. **`[Polars.NET Query Plan - Hybrid Execution (Partial Pushdown)]`**: All filtering/joining/grouping runs in Rust Polars, and only the final `.Select(...)` projection invokes custom `.NET` code over the high-performance Arrow row cursor.
3. **`[Polars.NET Query Plan - Full Fallback to In-Memory LINQ]`**: Fallback to LINQ-to-Objects.

### Default Strict Policy (`ClientEvaluationPolicy.Strict`)
By default, Polars.NET **allows** client evaluation in the final `.Select(...)` (`Hybrid Execution`), but **throws `PolarsPushdownException`** if an intermediate stage (`Where`, `GroupBy`, `Join`, `OrderBy`) contains an unpushed custom method/UDF.

When `PolarsPushdownException` is thrown:
- **Option 1 (Best Performance)**: Replace the custom helper in `Where`/`GroupBy` with standard BCL methods (`string.*`, `Math.*`, `Regex.IsMatch`) so Polars can push it down natively.
- **Option 2 (Hybrid Projection)**: Keep `Where`/`GroupBy` pure and move custom C#/F# formatting or method calls into the final `.Select(...)` / `select ...` clause.
- **Option 3 (Explicit Client Evaluation)**:
  - In C#: insert `.AsEnumerable()` right before the client-only `.Where(x => MyCustomUdf(x))`.
  - In F#: insert `.AsSeq()` or `|> Query.asSeq` before `Seq.filter`.
  - Or relax the policy globally:
    - C#: `PolarsConfig.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow);` (or `Warn`)
    - F#: `Config.clientEvaluationPolicy <- ClientEvaluationPolicy.Allow`
    - Env var: `POLARS_CLIENT_EVAL=allow`

---

## 4. Extended LINQ Operators & The Escape Hatch Strategy

Standard LINQ covers common relational operations (`Where`, `Select`, `GroupBy`, `Join`, `OrderBy`). For analytics operations beyond standard C# LINQ syntax:

### Step 1: Try Polars.NET LINQ Extension Methods
`using Polars.CSharp.Linq;` provides native query extensions that can be called directly on column expressions inside `.Select(...)`:

- **Window Functions (`.Over`)**:
  ```csharp
  // Calculate difference from departmental average salary:
  var query = df.AsQueryable<Employee>()
      .Select(e => new
      {
          e.Name,
          e.Salary,
          DeptAvg = e.Salary.Average().Over(e.DepartmentId),
          Diff = e.Salary - e.Salary.Average().Over(e.DepartmentId)
      });
  ```
- **Lead / Lag Shifts (`.Shift`)**:
  ```csharp
  // Compare today's price with yesterday's price:
  var query = df.AsQueryable<StockPrice>()
      .Select(s => new
      {
          s.Date,
          s.Close,
          PrevClose = s.Close.Shift(1),
          Diff = s.Close.Diff(1)
      });
  ```
- **Time-Series Interpolation (`.Interpolate`, `.InterpolateBy`)**:
  ```csharp
  var query = df.AsQueryable<SensorReading>()
      .Select(r => new
      {
          r.Timestamp,
          CleanVal = r.Value.Interpolate(InterpolationMethod.Linear)
      });
  ```
- **Ranking & Cumulative Aggregates**:
  - Ranking: `e.Score.Rank()` (supports `RankMethod.Dense`, `RankMethod.Min`, etc.)
  - Percent Change: `s.Price.PctChange(1)`
  - Cumulative: `x.Val.CumSum()`, `x.Val.CumMax()`, `x.Val.CumMin()`, `x.Val.CumProd()`, `x.Val.CumCount()`
  - Statistics: `x.Val.Std()`, `x.Val.Var()`, `x.Val.Median()`, `x.Val.Quantile(0.95)`

### Step 2: The Bidirectional Escape Hatch

When a pipeline contains operations that standard LINQ simply cannot express (e.g. specialized regex pivots, unnesting structs, Aho-Corasick multiple string replacement, or specialized IO scan configurations):

1. **LINQ to Polars API (`.ToLazyFrame()`)**:
   Compile the LINQ query pipeline directly to a native Polars `LazyFrame`, then use the full raw Polars API:
   ```csharp
   // 1. Build initial filtering/joining with comfortable LINQ:
   var baseQuery = df.AsQueryable<EventLog>()
       .Where(e => e.Level == "ERROR" && e.Timestamp >= startDate);

   // 2. Escape hatch to native LazyFrame for advanced Polars-specific operations:
   using LazyFrame lf = baseQuery.ToLazyFrame();
   using var finalDf = lf
       .WithColumns(Pl.Col("message").Str.ExtractAll(@"\[code:(\d+)\]").Alias("error_codes"))
       .Collect();
   ```

2. **Polars API to LINQ (`.AsQueryable<T>()`)**:
   Vice-versa, any Polars `LazyFrame` or `DataFrame` can be converted back into an `IQueryable<T>` at any time:
   ```csharp
   using var lf = LazyFrame.ScanParquet("large_data.parquet")
       .Filter(Pl.Col("status") == "active");

   // Resume querying with LINQ:
   var linqQuery = lf.AsQueryable<UserDto>()
       .GroupBy(u => u.Country)
       .Select(g => new { Country = g.Key, Count = g.Count() });
   ```

---

## 5. Polars.NET Exclusive: Strongly-Typed LINQ Merge (`TypedMerge`)

In addition to the fluent untyped API, `Polars.CSharp.Linq` provides a fully strongly-typed `Merge` operator that works with C# expression trees and lambdas over `IQueryable<T>`:

```csharp
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
    // 2. Matched condition -> Update
    .WhenMatched(
        (t, s) => s.Price > t.Price,
        then => then.Update(set => set.Set(t => t.Price, (t, s) => s.Price))
    )
    // 3. Not matched in target -> Insert new item from source
    .WhenNotMatchedByTarget(
        s => s.Price > 0.0,
        then => then.Insert()
    )
    // 4. Not matched in source -> Delete target item
    .WhenNotMatchedBySource(
        t => t.Category == "Seasonal",
        then => then.Delete()
    )
    .ToDataFrame(); // Or .ToLazyFrame(), .ToList()!
```

### In-Memory Collection Merge & Seamless Transposition
`TypedMerge` also extends `IEnumerable<TTarget>`, enabling ANSI SQL `MERGE` directly on plain in-memory `List<T>`:

```csharp
List<ProductItem> inMemoryTarget = GetTargetProducts();
List<ProductItem> incomingUpdates = GetSourceProducts();

// Start merge directly from an in-memory List<T>:
using DataFrame mergedDf = inMemoryTarget.Merge(incomingUpdates, t => t.Id, s => s.Id)
    .WhenMatched(then => then.Update(set => set.Set(t => t.Price, (t, s) => s.Price)))
    .WhenNotMatchedByTarget(then => then.Insert())
    .ToDataFrame();
```


