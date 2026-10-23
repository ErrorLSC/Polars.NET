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

