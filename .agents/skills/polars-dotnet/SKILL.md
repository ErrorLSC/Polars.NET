---
name: polars-dotnet
description: >-
  Idiomatic usage guide, architecture routing, and anti-hallucination reference for Polars.NET
  (Polars.CSharp, Polars.FSharp, and Polars.NET.Linq). Use this skill whenever writing, reviewing,
  migrating, or debugging Polars.NET code, DataFrame/LazyFrame/Expr/Series pipelines, C# LINQ
  queries, or F# query computation expressions.
---

# Polars.NET Usage & Best Practices Skill

Polars.NET provides high-performance, strongly-typed C# and F# bindings and a native LINQ / F# Query provider over the Rust Polars engine with zero-copy Apache Arrow interop.

> [!IMPORTANT]
> **Native Runtime Dependency (Different from Python!)**:
> Unlike Python (where `pip install polars` automatically bundles precompiled wheels), .NET separates managed API packages from native binaries. You **must** reference the native package matching your execution environment (e.g., `Polars.NET.Native.linux-x64`, `Polars.NET.Native.win-x64`, `Polars.NET.Native.osx-arm64`). See [references/interop.md](./references/interop.md#1-native-runtime-packages-different-from-python).

---

## 1. Entrypoint Routing (Choose by Developer Background)

### A. Coming from C# / .NET (Prefer LINQ)
Open `Polars.CSharp.Linq` to write standard C# LINQ queries that compile directly into native Rust Polars LazyPlans:
```csharp
using Polars.CSharp;
using Polars.CSharp.Linq;

using var df = DataFrame.ReadParquet("orders.parquet");

// Convert DataFrame or LazyFrame to IQueryable<T> (supports records, readonly record structs, POCOs, tuples)
var query = df.AsQueryable<OrderRecord>()
    .Where(o => o.Amount > 100.0 && o.CustomerName.StartsWith("A"))
    .GroupBy(o => o.CustomerName)
    .Select(g => new { Customer = g.Key, Total = g.Sum(x => x.Amount), Count = g.Count() })
    .OrderByDescending(x => x.Total);

// Inspect native Polars plan vs Hybrid/Fallback status
Console.WriteLine(query.Explain());

// Materialize back to Polars DataFrame/LazyFrame (zero object hydration!) or to .NET objects (.ToList() / .ToArray())
using DataFrame resultDf = query.ToDataFrame();
```
- **Standard BCL Mappings**: `string.*` (`Contains`, `StartsWith`, `ToUpper`, `Trim`, `Replace`, `Substring`, etc.), `Math.*`, `DateTime` / `DateOnly` / `TimeSpan` properties and arithmetic, `Regex.IsMatch`, window functions, subqueries (`Any`, `All`, `Count`, `Sum`), and multi-key `Join` / `GroupJoin` (`DefaultIfEmpty` Left Join) are natively translated.
- **Handling Unsupported UDFs (`PolarsPushdownException`)**:
  - By default, `ClientEvaluationPolicy.Strict` is active.
  - **Terminal `.Select(...)` projections** automatically support custom C# logic via Polars.NET's high-performance row cursor (`Hybrid Execution`).
  - If an **intermediate stage** (`Where`, `GroupBy`, `Join`, `OrderBy`) contains an untranslatable C# method/UDF, Polars.NET throws `PolarsPushdownException` with actionable remediation hints:
    1. Rewrite using mapped BCL methods or native `Expr` API.
    2. Move custom formatting/constructors into the final `.Select(...)`.
    3. Insert `.AsEnumerable()` before the client-only step, or set `PolarsConfig.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow)` (or env `POLARS_CLIENT_EVAL=allow`).
- **3-Tier Strategy for Operations Beyond Standard LINQ**:
  1. **Standard LINQ**: Use standard clauses (`Where`, `Select`, `GroupBy`, `Join`, `OrderBy`).
  2. **Polars LINQ Extensions**: For analytical operations not covered by standard LINQ syntax, use extensions inside `using Polars.CSharp.Linq;`:
     - Window functions: `.Over(e => e.DeptId)` / `.Over(partitionBy, orderBy)`
     - Time-series lag/lead: `x.Close.Shift(1)`
     - Differencing: `x.Close.Diff(1)`
     - Interpolation: `x.Value.Interpolate(InterpolationMethod.Linear)` / `.InterpolateBy(x.Time)`
     - Ranking & Stats: `x.Score.Rank()`, `x.Price.PctChange()`, `x.Val.CumSum()`, `x.Val.Std()`, `x.Val.Median()`.
  3. **Bidirectional Escape Hatch**:
     - **LINQ -> Polars API**: If an operation cannot be expressed in LINQ (e.g. specialized regex pivots, unnesting structs, Aho-Corasick matching), call `query.ToLazyFrame()` to retrieve the underlying `LazyFrame` and continue with the raw Polars DSL.
     - **Polars API -> LINQ**: Vice-versa, any `LazyFrame` or `DataFrame` can re-enter LINQ at any time via `lf.AsQueryable<T>()`!
- Deep dive: [references/linq-and-query-ce.md](./references/linq-and-query-ce.md)

---

### B. Coming from C# / Rust / Python (Prefer Expression DSL)
Choose one of two idiomatic import styles in C#:

1. **Python-like Alias Style (Recommended for clarity in large files)**:
   ```csharp
   using Polars.CSharp;
   using Pl = Polars.CSharp.Polars;
   using Cs = Polars.CSharp.Polars.Selectors;

   using var res = df.Lazy()
       .Filter(Pl.Col("age") >= 18 & Pl.Col("status") == "active")
       .WithColumns(
           (Pl.Col("salary") * 1.1).Alias("new_salary"),
           Cs.Numeric().FillNull(0)
       )
       .Collect();
   ```
2. **Rust-like Static Import Style (Concise DSL)**:
   ```csharp
   using Polars.CSharp;
   using static Polars.CSharp.Polars;
   using static Polars.CSharp.Polars.Selectors;

   using var res = df.Select(
       Col("name"),
       When(Col("score") >= 60).Then("Pass").Otherwise("Fail").Alias("grade")
   );
   ```
- Deep dive: [references/csharp-idioms.md](./references/csharp-idioms.md)

---

### C. Coming from F# (`Polars.FSharp`)
F# developers have three complementary styles that interoperate seamlessly:

1. **F# `query { }` Computation Expression (`open Polars.FSharp.Query`)**:
   ```fsharp
   open System.Linq
   open Polars.FSharp
   open Polars.FSharp.Query

   let q =
       query {
           for d in dfDepts.AsQueryable<Department>() do
           leftOuterJoin e in dfEmps.AsQueryable<Employee>() on (d.DeptId = e.DeptId) into empGroup
           for e in empGroup.DefaultIfEmpty() do
           where (d.DeptId > 10)
           sortBy d.DeptName
           select {| Dept = d.DeptName; Emp = if isNull (box e) then "None" else e.Name |}
       }
   use resDf = q.ToDataFrame() // Or q.ToList(), q.Explain()
   ```
   - Supports F# `Record`, `Anon Record {| ... |}`, `Tuple`, `option`, and `voption` seamlessly.
   - If a custom F# function cannot be pushed down in `where`, insert `.AsSeq()` or `|> Query.asSeq` before client-side `Seq` operations.

2. **Functional Pipeline Modules (`pl`, `DataFrame`, `LazyFrame`, `Series`, `cs`)**:
   - Module functions are **camelCase** (`DataFrame.select`, `LazyFrame.withColumns`, `pl.col`, `pl.lit`, `pl.alias`).
   - Use **dot-prefixed comparison & logical operators** on `Expr`: `.==`, `.!=`, `.>`, `.>=`, `.<`, `.<=`, `.&&`, `.||` (because bare `=`, `>`, `<` in F# return `bool`).
   ```fsharp
   open Polars.FSharp

   use res =
       df
       |> DataFrame.filter (pl.col "age" .>= pl.lit 18 .&& (pl.col "dept" .== pl.lit "IT"))
       |> DataFrame.withColumns [
           (pl.col "salary" * pl.lit 1.15) |> pl.alias "boosted"
       ]
   ```

3. **Member Methods (When Module Functions Don't Cover an Option or Sub-namespace)**:
   - Sub-namespaces (`.Str`, `.Dt`, `.List`, `.Array`, `.Struct`, `.Cat`, `.Bin`, `.Meta`, `.Name`) and methods with many optional parameters (default arguments) are exposed as **PascalCase member methods** on `Expr`, `DataFrame`, `LazyFrame`, and `Series`.
   - Whenever a function or optional overload isn't in the `pl.*` / `DataFrame.*` module, call the member method directly:
     ```fsharp
     (pl.col "ts").Str.ToDatetime("%Y-%m-%d").Alias "date"
     df.Pivot(index = ["date"], on = ["type"], values = ["val"], aggregateFunction = PivotAgg.Sum)
     ```
- Deep dive: [references/fsharp-idioms.md](./references/fsharp-idioms.md)

---

### D. Coming from Python Polars (Key Differences & `Into*` Union Types)
1. **PascalCase Naming**: Python `snake_case` maps to `.NET` `PascalCase` (`with_columns` -> `WithColumns`, `fill_null` -> `FillNull`, `group_by` -> `GroupBy`, `collect` -> `Collect`).
2. **Sub-namespaces are Properties, Not Methods**:
   - Write `expr.Str.Contains("x")`, `expr.Dt.Year()`, `expr.List.Get(0)`, `expr.Name.Prefix("p_")` (NOT `expr.str().contains()`).
3. **`Into*` and `*OrExpr` Implicit Union Types (Python Ergonomics with Static Typing)**:
   Polars.CSharp uses `readonly struct` implicit conversion wrappers so methods accept strings, primitives, `Expr`, `Selector`, or `Type` without manual wrapping:

   | Wrapper Type | Where Used | Implicitly Accepts |
   | :--- | :--- | :--- |
   | `IntoExprColumn` | `Select`, `GroupBy`, `Sort`, `Filter` | `string` (parsed as **column name** `Pl.Col(s)`!), `Expr`, `Selector`, `Series`, `DataType`, `Type`, numbers/dates/arrays (as `Lit`) |
   | `IntoExpr` | `When/Then/Otherwise`, `FillNull`, `Lit` | `string` (parsed as **string literal** `Pl.Lit(s)`!), `Expr`, `Series`, numbers, `bool`, `DateTime`, `DateOnly`, `TimeSpan` |
   | `IntoSelector` | `Drop`, `Unique`, `Unpivot`, `df[...]` | `Selector` (`Cs.*`), `string` (column name), `DataType`, `Type` (e.g. `typeof(int)`), column-selection `Expr` |
   | `StringOrExpr` | `Expr.Str.*` (`Contains`, `Replace`, etc.) | `string` pattern/literal OR `Expr` |
   | `IntOrExpr` | `Gather`, `Slice`, `Shift`, `Reshape` | `int` / `long` OR `Expr` |
   | `IntoDuration` / `DurationOrExpr` | `GroupByDynamic`, `Rolling`, `Dt.Truncate` | Polars duration `string` (`"1d"`, `"2h30m"`, `"1mo"`) OR `.NET` `TimeSpan` (and `Expr` for `DurationOrExpr`) |
   | `IntoDataTypeExpr` | `Cast`, `DataTypeExpr` | `DataType`, `DataTypeExpr`, or `.NET` `Type` (e.g. `expr.Cast(typeof(double))`) |
   | `IntoSchema` | Schema validation / IO | `PolarsSchema`, `DataFrame`, `LazyFrame`, `Dictionary<string, DataType>`, `(string, DataType)[]` |

   > [!IMPORTANT]
   > **`IntoExprColumn` vs `IntoExpr` for `string`**:
   > In `df.Select("a", "b")`, `"a"` is an `IntoExprColumn` so it means `Pl.Col("a")`.
   > In `expr.FillNull("unknown")` or `Pl.When(...).Then("yes").Otherwise("no")`, the parameter is `IntoExpr` so `"unknown"` means `Pl.Lit("unknown")`!
   > In binary operators like `Pl.Col("a") == "foo"`, `"foo"` is compared as a literal value; to compare two columns, write `Pl.Col("a") == Pl.Col("b")`.
4. **First-Class .NET Collection Protocols**:
   - **`DataFrame` implements `IEnumerable<Series>` (`seq<Series>`)**:
     - **C# 12 Collection Expressions**: Directly build DataFrames using `[ ... ]` syntax (via `[CollectionBuilder]`):
       ```csharp
       DataFrame df = [
           Series.From("id", [1, 2, 3]),
           Series.From("name", ["Alice", "Bob", "Charlie"])
       ];
       ```
     - **Column Iteration**: Directly enumerate columns via `foreach (Series col in df)` or in F# `for col in df do ...` / `df |> Seq.iter ...`.
   - **`PolarsSchema` implements `IReadOnlyDictionary<string, DataType>`**:
     - Direct dictionary indexing: `schema["age"]` -> `DataType.Int32`.
     - Standard dictionary members: `schema.ContainsKey("name")`, `schema.Count`, `schema.Keys`, `schema.Values`.
     - KeyValuePair enumeration: `foreach (var (name, dtype) in schema)` or F# `schema |> Seq.iter ...`.

---

## 2. Resource, Ownership & Lifecycle Rules
- **`IDisposable` Native Handles**: `DataFrame`, `LazyFrame`, `Series`, `PolarsSchema`, and `DataType` wrap native Rust handles and implement `IDisposable`. Always use `using var` in C# and `use` in F#.
- **`Expr` is an Immutable, Freely Reusable Expression Tree**:
  - `Expr` in both C# and F# is immutable and non-destructive.
  - Calling methods (`.Clip()`, `.Alias()`, `.Filter()`), applying operators, or chaining sub-namespace operations (`.Dt`, `.Str`, `.Name`, etc.) clones handles internally so the source expression is never consumed or invalidated.
  - You can safely define an `Expr` once (e.g. `var col = Pl.Col("price");`) and reuse it across multiple selections, filters, and aggregations.
- **`LazyFrame` is an Immutable Query Plan**:
  - Unlike Rust's moving `collect(self)`, Polars.NET's `LazyFrame` is **non-consuming and safe to reuse multiple times**!
  - Calling `.Collect()`, `.CollectAsync()`, or any `.Sink*()` method (`SinkParquet`, `SinkCsv`, etc.) clones the plan handle internally. The original `LazyFrame` remains fully valid and can be collected, branched, or inspected repeatedly.
  - Lifecycle is governed cleanly by standard .NET `using` / `use` scopes or GC finalization:
    ```csharp
    // Safe to branch or reuse LazyFrame in C#:
    using var baseLf = LazyFrame.ScanParquet("events.parquet");
    using var df1 = baseLf.Filter(Pl.Col("status") == "success").Collect();
    using var df2 = baseLf.Filter(Pl.Col("status") == "failed").Collect(); // baseLf is NOT consumed!
    ```
    ```fsharp
    // Safe to branch or reuse LazyFrame in F#:
    use baseLf = LazyFrame.ScanParquet "events.parquet"
    use df1 = baseLf |> LazyFrame.filter (pl.col "status" .== pl.lit "success") |> LazyFrame.collect
    use df2 = baseLf |> LazyFrame.filter (pl.col "status" .== pl.lit "failed") |> LazyFrame.collect
    ```
- **Asynchronous Streaming (`IAsyncEnumerable`)**:
  - `DataFrame` and `LazyFrame` stream rows as cooperative, cancellation-aware `IAsyncEnumerable<T>` via `.ToAsyncEnumerable<T>()` without thread starvation.
  - `LazyFrame.CollectBatchesAsync()` emits native DataFrame batches as soon as computed by Rust.
  - LINQ queries support `query.AsAsyncEnumerable()` and `IAsyncEnumerable<T>.ToDataFrameAsync()`.
  - F# supports `df.RowsAsync<'T>()`, `lf.RowsAsync<'T>()`, and `RowsAsync.*` pipeline operators (`toList`, `fold`, `filter`, `map`).
- **Polars.NET Exclusive: SQL MERGE / Upsert**:
  - Full ANSI SQL `MERGE` semantics natively integrated into query planning (exclusive to Polars.NET, not in Python).
  - C# Fluent: `targetDf.Merge(sourceDf, "Id").WhenMatchedUpdate(...).Execute()`.
  - C# Strongly-Typed LINQ: `targetQuery.Merge(sourceQuery, t => t.Id, s => s.Id).WhenMatched((t, s) => ...).ToDataFrame()`.
  - F# Pipeline Functions: `target.Merge(source, pl.col "id") |> Merge.whenMatchedUpdateAll |> Merge.executeEager`.

---

## 3. Detailed Reference Files
Load these on demand depending on your task:
- **Ecosystem Interoperability (Native Runtimes, Spans, Arrow, ADBC, ADO.NET, ML.NET DataView, Tensors)**: [references/interop.md](./references/interop.md)
- **C# DSL, Indexers, IO, Arrow, and UDFs**: [references/csharp-idioms.md](./references/csharp-idioms.md)
- **F# Modules, Operators, and Hybrid OOP/FP Patterns**: [references/fsharp-idioms.md](./references/fsharp-idioms.md)
- **LINQ Provider & F# Query CE Deep Dive**: [references/linq-and-query-ce.md](./references/linq-and-query-ce.md)
- **Full Generated API Markdown Reference**:
  - C# API Pages: `docs/api/csharp_generated/` (index in [toc.yml](../../docs/api/csharp_generated/toc.yml))
  - F# API Pages: `docs/api/fsharp_generated/` (index in [toc.yml](../../docs/api/fsharp_generated/toc.yml))

