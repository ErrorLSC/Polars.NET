# Polars.NET LINQ (`Polars.CSharp.Linq`) & F# Query CE (`Polars.FSharp.Query`)

Polars.NET includes a full expression-tree compiler (`Polars.NET.Linq`, built on `Kaminari.Samidare`) that compiles C# `IQueryable<T>` pipelines and F# `query { }` computation expressions into a logical plan (IR), optimizes it, and runs it as a native Rust Polars `LazyFrame`, with query plan caching, subquery decorrelation, and zero-allocation Arrow row materialization. All 22 TPC-H and 99 TPC-DS queries are written as LINQ in the test suite and run without client-side fallback. Results follow .NET semantics, not SQL semantics, unless a setting says otherwise: see [Section 7](#7-semantics-cheat-sheet).

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
2. **Mapped BCL members**: see [Section 6](#6-mapped-bcl-members). Anything else that reads row values runs as a managed UDF.
3. **Joins & Set Operations**:
   - Inner `Join`, multi-key `Join` (`on new { a.K1, a.K2 } equals new { b.K1, b.K2 }`), `GroupJoin` + `DefaultIfEmpty()` (compiled to native Polars **Left Outer Join**), Cross Join (multiple `from` / `for` clauses), `Zip`, `Concat`, `Union`, `Distinct`, `DistinctBy`, `Except`, `Intersect`.
   - `.JoinAsOf(...)` on `IQueryable`. There is no `.SemiJoin(...)` or `.AntiJoin(...)` on `IQueryable`: write a semi-join as `.Where(a => others.Any(b => b.K == a.K))` and an anti-join as `.Where(a => !others.Any(b => b.K == a.K))`, or use `lf.Join(other, on, JoinType.Semi)` / `JoinType.Anti` on a `LazyFrame`. The .NET 11 `LeftJoin`, `RightJoin` and `FullJoin` operators are translated, with or without a result selector.
4. **GroupBy, Having, Window Functions & Correlated Subqueries**:
   - `GroupBy` followed by `.Where(g => g.Sum(...) > X)` (compiled to native Polars `Having`), `.Select(...)` with `Count`, `LongCount`, `Sum`, `Average`, `Min`, `Max`, `First`, `Last`, `MinBy`, `MaxBy`, `.ToList()` / `.ToArray()` (compiled to native Polars `Implode` list aggregation!).
   - Correlated subqueries inside `Select` or `Where` (e.g. `orders.Where(o => o.CustomerId == c.Id).Sum(o => o.Amount)`) are automatically decorrelated into native Polars joins!
5. **Zero-Compromise Pushdown Patterns**:
   - **Left Outer Join & Anti-Join (`where r == null`)**:
     In Left Joins (`group ... join ... into g from r in g.DefaultIfEmpty()`), write natural entity null checks:
     ```csharp
     // Idiomatic anti-join (fully supported natively, 0 warnings):
     var antiJoin = from s in sales
                    join r in returns on s.OrderId equals r.OrderId into rGroup
                    from r in rGroup.DefaultIfEmpty()
                    where r == null
                    select s;
     ```
     The engine automatically resolves `r == null` to physical column `is_null()` checks. Never cast non-nullable properties to nullable (e.g. `(long?)r.OrderId == null`), which triggers Roslyn CS0472 warnings.
   - **Post-GroupBy Window Calculations (`let` Over Aggregates)**:
     Chain window functions directly over grouping aggregates in a single comprehension:
     ```csharp
     var query = from s in sales
                 group s by new { s.Category, s.Year, s.Month } into g
                 let sum_sales = g.Sum(x => x.Price)
                 let avg_monthly = sum_sales.Average().Over(new { g.Key.Category, g.Key.Year })
                 let prev_month = sum_sales.Shift(1).Over(g.Key.Category)
                 select new { g.Key.Category, sum_sales, avg_monthly, prev_month };
     ```
   - **Inline Subquery Comparisons**:
     Write inline scalar subqueries directly without manual in-memory pre-computation:
     ```csharp
     var query = from s in sales
                 where s.Amount > 0.9 * benchmarkSales.Average(x => (double?)x.Amount)
                 select s;
     ```

---

### Native Managed UDF Pushdown (Where, Select, GroupBy & Group Aggregations)

Polars.NET LINQ bridges custom C# methods and lambdas directly into native Polars execution graphs via zero-copy Apache Arrow chunk adapters (`ArrowUdfBatchAdapter`). Instead of aborting pushdown or falling back to client evaluation:

1. **Filter & Projection UDFs (`.Where(...)` & `.Select(...)`)**:
   - Single-column and multi-column methods are compiled into `Expr.map()` / `Expr.map_many()`:
   ```csharp
   // Pushes down CheckSecurity as a native vectorized arrow callback:
   var query = df.AsQueryable<Employee>()
       .Where(e => SecurityService.CheckSecurity(e.Name, e.Salary))
       .Select(e => new { e.Name, Score = CustomMath.ComputeScore(e.Age, e.Salary) });
   ```
2. **Group-Level Aggregation UDFs (`.GroupBy(...)`)**:
   - Custom reduction logic over group sequences, arrays, or lists compiles into native `Expr.apply()` / `Expr.apply_many()` with `FunctionOptions::groupwise()`:
   ```csharp
   var query = df.AsQueryable<Employee>()
       .GroupBy(e => e.Department)
       .Select(g => new
       {
           Dept = g.Key,
           // Custom reduction over IEnumerable<T>:
           Spread = g.Select(x => x.Salary).CustomSpread(),
           // Custom multi-column weighted aggregation:
           WeightedAvg = Stats.WeightedAverage(g.Select(x => x.Salary), g.Select(x => x.Age)),
           // Array / List aggregations:
           Peak = Stats.PeakToTrough(g.Select(x => x.Salary).ToArray()),
           Median = Stats.Median(g.Select(x => x.Salary).ToList()),
           // LINQ Aggregate folding:
           Total = g.Select(x => x.Salary).Aggregate((acc, x) => acc + x),
           // Whole group object UDF:
           Payroll = PayrollService.TotalPayroll(g)
       });
   ```
   - Group UDFs can be freely composed with standard aggregations (`Count()`, `Sum()`, etc.) and arithmetic expressions (e.g. `CustomAgg(...) * 1.5 + 100`).

3. **Rules an agent must follow**:
   - A UDF is any method or delegate that reads one or more row values and returns one value. Single-column and multi-column-to-single-value UDFs are both pushed down, in `Where`, `Select`, `OrderBy` and group keys. Pushdown is on by default; `Pl.Config.SetDisallowUdfPushdown(true)` turns it off.
   - Pass columns, not the whole row. `Where(s => IsValid(s))` throws `PolarsPushdownException` under the Strict policy, while `Where(s => IsValid(s.Sku, s.Qty))` is pushed down.
   - The return type must map to an Arrow type. The method must be thread safe, because Polars may call it from several threads.
   - On the right of `&&`, `||`, `??`, or in a `?:` branch, a UDF runs only on the rows that reach it, so null guards work as in C#.
   - Prefer a mapped BCL member: a UDF is slower than a native expression, and each UDF site prints a warning once.

---

## 3. Execution Modes & Troubleshooting `PolarsPushdownException`

When you call `query.Explain()`, Polars.NET classifies the plan into one of the following modes:
1. **`[Query Plan - 100% Native Pushdown]`**: Every stage runs inside Rust Polars. Calling `.ToDataFrame()` never allocates a single `.NET` row object.
   - **`[Query Plan - 100% Native Pushdown (with C# UDF Callback)]`**: When custom C# methods/delegates are present, the plan maintains 100% native pushdown in Rust while explicitly documenting the pushed-down Arrow UDF callbacks.
2. **`[Query Plan - Hybrid Execution (Partial Pushdown)]`**: All filtering/joining/grouping runs in Rust Polars, and only the final `.Select(...)` projection invokes custom `.NET` code over the high-performance Arrow row cursor.
3. **`[Query Plan - Full Fallback to In-Memory LINQ]`**: Fallback to LINQ-to-Objects.

### Default Strict Policy (`ClientEvaluationPolicy.Strict`)
By default, Polars.NET **allows** custom UDFs in `Where`, `Select`, and `GroupBy` via native Arrow callbacks, and supports custom client projection in final `.Select(...)` (`Hybrid Execution`).
It **throws `PolarsPushdownException`** only when:
- An intermediate stage contains an untranslatable blackbox operation (e.g. an opaque entity reference that cannot be decomposed into columns).
- UDF pushdown is disabled with `Pl.Config.SetDisallowUdfPushdown(true)`.

When `PolarsPushdownException` is thrown:
- **Option 1 (Best Performance)**: Rewrite using mapped BCL methods (`string.*`, `Math.*`, `Regex.IsMatch`) or column-based UDF signatures.
- **Option 2 (Hybrid Projection)**: Move custom formatting/constructors into the final `.Select(...)` clause.
- **Option 3 (Explicit Client Evaluation)**:
  - In C#: insert `.AsEnumerable()` right before the client-only step.
  - In F#: insert `.AsSeq()` or `|> Query.asSeq` before `Seq.*`.
  - Or relax the policy globally:
    - C#: `Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow);` (or `Warn`)
    - F#: `Config.withConfig [ Config.clientEvaluationPolicy ClientEvaluationPolicy.Allow ] (fun () -> ...)`
    - Env var: `POLARS_CLIENT_EVAL=allow`

---

## 4. Extended LINQ Operators & The Escape Hatch Strategy

Standard LINQ covers common relational operations (`Where`, `Select`, `GroupBy`, `Join`, `OrderBy`). For analytics operations beyond standard C# LINQ syntax:

### Step 1: Try Polars.NET LINQ Extension Methods
`using Polars.CSharp.Linq;` provides native query extensions that can be called directly on queries and column expressions:

- **Multidimensional Grouping (`.RollUp()`, `.Cube()`, `.GroupingSets()`) & `.Grouping()`**:
  Generates hierarchical subtotals and grand totals across grouped dimensions natively in Rust Polars without in-memory grouping:
  - `.RollUp(...)` / `.Rollup(...)`: Hierarchical rollup across dimensions (e.g. `(A, B) -> (A, null) -> (null, null)`).
  - `.Cube(...)`: Full power-set combinations across all dimensions (e.g. `(A, B) -> (A, null) -> (null, B) -> (null, null)`).
  - `.GroupingSets(sets, ...)`: Explicit grouping sets across dimensions.
  
  Both **Scheme A** (direct result selector) and **Scheme B** (modifier followed by `.Select(...)`) are supported:
  ```csharp
  // Scheme A (Direct Result Selector):
  var rollupQuery = df.AsQueryable<SalesRecord>()
      .GroupBy(x => new { x.Dept, x.Team })
      .RollUp((key, g) => new
      {
          key.Dept,
          key.Team,
          TotalAmount = g.Sum(x => x.Amount),
          Spread = g.Select(x => x.Amount).CustomSpread(), // Custom UDF supported!
          Count = g.Count(),
          // Use .Grouping() to detect aggregated/subtotal levels (1 = aggregated, 0 = detailed):
          G_Dept = key.Dept.Grouping(),
          G_Team = key.Grouping(k => k.Team)
      });

  // Scheme B (Modifier followed by Select):
  var cubeQuery = df.AsQueryable<SalesRecord>()
      .GroupBy(x => new { x.Dept, x.Team })
      .Cube()
      .Select(g => new
      {
          g.Key.Dept,
          g.Key.Team,
          TotalAmount = g.Sum(x => x.Amount),
          Spread = TestUdfMethods.CalculateRange(g.Select(x => x.Amount)),
          G_Dept = g.Key.Dept.Grouping()
      });
  ```

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

### Full UDF Pushdown Support in `TypedMerge`
`TypedMerge` natively pushes down custom C# methods and delegates directly into the Polars execution plan:
- **Match Conditions**: Custom UDFs can be used in `WhenMatched((t, s) => ShouldUpdate(t.Price, s.Price))`, `WhenNotMatchedByTarget(s => IsEligible(s.Category, s.Price))`, and `WhenNotMatchedBySource(t => ShouldRetire(t.Category))`.
- **Value Setters**: Custom UDFs can calculate updated values `set.Set(t => t.Price, (t, s) => BlendPrice(t.Price, s.Price))` or inserted values `set.Set(t => t.Price, s => DiscountPrice(s.Price))` without full table materialization.
- **Re-entering LINQ**: Calling `.AsQueryable()` on `TypedMergeBuilder` seamlessly compiles the merge into an `IQueryable<T>` for further LINQ pipelines (`.Where(...)`, `.OrderBy(...)`, `.Select(...)`).

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

---

## 6. Mapped BCL Members

Each member below compiles to a native expression. Verified against LINQ to Objects.

| .NET member | Notes |
| :--- | :--- |
| `s.Length`, `s[i]` | `Length` counts characters |
| `Contains`, `StartsWith`, `EndsWith` (`string` or `char`, optional `StringComparison`) | Only the `*IgnoreCase` comparisons change the result; the comparison must be a constant |
| `IndexOf(value[, start[, count]][, StringComparison])`, `IndexOfAny(chars[, start[, count]])` | Returns -1 when absent |
| `ToUpper()`, `ToLower()`, `ToUpperInvariant()`, `ToLowerInvariant()` | No culture overloads |
| `Substring(start[, length])`, `Remove(start[, count])` | |
| `Replace(old, new[, StringComparison])` | `string` or `char` operands |
| `Trim`, `TrimStart`, `TrimEnd` (optional `char` or `char[]`), `PadLeft`, `PadRight` (optional constant pad) | |
| `Split(sep[, count][, StringSplitOptions.None])`, `Split(sep)[i]`, `Normalize([form])`, `CompareTo(other)` | The separator must be a constant, and the count a constant up to 32. Other options or larger counts run as a managed UDF |
| `Equals(other[, StringComparison])`, `string.Equals(a, b[, StringComparison])` | |
| `string.IsNullOrEmpty`, `string.IsNullOrWhiteSpace` | |
| `a + b`, `string.Concat(a, b, ...)`, interpolation, `string.Format(constantFormat, ...)`, `string.Join(constantSep, items)` | A null operand reads as `""`. A format with an alignment or specifier (`{0,8}`, `{0:F2}`) or escaped braces runs as a managed UDF, as does `string.Concat(x)` with one non-string argument |
| `char.ToUpper(c)`, `char.ToLower(c)` and the invariant forms | |
| `Encoding.UTF8.GetByteCount(s)` | Any other `Encoding` runs as a managed UDF |
| `Math` / `MathF`: `Abs`, `Ceiling`, `Floor`, `Truncate`, `Sign`, `Round` (digits, `MidpointRounding`), `Min`, `Max`, `Clamp`, `Sqrt`, `Cbrt`, `Exp`, `Log`, `Log2`, `Log10`, `Sin`, `Cos`, `Tan`, `Asin`, `Acos`, `Atan`, `Atan2`, `Sinh`, `Cosh`, `Tanh`, `Pow`, `PI`, `E`, `Tau` | `MathF` results are `Float64` columns |
| `BitOperations.PopCount`, `LeadingZeroCount`, `TrailingZeroCount` | |
| `decimal.Round`, `Floor`, `Ceiling`, `Truncate`, `ToDouble` and the other `decimal.ToXxx` conversions; `d.ToString()`, `d.ToString("F2")` | Only `F` formats are native |
| `int`, `long`, `short`, `byte`, `sbyte`, `uint`, `ulong`, `ushort`, `decimal`, `double`, `bool` `.Parse(s)` | A failed parse gives null |
| `Convert.ToInt16/32/64`, `ToUInt16/32/64`, `ToByte`, `ToSByte`, `ToDecimal`, `ToDouble`, `ToSingle`, `ToBoolean` over a string; `Convert.ToInt32(s, base)`, `Convert.ToInt64(s, base)`; `ToHexString`, `FromHexString`, `ToBase64String`, `FromBase64String` | |
| Numeric casts `(int)x`, `(double)x`, ... | Follow `Query Semantics` (wrap or saturate) |
| `DateTime`, `DateOnly`, `TimeOnly`, `DateTimeOffset`: `Year`, `Month`, `Day`, `DayOfYear`, `DayOfWeek`, `Hour`, `Minute`, `Second`, `Millisecond`, `Microsecond`, `Nanosecond`, `Date`, `TimeOfDay` | |
| `AddDays`, `AddHours`, `AddMinutes`, `AddSeconds` | The amount must be a constant |
| `TimeSpan`: `TotalDays`, `TotalHours`, `TotalMinutes`, `TotalSeconds`, `TotalMilliseconds`, `TotalMicroseconds`, `TotalNanoseconds`, `Days`, `Hours`, `Minutes`, `Seconds`, `Milliseconds`, `Microseconds`, `Nanoseconds`, also on a date subtraction | |
| `ToString(constantFormat)`, `DateTime.Parse`, `DateTime.ParseExact`, `DateOnly.Parse`, `DateOnly.ParseExact`, `TimeOnly.Parse`, `TimeOnly.ParseExact`, `DateTime.IsLeapYear` | |
| `Regex.IsMatch`, `Regex.Count`, `Regex.Split`, `Regex.Replace`, `Regex.Match(...).Success`, `.Value`, `.Groups[i].Value`, `.Groups["name"].Value`, `Regex.Matches(...).Count`, `Regex.Matches(...).Select(m => m.Value)`, `Regex.Escape` | Static or instance form, with a constant pattern. Any other `Matches` selector runs as a managed UDF. `RegexOptions.IgnoreCase`, `Multiline`, `Singleline` and `IgnorePatternWhitespace` are honored. A pattern the Rust engine rejects, such as one with lookaround, runs as a managed UDF |
| `JsonDocument.Parse(s).RootElement.GetProperty(k)...GetString()/GetInt32()/GetInt64()/GetDouble()/GetSingle()/GetDecimal()/GetBoolean()/GetInt16()/GetByte()`, `JsonNode.Parse(s)["k"]`, `(int)node`, `node.GetValue<T>()`, `JsonSerializer.Deserialize<T>(s).Member`, `JsonSerializer.Serialize(value)` | Paths must be constants; serializer options are not honored |
| List columns: `arr[i]`, `Length`, `Count`, `Count()`, `Contains(x)`, `First()`, `Last()`, `ElementAt(i)`, `Sum()`, `Average()`, `Min()`, `Max()`, `Distinct()`, `Reverse()`, `Order()`, `OrderDescending()`, `OrderBy(x => x)`, `OrderByDescending(x => x)`, `Any()`, `Any(pred)`, `All(pred)`, `Select(selector)`, `Take(n)`, `TakeLast(n)`, `Skip(n)`, `string.Join(sep, list)` | `Union`, `Intersect` and `Except` are native only between two list columns; with an in-memory operand they run on the client |
| `collection.Contains(x.Id)` over an in-memory collection | Compiles to an `IN` predicate |
| `SelectMany(x => x.Tags)` | Compiles to `Explode` |
| Group aggregates: `Sum`, `Average`, `Min`, `Max`, `Count`, `LongCount`, `First`, `FirstOrDefault`, `Last`, `LastOrDefault`, `ElementAt`, `MinBy(...).Member`, `MaxBy(...).Member`, `ToList`, `ToArray`, `string.Join(sep, g.Select(...))`, composed with `Where`, `Select`, `OrderBy`, `ThenBy`, `Take`, `Skip`, `Distinct`, `DistinctBy` | |
| `enum.ToString()`, `enum.Equals(other)`, `enum.HasFlag(flag)`, `(int)e`, `(Enum)i` | |
| `Nullable<T>.Value`, `.HasValue`, `x == null`, `x != null`, `a ?? b`, `x ? a : b` | |
| Operators `+ - * / % && \|\| ! & \| ^`, `<<` and `>>` with a constant count, comparisons | Follow `Query Semantics` |

### Not Native: Managed UDF or Client Evaluation

These members are not translated. Those that take row values run as managed UDFs by default, and the others follow the client evaluation policy:

* **Managed UDF**: `AddMonths`, `AddYears`, `AddMilliseconds`, `AddDays(columnValue)`, `int.ToString()`, `double.ToString(format)`, `double.IsNaN`, `float.Parse`, `Guid.Parse`, `char.IsDigit` and the other `char.IsXxx`, `Count(predicate)` and `Sum(selector)` over a list column.
* **Client evaluation**: a method that takes the whole row object, and any other expression with no translation and no column inputs. Under the Strict policy it throws `PolarsPushdownException` in `Where`, and runs on the client in a final `Select`.

---

## 7. Semantics Cheat Sheet

The normative source is `kernel/Samidare/IR/SEMANTICS.md`, published as [LINQ Semantics Contract](https://errorlsc.github.io/Polars.NET/articles/linq-semantics). When generating or reviewing a query, assume:

- **Nulls follow C#**: `a == b` is true when both are null, and a lifted `<` / `>` with a null operand is false. Explicit `join` never matches null keys, and a composite key with a null component never matches. `UseRelationalNulls` switches comparisons to SQL semantics.
- **NaN follows .NET**: a comparison with NaN is false, `!=` is true, `OrderBy` places NaN before every number, `Min` returns NaN as soon as one value is NaN, and `Max` skips NaN unless all values are NaN.
- **Integer `/` and `%` truncate** toward zero. Integer or decimal division by zero gives null, where .NET throws.
- **`Sum` over `int` / `long` and `Average` over `long` are checked**: a total out of range throws `OverflowException`. The check is on the total, not on partial sums. `decimal` sums are not checked.
- **Casts**: integer narrowing wraps (`(byte)300 == 44`). Floating to integer saturates for every target type, following .NET 11 (`(int)double.NaN == 0`).
- **Decimals** have fixed scale: division keeps 18 fractional digits, and `ToString()` shows the column's scale.
- **Strings sort ordinally**, by code point, not by culture. An operator overload that takes a comparer is not translated: it follows the client evaluation policy.
- **Concat, Format and Join read null as `""`.**
- **Where .NET would throw, the query gives null** (index out of range, failed parse, member on null, as in EF Core). A null that reaches a non-nullable result member throws `InvalidOperationException` naming the member and column. Declare the member nullable (`int?`, or `option` in F#) to accept it.
- **`First`, `Single`, `ElementAt` over a subquery** raise the same exceptions as LINQ.
- **Order**: `Sort` is stable unless `AllowUnstableOrdering` is set. `Filter`, `Project`, `Union` keep order. Inner and left joins keep left order, and cross joins have none.
- **Outer joins**: an unmatched class side is `null`, and an unmatched struct side is `default(T)`.
- **`DateTimeOffset`** is stored as a UTC instant, so `Offset` is zero.

---

## 8. Query Settings

The LINQ compiler reads these settings when it compiles a query. In C#, `Pl.Config.SetXxx(...)` applies a setting immediately, and `.BeginScope()` restores the previous values when the scope ends. In F#, `Config.withConfig [ ... ] (fun () -> ...)` applies settings around a function.

| Setting | C# | F# | Environment variable | Default |
| :--- | :--- | :--- | :--- | :--- |
| Client evaluation policy | `SetClientEvaluationPolicy(policy)` | `Config.clientEvaluationPolicy policy` | `POLARS_CLIENT_EVAL` | `Strict` |
| Disallow UDF pushdown | `SetDisallowUdfPushdown(bool)` | `Config.disallowUdfPushdown bool` | `POLARS_DISALLOW_UDF_PUSHDOWN` | `false` |
| Relational (SQL) nulls | `SetUseRelationalNulls(bool)` | `Config.useRelationalNulls bool` | `POLARS_USE_RELATIONAL_NULLS` | `false` |
| Unstable ordering | `SetAllowUnstableOrdering(bool)` | `Config.allowUnstableOrdering bool` | `POLARS_ALLOW_UNSTABLE_ORDERING` | `false` |
| Query plan cache | `SetQueryCacheEnabled(bool?)`, `SetQueryCacheCapacity(int?)` | `Config.queryCacheEnabled`, `Config.queryCacheCapacity` | `POLARS_QUERY_CACHE_ENABLED`, `POLARS_QUERY_CACHE_CAPACITY` | Enabled |
| Column statistics for join ordering | None | None | `POLARS_COLLECT_COLUMN_STATISTICS` | Enabled |

### `UseRelationalNulls`

By default, `==` follows C#: two nulls are equal. With relational nulls, as with EF Core's `UseRelationalNulls()`, a comparison with a null operand is null and the row is filtered out. The plan is also simpler, because no null compensation is needed.

```csharp
using Pl = Polars.CSharp.Polars;

public class Reading
{
    public int Id { get; set; }
    public string? A { get; set; }
    public string? B { get; set; }
    public int? V { get; set; }
}

Reading[] rows =
[
    new() { Id = 1, A = "x",  B = "x",  V = 10 },
    new() { Id = 2, A = "x",  B = "y",  V = null },
    new() { Id = 3, A = null, B = "y",  V = 30 },
    new() { Id = 4, A = null, B = null, V = null },
];
using var df = DataFrame.FromRows(rows);

var csharp = df.AsQueryable<Reading>().Where(x => x.A == x.B).Select(x => x.Id).ToList();   // [1, 4]
var notTen = df.AsQueryable<Reading>().Where(x => x.V != 10).Select(x => x.Id).ToList();    // [2, 3, 4]

using (Pl.Config.SetUseRelationalNulls(true).BeginScope())
{
    var sql = df.AsQueryable<Reading>().Where(x => x.A == x.B).Select(x => x.Id).ToList();         // [1]
    var sqlNotTen = df.AsQueryable<Reading>().Where(x => x.V != 10).Select(x => x.Id).ToList();    // [3]
}
```

```fsharp
let ids () = query { for p in df.AsQueryable<Pair>() do where (p.A = p.B); select p.Id } |> Seq.toList

let csharpStyle = ids ()                                                  // both None compares equal
let sqlStyle = Config.withConfig [ Config.useRelationalNulls true ] ids   // None never matches
```

### `AllowUnstableOrdering`

`OrderBy` is stable by default, as in LINQ to Objects, so the native plan sorts with `maintain_order: true`. If the order of tied rows does not matter, allow unstable sorts. As with SQL `ORDER BY`, tied rows may then come back in any order, and the optimizer may also reorder the joins and groupings beneath the sort. This can be noticeably faster on join-heavy queries, though a specific query can also get slower, so measure yours.

```csharp
public record Sale(string Sku, double Price, int Qty, string Region);

var q = sales.AsQueryable<Sale>().OrderBy(s => s.Region).Select(s => s.Sku);
Console.WriteLine(q.Explain());   // SORT BY [maintain_order: true] [col("Region")]

using (Pl.Config.SetAllowUnstableOrdering().BeginScope())
{
    var fast = sales.AsQueryable<Sale>().OrderBy(s => s.Region).Select(s => s.Sku);
    Console.WriteLine(fast.Explain());   // SORT BY [col("Region")]
    var skus = fast.ToList();            // Sorted by Region; order within a Region is unspecified
}
```

```fsharp
let skus =
    Config.withConfig [ Config.allowUnstableOrdering true ] (fun () ->
        query { for s in df.AsQueryable<Sale>() do sortBy s.Qty; select s.Sku } |> Seq.toList)
```

### `DisallowUdfPushdown`

UDF pushdown is allowed by default. With `DisallowUdfPushdown`, no managed callback enters the native plan, and each UDF follows the client evaluation policy instead:

```csharp
using (Pl.Config.SetDisallowUdfPushdown().BeginScope())
{
    var filter = df.AsQueryable<Sale>().Where(s => Scoring.IsHot(s.Sku, s.Qty));

    // Strict policy: running a UDF in Where now throws PolarsPushdownException.
    try { filter.ToList(); } catch (PolarsPushdownException e) { Console.WriteLine(e.Message); }

    // A UDF in the final projection runs on the client: [Query Plan - Hybrid Execution (Partial Pushdown)]
    var projection = df.AsQueryable<Sale>().Select(s => new { s.Sku, Total = Scoring.Total(s.Price, s.Qty, 0.2) }).ToList();

    using (Pl.Config.SetClientEvaluationPolicy(ClientEvaluationPolicy.Allow).BeginScope())
    {
        var clientSide = filter.ToList();   // The filter runs on the client
    }
}
```

```fsharp
Config.withConfig [ Config.disallowUdfPushdown true ] (fun () ->
    query { for s in df.AsQueryable<Sale>() do select {| Sku = s.Sku; Total = total s.Price s.Qty |} }
    |> Seq.toList)
```

---

## 9. How Queries Are Optimized

The provider compiles a query into a backend-neutral logical plan (IR), optimizes that plan, and only then lowers it to a Polars `LazyFrame`. All 22 TPC-H and 99 TPC-DS queries are written as LINQ in the test suite and run without client-side fallback. `query.ExplainLogicalPlan()` prints the IR plan, and `query.Explain()` prints the native plan Polars runs.

Before the passes run, correlated subqueries (`Any`, `All`, `Count`, aggregates over another queryable) are decorrelated into joins. The compiled plan is kept in a query plan cache, so repeating a query shape skips compilation.

| Pass | Effect |
| :--- | :--- |
| Common subplan sharing | A repeated sub-query, such as a shared `IQueryable` variable, runs once and is cached |
| Filter null simplification | Removes null compensation where a filter already discards null |
| Predicate pushdown | Moves filters beneath projections, sorts, groupings and joins, down to the scan |
| Projection pruning | Reads only the columns the query uses |
| Outer join simplification | Turns a left join into an inner join when an inner join above it rejects the rows the left join would add |
| Anti join recognition | Compiles `left join ... where right == null` into an anti join |
| Sort-slice fusion | Fuses a sort with the `Take` / `Skip` that follows it |
| Group order relaxation | Drops first-appearance ordering of a `GroupBy` or `Distinct` when nothing observes it |
| Eager aggregation | Groups a table on the join key before joining the table that holds the grouping columns |
| Union aggregate flattening | Merges a grouping of a `Concat` of groupings on the same keys into one grouping |
| Grouping sets cascade | Computes coarser `RollUp` / `Cube` levels from finer ones |
| Join reordering | Picks a join order from estimated row counts, only where the query does not observe the join order |
| Semi-join reduction | Filters a large join input by the keys of a highly selective one |

Reordering and semi-join reduction use source statistics. Passes never change a query's result: the rules they rely on are the [LINQ Semantics Contract](https://errorlsc.github.io/Polars.NET/articles/linq-semantics).
