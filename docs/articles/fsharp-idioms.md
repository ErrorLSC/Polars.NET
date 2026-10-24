# Polars.FSharp Idioms & Dual-Paradigm Guide

`Polars.FSharp` is designed as a **hybrid functional + object-oriented** library:
- **Functional Pipeline Modules** (`pl`, `DataFrame`, `LazyFrame`, `Series`, `cs`) let you chain transformations idiomatically with `|>`.
- **PascalCase Member Methods** on `DataFrame`, `LazyFrame`, `Expr`, and `Series` provide full access to optional parameters with default values and all sub-namespaces (`.Str`, `.Dt`, `.List`, `.Array`, `.Struct`, `.Cat`, `.Bin`, `.Meta`, `.Name`).
- **Rule of Thumb**: Start with `Polars.FSharp.Query` (`query { }`) or pipeline module functions (`DataFrame.*`, `LazyFrame.*`, `pl.*`). Whenever a specialized overload, optional parameter, or sub-namespace operation is not in the module, invoke the **member method** directly.

---

## 1. Custom Comparison & Logical Operators on `Expr`

In F#, standard `=`, `<>`, `>`, `<`, `>=`, `<=` return `bool` and `&&` / `||` cannot be overloaded. Always use the **dot-prefixed operators** when constructing Polars expressions:

| Operation | F# `Expr` Operator | Example |
| :--- | :--- | :--- |
| Equal | `.==` | `pl.col "dept" .== pl.lit "IT"` |
| Not Equal | `.!=` | `pl.col "status" .!= pl.lit "closed"` |
| Greater / GreaterOrEqual | `.>` / `.>=` | `pl.col "age" .>= pl.lit 18` |
| Less / LessOrEqual | `.<` / `.<=` | `pl.col "price" .< pl.lit 99.9` |
| Logical AND | `.&&` | `(pl.col "a" .> pl.lit 0) .&& (pl.col "b" .< pl.lit 10)` |
| Logical OR | `.||` | `(pl.col "a" .== pl.lit 1) .|| (pl.col "b" .== pl.lit 2)` |
| Arithmetic | `+`, `-`, `*`, `/`, `%` | `(pl.col "price" * pl.lit 1.08)` |

> [!NOTE]
> Because `[<AutoOpen>]` modules expose `col`, `lit`, `alias`, `when'`, etc., you can write either `pl.col "a"` or bare `col "a"`. Using `pl.col` / `pl.lit` is recommended when variables in scope might shadow `col`.

---

## 2. Pipeline Modules vs Member Methods

### Pipeline Style (`DataFrame.*`, `LazyFrame.*`, `pl.*`)
```fsharp
open Polars.FSharp

use df =
    DataFrame.ReadCsv(path = "users.csv", tryParseDates = true)
    |> DataFrame.filter (pl.col "age" .>= pl.lit 18)
    |> DataFrame.withColumns [
        (pl.col "weight" / (pl.col "height" * pl.col "height")) |> pl.alias "bmi"
        (pl.col "birthdate").Dt.Year().Alias "birth_year"
    ]
    |> DataFrame.select [
        pl.col "name"
        pl.col "bmi"
        pl.col "birth_year"
    ]
```

### Mixing Member Methods for Sub-namespaces & Optional Parameters
Sub-namespaces (`.Str`, `.Dt`, `.List`, `.Array`, `.Struct`, `.Cat`, `.Bin`, `.Meta`, `.Name`) and multi-option methods are called via dot notation:
```fsharp
let expr =
    (pl.col "raw_ts")
        .Str.StripChars()
        .Str.ToDatetime("%Y-%m-%d %H:%M:%S", strict = false)
        .Dt.Truncate("1h")
        .Alias("hour_bucket")
```

---

## 3. F# Records, Option / ValueOption & Type Safety

`Polars.FSharp` natively understands F# `Record`, `Anon Record`, `'T option`, and `'T voption`:
```fsharp
type UserRecord = {
    Id: int
    Name: string
    Score: float option      // Maps to nullable Float64 column
    Tag: string voption      // Maps to nullable String column
}

// Create DataFrame from F# records:
use df = DataFrame.ofRecords users

// Convert DataFrame back to F# records:
let rows: UserRecord list = df.ToRecords<UserRecord>()

// Create Series from F# list/seq/array (including option lists):
use s1 = Series.create("scores", [Some 10.5; None; Some 20.0])

// Typed scalar access:
let vOpt: float option = s1.GetValueOption<float>(1L) // None
let vRaw: float = s1.GetValue<float>(0L)              // 10.5

// Column enumeration (DataFrame implements seq<Series>):
for col in df do
    printfn "Column: %s (%O)" col.Name col.DataType

// Schema dictionary access (PolarsSchema implements IReadOnlyDictionary<string, DataType>):
use schema = df.Schema
let idType = schema.["Id"]
let hasScore = schema.ContainsKey "Score"
for kv in schema do
    printfn "%s -> %O" kv.Key kv.Value
```

---

## 4. Zero-Allocation Stack Enumeration & Higher-Order Combinators

In high-throughput F# applications, processing elements or rows through standard `IEnumerable<'T>` incurs interface dispatch, heap-allocated enumerators, and boxing overhead. `Polars.FSharp` provides **stack-allocated `[<Struct>]` enumerators** alongside functional combinator modules for both `Series` and `DataFrame`.

### Stack-Allocated `SeriesEnumerator<'T>`

Invoking `series.As<'T>()` returns a stack struct enumerator. Because it defines `MoveNext()` and `Current`, F#'s compiler leverages duck-typing to generate a direct `while` loop on the stack with **zero heap allocations**:

```fsharp
open Polars.FSharp

let s = pl.series "values" [ 10; 20; 30; 40; 50 ]

// 1. Duck-typed for-loop directly on the stack (zero GC pressure):
for x in s.As<int>() do
    printfn "Value: %d" x

// 2. Materialize without allocating Seq wrappers:
let arr: int array = s.As<int>().ToArray()
let lst: int list = s.As<int>().ToList()
let headVal: int voption = s.As<int>().TryHead()
```

### Functional `Series` Combinators

The `Series` module and `Series` extensions provide functional combinators:

```fsharp
// Pure mapping (with automatic Arrow return DataType inference)
let sDouble = s |> Series.map (fun (x: int) -> float x * 1.5)
let sWithIdx = s |> Series.mapi (fun idx x -> sprintf "%d: %d" idx x)

// Handling nulls via Option / ValueOption
let sOpt = s |> Series.mapValueOption (fun (v: int voption) ->
    match v with
    | ValueSome x when x > 20 -> ValueSome (x * 2)
    | _ -> ValueNone
)

// High-speed predicates and searches
let hasPositive = s |> Series.exists (fun x -> x > 0)
let allPositive = s |> Series.forall (fun x -> x > 0)
let containsThirty = s |> Series.contains 30

// Dense vs. Null-Aware side-effect iteration:
// .Iter<'T> skips nulls automatically:
s.Iter<int>(fun v -> printfn "Dense non-null: %d" v)

// .IterOpt<'T> delivers ValueOption explicitly for every single row:
s.IterOpt<int>(fun opt ->
    match opt with
    | ValueSome v -> printfn "Present: %d" v
    | ValueNone -> printfn "Null"
)

// Pairwise iteration across two equal-length Series:
let s2 = pl.series "weights" [ 1.0; 2.0; 3.0; 4.0; 5.0 ]
s.Iter2<int, float>(s2, fun v1 v2 -> printfn "Point: %d * %f" v1 v2)
```

---

## 5. High-Throughput Row Processing: `df.Rows<'T>()` & `Rows` Module

For record-level processing, `df.Rows<'T>()` constructs a `RowEnumerator<'T>`, which uses compiled expression trees to project columnar memory directly into typed F# records.

The `Rows` module exposes inlined higher-order functions (`[<InlineIfLambda>]`), eliminating lambda allocations and iterator heap overhead:

```fsharp
type SensorRecord = {
    DeviceId: string
    Temperature: float
    Pressure: float
}

let df =
    DataFrame.create [
        pl.series "DeviceId" [ "A1"; "A1"; "B2"; "B2" ]
        pl.series "Temperature" [ 22.4; 24.1; 19.8; 21.2 ]
        pl.series "Pressure" [ 101.3; 101.1; 100.8; 100.9 ]
    ]

// 1. Zero-allocation loop over strongly-typed records:
for row in df.Rows<SensorRecord>() do
    printfn "Device %s: Temp=%f" row.DeviceId row.Temperature

// 2. Rows.fold: Stack-based accumulation with inlined folder lambda
let totalTemp =
    df.Rows<SensorRecord>()
    |> Rows.fold (fun acc r -> acc + r.Temperature) 0.0

// 3. Rows.reduce: In-place record reduction
let hottestSensor =
    df.Rows<SensorRecord>()
    |> Rows.reduce (fun r1 r2 -> if r1.Temperature > r2.Temperature then r1 else r2)

// 4. Rows.mapToArray: Direct typed array projection
let formattedNames: string array =
    df.Rows<SensorRecord>()
    |> Rows.mapToArray (fun r -> sprintf "%s (%.1f C)" r.DeviceId r.Temperature)
```

### DataFrame-Level Functional Transformations

Beyond row-level consumption, you can transform DataFrames functionally:

```fsharp
type SummaryRow = {
    DeviceId: string
    IsWarning: bool
}

// MapRows: Transforms source records to target records, automatically materializing
// a new columnar DataFrame without reflection:
let summaryDf =
    df.MapRows<SensorRecord, SummaryRow>(fun r -> {
        DeviceId = r.DeviceId
        IsWarning = r.Temperature > 23.0
    })

// Map: Transform every column Series while preserving column names
let normalizedDf =
    df.Map(fun (col: Series) ->
        if col.DataType = DataType.Float64 then
            col / 100.0
        else
            col
    )

// IterRows: High-throughput side effects per record
df.IterRows<SensorRecord>(fun r ->
    if r.Temperature > 23.0 then
        printfn "[WARNING] Device %s temp: %f" r.DeviceId r.Temperature
)
```

---

## 6. Pipeline Execution & Sinks

F# pipelines support both in-memory materialization (`LazyFrame.collect`, `.Collect()`) and direct out-of-core streaming sinks:
```fsharp
use lf = LazyFrame.ScanParquet "events.parquet"

// 1. In-memory execution:
use df = lf |> LazyFrame.filter (pl.col "valid" .== pl.lit true) |> LazyFrame.collect

// 2. Direct streaming sinks (out-of-core):
lf.SinkParquet "backup.parquet"
```

---

## 7. Polars.NET Exclusive: Merge Pipeline API

In `Polars.FSharp`, SQL MERGE / Upsert operations are expressed naturally as composable pipeline functions (`Merge.*`):

```fsharp
open Polars.FSharp

let target = pl.dataframe [
    pl.series "id" [ 1; 2; 3 ]
    pl.series "score" [ 10; 20; 30 ]
]

let source = pl.dataframe [
    pl.series "id" [ 2; 3; 4 ]
    pl.series "score" [ 200; 0; 40 ]
    pl.series "is_deleted" [ false; true; false ]
]

use result =
    target.Merge(source, pl.col "id")
    // 1. Delete matched records based on condition
    |> Merge.whenMatchedDelete (Some (fun ctx -> ctx.SourceCol "is_deleted" .== pl.lit true))
    // 2. Update remaining matched records
    |> Merge.whenMatchedUpdateAll
    // 3. Insert new records from source
    |> Merge.whenNotMatchedInsertAll
    // 4. Execute eager (or Merge.executeLazy)
    |> Merge.executeEager Engine.Auto
```

---

## 8. Asynchronous Streaming (`RowsAsync`)

Polars.FSharp provides first-class support for asynchronous pipelines through the `RowsAsync` module:

```fsharp
open Polars.FSharp

task {
    // 1. Filter and drain async row streams into an F# list:
    let! hotSensors =
        df.RowsAsync<SensorRecord>(chunkSize = 5000)
        |> RowsAsync.filter (fun s -> s.Temperature > 80.0)
        |> RowsAsync.toList

    // 2. Aggregate over an asynchronous stream without materializing the entire dataset in memory:
    let! totalRevenue =
        lf.RowsAsync<OrderRecord>(chunkSize = 10_000L, engine = Engine.Auto)
        |> RowsAsync.fold (fun acc o -> acc + o.Revenue) 0.0

    printfn $"Total revenue computed asynchronously: %f{totalRevenue}"
}
```

