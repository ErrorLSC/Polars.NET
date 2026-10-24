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

> [!Note]
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

## 4. Pipeline Execution & Sinks

F# pipelines support both in-memory materialization (`LazyFrame.collect`, `.Collect()`) and direct out-of-core streaming sinks:
```fsharp
use lf = LazyFrame.ScanParquet "events.parquet"

// 1. In-memory execution:
use df = lf |> LazyFrame.filter (pl.col "valid" .== pl.lit true) |> LazyFrame.collect

// 2. Direct streaming sinks (out-of-core):
lf.SinkParquet "backup.parquet"
```

---

## 5. Polars.NET Exclusive: Merge Pipeline API

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

