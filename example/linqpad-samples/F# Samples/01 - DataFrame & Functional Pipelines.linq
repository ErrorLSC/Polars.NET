<Query Kind="Program">
  <NuGetReference>Polars.NET</NuGetReference>
  <NuGetReference>Polars.NET.Native.win-x64</NuGetReference>
  <Namespace>Polars.CSharp</Namespace>
  <Namespace>Polars.CSharp.Linq</Namespace>
  <Namespace>static Polars.CSharp.Polars</Namespace>
</Query>

// ============================================================================
// 01 - F# DataFrame & Functional Pipelines
// ============================================================================
// Polars.FSharp provides idiomatic functional pipeline combinators (|>),
// strict type-safety, and zero-allocation struct enumerators over Polars.

open System
open Polars.FSharp

// 1. Create a DataFrame using F# collections & pl.series
let df =
    DataFrame.create [
        pl.series "Employee" [ "Alice"; "Bob"; "Charlie"; "David"; "Eva" ]
        pl.series "Department" [ "Engineering"; "HR"; "Engineering"; "Marketing"; "Engineering" ]
        pl.series "Salary" [ 95000.0; 62000.0; 105000.0; 71000.0; 88000.0 ]
        pl.series "Age" [ 29; 34; 42; 31; 26 ]
    ]

// Dump Polars's styled HTML table directly into LINQPad's output window
df.Dump("Original DataFrame")

// 2. Functional pipeline: Filter -> WithColumn -> Sort
let processed =
    df
    |> DataFrame.filter (pl.col("Salary") .> pl.lit(70000.0) .&& (pl.col("Department") .== pl.lit("Engineering")))
    |> DataFrame.withColumn (pl.col("Salary") * pl.lit 0.10 |> pl.alias "Bonus")
    |> DataFrame.sortAscending [pl.col "Salary"]

processed.Dump("Processed Engineers (Salary > 70k, Sorted)")

// 3. GroupBy and Aggregations
let deptStats =
    df
    |> DataFrame.groupBy [ pl.col "Department" ]
    |> DataFrame.agg [
        pl.col("Salary").Sum() |> pl.alias "TotalBudget"
        pl.col("Salary").Mean() |> pl.alias "AvgSalary"
        pl.col("Salary").Max() |> pl.alias "MaxSalary"
        pl.count(["Employee"]) |> pl.alias "Headcount"
    ]

deptStats.Dump("Department Summary Aggregations")
