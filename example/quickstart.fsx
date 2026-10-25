// ==========================================
// Polars.NET F# Quick Start Script
// ==========================================

#r "nuget: Polars.FSharp, 1.0.0"
#r "nuget: Polars.NET.Native.linux-x64, 1.0.0"

open System
open Polars.FSharp
let printHeader (title: string) (data: DataFrame) =
    Console.ForegroundColor <- ConsoleColor.Cyan
    printfn "\n>>> %s" title
    Console.ResetColor()
    printfn "%O" data
    data

let printHeaderWithString (title: string) (data: string) =
    Console.ForegroundColor <- ConsoleColor.Cyan
    printfn "\n>>> %s" title
    Console.ResetColor()
    printfn "%O" data

// ==========================================
// 1. Create DataFrame by F# Records
// ==========================================

type WeatherData = {
    Date: string
    City: string
    Temperature: float
    Rain: bool
}

let data = [
    { Date="2023-01-01"; City="London";     Temperature=10.5; Rain=true }
    { Date="2023-01-01"; City="Manchester"; Temperature=9.0;  Rain=true }
    { Date="2023-01-02"; City="London";     Temperature=12.1; Rain=false }
    { Date="2023-01-02"; City="Manchester"; Temperature=8.5;  Rain=false }
    { Date="2023-01-03"; City="London";     Temperature=13.5; Rain=false }
]

let df =
    DataFrame.ofRecords data
    |> DataFrame.withColumn (pl.col("Date").Str.ToDate "%Y-%m-%d")
    |> printHeader "Creating DataFrame from Records (Idiomatic F#)"

// ==========================================
// 2. Filter
// ==========================================

df
|> DataFrame.filter (pl.col "City" .== pl.lit "London")
|> printHeader "Filtering: London Only"

// ==========================================
// 2.5 Config
// ==========================================

Config.tableFormatting (TableFormatting.AsciiFull,Active.ResetToDefault)()

// ==========================================
// 3. GroupBy & Aggregation
// ==========================================

df
|> DataFrame.groupBy [pl.col "City"]
|> DataFrame.agg [
    pl.col("Temperature").Mean().Alias "Avg_Temp"
    pl.col("Temperature").Max().Alias "Max_Temp"
    pl.col("Rain").Sum().Alias "Rainy_Days" // bool sum -> count of true
    pl.len().Alias "Total_Records"
]
|> printHeader "Aggregation: Stats per City"

// ==========================================
// 4. Window Functions
// ==========================================

df
|> DataFrame.select [
    pl.col "Date"
    pl.col "City"
    pl.col "Temperature"

    // Over(Date): Calculate mean value by group
    pl.col("Temperature").Mean().Over(pl.col "Date")
        |> pl.alias "Daily_Avg"

    pl.col "Temperature" - pl.col("Temperature").Mean().Over(pl.col "Date")
        |> pl.alias "Diff"
]
|> DataFrame.sortAscending [pl.col "Date"]
|> printHeader "Window: Diff from Daily Average"

// ==========================================
// 5. Lazy API
// ==========================================

// pl.asLazy won't execuate eagerly, only execution plan will be built
let lf =
    df
    |> pl.asLazy
    |> LazyFrame.filter(pl.col "Temperature" .> pl.lit 10.0)
    |> LazyFrame.groupBy [pl.col "City"]
    |> LazyFrame.agg [pl.col("Temperature").Mean() |> pl.alias "Lazy_Avg_Temp"]

// Print Exection Optimized Plan
printHeaderWithString "Lazy Execution & Query Plan" (lf |> LazyFrame.explain)

// Execuate after Collect
lf |> LazyFrame.collect |> printHeader "Lazy Execution Result"

// ==========================================
// 6. SQL MERGE
// ==========================================
let plan =
    [
        pl.series "Id"    [1; 2; 3]
        pl.series "Value" ["A"; "B"; "C"]
    ]
    |> pl.dataframe |> pl.asLazy
    |> Merge.initiate
        (
            [
                pl.series "Id"    [2; 3; 4]
                pl.series "Value" ["B_new"; "C_new"; "D"]
            ]
            |> pl.dataframe |> pl.asLazy
        )
        ["Id"]
    |> Merge.whenMatchedUpdateSet (Set.build [
        Set.col "Value" (fun ctx -> ctx.SourceCol "Value")
    ])
    |> Merge.whenNotMatchedInsertAll

printHeaderWithString "MERGE Plan" (plan |> Merge.toPlanString)

plan
|> Merge.execute
|> LazyFrame.sortAscending [pl.col "Id"]
|> LazyFrame.collect
|> printHeader "MERGE Execution Result"
