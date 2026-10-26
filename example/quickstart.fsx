// ==========================================
// Polars.NET F# Quick Start Script
// ==========================================

#r "nuget: Polars.FSharp, 1.0.1"
#r "nuget: Polars.NET.Native.linux-x64, 1.0.1"
#r "nuget: Polars.NET.Native.win-x64, 1.0.1"

open System
open Polars.FSharp
open Polars.FSharp.Query
open System.Linq

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

type Customer = { Id: int; Name: string; Region: string }

type Order = { OrderId: int; CustomerId: int; Product: string; Amount: float; Quantity: int }

let customers = [|
    { Id = 1; Name = "Alice"; Region = "North" }
    { Id = 2; Name = "Bob"; Region = "South" }
    { Id = 3; Name = "Charlie"; Region = "North" }
    { Id = 4; Name = "David"; Region = "East" }
|]

let orders = [|
    { OrderId = 101; CustomerId = 1; Product = "Laptop"; Amount = 1200.0; Quantity = 1 }
    { OrderId = 102; CustomerId = 1; Product = "Mouse"; Amount = 45.0; Quantity = 2 }
    { OrderId = 103; CustomerId = 2; Product = "Monitor"; Amount = 350.0; Quantity = 2 }
    { OrderId = 104; CustomerId = 3; Product = "Keyboard"; Amount = 120.0; Quantity = 1 }
    { OrderId = 105; CustomerId = 3; Product = "Desk Chair"; Amount = 280.0; Quantity = 1 }
    { OrderId = 106; CustomerId = 4; Product = "Headphones"; Amount = 150.0; Quantity = 1 }
    { OrderId = 107; CustomerId = 1; Product = "USB Cable"; Amount = 25.0; Quantity = 3 }
|]

// ----------------------------------------------------------------------------
// 7. Relational JOIN using F# query { ... } computation expression
// ----------------------------------------------------------------------------
let orderQuery = orders.AsPolarsQueryable()
let customerQuery = customers.AsPolarsQueryable()

let joinedQuery =
    query {
        for o in orderQuery do
        join c in customerQuery on (o.CustomerId = c.Id)
        where (o.Amount >= 50.0)
        sortByDescending o.Amount
        select
            {|
                OrderId = o.OrderId
                Customer = c.Name
                Region = c.Region
                Product = o.Product
                Total = o.Amount * float o.Quantity
            |}
    }

// Inspect the compiled native Rust LazyPlan
printfn "%s" (joinedQuery.Explain())

// Materialize results into DataFrame
joinedQuery.ToDataFrame().Show()

// ----------------------------------------------------------------------------
// 8. F# GroupBy & Aggregations
// ----------------------------------------------------------------------------
let groupQuery =
    query {
        for o in orderQuery do
        join c in customerQuery on (o.CustomerId = c.Id)
        groupValBy o c.Region into g
        sortByDescending (g.Sum(fun x -> x.Amount * float x.Quantity))
        select
            {|
                Region = g.Key
                TotalRevenue = g.Sum(fun x -> x.Amount * float x.Quantity)
                AverageOrderValue = g.Average(fun x -> x.Amount)
                OrderCount = g.Count()
            |}
    }

printfn "%s" (groupQuery.Explain())
groupQuery.ToDataFrame().Show()
