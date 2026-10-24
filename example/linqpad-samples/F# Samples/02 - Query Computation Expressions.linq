<Query Kind="FSharpProgram">
  <NuGetReference>Polars.FSharp</NuGetReference>
  <NuGetReference>Polars.NET.Native.win-x64</NuGetReference>
  <Namespace>Polars.FSharp</Namespace>
  <Namespace>Polars.FSharp.Query</Namespace>
</Query>

// ============================================================================
// 02 - F# Query Computation Expressions (LINQ in F#)
// ============================================================================
// Polars.NET compiles F# query { ... } computation expressions directly
// into native Rust Polars LazyPlans without managed iteration overhead!

open System
open Polars.FSharp
open Polars.FSharp.Query

// Strongly typed domain records
[<CLIMutable>]
type Customer = { Id: int; Name: string; Region: string }

[<CLIMutable>]
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
// 1. Relational JOIN using F# query { ... } computation expression
// ----------------------------------------------------------------------------
let orderQuery = orders.AsPolarsQueryable()
let customerQuery = customers.AsPolarsQueryable()

let joinedQuery =
    query {
        for o in orderQuery do
        join c in customerQuery on (o.CustomerId = c.Id)
        where (o.Amount >= 50.0)
        sortByDescending o.Amount
        select (o.OrderId, c.Name, c.Region, o.Product, o.Amount * float o.Quantity)
    }

// Inspect the compiled native Rust LazyPlan
joinedQuery.Explain().Dump("F# query { join ... } -> Compiled Rust LazyPlan")

// Materialize results into an array
let joinResults = joinedQuery.ToArray()
joinResults.Dump("Relational Join Results (Amount >= $50)")

// ----------------------------------------------------------------------------
// 2. F# GroupBy & Aggregations
// ----------------------------------------------------------------------------
let groupQuery =
    query {
        for o in orderQuery do
        join c in customerQuery on (o.CustomerId = c.Id)
        groupBy c.Region into g
        sortByDescending (g.Sum(fun x -> x.Amount * float x.Quantity))
        select (g.Key, g.Sum(fun x -> x.Amount * float x.Quantity), g.Average(fun x -> x.Amount), g.Count())
    }

groupQuery.Explain().Dump("F# query { groupBy ... } -> Compiled Rust LazyPlan")
groupQuery.ToArray().Dump("Regional Sales Summary")
