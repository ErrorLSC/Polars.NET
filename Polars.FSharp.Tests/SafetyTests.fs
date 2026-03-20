namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp
open Polars.NET.Core

type ``Safety Tests`` () =

    [<Fact>]
    member _.``Throws Exception on invalid column name`` () =
        use csv = new TempCsv "a,b\n1,2"
        let df = DataFrame.ReadCsv csv.Path
        
        let ex = Assert.Throws<PolarsException>(fun () -> 
            df 
            |> pl.filter (pl.col "WrongColumn" .>  pl.lit 1) 
            |> ignore
        )
        Assert.Contains("column", ex.Message.ToLower())