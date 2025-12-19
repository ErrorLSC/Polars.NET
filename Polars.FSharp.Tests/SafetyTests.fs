namespace Polars.FSharp.Tests

open System
open Xunit
open Polars.FSharp

type ``Safety Tests`` () =

    [<Fact>]
    member _.``Throws Exception on invalid column name`` () =
        use csv = new TempCsv "a,b\n1,2"
        let df = DataFrame.ReadCsv csv.Path
        
        let ex = Assert.Throws<Exception>(fun () -> 
            df 
            |> Polars.filter (Polars.col "WrongColumn" .> Polars.lit 1) 
            |> ignore
        )
        // 验证错误信息是否包含 Polars 的关键词，而不是乱码或 Segfault
        Assert.Contains("column", ex.Message.ToLower())