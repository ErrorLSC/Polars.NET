#r "nuget: Polars.FSharp, 1.0.1"
#r "nuget: Polars.NET.Native.linux-x64, 1.0.1"
#r "nuget: Polars.NET.Native.win-x64, 1.0.1"
#r "nuget: FSharp.Data"

open FSharp.Data
open Polars.FSharp
[<Literal>]
let path = "test.csv"

let dfPre =
    [
        pl.series "nihao" [1;1;4;5;1;4]
        pl.series "byebye" ["polars";"dotnet";"fsharp";"csv";"qinglei";"errorlsc"]
    ] 
    |> pl.dataframe

dfPre.WriteCsv path

type Data = CsvProvider<path,UseOriginalNames=true>

let schema = Unchecked.defaultof<Data.Row>

DataFrame.ReadCsv path 
    |> DataFrame.select [pl.col (nameof schema.byebye)] |> DataFrame.show 
