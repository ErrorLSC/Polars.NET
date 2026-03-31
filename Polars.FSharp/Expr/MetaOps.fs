namespace Polars.FSharp

open Polars.NET.Core

type [<Struct>] MetaOps(handle: ExprHandle) = 
    member this.OutputName() =
        PolarsWrapper.ExprGetOutputName handle
