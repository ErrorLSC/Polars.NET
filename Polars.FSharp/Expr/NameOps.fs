namespace Polars.FSharp

open Polars.NET.Core

type [<Struct>] NameOps(handle: ExprHandle) =
    // let wrap op arg = new Expr(op(handle, arg))
    member _.Prefix(p: string) = new Expr(PolarsWrapper.Prefix(handle,p))
    member _.Suffix(s: string) = new Expr(PolarsWrapper.Suffix(handle,s))

