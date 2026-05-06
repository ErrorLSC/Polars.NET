namespace Polars.FSharp

open Polars.NET.Core
open System

type [<Struct>] StructOps(handle: ExprHandle) =
    /// <summary> Retrieve a field from the struct by name. </summary>
    member _.Field([<ParamArray>]names: string array) = 
        new Expr(PolarsWrapper.StructFieldByName(handle, names))
    member _.Field(index: int) = 
        new Expr(PolarsWrapper.StructFieldByIndex(handle, index))
    member _.RenameFields([<ParamArray>]names: string array) =
        new Expr(PolarsWrapper.StructRenameFields(handle, names));
    member _.JsonEncode() = 
        new Expr(PolarsWrapper.StructJsonEncode handle);