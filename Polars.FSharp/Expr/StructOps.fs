namespace Polars.FSharp

open Polars.NET.Core

type [<Struct>] StructOps(handle: ExprHandle) =
    /// <summary> Retrieve a field from the struct by name. </summary>
    member _.Field(name: string) = 
        new Expr(PolarsWrapper.StructFieldByName(handle, name))
    member _.Field(index: int) = 
        new Expr(PolarsWrapper.StructFieldByIndex(handle, index))
    member _.RenameFields(names: string list) =
        let cArr = List.toArray names
        new Expr(PolarsWrapper.StructRenameFields(handle, cArr));
    member _.JsonEncode() = 
        new Expr(PolarsWrapper.StructJsonEncode handle);