namespace Polars.FSharp

open System
open Polars.NET.Core

type SeriesStructNameSpace(parent: Series) =
    
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary> Retrieve a field from the struct by name. </summary>
    member _.Field(name: string) = 
        apply (fun e -> e.Struct.Field name)

    /// <summary> Retrieve a field from the struct by index. </summary>
    member _.Field(index: int) = 
        apply (fun e -> e.Struct.Field index)

    /// <summary> Rename the fields of the struct. </summary>
    member _.RenameFields([<ParamArray>] names:string array) =
        apply (fun e -> e.Struct.RenameFields names)

    /// <summary> Convert struct to JSON string. </summary>
    member _.JsonEncode() = 
        apply (fun e -> e.Struct.JsonEncode())
    /// <summary>
    /// Unnest the struct column into a DataFrame.
    /// Each field of the struct becomes a separate column.
    /// </summary>
    member _.Unnest() =
        let dfHandle = PolarsWrapper.SeriesStructUnnest parent.Handle
        new DataFrame(dfHandle)