namespace Polars.FSharp

open System
open Polars.NET.Core

type SeriesStructNameSpace(parent: Series) =

    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary> Retrieve a field from the struct by name. </summary>
    member _.Field([<ParamArray>] names: string array) = apply (fun e -> e.Struct.Field names)

    /// <summary> Retrieve a field from the struct by index. </summary>
    member _.Field(index: int) = apply (fun e -> e.Struct.Field index)

    /// <summary> Rename the fields of the struct. </summary>
    member _.RenameFields([<ParamArray>] names: string array) =
        apply (fun e -> e.Struct.RenameFields names)

    /// <summary>
    /// Drop one or more fields from the struct.
    /// </summary>
    /// <param name="names">Names of the fields to drop.</param>
    /// <param name="strict">If True, raise an error if any of the specified fields do not exist in the struct.</param>
    member _.Drop(names, ?strict) =
        apply (fun e -> e.Struct.Drop(names, ?strict = strict))

    /// <summary> Convert struct to JSON string. </summary>
    member _.JsonEncode() = apply (fun e -> e.Struct.JsonEncode())

    /// <summary>
    /// Unnest the struct column into a DataFrame.
    /// Each field of the struct becomes a separate column.
    /// </summary>
    member _.Unnest() =
        let dfHandle = PolarsWrapper.SeriesStructUnnest parent.Handle
        new DataFrame(dfHandle)

    /// <summary>
    /// Add or overwrite fields of this struct.This is similar to with_columns on DataFrame.
    /// </summary>
    /// <param name="fields">Field(s) to add, specified as positional arguments.</param>
    member _.WithFields([<ParamArray>] fields: Expr array) =
        apply (fun e -> e.Struct.WithFields fields)

    /// <summary>
    /// Access the fields of the struct by index.
    /// </summary>
    member this.Item
        with get (index: int) = this.Field index

    /// <summary>
    /// Access the fields of the struct by name.
    /// </summary>
    member this.Item
        with get (name: string) = this.Field name

    /// <summary>
    /// Access the fields of the struct by multiple names.
    /// </summary>
    member this.Item
        with get (names: string array) = this.Field names
