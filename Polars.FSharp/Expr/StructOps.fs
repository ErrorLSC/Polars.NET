namespace Polars.FSharp

open Polars.NET.Core
open System

type [<Struct>] StructOps(handle: ExprHandle) =
    /// <summary> Retrieve a field from the struct by name. </summary>
    member _.Field([<ParamArray>]names: string array) = 
        new Expr(PolarsWrapper.StructFieldByName(handle, names))
    /// <summary>
    /// Retrieve a field by its index.
    /// </summary>
    member _.Field(index: int) = 
        new Expr(PolarsWrapper.StructFieldByIndex(handle, index))
    /// <summary>
    /// Rename the fields of the struct.
    /// </summary>
    /// <param name="names">The new names for the fields.</param>
    member _.RenameFields([<ParamArray>]names: string array) =
        new Expr(PolarsWrapper.StructRenameFields(handle, names));
    /// <summary>
    /// Convert the struct column into a JSON string column.
    /// Useful for debugging or exporting to systems that support JSON strings.
    /// </summary>
    member _.JsonEncode() = 
        new Expr(PolarsWrapper.StructJsonEncode handle);
    /// <summary>
    /// Expand the struct into its individual fields.Alias for Expr.Struct.field("*").
    /// </summary>
    member this.Unnest() = this.Field("*")
    /// <summary>
    /// Add or overwrite fields of this struct.This is similar to with_columns on DataFrame.
    /// </summary>
    /// <param name="expr">Field(s) to add, specified as positional arguments.</param>
    member _.WithFields([<ParamArray>]fields: Expr array) = 
        if fields.Length = 0 then
            new Expr(PolarsWrapper.StructWithFields(handle,[||]))
        else 
            let fr = fields |> Array.map (fun e -> e.CloneHandle())
            new Expr(PolarsWrapper.StructWithFields(handle,fr))
    member this.Item with get (index:int) = this.Field index
    member this.Item with get (name:string) = this.Field name
    member this.Item with get (names:string array) = this.Field names

