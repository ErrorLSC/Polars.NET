namespace Polars.FSharp

open Polars.NET.Core

type [<Struct>] CategoricalOps(handle: ExprHandle) =
    /// <summary>
    /// Get the categories stored in this data type.
    /// </summary>
    member _.GetCategories() = new Expr(PolarsWrapper.CatGetCategories(handle))
    /// <summary>
    /// Return the byte-length of the string representation of each value.
    /// </summary>
    member _.LenBytes() = new Expr(PolarsWrapper.CatLenBytes(handle))
    /// <summary>
    /// Return the number of characters of the string representation of each value.
    /// </summary>
    member _.LenChars() = new Expr(PolarsWrapper.CatLenChars(handle))
    member _.StartsWith(prefix) = new Expr(PolarsWrapper.CatStartsWith(handle,prefix))
    member _.EndsWith(suffix) = new Expr(PolarsWrapper.CatEndsWith(handle,suffix))