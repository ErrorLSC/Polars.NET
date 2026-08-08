namespace Polars.FSharp

open Polars.NET.Core

type [<Struct>] CategoricalOps(handle: ExprHandle) =
    /// <summary>
    /// Get the categories stored in this data type.
    /// </summary>
    member _.GetCategories() = new Expr(PolarsWrapper.CatGetCategories handle)
    /// <summary>
    /// Return the byte-length of the string representation of each value.
    /// </summary>
    member _.LenBytes() = new Expr(PolarsWrapper.CatLenBytes handle)
    /// <summary>
    /// Return the number of characters of the string representation of each value.
    /// </summary>
    member _.LenChars() = new Expr(PolarsWrapper.CatLenChars handle)
    /// <summary>
    /// Check if string representations of values start with a substring.
    /// </summary>
    member _.StartsWith prefix = new Expr(PolarsWrapper.CatStartsWith(handle,prefix))
    /// <summary>
    /// Check if string representations of values end with a substring.
    /// </summary>
    member _.EndsWith suffix = new Expr(PolarsWrapper.CatEndsWith(handle,suffix))
    /// <summary>
    /// Get the physical values of a categorical or enum data type.
    /// </summary>
    member _.Physical() = new Expr(PolarsWrapper.CatPhysical handle)
    /// <summary>
    /// Convert to a categorical or enum dtype.
    /// The input must be of the physical type of the categorical or enum dtype.
    /// </summary>
    /// <param name="dtype">The target categorical or enum dtype.</param>
    /// <param name="strict">Whether to panic when encountering an illegal category.</param>
    member _.To(dtype:DataTypeExpr,?strict) =
        let st = defaultArg strict true 
        new Expr(PolarsWrapper.CatTo(handle,dtype.CloneHandle(),st))