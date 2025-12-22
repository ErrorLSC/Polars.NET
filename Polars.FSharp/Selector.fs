namespace Polars.FSharp


open Polars.NET.Core

/// <summary>
/// A column selection strategy (e.g., all columns, or specific columns).
/// </summary>
type Selector(handle: SelectorHandle) =
    member _.Handle = handle
    
    member internal this.CloneHandle() = 
        PolarsWrapper.CloneSelector handle

    /// <summary> Exclude columns from a wildcard selection (col("*")). </summary>
    member this.Exclude(names: string list) =
        let arr = List.toArray names
        new Selector(PolarsWrapper.SelectorExclude(this.CloneHandle(), arr))

    member this.ToExpr() =
        new Expr(PolarsWrapper.SelectorToExpr(this.CloneHandle()))