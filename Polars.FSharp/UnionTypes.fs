namespace Polars.FSharp

open System

[<RequireQualifiedAccess>]
type Dtype =
    | DataTypeExpr of DataTypeExpr         
    | DataType of DataType        
    | NetType of Type            

[<RequireQualifiedAccess>]
module DtypeExpr =
    let consume (src: Dtype) : DataTypeExpr =
        match src with
        | Dtype.DataTypeExpr e -> e.Clone()
        | Dtype.DataType dt -> dt.ToDataTypeExpr()
        | Dtype.NetType t ->
            let dt = DataType.op_Implicit t
            dt.ToDataTypeExpr()           

[<RequireQualifiedAccess>]
type Sel =
    | Selector of Selector
    | ByName of string
    | ByDtype of DataType
    | ByNetType of Type
    | ByExpr of Expr

[<RequireQualifiedAccess>]
module Sel =
    open Polars.NET.Core
    /// <summary>Consume the selector source and return a usable Selector (caller owns, no further cloning needed).</summary>
    let consume (src: Sel) : Selector =
        match src with
        | Sel.Selector s -> 
            s.Clone()
        | Sel.ByName name -> 
            Selector.ByName(name)
        | Sel.ByDtype dt -> 
            new Selector(PolarsWrapper.SelectorByDtype(dt.ToPlDataType()))
        | Sel.ByNetType t ->
            let dt = DataType.FromNetType t
            new Selector(PolarsWrapper.SelectorByDtype(dt.ToPlDataType()))
        | Sel.ByExpr expr ->
            if not (expr.Meta.IsColumnSelection(allowAliasing = true)) then
                invalidArg "expr" 
                    "Invalid conversion to Selector. A Selector must strictly be a column selection (e.g., pl.col(\"name\"), pl.cs.numeric(), or regex). Mathematical computations, aggregations, or literals cannot be used as Selectors."
            expr.ToSelector()

[<RequireQualifiedAccess>]
type Dur =
    | String of string
    | TimeSpan of TimeSpan

[<RequireQualifiedAccess>]
module Dur =
    open Polars.NET.Core.Helpers
    let consume (src: Dur) =
        match src with
        | Dur.String s ->
            if String.IsNullOrWhiteSpace s then
                invalidArg "src" "Duration string cannot be null or empty."
            s
        | Dur.TimeSpan ts ->
            ts.ToPolarsDuration()