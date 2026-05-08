namespace Polars.FSharp

open System

[<RequireQualifiedAccess>]
type Dtype =
    | DataTypeExpr of DataTypeExpr         
    | PlDataType of DataType        
    | NetType of Type            

[<RequireQualifiedAccess>]
module Dtype =
    let consume (src: Dtype) : DataTypeExpr =
        match src with
        | Dtype.DataTypeExpr e -> e.Clone()
        | Dtype.PlDataType dt -> dt.ToDataTypeExpr()
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