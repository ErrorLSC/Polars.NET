namespace Polars.FSharp

open Polars.NET.Core

type [<Struct>] ListOps(handle: ExprHandle) =

    /// <summary> Get element at index. </summary>
    member _.Get(index: int,?nullOnOob:bool) = 
        let nob = defaultArg nullOnOob false
        new Expr(PolarsWrapper.ListGet(handle, PolarsWrapper.Lit index,nob))
    /// <summary> Get the first element of the list. </summary>
    member this.First() = this.Get(0,true)
    member this.Last() = this.Get(-1,true)
    /// <summary> Join list elements with separator. </summary>
    member _.Join(separator: string,?ignoreNulls:bool) = 
        let ign = defaultArg ignoreNulls true
        new Expr(PolarsWrapper.ListJoin(handle, separator,ign))
    /// <summary> Get list length. </summary>
    member _.Len() = new Expr(PolarsWrapper.ListLen handle)
    /// <summary> Reverse the list. </summary>
    member _.Reverse() = new Expr(PolarsWrapper.ListReverse handle)
    // Aggregations within list
    member _.Sum() = new Expr(PolarsWrapper.ListSum handle)
    member _.Min() = new Expr(PolarsWrapper.ListMin handle)
    member _.Max() = new Expr(PolarsWrapper.ListMax handle)
    member _.Mean() = new Expr(PolarsWrapper.ListMean handle)
    /// <summary> Sort the list. </summary>
    member _.Sort(?descending: bool, ?nullsLast:bool,?maintainOrder: bool) =
        let desc = defaultArg descending false
        let nullsLastOption = defaultArg nullsLast false 
        let maintainOrderOption = defaultArg maintainOrder false 
        new Expr(PolarsWrapper.ListSort(handle, desc,nullsLastOption,maintainOrderOption))
    /// <summary>
    /// Combine the current expression with other expressions into a List.
    /// Result: [parent_val, other_val_1, other_val_2, ...]
    /// Equivalent to: pl.concatList([parent, others...])
    /// </summary>
    member _.Concat(others: seq<#IColumnExpr>) =
        let currentHandle = handle
        let handles = 
            seq {
                yield currentHandle
                
                yield! others 
                       |> Seq.collect (fun x -> x.ToExprs()) 
                       |> Seq.map (fun e -> e.CloneHandle())
            }
            |> Seq.toArray

        new Expr(PolarsWrapper.ConcatList handles)

    /// <summary>
    /// Overload: Concat a single expression/column.
    /// </summary>
    member this.Concat(other: #IColumnExpr) =
        this.Concat [other]
    // Contains
    member _.Contains(item: Expr,?nullsEqual: bool) : Expr = 
        let nE = defaultArg nullsEqual false
        new Expr(PolarsWrapper.ListContains(handle, item.CloneHandle(),nE))
    member _.Contains(item: int,?nullsEqual: bool) = 
        let itemHandle = PolarsWrapper.Lit item
        let nE = defaultArg nullsEqual false
        new Expr(PolarsWrapper.ListContains(PolarsWrapper.CloneExpr handle, itemHandle,nE))
    member _.Contains(item: string,?nullsEqual:bool) =
        let nE = defaultArg nullsEqual false 
        let itemHandle = PolarsWrapper.Lit item
        new Expr(PolarsWrapper.ListContains(PolarsWrapper.CloneExpr handle, itemHandle, nE))