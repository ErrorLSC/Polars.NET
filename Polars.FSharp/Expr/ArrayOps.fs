namespace Polars.FSharp

open System
open Polars.NET.Core

type [<Struct>] ArrayOps(handle: ExprHandle) = 
    member _.Len() = new Expr(PolarsWrapper.ArrayLen handle)
    /// <summary> Compute the sum of the values in the array. </summary>
    member _.Sum() = new Expr(PolarsWrapper.ArraySum handle)

    /// <summary> Compute the minimum value in the array. </summary>
    member _.Min() = new Expr(PolarsWrapper.ArrayMin handle)

    /// <summary> Compute the maximum value in the array. </summary>
    member _.Max() = new Expr(PolarsWrapper.ArrayMax handle)

    /// <summary> Compute the mean value in the array. </summary>
    member _.Mean() = new Expr(PolarsWrapper.ArrayMean handle)

    /// <summary> Compute the median value in the array. </summary>
    member _.Median() = new Expr(PolarsWrapper.ArrayMedian handle)
    /// <summary>
    /// Count the number of unique values in every sub-arrays.
    /// </summary>
    member _.NUnique() = new Expr(PolarsWrapper.ArrayNUnique handle)
    /// <summary> Compute the standard deviation of the values in the array. </summary>
    member _.Std(?ddof: int) = 
        let d = defaultArg ddof 1 |> byte
        new Expr(PolarsWrapper.ArrayStd(handle, d))

    /// <summary> Compute the variance of the values in the array. </summary>
    member _.Var(?ddof: int) = 
        let d = defaultArg ddof 1 |> byte
        new Expr(PolarsWrapper.ArrayVar(handle, d))
    /// <summary>
    /// Count how often the value produced by element occurs.
    /// </summary>
    /// <param name="element">An expression that produces a single value</param>
    member _.CountMatches(element:Expr) = new Expr(PolarsWrapper.ArrayCountMatches(handle,element.CloneHandle()))
    member _.Agg(element:Expr) = new Expr(PolarsWrapper.ArrayAgg(handle,element.CloneHandle()))
    /// <summary>
    /// Run any polars expression against the arrays’ elements.
    /// </summary>
    /// <param name="expr">Expression to run. Note that you can select an element with pl.element()</param>
    /// <param name="asList">Collect the resulting data as a list. This allows for expressions which output a variable amount of data.</param>
    member _.Eval(expr:Expr,?asList:bool) =
        let asL = defaultArg asList false
        new Expr(PolarsWrapper.ArrayEval(handle,expr.CloneHandle(),asL))
    /// <summary> Check if any value in the array is true. </summary>
    member _.Any() = new Expr(PolarsWrapper.ArrayAny handle)

    /// <summary> Check if all values in the array are true. </summary>
    member _.All() = new Expr(PolarsWrapper.ArrayAll handle)

    /// <summary> Check if the array contains a specific item. </summary>
    member _.Contains(item: Expr, ?nullsEqual: bool) =
        let eq = defaultArg nullsEqual false
        // Wrapper consumes item ownership
        new Expr(PolarsWrapper.ArrayContains(handle, item.CloneHandle(), eq))

    /// <summary> Check if the array contains a literal value. </summary>
    member this.Contains(item: string, ?nullsEqual) =
        let eq = defaultArg nullsEqual false 
        let itemHandle = PolarsWrapper.Lit item
        new Expr(PolarsWrapper.ArrayContains(handle, itemHandle, eq))
    member this.Contains(item: int, ?nullsEqual) =
        let eq = defaultArg nullsEqual false 
        let itemHandle = PolarsWrapper.Lit item
        new Expr(PolarsWrapper.ArrayContains(handle, itemHandle, eq))
    // --- Operations ---

    /// <summary> Get unique values in the array. </summary>
    member _.Unique(?stable: bool) = 
        let s = defaultArg stable false
        new Expr(PolarsWrapper.ArrayUnique(handle, s))

    /// <summary> Join array elements into a string. </summary>
    member _.Join(separator: string, ?ignoreNulls: bool) =
        let ign = defaultArg ignoreNulls true
        new Expr(PolarsWrapper.ArrayJoin(handle, separator, ign))

    /// <summary> Sort the array. </summary>
    member _.Sort(?descending: bool, ?nullsLast: bool, ?maintainOrder: bool) =
        let desc = defaultArg descending false
        let nLast = defaultArg nullsLast false
        let stable = defaultArg maintainOrder false 
        new Expr(PolarsWrapper.ArraySort(handle, desc, nLast, stable))

    /// <summary> Reverse the array. </summary>
    member _.Reverse() = new Expr(PolarsWrapper.ArrayReverse handle)

    /// <summary> Get the index of the minimum value. </summary>
    member _.ArgMin() = new Expr(PolarsWrapper.ArrayArgMin handle)

    /// <summary> Get the index of the maximum value. </summary>
    member _.ArgMax() = new Expr(PolarsWrapper.ArrayArgMax handle)

    /// <summary> Explode the array to rows. </summary>
    member _.Explode(?emptyAsNull:bool,?keepNulls:bool) = 
        let emp = defaultArg emptyAsNull true
        let kn = defaultArg keepNulls true
        new Expr(PolarsWrapper.ArrayExplode(handle,emp,kn))

    /// <summary> Get value at integer index. </summary>
    member this.Get(index: int, ?nullOnOob) =
        let oob = defaultArg nullOnOob false
        let indexHandle = PolarsWrapper.Lit index
        new Expr(PolarsWrapper.ArrayGet(handle, indexHandle, oob))
    /// <summary>
    /// Get the first value of the sub-arrays.
    /// </summary>
    member this.First() = this.Get(0,true)
    /// <summary>
    /// Get the last value of the sub-arrays.
    /// </summary>
    member this.Last() = this.Get(-1,true)

    /// <summary>
    /// Combine the current expression with other expressions into an Array.
    /// Result: [parent_val, other_val_1, other_val_2, ...]
    /// Equivalent to: pl.concatArray([parent, others...])
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

        new Expr(PolarsWrapper.ConcatArray handles)

    // --- Conversion ---

    /// <summary> Convert Array to List (variable length). </summary>
    member _.ToList() = new Expr(PolarsWrapper.ArrayToList handle)

    /// <summary> Convert Array to Struct. </summary>
    member _.ToStruct([<ParamArray>]names:string array) = new Expr(PolarsWrapper.ArrayToStruct(handle,names))