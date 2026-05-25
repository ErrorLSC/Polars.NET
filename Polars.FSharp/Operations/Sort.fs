namespace Polars.FSharp
open Polars.NET.Core

[<AutoOpen>]
module SortOps = 
    type Series with
        /// <summary>
        /// Sort this Series. Returns a new Series.
        /// </summary>
        /// <param name="descending">Sort in descending order (default: false).</param>
        /// <param name="nullsLast">Place null values last (default: false).</param>
        /// <param name="maintainOrder">Maintain the order of equal elements (Stable sort) (default: false).</param>
        /// <param name="multithreaded">Use multiple threads (default: true).</param>
        member this.Sort(
            ?descending: bool,
            ?nullsLast: bool,
            ?maintainOrder: bool,
            ?multithreaded: bool
        ) =
            let desc = defaultArg descending false
            let nLast = defaultArg nullsLast false
            let stable = defaultArg maintainOrder false
            let multi = defaultArg multithreaded true

            new Series(PolarsWrapper.SeriesSort(this.Handle, desc, nLast, multi, stable))
    type LazyFrame with
        member this.Sort(
            columns: seq<IColumnExpr>, 
            descending: seq<bool>,
            nullsLast: seq<bool>,
            ?maintainOrder: bool
        ): LazyFrame =
            let exprHandles = 
                columns 
                |> Seq.collect (fun x -> x.ToExprs()) 
                |> Seq.map (fun e -> e.CloneHandle()) 
                |> Seq.toArray
            
            let descArr = descending |> Seq.toArray
            let nullsArr = nullsLast |> Seq.toArray
            let stable = defaultArg maintainOrder false

            let lfHandle = this.CloneHandle()
            let h = PolarsWrapper.LazyFrameSort(lfHandle, exprHandles, descArr, nullsArr, stable)
            new LazyFrame(h)
        member this.Sort(
            columns: seq<Expr>, 
            descending: seq<bool>,
            nullsLast: seq<bool>,
            ?maintainOrder: bool
        ): LazyFrame =
            let cols = columns |> Seq.cast<IColumnExpr>
            this.Sort(cols, descending, nullsLast, ?maintainOrder = maintainOrder)
        member this.Sort(
            columns: seq<IColumnExpr>,
            ?descending: bool,
            ?nullsLast: bool,
            ?maintainOrder: bool
        ) : LazyFrame =
            let desc = defaultArg descending false
            let nLast = defaultArg nullsLast false
            this.Sort(columns, [| desc |], [| nLast |], ?maintainOrder = maintainOrder)
        member this.Sort(
            columns: seq<Expr>,
            ?descending: bool,
            ?nullsLast: bool,
            ?maintainOrder: bool
        ) : LazyFrame =
            let cols = columns |> Seq.cast<IColumnExpr>
            this.Sort(cols, ?descending = descending, ?nullsLast = nullsLast, ?maintainOrder = maintainOrder)
        member this.Sort(expr: Expr, ?descending: bool, ?nullsLast: bool) : LazyFrame =
            this.Sort([expr :> IColumnExpr], ?descending=descending, ?nullsLast=nullsLast)

        member this.Sort(colName: string, ?descending: bool, ?nullsLast: bool) : LazyFrame =
            this.Sort([Expr.Col colName :> IColumnExpr], ?descending=descending, ?nullsLast=nullsLast)

        member this.Orderby (expr: Expr, desc: bool) =
            this.Sort(expr, descending = desc)

    type DataFrame with
        /// <summary>
        /// Sort the DataFrame.
        /// </summary>
        /// <param name="columns">the column which needs to be sorted (Expr/Selector)。</param>
        /// <param name="descending">sort direction (true=descending).Length must be 1 (broadcasting) or same with columns.</param>
        /// <param name="nullsLast">null value position (true=last).Length must be 1 (broadcasting) or same with columns</param>
        /// <param name="maintainOrder">Stable Sort option</param>
        member this.Sort (
            columns: seq<IColumnExpr>,
            descending: seq<bool>,
            nullsLast: seq<bool>,
            ?maintainOrder: bool
        ): DataFrame =
            let lf = this.Lazy()
            lf.Sort(columns, descending, nullsLast, ?maintainOrder = maintainOrder).Collect()

        member this.Sort (
            columns: seq<Expr>,
            descending: seq<bool>,
            nullsLast: seq<bool>,
            ?maintainOrder: bool
        ): DataFrame =
            let cols = columns |> Seq.cast<IColumnExpr>
            this.Sort(cols, descending, nullsLast, ?maintainOrder = maintainOrder)

        /// <summary> Sort with simple broadcasting options. </summary>
        member this.Sort(
            columns: seq<IColumnExpr>,
            ?descending: bool,
            ?nullsLast: bool,
            ?maintainOrder: bool
        ) : DataFrame =
            let desc = defaultArg descending false
            let nLast = defaultArg nullsLast false
            this.Sort(columns, [| desc |], [| nLast |], ?maintainOrder = maintainOrder)
        member this.Sort(
            columns: seq<Expr>,
            ?descending: bool,
            ?nullsLast: bool,
            ?maintainOrder: bool
        ) : DataFrame =
            let cols = columns |> Seq.cast<IColumnExpr>
            this.Sort(cols, ?descending = descending, ?nullsLast = nullsLast, ?maintainOrder = maintainOrder)

        /// <summary> Sort by a single expression. </summary>
        member this.Sort(expr: Expr, ?descending: bool, ?nullsLast: bool) : DataFrame =
            this.Sort([expr :> IColumnExpr], ?descending=descending, ?nullsLast=nullsLast)

        /// <summary> Sort by a single column name. </summary>
        member this.Sort(colName: string, ?descending: bool, ?nullsLast: bool,?maintainOrder) : DataFrame =
            this.Sort([Expr.Col colName :> IColumnExpr], ?descending=descending, ?nullsLast=nullsLast,?maintainOrder = maintainOrder)
