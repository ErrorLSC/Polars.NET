namespace Polars.FSharp
open Polars.NET.Core

[<AutoOpen>]
module PivotOps = 
    type LazyFrame with
       /// <summary>
        /// Pivot the LazyFrame.
        /// <para>
        /// <b>Important:</b> Lazy pivot requires an eager <paramref name="onColumns"/> DataFrame 
        /// to determine the output schema (column names) during the planning phase.
        /// </para>
        /// </summary>
        /// <param name="index">Selector for the index column(s) (the rows).</param>
        /// <param name="columns">Selector for the column(s) to pivot (the new column headers).</param>
        /// <param name="values">Selector for the value column(s) to populate the cells.</param>
        /// <param name="onColumns">
        /// An <b>Eager DataFrame</b> containing the unique values of the <paramref name="columns"/>.
        /// <br/>This is strictly used for schema inference.
        /// </param>
        /// <param name="aggregateExpr">Optional expression to aggregate the values. If null, uses <paramref name="aggregateFunction"/>.</param>
        /// <param name="aggregateFunction">Aggregation function to use if <paramref name="aggregateExpr"/> is null. Default is First.</param>
        /// <param name="maintainOrder">Sort the result by the index column.</param>
        /// <param name="separator">Separator used to combine column names when multiple value columns are selected.</param>
        /// <returns>A new LazyFrame with the pivot operation applied.</returns>
        member this.Pivot(
            index: Selector,
            columns: Selector,
            values: Selector,
            onColumns: DataFrame,
            ?aggregateExpr: Expr,
            ?aggregateFunction: PivotAgg,
            ?maintainOrder: bool,
            ?separator: string,
            ?columnNaming: PivotColumnNaming
        ) =
            let aggFunc = defaultArg aggregateFunction PivotAgg.First
            let mo = defaultArg maintainOrder true
            let sep = Option.toObj separator
            let cn = defaultArg columnNaming PivotColumnNaming.Auto

            use indexH = index.CloneHandle()
            use columnsH = columns.CloneHandle()
            use valuesH = values.CloneHandle()
            use aggExprH = 
                match aggregateExpr with
                | Some e -> e.CloneHandle()
                | None -> null

            let h = PolarsWrapper.LazyPivot(
                this.CloneHandle(),
                columnsH,           // on (columns selector)
                onColumns.Handle,   // onColumns (Eager DF Handle - passed directly, not cloned/disposed here)
                indexH,             // index
                valuesH,            // values
                aggExprH,           // aggExpr
                aggFunc.ToNative(), // aggregateFunction mapping
                mo,                 // maintainOrder
                sep,                 // separator
                cn.ToNative()
            )

            new LazyFrame(h)
        /// <summary>
        /// Pivot the LazyFrame using column names.
        /// </summary>
        /// <param name="index">Column names to use as the index.</param>
        /// <param name="columns">Column names to use for the new column headers.</param>
        /// <param name="values">Column names to use for the values.</param>
        /// <param name="onColumns">
        /// An <b>Eager DataFrame</b> containing the unique values of the <paramref name="columns"/>.
        /// </param>
        /// <param name="aggregateFunction">Aggregation function. Default is First.</param>
        /// <param name="maintainOrder">Sort the result by the index column.</param>
        /// <param name="separator">Separator for generated column names.</param>
        member this.Pivot(
            index: seq<string>,
            columns: seq<string>,
            values: seq<string>,
            onColumns: DataFrame,
            ?aggregateFunction: PivotAgg,
            ?maintainOrder: bool,
            ?separator: string,
            ?columnNaming: PivotColumnNaming
        ) =
            use sIndex = new Selector(PolarsWrapper.SelectorCols(index |> Seq.toArray))
            use sColumns = new Selector(PolarsWrapper.SelectorCols(columns |> Seq.toArray))
            use sValues = new Selector(PolarsWrapper.SelectorCols(values |> Seq.toArray))

            this.Pivot(
                sIndex,
                sColumns,
                sValues,
                onColumns,
                ?aggregateFunction = aggregateFunction,
                ?maintainOrder = maintainOrder,
                ?separator = separator,
                ?columnNaming = columnNaming
            )

        /// <summary>
        /// Pivot the LazyFrame using column names and a custom aggregation expression.
        /// </summary>
        member this.Pivot(
            index: seq<string>,
            columns: seq<string>,
            values: seq<string>,
            onColumns: DataFrame,
            aggregateExpr: Expr,
            ?maintainOrder: bool,
            ?separator: string,
            ?columnNaming: PivotColumnNaming
        ) =
            use sIndex = new Selector(PolarsWrapper.SelectorCols(index |> Seq.toArray))
            use sColumns = new Selector(PolarsWrapper.SelectorCols(columns |> Seq.toArray))
            use sValues = new Selector(PolarsWrapper.SelectorCols(values |> Seq.toArray))

            this.Pivot(
                sIndex,
                sColumns,
                sValues,
                onColumns,
                aggregateExpr = aggregateExpr,
                ?maintainOrder = maintainOrder,
                ?separator = separator,
                ?columnNaming = columnNaming
            )
    type DataFrame with
           /// <summary>
        /// Pivot the DataFrame from long to wide format.
        /// </summary>
        /// <param name="index">Selector for the index column(s) (the rows).</param>
        /// <param name="columns">Selector for the column(s) to pivot (the new column headers).</param>
        /// <param name="values">Selector for the value column(s) to populate the cells.</param>
        /// <param name="aggregateExpr">Optional expression to aggregate the values. If null, uses <paramref name="aggregateFunction"/>.</param>
        /// <param name="aggregateFunction">Aggregation function to use if <paramref name="aggregateExpr"/> is null. Default is First.</param>
        /// <param name="sortColumns">Sort the pivoted columns.</param>
        /// <param name="maintainOrder">Maintain the order of the data.</param>
        /// <param name="separator">Separator used to combine column names when multiple value columns are selected.</param>
        member this.Pivot(
            index: Selector,
            columns: Selector,
            values: Selector,
            ?aggregateExpr: Expr,
            ?aggregateFunction: PivotAgg,
            ?sortColumns: bool,
            ?maintainOrder: bool,
            ?separator: string,
            ?columnNaming: PivotColumnNaming
        ) =
            // 1. Resolve Defaults
            let aggFunc = defaultArg aggregateFunction PivotAgg.First
            let sort = defaultArg sortColumns false
            let mo = defaultArg maintainOrder true
            let sep = Option.toObj separator
            let cn = defaultArg columnNaming PivotColumnNaming.Auto
            // 2. Clone Handles
            use indexH = index.CloneHandle()
            use columnsH = columns.CloneHandle()
            use valuesH = values.CloneHandle()
            use aggExprH = 
                match aggregateExpr with
                | Some e -> e.CloneHandle()
                | None -> null

            // 3. Native Call
            let h = PolarsWrapper.Pivot(
                this.Handle,
                indexH,
                columnsH,
                valuesH,
                aggExprH,
                aggFunc.ToNative(),
                sort,
                mo,
                sep,
                cn.ToNative()
            )
            new DataFrame(h)

        /// <summary>
        /// Pivot the DataFrame using column names.
        /// </summary>
        member this.Pivot(
            index: seq<string>,
            columns: seq<string>,
            values: seq<string>,
            ?aggFn: PivotAgg,
            ?sortColumns: bool,
            ?maintainOrder: bool,
            ?separator: string,
            ?columnNaming: PivotColumnNaming
        ) =
            use sIndex = new Selector(PolarsWrapper.SelectorCols(index |> Seq.toArray))
            use sColumns = new Selector(PolarsWrapper.SelectorCols(columns |> Seq.toArray))
            use sValues = new Selector(PolarsWrapper.SelectorCols(values |> Seq.toArray))

            this.Pivot(
                sIndex,
                sColumns,
                sValues,
                ?aggregateFunction = aggFn,
                ?sortColumns = sortColumns,
                ?maintainOrder = maintainOrder,
                ?separator = separator,
                ?columnNaming=columnNaming
            )

        /// <summary>
        /// Pivot the DataFrame using column names and a custom aggregation expression.
        /// </summary>
        member this.Pivot(
            index: seq<string>,
            columns: seq<string>,
            values: seq<string>,
            aggExpr: Expr,
            ?sortColumns: bool,
            ?maintainOrder: bool,
            ?separator: string,
            ?columnNaming: PivotColumnNaming
        ) =        
            use sIndex = new Selector(PolarsWrapper.SelectorCols(index |> Seq.toArray))
            use sColumns = new Selector(PolarsWrapper.SelectorCols(columns |> Seq.toArray))
            use sValues = new Selector(PolarsWrapper.SelectorCols(values |> Seq.toArray))

            this.Pivot(
                sIndex,
                sColumns,
                sValues,
                aggregateExpr = aggExpr,
                ?sortColumns = sortColumns,
                ?maintainOrder = maintainOrder,
                ?separator = separator,
                ?columnNaming= columnNaming
            )


[<AutoOpen>]
module UnpivotOps = 
    type LazyFrame with
        /// <summary>
        /// Unpivot (Melt) the LazyFrame using Selectors.
        /// Primary overload backed by native binding.
        /// </summary>
        member this.Unpivot(index: Selector, on: Selector, variableName: string option, valueName: string option) : LazyFrame =
            let lfClone = this.CloneHandle()
            
            let hIndex = index.CloneHandle()
            let hOn = on.CloneHandle()
            let varN = Option.toObj variableName
            let valN = Option.toObj valueName
            
            new LazyFrame(PolarsWrapper.LazyUnpivot(lfClone, hIndex, hOn, varN, valN))

        /// <summary>
        /// Unpivot (Melt) overload for simple string lists.
        /// Auto-converts to Selectors.
        /// </summary>
        member this.Unpivot(index: seq<string>, on: seq<string>, variableName: string option, valueName: string option) =
            // 1. Convert Index strings to Selector
            let idxArr = Seq.toArray index
            let sIndex = new Selector(PolarsWrapper.SelectorCols idxArr)

            // 2. Convert On strings to Selector
            let onArr = Seq.toArray on
            let sOn = new Selector(PolarsWrapper.SelectorCols onArr)

            // 3. Route to main logic
            this.Unpivot(sIndex, sOn, variableName, valueName)

        member this.Unpivot(index: string list, on: string list) =
            this.Unpivot(index, on, None, None)

        // ==========================================
        // Aliases (Melt)
        // ==========================================
        
        member this.Melt(index: Selector, on: Selector, variableName, valueName) = 
            this.Unpivot(index, on, variableName, valueName)

        member this.Melt(index: seq<string>, on: seq<string>, variableName, valueName) = 
            this.Unpivot(index, on, variableName, valueName)

        member this.Melt(index: string list, on: string list) =
            this.Unpivot(index, on)

    type DataFrame with
        /// <summary> 
        /// Unpivot (Melt) the DataFrame from wide to long format using Selectors.
        /// This is the primary implementation backed by native binding.
        /// </summary>
        /// <param name="index">Selector for ID variables (columns to keep)</param>
        /// <param name="on">Selector for Value variables (columns to melt)</param>
        /// <param name="variableName">Name for the variable column (default: "variable")</param>
        /// <param name="valueName">Name for the value column (default: "value")</param>
        member this.Unpivot (index: Selector,on: Selector,variableName: string option,valueName: string option) : DataFrame =
            this.Lazy().Unpivot(index,on,variableName,valueName).Collect()

        /// <summary> 
        /// Unpivot (Melt) overload for simple string lists.
        /// Auto-converts string lists to Column Selectors.
        /// </summary>
        member this.Unpivot (index: seq<string>,on: seq<string>,variableName: string option,valueName: string option) =
            // 1. Index Selector
            let idxArr = Seq.toArray index
            let sIndex = new Selector(PolarsWrapper.SelectorCols idxArr)

            // 2. On (Value) Selector
            let onArr = Seq.toArray on
            let sOn = new Selector(PolarsWrapper.SelectorCols onArr)

            this.Unpivot(sIndex,sOn,variableName,valueName)
        member this.Unpivot (index: seq<string>,on: seq<string>) =
            this.Unpivot(index,on,None,None)
        /// <summary> Alias for Unpivot. </summary>
        member this.Melt(index: Selector, on: Selector, variableName, valueName) = 
            this.Unpivot(index, on, variableName, valueName)

        member this.Melt(index: seq<string>, on: seq<string>, variableName, valueName) = 
            this.Unpivot(index, on, variableName, valueName)

        member this.Melt(index: seq<string>, on: seq<string>) =
            this.Unpivot(index, on)