namespace Polars.FSharp

[<AutoOpen>]
module DataFrameUnstackExtension =
    open System
    type DataFrame with
        member this.UnstackInternal (step: int64,
                    how: UnstackDirection,
                    resolvedColumns: string[],
                    ?fillValues: Expr seq) =
            // 1. Select or clone columns
            let mutable work = 
                if resolvedColumns.Length > 0 then this.Select(pl.cols resolvedColumns)
                else this.Clone()
            try
                let height = work.Height

                // 2. Calculate dimensions
                let nRows, nCols =
                    if how = UnstackDirection.Vertical then
                        let r = step
                        r, (height + r - 1L) / r
                    else
                        let c = step
                        (height + c - 1L) / c, c

                // 3. Padding if needed
                let nFill = nCols * nRows - height
                if nFill > 0L then
                    let w = Convert.ToInt32 work.Width
                    // Prepare fill expressions
                    let fillExprs =
                        match fillValues with
                        | Some exprs ->
                            let arr = Seq.toArray exprs
                            if arr.Length = w then arr
                            elif arr.Length > 0 then Array.create w arr.[0]
                            else Array.create w (pl.litNull())
                        | None -> Array.create w (pl.litNull())

                    let schema = work.Schema
                    let cols = work.Columns |> Seq.toArray
                    let extendExprs =
                        cols |> Array.mapi (fun i colName ->
                            let colType = schema.[colName]
                            let fillExpr = fillExprs.[i].Cast colType
                            let nFillExpr = lit nFill
                            (pl.col colName).ExtendConstant(fillExpr, nFillExpr).Alias colName)
                    let newWork = work.Select extendExprs
                    work.Dispose()
                    work <- newWork

                // 4. Horizontal sort (only for horizontal unstack)
                if how = UnstackDirection.Horizontal then
                    let sortExpr =
                        pl.intRange<int64> 0L (nCols * nRows) 1L % pl.lit nCols
                        |> pl.alias "__sort_order"
                    let newWork = work.WithColumns(sortExpr).Sort("__sort_order").Drop("__sort_order")
                    work.Dispose()
                    work <- newWork

                // 5. Slice columns and rename
                let zfillVal = int (Math.Floor(Math.Log10(float nCols))) + 1
                let slices = ResizeArray<Series>()
                for s in work.GetColumns() do
                    let colName = s.Name
                    for sliceNbr in 0L .. nCols - 1L do
                        let sliced = s.Slice(sliceNbr * nRows, uint64 nRows)
                        let slicedRenamed = sliced.Rename $"{colName}_{sliceNbr.ToString().PadLeft(zfillVal, '0')}"
                        slices.Add slicedRenamed
                    s.Dispose()

                let result = DataFrame.create slices
                result
            finally
                work.Dispose()
        /// <summary>
        /// Unstack a long table to a wide form without doing an aggregation.
        /// This can be much faster than a pivot, because it can skip the grouping phase.
        /// </summary>
        /// <param name="step">Number of rows in the unstacked frame.</param>
        /// <param name="how">Direction of the unstack.</param>
        /// <param name="columns">Column name(s) or selector(s) to include in the operation. If set to None (default), use all columns.</param>
        /// <param name="fillValues">Fill values that don’t fit the new size with this value.</param>
        member this.Unstack(step:int64,?columns:Selector,?how:UnstackDirection,?fillValues:seq<Expr>) =
            let direction = defaultArg how UnstackDirection.Vertical
            let cols =
                match columns with
                | Some c -> c 
                | None -> pl.cs.all()
            let resolvedCols = this |> pl.cs.expandDf cols
            this.UnstackInternal(step,direction,resolvedCols,?fillValues=fillValues)