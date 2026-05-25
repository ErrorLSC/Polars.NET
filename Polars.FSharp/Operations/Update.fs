namespace Polars.FSharp

open System

[<AutoOpen>]
module UpdateOps = 
    type LazyFrame with
        member this.Update(
            other: LazyFrame,
            ?on: IColumnExpr,
            ?how: JoinType,
            ?leftOn: IColumnExpr,
            ?rightOn: IColumnExpr,
            ?includeNulls: bool,
            ?maintainOrder: JoinMaintainOrder
        ) : LazyFrame =
            let how = defaultArg how JoinType.Left
            if how <> JoinType.Left && how <> JoinType.Inner && how <> JoinType.Outer then
                invalidArg (nameof how) $"'how' must be one of {{Left; Inner; Outer}}; found '{how}'"

            let mutable rowIndexUsed = false
            let rowIndexName = "__POLARS_ROW_INDEX"

            let leftFrame, rightFrame, leftOnKeys, rightOnKeys =
                match on with
                | Some onCols ->
                    let cols = other |> pl.cs.expandLf onCols
                    if cols.Length = 0 then 
                        raise (ArgumentException "'on' Selector hits no columns, please review 'on' argument")
                    else this, other, cols, cols
                | None ->
                    match leftOn, rightOn with
                    | Some l, Some r ->
                        this, other, this |> pl.cs.expandLf l, other |> pl.cs.expandLf r
                    | None, None ->
                        rowIndexUsed <- true
                        let leftIdx = this.WithRowIndex(rowIndexName)
                        let rightIdx = other.WithRowIndex(rowIndexName)
                        let idx = [| rowIndexName |]
                        leftIdx, rightIdx, idx, idx
                    | Some _, None -> invalidArg "rightOn" "Missing join columns for right frame."
                    | None, Some _ -> invalidArg "leftOn" "Missing join columns for left frame."

            // Column name validation
            let leftCols = leftFrame.CollectSchema().Names |> Set.ofSeq
            let rightCols = rightFrame.CollectSchema().Names |> Set.ofSeq
            for name in leftOnKeys do
                if not (leftCols.Contains name) then
                    invalidArg "leftOn" $"Left join column '{name}' not found."
            for name in rightOnKeys do
                if not (rightCols.Contains name) then
                    invalidArg "rightOn" $"Right join column '{name}' not found."

            // Early return: if right frame contains nothing to update (all cols are join keys)
            if how <> JoinType.Outer && rightCols.Count = rightOnKeys.Length then
                if rowIndexUsed then leftFrame.Drop(rowIndexName) else leftFrame
            else
                // Columns to update = intersection of left & right, excluding join keys
                let rightOther =
                    Set.intersect leftCols rightCols
                    |> Set.filter (fun col -> not (rightOnKeys |> Array.contains col))
                    |> Set.toList

                let includeNulls = defaultArg includeNulls false
                let validityCol = if includeNulls then Some "__POLARS_VALIDITY" else None

                let rightFrame =
                    match validityCol with
                    | Some v -> rightFrame.WithColumns(pl.lit true |> pl.alias v)
                    | None -> rightFrame

                let tmpSuffix = "__POLARS_RIGHT"
                let dropColumns =
                    let baseDrops = rightOther |> List.map (fun name -> $"{name}{tmpSuffix}")
                    match validityCol with
                    | Some v -> v :: baseDrops
                    | None -> baseDrops

                // Right frame selection: join columns + other columns + validity column
                let rightSelectExprs =
                    [|
                        yield! rightOnKeys |> Array.map pl.col
                        yield! rightOther |> List.map pl.col
                        if validityCol.IsSome then yield pl.col validityCol.Value
                    |]

                // Perform the join
                let leftOnExprs = leftOnKeys |> Array.map pl.col
                let rightOnExprs = rightOnKeys |> Array.map pl.col

                let joined =
                    leftFrame.Join(
                        rightFrame.Select(rightSelectExprs),
                        leftOn = leftOnExprs,
                        rightOn = rightOnExprs,
                        how = how,
                        suffix = tmpSuffix,
                        maintainOrder = defaultArg maintainOrder JoinMaintainOrder.Left,
                        coalesce = JoinCoalesce.CoalesceColumns
                    )

                // Build update expressions
                let updateExprs =
                    rightOther
                    |> List.map (fun name ->
                        let rightColName = $"{name}{tmpSuffix}"
                        let expr =
                            match validityCol with
                            | Some v ->
                                pl.when'((pl.col v).IsNull())
                                    |> pl.then'(pl.col name)
                                    |> pl.otherwise(pl.col rightColName)
                            | None ->
                                pl.col(rightColName).Coalesce [pl.col name]
                        expr.Alias name
                    )

                let mutable result = joined
                if not updateExprs.IsEmpty then
                    result <- result.WithColumns(updateExprs |> List.toArray)

                if not dropColumns.IsEmpty then
                    result <- result.Drop(dropColumns |> List.toArray)

                if rowIndexUsed then
                    result <- result.Drop rowIndexName

                result
    type DataFrame with
        member this.Update(
            other: DataFrame,
            ?on: IColumnExpr,
            ?how: JoinType,
            ?leftOn: IColumnExpr,
            ?rightOn: IColumnExpr,
            ?includeNulls: bool,
            ?maintainOrder: JoinMaintainOrder
        ) =
            this.Lazy().Update(other.Lazy(),?on=on,?how=how,?leftOn=leftOn,?rightOn=rightOn,
                ?includeNulls=includeNulls,?maintainOrder=maintainOrder).Collect()