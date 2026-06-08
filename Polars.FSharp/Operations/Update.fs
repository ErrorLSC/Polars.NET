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

            let rowIndexName = "__POLARS_ROW_INDEX"

            let leftFrame, rightFrame, leftOnKeys, rightOnKeys, rowIndexUsed =
                match on with
                | Some onCols ->
                    let cols = other |> pl.cs.expandLf onCols
                    if cols.Length = 0 then 
                        raise (ArgumentException "'on' Selector hits no columns, please review 'on' argument")
                    this, other, cols, cols, false
                | None ->
                    match leftOn, rightOn with
                    | Some l, Some r ->
                        this, other, this |> pl.cs.expandLf l, other |> pl.cs.expandLf r, false
                    | None, None ->
                        let leftIdx = this.WithRowIndex rowIndexName
                        let rightIdx = other.WithRowIndex rowIndexName
                        let idx = [| rowIndexName |]
                        leftIdx, rightIdx, idx, idx, true
                    | Some _, None -> invalidArg "rightOn" "Missing join columns for right frame."
                    | None, Some _ -> invalidArg "leftOn" "Missing join columns for left frame."

            let leftSchema = leftFrame.CollectSchema()
            let rightSchema = rightFrame.CollectSchema()
            
            leftOnKeys |> Array.iter (fun name ->
                if not (leftSchema.ContainsKey name) then invalidArg "leftOn" $"Left join column '{name}' not found.")
            rightOnKeys |> Array.iter (fun name ->
                if not (rightSchema.ContainsKey name) then invalidArg "rightOn" $"Right join column '{name}' not found.")

            let leftCols = leftSchema.Names |> Set.ofSeq
            let rightCols = rightSchema.Names |> Set.ofSeq

            if how <> JoinType.Outer && rightCols.Count = rightOnKeys.Length then
                if rowIndexUsed then leftFrame.Drop rowIndexName else leftFrame
            else
                let rightOther =
                    Set.intersect leftCols rightCols
                    |> Set.filter (fun col -> not (rightOnKeys |> Array.contains col))
                    |> Set.toList

                let includeNulls = defaultArg includeNulls false
                let validityCol = if includeNulls then Some "__POLARS_VALIDITY" else None

                let rightFramePrepared =
                    match validityCol with
                    | Some v -> rightFrame.WithColumns(pl.lit true |> pl.alias v)
                    | None -> rightFrame

                let tmpSuffix = "__POLARS_RIGHT"
                let dropColumns = [
                    yield! rightOther |> List.map (fun name -> $"{name}{tmpSuffix}")
                    if validityCol.IsSome then yield validityCol.Value
                    if rowIndexUsed then yield rowIndexName
                ]

                let rightSelectExprs = [|
                    yield! rightOnKeys |> Array.map pl.col
                    yield! rightOther |> Array.ofList |> Array.map pl.col
                    if validityCol.IsSome then yield pl.col validityCol.Value
                |]

                let leftOnExprs = leftOnKeys |> Array.map pl.col
                let rightOnExprs = rightOnKeys |> Array.map pl.col

                let joined =
                    leftFrame.Join(
                        rightFramePrepared.Select rightSelectExprs,
                        leftOn = leftOnExprs,
                        rightOn = rightOnExprs,
                        how = how,
                        suffix = tmpSuffix,
                        maintainOrder = defaultArg maintainOrder JoinMaintainOrder.Left,
                        coalesce = JoinCoalesce.CoalesceColumns
                    )

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

                joined
                |> fun lf -> if not updateExprs.IsEmpty then lf.WithColumns(updateExprs |> List.toArray) else lf
                |> fun lf -> if not dropColumns.IsEmpty then lf.Drop(dropColumns |> List.toArray) else lf
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