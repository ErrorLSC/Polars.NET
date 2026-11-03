namespace Polars.NET.Linq

open System
open Polars.NET.Core

[<AutoOpen>]
module internal NativeOpsApplier =

    /// Helper to extract all column names and their corresponding DataTypeHandle from a LazyFrame schema
    let getLazySchemaFields (lf: LazyFrameHandle) : (string * DataTypeHandle) array =
        let schema = PolarsWrapper.GetLazySchema lf
        let count = PolarsWrapper.GetSchemaLen schema

        Array.init (int count) (fun i ->
            let mutable name = null
            let mutable dtHandle = new DataTypeHandle()
            PolarsWrapper.GetSchemaFieldAt(schema, uint64 i, &name, &dtHandle)
            name, dtHandle)

    let alignSchema (baseLf: LazyFrameHandle) (targetLf: LazyFrameHandle) : LazyFrameHandle =
        let baseFields = getLazySchemaFields baseLf
        let targetFields = getLazySchemaFields targetLf

        let baseNames = baseFields |> Array.map fst
        let targetNames = targetFields |> Array.map fst

        if Set.ofArray baseNames = Set.ofArray targetNames then
            let targetFieldMap = targetFields |> Map.ofArray

            let reorderedAndCastedExprs =
                baseFields
                |> Array.map (fun (colName, baseDt) ->
                    let targetDt = targetFieldMap.[colName]
                    let colExpr = PolarsWrapper.Col colName

                    if not (PolarsWrapper.DataTypeEq(baseDt, targetDt)) then
                        let dtExpr = PolarsWrapper.DataTypeExprFromDataType baseDt
                        PolarsWrapper.ExprCast(colExpr, dtExpr, strict = false, wrapNumerical = false)
                    else
                        colExpr)

            PolarsWrapper.LazySelect(targetLf, reorderedAndCastedExprs)
        elif baseFields.Length = targetFields.Length then
            let projectedExprs =
                Array.zip baseFields targetFields
                |> Array.map (fun ((baseName, baseDt), (targetName, targetDt)) ->
                    let colExpr = PolarsWrapper.Col targetName
                    let castedExpr =
                        if not (PolarsWrapper.DataTypeEq(baseDt, targetDt)) then
                            let dtExpr = PolarsWrapper.DataTypeExprFromDataType baseDt
                            PolarsWrapper.ExprCast(colExpr, dtExpr, strict = false, wrapNumerical = false)
                        else
                            colExpr
                    PolarsWrapper.Alias(castedExpr, baseName))
            PolarsWrapper.LazySelect(targetLf, projectedExprs)
        else
            targetLf

    let applyNativeOps (sourceLf: LazyFrameHandle) (ops: QueryOp list) : LazyFrameHandle =
        ops
        |> List.fold
            (fun currentLf op ->
                match op with
                | QueryOp.WithColumns exprs -> PolarsWrapper.LazyWithColumns(currentLf, exprs)

                | QueryOp.WithRowIndex(name, offsetOpt) ->
                    let offsetNullable =
                        match offsetOpt with
                        | Some o -> Nullable(int o)
                        | None -> Nullable 0

                    PolarsWrapper.LazyFrameWithRowIndex(currentLf, name, offsetNullable)

                | QueryOp.Filter exprHandle -> PolarsWrapper.LazyFilter(currentLf, exprHandle)

                | QueryOp.Select exprs -> PolarsWrapper.LazySelect(currentLf, exprs)

                | QueryOp.Slice(offset, len) -> PolarsWrapper.LazySlice(currentLf, offset, len)

                | QueryOp.Sort specs ->
                    let exprs = specs |> List.map (fun s -> s.Expr) |> List.toArray
                    let descending = specs |> List.map (fun s -> s.Descending) |> List.toArray
                    let nullsLast = specs |> List.map (fun s -> s.NullsLast) |> List.toArray
                    PolarsWrapper.LazyFrameSort(currentLf, exprs, descending, nullsLast, maintainOrder = false)

                | QueryOp.Join spec ->
                    let leftLf, rightLf, leftOn, rightOn, suffix =
                        if spec.InvertSides then
                            spec.RightLf,
                            currentLf,
                            spec.RightOn,
                            spec.LeftOn,
                            Option.toObj (spec.Suffix |> Option.orElse (Some LinqConstants.DefaultLeftSuffix))
                        else
                            currentLf, spec.RightLf, spec.LeftOn, spec.RightOn, Option.toObj spec.Suffix

                    PolarsWrapper.Join(
                        leftLf,
                        rightLf,
                        leftOn,
                        rightOn,
                        spec.How,
                        suffix,
                        PlJoinValidation.ManyToMany,
                        spec.Coalesce,
                        PlJoinMaintainOrder.None,
                        PlJoinSide.None,
                        spec.NullsEqual,
                        Nullable(),
                        0UL
                    )

                | QueryOp.JoinAsOf spec ->
                    PolarsWrapper.JoinAsOf(
                        currentLf,
                        spec.RightLf,
                        spec.LeftOn,
                        spec.RightOn,
                        Option.toObj spec.LeftBy,
                        Option.toObj spec.RightBy,
                        spec.Strategy,
                        Option.toObj spec.ToleranceStr,
                        Option.toNullable spec.ToleranceInt,
                        Option.toNullable spec.ToleranceFloat,
                        spec.AllowEq,
                        spec.CheckSorted,
                        Option.toObj spec.Suffix,
                        spec.Validation,
                        spec.Coalesce,
                        spec.MaintainOrder,
                        spec.JoinSide,
                        spec.NullsEqual,
                        Option.toNullable spec.SliceOffset,
                        spec.SliceLen
                    )

                | QueryOp.GroupBy spec ->
                    PolarsWrapper.LazyGroupByAgg(
                        currentLf,
                        spec.Keys,
                        spec.Aggs,
                        Option.toObj spec.Having,
                        spec.MaintainOrder
                    )

                | QueryOp.Unique spec ->
                    let selector =
                        match spec.SubsetCols with
                        | Some cols when cols.Length > 0 -> PolarsWrapper.SelectorCols cols
                        | _ -> null

                    PolarsWrapper.LazyUnique(currentLf, selector, spec.Keep, spec.MaintainOrder)

                | QueryOp.Concat spec ->
                    let alignedOtherLf = alignSchema currentLf spec.OtherLf

                    let handles =
                        if spec.Prepend then
                            [| alignedOtherLf; currentLf |]
                        else
                            [| currentLf; alignedOtherLf |]

                    PolarsWrapper.LazyConcat(handles, PlConcatType.Vertical, false, true)

                | QueryOp.HorizontalConcat otherLfs ->
                    let allHandles = Array.append [| currentLf |] otherLfs
                    PolarsWrapper.LazyConcat(allHandles, PlConcatType.Horizontal, false, true)

                | QueryOp.Explode spec ->
                    let selector = PolarsWrapper.SelectorCols spec.ColumnNames
                    PolarsWrapper.LazyExplode(currentLf, selector, spec.EmptyAsNull, spec.KeepNulls)

                | QueryOp.Unpivot spec ->
                    let indexSel = PolarsWrapper.SelectorCols spec.Index
                    let onSel =
                        match spec.On with
                        | Some onCols when onCols.Length > 0 -> PolarsWrapper.SelectorCols onCols
                        | _ -> null
                    PolarsWrapper.LazyUnpivot(currentLf, indexSel, onSel, spec.VariableName, spec.ValueName)

                | QueryOp.Rename spec ->
                    PolarsWrapper.LazyRename(currentLf, spec.ExistingNames, spec.NewNames, strict = false)

                | QueryOp.SkipLast count ->
                    let withIndexLf =
                        PolarsWrapper.LazyFrameWithRowIndex(currentLf, "__row_idx", Nullable 0)

                    let rowIdxCol = PolarsWrapper.Col "__row_idx"
                    let countLit = PolarsWrapper.Lit(uint32 count)
                    let lhsExpr = PolarsWrapper.Add(rowIdxCol, countLit)
                    let totalLen = PolarsWrapper.Len()
                    let filterExpr = PolarsWrapper.Lt(lhsExpr, totalLen)

                    let filteredLf = PolarsWrapper.LazyFilter(withIndexLf, filterExpr)
                    let dropSelector = PolarsWrapper.SelectorCols [| "__row_idx" |]
                    PolarsWrapper.LazyFrameDrop(filteredLf, dropSelector)

                | QueryOp.Reverse -> PolarsWrapper.LazyFrameReverse currentLf

                | QueryOp.SelectPassthrough -> currentLf)
            (PolarsWrapper.LazyClone sourceLf)
