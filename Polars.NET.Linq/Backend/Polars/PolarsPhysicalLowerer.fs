namespace Polars.NET.Linq.Backend.Polars

open System
open Polars.NET.Core
open Polars.NET.Core.Wrappers
open Polars.NET.Core.Enums
open Polars.NET.Core.Helpers
open Polars.NET.Core.Config
open Polars.NET.Linq
open Polars.NET.Linq.IR

/// Physical Polars LazyFrame implementation of ILogicalTableSource.
type internal PolarsTableSource(handle: LazyFrameHandle, schema: LogicalSchema) =
    member _.Handle = handle
    interface ILogicalTableSource with
        member _.Name = "PolarsLazyFrame"
        member _.Schema = schema

/// Physical lowerer that translates LogicalPlan trees into native Polars operations.
module internal PolarsPhysicalLowerer =

    let private createEmptyLazyFrame () : LazyFrameHandle =
        let df = PolarsWrapper.DataFrameNew ReadOnlySpan.Empty
        PolarsWrapper.DataFrameToLazy df

    /// The schema, with column types, of a physical LazyFrameHandle.
    let schemaOfLazyFrame (lf: LazyFrameHandle) : LogicalSchema =
        PolarsSchemaExtractor.GetFields lf
        |> Array.map (fun struct (name, dataType) ->
            use dataType = dataType
            name, PolarsVocabulary.ofDataTypeHandle dataType)
        |> LogicalSchema.OfFields

    /// Wraps a physical LazyFrameHandle as an opaque ILogicalTableSource (schema, with column types, read from the frame).
    let tableSourceOfLazyFrame (lf: LazyFrameHandle) : ILogicalTableSource =
        PolarsTableSource(lf, schemaOfLazyFrame lf)

    /// Materializes an in-memory row snapshot into a LazyFrame, verifying that the transposed columns
    /// match the schema the frontend declared for the Scan node.
    let private materializeInMemory (rows: Array) (elemType: Type) (declared: LogicalSchema) : LazyFrameHandle =
        let df = MaterializerInvokers.FromRowsMethod.MakeGenericMethod(elemType).Invoke(null, [| box rows |]) :?> DataFrameHandle
        let lf = PolarsWrapper.DataFrameToLazy df
        let actual = PolarsSchemaExtractor.GetColumnNames lf |> Array.toList
        if actual <> declared.ColumnNames then
            failwithf
                "[Polars.NET.Linq Bug] InMemory(%s) declared columns [%s] but FromRows produced [%s]."
                elemType.Name
                (String.concat ", " declared.ColumnNames)
                (String.concat ", " actual)
        lf

    /// Recursively lowers a LogicalPlan into an optimized LazyFrameHandle, reading each Scan slot from `sources`.
    /// Whether a slice over this plan would reach an order-keeping GroupBy through nodes Polars pushes a slice
    /// through (projections, renames), i.e. whether the slice selects groups by their order.
    let rec private slicesOrderedGroups (plan: LogicalPlan) : bool =
        match plan with
        | LogicalPlan.GroupBy(_, _, _, _, maintainOrder) -> maintainOrder
        | LogicalPlan.Project(input, _)
        | LogicalPlan.WithColumns(input, _)
        | LogicalPlan.Rename(input, _, _) -> slicesOrderedGroups input
        | _ -> false

    let rec lowerPlan (sources: SourceBinding array) (plan: LogicalPlan) : LazyFrameHandle =
        match plan with
        | LogicalPlan.Scan(source, schema) ->
            match source with
            | LogicalPlanSource.Slot index ->
                if index < 0 || index >= sources.Length then
                    failwithf "[Polars.NET.Linq Bug] Scan slot %d has no source binding (%d bound)." index sources.Length
                match sources.[index] with
                | SourceBinding.Table(:? PolarsTableSource as pts) -> PolarsWrapper.LazyClone pts.Handle
                | SourceBinding.Table other -> failwithf "Unsupported table source in PolarsPhysicalLowerer: %s" other.Name
                | SourceBinding.InMemory(rows, elemType) -> materializeInMemory rows elemType schema
            | LogicalPlanSource.SubqueryRef id -> failwithf "Unresolved SubqueryRef: %s" id
            | LogicalPlanSource.Empty -> createEmptyLazyFrame ()

        | LogicalPlan.Filter(input, pred) ->
            let lf = lowerPlan sources input
            let exprH = PolarsScalarLowerer.lower pred
            PolarsWrapper.LazyFilter(lf, exprH)

        | LogicalPlan.Project(input, projs) ->
            let lf = lowerPlan sources input
            let exprs =
                projs
                |> List.map (fun (name, expr) ->
                    let h = PolarsScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray

            // A Polars select of expressions that read no column (constants) yields a single row; a projection
            // yields one row per input row, so such constants are added as columns, which broadcast to the
            // frame's height, and then selected.
            let isRowIndependent (_, expr) =
                (ScalarExprOps.collectColumns expr).IsEmpty && not (ScalarExprOps.containsAggregation expr)

            if not projs.IsEmpty && projs |> List.forall isRowIndependent then
                let withConstants = PolarsWrapper.LazyWithColumns(lf, exprs)
                let names = projs |> List.map (fst >> PolarsWrapper.Col) |> List.toArray
                PolarsWrapper.LazySelect(withConstants, names)
            else
                PolarsWrapper.LazySelect(lf, exprs)

        | LogicalPlan.WithColumns(input, cols) ->
            let lf = lowerPlan sources input
            let exprs =
                cols
                |> List.map (fun (name, expr) ->
                    let h = PolarsScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray
            PolarsWrapper.LazyWithColumns(lf, exprs)

        | LogicalPlan.Join(left, right, leftOn, rightOn, how, suffix, coalesce, nullsEqual) ->
            let leftLf = lowerPlan sources left
            let rightLf = lowerPlan sources right
            let leftOnHs = leftOn |> List.map PolarsScalarLowerer.lower |> List.toArray
            let rightOnHs = rightOn |> List.map PolarsScalarLowerer.lower |> List.toArray
            PolarsWrapper.Join(
                leftLf,
                rightLf,
                leftOnHs,
                rightOnHs,
                PolarsVocabulary.joinType how,
                Option.toObj suffix,
                PlJoinValidation.ManyToMany,
                PolarsVocabulary.joinCoalesce coalesce,
                PolarsVocabulary.joinMaintainOrder how,
                PlJoinSide.None,
                nullsEqual,
                Nullable(),
                0UL
            )

        | LogicalPlan.JoinAsOf(left, right, spec) ->
            let leftLf = lowerPlan sources left
            let rightLf = lowerPlan sources right
            let leftOnH = [| PolarsScalarLowerer.lower spec.LeftOn |]
            let rightOnH = [| PolarsScalarLowerer.lower spec.RightOn |]
            let leftByHs =
                if spec.LeftBy.IsEmpty then null
                else spec.LeftBy |> List.map PolarsScalarLowerer.lower |> List.toArray
            let rightByHs =
                if spec.RightBy.IsEmpty then null
                else spec.RightBy |> List.map PolarsScalarLowerer.lower |> List.toArray
            PolarsWrapper.JoinAsOf(
                leftLf,
                rightLf,
                leftOnH,
                rightOnH,
                leftByHs,
                rightByHs,
                PolarsVocabulary.asofStrategy spec.Strategy,
                Option.toObj spec.ToleranceStr,
                Option.toNullable spec.ToleranceInt,
                Option.toNullable spec.ToleranceFloat,
                spec.AllowEq,
                spec.CheckSorted,
                Option.toObj spec.Suffix,
                PlJoinValidation.ManyToMany,
                PolarsVocabulary.joinCoalesce spec.Coalesce,
                // An as-of join keeps every left row once, in left order.
                PlJoinMaintainOrder.Left,
                PlJoinSide.None,
                spec.NullsEqual,
                Nullable(),
                0UL
            )

        | LogicalPlan.GroupBy(input, keys, aggs, having, maintainOrder) ->
            let lf = lowerPlan sources input
            let keyHs = keys |> List.map PolarsScalarLowerer.lower |> List.toArray
            let aggHs =
                aggs
                |> List.map (fun (name, expr) ->
                    let h = PolarsScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray
            let havingH = having |> Option.map PolarsScalarLowerer.lower |> Option.toObj
            PolarsWrapper.LazyGroupByAgg(lf, keyHs, aggHs, havingH, maintainOrder)

        | LogicalPlan.GroupingSets(input, spec) ->
            let activeLf = lowerPlan sources input
            let numKeys = spec.KeyAliases.Length
            let keyAliasedHandles =
                spec.KeyAliases
                |> List.map (fun (alias, expr) -> PolarsWrapper.Alias(PolarsScalarLowerer.lower expr, alias))
                |> List.toArray
            let aggHandles =
                spec.Aggs
                |> List.map (fun (alias, expr) -> PolarsWrapper.Alias(PolarsScalarLowerer.lower expr, alias))
                |> List.toArray
            let outputAliasToKeyIdx =
                spec.KeyAliases
                |> List.mapi (fun idx (alias, _) -> alias, idx)
                |> Map.ofList

            let level0Keys = keyAliasedHandles |> Array.map PolarsWrapper.CloneExpr
            let level0Aggs = aggHandles |> Array.map PolarsWrapper.CloneExpr
            let level0GroupedLf = PolarsWrapper.LazyGroupByAgg(PolarsWrapper.LazyClone activeLf, level0Keys, level0Aggs, null, true)
            let level0SelectExprs =
                spec.OutputColumns
                |> List.map (fun name ->
                    match spec.Level0Literals.TryFind name with
                    | Some litExpr -> PolarsWrapper.Alias(PolarsScalarLowerer.lower litExpr, name)
                    | None -> PolarsWrapper.Col name)
                |> List.toArray
            let level0Lf = PolarsWrapper.LazySelect(level0GroupedLf, level0SelectExprs)
            let colTypeMap = getLazySchemaFields level0Lf |> Map.ofArray

            let compileLevel (subsetIndices: int list, levelLiterals: Map<string, ScalarExpr>) : LazyFrameHandle =
                if subsetIndices.Length = numKeys then
                    PolarsWrapper.LazyClone level0Lf
                else
                    let groupedLf =
                        if subsetIndices.IsEmpty then
                            let dummyKey = PolarsWrapper.Alias(PolarsWrapper.Lit 1, "__dummy_rollup_key__")
                            let levelAggs = aggHandles |> Array.map PolarsWrapper.CloneExpr
                            PolarsWrapper.LazyGroupByAgg(PolarsWrapper.LazyClone activeLf, [| dummyKey |], levelAggs, null, false)
                        else
                            let levelKeys = [| for i in subsetIndices -> PolarsWrapper.CloneExpr keyAliasedHandles.[i] |]
                            let levelAggs = aggHandles |> Array.map PolarsWrapper.CloneExpr
                            PolarsWrapper.LazyGroupByAgg(PolarsWrapper.LazyClone activeLf, levelKeys, levelAggs, null, true)

                    let subsetSet = Set.ofList subsetIndices
                    let projectedExprs =
                        spec.OutputColumns
                        |> List.map (fun name ->
                            match levelLiterals.TryFind name with
                            | Some(ScalarExpr.Literal(LiteralValue.Null _)) when colTypeMap.ContainsKey name ->
                                let dt = colTypeMap.[name]
                                let dtExpr = PolarsWrapper.DataTypeExprFromDataType dt
                                let nullExpr = PolarsWrapper.ExprCast(PolarsWrapper.LitNull(), dtExpr, strict = false, wrapNumerical = false)
                                PolarsWrapper.Alias(nullExpr, name)
                            | Some litExpr ->
                                PolarsWrapper.Alias(PolarsScalarLowerer.lower litExpr, name)
                            | None ->
                                match outputAliasToKeyIdx.TryFind name with
                                | Some keyIdx ->
                                    if subsetSet.Contains keyIdx then
                                        PolarsWrapper.Col name
                                    else
                                        let dt = colTypeMap.[name]
                                        let dtExpr = PolarsWrapper.DataTypeExprFromDataType dt
                                        let nullExpr = PolarsWrapper.ExprCast(PolarsWrapper.LitNull(), dtExpr, strict = false, wrapNumerical = false)
                                        PolarsWrapper.Alias(nullExpr, name)
                                | None ->
                                    PolarsWrapper.Col name)
                        |> List.toArray

                    PolarsWrapper.LazySelect(groupedLf, projectedExprs)

            let levelLfs = spec.Levels |> List.map compileLevel |> List.toArray
            PolarsWrapper.LazyConcat(levelLfs, PlConcatType.Vertical, false, true)

        | LogicalPlan.Sort(input, orderings) ->
            let lf = lowerPlan sources input
            let exprs = orderings |> List.map (fun o -> PolarsScalarLowerer.lower o.Expr) |> List.toArray
            let desc = orderings |> List.map (fun o -> o.Descending) |> List.toArray
            let nullsLast = orderings |> List.map (fun o -> o.NullsLast) |> List.toArray
            // LINQ ordering is stable: rows with equal keys keep their input order, unless the configuration lets
            // ties come out in any order.
            PolarsWrapper.LazyFrameSort(lf, exprs, desc, nullsLast, maintainOrder = not CoreConfig.AllowUnstableOrdering)

        | LogicalPlan.Limit(input, offset, count) when slicesOrderedGroups input ->
            // Polars (0.55) pushes a slice into a group_by and then, when nothing downstream observes the
            // order of the result (an aggregate, a semi join), drops the group_by's maintain_order although
            // the slice still selects groups by it. The slice is applied as a filter on a row index instead:
            // nothing pushes into the group_by, and the index observes its order. Same window as a slice:
            // [start, start + count) within the rows, start counting from the end when the offset is negative.
            let lf = lowerPlan sources input
            let indexName = "__slice_order_idx"
            let indexed = PolarsWrapper.LazyFrameWithRowIndex(lf, indexName, Nullable 0)
            let start () =
                if offset >= 0L then PolarsWrapper.Lit offset
                else PolarsWrapper.Add(PolarsWrapper.Len(), PolarsWrapper.Lit offset)
            let window =
                PolarsWrapper.And(
                    PolarsWrapper.GtEq(PolarsWrapper.Col indexName, start ()),
                    PolarsWrapper.Lt(PolarsWrapper.Col indexName, PolarsWrapper.Add(start (), PolarsWrapper.Lit(int64 count))))
            let sliced = PolarsWrapper.LazyFilter(indexed, window)
            PolarsWrapper.LazyFrameDrop(sliced, PolarsWrapper.SelectorCols [| indexName |])

        | LogicalPlan.Limit(input, offset, count) ->
            let lf = lowerPlan sources input
            PolarsWrapper.LazySlice(lf, offset, count)

        | LogicalPlan.Distinct(input, subsetOpt, keep, maintainOrder) ->
            let lf = lowerPlan sources input
            let selector =
                match subsetOpt with
                | Some cols when not cols.IsEmpty -> PolarsWrapper.SelectorCols(List.toArray cols)
                | _ -> null
            PolarsWrapper.LazyUnique(lf, selector, PolarsVocabulary.uniqueKeep keep, maintainOrder = maintainOrder)

        | LogicalPlan.Explode(input, cols, emptyAsNull, keepNulls) ->
            let lf = lowerPlan sources input
            let selector = PolarsWrapper.SelectorCols(List.toArray cols)
            PolarsWrapper.LazyExplode(lf, selector, emptyAsNull, keepNulls)

        | LogicalPlan.Unpivot(input, index, onOpt, varName, valName) ->
            let lf = lowerPlan sources input
            let indexSel = PolarsWrapper.SelectorCols(List.toArray index)
            let onSel =
                match onOpt with
                | Some cols when not cols.IsEmpty -> PolarsWrapper.SelectorCols(List.toArray cols)
                | _ -> null
            PolarsWrapper.LazyUnpivot(lf, indexSel, onSel, varName, valName)

        | LogicalPlan.Union(left, right, isAll) ->
            let leftLf = lowerPlan sources left
            let rightLf = lowerPlan sources right
            let alignedRightLf = alignSchema leftLf rightLf
            PolarsWrapper.LazyConcat([| leftLf; alignedRightLf |], PlConcatType.Vertical, false, isAll)

        | LogicalPlan.WithRowIndex(input, name, offsetOpt) ->
            let lf = lowerPlan sources input
            let offsetNullable =
                match offsetOpt with
                | Some o -> Nullable(int o)
                | None -> Nullable 0
            PolarsWrapper.LazyFrameWithRowIndex(lf, name, offsetNullable)

        | LogicalPlan.Rename(input, existingNames, newNames) ->
            let lf = lowerPlan sources input
            PolarsWrapper.LazyRename(lf, List.toArray existingNames, List.toArray newNames, strict = false)

        | LogicalPlan.Reverse input ->
            let lf = lowerPlan sources input
            PolarsWrapper.LazyFrameReverse lf

        | LogicalPlan.SkipLast(input, count) ->
            let lf = lowerPlan sources input
            let withIndexLf = PolarsWrapper.LazyFrameWithRowIndex(lf, "__row_idx", Nullable 0)
            let rowIdxCol = PolarsWrapper.Col "__row_idx"
            let countLit = PolarsWrapper.Lit(uint32 count)
            let lhsExpr = PolarsWrapper.Add(rowIdxCol, countLit)
            let totalLen = PolarsWrapper.Len()
            let filterExpr = PolarsWrapper.Lt(lhsExpr, totalLen)
            let filteredLf = PolarsWrapper.LazyFilter(withIndexLf, filterExpr)
            let dropSelector = PolarsWrapper.SelectorCols [| "__row_idx" |]
            PolarsWrapper.LazyFrameDrop(filteredLf, dropSelector)

        | LogicalPlan.HorizontalConcat inputs ->
            let allHandles = inputs |> List.map (lowerPlan sources) |> List.toArray
            PolarsWrapper.LazyConcat(allHandles, PlConcatType.Horizontal, false, true)

        | LogicalPlan.Empty _ ->
            createEmptyLazyFrame ()
