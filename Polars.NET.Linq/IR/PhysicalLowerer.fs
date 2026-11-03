namespace Polars.NET.Linq.IR

open System
open Polars.NET.Core
open Polars.NET.Linq

/// Physical lowerer that translates LogicalPlan trees into native Polars operations.
module internal PhysicalLowerer =

    let private createEmptyLazyFrame () : LazyFrameHandle =
        let df = PolarsWrapper.DataFrameNew ReadOnlySpan.Empty
        PolarsWrapper.DataFrameToLazy df

    /// Recursively lowers a LogicalPlan into an optimized LazyFrameHandle.
    let rec lowerPlan (plan: LogicalPlan) : LazyFrameHandle =
        match plan with
        | LogicalPlan.Scan(source, _) ->
            match source with
            | LogicalPlanSource.LazyFrame lf -> PolarsWrapper.LazyClone lf
            | LogicalPlanSource.SubqueryRef id -> failwithf "Unresolved SubqueryRef: %s" id
            | LogicalPlanSource.InMemoryTable(name, _) -> failwithf "InMemoryTable not yet supported directly: %s" name
            | LogicalPlanSource.Empty -> createEmptyLazyFrame ()

        | LogicalPlan.Filter(input, pred) ->
            let lf = lowerPlan input
            let exprH = ScalarLowerer.lower pred
            PolarsWrapper.LazyFilter(lf, exprH)

        | LogicalPlan.Project(input, projs) ->
            let lf = lowerPlan input
            let exprs =
                projs
                |> List.map (fun (name, expr) ->
                    let h = ScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray
            PolarsWrapper.LazySelect(lf, exprs)

        | LogicalPlan.WithColumns(input, cols) ->
            let lf = lowerPlan input
            let exprs =
                cols
                |> List.map (fun (name, expr) ->
                    let h = ScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray
            PolarsWrapper.LazyWithColumns(lf, exprs)

        | LogicalPlan.Join(left, right, leftOn, rightOn, how, suffix, coalesce, nullsEqual) ->
            let leftLf = lowerPlan left
            let rightLf = lowerPlan right
            let leftOnHs = leftOn |> List.map ScalarLowerer.lower |> List.toArray
            let rightOnHs = rightOn |> List.map ScalarLowerer.lower |> List.toArray
            PolarsWrapper.Join(
                leftLf,
                rightLf,
                leftOnHs,
                rightOnHs,
                how,
                Option.toObj suffix,
                PlJoinValidation.ManyToMany,
                coalesce,
                PlJoinMaintainOrder.None,
                PlJoinSide.None,
                nullsEqual,
                Nullable(),
                0UL
            )

        | LogicalPlan.JoinAsOf(left, right, spec) ->
            let leftLf = lowerPlan left
            let rightLf = lowerPlan right
            let leftOnH = [| ScalarLowerer.lower spec.LeftOn |]
            let rightOnH = [| ScalarLowerer.lower spec.RightOn |]
            let leftByHs =
                if spec.LeftBy.IsEmpty then null
                else spec.LeftBy |> List.map ScalarLowerer.lower |> List.toArray
            let rightByHs =
                if spec.RightBy.IsEmpty then null
                else spec.RightBy |> List.map ScalarLowerer.lower |> List.toArray
            PolarsWrapper.JoinAsOf(
                leftLf,
                rightLf,
                leftOnH,
                rightOnH,
                leftByHs,
                rightByHs,
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
                Nullable(),
                0UL
            )

        | LogicalPlan.GroupBy(input, keys, aggs, having, maintainOrder) ->
            let lf = lowerPlan input
            let keyHs = keys |> List.map ScalarLowerer.lower |> List.toArray
            let aggHs =
                aggs
                |> List.map (fun (name, expr) ->
                    let h = ScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray
            let havingH = having |> Option.map ScalarLowerer.lower |> Option.toObj
            PolarsWrapper.LazyGroupByAgg(lf, keyHs, aggHs, havingH, maintainOrder)

        | LogicalPlan.Sort(input, orderings) ->
            let lf = lowerPlan input
            let exprs = orderings |> List.map (fun o -> ScalarLowerer.lower o.Expr) |> List.toArray
            let desc = orderings |> List.map (fun o -> o.Descending) |> List.toArray
            let nullsLast = orderings |> List.map (fun o -> o.NullsLast) |> List.toArray
            PolarsWrapper.LazyFrameSort(lf, exprs, desc, nullsLast, maintainOrder = false)

        | LogicalPlan.Limit(input, offset, count) ->
            let lf = lowerPlan input
            PolarsWrapper.LazySlice(lf, offset, count)

        | LogicalPlan.Distinct(input, subsetOpt, keep) ->
            let lf = lowerPlan input
            let selector =
                match subsetOpt with
                | Some cols when not cols.IsEmpty -> PolarsWrapper.SelectorCols(List.toArray cols)
                | _ -> null
            PolarsWrapper.LazyUnique(lf, selector, keep, maintainOrder = false)

        | LogicalPlan.Explode(input, cols, emptyAsNull, keepNulls) ->
            let lf = lowerPlan input
            let selector = PolarsWrapper.SelectorCols(List.toArray cols)
            PolarsWrapper.LazyExplode(lf, selector, emptyAsNull, keepNulls)

        | LogicalPlan.Unpivot(input, index, onOpt, varName, valName) ->
            let lf = lowerPlan input
            let indexSel = PolarsWrapper.SelectorCols(List.toArray index)
            let onSel =
                match onOpt with
                | Some cols when not cols.IsEmpty -> PolarsWrapper.SelectorCols(List.toArray cols)
                | _ -> null
            PolarsWrapper.LazyUnpivot(lf, indexSel, onSel, varName, valName)

        | LogicalPlan.Union(left, right, isAll) ->
            let leftLf = lowerPlan left
            let rightLf = lowerPlan right
            let alignedRightLf = alignSchema leftLf rightLf
            PolarsWrapper.LazyConcat([| leftLf; alignedRightLf |], PlConcatType.Vertical, false, isAll)

        | LogicalPlan.WithRowIndex(input, name, offsetOpt) ->
            let lf = lowerPlan input
            let offsetNullable =
                match offsetOpt with
                | Some o -> Nullable(int o)
                | None -> Nullable 0
            PolarsWrapper.LazyFrameWithRowIndex(lf, name, offsetNullable)

        | LogicalPlan.Rename(input, existingNames, newNames) ->
            let lf = lowerPlan input
            PolarsWrapper.LazyRename(lf, List.toArray existingNames, List.toArray newNames, strict = false)

        | LogicalPlan.Reverse input ->
            let lf = lowerPlan input
            PolarsWrapper.LazyFrameReverse lf

        | LogicalPlan.SkipLast(input, count) ->
            let lf = lowerPlan input
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
            let allHandles = inputs |> List.map lowerPlan |> List.toArray
            PolarsWrapper.LazyConcat(allHandles, PlConcatType.Horizontal, false, true)

        | LogicalPlan.Empty _ ->
            createEmptyLazyFrame ()

    /// Lowers a LogicalPlan into a base LazyFrameHandle and a linear list of QueryOp instructions.
    let rec lowerToOps (plan: LogicalPlan) : LazyFrameHandle * QueryOp list =
        match plan with
        | LogicalPlan.Scan(source, _) ->
            match source with
            | LogicalPlanSource.LazyFrame lf -> PolarsWrapper.LazyClone lf, []
            | _ -> lowerPlan plan, []

        | LogicalPlan.Filter(input, pred) ->
            let baseLf, ops = lowerToOps input
            baseLf, ops @ [ QueryOp.Filter(ScalarLowerer.lower pred) ]

        | LogicalPlan.Project(input, projs) ->
            let baseLf, ops = lowerToOps input
            let exprs =
                projs
                |> List.map (fun (name, expr) ->
                    let h = ScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray
            baseLf, ops @ [ QueryOp.Select exprs ]

        | LogicalPlan.WithColumns(input, cols) ->
            let baseLf, ops = lowerToOps input
            let exprs =
                cols
                |> List.map (fun (name, expr) ->
                    let h = ScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray
            baseLf, ops @ [ QueryOp.WithColumns exprs ]

        | LogicalPlan.Sort(input, orderings) ->
            let baseLf, ops = lowerToOps input
            let sortSpecs =
                orderings
                |> List.map (fun o ->
                    {
                        Expr = ScalarLowerer.lower o.Expr
                        Descending = o.Descending
                        NullsLast = o.NullsLast
                    })
            baseLf, ops @ [ QueryOp.Sort sortSpecs ]

        | LogicalPlan.Limit(input, offset, count) ->
            let baseLf, ops = lowerToOps input
            baseLf, ops @ [ QueryOp.Slice(offset, count) ]

        | LogicalPlan.Distinct(input, subsetOpt, keep) ->
            let baseLf, ops = lowerToOps input
            let spec =
                {
                    SubsetCols = subsetOpt |> Option.map List.toArray
                    Keep = keep
                    MaintainOrder = false
                }
            baseLf, ops @ [ QueryOp.Unique spec ]

        | LogicalPlan.Explode(input, cols, emptyAsNull, keepNulls) ->
            let baseLf, ops = lowerToOps input
            let spec =
                {
                    ColumnNames = List.toArray cols
                    EmptyAsNull = emptyAsNull
                    KeepNulls = keepNulls
                }
            baseLf, ops @ [ QueryOp.Explode spec ]

        | LogicalPlan.Unpivot(input, index, onOpt, varName, valName) ->
            let baseLf, ops = lowerToOps input
            let spec =
                {
                    Index = List.toArray index
                    On = onOpt |> Option.map List.toArray
                    VariableName = varName
                    ValueName = valName
                }
            baseLf, ops @ [ QueryOp.Unpivot spec ]

        | LogicalPlan.WithRowIndex(input, name, offsetOpt) ->
            let baseLf, ops = lowerToOps input
            baseLf, ops @ [ QueryOp.WithRowIndex(name, offsetOpt) ]

        | LogicalPlan.Rename(input, existingNames, newNames) ->
            let baseLf, ops = lowerToOps input
            let spec =
                {
                    ExistingNames = List.toArray existingNames
                    NewNames = List.toArray newNames
                }
            baseLf, ops @ [ QueryOp.Rename spec ]

        | LogicalPlan.Reverse input ->
            let baseLf, ops = lowerToOps input
            baseLf, ops @ [ QueryOp.Reverse ]

        | LogicalPlan.SkipLast(input, count) ->
            let baseLf, ops = lowerToOps input
            baseLf, ops @ [ QueryOp.SkipLast count ]

        | LogicalPlan.Join(left, right, leftOn, rightOn, how, suffix, coalesce, nullsEqual) ->
            let leftLf, leftOps = lowerToOps left
            let rightLf = lowerPlan right
            let activeLeftLf = if leftOps.IsEmpty then leftLf else applyNativeOps leftLf leftOps
            let spec =
                {
                    RightLf = rightLf
                    LeftOn = leftOn |> List.map ScalarLowerer.lower |> List.toArray
                    RightOn = rightOn |> List.map ScalarLowerer.lower |> List.toArray
                    How = how
                    Suffix = suffix
                    InvertSides = false
                    Coalesce = coalesce
                    NullsEqual = nullsEqual
                }
            activeLeftLf, [ QueryOp.Join spec ]

        | LogicalPlan.JoinAsOf(left, right, spec) ->
            let leftLf, leftOps = lowerToOps left
            let rightLf = lowerPlan right
            let activeLeftLf = if leftOps.IsEmpty then leftLf else applyNativeOps leftLf leftOps
            let asOfSpec =
                {
                    RightLf = rightLf
                    LeftOn = [| ScalarLowerer.lower spec.LeftOn |]
                    RightOn = [| ScalarLowerer.lower spec.RightOn |]
                    LeftBy = if spec.LeftBy.IsEmpty then None else Some(spec.LeftBy |> List.map ScalarLowerer.lower |> List.toArray)
                    RightBy = if spec.RightBy.IsEmpty then None else Some(spec.RightBy |> List.map ScalarLowerer.lower |> List.toArray)
                    Strategy = spec.Strategy
                    ToleranceStr = spec.ToleranceStr
                    ToleranceInt = spec.ToleranceInt
                    ToleranceFloat = spec.ToleranceFloat
                    AllowEq = spec.AllowEq
                    CheckSorted = spec.CheckSorted
                    Suffix = spec.Suffix
                    Validation = spec.Validation
                    Coalesce = spec.Coalesce
                    MaintainOrder = spec.MaintainOrder
                    JoinSide = spec.JoinSide
                    NullsEqual = spec.NullsEqual
                    SliceOffset = None
                    SliceLen = 0UL
                }
            activeLeftLf, [ QueryOp.JoinAsOf asOfSpec ]

        | LogicalPlan.GroupBy(input, keys, aggs, having, maintainOrder) ->
            let inLf, inOps = lowerToOps input
            let activeLf = if inOps.IsEmpty then inLf else applyNativeOps inLf inOps
            let spec =
                {
                    Keys = keys |> List.map ScalarLowerer.lower |> List.toArray
                    Aggs =
                        aggs
                        |> List.map (fun (name, expr) ->
                            let h = ScalarLowerer.lower expr
                            PolarsWrapper.Alias(h, name))
                        |> List.toArray
                    Having = having |> Option.map ScalarLowerer.lower
                    MaintainOrder = maintainOrder
                }
            activeLf, [ QueryOp.GroupBy spec ]

        | LogicalPlan.Union(left, right, _) ->
            let leftLf = lowerPlan left
            let rightLf = lowerPlan right
            leftLf, [ QueryOp.Concat { OtherLf = rightLf; Prepend = false } ]

        | LogicalPlan.HorizontalConcat inputs ->
            let lf = lowerPlan plan
            lf, []

        | LogicalPlan.Empty _ ->
            createEmptyLazyFrame (), []
