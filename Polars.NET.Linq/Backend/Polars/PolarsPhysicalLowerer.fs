namespace Polars.NET.Linq.Backend.Polars

open System
open Polars.NET.Core
open Polars.NET.Core.Helpers
open Polars.NET.Linq
open Polars.NET.Linq.IR

/// Physical lowerer that translates LogicalPlan trees into native Polars operations.
module internal PolarsPhysicalLowerer =

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
            let exprH = PolarsScalarLowerer.lower pred
            PolarsWrapper.LazyFilter(lf, exprH)

        | LogicalPlan.Project(input, projs) ->
            let lf = lowerPlan input
            let exprs =
                projs
                |> List.map (fun (name, expr) ->
                    let h = PolarsScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray
            PolarsWrapper.LazySelect(lf, exprs)

        | LogicalPlan.WithColumns(input, cols) ->
            let lf = lowerPlan input
            let exprs =
                cols
                |> List.map (fun (name, expr) ->
                    let h = PolarsScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray
            PolarsWrapper.LazyWithColumns(lf, exprs)

        | LogicalPlan.Join(left, right, leftOn, rightOn, how, suffix, coalesce, nullsEqual) ->
            let leftLf = lowerPlan left
            let rightLf = lowerPlan right
            let leftOnHs = leftOn |> List.map PolarsScalarLowerer.lower |> List.toArray
            let rightOnHs = rightOn |> List.map PolarsScalarLowerer.lower |> List.toArray
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
            let keyHs = keys |> List.map PolarsScalarLowerer.lower |> List.toArray
            let aggHs =
                aggs
                |> List.map (fun (name, expr) ->
                    let h = PolarsScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray
            let havingH = having |> Option.map PolarsScalarLowerer.lower |> Option.toObj
            PolarsWrapper.LazyGroupByAgg(lf, keyHs, aggHs, havingH, maintainOrder)

        | LogicalPlan.Sort(input, orderings) ->
            let lf = lowerPlan input
            let exprs = orderings |> List.map (fun o -> PolarsScalarLowerer.lower o.Expr) |> List.toArray
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
            baseLf, ops @ [ QueryOp.Filter(PolarsScalarLowerer.lower pred) ]

        | LogicalPlan.Project(input, projs) ->
            let baseLf, ops = lowerToOps input
            let exprs =
                projs
                |> List.map (fun (name, expr) ->
                    let h = PolarsScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray
            baseLf, ops @ [ QueryOp.Select exprs ]

        | LogicalPlan.WithColumns(input, cols) ->
            let baseLf, ops = lowerToOps input
            let exprs =
                cols
                |> List.map (fun (name, expr) ->
                    let h = PolarsScalarLowerer.lower expr
                    PolarsWrapper.Alias(h, name))
                |> List.toArray
            baseLf, ops @ [ QueryOp.WithColumns exprs ]

        | LogicalPlan.Sort(input, orderings) ->
            let baseLf, ops = lowerToOps input
            let sortSpecs =
                orderings
                |> List.map (fun o ->
                    {
                        SortSpec.Expr = PolarsScalarLowerer.lower o.Expr
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
                    LeftOn = leftOn |> List.map PolarsScalarLowerer.lower |> List.toArray
                    RightOn = rightOn |> List.map PolarsScalarLowerer.lower |> List.toArray
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
                    LeftOn = [| PolarsScalarLowerer.lower spec.LeftOn |]
                    RightOn = [| PolarsScalarLowerer.lower spec.RightOn |]
                    LeftBy = if spec.LeftBy.IsEmpty then None else Some(spec.LeftBy |> List.map PolarsScalarLowerer.lower |> List.toArray)
                    RightBy = if spec.RightBy.IsEmpty then None else Some(spec.RightBy |> List.map PolarsScalarLowerer.lower |> List.toArray)
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
                    Keys = keys |> List.map PolarsScalarLowerer.lower |> List.toArray
                    Aggs =
                        aggs
                        |> List.map (fun (name, expr) ->
                            let h = PolarsScalarLowerer.lower expr
                            PolarsWrapper.Alias(h, name))
                        |> List.toArray
                    Having = having |> Option.map PolarsScalarLowerer.lower
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

    /// Constructs a LogicalPlan tree from a base LazyFrameHandle and a sequence of QueryOps.
    let fromOps (baseLf: LazyFrameHandle) (ops: QueryOp list) : LogicalPlan =
        let initialCols = PolarsSchemaExtractor.GetColumnNames baseLf |> Array.toList
        let initialPlan = LogicalPlan.Scan(LogicalPlanSource.LazyFrame baseLf, LogicalSchema.OfNames initialCols)
        ops
        |> List.fold (fun currentPlan op ->
            match op with
            | QueryOp.Filter exprH ->
                let scalar = ScalarExpr.Opaque(exprH, [], "Filter")
                LogicalPlan.Filter(currentPlan, scalar)

            | QueryOp.Select exprs ->
                let projs =
                    exprs
                    |> Array.mapi (fun i h ->
                        let name = defaultArg (Option.ofObj (PolarsWrapper.ExprGetOutputName h)) (sprintf "col_%d" i)
                        name, ScalarExpr.Opaque(h, [ name ], "Select"))
                    |> Array.toList
                LogicalPlan.Project(currentPlan, projs)

            | QueryOp.WithColumns exprs ->
                let cols =
                    exprs
                    |> Array.mapi (fun i h ->
                        let name = defaultArg (Option.ofObj (PolarsWrapper.ExprGetOutputName h)) (sprintf "col_%d" i)
                        name, ScalarExpr.Opaque(h, [ name ], "WithColumns"))
                    |> Array.toList
                LogicalPlan.WithColumns(currentPlan, cols)

            | QueryOp.Sort specs ->
                let orderings =
                    specs
                    |> List.map (fun s ->
                        {
                            SortOrder.Expr = ScalarExpr.Opaque(s.Expr, [], "Sort")
                            Descending = s.Descending
                            NullsLast = s.NullsLast
                        })
                LogicalPlan.Sort(currentPlan, orderings)

            | QueryOp.Slice(offset, len) ->
                LogicalPlan.Limit(currentPlan, offset, len)

            | QueryOp.SkipLast count ->
                LogicalPlan.SkipLast(currentPlan, count)

            | QueryOp.Unique spec ->
                let subset = spec.SubsetCols |> Option.map Array.toList
                LogicalPlan.Distinct(currentPlan, subset, spec.Keep)

            | QueryOp.WithRowIndex(name, offsetOpt) ->
                LogicalPlan.WithRowIndex(currentPlan, name, offsetOpt)

            | QueryOp.Rename spec ->
                LogicalPlan.Rename(currentPlan, Array.toList spec.ExistingNames, Array.toList spec.NewNames)

            | QueryOp.Explode spec ->
                LogicalPlan.Explode(currentPlan, Array.toList spec.ColumnNames, spec.EmptyAsNull, spec.KeepNulls)

            | QueryOp.Unpivot spec ->
                LogicalPlan.Unpivot(currentPlan, Array.toList spec.Index, spec.On |> Option.map Array.toList, spec.VariableName, spec.ValueName)

            | QueryOp.Reverse ->
                LogicalPlan.Reverse currentPlan

            | QueryOp.SelectPassthrough ->
                currentPlan

            | QueryOp.Join spec ->
                let rightCols = PolarsSchemaExtractor.GetColumnNames spec.RightLf |> Array.toList
                let rightPlan = LogicalPlan.Scan(LogicalPlanSource.LazyFrame spec.RightLf, LogicalSchema.OfNames rightCols)
                let leftOn = spec.LeftOn |> Array.map (fun h -> ScalarExpr.Opaque(h, [], "LeftOn")) |> Array.toList
                let rightOn = spec.RightOn |> Array.map (fun h -> ScalarExpr.Opaque(h, [], "RightOn")) |> Array.toList
                if spec.InvertSides then
                    LogicalPlan.Join(rightPlan, currentPlan, rightOn, leftOn, spec.How, spec.Suffix, spec.Coalesce, spec.NullsEqual)
                else
                    LogicalPlan.Join(currentPlan, rightPlan, leftOn, rightOn, spec.How, spec.Suffix, spec.Coalesce, spec.NullsEqual)

            | QueryOp.JoinAsOf spec ->
                let rightCols = PolarsSchemaExtractor.GetColumnNames spec.RightLf |> Array.toList
                let rightPlan = LogicalPlan.Scan(LogicalPlanSource.LazyFrame spec.RightLf, LogicalSchema.OfNames rightCols)
                let leftOn = spec.LeftOn |> Array.head |> fun h -> ScalarExpr.Opaque(h, [], "LeftOn")
                let rightOn = spec.RightOn |> Array.head |> fun h -> ScalarExpr.Opaque(h, [], "RightOn")
                let leftBy =
                    match spec.LeftBy with
                    | Some hs -> hs |> Array.map (fun h -> ScalarExpr.Opaque(h, [], "LeftBy")) |> Array.toList
                    | None -> []
                let rightBy =
                    match spec.RightBy with
                    | Some hs -> hs |> Array.map (fun h -> ScalarExpr.Opaque(h, [], "RightBy")) |> Array.toList
                    | None -> []
                let logicalSpec =
                    {
                        LeftOn = leftOn
                        RightOn = rightOn
                        LeftBy = leftBy
                        RightBy = rightBy
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
                    }
                LogicalPlan.JoinAsOf(currentPlan, rightPlan, logicalSpec)

            | QueryOp.GroupBy spec ->
                let keys = spec.Keys |> Array.map (fun h -> ScalarExpr.Opaque(h, [], "GroupKey")) |> Array.toList
                let aggs =
                    spec.Aggs
                    |> Array.mapi (fun i h ->
                        let name = defaultArg (Option.ofObj (PolarsWrapper.ExprGetOutputName h)) (sprintf "agg_%d" i)
                        name, ScalarExpr.Opaque(h, [ name ], "Agg"))
                    |> Array.toList
                let having = spec.Having |> Option.map (fun h -> ScalarExpr.Opaque(h, [], "Having"))
                LogicalPlan.GroupBy(currentPlan, keys, aggs, having, spec.MaintainOrder)

            | QueryOp.Concat spec ->
                let otherCols = PolarsSchemaExtractor.GetColumnNames spec.OtherLf |> Array.toList
                let otherPlan = LogicalPlan.Scan(LogicalPlanSource.LazyFrame spec.OtherLf, LogicalSchema.OfNames otherCols)
                if spec.Prepend then
                    LogicalPlan.Union(otherPlan, currentPlan, isAll = true)
                else
                    LogicalPlan.Union(currentPlan, otherPlan, isAll = true)

            | QueryOp.HorizontalConcat otherLfs ->
                let otherPlans =
                    otherLfs
                    |> Array.map (fun lf ->
                        let cols = PolarsSchemaExtractor.GetColumnNames lf |> Array.toList
                        LogicalPlan.Scan(LogicalPlanSource.LazyFrame lf, LogicalSchema.OfNames cols))
                    |> Array.toList
                LogicalPlan.HorizontalConcat(currentPlan :: otherPlans))
            initialPlan
