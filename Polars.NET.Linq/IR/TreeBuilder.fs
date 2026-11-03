namespace Polars.NET.Linq.IR

open System
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Linq

/// Constructs LogicalPlan relational trees from LINQ query stages or QueryOp streams.
module internal TreeBuilder =

    /// Translates an Expression into a ScalarExpr using available scope and column resolution context.
    let rec tryBuildScalar (ctx: TranslationContext) (expr: Expression) : ScalarExpr option =
        match ExprTranslator.unwrap expr with
        | :? ConstantExpression as ce ->
            Some(ScalarExpr.Literal(LiteralValue.OfObject ce.Value))

        | MemberAccess(target, m) when isNavigationMember m ->
            match ColumnResolver.tryResolveColumnNameWithContext ctx expr with
            | Some colName -> Some(ScalarExpr.Column colName)
            | None ->
                ExprTranslator.tryTranslateWithContext ctx expr
                |> Option.map (fun h ->
                    let name = defaultArg (Option.ofObj (PolarsWrapper.ExprGetOutputName h)) m.Name
                    ScalarExpr.Opaque(h, [ name ], expr.ToString()))

        | MemberAccess(_, m) when not (isNavigationMember m) ->
            match ColumnResolver.tryResolveColumnNameWithContext ctx expr with
            | Some colName -> Some(ScalarExpr.Column colName)
            | None ->
                ExprTranslator.tryTranslateWithContext ctx expr
                |> Option.map (fun h -> ScalarExpr.Opaque(h, [ m.Name ], expr.ToString()))

        | Binary(opType, left, right) ->
            let opOpt =
                match opType with
                | ExpressionType.Equal -> Some BinaryOp.Eq
                | ExpressionType.NotEqual -> Some BinaryOp.NotEq
                | ExpressionType.LessThan -> Some BinaryOp.Lt
                | ExpressionType.LessThanOrEqual -> Some BinaryOp.LtEq
                | ExpressionType.GreaterThan -> Some BinaryOp.Gt
                | ExpressionType.GreaterThanOrEqual -> Some BinaryOp.GtEq
                | ExpressionType.AndAlso
                | ExpressionType.And -> Some BinaryOp.And
                | ExpressionType.OrElse
                | ExpressionType.Or -> Some BinaryOp.Or
                | ExpressionType.ExclusiveOr -> Some BinaryOp.Xor
                | ExpressionType.Add -> Some BinaryOp.Add
                | ExpressionType.Subtract -> Some BinaryOp.Subtract
                | ExpressionType.Multiply -> Some BinaryOp.Multiply
                | ExpressionType.Divide -> Some BinaryOp.Divide
                | ExpressionType.Modulo -> Some BinaryOp.Modulo
                | _ -> None

            match opOpt with
            | Some op ->
                match tryBuildScalar ctx left, tryBuildScalar ctx right with
                | Some l, Some r -> Some(ScalarExpr.Binary(op, l, r))
                | _ ->
                    ExprTranslator.tryTranslateWithContext ctx expr
                    |> Option.map (fun h -> ScalarExpr.Opaque(h, [], expr.ToString()))
            | None ->
                ExprTranslator.tryTranslateWithContext ctx expr
                |> Option.map (fun h -> ScalarExpr.Opaque(h, [], expr.ToString()))

        | Unary(ExpressionType.Not, inner) ->
            tryBuildScalar ctx inner |> Option.map (fun i -> ScalarExpr.Unary(UnaryOp.Not, i))

        | Unary(ExpressionType.Negate, inner) ->
            tryBuildScalar ctx inner |> Option.map (fun i -> ScalarExpr.Unary(UnaryOp.Negate, i))

        | other ->
            ExprTranslator.tryTranslateWithContext ctx other
            |> Option.map (fun h ->
                let cols =
                    match ColumnResolver.tryResolveColumnNameWithContext ctx other with
                    | Some c -> [ c ]
                    | None ->
                        match Option.ofObj (PolarsWrapper.ExprGetOutputName h) with
                        | Some n when not (String.IsNullOrEmpty n) -> [ n ]
                        | _ -> []
                ScalarExpr.Opaque(h, cols, other.ToString()))

    /// Constructs a LogicalPlan tree from a base LazyFrameHandle and a sequence of QueryOps.
    let fromOps (baseLf: LazyFrameHandle) (ops: QueryOp list) : LogicalPlan =
        let initialPlan = LogicalPlan.Scan(LogicalPlanSource.LazyFrame baseLf, LogicalSchema.Empty)
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
                let rightPlan = LogicalPlan.Scan(LogicalPlanSource.LazyFrame spec.RightLf, LogicalSchema.Empty)
                let leftOn = spec.LeftOn |> Array.map (fun h -> ScalarExpr.Opaque(h, [], "LeftOn")) |> Array.toList
                let rightOn = spec.RightOn |> Array.map (fun h -> ScalarExpr.Opaque(h, [], "RightOn")) |> Array.toList
                if spec.InvertSides then
                    LogicalPlan.Join(rightPlan, currentPlan, rightOn, leftOn, spec.How, spec.Suffix, spec.Coalesce, spec.NullsEqual)
                else
                    LogicalPlan.Join(currentPlan, rightPlan, leftOn, rightOn, spec.How, spec.Suffix, spec.Coalesce, spec.NullsEqual)

            | QueryOp.JoinAsOf spec ->
                let rightPlan = LogicalPlan.Scan(LogicalPlanSource.LazyFrame spec.RightLf, LogicalSchema.Empty)
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
                let otherPlan = LogicalPlan.Scan(LogicalPlanSource.LazyFrame spec.OtherLf, LogicalSchema.Empty)
                if spec.Prepend then
                    LogicalPlan.Union(otherPlan, currentPlan, isAll = true)
                else
                    LogicalPlan.Union(currentPlan, otherPlan, isAll = true)

            | QueryOp.HorizontalConcat otherLfs ->
                let otherPlans =
                    otherLfs
                    |> Array.map (fun lf -> LogicalPlan.Scan(LogicalPlanSource.LazyFrame lf, LogicalSchema.Empty))
                    |> Array.toList
                LogicalPlan.HorizontalConcat(currentPlan :: otherPlans))
            initialPlan
