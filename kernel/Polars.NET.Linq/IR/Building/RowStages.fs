namespace Polars.NET.Linq.IR

open System
open System.Linq.Expressions
open Polars.NET.Linq
open Polars.NET.Linq.IR.TransparentScopes
open Polars.NET.Linq.IR.ProjectionExtraction
open Polars.NET.Linq.IR.StageContext

/// Row-level stages: filters, projections, ordering and distinct-by.
/// Each handler compiles one stage onto the query state and continues with the remaining stages.
module internal RowStages =

    /// Filter: a native predicate, a decorrelated subquery predicate, or a client predicate.
    let filter (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (lam: LambdaExpression) : CompiledLogicalQuery =
        let makeCtx = StageContext.makeCtx rowShape
        match SubqueryDecorrelator.tryDecorrelateFilter env.ResolveSequence state.Plan lam rowShape with
        | Some (decorPlan, residualOpt) ->
            match residualOpt with
            | Some resLam ->
                let p = resLam.Parameters.[0]
                let ctx = makeCtx p.Type p.Name decorPlan
                match ExprTranslator.tryTranslateToScalar ctx resLam.Body with
                | Some pred -> continueWith { state with Plan = LogicalPlan.Filter(decorPlan, pred) } rowShape tail
                // The residual reads the decorrelated columns through placeholders a client predicate cannot
                // bind: the original predicate runs on the client instead, over the original rows.
                | None ->
                    if isUnmaterializableClientRow p.Type then
                        continueWith (fallback state) rowShape tail
                    else
                        continueWith (clientFilter rowShape lam state) rowShape tail
            | None ->
                continueWith { state with Plan = decorPlan } rowShape tail
        | None ->
            let p = lam.Parameters.[0]
            let ctx = makeCtx p.Type p.Name state.Plan
            match ExprTranslator.tryTranslateToScalar ctx lam.Body with
            | Some pred ->
                continueWith { state with Plan = LogicalPlan.Filter(state.Plan, pred) } rowShape tail
            | None ->
                if isUnmaterializableClientRow p.Type then
                    continueWith (fallback state) rowShape tail
                else
                    continueWith (clientFilter rowShape lam state) rowShape tail

    /// TakeWhile / SkipWhile: a cumulative predicate over the rows in order.
    let takeSkipWhile (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (sStage: LinqStage) (lam: LambdaExpression) : CompiledLogicalQuery =
        let makeCtx = StageContext.makeCtx rowShape
        let isSkip = match sStage with LinqStage.SkipWhile _ -> true | _ -> false
        let p = lam.Parameters.[0]
        let ctx = makeCtx p.Type p.Name state.Plan
        match ExprTranslator.tryTranslateToScalar ctx lam.Body with
        | Some boolExpr ->
            let mask =
                if isSkip then
                    ScalarExpr.Function(ScalarFunction.CumMax, [ ScalarExpr.Unary(UnaryOp.Not, boolExpr) ], LogicalTypes.Boolean)
                else
                    ScalarExpr.Function(ScalarFunction.CumMin, [ boolExpr ], LogicalTypes.Boolean)
            continueWith { state with Plan = LogicalPlan.Filter(state.Plan, mask) } rowShape tail
        | None ->
            continueWith (clientFilter rowShape lam state) rowShape tail

    /// Select: a native projection, an entity reshaping, or a client projection.
    let project (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (lam: LambdaExpression) : CompiledLogicalQuery =
        let makeCtx = StageContext.makeCtx rowShape
        match ExpressionPatterns.unwrap lam.Body with
        | :? ParameterExpression as p when p.Name = lam.Parameters.[0].Name ->
            continueWith state rowShape tail
        | _ ->
            match SubqueryDecorrelator.tryDecorrelateProjection env.ResolveSequence state.Plan lam rowShape with
            | Some (decorPlan, rewrittenLam) ->
                let p = rewrittenLam.Parameters.[0]
                let ctx = makeCtx p.Type p.Name decorPlan
                match extractProjections ctx decorPlan rewrittenLam.Body with
                | Some projs ->
                    continueWith { state with Plan = LogicalPlan.Project(decorPlan, projs) } RowShape.Empty tail
                // As for a filter: the original projection runs on the client, over the original rows.
                | None ->
                    continueWith (clientMap rowShape lam state) RowShape.Empty tail
            | None ->
                let p = lam.Parameters.[0]
                let ctx = makeCtx p.Type p.Name state.Plan
                // A projection that keeps the row's entities (a `let`, or F#'s re-wrapping of its
                // tuple rows) leaves their columns in place: it appends the computed members and
                // moves the entities to their new paths in the row.
                let transparentRowOpt =
                    match tryExtractMemberBindings lam.Body with
                    | Some bindings when EntityPacking.isTransparentRowType lam.ReturnType ->
                        let rowCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                        let named = registerLetScopes rowShape state.Plan lam
                        let sides = [ p.Name, EntityPacking.rowSideOf rowShape rowCols ]
                        let computedBindings = bindings |> List.filter (fun (_, e) -> not (EntityPacking.holdsEntities e))
                        // A computed member replacing an existing column would change the entity
                        // that column belongs to.
                        let shadowsColumn = computedBindings |> List.exists (fun (name, _) -> List.contains name rowCols)

                        if computedBindings.Length = bindings.Length || shadowsColumn then
                            None
                        else
                            match EntityPacking.tryReshapeRow named sides bindings with
                            | Some reshaped ->
                                let translated =
                                    computedBindings
                                    |> List.choose (fun (name, expr) ->
                                        ExprTranslator.tryTranslateToScalar ctx expr |> Option.map (fun s -> name, s))
                                if translated.Length = computedBindings.Length then Some(translated, reshaped) else None
                            | None -> None
                    | _ -> None

                match transparentRowOpt with
                | Some(withCols, reshaped) ->
                    let plan = if withCols.IsEmpty then state.Plan else LogicalPlan.WithColumns(state.Plan, withCols)
                    continueWith { state with Plan = plan } reshaped tail
                | None ->
                    match extractProjections ctx state.Plan lam.Body with
                    | Some projs ->
                        continueWith { state with Plan = LogicalPlan.Project(state.Plan, projs) } RowShape.Empty tail
                    | None ->
                        continueWith (clientMap rowShape lam state) RowShape.Empty tail

    /// OrderBy / ThenBy over a key selector (ignored while the rows are groups).
    let sort (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (lam: LambdaExpression) (isDesc: bool) (nullsLast: bool) : CompiledLogicalQuery =
        let makeCtx = StageContext.makeCtx rowShape
        if env.IsGrouping then
            continueWith state rowShape tail
        else
            let p = lam.Parameters.[0]
            let ctx = makeCtx p.Type p.Name state.Plan
            match ExprTranslator.tryTranslateToScalar ctx lam.Body with
            | Some s ->
                // A floating key sorts NaN as .NET does (see NaNSemantics.sortOrders).
                let newOrders =
                    NaNSemantics.sortOrders lam.Body.Type { SortOrder.Expr = s; Descending = isDesc; NullsLast = nullsLast }
                let newPlan =
                    match state.Plan with
                    | LogicalPlan.Sort(input, prevOrders) ->
                        LogicalPlan.Sort(input, prevOrders @ newOrders)
                    | _ ->
                        LogicalPlan.Sort(state.Plan, newOrders)
                continueWith { state with Plan = newPlan } rowShape tail
            | None ->
                continueWith (fallback state) rowShape tail

    /// Order() / OrderDescending(): sorts by the first column.
    let sortDefault (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (isDesc: bool) (nullsLast: bool) : CompiledLogicalQuery =
        let cols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
        let targetCol = if cols.Length > 0 then cols.[0] else ""
        let newOrder = { SortOrder.Expr = ScalarExpr.Column targetCol; Descending = isDesc; NullsLast = nullsLast }
        let newPlan =
            match state.Plan with
            | LogicalPlan.Sort(input, prevOrders) ->
                LogicalPlan.Sort(input, prevOrders @ [ newOrder ])
            | _ ->
                LogicalPlan.Sort(state.Plan, [ newOrder ])
        continueWith { state with Plan = newPlan } rowShape tail

    /// DistinctBy over plain columns.
    let distinctBy (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (lam: LambdaExpression) : CompiledLogicalQuery =
        match PipelineStageParser.extractDistinctColumns lam with
        | Some cols ->
            continueWith { state with Plan = LogicalPlan.Distinct(state.Plan, Some (Array.toList cols), DistinctKeep.First, true) } rowShape tail
        | None ->
            continueWith (fallback state) rowShape tail
