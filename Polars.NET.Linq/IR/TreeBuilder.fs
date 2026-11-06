namespace Polars.NET.Linq.IR

open System
open System.Reflection
open System.Linq
open System.Linq.Expressions
open Polars.NET.Core.Helpers
open Polars.NET.Linq
open Polars.NET.Linq.IR.TransparentScopes
open Polars.NET.Linq.IR.ProjectionExtraction
open Polars.NET.Linq.IR.GroupByPlanning
open Polars.NET.Linq.IR.GroupingSetsPlanning
open Polars.NET.Linq.IR.GroupJoinAndZipPlanning

/// Constructs pure symbolic LogicalPlan relational trees from LINQ query stages.
module internal TreeBuilder =

    /// Compiles a sequence of LinqStages into a CompiledLogicalQuery, determining pure symbolic
    /// pushdown into LogicalPlan versus client-side fallback boundaries (client predicates, projections, full fallback).
    /// The root source is registered as slot 0; every other source the pipeline reads gets the next slot.
    /// Runs in the caller's compilation scope, which supplies the backend and collects the diagnostics.
    let buildCompiledQuery
        (root: SourceBinding)
        (rootSchema: LogicalSchema)
        (isGrouping: bool)
        (stages: LinqStage list)
        : CompiledLogicalQuery =
        let backend = CompilationScope.Current.Backend
        let sources = SourceRegistry()
        let initialPlan = sources.Scan(root, rootSchema)
        let resolveSequence = SourceResolver.resolveSequencePlan backend sources

        let initState: CompiledLogicalQuery =
            {
                Plan = initialPlan
                Sources = [||]
                Residual = ClientResidual.None
                RowScopes = Map.empty
            }

        let fallback (state: CompiledLogicalQuery) : CompiledLogicalQuery =
            { state with Residual = ClientResidual.LinqToObjects }

        /// Appends a client row operation. The first one ends the native plan, whose rows are then
        /// materialized as the operation's input type, so the entities of the join scopes still in effect
        /// are packed into struct columns first.
        let appendClientOp (scopeMap: Map<string * string, string>) (op: ClientRowOp) (state: CompiledLogicalQuery) : CompiledLogicalQuery =
            match state.Residual with
            | ClientResidual.None ->
                match EntityPacking.packRow scopeMap state.Plan op.Lambda.Parameters.[0].Type with
                | RowPacking.NotNeeded -> { state with Residual = ClientResidual.RowOps [ op ] }
                | RowPacking.Packed plan -> { state with Plan = plan; Residual = ClientResidual.RowOps [ op ] }
                | RowPacking.Unresolvable -> fallback state
            | ClientResidual.RowOps ops -> { state with Residual = ClientResidual.RowOps(ops @ [ op ]) }
            | ClientResidual.LinqToObjects -> state

        let clientFilter (scopeMap: Map<string * string, string>) (lam: LambdaExpression) (state: CompiledLogicalQuery) : CompiledLogicalQuery =
            appendClientOp scopeMap (ClientRowOp.Filter(PipelineStageParser.compileClientPredicate lam, lam)) state

        let clientMap (scopeMap: Map<string * string, string>) (lam: LambdaExpression) (state: CompiledLogicalQuery) : CompiledLogicalQuery =
            appendClientOp scopeMap (ClientRowOp.Map(PipelineStageParser.compileToBoxedFunc lam, lam)) state

        let rec processStages (state: CompiledLogicalQuery) (scopeMap: Map<string * string, string>) (remainingStages: LinqStage list) : CompiledLogicalQuery =
            let makeCtx (t: Type) (pName: string) (plan: LogicalPlan) =
                let cols = (SchemaInference.inferPlanSchema plan).ColumnNames
                PipelineStageParser.createContextWithScopeAndCols scopeMap t cols pName

            match remainingStages with
            | [] ->
                match state.Residual with
                | ClientResidual.None -> { state with RowScopes = scopeMap }
                | _ -> state
            | stage :: tail ->
                let canPushdown = ClientResidualOps.isNative state.Residual

                match state.Residual with
                | ClientResidual.LinqToObjects ->
                    // The remaining stages are evaluated by LINQ-to-Objects; nothing more to record.
                    processStages state scopeMap tail
                | ClientResidual.RowOps _ ->
                    match stage with
                    | LinqStage.Filter lam -> processStages (clientFilter scopeMap lam state) scopeMap tail
                    | LinqStage.Project lam -> processStages (clientMap scopeMap lam state) scopeMap tail
                    // Order-insensitive or handled at materialization; the end-of-build check below
                    // escalates combinations the row-op executor cannot express.
                    | LinqStage.Cast _ | LinqStage.OfType _ | LinqStage.DefaultIfEmpty _ | LinqStage.Chunk _ | LinqStage.Shuffle | LinqStage.AggregateBy _ ->
                        processStages state scopeMap tail
                    | _ -> processStages (fallback state) scopeMap tail
                | ClientResidual.None ->
                    match stage with
                    | LinqStage.Filter lam ->
                        match SubqueryDecorrelator.tryDecorrelateFilter resolveSequence state.Plan lam scopeMap with
                        | Some (decorPlan, residualOpt) ->
                            match residualOpt with
                            | Some resLam ->
                                let p = resLam.Parameters.[0]
                                let ctx = makeCtx p.Type p.Name decorPlan
                                match ExprTranslator.tryTranslateToScalar ctx resLam.Body with
                                | Some pred -> processStages { state with Plan = LogicalPlan.Filter(decorPlan, pred) } scopeMap tail
                                | None ->
                                    if isUnmaterializableClientRow p.Type then
                                        processStages (fallback { state with Plan = decorPlan }) scopeMap tail
                                    else
                                        processStages (clientFilter scopeMap resLam { state with Plan = decorPlan }) scopeMap tail
                            | None ->
                                processStages { state with Plan = decorPlan } scopeMap tail
                        | None ->
                            let p = lam.Parameters.[0]
                            let ctx = makeCtx p.Type p.Name state.Plan
                            match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                            | Some pred ->
                                processStages { state with Plan = LogicalPlan.Filter(state.Plan, pred) } scopeMap tail
                            | None ->
                                if isUnmaterializableClientRow p.Type then
                                    processStages (fallback state) scopeMap tail
                                else
                                    processStages (clientFilter scopeMap lam state) scopeMap tail

                    | LinqStage.TakeWhile lam | LinqStage.SkipWhile lam as sStage ->
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
                            processStages { state with Plan = LogicalPlan.Filter(state.Plan, mask) } scopeMap tail
                        | None ->
                            processStages (clientFilter scopeMap lam state) scopeMap tail

                    | LinqStage.Project lam ->
                        match ExpressionPatterns.unwrap lam.Body with
                        | :? ParameterExpression as p when p.Name = lam.Parameters.[0].Name ->
                            processStages state scopeMap tail
                        | _ ->
                            match SubqueryDecorrelator.tryDecorrelateProjection resolveSequence state.Plan lam scopeMap with
                            | Some (decorPlan, rewrittenLam) ->
                                let p = rewrittenLam.Parameters.[0]
                                let ctx = makeCtx p.Type p.Name decorPlan
                                match extractProjections ctx decorPlan rewrittenLam.Body with
                                | Some projs ->
                                    processStages { state with Plan = LogicalPlan.Project(decorPlan, projs) } Map.empty tail
                                | None ->
                                    processStages (clientMap scopeMap rewrittenLam { state with Plan = decorPlan }) Map.empty tail
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
                                        let named = registerLetScopes scopeMap state.Plan lam
                                        let sides = [ p.Name, EntityPacking.rowSideOf scopeMap rowCols ]
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
                                    processStages { state with Plan = plan } reshaped tail
                                | None ->
                                    match extractProjections ctx state.Plan lam.Body with
                                    | Some projs ->
                                        processStages { state with Plan = LogicalPlan.Project(state.Plan, projs) } Map.empty tail
                                    | None ->
                                        processStages (clientMap scopeMap lam state) Map.empty tail

                    | LinqStage.Sort(lam, isDesc, nullsLast) ->
                        if isGrouping then
                            processStages state scopeMap tail
                        else
                            let p = lam.Parameters.[0]
                            let ctx = makeCtx p.Type p.Name state.Plan
                            match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                            | Some s ->
                                let newOrder = { SortOrder.Expr = s; Descending = isDesc; NullsLast = nullsLast }
                                let newPlan =
                                    match state.Plan with
                                    | LogicalPlan.Sort(input, prevOrders) ->
                                        LogicalPlan.Sort(input, prevOrders @ [ newOrder ])
                                    | _ ->
                                        LogicalPlan.Sort(state.Plan, [ newOrder ])
                                processStages { state with Plan = newPlan } scopeMap tail
                            | None ->
                                processStages (fallback state) scopeMap tail

                    | LinqStage.SortDefault(isDesc, nullsLast) ->
                        let cols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                        let targetCol = if cols.Length > 0 then cols.[0] else ""
                        let newOrder = { SortOrder.Expr = ScalarExpr.Column targetCol; Descending = isDesc; NullsLast = nullsLast }
                        let newPlan =
                            match state.Plan with
                            | LogicalPlan.Sort(input, prevOrders) ->
                                LogicalPlan.Sort(input, prevOrders @ [ newOrder ])
                            | _ ->
                                LogicalPlan.Sort(state.Plan, [ newOrder ])
                        processStages { state with Plan = newPlan } scopeMap tail

                    | LinqStage.Take count ->
                        processStages { state with Plan = LogicalPlan.Limit(state.Plan, 0L, count) } scopeMap tail

                    | LinqStage.Skip count ->
                        processStages { state with Plan = LogicalPlan.Limit(state.Plan, int64 count, UInt32.MaxValue) } scopeMap tail

                    | LinqStage.TakeLast count ->
                        processStages { state with Plan = LogicalPlan.Limit(state.Plan, -int64 count, count) } scopeMap tail

                    | LinqStage.SkipLast count ->
                        processStages { state with Plan = LogicalPlan.SkipLast(state.Plan, count) } scopeMap tail

                    | LinqStage.Distinct ->
                        processStages { state with Plan = LogicalPlan.Distinct(state.Plan, None, DistinctKeep.First, true) } scopeMap tail

                    | LinqStage.DistinctBy lam ->
                        match PipelineStageParser.extractDistinctColumns lam with
                        | Some cols ->
                            processStages { state with Plan = LogicalPlan.Distinct(state.Plan, Some (Array.toList cols), DistinctKeep.First, true) } scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Reverse ->
                        processStages { state with Plan = LogicalPlan.Reverse state.Plan } scopeMap tail

                    | LinqStage.Index ->
                        processStages { state with Plan = LogicalPlan.WithRowIndex(state.Plan, "index", None) } scopeMap tail

                    | LinqStage.CountBy lam ->
                        let p = lam.Parameters.[0]
                        let ctx = makeCtx p.Type p.Name state.Plan
                        match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                        | Some keyExpr ->
                            let groupPlan = LogicalPlan.GroupBy(state.Plan, [ keyExpr ], [ ("Value", ScalarExpr.Aggregation(AggKind.Len, None, false)) ], None, true)
                            let keyCol = match keyExpr with ScalarExpr.Column c -> c | _ -> "Key"
                            let projs = [ ("Key", ScalarExpr.Column keyCol); ("Value", ScalarExpr.Column "Value") ]
                            processStages { state with Plan = LogicalPlan.Project(groupPlan, projs) } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.GroupByKey keyLambda ->
                        if isGrouping then
                            processStages state scopeMap tail
                        else
                            slurpPipelineGroupBy state tail { KeyLambda = keyLambda; ElemLambdaOpt = None; HavingPreds = []; SortStages = [] } Map.empty scopeMap

                    | LinqStage.GroupByWithResult(keyLam, resLam) ->
                        if resLam.Parameters.Count < 2 then
                            if isGrouping then
                                processStages state scopeMap tail
                            else
                                slurpPipelineGroupBy state tail { KeyLambda = keyLam; ElemLambdaOpt = Some resLam; HavingPreds = []; SortStages = [] } Map.empty scopeMap
                        else
                            let keyP = keyLam.Parameters.[0]
                            let keyCtx = makeCtx keyP.Type keyP.Name state.Plan
                            let keyExprs = tryExtractKeyScalars keyCtx keyLam |> Option.defaultValue [ ScalarExpr.Column "Key" ]
                            match tryExtractGroupByResult state.Plan keyExprs resLam with
                            | Some plan -> processStages { state with Plan = plan } Map.empty tail
                            | None ->
                                let resKeyParamName = if resLam.Parameters.Count >= 1 then resLam.Parameters.[0].Name else keyP.Name
                                let isKeyArg (e: Expression) =
                                    match ExpressionPatterns.unwrap e with
                                    | :? ParameterExpression as p when p.Name = resKeyParamName || p.Name = keyP.Name -> true
                                    | MemberAccess(:? ParameterExpression as p, _) when p.Name = resKeyParamName || p.Name = keyP.Name -> true
                                    | _ -> false
                                let ctx: GroupFoldContext = { KeyLambda = keyLam; ElemLambdaOpt = None; HavingPreds = []; SortStages = [] }
                                let groupParamName = if resLam.Parameters.Count >= 2 then resLam.Parameters.[1].Name else resLam.Parameters.[0].Name
                                match buildUnifiedGroupByPlan state.Plan ctx resLam.Body groupParamName isKeyArg scopeMap with
                                | Some nextPlan ->
                                    processStages { state with Plan = nextPlan } Map.empty tail
                                | None ->
                                    processStages (fallback state) scopeMap tail

                    | LinqStage.GroupByWithElementAndResult(keyLam, elemLam, resLam) ->
                        let keyP = keyLam.Parameters.[0]
                        let keyCtx = makeCtx keyP.Type keyP.Name state.Plan
                        let keyExprs = tryExtractKeyScalars keyCtx keyLam |> Option.defaultValue [ ScalarExpr.Column "Key" ]
                        match tryExtractGroupByResult state.Plan keyExprs resLam with
                        | Some plan -> processStages { state with Plan = plan } Map.empty tail
                        | None ->
                            let resKeyParamName = if resLam.Parameters.Count >= 1 then resLam.Parameters.[0].Name else keyP.Name
                            let isKeyArg (e: Expression) =
                                match ExpressionPatterns.unwrap e with
                                | :? ParameterExpression as p when p.Name = resKeyParamName || p.Name = keyP.Name -> true
                                | MemberAccess(:? ParameterExpression as p, _) when p.Name = resKeyParamName || p.Name = keyP.Name -> true
                                | _ -> false
                            let ctx: GroupFoldContext = { KeyLambda = keyLam; ElemLambdaOpt = Some elemLam; HavingPreds = []; SortStages = [] }
                            let groupParamName = if resLam.Parameters.Count >= 2 then resLam.Parameters.[1].Name else resLam.Parameters.[0].Name
                            match buildUnifiedGroupByPlan state.Plan ctx resLam.Body groupParamName isKeyArg scopeMap with
                            | Some nextPlan ->
                                processStages { state with Plan = nextPlan } Map.empty tail
                            | None ->
                                processStages (fallback state) scopeMap tail

                    | LinqStage.GroupByRollup(keyLambda, resLambda) ->
                        match buildUnifiedGroupingSetsPlan state.Plan GroupingKind.Rollup keyLambda resLambda scopeMap with
                        | Some nextPlan ->
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.GroupByCube(keyLambda, resLambda) ->
                        match buildUnifiedGroupingSetsPlan state.Plan GroupingKind.Cube keyLambda resLambda scopeMap with
                        | Some nextPlan ->
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.GroupByGroupingSets(keyLambda, setsExpr, resLambda) ->
                        match buildUnifiedGroupingSetsPlan state.Plan (GroupingKind.GroupingSets setsExpr) keyLambda resLambda scopeMap with
                        | Some nextPlan ->
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.GroupPartitionSlice(keySelector, innerStages, resSelOpt) ->
                        match buildGroupPartitionSlicePlan state.Plan keySelector innerStages resSelOpt with
                        | Some nextPlan ->
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Join(methodInfo, innerExpr, outerKey, innerKey, resSelOpt) ->
                        match resolveSequence innerExpr with
                        | Some rightPlan ->
                            let isRightJoin = methodInfo.Name = "RightJoin"
                            let how =
                                match methodInfo.Name with
                                | "RightJoin"
                                | "LeftJoin"
                                | "GroupJoin" -> JoinKind.Left
                                | "FullJoin" -> JoinKind.Full
                                | "SemiJoin" -> JoinKind.Semi
                                | "AntiJoin" -> JoinKind.Anti
                                | _ -> JoinKind.Inner

                            let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                            let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
                            let leftColSet = Set.ofList leftCols

                            let baseSuffix = if isRightJoin then LinqConstants.DefaultLeftSuffix else LinqConstants.DefaultRightSuffix
                            let rec findUniqueSuffix (cnt: int) =
                                let candidate = if cnt = 1 then baseSuffix else sprintf "%s%d" baseSuffix cnt
                                let hasConflict = rightCols |> List.exists (fun rc -> leftColSet.Contains (rc + candidate))
                                if not hasConflict then candidate
                                else findUniqueSuffix (cnt + 1)
                            let uniqueSuffix = findUniqueSuffix 1

                            let outerParam = outerKey.Parameters.[0]
                            let innerParam = innerKey.Parameters.[0]
                            let leftCtx = PipelineStageParser.createContextWithScopeAndCols scopeMap outerParam.Type leftCols outerParam.Name
                            let rightCtx = PipelineStageParser.createContextWithScopeAndCols Map.empty innerParam.Type rightCols innerParam.Name

                            match tryExtractKeyScalars leftCtx outerKey, tryExtractKeyScalars rightCtx innerKey with
                            | Some leftKeys, Some rightKeys when leftKeys.Length = rightKeys.Length ->
                                let joinPlan =
                                    if isRightJoin then
                                        LogicalPlan.Join(rightPlan, state.Plan, rightKeys, leftKeys, how, Some uniqueSuffix, JoinKeyCoalescing.KeepBoth, false)
                                    else
                                        LogicalPlan.Join(state.Plan, rightPlan, leftKeys, rightKeys, how, Some uniqueSuffix, JoinKeyCoalescing.KeepBoth, false)

                                let leftScope = outerKey.Parameters.[0].Name
                                let rightScope =
                                    match resSelOpt with
                                    | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                                    | _ -> innerKey.Parameters.[0].Name

                                let updatedScopeMap =
                                    PipelineStageParser.updateJoinScopeMap scopeMap leftScope (List.toArray leftCols) rightScope (List.toArray rightCols) uniqueSuffix isRightJoin
                                    |> aliasResultSelectorScopes (if scopeMap.IsEmpty then Some leftScope else None) rightScope resSelOpt
                                // Downstream stages see the result row; the concrete selector itself reads the joined sides.
                                let rowScopeMap = joinResultScopes scopeMap updatedScopeMap leftScope rightScope resSelOpt

                                match resSelOpt with
                                | TransparentOrNoResSelector ->
                                    processStages { state with Plan = joinPlan } rowScopeMap tail
                                | ConcreteResSelector resSel ->
                                    let postJoinCols = (SchemaInference.inferPlanSchema joinPlan).ColumnNames
                                    let pLeft = resSel.Parameters.[0]
                                    let pRight = resSel.Parameters.[1]
                                    let resCtx =
                                        if not updatedScopeMap.IsEmpty then
                                            TranslationContext.ScopedEntity([ pLeft.Name; pRight.Name ], updatedScopeMap, postJoinCols)
                                        else
                                            TranslationContext.BinaryJoin(pLeft.Name, pRight.Name, leftColSet, uniqueSuffix)

                                    match SubqueryDecorrelator.tryDecorrelateProjection resolveSequence joinPlan resSel updatedScopeMap with
                                    | Some (decorPlan, rewrittenResSel) ->
                                        let postDecorCols = (SchemaInference.inferPlanSchema decorPlan).ColumnNames
                                        let pLeft2 = rewrittenResSel.Parameters.[0]
                                        let pRight2 = rewrittenResSel.Parameters.[1]
                                        let resCtx2 =
                                            if not updatedScopeMap.IsEmpty then
                                                TranslationContext.ScopedEntity([ pLeft2.Name; pRight2.Name ], updatedScopeMap, postDecorCols)
                                            else
                                                TranslationContext.BinaryJoin(pLeft2.Name, pRight2.Name, leftColSet, uniqueSuffix)

                                        match tryExtractJoinProjections resCtx2 decorPlan pLeft2 pRight2 leftCols rightCols uniqueSuffix isRightJoin rewrittenResSel.Body with
                                        | Some projs -> processStages { state with Plan = LogicalPlan.Project(decorPlan, projs) } Map.empty tail
                                        | None -> processStages { state with Plan = decorPlan } rowScopeMap tail
                                    | None ->
                                        match tryExtractJoinProjections resCtx joinPlan pLeft pRight leftCols rightCols uniqueSuffix isRightJoin resSel.Body with
                                        | Some projs -> processStages { state with Plan = LogicalPlan.Project(joinPlan, projs) } Map.empty tail
                                        | None -> processStages { state with Plan = joinPlan } rowScopeMap tail
                            | _ ->
                                processStages (fallback state) scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, resSelOpt) ->
                        match tail with
                        | LinqStage.Explode(collSelector, explodeResSelOpt) :: nextTail when (tryExtractDefaultIfEmpty collSelector.Body).IsSome ->
                            let effResSelOpt = explodeResSelOpt |> Option.orElse resSelOpt
                            // The flattening selector reads the group join's row (outer, group), not the outer
                            // entity: that row's members holding the outer side become its path scopes.
                            let groupRowScopeMap =
                                match explodeResSelOpt, resSelOpt with
                                | Some _, Some groupSel when groupSel.Parameters.Count >= 2 ->
                                    let cols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                                    let sides = [ groupSel.Parameters.[0].Name, EntityPacking.rowSideOf scopeMap cols ]
                                    tryExtractMemberBindings groupSel.Body
                                    |> Option.bind (EntityPacking.tryReshapeRow scopeMap sides)
                                    |> Option.defaultValue scopeMap
                                | _ -> scopeMap
                            processStages state groupRowScopeMap (LinqStage.Join(m, innerExpr, outerKey, innerKey, effResSelOpt) :: nextTail)
                        | LinqStage.Project projLambda :: nextTail when resSelOpt.IsNone ->
                            let outerType, innerType = outerKey.Parameters.[0].Type, innerKey.Parameters.[0].Type
                            let desugaredResSel = PipelineStageParser.desugarGroupJoinProject projLambda outerType innerType
                            processStages state scopeMap (LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, Some desugaredResSel) :: nextTail)
                        | _ ->
                            match resSelOpt with
                            | Some resSel ->
                                match buildGroupJoinWithResultPlan resolveSequence state.Plan innerExpr outerKey innerKey resSel with
                                | Some nextPlan ->
                                    processStages { state with Plan = nextPlan } Map.empty tail
                                | None ->
                                    processStages (fallback state) scopeMap tail
                            | None ->
                                processStages state scopeMap (LinqStage.Join(m, innerExpr, outerKey, innerKey, None) :: tail)

                    | LinqStage.CrossJoin(innerExpr, resSelOpt) ->
                        match resolveSequence innerExpr with
                        | Some rightPlan ->
                            let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                            let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
                            let leftColSet = Set.ofList leftCols
                            let uniqueSuffix = LinqConstants.DefaultRightSuffix
                            let joinPlan = LogicalPlan.Join(state.Plan, rightPlan, [], [], JoinKind.Cross, Some uniqueSuffix, JoinKeyCoalescing.Default, false)

                            let leftScope = "outer"
                            let rightScope = "inner"
                            let updatedScopeMap =
                                PipelineStageParser.updateJoinScopeMap scopeMap leftScope (List.toArray leftCols) rightScope (List.toArray rightCols) uniqueSuffix false
                                |> aliasResultSelectorScopes (if scopeMap.IsEmpty then Some leftScope else None) rightScope resSelOpt
                            // Downstream stages see the result row; the concrete selector itself reads the joined sides.
                            let rowScopeMap = joinResultScopes scopeMap updatedScopeMap leftScope rightScope resSelOpt

                            match resSelOpt with
                            | TransparentOrNoResSelector ->
                                processStages { state with Plan = joinPlan } rowScopeMap tail
                            | ConcreteResSelector resSel ->
                                let postJoinCols = (SchemaInference.inferPlanSchema joinPlan).ColumnNames
                                let pLeft = resSel.Parameters.[0]
                                let pRight = resSel.Parameters.[1]
                                let resCtx =
                                    if not updatedScopeMap.IsEmpty then
                                        TranslationContext.ScopedEntity([ pLeft.Name; pRight.Name ], updatedScopeMap, postJoinCols)
                                    else
                                        TranslationContext.BinaryJoin(pLeft.Name, pRight.Name, leftColSet, uniqueSuffix)
                                match tryExtractJoinProjections resCtx joinPlan pLeft pRight leftCols rightCols uniqueSuffix false resSel.Body with
                                | Some projs -> processStages { state with Plan = LogicalPlan.Project(joinPlan, projs) } Map.empty tail
                                | None -> processStages { state with Plan = joinPlan } rowScopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.JoinAsOf(_, inner, outerKey, innerKey, outerByOpt, innerByOpt, resSelOpt, toleranceStr, toleranceInt, toleranceFloat, strategy) ->
                        match resolveSequence inner with
                        | Some rightPlan ->
                            let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                            let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
                            let leftColSet = Set.ofList leftCols
                            let leftCtx = PipelineStageParser.createContextWithScopeAndCols scopeMap outerKey.Parameters.[0].Type leftCols outerKey.Parameters.[0].Name
                            let rightCtx = PipelineStageParser.createContextWithScopeAndCols Map.empty innerKey.Parameters.[0].Type rightCols innerKey.Parameters.[0].Name
                            match ExprTranslator.tryTranslateToScalar leftCtx outerKey.Body, ExprTranslator.tryTranslateToScalar rightCtx innerKey.Body with
                            | Some leftOn, Some rightOn ->
                                let leftBy =
                                    match outerByOpt with
                                    | Some ob -> tryExtractKeyScalars leftCtx ob |> Option.defaultValue []
                                    | None -> []
                                let rightBy =
                                    match innerByOpt with
                                    | Some ib -> tryExtractKeyScalars rightCtx ib |> Option.defaultValue []
                                    | None -> []
                                let uniqueSuffix = LinqConstants.DefaultRightSuffix
                                let spec: LogicalAsOfJoinSpec =
                                    {
                                        LeftOn = leftOn
                                        RightOn = rightOn
                                        LeftBy = leftBy
                                        RightBy = rightBy
                                        Strategy = strategy
                                        ToleranceStr = toleranceStr
                                        ToleranceInt = toleranceInt
                                        ToleranceFloat = toleranceFloat
                                        AllowEq = true
                                        CheckSorted = true
                                        Suffix = Some uniqueSuffix
                                        Coalesce = JoinKeyCoalescing.KeepBoth
                                        NullsEqual = false
                                    }
                                let asOfPlan = LogicalPlan.JoinAsOf(state.Plan, rightPlan, spec)
                                let leftScope = outerKey.Parameters.[0].Name
                                let rightScope =
                                    match resSelOpt with
                                    | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                                    | _ -> innerKey.Parameters.[0].Name

                                let updatedScopeMap =
                                    PipelineStageParser.updateJoinScopeMap scopeMap leftScope (List.toArray leftCols) rightScope (List.toArray rightCols) uniqueSuffix false
                                    |> aliasResultSelectorScopes (if scopeMap.IsEmpty then Some leftScope else None) rightScope resSelOpt
                                // Downstream stages see the result row; the concrete selector itself reads the joined sides.
                                let rowScopeMap = joinResultScopes scopeMap updatedScopeMap leftScope rightScope resSelOpt

                                match resSelOpt with
                                | TransparentOrNoResSelector ->
                                    processStages { state with Plan = asOfPlan } rowScopeMap tail
                                | ConcreteResSelector resSel ->
                                    let postJoinCols = (SchemaInference.inferPlanSchema asOfPlan).ColumnNames
                                    let pLeft = resSel.Parameters.[0]
                                    let pRight = resSel.Parameters.[1]
                                    let resCtx =
                                        if not updatedScopeMap.IsEmpty then
                                            TranslationContext.ScopedEntity([ pLeft.Name; pRight.Name ], updatedScopeMap, postJoinCols)
                                        else
                                            TranslationContext.BinaryJoin(pLeft.Name, pRight.Name, leftColSet, uniqueSuffix)
                                    match tryExtractJoinProjections resCtx asOfPlan pLeft pRight leftCols rightCols uniqueSuffix false resSel.Body with
                                    | Some projs -> processStages { state with Plan = LogicalPlan.Project(asOfPlan, projs) } Map.empty tail
                                    | None -> processStages { state with Plan = asOfPlan } rowScopeMap tail
                            | _ ->
                                processStages (fallback state) scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.SetOp(methodInfo, secondExpr, key1Opt, key2Opt) ->
                        match resolveSequence secondExpr with
                        | Some rightPlan ->
                            let how =
                                if methodInfo.Name.StartsWith "Intersect" then
                                    JoinKind.Semi
                                else
                                    JoinKind.Anti

                            let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                            let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames

                            let keysOpt =
                                match key1Opt, key2Opt with
                                | Some k1, Some k2 ->
                                    let p1 = k1.Parameters.[0]
                                    let p2 = k2.Parameters.[0]
                                    let ctx1 = PipelineStageParser.createContextWithScopeAndCols scopeMap p1.Type leftCols p1.Name
                                    let ctx2 = PipelineStageParser.createContextWithScopeAndCols Map.empty p2.Type rightCols p2.Name
                                    match ExprTranslator.tryTranslateToScalar ctx1 k1.Body, ExprTranslator.tryTranslateToScalar ctx2 k2.Body with
                                    | Some lk, Some rk -> Some([ lk ], [ rk ])
                                    | _ -> None
                                | Some k1, None ->
                                    let p1 = k1.Parameters.[0]
                                    let ctx1 = PipelineStageParser.createContextWithScopeAndCols scopeMap p1.Type leftCols p1.Name
                                    match ExprTranslator.tryTranslateToScalar ctx1 k1.Body with
                                    | Some lk when rightCols.Length > 0 ->
                                        Some([ lk ], [ ScalarExpr.Column rightCols.[0] ])
                                    | _ -> None
                                | None, None ->
                                    if leftCols.Length = rightCols.Length && leftCols.Length > 0 then
                                        Some(leftCols |> List.map ScalarExpr.Column, rightCols |> List.map ScalarExpr.Column)
                                    else
                                        Some(leftCols |> List.map ScalarExpr.Column, leftCols |> List.map ScalarExpr.Column)
                                | _ -> None

                            match keysOpt with
                            | Some(leftOn, rightOn) ->
                                processStages { state with Plan = LogicalPlan.Join(state.Plan, rightPlan, leftOn, rightOn, how, None, JoinKeyCoalescing.Default, true) } scopeMap tail
                            | None ->
                                processStages (fallback state) scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Union(secondExpr, keyLambdaOpt) ->
                        match resolveSequence secondExpr with
                        | Some otherPlan ->
                            let subsetOpt = keyLambdaOpt |> Option.bind PipelineStageParser.extractDistinctColumns |> Option.map Array.toList
                            let unionPlan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true)
                            processStages { state with Plan = LogicalPlan.Distinct(unionPlan, subsetOpt, DistinctKeep.First, true) } scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Concat secondExpr ->
                        match resolveSequence secondExpr with
                        | Some otherPlan ->
                            processStages { state with Plan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true) } scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Append elemExpr ->
                        match PartialEvaluator.tryEvaluate elemExpr with
                        | Some elem when not (isNull elem) ->
                            match SourceResolver.singleRowPlan sources elem (elem.GetType()) with
                            | Some otherPlan ->
                                processStages { state with Plan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true) } scopeMap tail
                            | None ->
                                processStages (fallback state) scopeMap tail
                        | _ ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Prepend elemExpr ->
                        match PartialEvaluator.tryEvaluate elemExpr with
                        | Some elem when not (isNull elem) ->
                            match SourceResolver.singleRowPlan sources elem (elem.GetType()) with
                            | Some otherPlan ->
                                processStages { state with Plan = LogicalPlan.Union(otherPlan, state.Plan, isAll = true) } scopeMap tail
                            | None ->
                                processStages (fallback state) scopeMap tail
                        | _ ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Zip(secondExpr, resSelOpt) ->
                        match buildZipPlan resolveSequence state.Plan secondExpr resSelOpt with
                        | Some nextPlan ->
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Zip3(secondExpr, thirdExpr) ->
                        match buildZip3Plan resolveSequence state.Plan secondExpr thirdExpr with
                        | Some nextPlan ->
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Unpivot(colLam, resLambdaOpt) ->
                        let availableCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                        match tryExtractUnpivotPattern colLam resLambdaOpt with
                        | Some pattern ->
                            let explicitIndexCols = pattern.ExplicitIndexCols
                            let onCols = pattern.OnCols
                            let indexCols =
                                if not explicitIndexCols.IsEmpty then explicitIndexCols
                                else availableCols |> List.filter (fun c -> not (Array.contains c onCols))

                            if indexCols.IsEmpty then
                                processStages (fallback state) scopeMap tail
                            else
                                let unpivotPlan = LogicalPlan.Unpivot(state.Plan, indexCols, Some (Array.toList onCols), pattern.VariableName, pattern.ValueName)
                                let postUnpivotCols = indexCols @ [ pattern.VariableName; pattern.ValueName ]
                                let leftScope = colLam.Parameters.[0].Name
                                let innerScope =
                                    match resLambdaOpt with
                                    | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                                    | _ -> "q"
                                let updatedScopeMap =
                                    let outerMappings = indexCols |> List.map (fun c -> (leftScope, c), c)
                                    let innerMappings =
                                        [
                                            (innerScope, pattern.VariableName), pattern.VariableName
                                            (innerScope, pattern.ValueName), pattern.ValueName
                                            (innerScope, pattern.VarPropName), pattern.VariableName
                                            (innerScope, pattern.ValPropName), pattern.ValueName
                                            (innerScope, "Item1"), pattern.VariableName
                                            (innerScope, "Item2"), pattern.ValueName
                                        ]
                                    scopeMap
                                    |> Map.fold (fun m k v -> Map.add k v m) (Map.ofList (outerMappings @ innerMappings))

                                match resLambdaOpt with
                                | TransparentOrNoResSelector ->
                                    processStages { state with Plan = unpivotPlan } updatedScopeMap tail
                                | ConcreteResSelector resSel ->
                                    let resCtx = TranslationContext.ScopedEntity([ leftScope; innerScope ], updatedScopeMap, postUnpivotCols)
                                    match extractProjections resCtx unpivotPlan resSel.Body with
                                    | Some projs ->
                                        processStages { state with Plan = LogicalPlan.Project(unpivotPlan, projs) } Map.empty tail
                                    | None ->
                                        processStages (clientMap Map.empty resSel { state with Plan = unpivotPlan }) Map.empty tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.Explode(colLam, resSelOpt) ->
                        let p = colLam.Parameters.[0]
                        let ctx = makeCtx p.Type p.Name state.Plan
                        match ColumnResolver.tryResolveColumnNameWithContext ctx colLam.Body with
                        | Some col ->
                            let explodePlan = LogicalPlan.Explode(state.Plan, [ col ], true, true)
                            let finalPlan =
                                match resSelOpt with
                                | Some resSel when resSel.Parameters.Count >= 2 ->
                                    let itemParam = resSel.Parameters.[1]
                                    let renames = extractBinaryResultRenames resSel (Some(itemParam.Name, col))
                                    if not renames.IsEmpty then
                                        let oldCols = renames |> List.map fst
                                        let newCols = renames |> List.map snd
                                        LogicalPlan.Rename(explodePlan, oldCols, newCols)
                                    else
                                        explodePlan
                                | _ -> explodePlan
                            processStages { state with Plan = finalPlan } scopeMap tail
                        | None ->
                            processStages (fallback state) scopeMap tail

                    | LinqStage.OfType(sourceType, targetType) ->
                        if not (targetType.IsAssignableFrom sourceType || sourceType.IsAssignableFrom targetType) then
                            processStages { state with Plan = LogicalPlan.Filter(state.Plan, ScalarExpr.Literal(LiteralValue.Bool false)) } scopeMap tail
                        else
                            processStages state scopeMap tail

                    | LinqStage.Cast _
                    | LinqStage.DefaultIfEmpty _
                    | LinqStage.Chunk _
                    | LinqStage.Shuffle
                    | LinqStage.AggregateBy _ ->
                        processStages state scopeMap tail

        and slurpPipelineGroupBy
            (state: CompiledLogicalQuery)
            (s: LinqStage list)
            (ctx: GroupFoldContext)
            (letBindings: Map<string, Expression>)
            (scopeMap: Map<string * string, string>)
            =
            let substituteLet (expr: Expression) =
                if letBindings.IsEmpty then expr
                else
                    { new ExpressionVisitor() with
                        override self.VisitMember(node: MemberExpression) =
                            let visitedInner = self.Visit(node.Expression)
                            let rec isTransparentTarget (e: Expression) =
                                match ExpressionPatterns.unwrap e with
                                | :? ParameterExpression as p ->
                                    isTupleOrAnonType p.Type || (not (isNull p.Name) && p.Name.Contains "TransparentIdentifier")
                                | MemberAccess(_, m) when isNavigationMember m || isTupleOrAnonType m.DeclaringType ->
                                    true
                                | MemberAccess(target, _) ->
                                    isTransparentTarget target
                                | _ -> false
                            if isTransparentTarget visitedInner then
                                match letBindings.TryFind node.Member.Name with
                                | Some boundExpr -> boundExpr
                                | None -> node.Update(visitedInner) :> Expression
                            else
                                node.Update(visitedInner) :> Expression
                    }.Visit(expr)

            match s with
            | LinqStage.Filter pred :: next ->
                let newBody = substituteLet pred.Body
                let newPred = Expression.Lambda(newBody, pred.Parameters)
                slurpPipelineGroupBy state next { ctx with HavingPreds = ctx.HavingPreds @ [ newPred ] } letBindings scopeMap
            | LinqStage.Sort(sortLambda, isDesc, nullsLast) :: next ->
                let newBody = substituteLet sortLambda.Body
                let newSort = Expression.Lambda(newBody, sortLambda.Parameters)
                slurpPipelineGroupBy state next { ctx with SortStages = ctx.SortStages @ [ (newSort, isDesc, nullsLast) ] } letBindings scopeMap
            | LinqStage.Project projLambda :: next ->
                let param = projLambda.Parameters.[0]
                let isTransparentGroupLet =
                    if not (isNull projLambda.Body.Type) && not (isNull projLambda.Body.Type.FullName) && projLambda.Body.Type.FullName.StartsWith "Microsoft.FSharp." then
                        None
                    else
                        match tryExtractMemberBindings projLambda.Body with
                        | Some bindings when not next.IsEmpty ->
                            let hasGroupingNav =
                                bindings
                                |> List.exists (fun (propName, expr) ->
                                    if propName.Contains "TransparentIdentifier" then
                                        true
                                    else
                                        match ExpressionPatterns.unwrap expr with
                                        | :? ParameterExpression as p when
                                            (isGroupingType p.Type)
                                            || isTupleOrAnonType p.Type
                                            || (not (isNull p.Name) && p.Name.Contains "TransparentIdentifier") ->
                                            true
                                        | MemberAccess(_, m) when isNavigationMember m ->
                                            true
                                        | _ -> false)
                            if hasGroupingNav then
                                Some bindings
                            else
                                None
                        | _ -> None

                match isTransparentGroupLet with
                | Some bindings ->
                    let gParamOpt =
                        bindings
                        |> List.tryPick (fun (name, expr) ->
                            match ExpressionPatterns.unwrap expr with
                            | :? ParameterExpression as p when p.Name = param.Name && isGroupingType p.Type ->
                                Some(name, p :> Expression)
                            | _ -> None)
                    let updatedBindings =
                        bindings
                        |> List.fold (fun m (name, expr) ->
                            let subExpr = substituteLet expr
                            let normalizedExpr =
                                match gParamOpt with
                                | Some(_, gParamExpr) ->
                                    { new ExpressionVisitor() with
                                        override _.VisitParameter(p: ParameterExpression) =
                                            if p.Name = param.Name then gParamExpr
                                            else base.VisitParameter p
                                    }.Visit(subExpr)
                                | None -> subExpr
                            match ExpressionPatterns.unwrap normalizedExpr with
                            | :? ParameterExpression as p when p.Name = param.Name && isTupleOrAnonType p.Type ->
                                m
                            | _ ->
                                Map.add name normalizedExpr m) letBindings
                    slurpPipelineGroupBy state next ctx updatedBindings scopeMap
                | None ->
                    let finalProjBody = substituteLet projLambda.Body
                    let isKeyArg (e: Expression) =
                        match ExpressionPatterns.unwrap e with
                        | MemberAccess(p, m) when
                            not (isNull p) && (p.NodeType = ExpressionType.Parameter || isNavigationMember (match p with :? MemberExpression as me -> me.Member | _ -> null)) && m.Name = "Key"
                            ->
                            true
                        // A bare projection parameter bound as a member value stands for the key: F# `let` after
                        // groupBy carries the grouping as Item1 and only reads Item1.Key afterwards.
                        | :? ParameterExpression as p when p.Name = projLambda.Parameters.[0].Name ->
                            true
                        | _ -> false

                    let gParamName =
                        match letBindings |> Map.tryPick (fun _ v -> match ExpressionPatterns.unwrap v with :? ParameterExpression as p when isGroupingType p.Type -> Some p.Name | _ -> None) with
                        | Some name -> name
                        | None -> projLambda.Parameters.[0].Name

                    match buildUnifiedGroupByPlan state.Plan ctx finalProjBody gParamName isKeyArg scopeMap with
                    | Some nextPlan ->
                        processStages { state with Plan = nextPlan } Map.empty next
                    | None ->
                        processStages (fallback state) scopeMap next
            | [] ->
                if isGrouping then
                    // The rows are the grouped elements, materialized as the key selector's input type; the
                    // residual having predicates run over the groups.
                    let grouped = { state with RowScopes = scopeMap }
                    ctx.HavingPreds |> List.fold (fun acc havingPred -> clientFilter Map.empty havingPred acc) grouped
                else
                    (fallback state)
            | otherHead :: otherTail ->
                processStages (fallback state) scopeMap (otherHead :: otherTail)

        let finalState = processStages initState Map.empty stages

        let needsLinqToObjects = stages |> List.exists LinqStageTraits.breaksRowStream

        let completed =
            match finalState.Residual with
            | ClientResidual.RowOps _ when needsLinqToObjects -> fallback finalState
            | _ -> finalState

        { completed with Sources = sources.ToArray() }
