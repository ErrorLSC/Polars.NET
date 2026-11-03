namespace Polars.NET.Linq.IR

open System
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Linq
open Polars.NET.Linq.Backend.Polars

/// Constructs LogicalPlan relational trees from LINQ query stages or QueryOp streams.
module internal TreeBuilder =

    /// Extracts a sequence of key ScalarExprs from a key selector lambda (supporting scalar columns or anonymous key objects).
    let private tryExtractKeyScalars (ctx: TranslationContext) (keyLambda: LambdaExpression) : ScalarExpr list option =
        match tryExtractMemberBindings keyLambda.Body with
        | Some bindings ->
            let exprs =
                bindings
                |> List.choose (fun (_, e) -> ExprTranslator.tryTranslateToScalar ctx e)
            if exprs.Length = bindings.Length then
                Some exprs
            else
                None
        | None ->
            ExprTranslator.tryTranslateToScalar ctx keyLambda.Body
            |> Option.map (fun s -> [ s ])

    /// Extracts projection bindings (name * ScalarExpr) from a projection LambdaExpression.
    let private extractProjections (ctx: TranslationContext) (body: Expression) : (string * ScalarExpr) list option =
        match ExpressionPatterns.unwrap body with
        | :? MemberInitExpression as mie ->
            let bindings =
                mie.Bindings
                |> Seq.choose (function
                    | :? MemberAssignment as ma ->
                        ExprTranslator.tryTranslateToScalar ctx ma.Expression
                        |> Option.map (fun s -> ma.Member.Name, s)
                    | _ -> None)
                |> Seq.toList
            if bindings.Length = mie.Bindings.Count then Some bindings else None

        | :? NewExpression as ne ->
            let names =
                if not (isNull ne.Members) && ne.Members.Count = ne.Arguments.Count then
                    ne.Members |> Seq.map (fun m -> m.Name) |> Seq.toList
                elif not (isNull ne.Constructor) then
                    ne.Constructor.GetParameters() |> Seq.map (fun p -> p.Name) |> Seq.toList
                else
                    []

            let bindings =
                ne.Arguments
                |> Seq.mapi (fun i arg ->
                    let name = if i < names.Length then names.[i] else sprintf "col_%d" i
                    ExprTranslator.tryTranslateToScalar ctx arg |> Option.map (fun s -> name, s))
                |> Seq.toList

            if bindings |> List.forall Option.isSome then
                Some(bindings |> List.map Option.get)
            else
                None

        | other ->
            ExprTranslator.tryTranslateToScalar ctx other
            |> Option.map (fun s -> [ ("value", s) ])

    /// Extracts group aggregations and projections from a GroupBy result selector LambdaExpression.
    let private tryExtractGroupByResult
        (currentPlan: LogicalPlan)
        (keyExprs: ScalarExpr list)
        (resLam: LambdaExpression)
        : LogicalPlan option =

        let keyParam = resLam.Parameters.[0]
        let groupParamOpt = if resLam.Parameters.Count >= 2 then Some resLam.Parameters.[1] else None

        let isKeyArg (e: Expression) =
            match ExpressionPatterns.unwrap e with
            | :? ParameterExpression as p when p.Name = keyParam.Name -> true
            | MemberAccess(target, _) when
                (match ExpressionPatterns.unwrap target with :? ParameterExpression as p -> p.Name = keyParam.Name | _ -> false) -> true
            | _ -> false

        let tryTranslateGroupExpr (expr: Expression) : (bool * ScalarExpr) option =
            let clean = ExpressionPatterns.unwrap expr
            if isKeyArg clean then
                match clean with
                | MemberAccess(_, m) -> Some(true, ScalarExpr.Column m.Name)
                | _ ->
                    if keyExprs.Length = 1 then Some(true, keyExprs.[0])
                    else None
            else
                match groupParamOpt with
                | Some gp ->
                    let isGroupTarget (e: Expression) =
                        match ExpressionPatterns.unwrap e with
                        | :? ParameterExpression as p -> p.Name = gp.Name
                        | MethodCall(innerM, _, innerArgs) when innerM.Name = "Select" && innerArgs.Length >= 1 ->
                            match ExpressionPatterns.unwrap innerArgs.[0] with
                            | :? ParameterExpression as p -> p.Name = gp.Name
                            | _ -> false
                        | _ -> false

                    match clean with
                    | MethodCall(m, _, args) when args.Length >= 1 && isGroupTarget args.[0] ->
                        match m.Name with
                        | "Count" | "LongCount" when args.Length = 1 ->
                            Some(false, ScalarExpr.Aggregation(AggKind.Len, None, false))
                        | "Count" | "LongCount" when args.Length = 2 ->
                            match args.[1] with
                            | CleanLambda lam ->
                                let ctx = TranslationContext.Entity lam.Parameters.[0].Name
                                ExprTranslator.tryTranslateToScalar ctx lam.Body
                                |> Option.map (fun s -> false, ScalarExpr.Aggregation(AggKind.Count, Some s, false))
                            | _ -> None
                        | "Sum" | "Average" | "Mean" | "Min" | "Max" | "Median"
                        | "First" | "FirstOrDefault" | "Last" | "LastOrDefault"
                        | "Std" | "StdDev" | "Var" | "Variance" ->
                            if args.Length = 2 then
                                match args.[1] with
                                | CleanLambda lam ->
                                    let ctx = TranslationContext.Entity lam.Parameters.[0].Name
                                    ExprTranslator.tryTranslateToScalar ctx lam.Body
                                    |> Option.bind (fun s -> ScalarExprOps.translateReductionOp m.Name s)
                                    |> Option.map (fun s -> false, s)
                                | _ -> None
                            elif args.Length = 1 then
                                match args.[0] with
                                | MethodCall(sm, _, [ _; CleanLambda lam ]) when sm.Name = "Select" ->
                                    let ctx = TranslationContext.Entity lam.Parameters.[0].Name
                                    ExprTranslator.tryTranslateToScalar ctx lam.Body
                                    |> Option.bind (fun s -> ScalarExprOps.translateReductionOp m.Name s)
                                    |> Option.map (fun s -> false, s)
                                | _ -> None
                            else
                                None
                        | "ToList" | "ToArray" ->
                            Some(false, ScalarExpr.Aggregation(AggKind.List, None, false))
                        | _ -> None
                    | _ ->
                        let ctx = TranslationContext.Entity keyParam.Name
                        ExprTranslator.tryTranslateToScalar ctx clean |> Option.map (fun s -> false, s)
                | None ->
                    let ctx = TranslationContext.Entity keyParam.Name
                    ExprTranslator.tryTranslateToScalar ctx clean |> Option.map (fun s -> false, s)

        let bindingsOpt =
            match ExpressionPatterns.unwrap resLam.Body with
            | :? MemberInitExpression as mie ->
                let b =
                    mie.Bindings
                    |> Seq.choose (function
                        | :? MemberAssignment as ma ->
                            tryTranslateGroupExpr ma.Expression
                            |> Option.map (fun (isK, s) -> ma.Member.Name, isK, s)
                        | _ -> None)
                    |> Seq.toList
                if b.Length = mie.Bindings.Count then Some b else None

            | :? NewExpression as ne ->
                let names =
                    if not (isNull ne.Members) && ne.Members.Count = ne.Arguments.Count then
                        ne.Members |> Seq.map (fun m -> m.Name) |> Seq.toList
                    elif not (isNull ne.Constructor) then
                        ne.Constructor.GetParameters() |> Seq.map (fun p -> p.Name) |> Seq.toList
                    else
                        []

                let b =
                    ne.Arguments
                    |> Seq.mapi (fun i arg ->
                        let name = if i < names.Length then names.[i] else sprintf "col_%d" i
                        tryTranslateGroupExpr arg |> Option.map (fun (isK, s) -> name, isK, s))
                    |> Seq.toList

                if b |> List.forall Option.isSome then
                    Some(b |> List.map Option.get)
                else
                    None

            | other ->
                tryTranslateGroupExpr other
                |> Option.map (fun (isK, s) -> [ ("value", isK, s) ])

        match bindingsOpt with
        | Some bindings ->
            let aggs =
                bindings
                |> List.choose (fun (name, isKey, s) ->
                    if isKey then None
                    else Some(name, s))

            let groupPlan = LogicalPlan.GroupBy(currentPlan, keyExprs, aggs, None, false)

            let finalProjs =
                bindings
                |> List.map (fun (name, isKey, s) ->
                    if isKey then
                        match s with
                        | ScalarExpr.Column cName -> name, ScalarExpr.Column cName
                        | _ -> name, s
                    else
                        name, ScalarExpr.Column name)

            Some (LogicalPlan.Project(groupPlan, finalProjs))
        | None ->
            None

    /// Compiles a sequence of LinqStages into a CompiledLogicalQuery, determining pure symbolic
    /// pushdown into LogicalPlan versus client-side fallback boundaries (client predicates, projections, full fallback).
    let buildCompiledQuery (initialPlan: LogicalPlan) (stages: LinqStage list) : CompiledLogicalQuery =
        let initState: CompiledLogicalQuery =
            {
                Plan = initialPlan
                ClientPredicates = []
                ClientProjection = None
                RequiresClientFallback = false
            }

        let foldStage (state: CompiledLogicalQuery) (stage: LinqStage) : CompiledLogicalQuery =
            let canPushdown = state.ClientPredicates.IsEmpty && state.ClientProjection.IsNone && not state.RequiresClientFallback

            if state.RequiresClientFallback then
                match stage with
                | LinqStage.Filter lam ->
                    { state with ClientPredicates = (PipelineStageParser.compileClientPredicate lam, lam) :: state.ClientPredicates }
                | LinqStage.Project lam ->
                    let currentFunc = PipelineStageParser.compileToBoxedFunc lam
                    let nextProj =
                        match state.ClientProjection with
                        | None -> Some(lam.Parameters.[0].Type, currentFunc)
                        | Some(prevSrcType, prevFunc) -> Some(prevSrcType, Func<obj, obj>(fun rawItem -> currentFunc.Invoke(prevFunc.Invoke rawItem)))
                    { state with ClientProjection = nextProj }
                | _ ->
                    state
            elif not canPushdown then
                match stage with
                | LinqStage.Filter lam ->
                    { state with ClientPredicates = (PipelineStageParser.compileClientPredicate lam, lam) :: state.ClientPredicates }
                | LinqStage.Project lam ->
                    let currentFunc = PipelineStageParser.compileToBoxedFunc lam
                    let nextProj =
                        match state.ClientProjection with
                        | None -> Some(lam.Parameters.[0].Type, currentFunc)
                        | Some(prevSrcType, prevFunc) -> Some(prevSrcType, Func<obj, obj>(fun rawItem -> currentFunc.Invoke(prevFunc.Invoke rawItem)))
                    { state with ClientProjection = nextProj }
                | LinqStage.Cast _ | LinqStage.OfType _ | LinqStage.DefaultIfEmpty _ | LinqStage.Chunk _ | LinqStage.Shuffle ->
                    state
                | _ ->
                    { state with RequiresClientFallback = true }
            else
                match stage with
                | LinqStage.Filter lam ->
                    match SubqueryDecorrelator.tryDecorrelateFilter state.Plan lam Map.empty with
                    | Some (decorPlan, residualOpt) ->
                        match residualOpt with
                        | Some resLam ->
                            let p = resLam.Parameters.[0]
                            let ctx = TranslationContext.Entity p.Name
                            match ExprTranslator.tryTranslateToScalar ctx resLam.Body with
                            | Some pred -> { state with Plan = LogicalPlan.Filter(decorPlan, pred) }
                            | None ->
                                if isTupleOrAnonType p.Type then
                                    { state with Plan = decorPlan; RequiresClientFallback = true }
                                else
                                    { state with Plan = decorPlan; ClientPredicates = [ PipelineStageParser.compileClientPredicate resLam, resLam ] }
                        | None ->
                            { state with Plan = decorPlan }
                    | None ->
                        let p = lam.Parameters.[0]
                        let ctx = TranslationContext.Entity p.Name
                        match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                        | Some pred ->
                            { state with Plan = LogicalPlan.Filter(state.Plan, pred) }
                        | None ->
                            if isTupleOrAnonType p.Type then
                                { state with RequiresClientFallback = true }
                            else
                                { state with ClientPredicates = [ PipelineStageParser.compileClientPredicate lam, lam ] }

                | LinqStage.TakeWhile lam | LinqStage.SkipWhile lam as sStage ->
                    let isSkip = match sStage with LinqStage.SkipWhile _ -> true | _ -> false
                    let p = lam.Parameters.[0]
                    let ctx = TranslationContext.Entity p.Name
                    match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                    | Some boolExpr ->
                        let mask =
                            if isSkip then
                                ScalarExpr.Unary(UnaryOp.Not, ScalarExpr.Function(ScalarFunction.CumMax, [ ScalarExpr.Unary(UnaryOp.Not, boolExpr) ], PlDataType.Boolean))
                            else
                                ScalarExpr.Function(ScalarFunction.CumMin, [ boolExpr ], PlDataType.Boolean)
                        { state with Plan = LogicalPlan.Filter(state.Plan, mask) }
                    | None ->
                        { state with ClientPredicates = [ PipelineStageParser.compileClientPredicate lam, lam ] }

                | LinqStage.Project lam ->
                    match SubqueryDecorrelator.tryDecorrelateProjection state.Plan lam Map.empty with
                    | Some (decorPlan, rewrittenLam) ->
                        let p = rewrittenLam.Parameters.[0]
                        let ctx = TranslationContext.Entity p.Name
                        match extractProjections ctx rewrittenLam.Body with
                        | Some projs ->
                            { state with Plan = LogicalPlan.Project(decorPlan, projs) }
                        | None ->
                            let currentFunc = PipelineStageParser.compileToBoxedFunc rewrittenLam
                            let sourceType = rewrittenLam.Parameters.[0].Type
                            { state with Plan = decorPlan; ClientProjection = Some(sourceType, currentFunc) }
                    | None ->
                        let p = lam.Parameters.[0]
                        let ctx = TranslationContext.Entity p.Name
                        match extractProjections ctx lam.Body with
                        | Some projs ->
                            { state with Plan = LogicalPlan.Project(state.Plan, projs) }
                        | None ->
                            let currentFunc = PipelineStageParser.compileToBoxedFunc lam
                            let sourceType = lam.Parameters.[0].Type
                            { state with ClientProjection = Some(sourceType, currentFunc) }

                | LinqStage.Sort(lam, isDesc, nullsLast) ->
                    let p = lam.Parameters.[0]
                    let ctx = TranslationContext.Entity p.Name
                    match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                    | Some s ->
                        let newOrder = { SortOrder.Expr = s; Descending = isDesc; NullsLast = nullsLast }
                        let newPlan =
                            match state.Plan with
                            | LogicalPlan.Sort(input, prevOrders) ->
                                LogicalPlan.Sort(input, prevOrders @ [ newOrder ])
                            | _ ->
                                LogicalPlan.Sort(state.Plan, [ newOrder ])
                        { state with Plan = newPlan }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.SortDefault(isDesc, nullsLast) ->
                    let newOrder = { SortOrder.Expr = ScalarExpr.Column ""; Descending = isDesc; NullsLast = nullsLast }
                    let newPlan =
                        match state.Plan with
                        | LogicalPlan.Sort(input, prevOrders) ->
                            LogicalPlan.Sort(input, prevOrders @ [ newOrder ])
                        | _ ->
                            LogicalPlan.Sort(state.Plan, [ newOrder ])
                    { state with Plan = newPlan }

                | LinqStage.Take count ->
                    { state with Plan = LogicalPlan.Limit(state.Plan, 0L, count) }

                | LinqStage.Skip count ->
                    { state with Plan = LogicalPlan.Limit(state.Plan, int64 count, UInt32.MaxValue) }

                | LinqStage.TakeLast count ->
                    { state with Plan = LogicalPlan.Limit(state.Plan, -int64 count, count) }

                | LinqStage.SkipLast count ->
                    { state with Plan = LogicalPlan.SkipLast(state.Plan, count) }

                | LinqStage.Distinct ->
                    { state with Plan = LogicalPlan.Distinct(state.Plan, None, PlUniqueKeepStrategy.First) }

                | LinqStage.DistinctBy lam ->
                    let p = lam.Parameters.[0]
                    let ctx = TranslationContext.Entity p.Name
                    match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                    | Some (ScalarExpr.Column col) ->
                        { state with Plan = LogicalPlan.Distinct(state.Plan, Some [ col ], PlUniqueKeepStrategy.First) }
                    | _ ->
                        { state with RequiresClientFallback = true }

                | LinqStage.Reverse ->
                    { state with Plan = LogicalPlan.Reverse state.Plan }

                | LinqStage.Index ->
                    { state with Plan = LogicalPlan.WithRowIndex(state.Plan, "index", None) }

                | LinqStage.CountBy lam ->
                    let p = lam.Parameters.[0]
                    let ctx = TranslationContext.Entity p.Name
                    match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                    | Some keyExpr ->
                        { state with Plan = LogicalPlan.GroupBy(state.Plan, [ keyExpr ], [ ("Count", ScalarExpr.Aggregation(AggKind.Len, None, false)) ], None, false) }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.GroupByKey lam ->
                    let p = lam.Parameters.[0]
                    let ctx = TranslationContext.Entity p.Name
                    match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                    | Some keyExpr ->
                        { state with Plan = LogicalPlan.GroupBy(state.Plan, [ keyExpr ], [], None, false) }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.GroupByWithResult(keyLam, resLam) ->
                    let keyP = keyLam.Parameters.[0]
                    let keyCtx = TranslationContext.Entity keyP.Name
                    let keyExpr = ExprTranslator.tryTranslateToScalar keyCtx keyLam.Body |> Option.defaultValue (ScalarExpr.Column "Key")
                    match tryExtractGroupByResult state.Plan [ keyExpr ] resLam with
                    | Some plan -> { state with Plan = plan }
                    | None ->
                        let resP = resLam.Parameters.[0]
                        let resCtx = TranslationContext.Entity resP.Name
                        match extractProjections resCtx resLam.Body with
                        | Some aggs -> { state with Plan = LogicalPlan.GroupBy(state.Plan, [ keyExpr ], aggs, None, false) }
                        | None -> { state with RequiresClientFallback = true }

                | LinqStage.GroupByWithElementAndResult(keyLam, _elemLam, resLam) ->
                    let keyP = keyLam.Parameters.[0]
                    let keyCtx = TranslationContext.Entity keyP.Name
                    let keyExpr = ExprTranslator.tryTranslateToScalar keyCtx keyLam.Body |> Option.defaultValue (ScalarExpr.Column "Key")
                    match tryExtractGroupByResult state.Plan [ keyExpr ] resLam with
                    | Some plan -> { state with Plan = plan }
                    | None ->
                        let resP = resLam.Parameters.[0]
                        let resCtx = TranslationContext.Entity resP.Name
                        match extractProjections resCtx resLam.Body with
                        | Some aggs -> { state with Plan = LogicalPlan.GroupBy(state.Plan, [ keyExpr ], aggs, None, false) }
                        | None -> { state with RequiresClientFallback = true }

                | LinqStage.Join(methodInfo, innerExpr, outerKey, innerKey, resSelOpt) ->
                    match PolarsObjectConverter.resolveSequencePlan innerExpr with
                    | Some rightPlan ->
                        let isRightJoin = methodInfo.Name = "RightJoin"
                        let how =
                            match methodInfo.Name with
                            | "RightJoin"
                            | "LeftJoin" -> PlJoinType.Left
                            | "FullJoin" -> PlJoinType.Outer
                            | "SemiJoin" -> PlJoinType.Semi
                            | "AntiJoin" -> PlJoinType.Anti
                            | _ -> PlJoinType.Inner

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
                        let leftCtx = SubqueryDecorrelator.createContextForTypeWithCols outerParam.Type leftCols outerParam.Name
                        let rightCtx = SubqueryDecorrelator.createContextForTypeWithCols innerParam.Type rightCols innerParam.Name

                        match tryExtractKeyScalars leftCtx outerKey, tryExtractKeyScalars rightCtx innerKey with
                        | Some leftKeys, Some rightKeys when leftKeys.Length = rightKeys.Length ->
                            let joinPlan =
                                if isRightJoin then
                                    LogicalPlan.Join(rightPlan, state.Plan, rightKeys, leftKeys, how, Some uniqueSuffix, PlJoinCoalesce.KeepColumns, false)
                                else
                                    LogicalPlan.Join(state.Plan, rightPlan, leftKeys, rightKeys, how, Some uniqueSuffix, PlJoinCoalesce.KeepColumns, false)

                            let finalPlan =
                                match resSelOpt with
                                | Some resSel when not (isTransparentJoinResSelector resSel) ->
                                    let leftP = resSel.Parameters.[0]
                                    let rightP = resSel.Parameters.[1]
                                    let resCtx = TranslationContext.BinaryJoin(leftP.Name, rightP.Name, leftColSet, uniqueSuffix)
                                    match SubqueryDecorrelator.tryDecorrelateProjection joinPlan resSel Map.empty with
                                    | Some (decorPlan, rewrittenResSel) ->
                                        let leftP2 = rewrittenResSel.Parameters.[0]
                                        let rightP2 = rewrittenResSel.Parameters.[1]
                                        let resCtx2 = TranslationContext.BinaryJoin(leftP2.Name, rightP2.Name, leftColSet, uniqueSuffix)
                                        match extractProjections resCtx2 rewrittenResSel.Body with
                                        | Some projs -> LogicalPlan.Project(decorPlan, projs)
                                        | None -> decorPlan
                                    | None ->
                                        match extractProjections resCtx resSel.Body with
                                        | Some projs -> LogicalPlan.Project(joinPlan, projs)
                                        | None -> joinPlan
                                | _ -> joinPlan
                            { state with Plan = finalPlan }
                        | _ ->
                            { state with RequiresClientFallback = true }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.GroupJoin(_, innerExpr, outerKey, innerKey, resSelOpt) ->
                    match PolarsObjectConverter.resolveSequencePlan innerExpr with
                    | Some rightPlan ->
                        let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                        let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
                        let leftColSet = Set.ofList leftCols
                        let uniqueSuffix = LinqConstants.DefaultRightSuffix

                        let outerParam = outerKey.Parameters.[0]
                        let innerParam = innerKey.Parameters.[0]
                        let leftCtx = SubqueryDecorrelator.createContextForTypeWithCols outerParam.Type leftCols outerParam.Name
                        let rightCtx = SubqueryDecorrelator.createContextForTypeWithCols innerParam.Type rightCols innerParam.Name

                        match tryExtractKeyScalars leftCtx outerKey, tryExtractKeyScalars rightCtx innerKey with
                        | Some leftKeys, Some rightKeys when leftKeys.Length = rightKeys.Length ->
                            let joinPlan = LogicalPlan.Join(state.Plan, rightPlan, leftKeys, rightKeys, PlJoinType.Left, Some uniqueSuffix, PlJoinCoalesce.KeepColumns, false)
                            let finalPlan =
                                match resSelOpt with
                                | Some resSel when not (isTransparentJoinResSelector resSel) ->
                                    let leftP = resSel.Parameters.[0]
                                    let rightP = resSel.Parameters.[1]
                                    let resCtx = TranslationContext.BinaryJoin(leftP.Name, rightP.Name, leftColSet, uniqueSuffix)
                                    match extractProjections resCtx resSel.Body with
                                    | Some projs -> LogicalPlan.Project(joinPlan, projs)
                                    | None -> joinPlan
                                | _ -> joinPlan
                            { state with Plan = finalPlan }
                        | _ ->
                            { state with RequiresClientFallback = true }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.CrossJoin(innerExpr, resSelOpt) ->
                    match PolarsObjectConverter.resolveSequencePlan innerExpr with
                    | Some rightPlan ->
                        let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames |> Set.ofList
                        let uniqueSuffix = LinqConstants.DefaultRightSuffix
                        let joinPlan = LogicalPlan.Join(state.Plan, rightPlan, [], [], PlJoinType.Cross, Some uniqueSuffix, PlJoinCoalesce.JoinSpecific, false)
                        let finalPlan =
                            match resSelOpt with
                            | Some resSel when not (isTransparentJoinResSelector resSel) ->
                                let leftP = resSel.Parameters.[0]
                                let rightP = resSel.Parameters.[1]
                                let resCtx = TranslationContext.BinaryJoin(leftP.Name, rightP.Name, leftCols, uniqueSuffix)
                                match extractProjections resCtx resSel.Body with
                                | Some projs -> LogicalPlan.Project(joinPlan, projs)
                                | None -> joinPlan
                            | _ -> joinPlan
                        { state with Plan = finalPlan }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.JoinAsOf(_, inner, outerKey, innerKey, outerByOpt, innerByOpt, resSelOpt, toleranceStr, toleranceInt, toleranceFloat, strategy) ->
                    match PolarsObjectConverter.resolveSequencePlan inner with
                    | Some rightPlan ->
                        let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                        let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
                        let leftColSet = Set.ofList leftCols
                        let leftCtx = SubqueryDecorrelator.createContextForTypeWithCols outerKey.Parameters.[0].Type leftCols outerKey.Parameters.[0].Name
                        let rightCtx = SubqueryDecorrelator.createContextForTypeWithCols innerKey.Parameters.[0].Type rightCols innerKey.Parameters.[0].Name
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
                                    Suffix = Some LinqConstants.DefaultRightSuffix
                                    Validation = PlJoinValidation.ManyToMany
                                    Coalesce = PlJoinCoalesce.KeepColumns
                                    MaintainOrder = PlJoinMaintainOrder.None
                                    JoinSide = PlJoinSide.None
                                    NullsEqual = false
                                }
                            let asOfPlan = LogicalPlan.JoinAsOf(state.Plan, rightPlan, spec)
                            let finalPlan =
                                match resSelOpt with
                                | Some resSel when not (isTransparentJoinResSelector resSel) ->
                                    let leftP = resSel.Parameters.[0]
                                    let rightP = resSel.Parameters.[1]
                                    let resCtx = TranslationContext.BinaryJoin(leftP.Name, rightP.Name, leftColSet, LinqConstants.DefaultRightSuffix)
                                    match extractProjections resCtx resSel.Body with
                                    | Some projs -> LogicalPlan.Project(asOfPlan, projs)
                                    | None -> asOfPlan
                                | _ -> asOfPlan
                            { state with Plan = finalPlan }
                        | _ ->
                            { state with RequiresClientFallback = true }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.SetOp(methodInfo, secondExpr, key1Opt, key2Opt) ->
                    match PolarsObjectConverter.resolveSequencePlan secondExpr with
                    | Some rightPlan ->
                        let how =
                            if methodInfo.Name.StartsWith "Intersect" then
                                PlJoinType.Semi
                            else
                                PlJoinType.Anti

                        let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                        let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames

                        let keysOpt =
                            match key1Opt, key2Opt with
                            | Some k1, Some k2 ->
                                let p1 = k1.Parameters.[0]
                                let p2 = k2.Parameters.[0]
                                let ctx1 = SubqueryDecorrelator.createContextForTypeWithCols p1.Type leftCols p1.Name
                                let ctx2 = SubqueryDecorrelator.createContextForTypeWithCols p2.Type rightCols p2.Name
                                match ExprTranslator.tryTranslateToScalar ctx1 k1.Body, ExprTranslator.tryTranslateToScalar ctx2 k2.Body with
                                | Some lk, Some rk -> Some([ lk ], [ rk ])
                                | _ -> None
                            | Some k1, None ->
                                let p1 = k1.Parameters.[0]
                                let ctx1 = SubqueryDecorrelator.createContextForTypeWithCols p1.Type leftCols p1.Name
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
                            { state with Plan = LogicalPlan.Join(state.Plan, rightPlan, leftOn, rightOn, how, None, PlJoinCoalesce.JoinSpecific, true) }
                        | None ->
                            { state with RequiresClientFallback = true }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.Union(secondExpr, _) ->
                    match PolarsObjectConverter.resolveSequencePlan secondExpr with
                    | Some otherPlan ->
                        { state with Plan = LogicalPlan.Distinct(LogicalPlan.Union(state.Plan, otherPlan, isAll = true), None, PlUniqueKeepStrategy.First) }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.Concat secondExpr ->
                    match PolarsObjectConverter.resolveSequencePlan secondExpr with
                    | Some otherPlan ->
                        { state with Plan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true) }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.Append elemExpr ->
                    match tryEvaluate elemExpr with
                    | Some elem when not (isNull elem) ->
                        match PolarsObjectConverter.fromSingleRow elem (elem.GetType()) with
                        | Some otherLf ->
                            let otherPlan = PolarsPhysicalLowerer.scanLazyFrame otherLf
                            { state with Plan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true) }
                        | None ->
                            { state with RequiresClientFallback = true }
                    | _ ->
                        { state with RequiresClientFallback = true }

                | LinqStage.Prepend elemExpr ->
                    match tryEvaluate elemExpr with
                    | Some elem when not (isNull elem) ->
                        match PolarsObjectConverter.fromSingleRow elem (elem.GetType()) with
                        | Some otherLf ->
                            let otherPlan = PolarsPhysicalLowerer.scanLazyFrame otherLf
                            { state with Plan = LogicalPlan.Union(otherPlan, state.Plan, isAll = true) }
                        | None ->
                            { state with RequiresClientFallback = true }
                    | _ ->
                        { state with RequiresClientFallback = true }

                | LinqStage.Zip(secondExpr, resSelOpt) ->
                    match PolarsObjectConverter.resolveSequencePlan secondExpr with
                    | Some secondPlan ->
                        let zipPlan = LogicalPlan.HorizontalConcat [ state.Plan; secondPlan ]
                        let finalPlan =
                            match resSelOpt with
                            | Some resSel when not (isTransparentJoinResSelector resSel) ->
                                let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames |> Set.ofList
                                let leftP = resSel.Parameters.[0]
                                let rightP = resSel.Parameters.[1]
                                let resCtx = TranslationContext.BinaryJoin(leftP.Name, rightP.Name, leftCols, "_second")
                                match extractProjections resCtx resSel.Body with
                                | Some projs -> LogicalPlan.Project(zipPlan, projs)
                                | None -> zipPlan
                            | _ -> zipPlan
                        { state with Plan = finalPlan }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.Zip3(secondExpr, thirdExpr) ->
                    match PolarsObjectConverter.resolveSequencePlan secondExpr, PolarsObjectConverter.resolveSequencePlan thirdExpr with
                    | Some secondPlan, Some thirdPlan ->
                        { state with Plan = LogicalPlan.HorizontalConcat [ state.Plan; secondPlan; thirdPlan ] }
                    | _ ->
                        { state with RequiresClientFallback = true }

                | LinqStage.Unpivot(colLam, _) ->
                    match tryExtractUnpivotPattern colLam None with
                    | Some pat ->
                        { state with Plan = LogicalPlan.Unpivot(state.Plan, pat.ExplicitIndexCols, Some (pat.OnCols |> Array.toList), pat.VariableName, pat.ValueName) }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.Explode(colLam, _) ->
                    let p = colLam.Parameters.[0]
                    let ctx = TranslationContext.Entity p.Name
                    match ColumnResolver.tryResolveColumnNameWithContext ctx colLam.Body with
                    | Some col ->
                        { state with Plan = LogicalPlan.Explode(state.Plan, [ col ], false, false) }
                    | None ->
                        { state with RequiresClientFallback = true }

                | LinqStage.Cast _
                | LinqStage.OfType _
                | LinqStage.DefaultIfEmpty _
                | LinqStage.Chunk _
                | LinqStage.Shuffle ->
                    state

                | _ ->
                    { state with RequiresClientFallback = true }

        let finalState = stages |> List.fold foldStage initState
        { finalState with ClientPredicates = List.rev finalState.ClientPredicates }

    /// Constructs a LogicalPlan tree by applying a sequence of LinqStages to an initial LogicalPlan.
    let fromStages (initialPlan: LogicalPlan) (stages: LinqStage list) : LogicalPlan =
        (buildCompiledQuery initialPlan stages).Plan


    /// Constructs a LogicalPlan tree from an initial LogicalPlanSource, schema, and sequence of LinqStages.
    let fromScan (source: LogicalPlanSource) (schema: LogicalSchema) (stages: LinqStage list) : LogicalPlan =
        fromStages (LogicalPlan.Scan(source, schema)) stages