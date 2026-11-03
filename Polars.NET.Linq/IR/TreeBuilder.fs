namespace Polars.NET.Linq.IR

open System
open System.Reflection
open System.Linq
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Core.Helpers
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
        match tryExtractMemberBindings (ExpressionPatterns.unwrap body) with
        | Some bindings ->
            let translated =
                bindings
                |> List.choose (fun (name, expr) ->
                    ExprTranslator.tryTranslateToScalar ctx expr
                    |> Option.map (fun s -> name, s))
            if translated.Length = bindings.Length then Some translated else None
        | None ->
            let unwrappedBody = ExpressionPatterns.unwrap body
            match unwrappedBody with
            | MemberAccess _ as ma when not (PolarsTypeHelper.IsScalarType ma.Type) && (ColumnResolver.tryResolveColumnNameWithContext ctx ma).IsNone ->
                let entityProps = ma.Type.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                let matchedProjs =
                    entityProps
                    |> Array.choose (fun prop ->
                        let propAccess = Expression.Property(ma, prop) :> Expression
                        match ColumnResolver.tryResolveColumnNameWithContext ctx propAccess with
                        | Some physCol -> Some(prop.Name, ScalarExpr.Column physCol)
                        | None -> None)
                    |> Array.toList
                if not matchedProjs.IsEmpty then
                    Some matchedProjs
                else
                    None
            | other ->
                ExprTranslator.tryTranslateToScalar ctx other
                |> Option.map (fun s ->
                    let colName =
                        match s with
                        | ScalarExpr.Column col -> col
                        | _ ->
                            match ctx with
                            | TranslationContext.Scalar(_, physCol) -> physCol
                            | _ -> "value"
                    [ (colName, s) ])

    let private tryExtractJoinProjections
        (ctx: TranslationContext)
        (pLeft: ParameterExpression)
        (pRight: ParameterExpression)
        (leftCols: string list)
        (rightCols: string list)
        (uniqueSuffix: string)
        (invertSides: bool)
        (body: Expression)
        : (string * ScalarExpr) list option =
        let firstParam, secondParam, firstCols, secondCols, secondSuffix =
            if invertSides then
                pRight, pLeft, rightCols, leftCols, LinqConstants.DefaultLeftSuffix
            else
                pLeft, pRight, leftCols, rightCols, uniqueSuffix

        let firstColSet = Set.ofList firstCols
        let unwrapped = ExpressionPatterns.unwrap body
        match unwrapped with
        | :? ParameterExpression as p when p.Name = firstParam.Name ->
            Some(firstCols |> List.map (fun c -> c, ScalarExpr.Column c))
        | :? ParameterExpression as p when p.Name = secondParam.Name ->
            Some(secondCols |> List.map (fun c ->
                let physCol = if firstColSet.Contains c then c + secondSuffix else c
                c, ScalarExpr.Column physCol))
        | _ ->
            extractProjections ctx unwrapped

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
                                |> Option.map (fun s -> false, ScalarExpr.Aggregation(AggKind.Sum, Some (ScalarExpr.Cast(s, PlDataType.Int64, false)), false))
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
    let buildCompiledQuery (initialPlan: LogicalPlan) (isGrouping: bool) (stages: LinqStage list) : CompiledLogicalQuery =
        let initState: CompiledLogicalQuery =
            {
                Plan = initialPlan
                ClientPredicates = []
                ClientProjection = None
                RequiresClientFallback = false
            }

        let rec processStages (state: CompiledLogicalQuery) (scopeMap: Map<string * string, string>) (remainingStages: LinqStage list) : CompiledLogicalQuery =
            let makeCtx (t: Type) (pName: string) (plan: LogicalPlan) =
                let cols = (SchemaInference.inferPlanSchema plan).ColumnNames
                PipelineStageParser.createContextWithScopeAndCols scopeMap t cols pName

            match remainingStages with
            | [] -> state
            | stage :: tail ->
                let canPushdown = state.ClientPredicates.IsEmpty && state.ClientProjection.IsNone && not state.RequiresClientFallback

                if state.RequiresClientFallback then
                    match stage with
                    | LinqStage.Filter lam ->
                        processStages { state with ClientPredicates = (PipelineStageParser.compileClientPredicate lam, lam) :: state.ClientPredicates } scopeMap tail
                    | LinqStage.Project lam ->
                        let currentFunc = PipelineStageParser.compileToBoxedFunc lam
                        let nextProj =
                            match state.ClientProjection with
                            | None -> Some(lam.Parameters.[0].Type, currentFunc)
                            | Some(prevSrcType, prevFunc) -> Some(prevSrcType, Func<obj, obj>(fun rawItem -> currentFunc.Invoke(prevFunc.Invoke rawItem)))
                        processStages { state with ClientProjection = nextProj } scopeMap tail
                    | _ ->
                        processStages state scopeMap tail
                elif not canPushdown then
                    match stage with
                    | LinqStage.Filter lam ->
                        processStages { state with ClientPredicates = (PipelineStageParser.compileClientPredicate lam, lam) :: state.ClientPredicates } scopeMap tail
                    | LinqStage.Project lam ->
                        let currentFunc = PipelineStageParser.compileToBoxedFunc lam
                        let nextProj =
                            match state.ClientProjection with
                            | None -> Some(lam.Parameters.[0].Type, currentFunc)
                            | Some(prevSrcType, prevFunc) -> Some(prevSrcType, Func<obj, obj>(fun rawItem -> currentFunc.Invoke(prevFunc.Invoke rawItem)))
                        processStages { state with ClientProjection = nextProj } scopeMap tail
                    | LinqStage.Cast _ | LinqStage.OfType _ | LinqStage.DefaultIfEmpty _ | LinqStage.Chunk _ | LinqStage.Shuffle | LinqStage.AggregateBy _ ->
                        processStages state scopeMap tail
                    | _ ->
                        processStages { state with RequiresClientFallback = true } scopeMap tail
                else
                    match stage with
                    | LinqStage.Filter lam ->
                        match SubqueryDecorrelator.tryDecorrelateFilter state.Plan lam scopeMap with
                        | Some (decorPlan, residualOpt) ->
                            match residualOpt with
                            | Some resLam ->
                                let p = resLam.Parameters.[0]
                                let ctx = makeCtx p.Type p.Name decorPlan
                                match ExprTranslator.tryTranslateToScalar ctx resLam.Body with
                                | Some pred -> processStages { state with Plan = LogicalPlan.Filter(decorPlan, pred) } scopeMap tail
                                | None ->
                                    if isTupleOrAnonType p.Type then
                                        processStages { state with Plan = decorPlan; RequiresClientFallback = true } scopeMap tail
                                    else
                                        processStages { state with Plan = decorPlan; ClientPredicates = (PipelineStageParser.compileClientPredicate resLam, resLam) :: state.ClientPredicates } scopeMap tail
                            | None ->
                                processStages { state with Plan = decorPlan } scopeMap tail
                        | None ->
                            let p = lam.Parameters.[0]
                            let ctx = makeCtx p.Type p.Name state.Plan
                            match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                            | Some pred ->
                                processStages { state with Plan = LogicalPlan.Filter(state.Plan, pred) } scopeMap tail
                            | None ->
                                if isTupleOrAnonType p.Type then
                                    processStages { state with RequiresClientFallback = true } scopeMap tail
                                else
                                    processStages { state with ClientPredicates = (PipelineStageParser.compileClientPredicate lam, lam) :: state.ClientPredicates } scopeMap tail

                    | LinqStage.TakeWhile lam | LinqStage.SkipWhile lam as sStage ->
                        let isSkip = match sStage with LinqStage.SkipWhile _ -> true | _ -> false
                        let p = lam.Parameters.[0]
                        let ctx = makeCtx p.Type p.Name state.Plan
                        match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                        | Some boolExpr ->
                            let mask =
                                if isSkip then
                                    ScalarExpr.Function(ScalarFunction.CumMax, [ ScalarExpr.Unary(UnaryOp.Not, boolExpr) ], PlDataType.Boolean)
                                else
                                    ScalarExpr.Function(ScalarFunction.CumMin, [ boolExpr ], PlDataType.Boolean)
                            processStages { state with Plan = LogicalPlan.Filter(state.Plan, mask) } scopeMap tail
                        | None ->
                            processStages { state with ClientPredicates = (PipelineStageParser.compileClientPredicate lam, lam) :: state.ClientPredicates } scopeMap tail

                    | LinqStage.Project lam ->
                        match ExpressionPatterns.unwrap lam.Body with
                        | :? ParameterExpression as p when p.Name = lam.Parameters.[0].Name ->
                            processStages state scopeMap tail
                        | _ ->
                            match SubqueryDecorrelator.tryDecorrelateProjection state.Plan lam scopeMap with
                            | Some (decorPlan, rewrittenLam) ->
                                let p = rewrittenLam.Parameters.[0]
                                let ctx = makeCtx p.Type p.Name decorPlan
                                match extractProjections ctx rewrittenLam.Body with
                                | Some projs ->
                                    processStages { state with Plan = LogicalPlan.Project(decorPlan, projs) } Map.empty tail
                                | None ->
                                    let currentFunc = PipelineStageParser.compileToBoxedFunc rewrittenLam
                                    let sourceType = rewrittenLam.Parameters.[0].Type
                                    processStages { state with Plan = decorPlan; ClientProjection = Some(sourceType, currentFunc) } Map.empty tail
                            | None ->
                                let p = lam.Parameters.[0]
                                let ctx = makeCtx p.Type p.Name state.Plan
                                let isTransparentLetOpt =
                                    match tryExtractMemberBindings lam.Body with
                                    | Some bindings ->
                                        let isNav (_name: string, expr: Expression) =
                                            match ExpressionPatterns.unwrap expr with
                                            | :? ParameterExpression as p when p.Name = lam.Parameters.[0].Name && not (PolarsTypeHelper.IsScalarType p.Type) -> true
                                            | MemberAccess(_, m) when isNavigationMember m -> true
                                            | _ -> false
                                        let navBindings, computedBindings = bindings |> List.partition isNav
                                        if not navBindings.IsEmpty && not computedBindings.IsEmpty then
                                            let translated =
                                                computedBindings
                                                |> List.choose (fun (name, expr) ->
                                                    ExprTranslator.tryTranslateToScalar ctx expr
                                                    |> Option.map (fun s -> name, s))
                                            if translated.Length = computedBindings.Length then
                                                Some translated
                                            else None
                                        else None
                                    | _ -> None

                                match isTransparentLetOpt with
                                | Some withCols ->
                                    processStages { state with Plan = LogicalPlan.WithColumns(state.Plan, withCols) } scopeMap tail
                                | None ->
                                    match extractProjections ctx lam.Body with
                                    | Some projs ->
                                        processStages { state with Plan = LogicalPlan.Project(state.Plan, projs) } Map.empty tail
                                    | None ->
                                        let currentFunc = PipelineStageParser.compileToBoxedFunc lam
                                        let sourceType = lam.Parameters.[0].Type
                                        processStages { state with ClientProjection = Some(sourceType, currentFunc) } Map.empty tail

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
                                processStages { state with RequiresClientFallback = true } scopeMap tail

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
                        processStages { state with Plan = LogicalPlan.Distinct(state.Plan, None, PlUniqueKeepStrategy.First) } scopeMap tail

                    | LinqStage.DistinctBy lam ->
                        match PipelineStageParser.extractDistinctColumns lam with
                        | Some cols ->
                            processStages { state with Plan = LogicalPlan.Distinct(state.Plan, Some (Array.toList cols), PlUniqueKeepStrategy.First) } scopeMap tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.Reverse ->
                        processStages { state with Plan = LogicalPlan.Reverse state.Plan } scopeMap tail

                    | LinqStage.Index ->
                        processStages { state with Plan = LogicalPlan.WithRowIndex(state.Plan, "index", None) } scopeMap tail

                    | LinqStage.CountBy lam ->
                        let p = lam.Parameters.[0]
                        let ctx = makeCtx p.Type p.Name state.Plan
                        match ExprTranslator.tryTranslateToScalar ctx lam.Body with
                        | Some keyExpr ->
                            let groupPlan = LogicalPlan.GroupBy(state.Plan, [ keyExpr ], [ ("Value", ScalarExpr.Aggregation(AggKind.Len, None, false)) ], None, false)
                            let keyCol = match keyExpr with ScalarExpr.Column c -> c | _ -> "Key"
                            let projs = [ ("Key", ScalarExpr.Column keyCol); ("Value", ScalarExpr.Column "Value") ]
                            processStages { state with Plan = LogicalPlan.Project(groupPlan, projs) } Map.empty tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

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
                                let activeLf = PolarsPhysicalLowerer.lowerPlan state.Plan
                                let resKeyParamName = if resLam.Parameters.Count >= 1 then resLam.Parameters.[0].Name else keyP.Name
                                let isKeyArg (e: Expression) =
                                    match ExpressionPatterns.unwrap e with
                                    | :? ParameterExpression as p when p.Name = resKeyParamName || p.Name = keyP.Name -> true
                                    | MemberAccess(:? ParameterExpression as p, _) when p.Name = resKeyParamName || p.Name = keyP.Name -> true
                                    | _ -> false
                                let ctx: GroupFoldContext = { KeyLambda = keyLam; ElemLambdaOpt = None; HavingPreds = []; SortStages = [] }
                                let groupParamName = if resLam.Parameters.Count >= 2 then resLam.Parameters.[1].Name else resLam.Parameters.[0].Name
                                match compileUnifiedGroupBy activeLf ctx resLam.Body groupParamName isKeyArg scopeMap with
                                | Some ops ->
                                    let nextPlan = PolarsPhysicalLowerer.fromOpsOnPlan (PolarsPhysicalLowerer.scanLazyFrame activeLf) (List.rev ops)
                                    processStages { state with Plan = nextPlan } Map.empty tail
                                | None ->
                                    processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.GroupByWithElementAndResult(keyLam, elemLam, resLam) ->
                        let keyP = keyLam.Parameters.[0]
                        let keyCtx = makeCtx keyP.Type keyP.Name state.Plan
                        let keyExprs = tryExtractKeyScalars keyCtx keyLam |> Option.defaultValue [ ScalarExpr.Column "Key" ]
                        match tryExtractGroupByResult state.Plan keyExprs resLam with
                        | Some plan -> processStages { state with Plan = plan } Map.empty tail
                        | None ->
                            let activeLf = PolarsPhysicalLowerer.lowerPlan state.Plan
                            let resKeyParamName = if resLam.Parameters.Count >= 1 then resLam.Parameters.[0].Name else keyP.Name
                            let isKeyArg (e: Expression) =
                                match ExpressionPatterns.unwrap e with
                                | :? ParameterExpression as p when p.Name = resKeyParamName || p.Name = keyP.Name -> true
                                | MemberAccess(:? ParameterExpression as p, _) when p.Name = resKeyParamName || p.Name = keyP.Name -> true
                                | _ -> false
                            let ctx: GroupFoldContext = { KeyLambda = keyLam; ElemLambdaOpt = Some elemLam; HavingPreds = []; SortStages = [] }
                            let groupParamName = if resLam.Parameters.Count >= 2 then resLam.Parameters.[1].Name else resLam.Parameters.[0].Name
                            match compileUnifiedGroupBy activeLf ctx resLam.Body groupParamName isKeyArg scopeMap with
                            | Some ops ->
                                let nextPlan = PolarsPhysicalLowerer.fromOpsOnPlan (PolarsPhysicalLowerer.scanLazyFrame activeLf) (List.rev ops)
                                processStages { state with Plan = nextPlan } Map.empty tail
                            | None ->
                                processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.GroupByRollup(keyLambda, resLambda) ->
                        let activeLf = PolarsPhysicalLowerer.lowerPlan state.Plan
                        match compileUnifiedGroupingSets activeLf GroupingKind.Rollup keyLambda resLambda scopeMap with
                        | Some groupedLf ->
                            let nextPlan = PolarsPhysicalLowerer.scanLazyFrame groupedLf
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.GroupByCube(keyLambda, resLambda) ->
                        let activeLf = PolarsPhysicalLowerer.lowerPlan state.Plan
                        match compileUnifiedGroupingSets activeLf GroupingKind.Cube keyLambda resLambda scopeMap with
                        | Some groupedLf ->
                            let nextPlan = PolarsPhysicalLowerer.scanLazyFrame groupedLf
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.GroupByGroupingSets(keyLambda, setsExpr, resLambda) ->
                        let activeLf = PolarsPhysicalLowerer.lowerPlan state.Plan
                        match compileUnifiedGroupingSets activeLf (GroupingKind.GroupingSets setsExpr) keyLambda resLambda scopeMap with
                        | Some groupedLf ->
                            let nextPlan = PolarsPhysicalLowerer.scanLazyFrame groupedLf
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.GroupPartitionSlice(keySelector, innerStages, resSelOpt) ->
                        let activeLf = PolarsPhysicalLowerer.lowerPlan state.Plan
                        match compileGroupPartitionSlice activeLf keySelector innerStages resSelOpt with
                        | Some ops ->
                            let nextPlan = PolarsPhysicalLowerer.fromOpsOnPlan (PolarsPhysicalLowerer.scanLazyFrame activeLf) (List.rev ops)
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.Join(methodInfo, innerExpr, outerKey, innerKey, resSelOpt) ->
                        match PolarsObjectConverter.resolveSequencePlan innerExpr with
                        | Some rightPlan ->
                            let isRightJoin = methodInfo.Name = "RightJoin"
                            let how =
                                match methodInfo.Name with
                                | "RightJoin"
                                | "LeftJoin"
                                | "GroupJoin" -> PlJoinType.Left
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
                            let leftCtx = PipelineStageParser.createContextWithScopeAndCols scopeMap outerParam.Type leftCols outerParam.Name
                            let rightCtx = PipelineStageParser.createContextWithScopeAndCols Map.empty innerParam.Type rightCols innerParam.Name

                            match tryExtractKeyScalars leftCtx outerKey, tryExtractKeyScalars rightCtx innerKey with
                            | Some leftKeys, Some rightKeys when leftKeys.Length = rightKeys.Length ->
                                let joinPlan =
                                    if isRightJoin then
                                        LogicalPlan.Join(rightPlan, state.Plan, rightKeys, leftKeys, how, Some uniqueSuffix, PlJoinCoalesce.KeepColumns, false)
                                    else
                                        LogicalPlan.Join(state.Plan, rightPlan, leftKeys, rightKeys, how, Some uniqueSuffix, PlJoinCoalesce.KeepColumns, false)

                                let leftScope = outerKey.Parameters.[0].Name
                                let rightScope =
                                    match resSelOpt with
                                    | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                                    | _ -> innerKey.Parameters.[0].Name

                                let updatedScopeMap =
                                    PipelineStageParser.updateJoinScopeMap scopeMap leftScope (List.toArray leftCols) rightScope (List.toArray rightCols) uniqueSuffix isRightJoin

                                match resSelOpt with
                                | TransparentOrNoResSelector ->
                                    processStages { state with Plan = joinPlan } updatedScopeMap tail
                                | ConcreteResSelector resSel ->
                                    let postJoinCols = (SchemaInference.inferPlanSchema joinPlan).ColumnNames
                                    let pLeft = resSel.Parameters.[0]
                                    let pRight = resSel.Parameters.[1]
                                    let resCtx =
                                        if not updatedScopeMap.IsEmpty then
                                            TranslationContext.ScopedEntity([ pLeft.Name; pRight.Name ], updatedScopeMap, postJoinCols)
                                        else
                                            TranslationContext.BinaryJoin(pLeft.Name, pRight.Name, leftColSet, uniqueSuffix)

                                    match SubqueryDecorrelator.tryDecorrelateProjection joinPlan resSel updatedScopeMap with
                                    | Some (decorPlan, rewrittenResSel) ->
                                        let postDecorCols = (SchemaInference.inferPlanSchema decorPlan).ColumnNames
                                        let pLeft2 = rewrittenResSel.Parameters.[0]
                                        let pRight2 = rewrittenResSel.Parameters.[1]
                                        let resCtx2 =
                                            if not updatedScopeMap.IsEmpty then
                                                TranslationContext.ScopedEntity([ pLeft2.Name; pRight2.Name ], updatedScopeMap, postDecorCols)
                                            else
                                                TranslationContext.BinaryJoin(pLeft2.Name, pRight2.Name, leftColSet, uniqueSuffix)

                                        match tryExtractJoinProjections resCtx2 pLeft2 pRight2 leftCols rightCols uniqueSuffix isRightJoin rewrittenResSel.Body with
                                        | Some projs -> processStages { state with Plan = LogicalPlan.Project(decorPlan, projs) } Map.empty tail
                                        | None -> processStages { state with Plan = decorPlan } updatedScopeMap tail
                                    | None ->
                                        match tryExtractJoinProjections resCtx pLeft pRight leftCols rightCols uniqueSuffix isRightJoin resSel.Body with
                                        | Some projs -> processStages { state with Plan = LogicalPlan.Project(joinPlan, projs) } Map.empty tail
                                        | None -> processStages { state with Plan = joinPlan } updatedScopeMap tail
                            | _ ->
                                processStages { state with RequiresClientFallback = true } scopeMap tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, resSelOpt) ->
                        match tail with
                        | LinqStage.Explode(collSelector, explodeResSelOpt) :: nextTail when (tryExtractDefaultIfEmpty collSelector.Body).IsSome ->
                            let effResSelOpt = explodeResSelOpt |> Option.orElse resSelOpt
                            processStages state scopeMap (LinqStage.Join(m, innerExpr, outerKey, innerKey, effResSelOpt) :: nextTail)
                        | LinqStage.Project projLambda :: nextTail when resSelOpt.IsNone ->
                            let outerType, innerType = outerKey.Parameters.[0].Type, innerKey.Parameters.[0].Type
                            let desugaredResSel = PipelineStageParser.desugarGroupJoinProject projLambda outerType innerType
                            processStages state scopeMap (LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, Some desugaredResSel) :: nextTail)
                        | _ ->
                            match resSelOpt with
                            | Some resSel ->
                                let activeLf = PolarsPhysicalLowerer.lowerPlan state.Plan
                                match compileGroupJoinWithResult activeLf innerExpr outerKey innerKey resSel with
                                | Some ops ->
                                    let nextPlan = PolarsPhysicalLowerer.fromOpsOnPlan (PolarsPhysicalLowerer.scanLazyFrame activeLf) (List.rev ops)
                                    processStages { state with Plan = nextPlan } Map.empty tail
                                | None ->
                                    processStages { state with RequiresClientFallback = true } scopeMap tail
                            | None ->
                                processStages state scopeMap (LinqStage.Join(m, innerExpr, outerKey, innerKey, None) :: tail)

                    | LinqStage.CrossJoin(innerExpr, resSelOpt) ->
                        match PolarsObjectConverter.resolveSequencePlan innerExpr with
                        | Some rightPlan ->
                            let leftCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
                            let rightCols = (SchemaInference.inferPlanSchema rightPlan).ColumnNames
                            let leftColSet = Set.ofList leftCols
                            let uniqueSuffix = LinqConstants.DefaultRightSuffix
                            let joinPlan = LogicalPlan.Join(state.Plan, rightPlan, [], [], PlJoinType.Cross, Some uniqueSuffix, PlJoinCoalesce.JoinSpecific, false)

                            let leftScope = "outer"
                            let rightScope = "inner"
                            let updatedScopeMap =
                                PipelineStageParser.updateJoinScopeMap scopeMap leftScope (List.toArray leftCols) rightScope (List.toArray rightCols) uniqueSuffix false

                            match resSelOpt with
                            | TransparentOrNoResSelector ->
                                processStages { state with Plan = joinPlan } updatedScopeMap tail
                            | ConcreteResSelector resSel ->
                                let postJoinCols = (SchemaInference.inferPlanSchema joinPlan).ColumnNames
                                let pLeft = resSel.Parameters.[0]
                                let pRight = resSel.Parameters.[1]
                                let resCtx =
                                    if not updatedScopeMap.IsEmpty then
                                        TranslationContext.ScopedEntity([ pLeft.Name; pRight.Name ], updatedScopeMap, postJoinCols)
                                    else
                                        TranslationContext.BinaryJoin(pLeft.Name, pRight.Name, leftColSet, uniqueSuffix)
                                match tryExtractJoinProjections resCtx pLeft pRight leftCols rightCols uniqueSuffix false resSel.Body with
                                | Some projs -> processStages { state with Plan = LogicalPlan.Project(joinPlan, projs) } Map.empty tail
                                | None -> processStages { state with Plan = joinPlan } updatedScopeMap tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.JoinAsOf(methodInfo, inner, outerKey, innerKey, outerByOpt, innerByOpt, resSelOpt, toleranceStr, toleranceInt, toleranceFloat, strategy) ->
                        match PolarsObjectConverter.resolveSequencePlan inner with
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
                                        Validation = PlJoinValidation.ManyToMany
                                        Coalesce = PlJoinCoalesce.KeepColumns
                                        MaintainOrder = PlJoinMaintainOrder.None
                                        JoinSide = PlJoinSide.None
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

                                match resSelOpt with
                                | TransparentOrNoResSelector ->
                                    processStages { state with Plan = asOfPlan } updatedScopeMap tail
                                | ConcreteResSelector resSel ->
                                    let postJoinCols = (SchemaInference.inferPlanSchema asOfPlan).ColumnNames
                                    let pLeft = resSel.Parameters.[0]
                                    let pRight = resSel.Parameters.[1]
                                    let resCtx =
                                        if not updatedScopeMap.IsEmpty then
                                            TranslationContext.ScopedEntity([ pLeft.Name; pRight.Name ], updatedScopeMap, postJoinCols)
                                        else
                                            TranslationContext.BinaryJoin(pLeft.Name, pRight.Name, leftColSet, uniqueSuffix)
                                    match tryExtractJoinProjections resCtx pLeft pRight leftCols rightCols uniqueSuffix false resSel.Body with
                                    | Some projs -> processStages { state with Plan = LogicalPlan.Project(asOfPlan, projs) } Map.empty tail
                                    | None -> processStages { state with Plan = asOfPlan } updatedScopeMap tail
                            | _ ->
                                processStages { state with RequiresClientFallback = true } scopeMap tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

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
                                processStages { state with Plan = LogicalPlan.Join(state.Plan, rightPlan, leftOn, rightOn, how, None, PlJoinCoalesce.JoinSpecific, true) } scopeMap tail
                            | None ->
                                processStages { state with RequiresClientFallback = true } scopeMap tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.Union(secondExpr, keyLambdaOpt) ->
                        match PolarsObjectConverter.resolveSequencePlan secondExpr with
                        | Some otherPlan ->
                            let subsetOpt = keyLambdaOpt |> Option.bind PipelineStageParser.extractDistinctColumns |> Option.map Array.toList
                            let unionPlan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true)
                            processStages { state with Plan = LogicalPlan.Distinct(unionPlan, subsetOpt, PlUniqueKeepStrategy.First) } scopeMap tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.Concat secondExpr ->
                        match PolarsObjectConverter.resolveSequencePlan secondExpr with
                        | Some otherPlan ->
                            processStages { state with Plan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true) } scopeMap tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.Append elemExpr ->
                        match tryEvaluate elemExpr with
                        | Some elem when not (isNull elem) ->
                            match PolarsObjectConverter.fromSingleRow elem (elem.GetType()) with
                            | Some otherLf ->
                                let otherPlan = PolarsPhysicalLowerer.scanLazyFrame otherLf
                                processStages { state with Plan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true) } scopeMap tail
                            | None ->
                                processStages { state with RequiresClientFallback = true } scopeMap tail
                        | _ ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.Prepend elemExpr ->
                        match tryEvaluate elemExpr with
                        | Some elem when not (isNull elem) ->
                            match PolarsObjectConverter.fromSingleRow elem (elem.GetType()) with
                            | Some otherLf ->
                                let otherPlan = PolarsPhysicalLowerer.scanLazyFrame otherLf
                                processStages { state with Plan = LogicalPlan.Union(otherPlan, state.Plan, isAll = true) } scopeMap tail
                            | None ->
                                processStages { state with RequiresClientFallback = true } scopeMap tail
                        | _ ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.Zip(secondExpr, resSelOpt) ->
                        let activeLf = PolarsPhysicalLowerer.lowerPlan state.Plan
                        match RelationalCompilers.compileZip activeLf secondExpr resSelOpt with
                        | Some ops ->
                            let nextPlan = PolarsPhysicalLowerer.fromOpsOnPlan (PolarsPhysicalLowerer.scanLazyFrame activeLf) (List.rev ops)
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

                    | LinqStage.Zip3(secondExpr, thirdExpr) ->
                        let activeLf = PolarsPhysicalLowerer.lowerPlan state.Plan
                        match RelationalCompilers.compileZip3 activeLf secondExpr thirdExpr with
                        | Some op ->
                            let nextPlan = PolarsPhysicalLowerer.fromOpsOnPlan (PolarsPhysicalLowerer.scanLazyFrame activeLf) [ op ]
                            processStages { state with Plan = nextPlan } Map.empty tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

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
                                processStages { state with RequiresClientFallback = true } scopeMap tail
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
                                    match extractProjections resCtx resSel.Body with
                                    | Some projs ->
                                        processStages { state with Plan = LogicalPlan.Project(unpivotPlan, projs) } Map.empty tail
                                    | None ->
                                        let currentFunc = PipelineStageParser.compileToBoxedFunc resSel
                                        let sourceType = resSel.Parameters.[0].Type
                                        processStages { state with Plan = unpivotPlan; ClientProjection = Some(sourceType, currentFunc) } Map.empty tail
                        | None ->
                            processStages { state with RequiresClientFallback = true } scopeMap tail

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
                            processStages { state with RequiresClientFallback = true } scopeMap tail

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
                                | MemberAccess(target, m) when isNavigationMember m || isTupleOrAnonType m.DeclaringType ->
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
                                            (p.Type.IsGenericType && p.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>)
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
                            | :? ParameterExpression as p when p.Name = param.Name && p.Type.IsGenericType && p.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>> ->
                                Some(name, p :> Expression)
                            | _ -> None)
                    let updatedBindings =
                        bindings
                        |> List.fold (fun m (name, expr) ->
                            let subExpr = substituteLet expr
                            let normalizedExpr =
                                match gParamOpt with
                                | Some(gPropName, gParamExpr) ->
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
                        | :? ParameterExpression as p when p.Name = projLambda.Parameters.[0].Name ->
                            true
                        | _ -> false

                    let gParamName =
                        match letBindings |> Map.tryPick (fun _ v -> match ExpressionPatterns.unwrap v with :? ParameterExpression as p when p.Type.IsGenericType && p.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>> -> Some p.Name | _ -> None) with
                        | Some name -> name
                        | None -> projLambda.Parameters.[0].Name

                    let activeLf = PolarsPhysicalLowerer.lowerPlan state.Plan
                    match compileUnifiedGroupBy activeLf ctx finalProjBody gParamName isKeyArg scopeMap with
                    | Some compiledOps ->
                        let nextPlan = PolarsPhysicalLowerer.fromOpsOnPlan (PolarsPhysicalLowerer.scanLazyFrame activeLf) (List.rev compiledOps)
                        processStages { state with Plan = nextPlan } Map.empty next
                    | None ->
                        processStages { state with RequiresClientFallback = true } scopeMap next
            | [] ->
                if isGrouping then
                    let clientHavingPreds =
                        ctx.HavingPreds |> List.map (fun p -> PipelineStageParser.compileClientPredicate p, p)
                    { state with ClientPredicates = clientHavingPreds @ state.ClientPredicates }
                else
                    { state with RequiresClientFallback = true }
            | otherHead :: otherTail ->
                processStages { state with RequiresClientFallback = true } scopeMap (otherHead :: otherTail)

        let finalState = processStages initState Map.empty stages
        { finalState with ClientPredicates = List.rev finalState.ClientPredicates }

    /// Constructs a LogicalPlan tree by applying a sequence of LinqStages to an initial LogicalPlan.
    let fromStages (initialPlan: LogicalPlan) (stages: LinqStage list) : LogicalPlan =
        (buildCompiledQuery initialPlan false stages).Plan


    /// Constructs a LogicalPlan tree from an initial LogicalPlanSource, schema, and sequence of LinqStages.
    let fromScan (source: LogicalPlanSource) (schema: LogicalSchema) (stages: LinqStage list) : LogicalPlan =
        fromStages (LogicalPlan.Scan(source, schema)) stages