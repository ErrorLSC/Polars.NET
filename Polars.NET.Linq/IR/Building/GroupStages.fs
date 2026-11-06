namespace Polars.NET.Linq.IR

open System.Linq.Expressions
open Polars.NET.Linq
open Polars.NET.Linq.IR.ProjectionExtraction
open Polars.NET.Linq.IR.GroupByPlanning
open Polars.NET.Linq.IR.GroupingSetsPlanning
open Polars.NET.Linq.IR.StageContext

/// Grouping stages: group-bys (plain, with selectors, rollup / cube / grouping sets), per-group slices, and the fold of the stages that follow a group-by into it.
/// Each handler compiles one stage onto the query state and continues with the remaining stages.
module internal GroupStages =

    /// Folds the stages that follow a group-by (having filters, orderings, transparent lets) into the grouped plan,
    /// up to the projection that reads the groups.
    let rec slurp
        (env: StageEnv)
        (continueWith: StageContinuation)
        (state: CompiledLogicalQuery)
        (s: LinqStage list)
        (ctx: GroupFoldContext)
        (letBindings: Map<string, Expression>)
        (scopeMap: ScopeMap)
        : CompiledLogicalQuery =
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
            slurp env continueWith state next { ctx with HavingPreds = ctx.HavingPreds @ [ newPred ] } letBindings scopeMap
        | LinqStage.Sort(sortLambda, isDesc, nullsLast) :: next ->
            let newBody = substituteLet sortLambda.Body
            let newSort = Expression.Lambda(newBody, sortLambda.Parameters)
            slurp env continueWith state next { ctx with SortStages = ctx.SortStages @ [ (newSort, isDesc, nullsLast) ] } letBindings scopeMap
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
                slurp env continueWith state next ctx updatedBindings scopeMap
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
                    continueWith { state with Plan = nextPlan } Map.empty next
                | None ->
                    continueWith (fallback state) scopeMap next
        | [] ->
            if env.IsGrouping then
                // The rows are the grouped elements, materialized as the key selector's input type; the
                // residual having predicates run over the groups.
                let grouped = { state with RowScopes = scopeMap }
                ctx.HavingPreds |> List.fold (fun acc havingPred -> clientFilter Map.empty havingPred acc) grouped
            else
                (fallback state)
        | otherHead :: otherTail ->
            continueWith (fallback state) scopeMap (otherHead :: otherTail)

    /// CountBy: a group-by with a row count per key.
    let countBy (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (lam: LambdaExpression) : CompiledLogicalQuery =
        let makeCtx = StageContext.makeCtx scopeMap
        let p = lam.Parameters.[0]
        let ctx = makeCtx p.Type p.Name state.Plan
        match ExprTranslator.tryTranslateToScalar ctx lam.Body with
        | Some keyExpr ->
            let groupPlan = LogicalPlan.GroupBy(state.Plan, [ keyExpr ], [ ("Value", ScalarExpr.Aggregation(AggKind.Len, None, false)) ], None, true)
            let keyCol = match keyExpr with ScalarExpr.Column c -> c | _ -> "Key"
            let projs = [ ("Key", ScalarExpr.Column keyCol); ("Value", ScalarExpr.Column "Value") ]
            continueWith { state with Plan = LogicalPlan.Project(groupPlan, projs) } Map.empty tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// GroupBy(key): the start of a grouped pipeline.
    let groupByKey (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (keyLambda: LambdaExpression) : CompiledLogicalQuery =
        if env.IsGrouping then
            continueWith state scopeMap tail
        else
            slurp env continueWith state tail { KeyLambda = keyLambda; ElemLambdaOpt = None; HavingPreds = []; SortStages = [] } Map.empty scopeMap

    /// GroupBy(key, resultSelector).
    let groupByWithResult (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (keyLam: LambdaExpression) (resLam: LambdaExpression) : CompiledLogicalQuery =
        let makeCtx = StageContext.makeCtx scopeMap
        if resLam.Parameters.Count < 2 then
            if env.IsGrouping then
                continueWith state scopeMap tail
            else
                slurp env continueWith state tail { KeyLambda = keyLam; ElemLambdaOpt = Some resLam; HavingPreds = []; SortStages = [] } Map.empty scopeMap
        else
            let keyP = keyLam.Parameters.[0]
            let keyCtx = makeCtx keyP.Type keyP.Name state.Plan
            let keyExprs = tryExtractKeyScalars keyCtx keyLam |> Option.defaultValue [ ScalarExpr.Column "Key" ]
            match tryExtractGroupByResult state.Plan keyExprs resLam with
            | Some plan -> continueWith { state with Plan = plan } Map.empty tail
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
                    continueWith { state with Plan = nextPlan } Map.empty tail
                | None ->
                    continueWith (fallback state) scopeMap tail

    /// GroupBy(key, elementSelector, resultSelector).
    let groupByWithElementAndResult (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (keyLam: LambdaExpression) (elemLam: LambdaExpression) (resLam: LambdaExpression) : CompiledLogicalQuery =
        let makeCtx = StageContext.makeCtx scopeMap
        let keyP = keyLam.Parameters.[0]
        let keyCtx = makeCtx keyP.Type keyP.Name state.Plan
        let keyExprs = tryExtractKeyScalars keyCtx keyLam |> Option.defaultValue [ ScalarExpr.Column "Key" ]
        match tryExtractGroupByResult state.Plan keyExprs resLam with
        | Some plan -> continueWith { state with Plan = plan } Map.empty tail
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
                continueWith { state with Plan = nextPlan } Map.empty tail
            | None ->
                continueWith (fallback state) scopeMap tail

    /// GroupBy ... Rollup.
    let rollup (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (keyLambda: LambdaExpression) (resLambda: LambdaExpression) : CompiledLogicalQuery =
        match buildUnifiedGroupingSetsPlan state.Plan GroupingKind.Rollup keyLambda resLambda scopeMap with
        | Some nextPlan ->
            continueWith { state with Plan = nextPlan } Map.empty tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// GroupBy ... Cube.
    let cube (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (keyLambda: LambdaExpression) (resLambda: LambdaExpression) : CompiledLogicalQuery =
        match buildUnifiedGroupingSetsPlan state.Plan GroupingKind.Cube keyLambda resLambda scopeMap with
        | Some nextPlan ->
            continueWith { state with Plan = nextPlan } Map.empty tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// GroupBy ... GroupingSets.
    let groupingSets (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (keyLambda: LambdaExpression) (setsExpr: Expression) (resLambda: LambdaExpression) : CompiledLogicalQuery =
        match buildUnifiedGroupingSetsPlan state.Plan (GroupingKind.GroupingSets setsExpr) keyLambda resLambda scopeMap with
        | Some nextPlan ->
            continueWith { state with Plan = nextPlan } Map.empty tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// A per-group slice (Take / Skip / ordering inside each group).
    let partitionSlice (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (keySelector: LambdaExpression) (innerStages: LinqStage list) (resSelOpt: LambdaExpression option) : CompiledLogicalQuery =
        match buildGroupPartitionSlicePlan state.Plan keySelector innerStages resSelOpt with
        | Some nextPlan ->
            continueWith { state with Plan = nextPlan } Map.empty tail
        | None ->
            continueWith (fallback state) scopeMap tail
