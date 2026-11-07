namespace Polars.NET.Linq.IR

open System
open System.Reflection
open System.Linq.Expressions
open Polars.NET.Linq
open Polars.NET.Linq.IR.TransparentScopes
open Polars.NET.Linq.IR.ProjectionExtraction
open Polars.NET.Linq.IR.GroupJoinAndZipPlanning
open Polars.NET.Linq.IR.StageContext

/// Join stages: equi joins, group joins, cross joins and as-of joins.
/// Each handler compiles one stage onto the query state and continues with the remaining stages.
module internal JoinStages =

    /// True when every member of a result selector is a path over its parameters (a parameter, a member chain,
    /// or a nested tuple / anonymous row of such paths). Downstream stages resolve such a row through its path
    /// scopes even when it cannot be projected as columns here (F# AnonymousObject rows, for instance).
    let private isPathOnlySelector (resSel: LambdaExpression) =
        let names = resSel.Parameters |> Seq.map (fun p -> p.Name) |> Set.ofSeq
        let rec isPath (e: Expression) =
            match ExpressionPatterns.unwrap e with
            | :? ParameterExpression as p -> names.Contains p.Name
            | MemberAccess(target, _) when not (isNull target) -> isPath target
            | :? NewExpression as ne when ScopeNavigation.isTupleOrAnonType ne.Type -> ne.Arguments |> Seq.forall isPath
            | _ -> false
        match tryExtractMemberBindings resSel.Body with
        | Some bindings -> bindings |> List.forall (snd >> isPath)
        | None -> isPath resSel.Body

    /// True when no later stage consumes the joined row into a new shape, so the result selector's own type is
    /// what gets materialized: its members must then exist as columns or be computed by a client map.
    let private rowIsMaterialized (tail: LinqStage list) =
        tail
        |> List.forall (function
            | LinqStage.Project _
            | LinqStage.Join _
            | LinqStage.JoinAsOf _
            | LinqStage.GroupJoin _
            | LinqStage.CrossJoin _
            | LinqStage.Explode _
            | LinqStage.Unpivot _
            | LinqStage.GroupByKey _
            | LinqStage.GroupPartitionSlice _
            | LinqStage.GroupByWithResult _
            | LinqStage.GroupByWithElementAndResult _
            | LinqStage.GroupByRollup _
            | LinqStage.GroupByCube _
            | LinqStage.GroupByGroupingSets _
            | LinqStage.CountBy _
            | LinqStage.AggregateBy _
            | LinqStage.Zip _
            | LinqStage.Zip3 _ -> false
            | _ -> true)

    /// True when the result selector can be left to the row scopes: its members are all paths, or a later
    /// stage reshapes the row (resolving computed members through the scopes) before anything is materialized.
    let private keepsRowScopes (resSel: LambdaExpression) (tail: LinqStage list) =
        isPathOnlySelector resSel || not (rowIsMaterialized tail)

    /// Splits a join result selector `(o, i) => body` that cannot be projected natively into a transparent
    /// `(o, i) => (o, i)` join followed by `t => body[o := t.Item1, i := t.Item2]`. The projection stage then
    /// runs it natively where it can and as a client map otherwise, instead of the selector being dropped.
    let private splitResultSelector (resSel: LambdaExpression) : LambdaExpression * LambdaExpression =
        let o, i = resSel.Parameters.[0], resSel.Parameters.[1]
        let tupleType = typedefof<ValueTuple<_, _>>.MakeGenericType(o.Type, i.Type)
        let tupleSel = Expression.Lambda(Expression.New(tupleType.GetConstructor([| o.Type; i.Type |]), o, i), [| o; i |])
        let row = Expression.Parameter(tupleType, "__joinRow")
        let item1 = Expression.Field(row, "Item1") :> Expression
        let item2 = Expression.Field(row, "Item2") :> Expression
        let body =
            { new ExpressionVisitor() with
                override _.VisitParameter(p: ParameterExpression) =
                    if obj.ReferenceEquals(p, o) then item1
                    elif obj.ReferenceEquals(p, i) then item2
                    else p :> Expression
            }.Visit(resSel.Body)
        tupleSel, Expression.Lambda(body, [| row |])

    /// Join (inner, left, right, full) on key selectors.
    let join (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (methodInfo: MethodInfo) (innerExpr: Expression) (outerKey: LambdaExpression) (innerKey: LambdaExpression) (resSelOpt: LambdaExpression option) : CompiledLogicalQuery =
        match env.ResolveSequence innerExpr with
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
                    continueWith { state with Plan = joinPlan } rowScopeMap tail
                | ConcreteResSelector resSel ->
                    let postJoinCols = (SchemaInference.inferPlanSchema joinPlan).ColumnNames
                    let pLeft = resSel.Parameters.[0]
                    let pRight = resSel.Parameters.[1]
                    let resCtx =
                        if not updatedScopeMap.IsEmpty then
                            TranslationContext.ScopedEntity([ pLeft.Name; pRight.Name ], updatedScopeMap, postJoinCols)
                        else
                            TranslationContext.BinaryJoin(pLeft.Name, pRight.Name, leftColSet, uniqueSuffix)
                    let splitSelector (plan: LogicalPlan) =
                        if keepsRowScopes resSel tail then
                            continueWith { state with Plan = plan } rowScopeMap tail
                        else
                            let tupleSel, proj = splitResultSelector resSel
                            continueWith state scopeMap (LinqStage.Join(methodInfo, innerExpr, outerKey, innerKey, Some tupleSel) :: LinqStage.Project proj :: tail)

                    match SubqueryDecorrelator.tryDecorrelateProjection env.ResolveSequence joinPlan resSel updatedScopeMap with
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
                        | Some projs -> continueWith { state with Plan = LogicalPlan.Project(decorPlan, projs) } Map.empty tail
                        | None -> splitSelector decorPlan
                    | None ->
                        match tryExtractJoinProjections resCtx joinPlan pLeft pRight leftCols rightCols uniqueSuffix isRightJoin resSel.Body with
                        | Some projs -> continueWith { state with Plan = LogicalPlan.Project(joinPlan, projs) } Map.empty tail
                        | None -> splitSelector joinPlan
            | _ ->
                continueWith (fallback state) scopeMap tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// The operators applied to a group join's group before DefaultIfEmpty, in
    /// `g.Where(p1)...Where(pn).Select(s).DefaultIfEmpty(d)`. All parts are optional except DefaultIfEmpty.
    type private GroupSequenceOps =
        {
            Filters: LambdaExpression list
            Selector: LambdaExpression option
            DefaultValue: Expression option
        }

    /// True when a lambda reads only its own parameters (and constants), so it can run on the inner side alone.
    let private isSelfContained (lam: LambdaExpression) =
        let declared = System.Collections.Generic.HashSet<ParameterExpression>(lam.Parameters)
        let used = System.Collections.Generic.HashSet<ParameterExpression>()
        { new ExpressionVisitor() with
            override _.VisitLambda<'T>(node: Expression<'T>) =
                // Parameters of nested lambdas are their own.
                for p in node.Parameters do declared.Add p |> ignore
                base.VisitLambda node
            override _.VisitParameter(p: ParameterExpression) =
                used.Add p |> ignore
                p :> Expression
        }.Visit(lam.Body) |> ignore
        used |> Seq.forall declared.Contains

    /// Decomposes `g.Where(..)*.Select(..)?.DefaultIfEmpty(d?)` over the group reference. Returns None for any other
    /// shape, or when a filter or selector reads the outer row (a correlated filter is not a pre-join filter).
    let private tryDecomposeGroupSequence (body: Expression) : GroupSequenceOps option =
        let rec walk (e: Expression) (filters: LambdaExpression list) (selector: LambdaExpression option) =
            match ExpressionPatterns.unwrap e with
            | MethodCall(m, null, [ src; StripQuotes(:? LambdaExpression as lam) ]) when
                m.Name = "Select" && lam.Parameters.Count = 1 && selector.IsNone && filters.IsEmpty && isSelfContained lam ->
                walk src filters (Some lam)
            | MethodCall(m, null, [ src; StripQuotes(:? LambdaExpression as lam) ]) when
                m.Name = "Where" && lam.Parameters.Count = 1 && isSelfContained lam ->
                walk src (lam :: filters) selector
            | MemberAccess _
            | :? ParameterExpression -> Some(filters, selector)
            | _ -> None

        match ExpressionPatterns.unwrap body with
        | MethodCall(m, null, src :: rest) when m.Name = "DefaultIfEmpty" && rest.Length <= 1 ->
            walk src [] None
            |> Option.map (fun (filters, selector) ->
                { Filters = filters; Selector = selector; DefaultValue = List.tryHead rest })
        | _ -> None

    /// Applies group filters to the inner sequence of a group join, so they run before the join.
    let private filterInner (innerExpr: Expression) (innerType: Type) (filters: LambdaExpression list) : Expression =
        let queryableType = typedefof<System.Linq.IQueryable<_>>.MakeGenericType innerType
        filters
        |> List.fold (fun (acc: Expression) pred ->
            if queryableType.IsAssignableFrom acc.Type then
                let where = (typeof<System.Linq.Queryable>.GetMethods() |> Array.find (fun mi -> mi.Name = "Where" && mi.GetParameters().[1].ParameterType.GetGenericArguments().[0].GetGenericArguments().Length = 2)).MakeGenericMethod innerType
                Expression.Call(where, acc, Expression.Quote pred) :> Expression
            else
                let where = (typeof<System.Linq.Enumerable>.GetMethods() |> Array.find (fun mi -> mi.Name = "Where" && mi.GetParameters().[1].ParameterType.GetGenericArguments().Length = 2)).MakeGenericMethod innerType
                Expression.Call(where, acc, pred) :> Expression) innerExpr

    /// Rewrites the flattening selector `(row, x) => body` into `(row, r) => body[x := r == null ? d : s(r)]`, where
    /// r is the (possibly null-extended) inner element: LINQ yields DefaultIfEmpty's value for a group with no match.
    let private tryInlineGroupSelector (resSel: LambdaExpression) (innerType: Type) (ops: GroupSequenceOps) : LambdaExpression option =
        let xParam = resSel.Parameters.[1]
        match ops.Selector, ops.DefaultValue with
        | None, None -> Some resSel
        | _ when innerType.IsValueType -> None
        | _ ->
            let rParam = Expression.Parameter(innerType, xParam.Name)
            let valueExpr =
                match ops.Selector with
                | Some sel ->
                    { new ExpressionVisitor() with
                        override _.VisitParameter(p: ParameterExpression) = if obj.ReferenceEquals(p, sel.Parameters.[0]) then rParam :> Expression else p :> Expression
                    }.Visit(sel.Body)
                | None -> rParam :> Expression
            let defaultExpr =
                match ops.DefaultValue with
                | Some d when d.Type = valueExpr.Type -> d
                | Some d -> Expression.Convert(d, valueExpr.Type) :> Expression
                | None ->
                    let t = valueExpr.Type
                    Expression.Constant((if t.IsValueType then Activator.CreateInstance t else null), t) :> Expression
            let elementExpr =
                Expression.Condition(Expression.Equal(rParam, Expression.Constant(null, innerType)), defaultExpr, valueExpr) :> Expression
            let newBody =
                { new ExpressionVisitor() with
                    override _.VisitParameter(p: ParameterExpression) = if obj.ReferenceEquals(p, xParam) then elementExpr else p :> Expression
                }.Visit(resSel.Body)
            Some(Expression.Lambda(newBody, [| resSel.Parameters.[0]; rParam |]))

    /// GroupJoin.
    let groupJoin (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (m: MethodInfo) (innerExpr: Expression) (outerKey: LambdaExpression) (innerKey: LambdaExpression) (resSelOpt: LambdaExpression option) : CompiledLogicalQuery =
        match tail with
        | LinqStage.Explode(collSelector, explodeResSelOpt) :: nextTail when (tryExtractDefaultIfEmpty collSelector.Body).IsSome ->
            let innerType = innerKey.Parameters.[0].Type
            // The group's Where filters run on the inner side before the join, and its Select (and the
            // DefaultIfEmpty value) is inlined into the flattening selector. Any other shape is not translated.
            let rewritten =
                match tryDecomposeGroupSequence collSelector.Body with
                | Some ops when ops.Filters.IsEmpty && ops.Selector.IsNone && ops.DefaultValue.IsNone ->
                    Some(innerExpr, explodeResSelOpt)
                | Some ops ->
                    match explodeResSelOpt with
                    | Some resSel ->
                        tryInlineGroupSelector resSel innerType ops
                        |> Option.map (fun newSel -> filterInner innerExpr innerType ops.Filters, Some newSel)
                    | None -> None
                | None -> None
            match rewritten with
            | None -> continueWith (fallback state) scopeMap tail
            | Some(innerExpr, explodeResSelOpt) ->
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
                continueWith state groupRowScopeMap (LinqStage.Join(m, innerExpr, outerKey, innerKey, effResSelOpt) :: nextTail)
        | LinqStage.Project projLambda :: nextTail when resSelOpt.IsNone ->
            let outerType, innerType = outerKey.Parameters.[0].Type, innerKey.Parameters.[0].Type
            let desugaredResSel = PipelineStageParser.desugarGroupJoinProject projLambda outerType innerType
            continueWith state scopeMap (LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, Some desugaredResSel) :: nextTail)
        | _ ->
            match resSelOpt with
            | Some resSel ->
                match buildGroupJoinWithResultPlan env.ResolveSequence state.Plan innerExpr outerKey innerKey resSel with
                | Some nextPlan ->
                    continueWith { state with Plan = nextPlan } Map.empty tail
                | None ->
                    continueWith (fallback state) scopeMap tail
            | None ->
                continueWith state scopeMap (LinqStage.Join(m, innerExpr, outerKey, innerKey, None) :: tail)

    /// A cross join (SelectMany over an uncorrelated sequence).
    let crossJoin (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (innerExpr: Expression) (resSelOpt: LambdaExpression option) : CompiledLogicalQuery =
        match env.ResolveSequence innerExpr with
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
                continueWith { state with Plan = joinPlan } rowScopeMap tail
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
                | Some projs -> continueWith { state with Plan = LogicalPlan.Project(joinPlan, projs) } Map.empty tail
                | None when keepsRowScopes resSel tail -> continueWith { state with Plan = joinPlan } rowScopeMap tail
                | None ->
                    let tupleSel, proj = splitResultSelector resSel
                    continueWith state scopeMap (LinqStage.CrossJoin(innerExpr, Some tupleSel) :: LinqStage.Project proj :: tail)
        | None ->
            continueWith (fallback state) scopeMap tail

    /// JoinAsOf.
    let rec joinAsOf (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (inner: Expression) (outerKey: LambdaExpression) (innerKey: LambdaExpression) (outerByOpt: LambdaExpression option) (innerByOpt: LambdaExpression option) (resSelOpt: LambdaExpression option) (toleranceStr: string option) (toleranceInt: int64 option) (toleranceFloat: double option) (strategy: AsofStrategy) : CompiledLogicalQuery =
        match env.ResolveSequence inner with
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
                    continueWith { state with Plan = asOfPlan } rowScopeMap tail
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
                    | Some projs -> continueWith { state with Plan = LogicalPlan.Project(asOfPlan, projs) } Map.empty tail
                    | None when keepsRowScopes resSel tail -> continueWith { state with Plan = asOfPlan } rowScopeMap tail
                    | None ->
                        let tupleSel, proj = splitResultSelector resSel
                        joinAsOf env continueWith state scopeMap (LinqStage.Project proj :: tail) inner outerKey innerKey outerByOpt innerByOpt (Some tupleSel) toleranceStr toleranceInt toleranceFloat strategy
            | _ ->
                continueWith (fallback state) scopeMap tail
        | None ->
            continueWith (fallback state) scopeMap tail
