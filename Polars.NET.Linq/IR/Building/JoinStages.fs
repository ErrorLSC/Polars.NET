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
                        | None -> continueWith { state with Plan = decorPlan } rowScopeMap tail
                    | None ->
                        match tryExtractJoinProjections resCtx joinPlan pLeft pRight leftCols rightCols uniqueSuffix isRightJoin resSel.Body with
                        | Some projs -> continueWith { state with Plan = LogicalPlan.Project(joinPlan, projs) } Map.empty tail
                        | None -> continueWith { state with Plan = joinPlan } rowScopeMap tail
            | _ ->
                continueWith (fallback state) scopeMap tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// GroupJoin.
    let groupJoin (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (m: MethodInfo) (innerExpr: Expression) (outerKey: LambdaExpression) (innerKey: LambdaExpression) (resSelOpt: LambdaExpression option) : CompiledLogicalQuery =
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
                | None -> continueWith { state with Plan = joinPlan } rowScopeMap tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// JoinAsOf.
    let joinAsOf (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (inner: Expression) (outerKey: LambdaExpression) (innerKey: LambdaExpression) (outerByOpt: LambdaExpression option) (innerByOpt: LambdaExpression option) (resSelOpt: LambdaExpression option) (toleranceStr: string option) (toleranceInt: int64 option) (toleranceFloat: double option) (strategy: AsofStrategy) : CompiledLogicalQuery =
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
                    | None -> continueWith { state with Plan = asOfPlan } rowScopeMap tail
            | _ ->
                continueWith (fallback state) scopeMap tail
        | None ->
            continueWith (fallback state) scopeMap tail
