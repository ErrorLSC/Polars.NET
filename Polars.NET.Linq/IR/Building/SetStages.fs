namespace Polars.NET.Linq.IR

open System
open System.Reflection
open System.Linq.Expressions
open Polars.NET.Linq
open Polars.NET.Linq.IR.TransparentScopes
open Polars.NET.Linq.IR.ProjectionExtraction
open Polars.NET.Linq.IR.GroupJoinAndZipPlanning
open Polars.NET.Linq.IR.StageContext

/// Set and sequence-combining stages: set operations, concatenation, zips, unpivot and explode.
/// Each handler compiles one stage onto the query state and continues with the remaining stages.
module internal SetStages =

    /// Intersect / Except (and their By forms).
    let setOp (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (methodInfo: MethodInfo) (secondExpr: Expression) (key1Opt: LambdaExpression option) (key2Opt: LambdaExpression option) : CompiledLogicalQuery =
        match env.ResolveSequence secondExpr with
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

            // LINQ set operations return distinct elements: Intersect/Except by the whole element, the By forms
            // by key, keeping the first element of each key in source order.
            let dedupSubset (leftOn: ScalarExpr list) : string list option option =
                match key1Opt with
                | None -> Some None
                | Some _ ->
                    let cols = leftOn |> List.choose (function ScalarExpr.Column c -> Some c | _ -> None)
                    if cols.Length = leftOn.Length then Some(Some cols) else None

            match keysOpt with
            | Some(leftOn, rightOn) ->
                match dedupSubset leftOn with
                | Some subset ->
                    let joined = LogicalPlan.Join(state.Plan, rightPlan, leftOn, rightOn, how, None, JoinKeyCoalescing.Default, true)
                    continueWith { state with Plan = LogicalPlan.Distinct(joined, subset, DistinctKeep.First, true) } scopeMap tail
                | None ->
                    continueWith (fallback state) scopeMap tail
            | None ->
                continueWith (fallback state) scopeMap tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// Union (and UnionBy).
    let union (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (secondExpr: Expression) (keyLambdaOpt: LambdaExpression option) : CompiledLogicalQuery =
        match env.ResolveSequence secondExpr with
        | Some otherPlan ->
            // UnionBy dedups by key; a key that is not plain columns is not translated (rather than
            // silently deduplicating whole rows).
            let subsetOpt =
                match keyLambdaOpt with
                | None -> Some None
                | Some keyLambda -> PipelineStageParser.extractDistinctColumns keyLambda |> Option.map (Array.toList >> Some)
            match subsetOpt with
            | Some subset ->
                let unionPlan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true)
                continueWith { state with Plan = LogicalPlan.Distinct(unionPlan, subset, DistinctKeep.First, true) } scopeMap tail
            | None ->
                continueWith (fallback state) scopeMap tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// Concat.
    let concat (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (secondExpr: Expression) : CompiledLogicalQuery =
        match env.ResolveSequence secondExpr with
        | Some otherPlan ->
            continueWith { state with Plan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true) } scopeMap tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// Append.
    let append (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (elemExpr: Expression) : CompiledLogicalQuery =
        match PartialEvaluator.tryEvaluate elemExpr with
        | Some elem when not (isNull elem) ->
            match SourceResolver.singleRowPlan env.Sources elem (elem.GetType()) with
            | Some otherPlan ->
                continueWith { state with Plan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true) } scopeMap tail
            | None ->
                continueWith (fallback state) scopeMap tail
        | _ ->
            continueWith (fallback state) scopeMap tail

    /// Prepend.
    let prepend (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (elemExpr: Expression) : CompiledLogicalQuery =
        match PartialEvaluator.tryEvaluate elemExpr with
        | Some elem when not (isNull elem) ->
            match SourceResolver.singleRowPlan env.Sources elem (elem.GetType()) with
            | Some otherPlan ->
                continueWith { state with Plan = LogicalPlan.Union(otherPlan, state.Plan, isAll = true) } scopeMap tail
            | None ->
                continueWith (fallback state) scopeMap tail
        | _ ->
            continueWith (fallback state) scopeMap tail

    /// Zip of two sequences.
    let zip (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (secondExpr: Expression) (resSelOpt: LambdaExpression option) : CompiledLogicalQuery =
        match buildZipPlan env.ResolveSequence state.Plan secondExpr resSelOpt with
        | Some nextPlan ->
            continueWith { state with Plan = nextPlan } Map.empty tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// Zip of three sequences.
    let zip3 (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (secondExpr: Expression) (thirdExpr: Expression) : CompiledLogicalQuery =
        match buildZip3Plan env.ResolveSequence state.Plan secondExpr thirdExpr with
        | Some nextPlan ->
            continueWith { state with Plan = nextPlan } Map.empty tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// Unpivot (a SelectMany over member values).
    let unpivot (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (colLam: LambdaExpression) (resLambdaOpt: LambdaExpression option) : CompiledLogicalQuery =
        let availableCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
        match tryExtractUnpivotPattern colLam resLambdaOpt with
        | Some pattern ->
            let explicitIndexCols = pattern.ExplicitIndexCols
            let onCols = pattern.OnCols
            let indexCols =
                if not explicitIndexCols.IsEmpty then explicitIndexCols
                else availableCols |> List.filter (fun c -> not (Array.contains c onCols))

            if indexCols.IsEmpty then
                continueWith (fallback state) scopeMap tail
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
                    continueWith { state with Plan = unpivotPlan } updatedScopeMap tail
                | ConcreteResSelector resSel ->
                    let resCtx = TranslationContext.ScopedEntity([ leftScope; innerScope ], updatedScopeMap, postUnpivotCols)
                    match extractProjections resCtx unpivotPlan resSel.Body with
                    | Some projs ->
                        continueWith { state with Plan = LogicalPlan.Project(unpivotPlan, projs) } Map.empty tail
                    | None ->
                        continueWith (clientMap Map.empty resSel { state with Plan = unpivotPlan }) Map.empty tail
        | None ->
            continueWith (fallback state) scopeMap tail

    /// Explode (a SelectMany over a collection member).
    let explode (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (scopeMap: ScopeMap) (tail: LinqStage list) (colLam: LambdaExpression) (resSelOpt: LambdaExpression option) : CompiledLogicalQuery =
        let makeCtx = StageContext.makeCtx scopeMap
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
            continueWith { state with Plan = finalPlan } scopeMap tail
        | None ->
            continueWith (fallback state) scopeMap tail
