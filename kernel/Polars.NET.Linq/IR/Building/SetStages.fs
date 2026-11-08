namespace Polars.NET.Linq.IR

open System
open System.Reflection
open Polars.NET.Core.Helpers
open System.Linq.Expressions
open Polars.NET.Linq
open Polars.NET.Linq.IR.TransparentScopes
open Polars.NET.Linq.IR.ProjectionExtraction
open Polars.NET.Linq.IR.GroupJoinAndZipPlanning
open Polars.NET.Linq.IR.StageContext

/// Set and sequence-combining stages: set operations, concatenation, zips, unpivot and explode.
/// Each handler compiles one stage onto the query state and continues with the remaining stages.
module internal SetStages =

    /// Gives two sides of a set operation the same columns when their element is an object whose members the
    /// sides assign differently (`new P { A = .. }` and `new P { B = .. }`): each side gets every member of the
    /// element, an unassigned one holding its type's default, as C# object initialization leaves it. Without this
    /// the sides would be matched by position. Scalar elements, and sides with the same columns, are left as they
    /// are. None when a default cannot be expressed.
    let private alignMembers (elementType: Type) (left: LogicalPlan) (right: LogicalPlan) : (LogicalPlan * LogicalPlan) option =
        let leftCols = (SchemaInference.inferPlanSchema left).ColumnNames
        let rightCols = (SchemaInference.inferPlanSchema right).ColumnNames
        if Set.ofList leftCols = Set.ofList rightCols || isNull elementType || PolarsTypeHelper.IsSupportedSimpleType elementType then
            Some(left, right)
        else
            let members = PolarsTypeHelper.GetModelProperties elementType |> Array.toList
            let names = members |> List.map (fun (p: PropertyInfo) -> p.Name) |> Set.ofList
            if not (List.forall names.Contains leftCols && List.forall names.Contains rightCols) then
                Some(left, right)
            else
                let defaultOf (t: Type) : ScalarExpr option =
                    let logical = LogicalTypes.tryOfClrType t
                    if not t.IsValueType || not (isNull (Nullable.GetUnderlyingType t)) then
                        logical |> Option.map (fun lt -> ScalarExpr.Literal(LiteralValue.Null lt))
                    else
                        match LiteralValue.TryOfObject(Activator.CreateInstance t), logical with
                        | Some lit, Some lt -> Some(ScalarExpr.Cast(ScalarExpr.Literal lit, lt, false))
                        | _ -> None
                let complete (plan: LogicalPlan) (cols: string list) =
                    let present = Set.ofList cols
                    let projections =
                        members
                        |> List.map (fun (p: PropertyInfo) ->
                            if present.Contains p.Name then Some(p.Name, ScalarExpr.Column p.Name)
                            else defaultOf p.PropertyType |> Option.map (fun d -> p.Name, d))
                    if projections |> List.forall Option.isSome then
                        Some(LogicalPlan.Project(plan, projections |> List.map Option.get))
                    else
                        None
                match complete left leftCols, complete right rightCols with
                | Some l, Some r -> Some(l, r)
                | _ -> None

    let private elementTypeOf (sequence: Expression) : Type =
        PolarsTypeHelper.TryGetEnumerableElementType sequence.Type

    /// Intersect / Except (and their By forms).
    let setOp (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (methodInfo: MethodInfo) (secondExpr: Expression) (key1Opt: LambdaExpression option) (key2Opt: LambdaExpression option) : CompiledLogicalQuery =
        match
            env.ResolveSequence secondExpr
            |> Option.map (fun rightPlan ->
                match key1Opt with
                | None -> alignMembers (elementTypeOf secondExpr) state.Plan rightPlan
                | Some _ -> Some(state.Plan, rightPlan))
        with
        | Some None -> continueWith (fallback state) rowShape tail
        | None -> continueWith (fallback state) rowShape tail
        | Some(Some(leftPlan, rightPlan)) ->
            let state = { state with Plan = leftPlan }
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
                    let ctx1 = PipelineStageParser.createContextWithScopeAndCols rowShape p1.Type leftCols p1.Name
                    let ctx2 = PipelineStageParser.createContextWithScopeAndCols RowShape.Empty p2.Type rightCols p2.Name
                    match ExprTranslator.tryTranslateToScalar ctx1 k1.Body, ExprTranslator.tryTranslateToScalar ctx2 k2.Body with
                    | Some lk, Some rk -> Some([ lk ], [ rk ])
                    | _ -> None
                | Some k1, None ->
                    let p1 = k1.Parameters.[0]
                    let ctx1 = PipelineStageParser.createContextWithScopeAndCols rowShape p1.Type leftCols p1.Name
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
                    continueWith { state with Plan = LogicalPlan.Distinct(joined, subset, DistinctKeep.First, true) } rowShape tail
                | None ->
                    continueWith (fallback state) rowShape tail
            | None ->
                continueWith (fallback state) rowShape tail

    /// Union (and UnionBy).
    let union (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (secondExpr: Expression) (keyLambdaOpt: LambdaExpression option) : CompiledLogicalQuery =
        match env.ResolveSequence secondExpr with
        | Some otherPlan ->
            // UnionBy dedups by key; a key that is not plain columns is not translated (rather than
            // silently deduplicating whole rows).
            let subsetOpt =
                match keyLambdaOpt with
                | None -> Some None
                | Some keyLambda -> PipelineStageParser.extractDistinctColumns keyLambda |> Option.map (Array.toList >> Some)
            match subsetOpt, alignMembers (elementTypeOf secondExpr) state.Plan otherPlan with
            | Some subset, Some(leftPlan, otherPlan) ->
                let unionPlan = LogicalPlan.Union(leftPlan, otherPlan, isAll = true)
                continueWith { state with Plan = LogicalPlan.Distinct(unionPlan, subset, DistinctKeep.First, true) } rowShape tail
            | _ ->
                continueWith (fallback state) rowShape tail
        | None ->
            continueWith (fallback state) rowShape tail

    /// Concat.
    let concat (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (secondExpr: Expression) : CompiledLogicalQuery =
        match env.ResolveSequence secondExpr |> Option.bind (alignMembers (elementTypeOf secondExpr) state.Plan) with
        | Some(leftPlan, otherPlan) ->
            continueWith { state with Plan = LogicalPlan.Union(leftPlan, otherPlan, isAll = true) } rowShape tail
        | None ->
            continueWith (fallback state) rowShape tail

    /// Append.
    let append (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (elemExpr: Expression) : CompiledLogicalQuery =
        match PartialEvaluator.tryEvaluate elemExpr with
        | Some elem when not (isNull elem) ->
            match SourceResolver.singleRowPlan env.Sources elem (elem.GetType()) with
            | Some otherPlan ->
                continueWith { state with Plan = LogicalPlan.Union(state.Plan, otherPlan, isAll = true) } rowShape tail
            | None ->
                continueWith (fallback state) rowShape tail
        | _ ->
            continueWith (fallback state) rowShape tail

    /// Prepend.
    let prepend (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (elemExpr: Expression) : CompiledLogicalQuery =
        match PartialEvaluator.tryEvaluate elemExpr with
        | Some elem when not (isNull elem) ->
            match SourceResolver.singleRowPlan env.Sources elem (elem.GetType()) with
            | Some otherPlan ->
                continueWith { state with Plan = LogicalPlan.Union(otherPlan, state.Plan, isAll = true) } rowShape tail
            | None ->
                continueWith (fallback state) rowShape tail
        | _ ->
            continueWith (fallback state) rowShape tail

    /// Zip of two sequences.
    let zip (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (secondExpr: Expression) (resSelOpt: LambdaExpression option) : CompiledLogicalQuery =
        match buildZipPlan env.ResolveSequence state.Plan secondExpr resSelOpt with
        | Some nextPlan ->
            continueWith { state with Plan = nextPlan } RowShape.Empty tail
        | None ->
            continueWith (fallback state) rowShape tail

    /// Zip of three sequences.
    let zip3 (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (secondExpr: Expression) (thirdExpr: Expression) : CompiledLogicalQuery =
        match buildZip3Plan env.ResolveSequence state.Plan secondExpr thirdExpr with
        | Some nextPlan ->
            continueWith { state with Plan = nextPlan } RowShape.Empty tail
        | None ->
            continueWith (fallback state) rowShape tail

    /// Unpivot (a SelectMany over member values).
    let unpivot (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (colLam: LambdaExpression) (resLambdaOpt: LambdaExpression option) : CompiledLogicalQuery =
        let availableCols = (SchemaInference.inferPlanSchema state.Plan).ColumnNames
        match tryExtractUnpivotPattern colLam resLambdaOpt with
        | Some pattern ->
            let explicitIndexCols = pattern.ExplicitIndexCols
            let onCols = pattern.OnCols
            let indexCols =
                if not explicitIndexCols.IsEmpty then explicitIndexCols
                else availableCols |> List.filter (fun c -> not (Array.contains c onCols))

            if indexCols.IsEmpty then
                continueWith (fallback state) rowShape tail
            else
                let unpivotPlan = LogicalPlan.Unpivot(state.Plan, indexCols, Some (Array.toList onCols), pattern.VariableName, pattern.ValueName)
                let postUnpivotCols = indexCols @ [ pattern.VariableName; pattern.ValueName ]
                let leftScope = colLam.Parameters.[0].Name
                let innerScope =
                    match resLambdaOpt with
                    | Some resSel when resSel.Parameters.Count >= 2 -> resSel.Parameters.[1].Name
                    | _ -> "q"
                let updatedRowShape =
                    let outer, inner = RowScope.Named leftScope, RowScope.Named innerScope
                    let outerMappings = indexCols |> List.map (fun c -> (outer, c), c)
                    let innerMappings =
                        [
                            (inner, pattern.VariableName), pattern.VariableName
                            (inner, pattern.ValueName), pattern.ValueName
                            (inner, pattern.VarPropName), pattern.VariableName
                            (inner, pattern.ValPropName), pattern.ValueName
                            (inner, "Item1"), pattern.VariableName
                            (inner, "Item2"), pattern.ValueName
                        ]
                    // The current shape's entries win over the unpivot's.
                    RowShape.OfEntries(outerMappings @ innerMappings).Union rowShape

                match resLambdaOpt with
                | TransparentOrNoResSelector ->
                    continueWith { state with Plan = unpivotPlan } updatedRowShape tail
                | ConcreteResSelector resSel ->
                    let resCtx = TranslationContext.ScopedEntity([ leftScope; innerScope ], updatedRowShape, postUnpivotCols)
                    match extractProjections resCtx unpivotPlan resSel.Body with
                    | Some projs ->
                        continueWith { state with Plan = LogicalPlan.Project(unpivotPlan, projs) } RowShape.Empty tail
                    | None ->
                        continueWith (clientMap RowShape.Empty resSel { state with Plan = unpivotPlan }) RowShape.Empty tail
        | None ->
            continueWith (fallback state) rowShape tail

    /// Explode (a SelectMany over a collection member).
    let explode (env: StageEnv) (continueWith: StageContinuation) (state: CompiledLogicalQuery) (rowShape: RowShape) (tail: LinqStage list) (colLam: LambdaExpression) (resSelOpt: LambdaExpression option) : CompiledLogicalQuery =
        let makeCtx = StageContext.makeCtx rowShape
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
            continueWith { state with Plan = finalPlan } rowShape tail
        | None ->
            continueWith (fallback state) rowShape tail
