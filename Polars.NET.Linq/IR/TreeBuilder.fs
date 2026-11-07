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
open Polars.NET.Linq.IR.StageContext

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

        let env: StageEnv =
            {
                Sources = sources
                ResolveSequence = SourceResolver.resolveSequencePlan backend sources
                IsGrouping = isGrouping
            }

        let initState: CompiledLogicalQuery =
            {
                Plan = initialPlan
                Sources = [||]
                Residual = ClientResidual.None
                RowShape = RowShape.Empty
            }

        /// Compiles the remaining stages onto the state: natively while the rows stay native, as client row
        /// operations once a client step has run, and not at all once LINQ-to-Objects has taken over.
        let rec processStages (state: CompiledLogicalQuery) (rowShape: RowShape) (remainingStages: LinqStage list) : CompiledLogicalQuery =
            match remainingStages with
            | [] ->
                match state.Residual with
                | ClientResidual.None -> { state with RowShape = rowShape }
                | _ -> state
            | stage :: tail ->
                match state.Residual with
                | ClientResidual.LinqToObjects ->
                    // The remaining stages are evaluated by LINQ-to-Objects; nothing more to record.
                    processStages state rowShape tail
                | ClientResidual.RowOps _ ->
                    match stage with
                    | LinqStage.Filter lam -> processStages (clientFilter rowShape lam state) rowShape tail
                    | LinqStage.Project lam -> processStages (clientMap rowShape lam state) rowShape tail
                    // Order-insensitive or handled at materialization; the end-of-build check below
                    // escalates combinations the row-op executor cannot express.
                    | LinqStage.Cast _ | LinqStage.OfType _ | LinqStage.DefaultIfEmpty _ | LinqStage.Chunk _ | LinqStage.Shuffle | LinqStage.AggregateBy _ ->
                        processStages state rowShape tail
                    | _ -> processStages (fallback state) rowShape tail
                | ClientResidual.None ->
                    match stage with
                    | LinqStage.Filter lam ->
                        RowStages.filter env processStages state rowShape tail lam

                    | LinqStage.TakeWhile lam | LinqStage.SkipWhile lam as sStage ->
                        RowStages.takeSkipWhile env processStages state rowShape tail sStage lam

                    | LinqStage.Project lam ->
                        RowStages.project env processStages state rowShape tail lam

                    | LinqStage.Sort(lam, isDesc, nullsLast) ->
                        RowStages.sort env processStages state rowShape tail lam isDesc nullsLast

                    | LinqStage.SortDefault(isDesc, nullsLast) ->
                        RowStages.sortDefault env processStages state rowShape tail isDesc nullsLast

                    | LinqStage.Take count ->
                        processStages { state with Plan = LogicalPlan.Limit(state.Plan, 0L, count) } rowShape tail

                    | LinqStage.Skip count ->
                        processStages { state with Plan = LogicalPlan.Limit(state.Plan, int64 count, UInt32.MaxValue) } rowShape tail

                    | LinqStage.TakeLast count ->
                        processStages { state with Plan = LogicalPlan.Limit(state.Plan, -int64 count, count) } rowShape tail

                    | LinqStage.SkipLast count ->
                        processStages { state with Plan = LogicalPlan.SkipLast(state.Plan, count) } rowShape tail

                    | LinqStage.Distinct ->
                        processStages { state with Plan = LogicalPlan.Distinct(state.Plan, None, DistinctKeep.First, true) } rowShape tail

                    | LinqStage.DistinctBy lam ->
                        RowStages.distinctBy env processStages state rowShape tail lam

                    | LinqStage.Reverse ->
                        processStages { state with Plan = LogicalPlan.Reverse state.Plan } rowShape tail

                    | LinqStage.Index ->
                        processStages { state with Plan = LogicalPlan.WithRowIndex(state.Plan, "index", None) } rowShape tail

                    | LinqStage.CountBy lam ->
                        GroupStages.countBy env processStages state rowShape tail lam

                    | LinqStage.GroupByKey keyLambda ->
                        GroupStages.groupByKey env processStages state rowShape tail keyLambda

                    | LinqStage.GroupByWithResult(keyLam, resLam) ->
                        GroupStages.groupByWithResult env processStages state rowShape tail keyLam resLam

                    | LinqStage.GroupByWithElementAndResult(keyLam, elemLam, resLam) ->
                        GroupStages.groupByWithElementAndResult env processStages state rowShape tail keyLam elemLam resLam

                    | LinqStage.GroupByRollup(keyLambda, resLambda) ->
                        GroupStages.rollup env processStages state rowShape tail keyLambda resLambda

                    | LinqStage.GroupByCube(keyLambda, resLambda) ->
                        GroupStages.cube env processStages state rowShape tail keyLambda resLambda

                    | LinqStage.GroupByGroupingSets(keyLambda, setsExpr, resLambda) ->
                        GroupStages.groupingSets env processStages state rowShape tail keyLambda setsExpr resLambda

                    | LinqStage.GroupPartitionSlice(keySelector, innerStages, resSelOpt) ->
                        GroupStages.partitionSlice env processStages state rowShape tail keySelector innerStages resSelOpt

                    | LinqStage.Join(methodInfo, innerExpr, outerKey, innerKey, resSelOpt) ->
                        JoinStages.join env processStages state rowShape tail methodInfo innerExpr outerKey innerKey resSelOpt

                    | LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, resSelOpt) ->
                        JoinStages.groupJoin env processStages state rowShape tail m innerExpr outerKey innerKey resSelOpt

                    | LinqStage.CrossJoin(innerExpr, resSelOpt) ->
                        JoinStages.crossJoin env processStages state rowShape tail innerExpr resSelOpt

                    | LinqStage.JoinAsOf(_, inner, outerKey, innerKey, outerByOpt, innerByOpt, resSelOpt, toleranceStr, toleranceInt, toleranceFloat, strategy) ->
                        JoinStages.joinAsOf env processStages state rowShape tail inner outerKey innerKey outerByOpt innerByOpt resSelOpt toleranceStr toleranceInt toleranceFloat strategy

                    | LinqStage.SetOp(methodInfo, secondExpr, key1Opt, key2Opt) ->
                        SetStages.setOp env processStages state rowShape tail methodInfo secondExpr key1Opt key2Opt

                    | LinqStage.Union(secondExpr, keyLambdaOpt) ->
                        SetStages.union env processStages state rowShape tail secondExpr keyLambdaOpt

                    | LinqStage.Concat secondExpr ->
                        SetStages.concat env processStages state rowShape tail secondExpr

                    | LinqStage.Append elemExpr ->
                        SetStages.append env processStages state rowShape tail elemExpr

                    | LinqStage.Prepend elemExpr ->
                        SetStages.prepend env processStages state rowShape tail elemExpr

                    | LinqStage.Zip(secondExpr, resSelOpt) ->
                        SetStages.zip env processStages state rowShape tail secondExpr resSelOpt

                    | LinqStage.Zip3(secondExpr, thirdExpr) ->
                        SetStages.zip3 env processStages state rowShape tail secondExpr thirdExpr

                    | LinqStage.Unpivot(colLam, resLambdaOpt) ->
                        SetStages.unpivot env processStages state rowShape tail colLam resLambdaOpt

                    | LinqStage.Explode(colLam, resSelOpt) ->
                        SetStages.explode env processStages state rowShape tail colLam resSelOpt

                    | LinqStage.OfType(sourceType, targetType) ->
                        if not (targetType.IsAssignableFrom sourceType || sourceType.IsAssignableFrom targetType) then
                            processStages { state with Plan = LogicalPlan.Filter(state.Plan, ScalarExpr.Literal(LiteralValue.Bool false)) } rowShape tail
                        else
                            processStages state rowShape tail

                    | LinqStage.Cast _
                    | LinqStage.DefaultIfEmpty _
                    | LinqStage.Chunk _
                    | LinqStage.Shuffle
                    | LinqStage.AggregateBy _ ->
                        processStages state rowShape tail

        let finalState = processStages initState RowShape.Empty stages

        let needsLinqToObjects = stages |> List.exists LinqStageTraits.breaksRowStream

        let completed =
            match finalState.Residual with
            | ClientResidual.RowOps _ when needsLinqToObjects -> fallback finalState
            | _ -> finalState

        { completed with Sources = sources.ToArray() }
