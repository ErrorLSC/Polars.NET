namespace Kaminari.Samidare.IR

open System
open System.Linq.Expressions
open Kaminari.Samidare


/// Continues compiling the remaining stages from a query state.
type internal StageContinuation = CompiledLogicalQuery -> RowShape -> LinqStage list -> CompiledLogicalQuery

/// What every stage handler of one compilation shares.
type internal StageEnv =
    {
        /// The sources the plan reads; a stage that reads another sequence registers it here.
        Sources: SourceRegistry
        /// Resolves a sequence expression (a join's inner side, a set operation's second operand, ...) to a plan.
        ResolveSequence: Expression -> LogicalPlan option
        /// Whether the query's elements are groups (IGrouping), materialized from the grouped rows.
        IsGrouping: bool
    }

/// Shared steps of the stage handlers: giving up to LINQ-to-Objects, appending client row operations, and
/// building translation contexts over a plan.
module internal StageContext =

    /// Hands the rest of the pipeline to LINQ-to-Objects.
    let fallback (state: CompiledLogicalQuery) : CompiledLogicalQuery =
        { state with Residual = ClientResidual.LinqToObjects }

    /// Appends a client row operation. The first one ends the native plan, whose rows are then
    /// materialized as the operation's input type, so the entities of the join scopes still in effect
    /// are packed into struct columns first.
    let appendClientOp (rowShape: RowShape) (op: ClientRowOp) (state: CompiledLogicalQuery) : CompiledLogicalQuery =
        match state.Residual with
        | ClientResidual.None ->
            match EntityPacking.packRow rowShape state.Plan op.Lambda.Parameters.[0].Type with
            | RowPacking.NotNeeded -> { state with Residual = ClientResidual.RowOps [ op ] }
            | RowPacking.Packed plan -> { state with Plan = plan; Residual = ClientResidual.RowOps [ op ] }
            | RowPacking.Unresolvable -> fallback state
        | ClientResidual.RowOps ops -> { state with Residual = ClientResidual.RowOps(ops @ [ op ]) }
        | ClientResidual.LinqToObjects -> state

    let clientFilter (rowShape: RowShape) (lam: LambdaExpression) (state: CompiledLogicalQuery) : CompiledLogicalQuery =
        appendClientOp rowShape (ClientRowOp.Filter(PipelineStageParser.compileClientPredicate lam, lam)) state

    let clientMap (rowShape: RowShape) (lam: LambdaExpression) (state: CompiledLogicalQuery) : CompiledLogicalQuery =
        appendClientOp rowShape (ClientRowOp.Map(PipelineStageParser.compileToBoxedFunc lam, lam)) state

    /// A translation context for a lambda parameter of type `t` over the rows of `plan`.
    let makeCtx (rowShape: RowShape) (t: Type) (pName: string) (plan: LogicalPlan) =
        let cols = (SchemaInference.inferPlanSchema plan).ColumnNames
        PipelineStageParser.createContextWithScopeAndCols rowShape t cols pName

    /// For a lambda body holding a subquery: translates with subqueries allowed, builds the node that holds them,
    /// and replaces them with the Decorrelation pass. None when the body holds no subquery, or a part does not
    /// translate or decorrelate: the caller then takes its other path (a client step).
    let tryWithSubqueries (body: Expression) (translate: unit -> 'T option) (build: 'T -> LogicalPlan) : LogicalPlan option =
        if not (SubqueryPatterns.containsSubquery body) then
            None
        else
            let scope = CompilationScope.Current
            let saved = scope.AllowSubqueries
            let evaluatedBefore = scope.EvaluatedAtCompile
            scope.AllowSubqueries <- true
            let translated =
                try
                    translate ()
                finally
                    scope.AllowSubqueries <- saved
            translated
            |> Option.bind (fun t ->
                let node = build t
                if LogicalPlanOps.containsSubqueries node then
                    let options: Passes.Decorrelation.Options =
                        { Fresh = scope.FreshName; SupportsAssertions = scope.Backend.SupportsAssertions }
                    match Passes.Decorrelation.tryNode options node with
                    | Ok plan -> Some plan
                    | Error _ -> None
                // A subquery evaluated while compiling (design Qc) left a literal: the node is final.
                elif scope.EvaluatedAtCompile && not evaluatedBefore then
                    Some node
                // A body that only looked like it held a subquery (a sequence method of another kind): the usual path.
                else
                    None)
