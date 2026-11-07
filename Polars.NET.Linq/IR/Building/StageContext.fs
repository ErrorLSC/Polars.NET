namespace Polars.NET.Linq.IR

open System
open System.Linq.Expressions
open Polars.NET.Linq


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
