namespace Polars.NET.Linq.IR

open Polars.NET.Linq.IR.Passes

/// Coordinates and executes rule-based relational optimization passes over a LogicalPlan tree.
module internal LogicalOptimizer =

    /// Runs all middle-end optimization passes sequentially on the logical plan.
    let optimize (plan: LogicalPlan) : LogicalPlan =
        plan
        |> PredicatePushdown.apply
        |> ProjectionPruning.apply
        |> SortSliceFusion.apply

