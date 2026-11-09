namespace Polars.NET.Linq.IR

open Polars.NET.Linq.IR.Passes

/// Coordinates and executes rule-based relational optimization passes over a LogicalPlan tree.
module internal LogicalOptimizer =

    /// Runs all middle-end optimization passes sequentially on the logical plan.
    let optimize (plan: LogicalPlan) : LogicalPlan =
        plan
        |> FilterNullSimplification.apply
        |> PredicatePushdown.apply
        |> ProjectionPruning.apply
        |> OuterJoinSimplification.apply
        |> AntiJoinRecognition.apply
        |> SortSliceFusion.apply
        |> GroupOrderRelaxation.apply

    /// Optimizes the plan of a complete query, reading statistics of its sources. A plan that will be grafted into
    /// another is left to `optimize`: join order is chosen once the whole query is known. With `allowUnstableSort`
    /// ties of the plan's sorts are unspecified, which frees the order beneath them.
    let optimizeComplete (allowUnstableSort: bool) (sources: SourceBinding array) (plan: LogicalPlan) : LogicalPlan =
        let relaxed, unorderedJoins =
            plan
            |> optimize
            |> EagerAggregation.apply sources
            |> UnionAggregateFlattening.apply
            |> GroupingSetsCascade.apply
            |> GroupOrderRelaxation.applyCollectingUnorderedJoins allowUnstableSort
        JoinReordering.apply sources unorderedJoins relaxed |> SemiJoinReduction.apply sources
