namespace Polars.NET.Linq.IR.Passes

open Polars.NET.Linq.IR

/// Lets a grouping-sets level be computed from a finer level instead of from the input.
///
/// A ROLLUP of eight keys groups the whole input nine times. Each level is the next finer one with a key removed, so
/// it can group that level's rows, which are usually far fewer. This pass declares how (`LevelMerges`) when every
/// aggregate merges exactly: Sum over integers and decimals (a floating sum rounds differently in another order), and
/// Min and Max. Groups keep their order of first appearance, since a finer level lists its groups in that order.
module internal GroupingSetsCascade =

    let rec apply (plan: LogicalPlan) : LogicalPlan =
        match LogicalPlanOps.mapInputs apply plan with
        | LogicalPlan.GroupingSets(input, spec) as grouping when spec.LevelMerges.IsNone ->
            let schema = SchemaInference.inferPlanSchema input
            let merge (alias: string, aggregate: ScalarExpr) =
                match aggregate with
                | ScalarExpr.Aggregation(AggKind.Sum, Some argument, false) when
                    not (ScalarExprOps.dependsOnOtherRows argument) && EagerAggregation.isExactNumber schema argument
                    ->
                    Some(alias, ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Column alias), false))
                | ScalarExpr.Aggregation((AggKind.Min | AggKind.Max as kind), Some argument, false) when
                    not (ScalarExprOps.dependsOnOtherRows argument)
                    ->
                    Some(alias, ScalarExpr.Aggregation(kind, Some(ScalarExpr.Column alias), false))
                | _ -> None
            let merges = spec.Aggs |> List.map merge
            if spec.Levels.Length > 1 && not spec.Aggs.IsEmpty && merges |> List.forall Option.isSome then
                LogicalPlan.GroupingSets(input, { spec with LevelMerges = Some(merges |> List.map Option.get) })
            else
                grouping
        | other -> other
