namespace Kaminari.Samidare.IR.Passes

open System
open Kaminari.Samidare.IR

/// Optimization pass for projection collapsing, identity elimination, and column pruning.
module internal ProjectionPruning =

    /// Collapses consecutive projections: Project(Project(input, p1), p2) -> Project(input, p2 ∘ p1)
    let rec prune (plan: LogicalPlan) : LogicalPlan =
        match plan with
        // 1. Double Project Collapsing
        | LogicalPlan.Project(LogicalPlan.Project(innerInput, innerProjs), outerProjs) ->
            // Over an aggregating projection (one row), an outer expression that aggregates or reads other rows would
            // read the input's rows once collapsed, and one reading no inner column would make it a row per input row.
            let innerNames = innerProjs |> List.map fst |> Set.ofList
            let overAggregates =
                innerProjs |> List.exists (snd >> ScalarExprOps.containsAggregation)
                && outerProjs
                   |> List.exists (fun (_, e) ->
                       ScalarExprOps.containsAggregation e
                       || ScalarExprOps.dependsOnOtherRows e
                       || Set.isEmpty (Set.intersect (ScalarExprOps.collectColumns e) innerNames))
            if overAggregates || innerProjs |> List.exists (fun (_, e) -> ScalarExprOps.containsWindow e) then
                LogicalPlan.Project(prune (LogicalPlan.Project(innerInput, innerProjs)), outerProjs)
            else
                let prunedInput = prune innerInput
                let innerMap = innerProjs |> Map.ofList
                let collapsedProjs =
                    outerProjs
                    |> List.map (fun (name, expr) ->
                        name, ScalarExprOps.substitute (fun col -> Map.tryFind col innerMap) expr)
                prune (LogicalPlan.Project(prunedInput, collapsedProjs))

        // 2. Identity Project Elimination: Project selecting exactly input columns unchanged
        | LogicalPlan.Project(input, projs) ->
            let prunedInput = prune input
            let inSchema = SchemaInference.inferPlanSchema prunedInput
            let isIdentity =
                inSchema.Fields.Length = projs.Length
                && List.zip inSchema.Fields projs
                   |> List.forall (fun (field, (projName, projExpr)) ->
                       field.Name = projName
                       && match projExpr with
                          | ScalarExpr.Column c when c = projName -> true
                          | _ -> false)

            if isIdentity && inSchema.Fields.Length > 0 then
                prunedInput
            else
                LogicalPlan.Project(prunedInput, projs)

        // 3. Empty WithColumns Elimination
        | LogicalPlan.WithColumns(input, []) ->
            prune input

        // Recursive tree mapping
        | other ->
            LogicalPlanOps.mapInputs prune other

    /// Entry point for Projection Pruning pass.
    let apply (plan: LogicalPlan) : LogicalPlan =
        prune plan

