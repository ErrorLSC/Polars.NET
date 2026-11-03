namespace Polars.NET.Linq.IR.Passes

open System
open Polars.NET.Core
open Polars.NET.Linq.IR

/// Relational optimization pass that pushes Filter predicates down through
/// projections, joins, unions, and renames toward the scan leaf nodes.
module internal PredicatePushdown =

    /// Decomposes a ScalarExpr conjunction into a list of independent conjuncts.
    let rec splitConjuncts (expr: ScalarExpr) : ScalarExpr list =
        match expr with
        | ScalarExpr.Binary(BinaryOp.And, left, right) ->
            splitConjuncts left @ splitConjuncts right
        | other -> [ other ]

    /// Recombines a list of conjuncts into a single conjunction expression.
    let combineConjuncts (conjuncts: ScalarExpr list) : ScalarExpr option =
        match conjuncts with
        | [] -> None
        | [ single ] -> Some single
        | head :: tail ->
            Some(tail |> List.fold (fun acc c -> ScalarExpr.Binary(BinaryOp.And, acc, c)) head)

    /// Attempts to push a single conjunct predicate down into a target plan.
    let rec pushdownConjunct (pred: ScalarExpr) (plan: LogicalPlan) : LogicalPlan =
        match pred with
        // Trivial true: no-op, eliminate
        | ScalarExpr.Literal(LiteralValue.Bool true) -> plan

        // Trivial false: empty result
        | ScalarExpr.Literal(LiteralValue.Bool false) ->
            let schema = SchemaInference.inferPlanSchema plan
            LogicalPlan.Empty schema

        | _ ->
            let refCols = ScalarExprOps.collectColumns pred

            match plan with
            // 1. Pushdown through Union: σ_p(R ∪ S) ≡ σ_p(R) ∪ σ_p(S)
            | LogicalPlan.Union(left, right, isAll) ->
                let leftPushed = pushdownConjunct pred left
                let rightPushed = pushdownConjunct pred right
                LogicalPlan.Union(leftPushed, rightPushed, isAll)

            // 2. Pushdown through Rename: invert column names and push down
            | LogicalPlan.Rename(input, existingNames, newNames) ->
                let renameMap = List.zip newNames existingNames |> Map.ofList
                let revertedPred = ScalarExprOps.renameColumns renameMap pred
                let pushedInput = pushdownConjunct revertedPred input
                LogicalPlan.Rename(pushedInput, existingNames, newNames)

            // 3. Pushdown through Projection: substitute aliases and push below Project
            | LogicalPlan.Project(input, projs) ->
                // Ensure predicate does not contain aggregations, window functions, or subqueries before pushing
                if ScalarExprOps.containsAggregation pred || ScalarExprOps.containsWindow pred || ScalarExprOps.containsSubquery pred then
                    LogicalPlan.Filter(plan, pred)
                else
                    let projMap = projs |> Map.ofList
                    let canSubstitute =
                        refCols |> Set.forall (fun col -> projMap.ContainsKey col)

                    if canSubstitute then
                        let substPred =
                            ScalarExprOps.substitute (fun col -> projMap.TryFind col) pred
                        // If substituted predicate contains aggregations, window functions, or subqueries, cannot push below Project!
                        if ScalarExprOps.containsAggregation substPred || ScalarExprOps.containsWindow substPred || ScalarExprOps.containsSubquery substPred then
                            LogicalPlan.Filter(plan, pred)
                        else
                            let pushedInput = pushdownConjunct substPred input
                            LogicalPlan.Project(pushedInput, projs)
                    else
                        LogicalPlan.Filter(plan, pred)

            // 4. Pushdown through Join
            | LogicalPlan.Join(left, right, lOn, rOn, how, suffix, coalesce, nullEq) ->
                let leftSchema = SchemaInference.inferPlanSchema left
                let leftCols = leftSchema.ColumnNames |> Set.ofList
                let rightSchema = SchemaInference.inferPlanSchema right
                let rightCols = rightSchema.ColumnNames |> Set.ofList

                match how with
                | PlJoinType.Inner ->
                    if not refCols.IsEmpty && refCols |> Set.isSubset leftCols then
                        let pushedLeft = pushdownConjunct pred left
                        LogicalPlan.Join(pushedLeft, right, lOn, rOn, how, suffix, coalesce, nullEq)
                    elif not refCols.IsEmpty && refCols |> Set.isSubset rightCols then
                        let pushedRight = pushdownConjunct pred right
                        LogicalPlan.Join(left, pushedRight, lOn, rOn, how, suffix, coalesce, nullEq)
                    else
                        LogicalPlan.Filter(plan, pred)

                | PlJoinType.Left ->
                    if not refCols.IsEmpty && refCols |> Set.isSubset leftCols then
                        let pushedLeft = pushdownConjunct pred left
                        LogicalPlan.Join(pushedLeft, right, lOn, rOn, how, suffix, coalesce, nullEq)
                    else
                        LogicalPlan.Filter(plan, pred)

                | _ ->
                    LogicalPlan.Filter(plan, pred)

            // Base case: cannot push down further
            | _ ->
                LogicalPlan.Filter(plan, pred)

    /// Main recursive pass applying predicate pushdown across the entire logical tree.
    let rec apply (plan: LogicalPlan) : LogicalPlan =
        match plan with
        | LogicalPlan.Filter(input, pred) ->
            let optimizedInput = apply input
            let conjuncts = splitConjuncts pred
            conjuncts
            |> List.fold (fun currentPlan conjunct -> pushdownConjunct conjunct currentPlan) optimizedInput

        | other ->
            LogicalPlanOps.mapInputs apply other

