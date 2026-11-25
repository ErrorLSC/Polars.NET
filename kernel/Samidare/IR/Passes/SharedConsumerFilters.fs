namespace Kaminari.Samidare.IR.Passes

open System.Collections.Generic
open Kaminari.Samidare.IR

/// Filters each shared plan by what its consumers read, once predicate pushdown has brought their filters to it.
///
/// A shared plan is a barrier to the filters of its consumers, so it computes every row any of them might read. When
/// each consumer filters it, the plan computes only the rows of the disjunction of their filters, and each consumer
/// keeps its own: a common table expression read once per year computes the years read, in one pass. A consumer
/// without a filter reads every row, and the plan stays whole. A shared plan left with one consumer (the others
/// dropped as empty branches of a union) is no longer shared, so the consumer's filters reach into it.
///
/// It runs after predicate pushdown, which leaves a consumer's filters directly above the shared node (filters it
/// cannot move into the plan), and after constant folding, which drops the consumers a constant filter empties. Another
/// pushdown run then moves the new filters into the plan.
module internal SharedConsumerFilters =

    let private disjoin (predicates: ScalarExpr list) =
        predicates |> List.reduce (fun a b -> ScalarExpr.Binary(BinaryOp.Or, a, b))

    let private conjoin (predicates: ScalarExpr list) =
        predicates |> List.reduce (fun a b -> ScalarExpr.Binary(BinaryOp.And, a, b))

    let apply (plan: LogicalPlan) : LogicalPlan =
        let consumers = Dictionary<int, ResizeArray<ScalarExpr list>>()
        let bodies = Dictionary<int, LogicalPlan>()
        /// `filters`: the conjuncts of the filters directly above the node, when they are (empty: none).
        let rec visit (filters: ScalarExpr list) (p: LogicalPlan) =
            match p with
            | LogicalPlan.Filter(input, predicate) ->
                match input with
                | LogicalPlan.Filter _
                | LogicalPlan.Shared _ ->
                    // Only conjuncts of the row alone: leaving one out makes the shared filter weaker, never wrong.
                    let rowLocal = FilterShapes.conjuncts predicate |> List.filter (ScalarExprOps.dependsOnOtherRows >> not)
                    // The filter nearest the shared node first.
                    visit (rowLocal @ filters) input
                | _ -> visit [] input
            | LogicalPlan.Shared(id, body) ->
                match consumers.TryGetValue id with
                | true, seen -> seen.Add filters
                | _ ->
                    consumers.[id] <- ResizeArray [ filters ]
                    bodies.[id] <- body
                    visit [] body
            | other -> LogicalPlanOps.getInputs other |> List.iter (visit [])
        visit [] plan
        let rewritten = Dictionary<int, LogicalPlan>()
        let rec rewrite (p: LogicalPlan) =
            match p with
            | LogicalPlan.Shared(id, _) when consumers.[id].Count = 1 -> bodyOf id
            | LogicalPlan.Shared(id, _) -> LogicalPlan.Shared(id, bodyOf id)
            | other -> LogicalPlanOps.mapInputs rewrite other
        and bodyOf id =
            match rewritten.TryGetValue id with
            | true, body -> body
            | _ ->
                let inner = rewrite bodies.[id]
                let filters = consumers.[id]
                let body =
                    if filters.Count >= 2 && filters |> Seq.forall (List.isEmpty >> not) then
                        LogicalPlan.Filter(inner, filters |> Seq.map conjoin |> Seq.distinct |> List.ofSeq |> disjoin)
                    else
                        inner
                rewritten.[id] <- body
                body
        rewrite plan
