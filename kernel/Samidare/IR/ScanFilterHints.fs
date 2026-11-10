namespace Kaminari.Samidare.IR

/// Tells filter-aware table sources (IFilterAwareSource) which rows an optimized plan keeps from them, so a
/// backend can bind the narrowed sources before it lowers the plan. The plan itself is not changed: its filters
/// still run.
module ScanFilterHints =

    open System.Collections.Generic

    /// The filters stacked directly on a scan, innermost first, and the node under them.
    let rec private stripFilters (plan: LogicalPlan) (above: ScalarExpr list) =
        match plan with
        | LogicalPlan.Filter(input, predicate) -> stripFilters input (predicate :: above)
        | _ -> plan, above

    let private conjunction (predicates: ScalarExpr list) =
        predicates |> List.reduce (fun l r -> ScalarExpr.Binary(BinaryOp.And, l, r))

    let private disjunction (predicates: ScalarExpr list) =
        predicates |> List.reduce (fun l r -> ScalarExpr.Binary(BinaryOp.Or, l, r))

    /// For each slot, what its scans keep: Some filter per filtered scan, None for a scan that reads every row.
    /// None as a whole when the plan reads a slot in a way the walk cannot see (a subquery inside an expression).
    let private scanFilters (plan: LogicalPlan) : Dictionary<int, ScalarExpr option list> option =
        let found = Dictionary<int, ScalarExpr option list>()
        let sharedSeen = HashSet<int>()
        let mutable opaque = false

        let record slot filter =
            found.[slot] <- filter :: (match found.TryGetValue slot with | true, l -> l | _ -> [])

        let check (e: ScalarExpr) =
            if ScalarExprOps.containsSubquery e then opaque <- true
            e

        let rec visit (node: LogicalPlan) =
            if not opaque then
                let under, filters = stripFilters node []
                filters |> List.iter (check >> ignore)
                LogicalPlanOps.mapExpressions check under |> ignore

                match under with
                // Filters right above a scan read the scan's own columns.
                | LogicalPlan.Scan(LogicalPlanSource.Slot slot, _) ->
                    record slot (if filters.IsEmpty then None else Some(conjunction filters))
                // Every occurrence of a shared subplan reads the same rows once.
                | LogicalPlan.Shared(id, input) -> if sharedSeen.Add id then visit input
                | _ -> LogicalPlanOps.getInputs under |> List.iter visit

        visit plan
        if opaque then None else Some found

    /// The sources with every filter-aware table narrowed to the rows the plan keeps from it. A slot gets a hint
    /// only when every scan of it is filtered; the hint is the disjunction of those filters.
    let apply (sources: SourceBinding array) (plan: LogicalPlan) : SourceBinding array =
        let anyAware =
            sources
            |> Array.exists (function
                | SourceBinding.Table(:? IFilterAwareSource) -> true
                | _ -> false)

        if not anyAware then
            sources
        else
            match scanFilters plan with
            | None -> sources
            | Some found ->
                sources
                |> Array.mapi (fun slot binding ->
                    match binding, found.TryGetValue slot with
                    | SourceBinding.Table(:? IFilterAwareSource as aware), (true, filters) when
                        filters |> List.forall Option.isSome
                        ->
                        SourceBinding.Table(aware.WithFilterHint(filters |> List.map Option.get |> List.rev |> disjunction))
                    | _ -> binding)
