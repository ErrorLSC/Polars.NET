namespace Kaminari.Samidare.IR

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading

/// Runtime feedback: engines report how many rows a subplan produced, and the estimator prefers that to its own guess.
///
/// An observation belongs to a subplan, not to a table, so it captures what source statistics cannot, such as how many
/// rows correlated filters keep or a grouping merges. Join reordering consults it for each relation of the join graph,
/// which covers joins nested inside a relation; the intermediate results of the orders it compares are still estimated.
/// It only exists from the second run on, and a plan with a different literal is a different subplan.
///
/// A backend that cannot observe per-node row counts reports nothing.
module CardinalityFeedback =

    /// The key of a subplan of a query with these source bindings, or None when one of its scans has no lasting identity.
    /// Scans inside a scalar subquery are not renumbered: such a plan only matches itself.
    [<CompiledName("TryKey")>]
    let tryKey (sources: SourceBinding array) (plan: LogicalPlan) : ObservedPlan option =
        let order = Dictionary<int, int>()
        let identities = ResizeArray<string>()
        let identityOf slot =
            if slot < 0 || slot >= sources.Length then
                None
            else
                match sources.[slot] with
                | SourceBinding.Table table ->
                    match box table with
                    | :? ISourceIdentity as source -> source.SourceIdentity
                    | _ -> None
                | SourceBinding.InMemory _ -> None
        let mutable identified = true
        let rec number (node: LogicalPlan) : LogicalPlan =
            match node with
            | LogicalPlan.Scan(LogicalPlanSource.Slot slot, schema) ->
                match order.TryGetValue slot with
                | true, known -> LogicalPlan.Scan(LogicalPlanSource.Slot known, schema)
                | _ ->
                    match identityOf slot with
                    | Some identity ->
                        let next = identities.Count
                        order.[slot] <- next
                        identities.Add identity
                        LogicalPlan.Scan(LogicalPlanSource.Slot next, schema)
                    | None ->
                        identified <- false
                        node
            | LogicalPlan.Scan _ ->
                identified <- false
                node
            | other -> LogicalPlanOps.mapInputs number other
        let numbered = number plan
        if identified then Some { Plan = numbered; Sources = List.ofSeq identities } else None

    /// Tells the registered feedback stores how many rows a subplan produced. For engines that execute the plans.
    [<CompiledName("Report")>]
    let report (sources: SourceBinding array) (plan: LogicalPlan) (rows: int64) : unit =
        if QueryStatistics.HasFeedback then
            tryKey sources plan |> Option.iter (fun key -> QueryStatistics.Report(key, rows))

/// A feedback store in this process's memory, keeping the latest count of each subplan. Its generation moves only when a
/// count changes by at least `replanFactor` times, so that ordinary variation between runs does not replan cached queries.
type InMemoryCardinalityFeedback(replanFactor: float) =
    let observed = ConcurrentDictionary<ObservedPlan, ObservedCardinality>()
    let mutable generation = 0L

    new() = InMemoryCardinalityFeedback(2.0)

    interface ICardinalityFeedback with
        member _.TryGetObserved plan =
            match observed.TryGetValue plan with
            | true, value -> Some value
            | _ -> None

        member _.Record(plan, rows) =
            let current = float rows
            let changedEnough (before: float) =
                let low, high = min before current, max before current
                max 1.0 high / max 1.0 low >= replanFactor
            let mutable replan = true
            observed.AddOrUpdate(
                plan,
                (fun _ -> { Rows = current; Runs = 1 }),
                (fun _ previous ->
                    replan <- changedEnough previous.Rows
                    { Rows = current; Runs = previous.Runs + 1 })
            )
            |> ignore
            if replan then
                Interlocked.Increment(&generation) |> ignore

        member _.Generation = Interlocked.Read(&generation)
