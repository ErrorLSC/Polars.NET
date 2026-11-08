namespace Polars.NET.Linq.IR.Passes

open System.Collections.Generic
open Polars.NET.Linq.IR

/// Finds subplans that a query computes more than once and marks them as common subplans (CTEs), so a backend
/// computes each once.
///
/// A queryable referenced several times (a CTE in SQL terms, e.g. TPC-DS Q14's cross_items used by three
/// channels) is built into the plan once per reference, each over its own source slots. This pass:
/// 1. Gives the slots that read the same source (the same table object, or the same in-memory snapshot) one
///    slot, so equal subplans become structurally equal.
/// 2. Groups structurally equal subtrees and picks, largest first, those occurring at least twice outside the
///    copies of an already picked subplan, and worth computing once: they join, group, deduplicate or union.
///    A plain scan, filter or projection of a table is left to each consumer, where its own predicates and
///    projections reach it.
///    A decorrelated EXISTS's row-indexed outer query is also left alone (see isRowIndexed).
/// 3. Replaces every occurrence with `Shared(id, subplan)`. When every consumer filters the subplan, the
///    subplan is filtered by the disjunction of their predicates (each consumer keeps its own), so the shared
///    computation covers only the rows some consumer reads.
///
/// It runs on a complete plan before the optimization passes, which treat Shared as a barrier.
module internal CommonSubplanSharing =

    /// One slot per source: every slot maps to the first slot reading the same source.
    let private canonicalSlots (sources: SourceBinding array) : int array =
        let first = Dictionary<obj, int>(HashIdentity.Reference)
        sources
        |> Array.mapi (fun i binding ->
            let identity =
                match binding with
                | SourceBinding.Table table -> box table
                | SourceBinding.InMemory(rows, _) -> box rows
            match first.TryGetValue identity with
            | true, j -> j
            | _ ->
                first.[identity] <- i
                i)

    let private placeholder = LogicalPlan.Empty LogicalSchema.Empty

    /// One position of a node in the plan tree. A node object can sit at several positions (a rewrite that
    /// reuses a subplan), so what is counted, and marked dead, is the position.
    [<AllowNullLiteral>]
    type private Occurrence(node: LogicalPlan, parent: LogicalPlan option) =
        member _.Node = node
        member _.Parent = parent
        member val Children: Occurrence list = [] with get, set
        member val Dead = false with get, set

    /// Whether computing the subplan is worth sharing: it does more than read, filter and reshape a table.
    let rec private isWorthSharing (plan: LogicalPlan) =
        match plan with
        | LogicalPlan.Join _
        | LogicalPlan.JoinAsOf _
        | LogicalPlan.GroupBy _
        | LogicalPlan.GroupingSets _
        | LogicalPlan.Distinct _
        | LogicalPlan.Union _ -> true
        | LogicalPlan.Scan _
        | LogicalPlan.Empty _ -> false
        | other -> LogicalPlanOps.getInputs other |> List.exists isWorthSharing

    /// A row-indexed query read twice is how a correlated EXISTS is decorrelated: one selective outer query, whose
    /// filters do the work. It is left, with everything inside it, to the backend: Polars finds this repeat itself
    /// and still pushes the filters into the scans, which it does not do beneath a cache that the plan declares.
    let private isRowIndexed (plan: LogicalPlan) =
        match plan with
        | LogicalPlan.WithRowIndex _ -> true
        | _ -> false

    /// Whether a consumer's predicate may also filter the shared subplan: it reads only the row it filters.
    let private isRowLocal (predicate: ScalarExpr) =
        not (
            ScalarExprOps.containsAggregation predicate
            || ScalarExprOps.containsWindow predicate
            || ScalarExprOps.containsSubquery predicate
            || ScalarExprOps.dependsOnOtherRows predicate
        )

    let apply (sources: SourceBinding array) (plan: LogicalPlan) : LogicalPlan =
        let slots = canonicalSlots sources
        let plan =
            if slots |> Array.forall2 (=) [| 0 .. slots.Length - 1 |] then
                plan
            else
                plan
                |> LogicalPlanOps.rewrite (function
                    | LogicalPlan.Scan(LogicalPlanSource.Slot index, schema) when slots.[index] <> index ->
                        Some(LogicalPlan.Scan(LogicalPlanSource.Slot slots.[index], schema))
                    | _ -> None)

        // Structural hash and size of every node, bottom-up, each node hashed once.
        let hashes = Dictionary<LogicalPlan, int>(HashIdentity.Reference)
        let sizes = Dictionary<LogicalPlan, int>(HashIdentity.Reference)
        let rec measure (node: LogicalPlan) : int =
            match hashes.TryGetValue node with
            | true, h -> h
            | _ ->
                let inputs = LogicalPlanOps.getInputs node
                let childHashes = inputs |> List.map measure
                let local = hash (LogicalPlanOps.mapInputs (fun _ -> placeholder) node)
                let h = childHashes |> List.fold (fun acc c -> acc * 31 + c) local
                hashes.[node] <- h
                sizes.[node] <- 1 + (inputs |> List.sumBy (fun i -> sizes.[i]))
                h

        measure plan |> ignore

        // Every position in the tree, grouped into classes of equal subtrees.
        let classes = Dictionary<int, ResizeArray<ResizeArray<Occurrence>>>()
        let rec collect (parent: LogicalPlan option) (node: LogicalPlan) : Occurrence =
            let occurrence = Occurrence(node, parent)
            let h = hashes.[node]
            let buckets =
                match classes.TryGetValue h with
                | true, b -> b
                | _ ->
                    let b = ResizeArray()
                    classes.[h] <- b
                    b
            match buckets |> Seq.tryFind (fun members -> let rep = members.[0].Node in obj.ReferenceEquals(rep, node) || rep = node) with
            | Some members -> members.Add occurrence
            | None -> buckets.Add(ResizeArray [ occurrence ])
            occurrence.Children <- LogicalPlanOps.getInputs node |> List.map (collect (Some node))
            occurrence

        collect None plan |> ignore

        let candidates =
            classes.Values
            |> Seq.collect id
            |> Seq.filter (fun members -> members.Count >= 2 && isWorthSharing members.[0].Node)
            |> Seq.sortByDescending (fun members -> sizes.[members.[0].Node])
            |> List.ofSeq

        // Picks classes largest first. The copies of a picked subplan other than its first occurrence are dead:
        // a class counts only its occurrences outside them.
        let rec markDead (occurrence: Occurrence) =
            occurrence.Dead <- true
            occurrence.Children |> List.iter markDead

        let replacements = Dictionary<LogicalPlan, int * ScalarExpr option>(HashIdentity.Reference)
        let mutable nextId = 0

        for members in candidates do
            let live = members |> Seq.filter (fun occurrence -> not occurrence.Dead) |> List.ofSeq
            if isRowIndexed members.[0].Node then
                live |> List.iter markDead
            elif live.Length >= 2 then
                let sharedId = nextId
                nextId <- nextId + 1
                // The disjunction of the consumers' predicates, when every live consumer filters the subplan.
                let filter =
                    let predicates =
                        live
                        |> List.map (fun occurrence ->
                            match occurrence.Parent with
                            | Some(LogicalPlan.Filter(_, predicate)) when isRowLocal predicate -> Some predicate
                            | _ -> None)
                    if predicates |> List.forall Option.isSome then
                        match predicates |> List.choose id |> List.distinct with
                        | [] -> None
                        | first :: rest -> Some(rest |> List.fold (fun acc p -> ScalarExpr.Binary(BinaryOp.Or, acc, p)) first)
                    else
                        None
                for occurrence in members do
                    replacements.[occurrence.Node] <- (sharedId, filter)
                live |> List.skip 1 |> List.iter markDead

        if replacements.Count = 0 then
            plan
        else
            // Bottom-up, so a subplan shared inside another is shared in every copy of it alike.
            let rec share (node: LogicalPlan) : LogicalPlan =
                let rewritten = LogicalPlanOps.mapInputs share node
                match replacements.TryGetValue node with
                | true, (id, filter) ->
                    let body =
                        match filter with
                        | Some predicate -> LogicalPlan.Filter(rewritten, predicate)
                        | None -> rewritten
                    LogicalPlan.Shared(id, body)
                | _ -> rewritten

            share plan
