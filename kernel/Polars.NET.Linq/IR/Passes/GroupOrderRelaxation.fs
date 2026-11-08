namespace Polars.NET.Linq.IR.Passes

open System.Collections.Generic
open Polars.NET.Linq.IR

/// Drops order guarantees whose effect cannot be observed.
///
/// LINQ groups come out in the order their keys first appear, so every GroupBy keeps its order by default, which
/// costs about 10% on high-cardinality groupings. Order is relaxed only where it provably does not matter:
/// - A GroupBy beneath a sort over all of its keys: each group has a distinct position, so the sort leaves no ties
///   for the (stable) sort to break by input order.
/// - Rows feeding order-free aggregations (Sum, Count, Min, Max, Mean, ...; not First, Last or List): a projection
///   made only of such aggregations, and a GroupBy whose order is relaxed and whose aggregations are all order-free.
///   Beneath them a GroupBy keeps no order and a Sort is dead and removed.
/// - Windows read order only inside their partitions, so a sort that, with the partition columns, covers a GroupBy's
///   keys leaves its groups' order unobserved.
/// - The inputs of a join, union or order-free grouping-sets node whose own order is not observed.
/// - A shared subplan none of whose consumers observes its order.
/// - With unstable sorts allowed (the query's optimization option), the input of any sort.
/// - The right side of a semi or anti join (an EXISTS / IN subquery): only which keys it holds is observed.
/// - A Distinct whose order is not observed. Its input is freed too when the kept row cannot depend on order
///   (whole-row distinct, or keep Any / None).
/// Anything else keeps its order. A GroupBy's input keeps its order unless the rules above free it, since
/// aggregations such as First and Last read it.
module internal GroupOrderRelaxation =

    /// What the consumer of a plan observes of its row order.
    [<RequireQualifiedAccess>]
    type private OrderNeed =
        /// The rows' order is observed.
        | Ordered
        /// Only the rows themselves are observed, not their order.
        | Unordered
        /// The rows are sorted by these columns: their order is not observed when the rows are unique on them.
        | SortedBy of columns: Set<string>

    /// The output column of a group key, when it is a plain or aliased column.
    let private keyName (key: ScalarExpr) =
        match key with
        | ScalarExpr.Column name -> Some name
        | ScalarExpr.Alias(_, name) -> Some name
        | _ -> None

    let private isOrderFreeAgg (kind: AggKind) =
        match kind with
        | AggKind.First
        | AggKind.Last
        | AggKind.List -> false
        | _ -> true

    /// Whether an expression is a function of order-free aggregations of its input only (every column read inside
    /// such an aggregation), so its value does not depend on the order of the input rows.
    let rec private isOrderFreeAggregate (insideAgg: bool) (expr: ScalarExpr) =
        let recurse = isOrderFreeAggregate insideAgg
        match expr with
        | ScalarExpr.Column _ -> insideAgg
        | ScalarExpr.Literal _ -> true
        | ScalarExpr.Aggregation(kind, inner, _) ->
            not insideAgg && isOrderFreeAgg kind && (inner |> Option.forall (isOrderFreeAggregate true))
        | ScalarExpr.Alias(inner, _)
        | ScalarExpr.Unary(_, inner)
        | ScalarExpr.Cast(inner, _, _)
        | ScalarExpr.EnumConvert(_, inner, _) -> recurse inner
        | ScalarExpr.Binary(_, left, right) -> recurse left && recurse right
        | ScalarExpr.Function(_, args, _) -> args |> List.forall recurse
        | ScalarExpr.Conditional(test, ifTrue, ifFalse) -> recurse test && recurse ifTrue && recurse ifFalse
        | ScalarExpr.Coalesce exprs -> exprs |> List.forall recurse
        | _ -> false

    /// Whether a projection may read the rows' order (windows, aggregations over the whole input).
    let private readsOrder (expr: ScalarExpr) =
        ScalarExprOps.containsWindow expr || ScalarExprOps.containsAggregation expr

    /// The partitions in which an expression reads its input's order: one column set per window whose value depends
    /// on order inside its partition. None when it reads order some other way (a frame function, an aggregation over
    /// the whole input, a partition by a computed value). A window of order-free aggregations reads no order.
    let rec private orderReadingPartitions (expr: ScalarExpr) : Set<string> list option =
        let all (exprs: ScalarExpr list) =
            let parts = exprs |> List.map orderReadingPartitions
            if parts |> List.forall Option.isSome then Some(parts |> List.collect Option.get) else None
        match expr with
        | ScalarExpr.Window(inner, partitionBy, _) ->
            let columns =
                partitionBy
                |> List.choose (function
                    | ScalarExpr.Column name -> Some name
                    | _ -> None)
            if partitionBy.IsEmpty || columns.Length <> partitionBy.Length then None
            elif isOrderFreeAggregate false inner then Some []
            else Some [ Set.ofList columns ]
        | ScalarExpr.Column _
        | ScalarExpr.Literal _ -> Some []
        | ScalarExpr.Alias(inner, _)
        | ScalarExpr.Unary(_, inner)
        | ScalarExpr.Cast(inner, _, _)
        | ScalarExpr.EnumConvert(_, inner, _) -> orderReadingPartitions inner
        | ScalarExpr.Binary(_, left, right) -> all [ left; right ]
        | ScalarExpr.Conditional(test, ifTrue, ifFalse) -> all [ test; ifTrue; ifFalse ]
        | ScalarExpr.Coalesce exprs -> all exprs
        | ScalarExpr.Function(_, args, _) when not (ScalarExprOps.dependsOnOtherRows expr) || ScalarExprOps.containsWindow expr ->
            all args
        | _ -> None

    /// The need below a projection-like node: sort columns are traced to the columns they copy; a column
    /// computed by the node does not identify input rows, so it is dropped from the set. Windows read order only
    /// among rows of one partition, which `SortedBy` expresses; several partitionings observe at least the rows
    /// equal on their common columns.
    let private throughProjection (need: OrderNeed) (columns: (string * ScalarExpr) list) (passThrough: bool) =
        let traced =
            match need with
            | OrderNeed.SortedBy sortColumns ->
                let defined = columns |> Map.ofList
                sortColumns
                |> Seq.choose (fun c ->
                    match defined.TryFind c with
                    | Some(ScalarExpr.Column source) -> Some source
                    | Some _ -> None
                    | None -> if passThrough then Some c else None)
                |> Set.ofSeq
                |> OrderNeed.SortedBy
            | other -> other

        if columns |> List.exists (snd >> readsOrder) then
            let partitions = columns |> List.map (snd >> orderReadingPartitions)
            if partitions |> List.forall Option.isSome then
                match partitions |> List.collect Option.get with
                | [] -> traced
                | first :: rest ->
                    let common = rest |> List.fold Set.intersect first
                    match traced with
                    | OrderNeed.Ordered -> OrderNeed.Ordered
                    | OrderNeed.Unordered -> OrderNeed.SortedBy common
                    | OrderNeed.SortedBy observed -> OrderNeed.SortedBy(Set.intersect observed common)
            else
                OrderNeed.Ordered
        else
            traced

    /// What one run over a plan knows and collects.
    type private RelaxRun =
        {
            UnorderedJoins: HashSet<LogicalPlan>
            /// Ties of a sort are unspecified, so nothing observes the order of a sort's input.
            AllowUnstableSort: bool
            /// Shared subplans known to have no consumer observing their order.
            FreeShared: Set<int>
            /// For each Shared id met, whether every occurrence so far left its order unobserved.
            SharedUnobserved: Dictionary<int, bool>
        }

    let rec private relax (run: RelaxRun) (need: OrderNeed) (plan: LogicalPlan) : LogicalPlan =
        let relax = relax run
        match plan with
        | LogicalPlan.GroupBy(input, keys, aggs, having, maintainOrder) ->
            let unobserved =
                match need with
                | OrderNeed.Ordered -> false
                | OrderNeed.Unordered -> true
                | OrderNeed.SortedBy sortColumns ->
                    let names = keys |> List.map keyName
                    not keys.IsEmpty
                    && names |> List.forall Option.isSome
                    && names |> List.forall (fun n -> sortColumns.Contains n.Value)
            let keepsOrder = maintainOrder && not unobserved
            let inputNeed =
                if not keepsOrder && aggs |> List.forall (snd >> isOrderFreeAggregate false) then
                    OrderNeed.Unordered
                else
                    OrderNeed.Ordered
            LogicalPlan.GroupBy(relax inputNeed input, keys, aggs, having, keepsOrder)

        // Nothing downstream observes the order this sort produces.
        | LogicalPlan.Sort(input, _) when need = OrderNeed.Unordered -> relax OrderNeed.Unordered input

        | LogicalPlan.Sort(input, orderings) ->
            let sortColumns =
                orderings
                |> List.choose (fun o ->
                    match o.Expr with
                    | ScalarExpr.Column name -> Some name
                    | _ -> None)
                |> Set.ofList
            // Rows the sort leaves tied keep their input order, observed among rows equal on the consumer's columns too.
            let observed =
                match need with
                | OrderNeed.SortedBy consumerColumns -> Set.union consumerColumns sortColumns
                | _ -> sortColumns
            let inputNeed = if run.AllowUnstableSort then OrderNeed.Unordered else OrderNeed.SortedBy observed
            LogicalPlan.Sort(relax inputNeed input, orderings)

        | LogicalPlan.Filter(input, predicate) ->
            let inputNeed = if readsOrder predicate then OrderNeed.Ordered else need
            LogicalPlan.Filter(relax inputNeed input, predicate)

        // A single row of order-free aggregates: the input's order is not observed.
        | LogicalPlan.Project(input, projections) when
            not projections.IsEmpty && projections |> List.forall (snd >> isOrderFreeAggregate false)
            ->
            LogicalPlan.Project(relax OrderNeed.Unordered input, projections)

        | LogicalPlan.Project(input, projections) ->
            LogicalPlan.Project(relax (throughProjection need projections false) input, projections)

        | LogicalPlan.WithColumns(input, columns) ->
            LogicalPlan.WithColumns(relax (throughProjection need columns true) input, columns)

        | LogicalPlan.Rename(input, existing, renamed) ->
            let inputNeed =
                match need with
                | OrderNeed.SortedBy sortColumns ->
                    let back = List.zip renamed existing |> Map.ofList
                    sortColumns |> Set.map (fun c -> back.TryFind c |> Option.defaultValue c) |> OrderNeed.SortedBy
                | other -> other
            LogicalPlan.Rename(relax inputNeed input, existing, renamed)

        // The result is the left rows in their order; the right side only decides which of them survive.
        | LogicalPlan.Join(left, right, leftOn, rightOn, (JoinKind.Semi | JoinKind.Anti as how), suffix, coalescing, nullsEqual) ->
            LogicalPlan.Join(relax need left, relax OrderNeed.Unordered right, leftOn, rightOn, how, suffix, coalescing, nullsEqual)

        | LogicalPlan.Union(left, right, isAll) when need = OrderNeed.Unordered ->
            LogicalPlan.Union(relax OrderNeed.Unordered left, relax OrderNeed.Unordered right, isAll)

        | LogicalPlan.GroupingSets(input, spec) when
            need = OrderNeed.Unordered && spec.Aggs |> List.forall (snd >> isOrderFreeAggregate false)
            ->
            LogicalPlan.GroupingSets(relax OrderNeed.Unordered input, spec)

        // Every occurrence gets the same need, so all stay equal: free only when no consumer observes the order.
        | LogicalPlan.Shared(id, input) ->
            let unobserved = need = OrderNeed.Unordered
            match run.SharedUnobserved.TryGetValue id with
            | true, soFar -> run.SharedUnobserved.[id] <- soFar && unobserved
            | _ -> run.SharedUnobserved.[id] <- unobserved
            let bodyNeed = if run.FreeShared.Contains id then OrderNeed.Unordered else OrderNeed.Ordered
            LogicalPlan.Shared(id, relax bodyNeed input)

        // Nothing observes the join's order, so neither its inputs' order.
        | LogicalPlan.Join(left, right, leftOn, rightOn, how, suffix, coalescing, nullsEqual) when need = OrderNeed.Unordered ->
            let join =
                LogicalPlan.Join(relax OrderNeed.Unordered left, relax OrderNeed.Unordered right, leftOn, rightOn, how, suffix, coalescing, nullsEqual)
            run.UnorderedJoins.Add join |> ignore
            join

        | LogicalPlan.Distinct(input, subset, keep, maintainOrder) when need = OrderNeed.Unordered ->
            let keptRowIgnoresOrder =
                subset.IsNone
                || (match keep with
                    | DistinctKeep.Any
                    | DistinctKeep.None -> true
                    | DistinctKeep.First
                    | DistinctKeep.Last -> false)
            let inputNeed = if keptRowIgnoresOrder then OrderNeed.Unordered else OrderNeed.Ordered
            LogicalPlan.Distinct(relax inputNeed input, subset, keep, false)

        | other -> LogicalPlanOps.mapInputs (relax OrderNeed.Ordered) other

    /// Relaxes the order guarantees that the plan's consumers cannot observe. The plan's own rows are observed
    /// in order (a single row of order-free aggregates aside).
    /// Runs until the shared subplans known to be free stop growing: freeing one can free another nested in it.
    let private relaxToFixpoint (allowUnstableSort: bool) (plan: LogicalPlan) =
        let rec loop (free: Set<int>) =
            let run =
                {
                    UnorderedJoins = HashSet<LogicalPlan>(HashIdentity.Reference)
                    AllowUnstableSort = allowUnstableSort
                    FreeShared = free
                    SharedUnobserved = Dictionary<int, bool>()
                }
            let result = relax run OrderNeed.Ordered plan
            let nowFree =
                run.SharedUnobserved |> Seq.filter (fun kv -> kv.Value) |> Seq.map (fun kv -> kv.Key) |> Set.ofSeq
            if nowFree = free then result, run.UnorderedJoins else loop nowFree
        loop Set.empty

    let apply (plan: LogicalPlan) : LogicalPlan = relaxToFixpoint false plan |> fst

    /// `apply`, and the join nodes of its result whose output order nothing observes. With `allowUnstableSort` the
    /// plan's sorts leave ties unspecified, so their inputs' order is not observed.
    let applyCollectingUnorderedJoins (allowUnstableSort: bool) (plan: LogicalPlan) : LogicalPlan * HashSet<LogicalPlan> =
        relaxToFixpoint allowUnstableSort plan
