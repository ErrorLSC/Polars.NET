namespace Polars.NET.Linq.IR.Passes

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

    /// The need below a projection-like node: sort columns are traced to the columns they copy; a column
    /// computed by the node does not identify input rows, so it is dropped from the set.
    let private throughProjection (need: OrderNeed) (columns: (string * ScalarExpr) list) (passThrough: bool) =
        if columns |> List.exists (snd >> readsOrder) then
            OrderNeed.Ordered
        else
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

    let rec private relax (need: OrderNeed) (plan: LogicalPlan) : LogicalPlan =
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
            LogicalPlan.Sort(relax (OrderNeed.SortedBy sortColumns) input, orderings)

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
    let apply (plan: LogicalPlan) : LogicalPlan = relax OrderNeed.Ordered plan
