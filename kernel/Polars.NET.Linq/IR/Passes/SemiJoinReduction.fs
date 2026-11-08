namespace Polars.NET.Linq.IR.Passes

open Polars.NET.Linq.IR

/// Filters the tables beneath a join by the keys of a selective table the join reads last.
///
/// `(ss ⋈ sr ⋈ cs) ⋈ σ(item)` computes the expensive joins for every item before the filter on item removes most of
/// them. When the join key can be followed down to a table, a semi join with the selective table filters that table
/// first:
/// - Through an inner join to both sides, since its key equality makes the other side's key the same value.
/// - Through a grouping on the key: it keeps or drops whole groups.
/// - Through row-local filters and projections, and to the left of left, semi and anti joins.
///
/// The join stays, so the result only loses rows the join would remove. A semi join keeps its left order, so this does
/// not depend on the order of the rows being free, unlike join reordering. It runs after join reordering: a selective
/// table already joined first leaves nothing to reduce.
///
/// A reduction is made only when it pays for reading the selective table again and for probing the whole table: the
/// table keeps a small share of its rows, and the reduction passes a grouping or an inner join.
module internal SemiJoinReduction =

    /// The largest estimated share of its table's rows that a selective table may keep. A single equality is estimated
    /// at 10% and says little (TPC-DS Q36's `s_state = 'TN'` keeps every store), so it takes more evidence than that.
    let private maxSelectivity = 0.05

    /// A table read through row-local filters and plain renames, with at least one filter.
    let private isFilteredTable (plan: LogicalPlan) =
        let isPlainColumn (expr: ScalarExpr) =
            match expr with
            | ScalarExpr.Column _ -> true
            | _ -> false
        let rec reads (node: LogicalPlan) =
            match node with
            | LogicalPlan.Scan _ -> Some false
            | LogicalPlan.Filter(input, predicate) when not (ScalarExprOps.dependsOnOtherRows predicate) ->
                reads input |> Option.map (fun _ -> true)
            | LogicalPlan.Rename(input, _, _) -> reads input
            | LogicalPlan.Project(input, columns) when columns |> List.forall (snd >> isPlainColumn) -> reads input
            | _ -> None
        reads plan = Some true

    let apply (sources: SourceBinding array) (plan: LogicalPlan) : LogicalPlan =
        let estimate = JoinReordering.estimate (JoinReordering.sourceRowCounts sources)

        /// `node` with its tables filtered by the keys in `selective.[selectiveKey]`, following column `key`, or None
        /// if the column reaches no table.
        let rec reduce (selective: LogicalPlan) (selectiveKey: string) (worthIt: bool ref) (key: string) (node: LogicalPlan) : LogicalPlan option =
            let recurse = reduce selective selectiveKey worthIt
            match node with
            | LogicalPlan.Scan _
            | LogicalPlan.Filter(LogicalPlan.Scan _, _) ->
                Some(
                    LogicalPlan.Join(
                        node,
                        selective,
                        [ ScalarExpr.Column key ],
                        [ ScalarExpr.Column selectiveKey ],
                        JoinKind.Semi,
                        None,
                        JoinKeyCoalescing.Default,
                        false
                    )
                )

            | LogicalPlan.Filter(input, predicate) when not (ScalarExprOps.dependsOnOtherRows predicate) ->
                recurse key input |> Option.map (fun reduced -> LogicalPlan.Filter(reduced, predicate))

            | LogicalPlan.Rename(input, existing, renamed) ->
                let original =
                    List.zip renamed existing |> List.tryPick (fun (n, e) -> if n = key then Some e else None) |> Option.defaultValue key
                recurse original input |> Option.map (fun reduced -> LogicalPlan.Rename(reduced, existing, renamed))

            | LogicalPlan.Project(input, columns) when columns |> List.forall (snd >> ScalarExprOps.dependsOnOtherRows >> not) ->
                match columns |> List.tryFind (fst >> (=) key) with
                | Some(_, ScalarExpr.Column source) ->
                    recurse source input |> Option.map (fun reduced -> LogicalPlan.Project(reduced, columns))
                | _ -> None

            | LogicalPlan.WithColumns(input, columns) when
                columns |> List.forall (fun (name, expr) -> name <> key && not (ScalarExprOps.dependsOnOtherRows expr))
                ->
                recurse key input |> Option.map (fun reduced -> LogicalPlan.WithColumns(reduced, columns))

            | LogicalPlan.GroupBy(input, keys, aggregations, having, maintainOrder) ->
                keys
                |> List.tryPick (function
                    | ScalarExpr.Column name when name = key -> Some name
                    | ScalarExpr.Alias(ScalarExpr.Column name, alias) when alias = key -> Some name
                    | _ -> None)
                |> Option.bind (fun source -> recurse source input)
                |> Option.map (fun reduced ->
                    worthIt.Value <- true
                    LogicalPlan.GroupBy(reduced, keys, aggregations, having, maintainOrder))

            | LogicalPlan.Join(left, right, leftOn, rightOn, how, suffix, coalescing, nullsEqual) ->
                let rebuild l r = LogicalPlan.Join(l, r, leftOn, rightOn, how, suffix, coalescing, nullsEqual)
                let leftHas = (SchemaInference.inferPlanSchema left).ColumnNames |> List.contains key
                match how with
                | JoinKind.Inner ->
                    let rightHas = not leftHas && ((SchemaInference.inferPlanSchema right).ColumnNames |> List.contains key)
                    // The same value on the other side: only join keys that reject nulls say so for every kept row.
                    let partner (ownKeys: ScalarExpr list) (otherKeys: ScalarExpr list) =
                        if nullsEqual then
                            None
                        else
                            List.zip ownKeys otherKeys
                            |> List.tryPick (function
                                | ScalarExpr.Column own, ScalarExpr.Column other when own = key -> Some other
                                | _ -> None)
                    let leftKey = if leftHas then Some key elif rightHas then partner rightOn leftOn else None
                    let rightKey = if rightHas then Some key elif leftHas then partner leftOn rightOn else None
                    let reducedLeft = leftKey |> Option.bind (fun k -> recurse k left)
                    let reducedRight = rightKey |> Option.bind (fun k -> recurse k right)
                    if reducedLeft.IsNone && reducedRight.IsNone then
                        None
                    else
                        worthIt.Value <- true
                        Some(rebuild (defaultArg reducedLeft left) (defaultArg reducedRight right))
                | JoinKind.Left
                | JoinKind.Semi
                | JoinKind.Anti when leftHas -> recurse key left |> Option.map (fun reduced -> rebuild reduced right)
                | _ -> None

            | _ -> None

        let rec rewrite (node: LogicalPlan) : LogicalPlan =
            match LogicalPlanOps.mapInputs rewrite node with
            | LogicalPlan.Join(left, right, [ ScalarExpr.Column leftKey ], [ ScalarExpr.Column rightKey ], (JoinKind.Inner | JoinKind.Semi as how), suffix, coalescing, false) as join when
                isFilteredTable right
                ->
                let rows, tableRows = estimate right
                let worthIt = ref false
                match
                    (if rows <= tableRows * maxSelectivity then reduce right rightKey worthIt leftKey left else None)
                with
                | Some reduced when worthIt.Value ->
                    LogicalPlan.Join(reduced, right, [ ScalarExpr.Column leftKey ], [ ScalarExpr.Column rightKey ], how, suffix, coalescing, false)
                | _ -> join
            | other -> other

        rewrite plan
