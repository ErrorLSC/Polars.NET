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
/// table keeps a small share of its rows, and the reduction passes a grouping or an inner join. A column bounded from
/// both sides counts as keeping a small share, whatever the fixed selectivities say: they treat the bounds as independent,
/// when a range such as a month of dates is far narrower than either bound. A wide range makes the semi join a modest
/// waste, as the probing costs little next to the joins it precedes.
///
/// A join on several columns gets a reduction of its own. Matching a large table on composite keys costs far more per
/// row than a semi join on one of those columns, so a table is first filtered by the values one key can take. They come
/// from the dimensions on the other side, following that key through joins between dimensions: in
/// `(f ⋈ σ(d1) ⋈ d2 ON d1.w = d2.w) ⋈ t ON (f.i, d2.k) = (t.i, t.k)` the column `d2.k` only takes the values of
/// `σ(d1) ⋈ d2`. The semi join costs little even when it removes few rows, so only the relative sizes decide.
module internal SemiJoinReduction =

    /// The largest estimated share of its table's rows that a selective table may keep. A single equality is estimated
    /// at 10% and says little (TPC-DS Q36's `s_state = 'TN'` keeps every store), so it takes more evidence than that.
    let private maxSelectivity = 0.05

    /// How many times more rows a table must have than the key set that filters it, for a join on several columns.
    let private minKeySetReduction = 100.0

    /// Whether the plan reads a table through row-local filters and plain renames, and if so whether it filters.
    let private tableRead (plan: LogicalPlan) =
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
        reads plan

    let isFilteredTable (plan: LogicalPlan) = tableRead plan = Some true

    /// Whether the plan's filters give some column both a lower and an upper bound that are literals.
    let private hasTwoSidedRange (plan: LogicalPlan) =
        let isLiteral (expr: ScalarExpr) =
            match expr with
            | ScalarExpr.Literal _
            | ScalarExpr.Cast(ScalarExpr.Literal _, _, _) -> true
            | _ -> false
        let rec conjuncts (predicate: ScalarExpr) =
            match predicate with
            | ScalarExpr.Binary(BinaryOp.And, left, right) -> conjuncts left @ conjuncts right
            | other -> [ other ]
        let rec predicates (node: LogicalPlan) =
            match node with
            | LogicalPlan.Filter(input, predicate) -> conjuncts predicate @ predicates input
            | _ -> []
        let bounds =
            predicates plan
            |> List.choose (function
                | ScalarExpr.Binary((BinaryOp.Gt | BinaryOp.GtEq), ScalarExpr.Column name, bound)
                | ScalarExpr.Binary((BinaryOp.Lt | BinaryOp.LtEq), bound, ScalarExpr.Column name) when isLiteral bound -> Some(name, true)
                | ScalarExpr.Binary((BinaryOp.Lt | BinaryOp.LtEq), ScalarExpr.Column name, bound)
                | ScalarExpr.Binary((BinaryOp.Gt | BinaryOp.GtEq), bound, ScalarExpr.Column name) when isLiteral bound -> Some(name, false)
                | _ -> None)
        bounds |> List.exists (fun (name, isLower) -> isLower && List.contains (name, false) bounds)

    let private columnsOf (plan: LogicalPlan) = Set.ofList (SchemaInference.inferPlanSchema plan).ColumnNames

    /// A relation over dimensions holding every value `key` takes in the rows of `node`, and the column holding them.
    /// A dimension without a filter of its own still narrows the key when it joins another dimension that has one.
    let rec private keySource (key: string) (node: LogicalPlan) : (LogicalPlan * string) option =
        match node with
        | LogicalPlan.Filter(input, predicate) when not (ScalarExprOps.dependsOnOtherRows predicate) -> keySource key input
        | LogicalPlan.Rename(input, existing, renamed) ->
            let original =
                List.zip renamed existing |> List.tryPick (fun (n, e) -> if n = key then Some e else None) |> Option.defaultValue key
            keySource original input
        | LogicalPlan.Project(input, columns) when columns |> List.forall (snd >> ScalarExprOps.dependsOnOtherRows >> not) ->
            match columns |> List.tryFind (fst >> (=) key) with
            | Some(_, ScalarExpr.Column source) -> keySource source input
            | _ -> None
        | LogicalPlan.WithColumns(input, columns) when
            columns |> List.forall (fun (name, expr) -> name <> key && not (ScalarExprOps.dependsOnOtherRows expr))
            ->
            keySource key input
        | LogicalPlan.Join(left, right, leftOn, rightOn, how, _, _, false) ->
            let leftColumns, rightColumns = columnsOf left, columnsOf right
            match how with
            | JoinKind.Inner when rightColumns.Contains key && not (leftColumns.Contains key) && (tableRead right).IsSome ->
                let throughLeft =
                    List.zip leftOn rightOn
                    |> List.tryPick (function
                        | ScalarExpr.Column leftKey, ScalarExpr.Column rightKey ->
                            keySource leftKey left |> Option.map (fun (relation, column) -> relation, column, rightKey)
                        | _ -> None)
                match throughLeft with
                | Some(relation, column, rightKey) when rightKey = key -> Some(relation, column)
                | Some(relation, column, rightKey) when Set.isEmpty (Set.intersect (columnsOf relation) rightColumns) ->
                    let joined =
                        LogicalPlan.Join(
                            relation,
                            right,
                            [ ScalarExpr.Column column ],
                            [ ScalarExpr.Column rightKey ],
                            JoinKind.Inner,
                            None,
                            JoinKeyCoalescing.Default,
                            false
                        )
                    Some(joined, key)
                | _ -> if isFilteredTable right then Some(right, key) else None
            | JoinKind.Inner
            | JoinKind.Left
            | JoinKind.Semi
            | JoinKind.Anti when leftColumns.Contains key && not (rightColumns.Contains key) -> keySource key left
            | _ -> None
        | _ -> None

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
                    (if rows <= tableRows * maxSelectivity || hasTwoSidedRange right then reduce right rightKey worthIt leftKey left else None)
                with
                | Some reduced when worthIt.Value ->
                    LogicalPlan.Join(reduced, right, [ ScalarExpr.Column leftKey ], [ ScalarExpr.Column rightKey ], how, suffix, coalescing, false)
                | _ -> join
            | LogicalPlan.Join(left, right, leftOn, rightOn, (JoinKind.Inner | JoinKind.Semi as how), suffix, coalescing, false) as join when
                leftOn.Length >= 2 && (tableRead right).IsSome
                ->
                let tableRows = fst (estimate right)
                let candidates =
                    List.zip leftOn rightOn
                    |> List.choose (function
                        | ScalarExpr.Column leftKey, ScalarExpr.Column rightKey ->
                            keySource leftKey left
                            |> Option.map (fun (relation, column) -> fst (estimate relation), relation, column, rightKey)
                        | _ -> None)
                    |> List.filter (fun (rows, _, _, _) -> tableRows >= minKeySetReduction * rows)
                match candidates with
                | [] -> join
                | _ ->
                    let _, relation, column, rightKey = candidates |> List.minBy (fun (rows, _, _, _) -> rows)
                    let reduced =
                        LogicalPlan.Join(
                            right,
                            relation,
                            [ ScalarExpr.Column rightKey ],
                            [ ScalarExpr.Column column ],
                            JoinKind.Semi,
                            None,
                            JoinKeyCoalescing.Default,
                            false
                        )
                    LogicalPlan.Join(left, reduced, leftOn, rightOn, how, suffix, coalescing, false)
            | other -> other

        rewrite plan
