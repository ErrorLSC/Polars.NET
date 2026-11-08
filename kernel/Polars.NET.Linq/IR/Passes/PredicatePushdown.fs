namespace Polars.NET.Linq.IR.Passes

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

    /// Whether an expression reads only the row it is evaluated on, so its value does not change when other rows
    /// are removed.
    let private isRowLocal (expr: ScalarExpr) =
        not (
            ScalarExprOps.containsAggregation expr
            || ScalarExprOps.containsWindow expr
            || ScalarExprOps.containsSubquery expr
            || ScalarExprOps.dependsOnOtherRows expr
        )

    /// Attempts to push a single conjunct predicate down into a target plan.
    let rec pushdownConjunct (pred: ScalarExpr) (plan: LogicalPlan) : LogicalPlan =
        match pred with
        // Trivial true: no-op, eliminate
        | ScalarExpr.Literal(LiteralValue.Bool true) -> plan

        // Trivial false: no rows, but the plan's columns, which the stages above read.
        | ScalarExpr.Literal(LiteralValue.Bool false) -> LogicalPlan.Limit(plan, 0L, 0u)

        // A predicate reading other rows would see fewer of them beneath this point.
        | _ when not (isRowLocal pred) -> LogicalPlan.Filter(plan, pred)

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
                // σ_p(π_rank(R)) ≠ π_rank(σ_p(R)): a projection whose values depend on other rows sees a
                // different input once the filter moves below it.
                elif projs |> List.exists (fun (_, e) -> ScalarExprOps.dependsOnOtherRows e) then
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
                | JoinKind.Inner
                | JoinKind.Cross ->
                    if not refCols.IsEmpty && Set.isSubset refCols leftCols then
                        let pushedLeft = pushdownConjunct pred left
                        LogicalPlan.Join(pushedLeft, right, lOn, rOn, how, suffix, coalesce, nullEq)
                    elif not refCols.IsEmpty && Set.isSubset refCols rightCols && (Set.intersect refCols leftCols).IsEmpty then
                        let pushedRight = pushdownConjunct pred right
                        LogicalPlan.Join(left, pushedRight, lOn, rOn, how, suffix, coalesce, nullEq)
                    else
                        LogicalPlan.Filter(plan, pred)

                // The result is left rows only (Semi, Anti), or every left row at least once (Left).
                | JoinKind.Left
                | JoinKind.Semi
                | JoinKind.Anti ->
                    if not refCols.IsEmpty && Set.isSubset refCols leftCols then
                        let pushedLeft = pushdownConjunct pred left
                        LogicalPlan.Join(pushedLeft, right, lOn, rOn, how, suffix, coalesce, nullEq)
                    else
                        LogicalPlan.Filter(plan, pred)

                | _ ->
                    LogicalPlan.Filter(plan, pred)

            // 5. Past another filter, unless that filter reads other rows, and only to go deeper: swapping two
            //    filters at the same depth would flip their order on every run, and subplans that the optimizer
            //    ran over a different number of times would stop being equal.
            | LogicalPlan.Filter(input, other) when isRowLocal other ->
                match pushdownConjunct pred input with
                | LogicalPlan.Filter(unchanged, kept) when obj.ReferenceEquals(unchanged, input) && kept = pred ->
                    LogicalPlan.Filter(plan, pred)
                | pushed -> LogicalPlan.Filter(pushed, other)

            // 6. Past a stable sort.
            | LogicalPlan.Sort(input, orderings) ->
                LogicalPlan.Sort(pushdownConjunct pred input, orderings)

            // 7. Past a distinct, when rows it treats as duplicates agree on the predicate.
            | LogicalPlan.Distinct(input, subset, keep, maintainOrder) when
                subset |> Option.forall (fun columns -> Set.isSubset refCols (Set.ofList columns))
                ->
                LogicalPlan.Distinct(pushdownConjunct pred input, subset, keep, maintainOrder)

            // 8. Past added columns it does not read, when they do not read other rows.
            | LogicalPlan.WithColumns(input, columns) when
                not refCols.IsEmpty
                && columns |> List.forall (snd >> isRowLocal)
                && columns |> List.forall (fun (name, _) -> not (refCols.Contains name))
                ->
                LogicalPlan.WithColumns(pushdownConjunct pred input, columns)

            // 9. Past a grouping, when it reads only group keys: it keeps or drops whole groups.
            | LogicalPlan.GroupBy(input, keys, aggs, having, maintainOrder) when not keys.IsEmpty ->
                let keyExprs =
                    keys
                    |> List.choose (function
                        | ScalarExpr.Column name as key -> Some(name, key)
                        | ScalarExpr.Alias(key, name) -> Some(name, key)
                        | _ -> None)
                    |> Map.ofList

                if not refCols.IsEmpty && refCols |> Set.forall keyExprs.ContainsKey then
                    let onInput = ScalarExprOps.substitute (fun column -> keyExprs.TryFind column) pred
                    LogicalPlan.GroupBy(pushdownConjunct onInput input, keys, aggs, having, maintainOrder)
                else
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

