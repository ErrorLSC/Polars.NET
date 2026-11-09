namespace Kaminari.Samidare.IR.Passes

open Kaminari.Samidare.IR

/// Turns a left join kept only for its unmatched rows into an anti join, and joins a filtering table before it.
///
/// `f LEFT JOIN r ON f.k = r.k WHERE r.k IS NULL` keeps the rows of `f` without a match: a matched row has a non-null
/// `r.k`, as its key equals `f.k` and null keys do not match. An anti join says the same without building the joined
/// rows. It needs nothing above to read `r`'s columns, which are then all null.
///
/// `(f ANTI r) JOIN d ON f.a = d.a` then equals `(f JOIN d ON f.a = d.a) ANTI r`, with the same rows in the same order:
/// an anti join only keeps or drops rows of its left input, by columns that the inner join leaves alone. Done when `d`
/// is filtered, so that the anti join probes only the rows the filter keeps.
module internal AntiJoinRecognition =

    let private columnsOf (plan: LogicalPlan) = Set.ofList (SchemaInference.inferPlanSchema plan).ColumnNames

    let rec private isFilteredDimension (plan: LogicalPlan) =
        match plan with
        | LogicalPlan.Shared(_, input) -> isFilteredDimension input
        | other -> SemiJoinReduction.isFilteredTable other

    /// `Join(Anti, filtered)` with the inner join beneath the anti join, or the join as it was.
    let private sinkFilteringJoin (join: LogicalPlan) =
        match join with
        | LogicalPlan.Join(
            LogicalPlan.Join(left, excluded, antiLeft, antiRight, JoinKind.Anti, antiSuffix, antiCoalesce, antiNullsEqual),
            dimension,
            ([ ScalarExpr.Column leftKey ] as leftOn),
            rightOn,
            JoinKind.Inner,
            suffix,
            coalesce,
            nullsEqual) when isFilteredDimension dimension && (columnsOf left).Contains leftKey ->
            let inner = LogicalPlan.Join(left, dimension, leftOn, rightOn, JoinKind.Inner, suffix, coalesce, nullsEqual)
            LogicalPlan.Join(inner, excluded, antiLeft, antiRight, JoinKind.Anti, antiSuffix, antiCoalesce, antiNullsEqual)
        | other -> other

    /// Rewrites the first left join kept for its unmatched rows along the plan's left spine of filters and inner joins.
    /// `read` holds the columns that the nodes above use.
    let rec private recognize (read: Set<string>) (node: LogicalPlan) : LogicalPlan option =
        match node with
        | LogicalPlan.Filter(
            (LogicalPlan.Join(left, right, leftOn, rightOn, JoinKind.Left, _, _, false) as join),
            ScalarExpr.Unary(UnaryOp.IsNull, ScalarExpr.Column tested)) ->
            let leftColumns = columnsOf left
            let added = Set.difference (columnsOf join) leftColumns
            if rightOn |> List.contains (ScalarExpr.Column tested) && added.Contains tested && Set.isEmpty (Set.intersect added read) then
                Some(LogicalPlan.Join(left, right, leftOn, rightOn, JoinKind.Anti, None, JoinKeyCoalescing.Default, false))
            else
                None
        | LogicalPlan.Filter(input, predicate) when not (ScalarExprOps.containsSubquery predicate) ->
            recognize (Set.union read (ScalarExprOps.collectColumns predicate)) input
            |> Option.map (fun rewritten -> LogicalPlan.Filter(rewritten, predicate))
        | LogicalPlan.Join(left, right, leftOn, rightOn, JoinKind.Inner, suffix, coalesce, nullsEqual) ->
            let usedHere = leftOn |> List.map ScalarExprOps.collectColumns |> Set.unionMany
            recognize (Set.union read usedHere) left
            |> Option.map (fun rewritten ->
                sinkFilteringJoin (LogicalPlan.Join(rewritten, right, leftOn, rightOn, JoinKind.Inner, suffix, coalesce, nullsEqual)))
        | _ -> None

    /// A subquery's outer references are not visible as columns, so an expression holding one blocks the rewrite.
    let private recognizeBeneath (expressions: ScalarExpr list) (input: LogicalPlan) =
        if expressions |> List.exists ScalarExprOps.containsSubquery then
            None
        else
            recognize (expressions |> List.map ScalarExprOps.collectColumns |> Set.unionMany) input

    let rec private rewrite (node: LogicalPlan) : LogicalPlan =
        match LogicalPlanOps.mapInputs rewrite node with
        | LogicalPlan.GroupBy(input, keys, aggs, having, maintainOrder) as grouped ->
            let expressions = keys @ List.map snd aggs @ Option.toList having
            match recognizeBeneath expressions input with
            | Some rewritten -> LogicalPlan.GroupBy(rewritten, keys, aggs, having, maintainOrder)
            | None -> grouped
        | LogicalPlan.Project(input, columns) as projected ->
            match recognizeBeneath (List.map snd columns) input with
            | Some rewritten -> LogicalPlan.Project(rewritten, columns)
            | None -> projected
        | LogicalPlan.Join _ as join -> sinkFilteringJoin join
        | other -> other

    let apply (plan: LogicalPlan) : LogicalPlan = rewrite plan
