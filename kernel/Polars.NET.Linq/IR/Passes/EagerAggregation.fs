namespace Polars.NET.Linq.IR.Passes

open System.Collections.Generic
open Apache.Arrow.Types
open Polars.NET.Linq.IR

/// Aggregates a fact table before joining the dimension that holds the grouping columns.
///
/// `SELECT d.a, SUM(f.v) FROM f JOIN d ON f.k = d.k GROUP BY d.a` carries every fact row, with the dimension's
/// columns attached, into a grouping on those columns. Grouping on the join key first leaves at most one row per key
/// to join and to group again:
/// `SELECT d.a, SUM(p.v) FROM (SELECT k, SUM(v) AS v FROM f GROUP BY k) p JOIN d ON p.k = d.k GROUP BY d.a`.
/// A Distinct over dimension columns is the same rewrite with no aggregates.
///
/// It applies to a star of inner equi-joins, in which one dimension supplies every grouping column and every join key
/// is a column of the fact table. That dimension moves above the other joins, which is safe because they only read
/// fact columns; a row's matches in it keep their order, so groups keep their order of first appearance.
///
/// Partial aggregates stay correct however many dimension rows a key matches, since each is then summed that many
/// times, as the original rows would be. Only aggregates that split exactly are rewritten:
/// - Sum over integers and decimals. Floating sums round differently in another order.
/// - Min, Max, Count and Len.
/// - The mean of integers read as doubles, from a partial sum and count.
/// - A sum guarded by a test on dimension columns (see `decompose`).
/// - Not CheckedSum: a partial total can leave the type's range when the full total does not.
///
/// When two dimensions supply the grouping columns, the fact table can still be aggregated before the unfiltered one, on
/// its join key and the other dimension's column: `GROUP BY c.name, d.year` first groups on `(f.customer, d.year)`, which
/// merges many rows when a customer buys often in a year. Nothing tells how many rows that merges; see
/// `deferrableKeyDimension` for what keeps the extra grouping cheap when it merges few. The dimension may come first in
/// the query, `(c JOIN f) JOIN d`, and then stays on the left.
///
/// A ROLLUP or other grouping sets are rewritten the same way, each level reading the pre-aggregated rows.
///
/// There are no key statistics, so the rewrite needs the fact table to outnumber the dimension, whose size bounds the
/// join key's values. The other joins' estimates are not used: they rest on fixed selectivities, and when they do
/// remove rows the grouping is cheap either way.
module internal EagerAggregation =

    /// How many times more rows than the dimension the fact table must have.
    let private minReduction = 4.0

    exception private NotDecomposable

    type private Step =
        | JoinStep of right: LogicalPlan * leftKey: string * rightKey: string * suffix: string option * coalescing: JoinKeyCoalescing
        | FilterStep of predicate: ScalarExpr

    /// Through projections of plain columns: the source of each output column, and the plan beneath them.
    let rec private peelProjects (mapping: Map<string, string>) (plan: LogicalPlan) : Map<string, string> * LogicalPlan =
        match plan with
        | LogicalPlan.Project(input, columns) ->
            let plain =
                columns
                |> List.map (fun (name, expr) ->
                    match expr with
                    | ScalarExpr.Column source -> Some(name, source)
                    | _ -> None)
            if plain |> List.forall Option.isSome then
                let lower = plain |> List.map Option.get |> Map.ofList
                let composed =
                    if mapping.IsEmpty then
                        lower
                    else
                        mapping |> Map.map (fun _ source -> lower.TryFind source |> Option.defaultValue source)
                peelProjects composed input
            else
                mapping, plan
        | _ -> mapping, plan

    let rec private flatten (plan: LogicalPlan) : LogicalPlan * Step list =
        match plan with
        | LogicalPlan.Join(left, right, [ ScalarExpr.Column leftKey ], [ ScalarExpr.Column rightKey ], JoinKind.Inner, suffix, coalescing, false) ->
            let fact, steps = flatten left
            fact, steps @ [ JoinStep(right, leftKey, rightKey, suffix, coalescing) ]
        | LogicalPlan.Filter(input, predicate) when not (ScalarExprOps.dependsOnOtherRows predicate) ->
            let fact, steps = flatten input
            fact, steps @ [ FilterStep predicate ]
        | other -> other, []

    let private rebuild (fact: LogicalPlan) (steps: Step list) : LogicalPlan =
        steps
        |> List.fold
            (fun input step ->
                match step with
                | JoinStep(right, leftKey, rightKey, suffix, coalescing) ->
                    LogicalPlan.Join(input, right, [ ScalarExpr.Column leftKey ], [ ScalarExpr.Column rightKey ], JoinKind.Inner, suffix, coalescing, false)
                | FilterStep predicate -> LogicalPlan.Filter(input, predicate))
            fact

    let private columnsOf (plan: LogicalPlan) = (SchemaInference.inferPlanSchema plan).ColumnNames

    /// A join of the star: right input, fact key, right key, suffix and key coalescing.
    type private StarJoin = LogicalPlan * string * string * string option * JoinKeyCoalescing

    /// A whole table: nothing beneath the plan removes rows.
    let rec private isWholeTable (plan: LogicalPlan) =
        match plan with
        | LogicalPlan.Scan _ -> true
        | LogicalPlan.Shared(_, input) -> isWholeTable input
        | LogicalPlan.Project(input, columns) ->
            columns
            |> List.forall (fun (_, expr) ->
                match expr with
                | ScalarExpr.Column _ -> true
                | _ -> false)
            && isWholeTable input
        | _ -> false

    let private isFixedWidth (plan: LogicalPlan) (column: string) =
        match SchemaInference.inferScalarType (SchemaInference.inferPlanSchema plan) (ScalarExpr.Column column) with
        | LogicalType.Arrow(:? FixedWidthType) -> true
        | _ -> false

    /// The one dimension that supplies every key column.
    let private soleKeyDimension (keyColumns: Set<string>) (joins: StarJoin list) : StarJoin list =
        joins |> List.filter (fun (right, _, _, _, _) -> Set.isSubset keyColumns (Set.ofList (columnsOf right)))

    /// The dimension to join after pre-aggregating when two dimensions supply the key columns: the only one that is a
    /// whole table, as a filtered one removes rows first. Empty when a key column belongs to the fact table. There are
    /// no statistics to tell how many rows the pre-aggregation merges, so it must be cheap: it groups on the join key
    /// and one fixed-width column, and the deferred dimension holds a key that is not fixed-width.
    let private deferrableKeyDimension (keyColumns: Set<string>) (joins: StarJoin list) : StarJoin list =
        let keysOf (right: LogicalPlan) = Set.intersect keyColumns (Set.ofList (columnsOf right))
        match joins |> List.filter (fun (right, _, _, _, _) -> not (Set.isEmpty (keysOf right))) with
        | [ (first, _, _, _, _) as a; (second, _, _, _, _) as b ] when Set.union (keysOf first) (keysOf second) = keyColumns ->
            let deferredOver (deferred: LogicalPlan) (carrier: LogicalPlan) =
                isWholeTable deferred
                && not (isWholeTable carrier)
                && keysOf deferred |> Set.exists (isFixedWidth deferred >> not)
                && (match Set.toList (keysOf carrier) with
                    | [ column ] -> isFixedWidth carrier column
                    | _ -> false)
            if deferredOver first second then [ a ]
            elif deferredOver second first then [ b ]
            else []
        | _ -> []

    /// The plan without the dimension `choose` picks among its joins: that dimension's join and its columns.
    let private isolateDimension (estimate: LogicalPlan -> float * float) (choose: StarJoin list -> StarJoin list) (plan: LogicalPlan) =
        let fact, steps = flatten plan
        let factColumns = columnsOf fact |> Set.ofList
        let joins =
            steps
            |> List.choose (function
                | JoinStep(right, leftKey, rightKey, suffix, coalescing) -> Some(right, leftKey, rightKey, suffix, coalescing)
                | FilterStep _ -> None)
        let everyColumn = columnsOf fact @ (joins |> List.collect (fun (right, _, _, _, _) -> columnsOf right))
        let isStar = joins |> List.forall (fun (_, leftKey, _, _, _) -> factColumns.Contains leftKey)
        if not isStar || (List.distinct everyColumn).Length <> everyColumn.Length then
            None
        else
            match choose joins with
            | [ (dimension, leftKey, rightKey, suffix, coalescing) ] ->
                let dimensionColumns = Set.ofList (columnsOf dimension)
                let filtersReadDimension =
                    steps
                    |> List.exists (function
                        | FilterStep predicate -> not (Set.isEmpty (Set.intersect (ScalarExprOps.collectColumns predicate) dimensionColumns))
                        | JoinStep _ -> false)
                let rest =
                    steps
                    |> List.filter (function
                        | JoinStep(right, _, _, _, _) -> not (obj.ReferenceEquals(right, dimension))
                        | FilterStep _ -> true)
                    |> rebuild fact
                if filtersReadDimension || fst (estimate fact) < minReduction * fst (estimate dimension) then
                    None
                else
                    Some(rest, dimension, leftKey, rightKey, suffix, coalescing, factColumns)
            | _ -> None

    /// As isolateDimension, for a star written with a dimension first: `(c JOIN f) JOIN d`, the dimension `c` being the
    /// one that deferrableKeyDimension picks.
    let private isolateLeadingDimension (estimate: LogicalPlan -> float * float) (keyColumns: Set<string>) (plan: LogicalPlan) =
        match flatten plan with
        | dimension, JoinStep(fact, dimensionKey, factKey, suffix, coalescing) :: steps when
            isWholeTable dimension && steps |> List.forall (function JoinStep _ -> true | FilterStep _ -> false)
            ->
            let factColumns = columnsOf fact |> Set.ofList
            let others =
                steps
                |> List.choose (function
                    | JoinStep(right, leftKey, rightKey, joinSuffix, joinCoalescing) -> Some(right, leftKey, rightKey, joinSuffix, joinCoalescing)
                    | FilterStep _ -> None)
            let joins: StarJoin list = (dimension, factKey, dimensionKey, suffix, coalescing) :: others
            let isStar = others |> List.forall (fun (_, leftKey, _, _, _) -> factColumns.Contains leftKey)
            let everyColumn = columnsOf fact @ (joins |> List.collect (fun (right, _, _, _, _) -> columnsOf right))
            let dimensionChosen =
                match deferrableKeyDimension keyColumns joins with
                | [ (chosen, _, _, _, _) ] -> obj.ReferenceEquals(chosen, dimension)
                | _ -> false
            if isStar && dimensionChosen && (List.distinct everyColumn).Length = everyColumn.Length && fst (estimate fact) >= minReduction * fst (estimate dimension) then
                Some(rebuild fact steps, dimension, factKey, dimensionKey, suffix, coalescing, factColumns)
            else
                None
        | _ -> None

    /// As isolateDimension, for the dimension that holds every key, when another dimension comes first in the query:
    /// `((d JOIN f) JOIN s)` with the keys in `s`. The first join stays beneath the aggregation.
    let private isolateBehindLeadingDimension (estimate: LogicalPlan -> float * float) (keyColumns: Set<string>) (plan: LogicalPlan) =
        let table, allSteps = flatten plan
        let isFilterStep step =
            match step with
            | FilterStep _ -> true
            | JoinStep _ -> false
        // The leading dimension's own filters are part of it.
        let leading = rebuild table (List.takeWhile isFilterStep allSteps)
        match List.skipWhile isFilterStep allSteps with
        | (JoinStep(fact, _, _, _, _) as first) :: steps when not (List.exists isFilterStep steps) ->
            let factColumns = columnsOf fact |> Set.ofList
            let joins: StarJoin list =
                steps
                |> List.choose (function
                    | JoinStep(right, leftKey, rightKey, suffix, coalescing) -> Some(right, leftKey, rightKey, suffix, coalescing)
                    | FilterStep _ -> None)
            let everyColumn = columnsOf leading @ columnsOf fact @ (joins |> List.collect (fun (right, _, _, _, _) -> columnsOf right))
            let isStar = joins |> List.forall (fun (_, leftKey, _, _, _) -> factColumns.Contains leftKey)
            match soleKeyDimension keyColumns joins with
            | [ (dimension, leftKey, rightKey, suffix, coalescing) ] when
                isStar
                && (List.distinct everyColumn).Length = everyColumn.Length
                && fst (estimate fact) >= minReduction * fst (estimate dimension)
                ->
                let remaining =
                    steps
                    |> List.filter (function
                        | JoinStep(right, _, _, _, _) -> not (obj.ReferenceEquals(right, dimension))
                        | FilterStep _ -> true)
                Some(rebuild leading (first :: remaining), dimension, leftKey, rightKey, suffix, coalescing, factColumns)
            | _ -> None
        | _ -> None

    /// Integers and decimals: their sums do not depend on the order of the additions.
    let isExactNumber (schema: LogicalSchema) (expr: ScalarExpr) =
        match SchemaInference.inferScalarType schema expr with
        | LogicalType.Arrow(:? Int8Type | :? Int16Type | :? Int32Type | :? Int64Type | :? UInt8Type | :? UInt16Type | :? UInt32Type | :? UInt64Type | :? Decimal128Type) ->
            true
        | _ -> false

    let private isInteger (schema: LogicalSchema) (expr: ScalarExpr) =
        match SchemaInference.inferScalarType schema expr with
        | LogicalType.Arrow(:? Int8Type | :? Int16Type | :? Int32Type | :? Int64Type | :? UInt8Type | :? UInt16Type | :? UInt32Type | :? UInt64Type) -> true
        | _ -> false

    /// The expression over partial results, adding the partial aggregates it needs to `partials`.
    /// A sum guarded by a test on dimension columns is summed unguarded, and the test applied to the partial sums after
    /// the join: `SUM(CASE WHEN d.day = 'Sun' THEN f.v END)` needs the test once per partial sum rather than once per fact
    /// row. A tested column that is present beneath the aggregation is added to `testColumns`, to group on as well.
    let private decompose (input: LogicalPlan) (factColumns: Set<string>) (partials: ResizeArray<string * ScalarExpr>) (testColumns: HashSet<string>) (guarded: bool ref) (expr: ScalarExpr) : ScalarExpr =
        let schema = SchemaInference.inferPlanSchema input
        let columns = Set.ofList schema.ColumnNames
        let partialOf (aggregate: ScalarExpr) =
            match partials |> Seq.tryFind (fun (_, existing) -> existing = aggregate) with
            | Some(name, _) -> name
            | None ->
                let name = $"__ea{partials.Count}"
                partials.Add(name, aggregate)
                name
        let readable (argument: ScalarExpr) =
            not (ScalarExprOps.dependsOnOtherRows argument) && Set.isSubset (ScalarExprOps.collectColumns argument) columns
        let rec final (node: ScalarExpr) =
            match node with
            | ScalarExpr.Literal _ -> node
            | ScalarExpr.Alias(inner, alias) -> ScalarExpr.Alias(final inner, alias)
            | ScalarExpr.Cast(inner, target, strict) -> ScalarExpr.Cast(final inner, target, strict)
            | ScalarExpr.Unary(op, inner) -> ScalarExpr.Unary(op, final inner)
            | ScalarExpr.Binary(op, left, right) -> ScalarExpr.Binary(op, final left, final right)
            | ScalarExpr.Conditional(test, ifTrue, ifFalse) -> ScalarExpr.Conditional(final test, final ifTrue, final ifFalse)
            | ScalarExpr.Coalesce items -> ScalarExpr.Coalesce(items |> List.map final)
            | ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Conditional(test, value, (ScalarExpr.Literal(LiteralValue.Null _) as none))), false) when
                readable value
                && isExactNumber schema value
                && not (ScalarExprOps.dependsOnOtherRows test)
                && not (ScalarExprOps.containsAggregation test)
                && Set.isEmpty (Set.intersect (ScalarExprOps.collectColumns test) factColumns)
                ->
                guarded.Value <- true
                for column in ScalarExprOps.collectColumns test do
                    if columns.Contains column then testColumns.Add column |> ignore
                let partial = partialOf (ScalarExpr.Aggregation(AggKind.Sum, Some value, false))
                ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Conditional(test, ScalarExpr.Column partial, none)), false)
            | ScalarExpr.Aggregation(AggKind.Sum, Some argument, false) when readable argument && isExactNumber schema argument ->
                ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Column(partialOf node)), false)
            // The mean of integers as doubles: the total of the partial sums over the total count, null without values.
            | ScalarExpr.Aggregation(AggKind.Mean, Some(ScalarExpr.Cast(argument, (LogicalType.Arrow(:? DoubleType) as double), _) as asDouble), false) when
                readable argument && isInteger schema argument
                ->
                let sumOf name = ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Column name), false)
                let total = sumOf (partialOf (ScalarExpr.Aggregation(AggKind.Sum, Some asDouble, false)))
                let count = sumOf (partialOf (ScalarExpr.Aggregation(AggKind.Count, Some argument, false)))
                ScalarExpr.Conditional(
                    ScalarExpr.Binary(BinaryOp.Eq, ScalarExpr.Cast(count, LogicalTypes.Int64, false), ScalarExpr.Literal(LiteralValue.Int64 0L)),
                    ScalarExpr.Literal(LiteralValue.Null double),
                    ScalarExpr.Binary(BinaryOp.Divide, total, ScalarExpr.Cast(count, double, false))
                )
            | ScalarExpr.Aggregation((AggKind.Min | AggKind.Max as kind), Some argument, false) when readable argument ->
                ScalarExpr.Aggregation(kind, Some(ScalarExpr.Column(partialOf node)), false)
            | ScalarExpr.Aggregation(AggKind.Len, None, false) ->
                ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Column(partialOf node)), false)
            | ScalarExpr.Aggregation(AggKind.Count, Some argument, false) when readable argument ->
                ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Column(partialOf node)), false)
            | _ -> raise NotDecomposable
        final expr

    let apply (sources: SourceBinding array) (plan: LogicalPlan) : LogicalPlan =
        let estimate = JoinReordering.estimate (JoinReordering.sourceRowCounts sources)
        let source (mapping: Map<string, string>) name = mapping.TryFind name |> Option.defaultValue name

        /// The join of the pre-aggregated rows with the dimension, the aggregates over them, and the keys by source
        /// column, for a grouping of `input` on `keys` (output name, expression read from `input`).
        let tryPreAggregate input (keys: (string * string) list option) aggs maintainOrder =
            let mapping, chain = peelProjects Map.empty input
            keys
            |> Option.bind (fun keys ->
                let namedKeys = keys |> List.map (fun (name, column) -> name, source mapping column)
                let keyColumns = namedKeys |> List.map snd |> Set.ofList
                let isolated =
                    if keyColumns.IsEmpty then
                        None
                    else
                        isolateDimension estimate (soleKeyDimension keyColumns) chain
                        |> Option.map (fun parts -> parts, false, false)
                        |> Option.orElseWith (fun () ->
                            isolateDimension estimate (deferrableKeyDimension keyColumns) chain |> Option.map (fun parts -> parts, false, false))
                        |> Option.orElseWith (fun () ->
                            isolateLeadingDimension estimate keyColumns chain |> Option.map (fun parts -> parts, true, false))
                        |> Option.orElseWith (fun () ->
                            isolateBehindLeadingDimension estimate keyColumns chain |> Option.map (fun parts -> parts, false, true))
                isolated
                |> Option.bind (fun ((rest, dimension, leftKey, rightKey, suffix, coalescing, factColumns), dimensionFirst, needsGuardedSum) ->
                    let toSource = ScalarExprOps.substitute (fun name -> mapping.TryFind name |> Option.map ScalarExpr.Column)
                    let partials = ResizeArray()
                    let testColumns = HashSet<string>()
                    let guarded = ref false
                    try
                        let finalAggs = aggs |> List.map (fun (name, expr) -> name, decompose rest factColumns partials testColumns guarded (toSource expr))
                        if partials.Count = 0 || (needsGuardedSum && not guarded.Value) then
                            None
                        else
                            let carried = Set.difference keyColumns (Set.ofList (columnsOf dimension)) |> Set.union (Set.ofSeq testColumns) |> Set.toList
                            let preKeys = leftKey :: List.filter ((<>) leftKey) carried |> List.map ScalarExpr.Column
                            let pre = LogicalPlan.GroupBy(rest, preKeys, List.ofSeq partials, None, maintainOrder)
                            let joined =
                                if dimensionFirst then
                                    LogicalPlan.Join(dimension, pre, [ ScalarExpr.Column rightKey ], [ ScalarExpr.Column leftKey ], JoinKind.Inner, suffix, coalescing, false)
                                else
                                    LogicalPlan.Join(pre, dimension, [ ScalarExpr.Column leftKey ], [ ScalarExpr.Column rightKey ], JoinKind.Inner, suffix, coalescing, false)
                            Some(namedKeys, joined, finalAggs)
                    with NotDecomposable ->
                        None))

        let plainKeys (keys: (string * ScalarExpr) list) =
            let named =
                keys
                |> List.map (function
                    | name, ScalarExpr.Column column -> Some(name, column)
                    | _ -> None)
            if named |> List.forall Option.isSome then Some(List.map Option.get named) else None

        let tryGroupBy input keys aggs maintainOrder =
            let named =
                keys
                |> List.map (function
                    | ScalarExpr.Column name -> name, ScalarExpr.Column name
                    | ScalarExpr.Alias(ScalarExpr.Column name, alias) -> alias, ScalarExpr.Column name
                    | other -> "", other)
            tryPreAggregate input (plainKeys named) aggs maintainOrder
            |> Option.map (fun (namedKeys, joined, finalAggs) ->
                let finalKeys =
                    namedKeys
                    |> List.map (fun (name, column) ->
                        if name = column then ScalarExpr.Column column else ScalarExpr.Alias(ScalarExpr.Column column, name))
                LogicalPlan.GroupBy(joined, finalKeys, finalAggs, None, maintainOrder))

        let tryGroupingSets input (spec: LogicalGroupingSetsSpec) =
            tryPreAggregate input (plainKeys spec.KeyAliases) spec.Aggs true
            |> Option.map (fun (namedKeys, joined, finalAggs) ->
                let keys = namedKeys |> List.map (fun (name, column) -> name, ScalarExpr.Column column)
                LogicalPlan.GroupingSets(joined, { spec with KeyAliases = keys; Aggs = finalAggs }))

        let tryDistinct input keep maintainOrder =
            let mapping, chain = peelProjects Map.empty input
            if mapping.IsEmpty then
                None
            else
                let keyColumns = mapping |> Map.toList |> List.map snd |> Set.ofList
                (if keyColumns.IsEmpty then None else isolateDimension estimate (soleKeyDimension keyColumns) chain)
                |> Option.map (fun (rest, dimension, leftKey, rightKey, suffix, coalescing, _) ->
                    let pre = LogicalPlan.Distinct(LogicalPlan.Project(rest, [ leftKey, ScalarExpr.Column leftKey ]), None, keep, maintainOrder)
                    let joined = LogicalPlan.Join(pre, dimension, [ ScalarExpr.Column leftKey ], [ ScalarExpr.Column rightKey ], JoinKind.Inner, suffix, coalescing, false)
                    let names = columnsOf input
                    let projected = LogicalPlan.Project(joined, names |> List.map (fun name -> name, ScalarExpr.Column(source mapping name)))
                    LogicalPlan.Distinct(projected, None, keep, maintainOrder))

        let rec rewrite (node: LogicalPlan) : LogicalPlan =
            match LogicalPlanOps.mapInputs rewrite node with
            | LogicalPlan.GroupBy(input, keys, aggs, None, maintainOrder) as grouped ->
                tryGroupBy input keys aggs maintainOrder |> Option.defaultValue grouped
            | LogicalPlan.Distinct(input, None, keep, maintainOrder) as distinct ->
                tryDistinct input keep maintainOrder |> Option.defaultValue distinct
            | LogicalPlan.GroupingSets(input, spec) as grouping when spec.LevelMerges.IsNone ->
                tryGroupingSets input spec |> Option.defaultValue grouping
            | other -> other

        rewrite plan
