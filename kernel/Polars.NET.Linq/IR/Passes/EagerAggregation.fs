namespace Polars.NET.Linq.IR.Passes

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
/// - Not CheckedSum: a partial total can leave the type's range when the full total does not.
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

    /// The plan without the dimension that holds `keyColumns`, that dimension's join and its columns.
    let private isolateDimension (estimate: LogicalPlan -> float * float) (keyColumns: Set<string>) (plan: LogicalPlan) =
        let fact, steps = flatten plan
        let factColumns = columnsOf fact |> Set.ofList
        let joins =
            steps
            |> List.choose (function
                | JoinStep(right, leftKey, rightKey, suffix, coalescing) -> Some(right, leftKey, rightKey, suffix, coalescing)
                | FilterStep _ -> None)
        let everyColumn = columnsOf fact @ (joins |> List.collect (fun (right, _, _, _, _) -> columnsOf right))
        let isStar = joins |> List.forall (fun (_, leftKey, _, _, _) -> factColumns.Contains leftKey)
        if keyColumns.IsEmpty || not isStar || (List.distinct everyColumn).Length <> everyColumn.Length then
            None
        else
            match joins |> List.filter (fun (right, _, _, _, _) -> Set.isSubset keyColumns (Set.ofList (columnsOf right))) with
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
                    Some(rest, dimension, leftKey, rightKey, suffix, coalescing)
            | _ -> None

    /// Integers and decimals: their sums do not depend on the order of the additions.
    let isExactNumber (schema: LogicalSchema) (expr: ScalarExpr) =
        match SchemaInference.inferScalarType schema expr with
        | LogicalType.Arrow(:? Int8Type | :? Int16Type | :? Int32Type | :? Int64Type | :? UInt8Type | :? UInt16Type | :? UInt32Type | :? UInt64Type | :? Decimal128Type) ->
            true
        | _ -> false

    /// The expression over partial results, adding the partial aggregates it needs to `partials`.
    let private decompose (input: LogicalPlan) (partials: ResizeArray<string * ScalarExpr>) (expr: ScalarExpr) : ScalarExpr =
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
            | ScalarExpr.Aggregation(AggKind.Sum, Some argument, false) when readable argument && isExactNumber schema argument ->
                ScalarExpr.Aggregation(AggKind.Sum, Some(ScalarExpr.Column(partialOf node)), false)
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

        let tryGroupBy input keys aggs maintainOrder =
            let mapping, chain = peelProjects Map.empty input
            let namedKeys =
                keys
                |> List.map (function
                    | ScalarExpr.Column name -> Some(name, source mapping name)
                    | ScalarExpr.Alias(ScalarExpr.Column name, alias) -> Some(alias, source mapping name)
                    | _ -> None)
            if namedKeys |> List.exists Option.isNone then
                None
            else
                let namedKeys = namedKeys |> List.map Option.get
                let keyColumns = namedKeys |> List.map snd |> Set.ofList
                isolateDimension estimate keyColumns chain
                |> Option.bind (fun (rest, dimension, leftKey, rightKey, suffix, coalescing) ->
                    let toSource = ScalarExprOps.substitute (fun name -> mapping.TryFind name |> Option.map ScalarExpr.Column)
                    let partials = ResizeArray()
                    try
                        let finalAggs = aggs |> List.map (fun (name, expr) -> name, decompose rest partials (toSource expr))
                        if partials.Count = 0 then
                            None
                        else
                            let pre = LogicalPlan.GroupBy(rest, [ ScalarExpr.Column leftKey ], List.ofSeq partials, None, maintainOrder)
                            let joined = LogicalPlan.Join(pre, dimension, [ ScalarExpr.Column leftKey ], [ ScalarExpr.Column rightKey ], JoinKind.Inner, suffix, coalescing, false)
                            let finalKeys =
                                namedKeys
                                |> List.map (fun (name, column) ->
                                    if name = column then ScalarExpr.Column column else ScalarExpr.Alias(ScalarExpr.Column column, name))
                            Some(LogicalPlan.GroupBy(joined, finalKeys, finalAggs, None, maintainOrder))
                    with NotDecomposable ->
                        None)

        let tryDistinct input keep maintainOrder =
            let mapping, chain = peelProjects Map.empty input
            if mapping.IsEmpty then
                None
            else
                let keyColumns = mapping |> Map.toList |> List.map snd |> Set.ofList
                isolateDimension estimate keyColumns chain
                |> Option.map (fun (rest, dimension, leftKey, rightKey, suffix, coalescing) ->
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
            | other -> other

        rewrite plan
