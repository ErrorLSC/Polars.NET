namespace Kaminari.Samidare.IR.Passes

open System
open Kaminari.Samidare.IR

/// Replaces subqueries (`ScalarExpr.Subquery`, SEMANTICS.md "Subqueries") by joins. It runs on a frontend's plan
/// before the optimizer, which never sees a subquery.
///
/// A subquery's plan is first stripped of its correlation: the conjuncts of its filters that read the subquery's
/// OuterColumns are pulled up to its root, through filters and row-local projections, and sorted into equi keys
/// (`inner = outer` with `Eq` or `EqMissing`), conditions on the outer row alone, and theta conditions (both rows).
/// Then:
/// - A filter conjunct that is `[Not] Exists` or `[Not] In` (positive, or with `nullsEqual`) with equi keys and
///   nothing else becomes a semi or anti join.
/// - Every other subquery is attached to the outer rows as a column by a left join, which keeps every outer row
///   once and in order. Its aggregates are grouped by the equi keys (with theta conditions, by the outer row's
///   index after joining the rows), unmatched rows get each aggregate's value over no rows, and the subquery's own
///   expression over them (scaling, a count check, ...) is evaluated above the join, where the enclosing expression
///   reads it unchanged. A condition on the outer row alone selects between that value and its value over no rows.
///   Exists is a count above zero; In is an Exists with its target as one more key.
/// - Subqueries of one node over the same rows and correlation share one grouping and one join.
/// Anything else (correlation under a limit, sort, grouping, join or window; an OuterColumn in a projection;
/// correlation with another subquery's row; a row-valued correlated scalar; null-aware NOT IN) is reported as
/// unsupported, never guessed: the frontend then evaluates that part otherwise.
module internal Decorrelation =

    type Options =
        {
            /// A fresh column name with the given hint, unique within the compilation and the same on every
            /// compilation of the same query.
            Fresh: string -> string
            /// Whether the backend can raise (AssertCount): a scalar subquery that is not an aggregate needs it to
            /// fail on several rows.
            SupportsAssertions: bool
        }

    exception private Unsupported of string

    let private unsupported (message: string) = raise (Unsupported message)

    // =========================================================================
    // Expression helpers
    // =========================================================================

    /// Rebuilds an expression with each immediate sub-expression mapped (a subquery's plan is kept).
    let private mapChildren (f: ScalarExpr -> ScalarExpr) (expr: ScalarExpr) : ScalarExpr =
        let orders (os: SortOrder list) = os |> List.map (fun o -> { o with Expr = f o.Expr })
        match expr with
        | ScalarExpr.Column _
        | ScalarExpr.Literal _
        | ScalarExpr.OuterColumn _ -> expr
        | ScalarExpr.Binary(op, l, r) -> ScalarExpr.Binary(op, f l, f r)
        | ScalarExpr.Unary(op, e) -> ScalarExpr.Unary(op, f e)
        | ScalarExpr.Cast(e, t, strict) -> ScalarExpr.Cast(f e, t, strict)
        | ScalarExpr.Function(fn, args, t) -> ScalarExpr.Function(fn, List.map f args, t)
        | ScalarExpr.Aggregation(kind, e, distinct) -> ScalarExpr.Aggregation(kind, Option.map f e, distinct)
        | ScalarExpr.Window(e, partitionBy, orderBy) -> ScalarExpr.Window(f e, List.map f partitionBy, orders orderBy)
        | ScalarExpr.SortBy(e, by) -> ScalarExpr.SortBy(f e, orders by)
        | ScalarExpr.Conditional(t, a, b) -> ScalarExpr.Conditional(f t, f a, f b)
        | ScalarExpr.Coalesce es -> ScalarExpr.Coalesce(List.map f es)
        | ScalarExpr.ItemAccess(t, i) -> ScalarExpr.ItemAccess(f t, f i)
        | ScalarExpr.Slice(t, offset, length) -> ScalarExpr.Slice(f t, offset, length)
        | ScalarExpr.Subquery(scope, plan, kind) ->
            match kind with
            | SubqueryKind.In(target, nullsEqual) -> ScalarExpr.Subquery(scope, plan, SubqueryKind.In(f target, nullsEqual))
            | SubqueryKind.Scalar
            | SubqueryKind.Exists -> expr
        | ScalarExpr.Alias(e, name) -> ScalarExpr.Alias(f e, name)
        | ScalarExpr.Udf(name, args, t, invoker) -> ScalarExpr.Udf(name, List.map f args, t, invoker)
        | ScalarExpr.InList(e, source, negated, nullsEqual) -> ScalarExpr.InList(f e, source, negated, nullsEqual)
        | ScalarExpr.EnumConvert(conversion, e, mapping) -> ScalarExpr.EnumConvert(conversion, f e, mapping)

    let rec private conjuncts (expr: ScalarExpr) : ScalarExpr list =
        match expr with
        | ScalarExpr.Binary(BinaryOp.And, l, r) -> conjuncts l @ conjuncts r
        | other -> [ other ]

    let private conjoin (exprs: ScalarExpr list) : ScalarExpr option =
        match exprs with
        | [] -> None
        | first :: rest -> Some(rest |> List.fold (fun acc e -> ScalarExpr.Binary(BinaryOp.And, acc, e)) first)

    /// Whether the expression reads an OuterColumn of `scope`, also inside the plans of the subqueries it embeds.
    let rec private reads (scope: int) (expr: ScalarExpr) : bool =
        match expr with
        | ScalarExpr.OuterColumn(s, _, _) -> s = scope
        | ScalarExpr.Subquery(_, plan, _) when not (LogicalPlanOps.outerColumns scope plan).IsEmpty -> true
        | _ -> ScalarExprOps.children expr |> List.exists (reads scope)

    /// Whether the expression reads any OuterColumn (or embeds a subquery, which this pass has not replaced).
    let rec private readsAnyOuter (expr: ScalarExpr) : bool =
        match expr with
        | ScalarExpr.OuterColumn _
        | ScalarExpr.Subquery _ -> true
        | _ -> ScalarExprOps.children expr |> List.exists readsAnyOuter

    /// The OuterColumns of `scope` as columns of the holding row.
    let rec private outerAsColumns (scope: int) (expr: ScalarExpr) : ScalarExpr =
        match expr with
        | ScalarExpr.OuterColumn(s, name, _) when s = scope -> ScalarExpr.Column name
        | _ -> mapChildren (outerAsColumns scope) expr

    let rec private collectAggregations (expr: ScalarExpr) : ScalarExpr list =
        match expr with
        | ScalarExpr.Aggregation _ -> [ expr ]
        | _ -> ScalarExprOps.children expr |> List.collect collectAggregations

    let rec private replaceAggregations (replace: ScalarExpr -> ScalarExpr) (expr: ScalarExpr) : ScalarExpr =
        match expr with
        | ScalarExpr.Aggregation _ -> replace expr
        | _ -> mapChildren (replaceAggregations replace) expr

    let rec private replaceSubqueries (replace: int -> ScalarExpr option) (expr: ScalarExpr) : ScalarExpr =
        match expr with
        | ScalarExpr.Subquery(scope, _, _) ->
            match replace scope with
            | Some replacement -> replacement
            | None -> expr
        | _ -> mapChildren (replaceSubqueries replace) expr

    let private zeroOf (t: LogicalType) : ScalarExpr =
        match t with
        | LogicalType.Arrow arrow ->
            match arrow with
            | :? Apache.Arrow.Types.Int8Type -> ScalarExpr.Literal(LiteralValue.Int8 0y)
            | :? Apache.Arrow.Types.Int16Type -> ScalarExpr.Literal(LiteralValue.Int16 0s)
            | :? Apache.Arrow.Types.Int32Type -> ScalarExpr.Literal(LiteralValue.Int32 0)
            | :? Apache.Arrow.Types.Int64Type -> ScalarExpr.Literal(LiteralValue.Int64 0L)
            | :? Apache.Arrow.Types.UInt8Type -> ScalarExpr.Literal(LiteralValue.UInt8 0uy)
            | :? Apache.Arrow.Types.UInt16Type -> ScalarExpr.Literal(LiteralValue.UInt16 0us)
            | :? Apache.Arrow.Types.UInt32Type -> ScalarExpr.Literal(LiteralValue.UInt32 0u)
            | :? Apache.Arrow.Types.UInt64Type -> ScalarExpr.Literal(LiteralValue.UInt64 0UL)
            | :? Apache.Arrow.Types.HalfFloatType -> ScalarExpr.Literal(LiteralValue.Float16 Half.Zero)
            | :? Apache.Arrow.Types.FloatType -> ScalarExpr.Literal(LiteralValue.Float32 0.0f)
            | :? Apache.Arrow.Types.DoubleType -> ScalarExpr.Literal(LiteralValue.Float64 0.0)
            | :? Apache.Arrow.Types.Decimal128Type
            | :? Apache.Arrow.Types.Decimal256Type -> ScalarExpr.Literal(LiteralValue.Decimal 0m)
            | _ -> ScalarExpr.Literal(LiteralValue.Int32 0)
        | _ -> ScalarExpr.Literal(LiteralValue.Int32 0)

    /// An aggregate's value over no rows, where it is not null (SEMANTICS.md, "Aggregates skip nulls").
    let private emptyValue (aggregation: ScalarExpr) (aggregateType: LogicalType) : ScalarExpr option =
        match aggregation with
        | ScalarExpr.Aggregation((AggKind.Len | AggKind.Count), _, _) -> Some(ScalarExpr.Literal(LiteralValue.UInt32 0u))
        | ScalarExpr.Aggregation((AggKind.Sum | AggKind.CheckedSum), _, _) -> Some(zeroOf aggregateType)
        | ScalarExpr.Aggregation(AggKind.List, _, _) ->
            let element =
                match aggregateType with
                | LogicalType.ListOf element -> element
                | other -> other
            Some(ScalarExpr.Literal(LiteralValue.EmptyList element))
        | _ -> None

    // =========================================================================
    // Pulling the correlation up
    // =========================================================================

    /// The plan without the conjuncts that read `scope`, and those conjuncts (over the returned plan's columns).
    /// Filters commute with them; a row-local projection keeps the columns they read under fresh names.
    let rec private pull (opts: Options) (scope: int) (plan: LogicalPlan) : LogicalPlan * ScalarExpr list =
        if (LogicalPlanOps.outerColumns scope plan).IsEmpty then
            plan, []
        else
            let keepColumns (pulled: ScalarExpr list) =
                let needed = pulled |> List.map ScalarExprOps.collectColumns |> Set.unionMany |> Set.toList
                let renames = needed |> List.map (fun c -> c, opts.Fresh "corr")
                renames |> List.map (fun (c, fresh) -> fresh, ScalarExpr.Column c),
                pulled |> List.map (ScalarExprOps.renameColumns (Map renames))
            let rowLocal (exprs: ScalarExpr list) =
                not (exprs |> List.exists (fun e -> reads scope e || ScalarExprOps.dependsOnOtherRows e))
            match plan with
            | LogicalPlan.Filter(input, predicate) ->
                let input', pulled = pull opts scope input
                let correlated, own = conjuncts predicate |> List.partition (reads scope)
                let filtered =
                    match conjoin own with
                    | Some p -> LogicalPlan.Filter(input', p)
                    | None -> input'
                filtered, pulled @ correlated
            | LogicalPlan.Project(input, projections) when rowLocal (List.map snd projections) ->
                let input', pulled = pull opts scope input
                let kept, pulled' = keepColumns pulled
                LogicalPlan.Project(input', projections @ kept), pulled'
            | LogicalPlan.WithColumns(input, columns) when rowLocal (List.map snd columns) ->
                let input', pulled = pull opts scope input
                let kept, pulled' = keepColumns pulled
                LogicalPlan.WithColumns(input', columns @ kept), pulled'
            | _ ->
                unsupported "the subquery correlates with its outer row under an operator other than a filter or a row-local projection"

    // =========================================================================
    // Analysis of one subquery
    // =========================================================================

    /// One subquery reduced to aggregates over its uncorrelated rows.
    type private Analysis =
        {
            Scope: int
            /// The subquery's rows without the correlation (columns of the subquery's plan).
            Rows: LogicalPlan
            /// The correlated conjuncts pulled out of the rows (OuterColumns still in place), plus an In's
            /// membership key; subqueries with equal Rows and Correlation share a grouping.
            Correlation: ScalarExpr list
            /// The aggregates the value reads (over Rows' columns).
            Aggregates: ScalarExpr list
            /// The value over the aggregates (Aggregation nodes stand for the aggregates' columns).
            Value: ScalarExpr
        }

    /// A row-valued uncorrelated scalar subquery: attached as a one-row column after a row-count check.
    type private RowScalar = { Scope: int; Plan: LogicalPlan }

    [<RequireQualifiedAccess>]
    type private Reduced =
        | Aggregated of Analysis
        | RowScalar of RowScalar

    let rec private stripDistinct (plan: LogicalPlan) =
        match plan with
        | LogicalPlan.Distinct(input, None, _, _) -> stripDistinct input
        | other -> other

    let private singleColumn (plan: LogicalPlan) : string =
        match (SchemaInference.inferPlanSchema plan).ColumnNames with
        | [ name ] -> name
        | names -> unsupported (sprintf "a subquery whose value is read has %d columns" names.Length)

    let private isAggregating (projections: (string * ScalarExpr) list) =
        not projections.IsEmpty && projections |> List.forall (snd >> ScalarExprOps.containsAggregation)

    let private analyze (opts: Options) (scope: int) (plan: LogicalPlan) (kind: SubqueryKind) : Reduced =
        let count = ScalarExpr.Aggregation(AggKind.Len, None, false)
        let positiveCount = ScalarExpr.Binary(BinaryOp.Gt, count, ScalarExpr.Literal(LiteralValue.UInt32 0u))
        let correlated = not (LogicalPlanOps.outerColumns scope plan).IsEmpty
        match kind with
        | SubqueryKind.Scalar ->
            match plan with
            | LogicalPlan.Project(input, [ (_, value) ]) when isAggregating [ ("", value) ] && not (reads scope value) ->
                let rows, pulled = pull opts scope input
                Reduced.Aggregated
                    {
                        Scope = scope
                        Rows = rows
                        Correlation = pulled
                        Aggregates = collectAggregations value
                        Value = value
                    }
            | _ when not correlated -> Reduced.RowScalar { Scope = scope; Plan = plan }
            | _ -> unsupported "a correlated scalar subquery that is not an aggregate"
        | SubqueryKind.Exists ->
            let rec rowsOf (p: LogicalPlan) =
                match p with
                | LogicalPlan.Distinct(input, _, _, _) -> rowsOf input
                | LogicalPlan.Project(input, projections) when not (isAggregating projections) -> rowsOf input
                | other -> other
            let rows, pulled = pull opts scope (rowsOf plan)
            Reduced.Aggregated
                {
                    Scope = scope
                    Rows = rows
                    Correlation = pulled
                    Aggregates = [ count ]
                    Value = positiveCount
                }
        | SubqueryKind.In(target, nullsEqual) ->
            let input, value =
                match stripDistinct plan with
                | LogicalPlan.Project(input, [ (_, value) ]) when not (ScalarExprOps.containsAggregation value) -> input, value
                | other -> other, ScalarExpr.Column(singleColumn other)
            if reads scope value then
                unsupported "an IN subquery whose value reads its outer row"
            let rows, pulled = pull opts scope input
            // The membership is one more equi key: the value against the target (an expression of the outer row).
            let membership =
                ScalarExpr.Binary((if nullsEqual then BinaryOp.EqMissing else BinaryOp.Eq), value, ScalarExprOps.substitute (fun c -> Some(ScalarExpr.OuterColumn(scope, c, LogicalType.Unknown))) target)
            Reduced.Aggregated
                {
                    Scope = scope
                    Rows = rows
                    Correlation = pulled @ [ membership ]
                    Aggregates = [ count ]
                    Value = positiveCount
                }

    // =========================================================================
    // Classifying the correlation
    // =========================================================================

    type private Correlation =
        {
            /// (inner expression over the rows, outer expression over the holding row, whether nulls match)
            Keys: (ScalarExpr * ScalarExpr * bool) list
            /// Conditions on the holding row alone.
            OuterOnly: ScalarExpr list
            /// Conditions on both rows: inner columns as they are, OuterColumns still in place.
            Theta: ScalarExpr list
        }

    let private classify (scope: int) (pulled: ScalarExpr list) : Correlation =
        let inner (e: ScalarExpr) = not (ScalarExprOps.collectColumns e).IsEmpty
        let outerSide (e: ScalarExpr) = reads scope e && not (inner e)
        let innerSide (e: ScalarExpr) = inner e && not (reads scope e)
        for p in pulled do
            if readsAnyOuter (outerAsColumns scope p) then
                unsupported "the subquery correlates with the row of another subquery"
        let keys, rest =
            pulled
            |> List.map (fun p ->
                match p with
                | ScalarExpr.Binary((BinaryOp.Eq | BinaryOp.EqMissing) as op, a, b) when innerSide a && outerSide b ->
                    Choice1Of2(a, outerAsColumns scope b, (op = BinaryOp.EqMissing))
                | ScalarExpr.Binary((BinaryOp.Eq | BinaryOp.EqMissing) as op, a, b) when outerSide a && innerSide b ->
                    Choice1Of2(b, outerAsColumns scope a, (op = BinaryOp.EqMissing))
                | other -> Choice2Of2 other)
            |> List.partition (function Choice1Of2 _ -> true | Choice2Of2 _ -> false)
        let rest = rest |> List.map (function Choice2Of2 e -> e | Choice1Of2 _ -> failwith "unreachable")
        let outerOnly, theta = rest |> List.partition (inner >> not)
        {
            Keys = keys |> List.map (function Choice1Of2 k -> k | Choice2Of2 _ -> failwith "unreachable")
            OuterOnly = outerOnly |> List.map (outerAsColumns scope)
            Theta = theta
        }

    /// One nullsEqual flag for a join on several keys: when the keys disagree, nulls match and the keys whose nulls
    /// must not match lose their null rows (on the inner side, which also keeps a null outer key from matching).
    let private joinNulls (keys: (ScalarExpr * ScalarExpr * bool) list) : bool * ScalarExpr list =
        match keys |> List.map (fun (_, _, n) -> n) |> List.distinct with
        | [ single ] -> single, []
        | [] -> false, []
        | _ ->
            true,
            keys
            |> List.choose (fun (innerKey, _, nullsEqual) ->
                if nullsEqual then None else Some(ScalarExpr.Unary(UnaryOp.IsNotNull, innerKey)))

    // =========================================================================
    // Building the joins
    // =========================================================================

    let private columnsOf (plan: LogicalPlan) = (SchemaInference.inferPlanSchema plan).ColumnNames

    let private keepOnly (plan: LogicalPlan) (columns: string list) =
        LogicalPlan.Project(plan, columns |> List.map (fun c -> c, ScalarExpr.Column c))

    let private constantKey (opts: Options) (outer: LogicalPlan) (oneRow: LogicalPlan) : LogicalPlan =
        let key = opts.Fresh "key"
        let one = ScalarExpr.Literal(LiteralValue.Int32 1)
        LogicalPlan.Join(
            LogicalPlan.WithColumns(outer, [ (key, one) ]),
            LogicalPlan.WithColumns(oneRow, [ (key, one) ]),
            [ ScalarExpr.Column key ],
            [ ScalarExpr.Column key ],
            JoinKind.Left,
            None,
            JoinKeyCoalescing.Default,
            false
        )

    /// Attaches a group of subqueries with equal rows and correlation to the outer plan: the outer plan with one
    /// more column per distinct aggregate, and each subquery's replacement over those columns.
    let private attach (opts: Options) (outer: LogicalPlan) (group: Analysis list) : LogicalPlan * (int * ScalarExpr) list =
        let head = group.Head
        let scope = head.Scope
        let correlation = classify scope head.Correlation
        let outerColumns = columnsOf outer

        let aggregates = group |> List.collect (fun a -> a.Aggregates) |> List.distinct
        // Only theta conditions join the rows themselves to the outer rows: there the rows are reduced to the
        // columns the keys, conditions and aggregates read, and those named like an outer column get a fresh name.
        // Elsewhere the rows are grouped (keys and aggregates only) or reduced to one row, and keep their names.
        let renames =
            if correlation.Theta.IsEmpty then
                []
            else
                let readColumns =
                    Set.unionMany [
                        yield! correlation.Keys |> List.map (fun (i, _, _) -> ScalarExprOps.collectColumns i)
                        yield! correlation.Theta |> List.map ScalarExprOps.collectColumns
                        yield! aggregates |> List.map ScalarExprOps.collectColumns
                    ]
                    |> Set.toList
                // A projection of no column would lose the row count: keep one column at least.
                let readColumns = if readColumns.IsEmpty then [ List.head (columnsOf head.Rows) ] else readColumns
                let outerSet = Set.ofList outerColumns
                readColumns |> List.map (fun c -> c, (if outerSet.Contains c then opts.Fresh "in" else c))
        let rename = ScalarExprOps.renameColumns (renames |> List.filter (fun (c, f) -> c <> f) |> Map)
        let rows =
            if renames.IsEmpty then head.Rows
            else LogicalPlan.Project(head.Rows, renames |> List.map (fun (c, fresh) -> fresh, ScalarExpr.Column c))
        let keys = correlation.Keys |> List.map (fun (i, o, n) -> rename i, o, n)
        let theta = correlation.Theta |> List.map (rename >> outerAsColumns scope)
        let aggregateColumns = aggregates |> List.map (fun a -> a, opts.Fresh "agg")
        let namedAggregates = aggregateColumns |> List.map (fun (a, name) -> name, rename a)

        let nullsEqual, nullGuards = joinNulls keys
        let rows =
            match conjoin nullGuards with
            | Some guard -> LogicalPlan.Filter(rows, guard)
            | None -> rows
        let keyNames = keys |> List.map (fun _ -> opts.Fresh "key")
        let keyedRows = LogicalPlan.WithColumns(rows, List.zip keyNames (keys |> List.map (fun (i, _, _) -> i)))
        let outerKeys = keys |> List.map (fun (_, o, _) -> o)
        let overNoRows (aggregate: ScalarExpr) =
            let aggregateType =
                SchemaInference.inferScalarType (SchemaInference.inferPlanSchema head.Rows) aggregate
            emptyValue aggregate aggregateType |> Option.defaultValue (ScalarExpr.Literal(LiteralValue.Null aggregateType))
        let selectByOuterOnly (a: Analysis) (value: ScalarExpr) =
            match conjoin correlation.OuterOnly with
            | Some condition -> ScalarExpr.Conditional(condition, value, replaceAggregations overNoRows a.Value)
            | None -> value

        if keys.IsEmpty && theta.IsEmpty then
            // Uncorrelated (or correlated through outer-only conditions alone): one row holds every value, computed
            // there whole (an aggregate over no rows already has its value over no rows).
            let values = group |> List.map (fun a -> a, opts.Fresh "value")
            let oneRow = LogicalPlan.Project(rows, values |> List.map (fun (a, name) -> name, rename a.Value))
            keepOnly (constantKey opts outer oneRow) (outerColumns @ (values |> List.map snd)),
            values |> List.map (fun (a, name) -> a.Scope, selectByOuterOnly a (ScalarExpr.Column name))
        else

        let joined =
            match keys, theta with
            | _, [] ->
                let grouped = LogicalPlan.GroupBy(keyedRows, keyNames |> List.map ScalarExpr.Column, namedAggregates, None, false)
                LogicalPlan.Join(outer, grouped, outerKeys, keyNames |> List.map ScalarExpr.Column, JoinKind.Left, None, JoinKeyCoalescing.Default, nullsEqual)
            | _ ->
                // Theta conditions: join the rows to each outer row, filter, and aggregate per outer row.
                let rid = opts.Fresh "rid"
                let indexed = LogicalPlan.WithRowIndex(outer, rid, Some 0u)
                let pairs =
                    if keys.IsEmpty then
                        LogicalPlan.Join(indexed, rows, [], [], JoinKind.Cross, None, JoinKeyCoalescing.KeepBoth, false)
                    else
                        LogicalPlan.Join(indexed, keyedRows, outerKeys, keyNames |> List.map ScalarExpr.Column, JoinKind.Inner, None, JoinKeyCoalescing.Default, nullsEqual)
                let matched = LogicalPlan.Filter(pairs, (conjoin theta).Value)
                let grouped = LogicalPlan.GroupBy(matched, [ ScalarExpr.Column rid ], namedAggregates, None, false)
                LogicalPlan.Join(indexed, grouped, [ ScalarExpr.Column rid ], [ ScalarExpr.Column rid ], JoinKind.Left, None, JoinKeyCoalescing.Default, false)

        let attachedSchema = SchemaInference.inferPlanSchema joined
        let filled (aggregate: ScalarExpr) =
            let name = aggregateColumns |> List.find (fun (a, _) -> a = aggregate) |> snd
            let column = ScalarExpr.Column name
            let aggregateType =
                attachedSchema.TryFindField name |> Option.map (fun f -> f.DataType) |> Option.defaultValue LogicalType.Unknown
            match emptyValue aggregate aggregateType with
            | Some empty -> ScalarExpr.Function(ScalarFunction.FillNull, [ column; empty ], LogicalType.Unknown)
            | None -> column
        let replacements = group |> List.map (fun a -> a.Scope, selectByOuterOnly a (replaceAggregations filled a.Value))
        keepOnly joined (outerColumns @ (aggregateColumns |> List.map snd)), replacements

    /// Attaches a row-valued uncorrelated scalar subquery: its single value, failing on more than one row.
    let private attachRowScalar (opts: Options) (outer: LogicalPlan) (s: RowScalar) : LogicalPlan * (int * ScalarExpr) =
        if not opts.SupportsAssertions then
            unsupported "a scalar subquery that is not an aggregate needs a backend that can raise"
        let column = singleColumn s.Plan
        let name = opts.Fresh "scalar"
        let tooMany = { Kind = QueryErrorKind.InvalidOperation; Message = "A scalar subquery yielded more than one row." }
        let check = { Min = 0L; Max = Some 1L; TooFew = tooMany; TooMany = tooMany }
        let value =
            ScalarExpr.Function(
                ScalarFunction.AssertCount check,
                [ ScalarExpr.Aggregation(AggKind.Len, None, false); ScalarExpr.Aggregation(AggKind.First, Some(ScalarExpr.Column column), false) ],
                LogicalType.Unknown
            )
        let oneRow = LogicalPlan.Project(s.Plan, [ (name, value) ])
        keepOnly (constantKey opts outer oneRow) (columnsOf outer @ [ name ]), (s.Scope, ScalarExpr.Column name)

    /// Attaches every subquery of the expressions: the outer plan with their columns, and their replacements.
    let private attachAll (opts: Options) (outer: LogicalPlan) (exprs: ScalarExpr list) : LogicalPlan * Map<int, ScalarExpr> =
        let subqueries = exprs |> List.collect ScalarExprOps.subqueries |> List.distinct
        if (subqueries |> List.distinctBy (fun (scope, _, _) -> scope)).Length <> subqueries.Length then
            failwith "[Samidare Bug] Two different subqueries of one node share a scope."
        for (_, _, kind) in subqueries do
            match kind with
            | SubqueryKind.In(_, false) ->
                unsupported "an IN subquery with SQL null semantics outside a positive filter conjunct (null-aware IN)"
            | _ -> ()
        let reduced = subqueries |> List.map (fun (scope, plan, kind) -> analyze opts scope plan kind)
        let groups =
            reduced
            |> List.choose (function Reduced.Aggregated a -> Some a | Reduced.RowScalar _ -> None)
            |> List.groupBy (fun a ->
                // Equal rows and correlation up to the scope: OuterColumns compared by name.
                a.Rows, a.Correlation |> List.map (outerAsColumns a.Scope))
            |> List.map snd
        let plan, replacements =
            groups
            |> List.fold
                (fun (plan, acc) group ->
                    let plan', rs = attach opts plan group
                    plan', acc @ rs)
                (outer, [])
        let plan, replacements =
            reduced
            |> List.choose (function Reduced.RowScalar r -> Some r | Reduced.Aggregated _ -> None)
            |> List.fold
                (fun (plan, acc) r ->
                    let plan', replacement = attachRowScalar opts plan r
                    plan', acc @ [ replacement ])
                (plan, replacements)
        plan, Map replacements

    /// A filter conjunct `Exists(a) OR Exists(b) OR ...` whose branches correlate by the same outer keys alone (TPC-DS
    /// Q10, Q35): one semi join with the union of the branches' keys.
    let private tryDisjunctiveSemiJoin (opts: Options) (outer: LogicalPlan) (conjunct: ScalarExpr) : LogicalPlan option =
        let rec disjuncts (e: ScalarExpr) =
            match e with
            | ScalarExpr.Binary(BinaryOp.Or, l, r) -> disjuncts l @ disjuncts r
            | other -> [ other ]
        match disjuncts conjunct with
        | [ _ ] -> None
        | branches ->
            let keyed =
                branches
                |> List.map (fun b ->
                    match b with
                    | ScalarExpr.Subquery(scope, plan, ((SubqueryKind.Exists | SubqueryKind.In(_, true)) as kind)) ->
                        match analyze opts scope plan kind with
                        | Reduced.Aggregated a ->
                            let c = classify scope a.Correlation
                            if c.Keys.IsEmpty || not c.Theta.IsEmpty || not c.OuterOnly.IsEmpty then None else Some(a.Rows, c.Keys)
                        | Reduced.RowScalar _ -> None
                    | _ -> None)
            if keyed |> List.exists Option.isNone then
                None
            else
                let keyed = keyed |> List.map Option.get
                let outerSide (keys: (ScalarExpr * ScalarExpr * bool) list) = keys |> List.map (fun (_, o, n) -> o, n)
                let first = outerSide (snd keyed.Head)
                if keyed |> List.exists (fun (_, keys) -> outerSide keys <> first) then
                    None
                else
                    let keyNames = first |> List.map (fun _ -> opts.Fresh "key")
                    let nullsEqual, _ = joinNulls (snd keyed.Head)
                    let union =
                        keyed
                        |> List.map (fun (rows, keys) ->
                            let _, guards = joinNulls keys
                            let rows =
                                match conjoin guards with
                                | Some guard -> LogicalPlan.Filter(rows, guard)
                                | None -> rows
                            LogicalPlan.Project(rows, List.zip keyNames (keys |> List.map (fun (i, _, _) -> i))))
                        |> List.reduce (fun a b -> LogicalPlan.Union(a, b, true))
                    Some(
                        LogicalPlan.Join(
                            outer,
                            union,
                            first |> List.map fst,
                            keyNames |> List.map ScalarExpr.Column,
                            JoinKind.Semi,
                            None,
                            JoinKeyCoalescing.Default,
                            nullsEqual
                        )
                    )

    /// A filter conjunct `[Not] Exists` / `[Not] In` whose correlation is equi keys alone, as a semi or anti join.
    let private trySemiJoin (opts: Options) (outer: LogicalPlan) (conjunct: ScalarExpr) : LogicalPlan option =
        let negated, core =
            match conjunct with
            | ScalarExpr.Unary(UnaryOp.Not, inner) -> true, inner
            | other -> false, other
        match core with
        | ScalarExpr.Binary(BinaryOp.Or, _, _) when not negated -> tryDisjunctiveSemiJoin opts outer core
        | ScalarExpr.Subquery(scope, plan, ((SubqueryKind.Exists | SubqueryKind.In _) as kind)) ->
            match kind with
            | SubqueryKind.In(_, false) when negated -> None
            | _ ->
                match analyze opts scope plan kind with
                | Reduced.Aggregated a ->
                    let correlation = classify scope a.Correlation
                    // An uncorrelated Exists (no key, no theta) is attached instead; so is a negated one with a
                    // condition on the outer row alone (`!Any(y => p && q(x))` keeps the rows where q(x) is not true).
                    if (correlation.Keys.IsEmpty && correlation.Theta.IsEmpty) || (negated && not correlation.OuterOnly.IsEmpty) then
                        None
                    elif not correlation.Theta.IsEmpty then
                        // Theta conditions: the outer rows (indexed) that some row matches, by a semi or anti join on
                        // the index.
                        let outer =
                            match conjoin correlation.OuterOnly with
                            | Some condition -> LogicalPlan.Filter(outer, condition)
                            | None -> outer
                        let readColumns =
                            Set.unionMany [
                                yield! correlation.Keys |> List.map (fun (i, _, _) -> ScalarExprOps.collectColumns i)
                                yield! correlation.Theta |> List.map ScalarExprOps.collectColumns
                            ]
                            |> Set.toList
                        let renames = readColumns |> List.map (fun c -> c, opts.Fresh "in")
                        let rename = ScalarExprOps.renameColumns (Map renames)
                        let rows = LogicalPlan.Project(a.Rows, renames |> List.map (fun (c, fresh) -> fresh, ScalarExpr.Column c))
                        let keys = correlation.Keys |> List.map (fun (i, o, n) -> rename i, o, n)
                        let nullsEqual, nullGuards = joinNulls keys
                        let rows =
                            match conjoin nullGuards with
                            | Some guard -> LogicalPlan.Filter(rows, guard)
                            | None -> rows
                        let rid = opts.Fresh "rid"
                        let indexed = LogicalPlan.WithRowIndex(outer, rid, Some 0u)
                        let pairs =
                            if keys.IsEmpty then
                                LogicalPlan.Join(indexed, rows, [], [], JoinKind.Cross, None, JoinKeyCoalescing.KeepBoth, false)
                            else
                                let keyNames = keys |> List.map (fun _ -> opts.Fresh "key")
                                let keyed = LogicalPlan.WithColumns(rows, List.zip keyNames (keys |> List.map (fun (i, _, _) -> i)))
                                LogicalPlan.Join(indexed, keyed, keys |> List.map (fun (_, o, _) -> o), keyNames |> List.map ScalarExpr.Column, JoinKind.Inner, None, JoinKeyCoalescing.Default, nullsEqual)
                        let theta = correlation.Theta |> List.map (rename >> outerAsColumns scope)
                        let matched =
                            LogicalPlan.Distinct(LogicalPlan.Project(LogicalPlan.Filter(pairs, (conjoin theta).Value), [ (rid, ScalarExpr.Column rid) ]), None, DistinctKeep.Any, false)
                        let joined =
                            LogicalPlan.Join(indexed, matched, [ ScalarExpr.Column rid ], [ ScalarExpr.Column rid ], (if negated then JoinKind.Anti else JoinKind.Semi), None, JoinKeyCoalescing.Default, false)
                        Some(keepOnly joined (columnsOf outer))
                    else
                        let outer =
                            match conjoin correlation.OuterOnly with
                            | Some condition -> LogicalPlan.Filter(outer, condition)
                            | None -> outer
                        let nullsEqual, nullGuards = joinNulls correlation.Keys
                        let rows =
                            match conjoin nullGuards with
                            | Some guard -> LogicalPlan.Filter(a.Rows, guard)
                            | None -> a.Rows
                        // The inner key expressions are the join's right keys: a semi or anti join keeps left columns only.
                        Some(
                            LogicalPlan.Join(
                                outer,
                                rows,
                                correlation.Keys |> List.map (fun (_, o, _) -> o),
                                correlation.Keys |> List.map (fun (i, _, _) -> i),
                                (if negated then JoinKind.Anti else JoinKind.Semi),
                                None,
                                JoinKeyCoalescing.Default,
                                nullsEqual
                            )
                        )
                | Reduced.RowScalar _ -> None
        | _ -> None

    // =========================================================================
    // Nodes and plans
    // =========================================================================

    let private decorrelateNode (opts: Options) (node: LogicalPlan) : LogicalPlan =
        let check (input: LogicalPlan) =
            if LogicalPlanOps.containsSubqueries input then
                failwith "[Samidare Bug] Decorrelation of a node whose input still holds a subquery."
        match node with
        | LogicalPlan.Filter(input, predicate) ->
            check input
            let plain, withSubqueries = conjuncts predicate |> List.partition (ScalarExprOps.containsSubquery >> not)
            let outer =
                match conjoin plain with
                | Some p -> LogicalPlan.Filter(input, p)
                | None -> input
            let outer, rest =
                withSubqueries
                |> List.fold
                    (fun (plan, rest) conjunct ->
                        match trySemiJoin opts plan conjunct with
                        | Some joined -> joined, rest
                        | None -> plan, rest @ [ conjunct ])
                    (outer, [])
            match rest with
            | [] -> outer
            | _ ->
                let attached, replacements = attachAll opts outer rest
                let predicate' = (conjoin rest).Value |> replaceSubqueries replacements.TryFind
                keepOnly (LogicalPlan.Filter(attached, predicate')) (columnsOf input)
        | LogicalPlan.Project(input, projections) ->
            check input
            if projections |> List.exists (snd >> ScalarExprOps.containsAggregation) then
                unsupported "a subquery in an aggregating projection"
            let attached, replacements = attachAll opts input (List.map snd projections)
            LogicalPlan.Project(attached, projections |> List.map (fun (n, e) -> n, replaceSubqueries replacements.TryFind e))
        | LogicalPlan.WithColumns(input, columns) ->
            check input
            if columns |> List.exists (snd >> ScalarExprOps.containsAggregation) then
                unsupported "a subquery in an aggregating projection"
            let inputColumns = columnsOf input
            let attached, replacements = attachAll opts input (List.map snd columns)
            let withColumns =
                LogicalPlan.WithColumns(attached, columns |> List.map (fun (n, e) -> n, replaceSubqueries replacements.TryFind e))
            let added = columns |> List.map fst |> List.filter (fun n -> not (List.contains n inputColumns)) |> List.distinct
            keepOnly withColumns (inputColumns @ added)
        | _ -> unsupported "a subquery outside a filter or a projection"

    /// Replaces the subqueries of one Filter, Project or WithColumns whose input holds none; Error names the shape
    /// that cannot be replaced.
    let tryNode (opts: Options) (node: LogicalPlan) : Result<LogicalPlan, string> =
        try
            Ok(decorrelateNode opts node)
        with Unsupported message ->
            Error message

    /// Replaces every subquery of a plan, innermost first.
    let tryPlan (opts: Options) (plan: LogicalPlan) : Result<LogicalPlan, string> =
        let rec go (p: LogicalPlan) =
            // A subquery's own plan is decorrelated first (its nested subqueries become joins inside it).
            let p = LogicalPlanOps.mapInputs go p
            if LogicalPlanOps.expressionsOf p |> List.exists ScalarExprOps.containsSubquery then
                decorrelateNode opts (LogicalPlanOps.mapExpressions replaceNested p)
            else
                p
        and replaceNested (e: ScalarExpr) =
            match e with
            | ScalarExpr.Subquery(scope, plan, kind) -> mapChildren replaceNested (ScalarExpr.Subquery(scope, go plan, kind))
            | _ -> mapChildren replaceNested e
        try
            Ok(go plan)
        with Unsupported message ->
            Error message
