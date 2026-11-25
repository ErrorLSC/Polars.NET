namespace Kaminari.Samidare.Sql

open System
open System.Collections.Generic
open Kaminari.Samidare.IR

/// A query's output column: its name, the IR column holding it, its type.
type internal SqlOutput = { Name: string; Internal: string; Type: LogicalType }

/// A bound query: its plan, whose columns are the outputs' `Internal` names, and its outputs in order.
type internal SqlBound = { Plan: LogicalPlan; Columns: SqlOutput list }

/// A translated query: its plan (columns named as the query names them, subqueries replaced by joins), the sources its
/// Scan slots read, and its columns.
type internal SqlPlan =
    { Plan: LogicalPlan
      Sources: SourceBinding array
      Columns: (string * LogicalType) list }

/// One translation's state: the text (for errors), the catalog, the sources read, fresh names, subquery scopes.
type internal SqlTranslation(text: string, catalog: ISqlCatalog, supportsAssertions: bool) =
    let sources = ResizeArray<SourceBinding>()
    let slots = Dictionary<ILogicalTableSource, int>(HashIdentity.Reference)
    let mutable names = 0
    let mutable scopes = 0
    let mutable shared = 0
    member _.Text = text
    member _.SupportsAssertions = supportsAssertions

    /// An id for a common table expression's shared plan.
    member _.NextShared() =
        shared <- shared + 1
        shared

    /// Derived tables bound so far, by their text and the common table expressions in scope: a derived table written
    /// again is the same plan, shared as a common table expression is.
    member val DerivedTables = Dictionary<string, SqlBound * int>()
    member _.Catalog = catalog
    member _.Sources = sources.ToArray()

    /// A column name no other column of the translation has: `name#n`.
    member _.Fresh(name: string) =
        names <- names + 1
        $"{name}#{names}"

    member _.NextScope() =
        scopes <- scopes + 1
        scopes

    /// The rows a plan reads at most: its largest table's estimate (0 when no table has one).
    member _.EstimatedRows(plan: LogicalPlan) : int64 =
        let rec scans (p: LogicalPlan) =
            match p with
            | LogicalPlan.Scan(LogicalPlanSource.Slot slot, _) when slot < sources.Count -> [ sources.[slot] ]
            | other -> LogicalPlanOps.getInputs other |> List.collect scans
        scans plan
        |> List.choose (function
            | SourceBinding.Table table -> QueryStatistics.RowCount table
            | SourceBinding.InMemory(rows, _) -> Some(int64 rows.Length))
        |> List.fold max 0L

    member _.Slot(source: ILogicalTableSource) =
        match slots.TryGetValue source with
        | true, slot -> slot
        | _ ->
            let slot = sources.Count
            sources.Add(SourceBinding.Table source)
            slots.[source] <- slot
            slot

/// What a query is bound in: the common table expressions in reach and the query around it (for correlation).
type internal SqlEnvironment =
    { Ctes: (SqlIdentifier * SqlBound * int) list
      Outer: (SqlScope * int) option }

/// Builds the IR plan of a query (sql-frontend design, S3): FROM (comma lists joined on the WHERE's equalities), WHERE,
/// GROUP BY (and grouping sets), HAVING, SELECT, DISTINCT, ORDER BY, LIMIT, set operations, common table expressions
/// and subqueries.
module internal SqlQueries =

    let private fail (t: SqlTranslation) (span: SqlSpan) (reason: string) (suggestion: string option) : 'T =
        raise (SqlException(reason, t.Text, span, suggestion))

    let private col name = ScalarExpr.Column name

    /// A plan of one row and no columns that matter (SELECT without FROM).
    let private oneRow = LogicalPlan.Project(LogicalPlan.Empty { Fields = [] }, [ "one", ScalarExpr.Aggregation(AggKind.Len, None, false) ])

    let private relationColumns (relations: SqlRelation list) =
        relations |> List.collect (fun r -> r.Columns |> List.map (fun c -> c.Internal)) |> Set.ofList

    /// Whether an expression reads only plain columns of this row (no subquery, no outer column, no other rows).
    let private rowLocal (e: ScalarExpr) =
        not (ScalarExprOps.containsOuterColumn e) && (ScalarExprOps.subqueries e).IsEmpty && not (ScalarExprOps.dependsOnOtherRows e)

    /// An equality conjunct between an expression over `left`'s columns and one over `right`'s: the two sides in that order.
    let private equiKey (left: Set<string>) (right: Set<string>) (conjunct: ScalarExpr) =
        match conjunct with
        | ScalarExpr.Binary(BinaryOp.Eq, a, b) when rowLocal a && rowLocal b ->
            let ca, cb = ScalarExprOps.collectColumns a, ScalarExprOps.collectColumns b
            if ca.IsEmpty || cb.IsEmpty then None
            elif Set.isSubset ca left && Set.isSubset cb right then Some(a, b)
            elif Set.isSubset cb left && Set.isSubset ca right then Some(b, a)
            else None
        | _ -> None

    /// A condition with SQL's IN under OR made two-valued: in a filter's positive position NULL drops the row as FALSE
    /// does, so `x IN (subquery)` there is `x IS NOT NULL AND x IN (subquery)` with nulls matching nulls, which the
    /// Decorrelation pass attaches under OR (it joins null-aware IN only as a whole conjunct).
    let private twoValuedInUnderOr (condition: ScalarExpr) =
        let rec underOr (e: ScalarExpr) =
            match e with
            | ScalarExpr.Binary((BinaryOp.Or | BinaryOp.And) as op, a, b) -> ScalarExpr.Binary(op, underOr a, underOr b)
            | ScalarExpr.Subquery(scope, plan, SubqueryKind.In(target, false)) ->
                ScalarExpr.Binary(BinaryOp.And, ScalarExpr.Unary(UnaryOp.IsNotNull, target), ScalarExpr.Subquery(scope, plan, SubqueryKind.In(target, true)))
            | other -> other
        let rec top (e: ScalarExpr) =
            match e with
            | ScalarExpr.Binary(BinaryOp.And, a, b) -> ScalarExpr.Binary(BinaryOp.And, top a, top b)
            | ScalarExpr.Binary(BinaryOp.Or, _, _) -> underOr e
            | other -> other
        top condition

    /// The conjuncts of a condition with what every disjunct of an OR holds taken out of it:
    /// `(a AND x) OR (a AND y)` is `a AND (x OR y)` (Kleene logic distributes), so a join key written in each branch
    /// (TPC-DS Q13, Q48) joins the tables instead of filtering their cross product.
    let private factorOrs (conjuncts: ScalarExpr list) : ScalarExpr list = FilterShapes.factorDisjunctions conjuncts

    let private conjoin (conjuncts: ScalarExpr list) =
        conjuncts |> List.reduce (fun a b -> ScalarExpr.Binary(BinaryOp.And, a, b))

    let private join left right (keys: (ScalarExpr * ScalarExpr) list) kind =
        LogicalPlan.Join(left, right, keys |> List.map fst, keys |> List.map snd, kind, None, JoinKeyCoalescing.KeepBoth, false)

    /// Rewrites the column names an expression holds (not inside subqueries): SELECT aliases in ORDER BY.
    let rec private substitute (f: SqlName -> SqlExpr option) (e: SqlExpr) : SqlExpr =
        let s = substitute f
        let kind =
            match e.Kind with
            | SqlExprKind.Column name ->
                match f name with
                | Some replacement -> replacement.Kind
                | None -> e.Kind
            | SqlExprKind.Literal _
            | SqlExprKind.Exists _
            | SqlExprKind.ScalarSubquery _ -> e.Kind
            | SqlExprKind.Unary(op, a) -> SqlExprKind.Unary(op, s a)
            | SqlExprKind.Binary(op, a, b) -> SqlExprKind.Binary(op, s a, s b)
            | SqlExprKind.IsNull(a, n) -> SqlExprKind.IsNull(s a, n)
            | SqlExprKind.IsDistinctFrom(a, b, n) -> SqlExprKind.IsDistinctFrom(s a, s b, n)
            | SqlExprKind.Between(a, b, c, n) -> SqlExprKind.Between(s a, s b, s c, n)
            | SqlExprKind.InList(a, items, n) -> SqlExprKind.InList(s a, List.map s items, n)
            | SqlExprKind.InSubquery(a, q, n) -> SqlExprKind.InSubquery(s a, q, n)
            | SqlExprKind.Like(a, p, esc, n) -> SqlExprKind.Like(s a, s p, Option.map s esc, n)
            | SqlExprKind.Case(o, whens, otherwise) -> SqlExprKind.Case(Option.map s o, whens |> List.map (fun (w, v) -> s w, s v), Option.map s otherwise)
            | SqlExprKind.Cast(a, t, isTry) -> SqlExprKind.Cast(s a, t, isTry)
            | SqlExprKind.Extract(field, a) -> SqlExprKind.Extract(field, s a)
            | SqlExprKind.Function call ->
                SqlExprKind.Function
                    { call with
                        Arguments = List.map s call.Arguments
                        Over =
                            call.Over
                            |> Option.map (fun w ->
                                { w with
                                    PartitionBy = List.map s w.PartitionBy
                                    OrderBy = w.OrderBy |> List.map (fun o -> { o with Expr = s o.Expr }) }) }
        { e with Kind = kind }

    /// The name a SELECT item shows: its alias, a column's name, otherwise its text.
    let private displayName (t: SqlTranslation) (e: SqlExpr) (alias: SqlIdentifier option) =
        match alias, e.Kind with
        | Some a, _ -> a.Text
        | None, SqlExprKind.Column name -> (List.last name).Text
        | None, _ -> t.Text.Substring(e.Span.Start, e.Span.Length)

    let private integerLiteral (e: SqlExpr) =
        match e.Kind with
        | SqlExprKind.Literal(SqlLiteral.Integer text) ->
            match Int64.TryParse text with
            | true, v -> Some v
            | _ -> None
        | _ -> None

    /// A FROM item before it is planned: inner joins and comma lists are one group of items joined on the equalities
    /// among their conditions and the WHERE's; an outer join keeps its two sides (its ON condition bound).
    [<RequireQualifiedAccess>]
    type private FromNode =
        | Leaf of LogicalPlan * SqlRelation list
        | Group of FromNode list * conditions: ScalarExpr list
        | Outer of SqlJoinKind * FromNode * FromNode * on: ScalarExpr * span: SqlSpan

    let rec private relationsOf (node: FromNode) : SqlRelation list =
        match node with
        | FromNode.Leaf(_, rs) -> rs
        | FromNode.Group(items, _) -> items |> List.collect relationsOf
        | FromNode.Outer(_, l, r, _, _) -> relationsOf l @ relationsOf r

    /// The columns a conjunct reads of this row, the outer columns of its subqueries included.
    let private reads (conjunct: ScalarExpr) =
        ScalarExprOps.subqueries conjunct
        |> List.fold (fun acc (scope, plan, _) -> Set.union acc (LogicalPlanOps.outerColumns scope plan)) (ScalarExprOps.collectColumns conjunct)

    /// Whether a conjunct can filter rows holding these columns before they are joined: it reads some of them and
    /// nothing else, and no column of the query around (a correlation stays on top, where the Decorrelation pass pulls
    /// it up from; it does not pull through joins).
    let private within (columns: Set<string>) (conjunct: ScalarExpr) =
        let r = reads conjunct
        not r.IsEmpty && Set.isSubset r columns && not (ScalarExprOps.containsOuterColumn conjunct)

    /// The plan with copies of a common table expression's plan for the references whose filters select branches of
    /// a UNION ALL in it.
    ///
    /// Every reference of a common table expression holds its shared plan, which the optimizer filters by what its
    /// references read (`SharedConsumerFilters`): TPC-DS Q64 reads its cross_sales once per year, one computation over
    /// both years. A filter that tests a column the plan gives as a literal (a branch of a union marked by
    /// `'s' AS sale_type`) selects branches, which it cannot drop behind a shared plan's barrier: then each filtered
    /// reference takes its own copy, where constant folding drops the branches it does not read, and the references
    /// without filters share the plan. Q4 and Q11 read a union of channels' yearly totals once per channel and year:
    /// sharing even the branches (two references per channel, each a year) saves no work, and the engine computes
    /// shared plans one after another, where copies run side by side (Q4: 590 ms in copies, 1090 shared by branch).
    let private unshareSelectingBranches (plan: LogicalPlan) : LogicalPlan =
        let selectsBranches = HashSet<int>()
        let visited = HashSet<int>()
        /// The output columns a plan gives as a literal in some row source: through projections, filters and unions.
        let rec literalColumns (p: LogicalPlan) : Set<string> =
            match p with
            | LogicalPlan.Project(input, columns) ->
                let below = literalColumns input
                columns
                |> List.choose (fun (name, expr) ->
                    match expr with
                    | ScalarExpr.Literal _ -> Some name
                    | ScalarExpr.Column source when below.Contains source -> Some name
                    | _ -> None)
                |> Set.ofList
            | LogicalPlan.Filter(input, _) -> literalColumns input
            | LogicalPlan.Union(left, right, true) -> Set.union (literalColumns left) (literalColumns right)
            | _ -> Set.empty
        let rec visit (p: LogicalPlan) =
            match p with
            | LogicalPlan.Filter(LogicalPlan.Project(LogicalPlan.Shared(id, body), renames), predicate) ->
                let literals = literalColumns body
                let read = ScalarExprOps.collectColumns predicate
                let selects =
                    renames
                    |> List.exists (fun (name, expr) ->
                        match expr with
                        | ScalarExpr.Column source -> literals.Contains source && read.Contains name
                        | _ -> false)
                if selects then selectsBranches.Add id |> ignore
                share id body
            | LogicalPlan.Shared(id, body) -> share id body
            | other -> LogicalPlanOps.getInputs other |> List.iter visit
        and share id body = if visited.Add id then visit body
        visit plan
        let rec rewrite (p: LogicalPlan) =
            match p with
            | LogicalPlan.Filter(LogicalPlan.Project(LogicalPlan.Shared(id, body), renames), predicate) when selectsBranches.Contains id ->
                LogicalPlan.Filter(LogicalPlan.Project(rewrite body, renames), predicate)
            | other -> LogicalPlanOps.mapInputs rewrite other
        if selectsBranches.Count = 0 then plan else rewrite plan

    /// The plan with a shared plan that only one reference holds made plain: a shared plan is computed once and kept,
    /// which keeps the engine from optimizing across it.
    let private unshareSingle (plan: LogicalPlan) : LogicalPlan =
        let counts = Dictionary<int, int>()
        let rec count (p: LogicalPlan) =
            match p with
            | LogicalPlan.Shared(id, input) ->
                let seen = match counts.TryGetValue id with | true, n -> n | _ -> 0
                counts.[id] <- seen + 1
                if seen = 0 then count input
            | other -> LogicalPlanOps.getInputs other |> List.iter count
        count plan
        let rec unwrap (p: LogicalPlan) =
            match p with
            | LogicalPlan.Shared(id, input) when counts.[id] = 1 -> unwrap input
            | other -> LogicalPlanOps.mapInputs unwrap other
        unwrap plan

    // ----------------------------------------------------------------------------------------------------------
    // Queries
    // ----------------------------------------------------------------------------------------------------------

    let rec bindQuery (t: SqlTranslation) (env: SqlEnvironment) (q: SqlQuery) : SqlBound =
        if q.Recursive then fail t q.Span "WITH RECURSIVE is not supported" None
        let env =
            q.With
            |> List.fold
                (fun (env: SqlEnvironment) cte ->
                    if env.Ctes |> List.exists (fun (name, _, _) -> SqlNames.matches cte.Name name.Text && SqlNames.matches name cte.Name.Text) then
                        fail t cte.Name.Span $"The common table expression {cte.Name.Text} is defined twice" None
                    let bound = bindQuery t env cte.Query
                    let bound =
                        if cte.Columns.IsEmpty then bound
                        elif cte.Columns.Length <> bound.Columns.Length then
                            fail t cte.Span $"{cte.Name.Text} names {cte.Columns.Length} columns, but its query gives {bound.Columns.Length}" None
                        else { bound with Columns = List.map2 (fun (c: SqlIdentifier) (o: SqlOutput) -> { o with Name = c.Text }) cte.Columns bound.Columns }
                    // Its subqueries become joins here, once, so that every reference holds the same plan.
                    let bound =
                        if not (LogicalPlanOps.containsSubqueries bound.Plan) then bound
                        else
                            let options: Passes.Decorrelation.Options = { Fresh = t.Fresh; SupportsAssertions = t.SupportsAssertions }
                            match Passes.Decorrelation.tryPlan options bound.Plan with
                            | Ok plan -> { bound with Plan = plan }
                            | Error _ -> bound
                    { env with Ctes = (cte.Name, bound, t.NextShared()) :: env.Ctes })
                env
        let bound =
            match q.Body with
            | SqlQueryBody.Select select -> bindSelect t env select q.OrderBy
            | body -> sortByOutputs t env (bindBody t env body) q.OrderBy
        limit t q bound

    and private limit (t: SqlTranslation) (q: SqlQuery) (bound: SqlBound) : SqlBound =
        let count (e: SqlExpr) what =
            match integerLiteral e with
            | Some v when v >= 0L -> v
            | _ -> fail t e.Span $"{what} must be a whole number written as a literal" None
        match q.Limit, q.Offset with
        | None, None -> bound
        | limit, offset ->
            let n = limit |> Option.map (fun l -> count l "LIMIT") |> Option.defaultValue (int64 UInt32.MaxValue)
            let skip = offset |> Option.map (fun o -> count o "OFFSET") |> Option.defaultValue 0L
            { bound with Plan = LogicalPlan.Limit(bound.Plan, skip, uint32 (min n (int64 UInt32.MaxValue))) }

    and private bindBody (t: SqlTranslation) (env: SqlEnvironment) (body: SqlQueryBody) : SqlBound =
        match body with
        | SqlQueryBody.Select select -> bindSelect t env select []
        | SqlQueryBody.Nested q -> bindQuery t env q
        | SqlQueryBody.SetOperation(op, all, left, right, span) ->
            let l, r = bindBody t env left, bindBody t env right
            if l.Columns.Length <> r.Columns.Length then
                fail t span $"The two sides of {op.ToString().ToUpperInvariant()} give {l.Columns.Length} and {r.Columns.Length} columns" None
            // Matched by position: both sides take the left's names, in the type each pair meets in.
            let outputs =
                List.map2
                    (fun (a: SqlOutput) (b: SqlOutput) ->
                        match SqlTypes.common a.Type b.Type with
                        | Some common -> { Name = a.Name; Internal = t.Fresh a.Name; Type = common }
                        | None -> fail t span $"Column {a.Name} is {SqlTypes.display a.Type} on one side and {SqlTypes.display b.Type} on the other" None)
                    l.Columns
                    r.Columns
            let side (b: SqlBound) =
                LogicalPlan.Project(
                    b.Plan,
                    List.map2
                        (fun (o: SqlOutput) (c: SqlOutput) ->
                            o.Internal, (if o.Type.Display = c.Type.Display then col c.Internal else ScalarExpr.Cast(col c.Internal, o.Type, false)))
                        outputs
                        b.Columns)
            let lp, rp = side l, side r
            let plan =
                match op, all with
                | SqlSetOp.Union, _ -> LogicalPlan.Union(lp, rp, all)
                | (SqlSetOp.Intersect | SqlSetOp.Except), false ->
                    // Distinct rows of the left with (without) an equal row on the right; NULL equals NULL here. The
                    // join only filters the left's rows, so they are made distinct before it, and the right only
                    // answers whether a row exists: each side is a Distinct over its own plan, which eager aggregation
                    // computes on the fact table's keys (TPC-DS Q14's cross_items intersects three channels' items).
                    // A side that is itself a set operation's result is distinct already.
                    let distinct (b: SqlBound) (plan: LogicalPlan) =
                        match b.Plan with
                        | LogicalPlan.Join(_, _, _, _, (JoinKind.Semi | JoinKind.Anti), _, _, true)
                        | LogicalPlan.Union(_, _, false) -> plan
                        | _ -> LogicalPlan.Distinct(plan, None, DistinctKeep.Any, false)
                    let keys = outputs |> List.map (fun o -> col o.Internal, col o.Internal)
                    let kind = if op = SqlSetOp.Intersect then JoinKind.Semi else JoinKind.Anti
                    LogicalPlan.Join(distinct l lp, distinct r rp, keys |> List.map fst, keys |> List.map snd, kind, None, JoinKeyCoalescing.Default, true)
                | _ -> fail t span $"{op.ToString().ToUpperInvariant()} ALL is not supported" None
            { Plan = plan; Columns = outputs }

    /// ORDER BY over a query's outputs only (after a set operation).
    and private sortByOutputs (t: SqlTranslation) (env: SqlEnvironment) (bound: SqlBound) (orderBy: SqlOrderItem list) : SqlBound =
        if orderBy.IsEmpty then bound
        else
            let relation = { Name = None; Columns = bound.Columns |> List.map (fun o -> { Name = o.Name; Internal = o.Internal; Type = o.Type; Unqualified = true }) }
            let binder =
                { Text = t.Text
                  Scope = { Relations = [ relation ]; Outer = env.Outer }
                  Aggregates = SqlAggregates.Refused "in ORDER BY"
                  WindowsRefused = Some "in the ORDER BY of a set operation"
                  Subquery = subquery t env }
            let orders =
                orderBy
                |> List.map (fun item ->
                    match integerLiteral item.Expr with
                    | Some n ->
                        if n < 1L || n > int64 bound.Columns.Length then fail t item.Expr.Span $"ORDER BY {n} is not a column position (1 to {bound.Columns.Length})" None
                        sortOrder item (col bound.Columns.[int n - 1].Internal)
                    | None -> sortOrder item (SqlExpressions.bind binder item.Expr).Expr)
            { bound with Plan = LogicalPlan.Sort(bound.Plan, orders) }

    and private sortOrder (item: SqlOrderItem) (expr: ScalarExpr) : SortOrder =
        let descending = item.Direction = SqlSortDirection.Descending
        let nullsLast =
            match item.Nulls with
            | SqlNullOrder.First -> false
            | SqlNullOrder.Last -> true
            | SqlNullOrder.Default -> descending
        { Expr = expr; Descending = descending; NullsLast = nullsLast }

    /// Binds a subquery inside an expression: a scope number of its own, the enclosing scope as its outer scope.
    and private subquery (t: SqlTranslation) (env: SqlEnvironment) : SqlBinder -> SqlQuery -> SqlSubquery =
        fun binder q ->
            let scope = t.NextScope()
            let bound = bindQuery t { env with Outer = Some(binder.Scope, scope) } q
            { Scope = scope; Plan = bound.Plan; Columns = bound.Columns |> List.map (fun o -> o.Internal, o.Type) }

    // ----------------------------------------------------------------------------------------------------------
    // FROM
    // ----------------------------------------------------------------------------------------------------------

    and private bindTable (t: SqlTranslation) (env: SqlEnvironment) (ref: SqlTableRef) : LogicalPlan * SqlRelation list =
        match ref with
        | SqlTableRef.Table(name, alias, span) ->
            let part =
                match name with
                | [ part ] -> part
                | _ -> fail t span "Qualified table names (schema.table) are not supported yet" None
            let relationName = alias |> Option.map (fun a -> a.Name) |> Option.defaultValue part
            let named (columns: (string * string * LogicalType) list) =
                // The alias's column list renames the first columns.
                let renames = alias |> Option.map (fun a -> a.Columns) |> Option.defaultValue []
                if renames.Length > columns.Length then
                    fail t span $"{relationName.Text} has {columns.Length} columns; the alias names {renames.Length}" None
                columns
                |> List.mapi (fun i (n, internalName, ty) ->
                    { Name = (if i < renames.Length then renames.[i].Text else n); Internal = internalName; Type = ty; Unqualified = true })
            match env.Ctes |> List.tryFind (fun (cte, _, _) -> SqlNames.matches part cte.Text) with
            | Some(_, bound, id) ->
                // A reference of its own: the CTE's plan, shared by every reference (see `unshareSelectingBranches` and
                // `unshareSingle`), under fresh names.
                let renamed = bound.Columns |> List.map (fun o -> o.Name, t.Fresh o.Name, o.Type)
                let body = if LogicalPlanOps.containsSubqueries bound.Plan then bound.Plan else LogicalPlan.Shared(id, bound.Plan)
                let plan = LogicalPlan.Project(body, List.map2 (fun (_, fresh, _) (o: SqlOutput) -> fresh, col o.Internal) renamed bound.Columns)
                plan, [ { Name = Some relationName; Columns = named renamed } ]
            | None ->
                match t.Catalog.FindTables part with
                | [ (_, source) ] ->
                    let fields = source.Schema.Fields
                    let renamed = fields |> List.map (fun f -> f.Name, t.Fresh f.Name, f.DataType)
                    let scan = LogicalPlan.Scan(LogicalPlanSource.Slot(t.Slot source), source.Schema)
                    let plan = LogicalPlan.Project(scan, List.map2 (fun (_, fresh, _) (f: LogicalField) -> fresh, col f.Name) renamed fields)
                    plan, [ { Name = Some relationName; Columns = named renamed } ]
                | [] ->
                    let hint = SqlNames.suggest part.Text t.Catalog.TableNames |> Option.map (fun n -> $"Did you mean {n}?")
                    fail t part.Span $"Unknown table {part.Text}" hint
                | many ->
                    let names = many |> List.map fst |> String.concat ", "
                    fail t part.Span $"The table name {part.Text} is ambiguous: {names}" (Some "Quote it to match one exactly.")
        | SqlTableRef.Derived(query, alias, span) ->
            // A derived table written twice (TPC-DS Q65's sa and sc) is bound once and shared, under fresh names the
            // second time; one written once is unshared again (`unshareSingle`). Not one that reads the query around
            // or holds a subquery, as common table expressions.
            let key = String.Join("|", SqlFormat.query query :: (env.Ctes |> List.map (fun (_, _, id) -> string id)))
            let readsOuter (plan: LogicalPlan) =
                let rec go (p: LogicalPlan) =
                    LogicalPlanOps.expressionsOf p |> List.exists ScalarExprOps.containsOuterColumn || LogicalPlanOps.getInputs p |> List.exists go
                go plan
            let bound =
                match t.DerivedTables.TryGetValue key with
                | true, (first, id) ->
                    let fresh = first.Columns |> List.map (fun o -> { o with Internal = t.Fresh o.Name })
                    let plan =
                        LogicalPlan.Project(
                            LogicalPlan.Shared(id, first.Plan),
                            List.map2 (fun (f: SqlOutput) (o: SqlOutput) -> f.Internal, col o.Internal) fresh first.Columns
                        )
                    { first with Plan = plan; Columns = fresh }
                | _ ->
                    let bound = bindQuery t env query
                    if LogicalPlanOps.containsSubqueries bound.Plan || readsOuter bound.Plan then
                        bound
                    else
                        let id = t.NextShared()
                        t.DerivedTables.[key] <- (bound, id)
                        { bound with Plan = LogicalPlan.Shared(id, bound.Plan) }
            let renames = alias |> Option.map (fun a -> a.Columns) |> Option.defaultValue []
            if renames.Length > bound.Columns.Length then fail t span $"The derived table has {bound.Columns.Length} columns; the alias names {renames.Length}" None
            let columns =
                bound.Columns
                |> List.mapi (fun i o -> { Name = (if i < renames.Length then renames.[i].Text else o.Name); Internal = o.Internal; Type = o.Type; Unqualified = true })
            bound.Plan, [ { Name = alias |> Option.map (fun a -> a.Name); Columns = columns } ]
        | SqlTableRef.Join(left, right, kind, condition, span) ->
            let lp, lr = bindTable t env left
            let rp, rr = bindTable t env right
            let relations = lr @ rr
            let lCols, rCols = relationColumns lr, relationColumns rr
            match kind, condition with
            | SqlJoinKind.Cross, _ -> LogicalPlan.Join(lp, rp, [], [], JoinKind.Cross, None, JoinKeyCoalescing.KeepBoth, false), relations
            | _, SqlJoinCondition.Using names ->
                if kind = SqlJoinKind.Full then fail t span "FULL JOIN ... USING is not supported yet" (Some "Write the condition with ON.")
                let scope (rs: SqlRelation list) = { Relations = rs; Outer = None }
                let find (rs: SqlRelation list) (n: SqlIdentifier) side =
                    match SqlNames.resolve (scope rs) [ n ] with
                    | SqlResolution.Found(c, _) -> c
                    | _ -> fail t n.Span $"USING names {n.Text}, which is not a single column of the {side} side" None
                let pairs = names |> List.map (fun n -> find lr n "left", find rr n "right")
                let keys =
                    pairs
                    |> List.map (fun (a, b) ->
                        match SqlTypes.common a.Type b.Type with
                        | Some c ->
                            let cast (x: SqlColumn) = if x.Type.Display = c.Display then col x.Internal else ScalarExpr.Cast(col x.Internal, c, false)
                            cast a, cast b
                        | None -> fail t span $"USING compares {a.Name} of types {SqlTypes.display a.Type} and {SqlTypes.display b.Type}" None)
                // The key is named once unqualified: the right side's copy is reached qualified only.
                let hidden = pairs |> List.map (fun (_, b) -> b.Internal) |> Set.ofList
                let rr = rr |> List.map (fun r -> { r with Columns = r.Columns |> List.map (fun c -> if hidden.Contains c.Internal then { c with Unqualified = false } else c) })
                let plan =
                    match kind with
                    | SqlJoinKind.Right -> join rp lp (keys |> List.map (fun (a, b) -> b, a)) JoinKind.Left
                    | SqlJoinKind.Left -> join lp rp keys JoinKind.Left
                    | _ -> join lp rp keys JoinKind.Inner
                plan, lr @ rr
            | _, SqlJoinCondition.On on ->
                let conjuncts = bindCondition t env relations "in a JOIN condition" on |> FilterShapes.conjuncts
                onJoin t kind (lp, lr) (rp, rr) conjuncts on.Span, relations
            | _, SqlJoinCondition.None -> fail t span "This join needs ON or USING" None

    /// A condition bound over relations: its IR expression, SQL's IN under OR made two-valued.
    and private bindCondition (t: SqlTranslation) (env: SqlEnvironment) (relations: SqlRelation list) (where: string) (e: SqlExpr) : ScalarExpr =
        let binder =
            { Text = t.Text
              Scope = { Relations = relations; Outer = env.Outer }
              Aggregates = SqlAggregates.Refused where
              WindowsRefused = Some where
              Subquery = subquery t env }
        let v = SqlExpressions.bind binder e
        match SqlTypes.classify v.Type with
        | SqlTypeClass.Boolean -> twoValuedInUnderOr v.Expr
        | SqlTypeClass.Null -> ScalarExpr.Literal(LiteralValue.Bool false)
        | _ -> fail t e.Span $"A condition is needed {where}, not a value of type {SqlTypes.display v.Type}" None

    /// A join on an ON condition's conjuncts: equalities between the two sides are its keys. An inner join filters by
    /// the rest; an outer join's other conditions may only restrict the side whose rows may go unmatched (they filter it
    /// before the join); a full join takes equalities only.
    and private onJoin (t: SqlTranslation) (kind: SqlJoinKind) (lp: LogicalPlan, lr: SqlRelation list) (rp: LogicalPlan, rr: SqlRelation list) (conjuncts: ScalarExpr list) (span: SqlSpan) : LogicalPlan =
        let lCols, rCols = relationColumns lr, relationColumns rr
        let classified = conjuncts |> List.map (fun c -> c, equiKey lCols rCols c)
        let keys = classified |> List.choose snd
        let rest = classified |> List.filter (fun (_, k) -> k.IsNone) |> List.map fst
        let readsOnly (side: Set<string>) (e: ScalarExpr) = rowLocal e && Set.isSubset (ScalarExprOps.collectColumns e) side
        match kind with
        | SqlJoinKind.Inner
        | SqlJoinKind.Cross ->
            let joined = if keys.IsEmpty then LogicalPlan.Join(lp, rp, [], [], JoinKind.Cross, None, JoinKeyCoalescing.KeepBoth, false) else join lp rp keys JoinKind.Inner
            if rest.IsEmpty then joined else LogicalPlan.Filter(joined, conjoin rest)
        | SqlJoinKind.Left
        | SqlJoinKind.Right ->
            let innerPlan = if kind = SqlJoinKind.Left then rp else lp
            let innerCols = if kind = SqlJoinKind.Left then rCols else lCols
            if keys.IsEmpty then fail t span "An outer join needs an equality between its two sides" None
            match rest |> List.tryFind (fun c -> not (readsOnly innerCols c)) with
            | Some _ -> fail t span "This outer join condition is not supported: besides equalities, it may only test the joined table's own columns" None
            | None ->
                let innerPlan = if rest.IsEmpty then innerPlan else LogicalPlan.Filter(innerPlan, conjoin rest)
                if kind = SqlJoinKind.Left then join lp innerPlan keys JoinKind.Left
                else join rp innerPlan (keys |> List.map (fun (a, b) -> b, a)) JoinKind.Left
        | SqlJoinKind.Full ->
            if keys.IsEmpty || not rest.IsEmpty then fail t span "A FULL JOIN condition must be equalities between its two sides" None
            join lp rp keys JoinKind.Full

    /// A FROM item as a node: inner joins (and their ON conditions) merge into one group; USING joins are planned as
    /// written.
    and private flatten (t: SqlTranslation) (env: SqlEnvironment) (ref: SqlTableRef) : FromNode =
        match ref with
        | SqlTableRef.Join(left, right, ((SqlJoinKind.Inner | SqlJoinKind.Cross) as kind), condition, _) when
            (match condition with SqlJoinCondition.Using _ -> false | _ -> true)
            ->
            let l, r = flatten t env left, flatten t env right
            let members (n: FromNode) =
                match n with
                | FromNode.Group(items, conditions) -> items, conditions
                | other -> [ other ], []
            let li, lc = members l
            let ri, rc = members r
            let on =
                match kind, condition with
                | SqlJoinKind.Inner, SqlJoinCondition.On e -> bindCondition t env (relationsOf l @ relationsOf r) "in a JOIN condition" e |> FilterShapes.conjuncts |> factorOrs
                | _ -> []
            FromNode.Group(li @ ri, lc @ rc @ on)
        | SqlTableRef.Join(left, right, kind, SqlJoinCondition.On e, _) ->
            let l, r = flatten t env left, flatten t env right
            FromNode.Outer(kind, l, r, bindCondition t env (relationsOf l @ relationsOf r) "in a JOIN condition" e, e.Span)
        | other -> FromNode.Leaf(bindTable t env other)

    /// The plan of a FROM node, with the conjuncts from outside it that it can apply before joining (each as deep as the
    /// columns it reads allow); the others are returned. In a group every condition is joined or filtered, its
    /// equalities chosen as join keys item by item. An outer join takes conjuncts on the side whose rows it keeps.
    and private build (t: SqlTranslation) (node: FromNode) (outside: ScalarExpr list) : LogicalPlan * ScalarExpr list =
        match node with
        | FromNode.Leaf(plan, rs) ->
            let mine, rest = outside |> List.partition (within (relationColumns rs))
            (if mine.IsEmpty then plan else LogicalPlan.Filter(plan, conjoin mine)), rest
        | FromNode.Group(items, conditions) ->
            let mine, rest = outside |> List.partition (within (relationColumns (relationsOf node)))
            let all = conditions @ mine
            // Each item takes the conditions it alone reads; the rest join or filter the items. A subquery is not joined
            // to the item with the most rows (the fact table) before the other items reduce it: a semi or anti join on
            // a whole fact table (TPC-DS Q16, Q94) costs more than on the rows the dimensions keep.
            let largest =
                if items.Length < 2 then -1
                else
                    let rec leaves (n: FromNode) =
                        match n with
                        | FromNode.Leaf(plan, _) -> [ plan ]
                        | FromNode.Group(members, _) -> members |> List.collect leaves
                        | FromNode.Outer(_, l, r, _, _) -> leaves l @ leaves r
                    items |> List.mapi (fun i item -> i, leaves item |> List.map t.EstimatedRows |> List.fold max 0L) |> List.maxBy snd |> fst
            let owner (c: ScalarExpr) =
                items
                |> List.tryFindIndex (fun item -> within (relationColumns (relationsOf item)) c)
                |> Option.filter (fun i -> i <> largest || (ScalarExprOps.subqueries c).IsEmpty)
            let owned = all |> List.map (fun c -> c, owner c)
            let built, leftovers =
                items
                |> List.mapi (fun i item ->
                    let plan, left = build t item (owned |> List.choose (fun (c, o) -> if o = Some i then Some c else None))
                    (plan, relationsOf item), left)
                |> List.unzip
            let pending = (owned |> List.filter (fun (_, o) -> o.IsNone) |> List.map fst) @ List.concat leftovers
            let plan, left = joinGreedy t built pending
            (if left.IsEmpty then plan else LogicalPlan.Filter(plan, conjoin left)), rest
        | FromNode.Outer(kind, l, r, on, span) ->
            let preserved = match kind with SqlJoinKind.Left -> Some l | SqlJoinKind.Right -> Some r | _ -> None
            let mine, rest =
                match preserved with
                | Some side -> outside |> List.partition (within (relationColumns (relationsOf side)))
                | None -> [], outside
            let side (n: FromNode) =
                let plan, left = build t n (if preserved |> Option.exists (fun p -> obj.ReferenceEquals(p, n)) then mine else [])
                (if left.IsEmpty then plan else LogicalPlan.Filter(plan, conjoin left)), relationsOf n
            onJoin t kind (side l) (side r) (FilterShapes.conjuncts on) span, rest

    /// Items joined one after another, starting from the one with the most rows (the fact table, as a LINQ query is
    /// written: the optimizer's rewrites read the fact table on the left), each next on the equalities between it and
    /// those joined so far (in FROM order among the items that have one; an item with none is a cross join); the
    /// conjuncts not used as keys are returned.
    and private joinGreedy (t: SqlTranslation) (items: (LogicalPlan * SqlRelation list) list) (conjuncts: ScalarExpr list) : LogicalPlan * ScalarExpr list =
        let items =
            match items with
            | [] | [ _ ] -> items
            | _ ->
                let largest = items |> List.maxBy (fst >> t.EstimatedRows)
                largest :: (items |> List.filter (fun i -> not (obj.ReferenceEquals(i, largest))))
        match items with
        | [] -> oneRow, conjuncts
        | (first, firstRelations) :: others ->
            let mutable plan = first
            let mutable columns = relationColumns firstRelations
            let mutable remaining = others
            let mutable pending = conjuncts
            while not remaining.IsEmpty do
                let keysWith (relations: SqlRelation list) =
                    let candidate = relationColumns relations
                    pending |> List.choose (fun c -> equiKey columns candidate c |> Option.map (fun k -> c, k))
                let next =
                    remaining
                    |> List.tryPick (fun (p, rs) ->
                        match keysWith rs with
                        | [] -> None
                        | keys -> Some(p, rs, keys))
                match next with
                | Some(p, rs, keys) ->
                    plan <- join plan p (keys |> List.map snd) JoinKind.Inner
                    columns <- Set.union columns (relationColumns rs)
                    let used = keys |> List.map fst
                    pending <- pending |> List.filter (fun c -> not (used |> List.exists (fun u -> obj.ReferenceEquals(u, c))))
                    remaining <- remaining |> List.filter (fun (q, _) -> not (obj.ReferenceEquals(q, p)))
                | None ->
                    let p, rs = remaining.Head
                    plan <- LogicalPlan.Join(plan, p, [], [], JoinKind.Cross, None, JoinKeyCoalescing.KeepBoth, false)
                    columns <- Set.union columns (relationColumns rs)
                    remaining <- remaining.Tail
            plan, pending

    // ----------------------------------------------------------------------------------------------------------
    // SELECT
    // ----------------------------------------------------------------------------------------------------------

    and private bindSelect (t: SqlTranslation) (env: SqlEnvironment) (s: SqlSelect) (orderBy: SqlOrderItem list) : SqlBound =
        let from = FromNode.Group(s.From |> List.map (flatten t env), [])
        let relations = relationsOf from
        // Two FROM items under one name are ambiguous wherever they are named.
        relations
        |> List.choose (fun r -> r.Name)
        |> List.groupBy (fun n -> n.Text.ToUpperInvariant())
        |> List.tryFind (fun (_, ns) -> ns.Length > 1)
        |> Option.iter (fun (_, ns) -> fail t ns.[1].Span $"The name {ns.[1].Text} is given to two tables in this FROM" (Some "Give one of them another alias."))
        let scope = { Relations = relations; Outer = env.Outer }
        let binderFor aggregates windows =
            { Text = t.Text; Scope = scope; Aggregates = aggregates; WindowsRefused = windows; Subquery = subquery t env }
        let where =
            s.Where
            |> Option.map (fun w ->
                let v = SqlExpressions.bind (binderFor (SqlAggregates.Refused "in WHERE") (Some "in WHERE")) w
                match SqlTypes.classify v.Type with
                | SqlTypeClass.Boolean -> twoValuedInUnderOr v.Expr
                | SqlTypeClass.Null -> ScalarExpr.Literal(LiteralValue.Bool false)
                | _ -> fail t w.Span $"WHERE needs a condition, not a value of type {SqlTypes.display v.Type}" None)
        // The FROM joined with the WHERE's conjuncts taken where they apply; the rest (a correlation with the query
        // around, a condition reading no column) filter the joined rows.
        let input, leftover = build t from (where |> Option.map (FilterShapes.conjuncts >> factorOrs) |> Option.defaultValue [])
        let input = if leftover.IsEmpty then input else LogicalPlan.Filter(input, conjoin leftover)

        let selectExprs = s.Items |> List.choose (function SqlSelectItem.Expr(e, alias) -> Some(e, alias) | _ -> None)
        let aggregating =
            not s.GroupBy.IsEmpty || s.Having.IsSome
            || selectExprs |> List.exists (fst >> SqlExpressions.containsAggregate)
            || orderBy |> List.exists (fun o -> SqlExpressions.containsAggregate o.Expr)

        // The value each SELECT item names, bound by `bindItem`, and the plan the items are evaluated over.
        let outputsOf (bindItem: SqlExpr -> SqlValue) =
            s.Items
            |> List.collect (fun item ->
                match item with
                | SqlSelectItem.Expr(e, alias) -> [ displayName t e alias, bindItem e, Some e ]
                | SqlSelectItem.Wildcard(qualifier, span) ->
                    if aggregating then fail t span "SELECT * cannot be used with GROUP BY or aggregates" (Some "Name the columns.")
                    let reached =
                        match qualifier with
                        | None -> relations
                        | Some [ q ] ->
                            match relations |> List.filter (fun r -> r.Name |> Option.exists (fun n -> SqlNames.matches q n.Text)) with
                            | [] -> fail t span $"No table or alias named {q.Text} in this query" None
                            | rs -> rs
                        | Some _ -> fail t span "Qualified names before .* are not supported" None
                    if reached.IsEmpty then fail t span "SELECT * needs a FROM" None
                    [ for r in reached do
                          for c in r.Columns do
                              if c.Unqualified || qualifier.IsSome then c.Name, { Expr = col c.Internal; Type = c.Type }, None ])

        let mutable grouped: (LogicalPlan -> LogicalPlan) option = None
        /// Without GROUP BY keys: the aggregates written into an expression in place of their columns.
        let mutable keylessInline: (ScalarExpr -> ScalarExpr) option = None
        let selectBinder =
            if not aggregating then binderFor (SqlAggregates.Refused "here") None
            else
                // Grouping keys: GROUP BY items bound over the input; a name that is no input column may be a SELECT alias.
                let keyBinder = binderFor (SqlAggregates.Refused "in GROUP BY") (Some "in GROUP BY")
                let keys = ResizeArray<ScalarExpr * SqlValue>()
                let keyIndex (v: SqlValue) =
                    match keys |> Seq.tryFindIndex (fun (k, _) -> k = v.Expr) with
                    | Some i -> i
                    | None ->
                        keys.Add((v.Expr, { Expr = col (t.Fresh "key"); Type = v.Type }))
                        keys.Count - 1
                let bindKey (e: SqlExpr) =
                    let aliasOf (name: SqlName) =
                        match name with
                        | [ n ] ->
                            match SqlNames.resolve scope name with
                            | SqlResolution.NotFound _ ->
                                selectExprs |> List.tryFind (fun (_, a) -> a |> Option.exists (fun a -> SqlNames.matches n a.Text)) |> Option.map fst
                            | _ -> None
                        | _ -> None
                    let target =
                        match e.Kind, integerLiteral e with
                        | _, Some n ->
                            if n < 1L || n > int64 selectExprs.Length then fail t e.Span $"GROUP BY {n} is not a SELECT item position" None
                            fst selectExprs.[int n - 1]
                        | SqlExprKind.Column name, _ -> aliasOf name |> Option.defaultValue e
                        | _ -> e
                    keyIndex (SqlExpressions.bind keyBinder target)
                /// The key sets an item groups by (several for ROLLUP, CUBE and GROUPING SETS).
                let rec setsOf (item: SqlGroupingItem) : int list list =
                    match item with
                    | SqlGroupingItem.Expr e -> [ [ bindKey e ] ]
                    | SqlGroupingItem.Set(es, _) -> [ es |> List.map bindKey ]
                    | SqlGroupingItem.Rollup(items, _) ->
                        let parts = items |> List.map (fun i -> List.concat (setsOf i))
                        [ for n in parts.Length .. -1 .. 0 -> List.concat (List.take n parts) ]
                    | SqlGroupingItem.Cube(items, span) ->
                        let parts = items |> List.map (fun i -> List.concat (setsOf i))
                        if parts.Length > 12 then fail t span "CUBE of more than 12 elements is not supported" None
                        [ for mask in (1 <<< parts.Length) - 1 .. -1 .. 0 ->
                              parts |> List.indexed |> List.filter (fun (i, _) -> mask &&& (1 <<< (parts.Length - 1 - i)) <> 0) |> List.collect snd ]
                    | SqlGroupingItem.GroupingSets(items, _) -> items |> List.collect setsOf
                let sets =
                    s.GroupBy
                    |> List.fold (fun acc item -> [ for a in acc do for b in setsOf item -> a @ b ]) [ [] ]
                    |> List.map List.distinct
                let keyCount = keys.Count
                let plainGrouping = sets |> List.forall (fun set -> set.Length = keyCount)
                let aggregates = ResizeArray<string * SqlValue>()
                let groupingColumns = Dictionary<int, string>()
                let grouping =
                    { Keys = List.ofSeq keys
                      Aggregate =
                        fun v ->
                            match aggregates |> Seq.tryFind (fun (_, a) -> a.Expr = v.Expr) with
                            | Some(name, a) -> { Expr = col name; Type = a.Type }
                            | None ->
                                let name = t.Fresh "agg"
                                aggregates.Add((name, v))
                                { Expr = col name; Type = v.Type }
                      GroupingOf =
                        fun index ->
                            if plainGrouping then None
                            else
                                let name =
                                    match groupingColumns.TryGetValue index with
                                    | true, n -> n
                                    | _ ->
                                        let n = t.Fresh "grouping"
                                        groupingColumns.[index] <- n
                                        n
                                Some { Expr = col name; Type = LogicalTypes.Int32 } }
                let keyName (v: SqlValue) = match v.Expr with ScalarExpr.Column n -> n | _ -> invalidOp "key"
                if keys.Count = 0 then
                    keylessInline <-
                        Some(ScalarExprOps.substitute (fun name -> aggregates |> Seq.tryFind (fun (n, _) -> n = name) |> Option.map (fun (_, v) -> v.Expr)))
                grouped <-
                    Some(fun input ->
                        let keyList = List.ofSeq keys
                        let aggs = aggregates |> Seq.map (fun (n, v) -> n, v.Expr) |> List.ofSeq
                        if keyList.IsEmpty then
                            LogicalPlan.Project(input, (if aggs.IsEmpty then [ "one", ScalarExpr.Aggregation(AggKind.Len, None, false) ] else aggs))
                        elif plainGrouping then
                            LogicalPlan.GroupBy(input, keyList |> List.map (fun (e, v) -> ScalarExpr.Alias(e, keyName v)), aggs, None, false)
                        else
                            let levels =
                                sets
                                |> List.map (fun set ->
                                    let literals =
                                        groupingColumns
                                        |> Seq.map (fun kv -> kv.Value, ScalarExpr.Literal(LiteralValue.Int32(if List.contains kv.Key set then 0 else 1)))
                                        |> Map.ofSeq
                                    set, literals)
                            let spec =
                                { KeyAliases = keyList |> List.map (fun (e, v) -> keyName v, e)
                                  Aggs = aggs
                                  Levels = levels
                                  OutputColumns = (keyList |> List.map (snd >> keyName)) @ (aggs |> List.map fst) @ (groupingColumns.Values |> List.ofSeq)
                                  LevelMerges = None }
                            LogicalPlan.GroupingSets(input, spec))
                binderFor (SqlAggregates.Grouped grouping) None

        let outputs = outputsOf (SqlExpressions.bind selectBinder)
        let having =
            s.Having
            |> Option.map (fun h ->
                let v = SqlExpressions.bind { selectBinder with WindowsRefused = Some "in HAVING" } h
                match SqlTypes.classify v.Type with
                | SqlTypeClass.Boolean -> twoValuedInUnderOr v.Expr
                | _ -> fail t h.Span "HAVING needs a condition" None)

        // ORDER BY: a position, an output's name, or an expression (SELECT aliases in it stand for their expressions).
        let outputNames = outputs |> List.map (fun (name, _, _) -> name)
        let aliasExpr (name: SqlName) =
            match name with
            | [ n ] ->
                match outputs |> List.filter (fun (o, _, e) -> e.IsSome && SqlNames.matches n o) with
                | (_, _, Some e) :: _ -> Some e
                | _ -> None
            | _ -> None
        let hidden = ResizeArray<string * SqlValue>()
        let orders =
            orderBy
            |> List.map (fun item ->
                let output =
                    match integerLiteral item.Expr, item.Expr.Kind with
                    | Some n, _ ->
                        if n < 1L || n > int64 outputs.Length then fail t item.Expr.Span $"ORDER BY {n} is not a column position (1 to {outputs.Length})" None
                        Some(int n - 1)
                    | None, SqlExprKind.Column [ n ] ->
                        match outputNames |> List.indexed |> List.filter (fun (_, o) -> SqlNames.matches n o) with
                        | [ (i, _) ] -> Some i
                        | (i, _) :: more when more |> List.forall (fun (j, _) -> let (_, a, _), (_, b, _) = outputs.[i], outputs.[j] in a.Expr = b.Expr) -> Some i
                        | _ :: _ -> fail t item.Expr.Span $"ORDER BY {n.Text} could be more than one SELECT item" None
                        | [] -> None
                    | _ -> None
                match output with
                | Some i -> Choice1Of2 i, item
                | None ->
                    let v = SqlExpressions.bind selectBinder (substitute aliasExpr item.Expr)
                    match outputs |> List.tryFindIndex (fun (_, o, _) -> o.Expr = v.Expr) with
                    | Some i -> Choice1Of2 i, item
                    | None ->
                        if s.Distinct then fail t item.Expr.Span "With SELECT DISTINCT, ORDER BY expressions must appear in the select list" None
                        let name = t.Fresh "sort"
                        hidden.Add((name, v))
                        Choice2Of2 name, item)

        // The plan: input, grouping, HAVING, the items (and hidden sort keys), DISTINCT, ORDER BY.
        let columns = outputs |> List.map (fun (name, v, _) -> { Name = name; Internal = t.Fresh name; Type = v.Type })
        let items = (List.map2 (fun (o: SqlOutput) (_, v: SqlValue, _) -> o.Internal, v.Expr) columns outputs) @ (hidden |> Seq.map (fun (n, v) -> n, v.Expr) |> List.ofSeq)
        let projected =
            // Aggregates without GROUP BY keys, each item over them: one Project of the items with the aggregates written
            // in (the shape a scalar subquery's aggregate takes, which the Decorrelation pass reads).
            let inlined =
                match keylessInline with
                | Some writeIn when having.IsNone ->
                    let items = items |> List.map (fun (n, e) -> n, writeIn e)
                    if items |> List.forall (snd >> ScalarExprOps.containsAggregation) then Some(LogicalPlan.Project(input, items)) else None
                | _ -> None
            match inlined with
            | Some plan -> plan
            | None ->
                let source = match grouped with Some g -> g input | None -> input
                let source = match having with Some h -> LogicalPlan.Filter(source, h) | None -> source
                LogicalPlan.Project(source, items)
        let distinct = if s.Distinct then LogicalPlan.Distinct(projected, None, DistinctKeep.Any, false) else projected
        let sorted =
            if orders.IsEmpty then distinct
            else
                let sortOrders =
                    orders
                    |> List.map (fun (target, item) ->
                        match target with
                        | Choice1Of2 i -> sortOrder item (col columns.[i].Internal)
                        | Choice2Of2 name -> sortOrder item (col name))
                LogicalPlan.Sort(distinct, sortOrders)
        let plan = if hidden.Count = 0 then sorted else LogicalPlan.Project(sorted, columns |> List.map (fun c -> c.Internal, col c.Internal))
        { Plan = plan; Columns = columns }

    // ----------------------------------------------------------------------------------------------------------
    // Statements
    // ----------------------------------------------------------------------------------------------------------

    /// Translates a query to an IR plan: parsed, bound, its columns named as the query names them (a name given twice
    /// takes `_1`, `_2` suffixes), its subqueries replaced by joins.
    let translate (catalog: ISqlCatalog) (supportsAssertions: bool) (text: string) : SqlPlan =
        let query = SqlParser.parseQuery SqlDialect.Standard text
        let t = SqlTranslation(text, catalog, supportsAssertions)
        let bound = bindQuery t { Ctes = []; Outer = None } query
        let seen = Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        let names =
            bound.Columns
            |> List.map (fun c ->
                match seen.TryGetValue c.Name with
                | true, n ->
                    seen.[c.Name] <- n + 1
                    $"{c.Name}_{n}"
                | _ ->
                    seen.[c.Name] <- 1
                    c.Name)
        let plan = LogicalPlan.Project(bound.Plan, List.map2 (fun name (c: SqlOutput) -> name, col c.Internal) names bound.Columns)
        let options: Passes.Decorrelation.Options = { Fresh = t.Fresh; SupportsAssertions = supportsAssertions }
        match Passes.Decorrelation.tryPlan options (unshareSelectingBranches plan) with
        | Ok plan -> { Plan = unshareSingle plan; Sources = t.Sources; Columns = List.map2 (fun name (c: SqlOutput) -> name, c.Type) names bound.Columns }
        | Error reason -> raise (SqlException($"This subquery is not supported: {reason}", text, query.Span, None))
