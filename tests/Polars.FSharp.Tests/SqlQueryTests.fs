namespace Polars.FSharp.Tests

open Xunit
open Kaminari.Samidare.IR
open Kaminari.Samidare.Sql

/// Translating SQL queries to IR plans (sql-frontend S3): how FROM, joins, grouping, ordering, set operations, common
/// table expressions and subqueries build the plan, the columns it gives, and the errors.
module SqlQueryTests =

    let private table name fields =
        { new ILogicalTableSource with
            member _.Name = name
            member _.Schema = LogicalSchema.OfFields fields }

    let private catalog =
        let c = SqlCatalog()
        c.Register("store", table "store" [ "s_id", LogicalTypes.Int64; "s_name", LogicalTypes.String; "s_state", LogicalTypes.String ])
        c.Register(
            "sales",
            table "sales" [ "ss_store", LogicalTypes.Int64; "ss_item", LogicalTypes.Int64; "ss_qty", LogicalTypes.Int32; "ss_price", LogicalTypes.decimalOf 7 2 ])
        c.Register("item", table "item" [ "i_id", LogicalTypes.Int64; "i_name", LogicalTypes.String ])
        c.Register("stock", table "stock" [ "s_id", LogicalTypes.Int64; "qty", LogicalTypes.Int64 ])
        c :> ISqlCatalog

    let private translate text = SqlQueries.translate catalog true text

    let private fails (text: string) =
        Assert.Throws<SqlException>(fun () -> translate text |> ignore)

    let rec private nodes (plan: LogicalPlan) = plan :: (LogicalPlanOps.getInputs plan |> List.collect nodes)

    let private joinsOf (plan: LogicalPlan) =
        nodes plan
        |> List.choose (function
            | LogicalPlan.Join(left, right, leftOn, rightOn, kind, _, _, nullsEqual) -> Some(left, right, leftOn, rightOn, kind, nullsEqual)
            | _ -> None)

    let private kinds plan = joinsOf plan |> List.map (fun (_, _, _, _, kind, _) -> kind)

    let private columnsOf text = (translate text).Columns |> List.map fst

    let rec private readsColumn (prefix: string) (e: ScalarExpr) =
        ScalarExprOps.collectColumns e |> Set.exists (fun c -> c.StartsWith(prefix + "#"))

    // ----------------------------------------------------------------------------------------------------------
    // FROM and joins
    // ----------------------------------------------------------------------------------------------------------

    [<Fact>]
    let ``A comma list joins on the WHERE's equalities`` () =
        let plan = (translate "SELECT s_name, i_name FROM sales, store, item WHERE ss_store = s_id AND i_id = ss_item AND s_state = 'CA'").Plan
        Assert.Equal<JoinKind list>([ JoinKind.Inner; JoinKind.Inner ], kinds plan)
        Assert.True(nodes plan |> List.exists (function LogicalPlan.Filter(_, p) -> readsColumn "s_state" p | _ -> false))
        // A key written in every branch of an OR joins too (TPC-DS Q13).
        let ors = (translate "SELECT s_name FROM sales, store WHERE (ss_store = s_id AND ss_qty > 1) OR (ss_store = s_id AND s_state = 'CA')").Plan
        Assert.Equal<JoinKind list>([ JoinKind.Inner ], kinds ors)

    [<Fact>]
    let ``ON splits join keys from the other conditions`` () =
        let inner = (translate "SELECT s_name FROM sales JOIN store ON ss_store = s_id AND s_state = 'CA'").Plan
        Assert.Equal<JoinKind list>([ JoinKind.Inner ], kinds inner)
        // A LEFT JOIN's condition on the joined table filters that table before the join.
        match joinsOf (translate "SELECT s_name FROM sales LEFT JOIN store ON ss_store = s_id AND s_state = 'CA'").Plan with
        | [ (_, LogicalPlan.Filter(_, condition), _, _, JoinKind.Left, false) ] -> Assert.True(readsColumn "s_state" condition)
        | other -> failwithf "unexpected %A" other
        Assert.StartsWith(
            "This outer join condition is not supported",
            (fails "SELECT s_name FROM sales LEFT JOIN store ON ss_store = s_id AND ss_qty > 1").Message)
        // RIGHT JOIN is a LEFT JOIN from the other side.
        match joinsOf (translate "SELECT s_name FROM sales RIGHT JOIN store ON ss_store = s_id").Plan with
        | [ (_, _, [ leftKey ], _, JoinKind.Left, _) ] -> Assert.True(readsColumn "s_id" leftKey)
        | other -> failwithf "unexpected %A" other

    [<Fact>]
    let ``USING names its key once`` () =
        Assert.Equal<string list>([ "s_id"; "qty" ], columnsOf "SELECT s_id, qty FROM store JOIN stock USING (s_id)")
        Assert.Equal<string list>([ "s_id"; "s_name"; "s_state"; "qty" ], columnsOf "SELECT * FROM store JOIN stock USING (s_id)")
        Assert.Equal<string list>([ "s_id" ], columnsOf "SELECT stock.s_id FROM store JOIN stock USING (s_id)")
        let e = fails "SELECT s_id FROM store JOIN stock ON store.s_id = stock.s_id"
        Assert.StartsWith("The column name s_id is ambiguous: it could be store.s_id or stock.s_id", e.Message)

    // ----------------------------------------------------------------------------------------------------------
    // Grouping
    // ----------------------------------------------------------------------------------------------------------

    let private groupKeys plan =
        nodes plan |> List.pick (function LogicalPlan.GroupBy(_, keys, _, _, _) -> Some keys | _ -> None)

    [<Fact>]
    let ``GROUP BY reads an input column before a SELECT alias`` () =
        // The input column wins (DuckDB, PostgreSQL): TPC-DS Q8 groups by the whole ca_zip.
        match groupKeys (translate "SELECT substr(s_name, 1, 2) s_name, count(*) n FROM store GROUP BY s_name").Plan with
        | [ ScalarExpr.Alias(ScalarExpr.Column name, _) ] -> Assert.StartsWith("s_name#", name)
        | other -> failwithf "unexpected %A" other
        match groupKeys (translate "SELECT substr(s_name, 1, 2) prefix, count(*) n FROM store GROUP BY prefix").Plan with
        | [ ScalarExpr.Alias(ScalarExpr.Function(ScalarFunction.Slice, _, _), _) ] -> ()
        | other -> failwithf "unexpected %A" other
        match groupKeys (translate "SELECT s_state, count(*) FROM store GROUP BY 1").Plan with
        | [ ScalarExpr.Alias(ScalarExpr.Column name, _) ] -> Assert.StartsWith("s_state#", name)
        | other -> failwithf "unexpected %A" other

    [<Fact>]
    let ``ROLLUP groups by its prefixes and grouping() reads each level`` () =
        let plan = (translate "SELECT s_state, s_name, sum(ss_qty) total, grouping(s_state) g FROM sales JOIN store ON ss_store = s_id GROUP BY ROLLUP (s_state, s_name)").Plan
        match nodes plan |> List.pick (function LogicalPlan.GroupingSets(_, spec) -> Some spec | _ -> None) with
        | spec ->
            Assert.Equal<int list list>([ [ 0; 1 ]; [ 0 ]; [] ], spec.Levels |> List.map fst)
            let literals = spec.Levels |> List.map (fun (_, m) -> m |> Map.toList |> List.map snd)
            Assert.Equal<ScalarExpr list list>(
                [ [ ScalarExpr.Literal(LiteralValue.Int32 0) ]; [ ScalarExpr.Literal(LiteralValue.Int32 0) ]; [ ScalarExpr.Literal(LiteralValue.Int32 1) ] ],
                literals)
        Assert.Equal<(string * LogicalType) list>(
            [ "s_state", LogicalTypes.String; "s_name", LogicalTypes.String; "total", LogicalTypes.Int64; "g", LogicalTypes.Int32 ],
            (translate "SELECT s_state, s_name, sum(ss_qty) total, grouping(s_state) g FROM sales JOIN store ON ss_store = s_id GROUP BY ROLLUP (s_state, s_name)").Columns)

    [<Fact>]
    let ``Aggregates without GROUP BY give one row`` () =
        let plan = (translate "SELECT sum(ss_qty) * 2 AS twice, count(*) FROM sales").Plan
        Assert.False(nodes plan |> List.exists (function LogicalPlan.GroupBy _ -> true | _ -> false))
        Assert.True(nodes plan |> List.exists (function LogicalPlan.Project(_, items) -> items |> List.forall (snd >> ScalarExprOps.containsAggregation) | _ -> false))

    // ----------------------------------------------------------------------------------------------------------
    // ORDER BY, LIMIT, output names
    // ----------------------------------------------------------------------------------------------------------

    let private sortOf plan = nodes plan |> List.pick (function LogicalPlan.Sort(_, orders) -> Some orders | _ -> None)

    [<Fact>]
    let ``ORDER BY takes positions, output names and expressions, nulls smallest by default`` () =
        let orders = sortOf (translate "SELECT s_name AS n, s_state FROM store ORDER BY 2, n DESC").Plan
        Assert.Equal<(bool * bool) list>([ false, false; true, true ], orders |> List.map (fun o -> o.Descending, o.NullsLast))
        Assert.True(readsColumn "s_state" orders.[0].Expr)
        Assert.True(readsColumn "n" orders.[1].Expr)
        // An ORDER BY expression outside the select list sorts by a hidden column, not in the result.
        let hidden = translate "SELECT s_name FROM store ORDER BY s_state NULLS LAST"
        Assert.Equal<string list>([ "s_name" ], hidden.Columns |> List.map fst)
        Assert.True((sortOf hidden.Plan).Head.NullsLast)
        // A SELECT alias inside an ORDER BY expression stands for its expression (TPC-DS Q36).
        Assert.Equal<string list>([ "d" ], columnsOf "SELECT s_id * 2 AS d FROM store ORDER BY CASE WHEN d > 1 THEN d END")
        Assert.StartsWith(
            "With SELECT DISTINCT, ORDER BY expressions must appear in the select list",
            (fails "SELECT DISTINCT s_name FROM store ORDER BY s_state").Message)
        Assert.StartsWith("ORDER BY 3 is not a column position (1 to 2)", (fails "SELECT s_id, s_name FROM store ORDER BY 3").Message)

    [<Fact>]
    let ``LIMIT and OFFSET take literal counts`` () =
        match nodes (translate "SELECT s_id FROM store LIMIT 5 OFFSET 2").Plan |> List.pick (function LogicalPlan.Limit(_, o, c) -> Some(o, c) | _ -> None) with
        | 2L, 5u -> ()
        | other -> failwithf "unexpected %A" other
        Assert.StartsWith("LIMIT must be a whole number written as a literal", (fails "SELECT s_id FROM store LIMIT s_id").Message)

    [<Fact>]
    let ``Output names are aliases, column names or the expression's text, made unique`` () =
        Assert.Equal<string list>([ "s_id"; "s_id_1"; "s_id + 1"; "two" ], columnsOf "SELECT s_id, s_id, s_id + 1, 1 + 1 AS two FROM store")
        Assert.Equal<(string * LogicalType) list>([ "two", LogicalTypes.Int32 ], (translate "SELECT 1 + 1 AS two").Columns)

    // ----------------------------------------------------------------------------------------------------------
    // Set operations and common table expressions
    // ----------------------------------------------------------------------------------------------------------

    [<Fact>]
    let ``Set operations match columns by position`` () =
        let union = translate "SELECT ss_qty FROM sales UNION ALL SELECT s_id FROM store"
        Assert.Equal<(string * LogicalType) list>([ "ss_qty", LogicalTypes.Int64 ], union.Columns)
        Assert.True(nodes union.Plan |> List.exists (function LogicalPlan.Union(_, _, true) -> true | _ -> false))
        // INTERSECT: the left's distinct rows with an equal right row, NULL equal to NULL; each side distinct before
        // the join, and a side that is a set operation's result left as it is.
        let semiOfDistincts plan =
            nodes plan
            |> List.choose (function
                | LogicalPlan.Join(left, right, _, _, kind, _, _, nullsEqual) ->
                    let isDistinct p = match p with LogicalPlan.Distinct _ -> true | _ -> false
                    Some(kind, nullsEqual, isDistinct left, isDistinct right)
                | _ -> None)
        Assert.Equal<(JoinKind * bool * bool * bool) list>(
            [ JoinKind.Semi, true, true, true ],
            semiOfDistincts (translate "SELECT s_id FROM store INTERSECT SELECT ss_store FROM sales").Plan)
        Assert.Equal<(JoinKind * bool * bool * bool) list>(
            [ JoinKind.Anti, true, false, true; JoinKind.Semi, true, true, true ],
            semiOfDistincts (translate "SELECT s_id FROM store INTERSECT SELECT ss_store FROM sales EXCEPT SELECT i_id FROM item").Plan)
        Assert.StartsWith("The two sides of UNION give 2 and 1 columns", (fails "SELECT s_id, s_name FROM store UNION SELECT i_id FROM item").Message)

    [<Fact>]
    let ``Each reference to a common table expression is the same subplan`` () =
        let plan = (translate "WITH c AS (SELECT s_id FROM store WHERE s_state = 'CA') SELECT a.s_id FROM c a, c b WHERE a.s_id = b.s_id").Plan
        match joinsOf plan with
        | [ (LogicalPlan.Project(left, _), LogicalPlan.Project(right, _), _, _, JoinKind.Inner, _) ] -> Assert.Equal(left, right)
        | other -> failwithf "unexpected %A" other

    let private sharedOf (plan: LogicalPlan) =
        nodes plan |> List.choose (function LogicalPlan.Shared(id, body) -> Some(id, body) | _ -> None)

    [<Fact>]
    let ``Filtered references share a common table expression`` () =
        // The optimizer filters the shared plan by what they read (SharedConsumerFilters).
        let plan =
            (translate "WITH c AS (SELECT ss_store, sum(ss_qty) AS q FROM sales GROUP BY ss_store) SELECT a.q, b.q FROM c a, c b WHERE a.ss_store = b.ss_store AND a.ss_store = 1 AND b.ss_store = 2").Plan
        match sharedOf plan with
        | [ (one, first); (other, second) ] when one = other -> Assert.Equal(first, second)
        | other -> failwithf "unexpected %A" other

    [<Fact>]
    let ``A derived table written twice is one shared plan, one written once is not shared`` () =
        let twice =
            (translate "SELECT a.ss_store FROM (SELECT ss_store, sum(ss_qty) AS q FROM sales GROUP BY ss_store) a, (SELECT ss_store, sum(ss_qty) AS q FROM sales GROUP BY ss_store) b WHERE a.ss_store = b.ss_store AND a.q > b.q").Plan
        match sharedOf twice with
        | [ (one, first); (other, second) ] when one = other -> Assert.Equal(first, second)
        | other -> failwithf "unexpected %A" other
        let different =
            (translate "SELECT a.ss_store FROM (SELECT ss_store, sum(ss_qty) AS q FROM sales GROUP BY ss_store) a, (SELECT ss_store, max(ss_qty) AS q FROM sales GROUP BY ss_store) b WHERE a.ss_store = b.ss_store").Plan
        Assert.Empty(sharedOf different)

    [<Fact>]
    let ``Blocks joining the same tables on the same equalities share one join, each filtering it`` () =
        // TPC-DS Q88's shape: counts over one join, each with its own conditions.
        let block filter = $"(SELECT count(*) AS n FROM sales, store WHERE ss_store = s_id AND {filter})"
        let ca, qty = block "s_state = 'CA'", block "ss_qty > 2 AND s_name = 'x'"
        let plan = (translate $"SELECT * FROM {ca} a, {qty} b").Plan
        match sharedOf plan with
        | [ (one, first); (other, second) ] when one = other ->
            Assert.Equal(first, second)
            // The shared join is unfiltered; each block filters it.
            Assert.Equal<JoinKind list>([ JoinKind.Inner ], kinds first)
            Assert.DoesNotContain(nodes first, (function LogicalPlan.Filter _ -> true | _ -> false))
        | other -> failwithf "unexpected %A" other
        let filters = nodes plan |> List.choose (function LogicalPlan.Filter(LogicalPlan.Shared _, p) | LogicalPlan.Filter(LogicalPlan.Project(LogicalPlan.Shared _, _), p) -> Some p | _ -> None)
        Assert.Equal(2, filters.Length)
        Assert.True(readsColumn "s_state" filters.[0])
        Assert.True(readsColumn "ss_qty" filters.[1] && readsColumn "s_name" filters.[1])
        // The key equality written the other way round is the same join.
        let swapped = (translate $"SELECT * FROM {ca} a, (SELECT count(*) AS n FROM sales, store WHERE s_id = ss_store) b").Plan
        Assert.Equal(2, (sharedOf swapped).Length)

    [<Fact>]
    let ``A join is shared only with blocks joining alike, of tables, on conditions of the row`` () =
        let shares text = not (sharedOf (translate text).Plan).IsEmpty
        // Once: no sharing.
        Assert.False(shares "SELECT count(*) AS n FROM sales, store WHERE ss_store = s_id AND s_state = 'CA'")
        // Other keys, other tables, another table order.
        Assert.False(shares "SELECT * FROM (SELECT count(*) AS n FROM sales, store WHERE ss_store = s_id) a, (SELECT count(*) AS n FROM sales, store WHERE ss_item = s_id) b")
        Assert.False(shares "SELECT * FROM (SELECT count(*) AS n FROM sales, store WHERE ss_store = s_id) a, (SELECT count(*) AS n FROM sales, item WHERE ss_item = i_id) b")
        Assert.False(shares "SELECT * FROM (SELECT count(*) AS n FROM sales, store WHERE ss_store = s_id) a, (SELECT count(*) AS n FROM store, sales WHERE ss_store = s_id) b")
        // Tables joined by no equality (a cross join), an outer join, a subquery in a condition.
        Assert.False(shares "SELECT * FROM (SELECT count(*) AS n FROM sales, store WHERE ss_qty = 1) a, (SELECT count(*) AS n FROM sales, store WHERE ss_qty = 2) b")
        Assert.False(shares "SELECT * FROM (SELECT count(*) AS n FROM sales LEFT JOIN store ON ss_store = s_id) a, (SELECT count(*) AS n FROM sales LEFT JOIN store ON ss_store = s_id WHERE ss_qty = 2) b")
        Assert.False(shares "SELECT * FROM (SELECT count(*) AS n FROM sales, store WHERE ss_store = s_id AND ss_item IN (SELECT i_id FROM item)) a, (SELECT count(*) AS n FROM sales, store WHERE ss_store = s_id) b")
        // Derived tables are not tables: two blocks joining different ones to the same table do not share.
        Assert.False(shares "SELECT * FROM (SELECT count(*) AS n FROM (SELECT ss_store AS k FROM sales WHERE ss_qty = 1) d, store WHERE d.k = s_id) a, (SELECT count(*) AS n FROM (SELECT ss_store AS k FROM sales WHERE ss_qty = 2) d, store WHERE d.k = s_id) b")
        // A block inside a subquery is left as bound.
        Assert.False(shares "SELECT s_name FROM store WHERE s_id IN (SELECT ss_store FROM sales, item WHERE ss_item = i_id AND i_name = 'a') AND s_id IN (SELECT ss_store FROM sales, item WHERE ss_item = i_id AND i_name = 'b')")

    [<Fact>]
    let ``Filters that select branches of a union take copies of the common table expression`` () =
        let text =
            "WITH c AS (SELECT ss_store AS k, ss_qty AS q, 's' AS t FROM sales UNION ALL SELECT s_id, 1, 'x' FROM store) SELECT a.q, b.q FROM c a, c b WHERE a.k = b.k AND a.t = 's' AND b.t = 'x'"
        Assert.Empty(sharedOf (translate text).Plan)

    // ----------------------------------------------------------------------------------------------------------
    // Subqueries
    // ----------------------------------------------------------------------------------------------------------

    [<Fact>]
    let ``Subqueries become joins`` () =
        let exists = (translate "SELECT s_name FROM store WHERE EXISTS (SELECT 1 FROM sales WHERE ss_store = s_id)").Plan
        Assert.Contains(JoinKind.Semi, kinds exists)
        Assert.False(LogicalPlanOps.containsSubqueries exists)
        let scalar = (translate "SELECT s_name FROM store WHERE s_id > (SELECT avg(ss_qty) FROM sales WHERE ss_store = s_id)").Plan
        Assert.False(LogicalPlanOps.containsSubqueries scalar)
        // IN under OR (TPC-DS Q45).
        let underOr = (translate "SELECT s_name FROM store WHERE s_state = 'CA' OR s_id IN (SELECT ss_store FROM sales)").Plan
        Assert.False(LogicalPlanOps.containsSubqueries underOr)
        // NOT IN by SQL's rules (null when no value matches and one is null).
        Assert.False(LogicalPlanOps.containsSubqueries (translate "SELECT s_name FROM store WHERE s_id NOT IN (SELECT ss_store FROM sales)").Plan)
        Assert.False(LogicalPlanOps.containsSubqueries (translate "SELECT s_name, s_id > ALL (SELECT ss_store FROM sales) AS top FROM store").Plan)

    [<Fact>]
    let ``A subquery over groups reads the grouping keys of the query around`` () =
        // In SELECT, HAVING and ORDER BY of a grouping query the subquery is evaluated per group: its outer column is
        // the key's column, which the plan's grouping gives.
        let plan = (translate "SELECT ss_store, (SELECT count(*) FROM store WHERE s_id = sales.ss_store) AS n FROM sales GROUP BY ss_store HAVING EXISTS (SELECT 1 FROM store WHERE s_id = ss_store) ORDER BY (SELECT max(s_name) FROM store WHERE s_id = ss_store)").Plan
        Assert.False(LogicalPlanOps.containsSubqueries plan)
        // In an aggregate's argument it is evaluated per input row, over the input's columns.
        let perRow = (translate "SELECT ss_store, sum((SELECT count(*) FROM item WHERE i_id = ss_item)) AS n FROM sales GROUP BY ss_store").Plan
        Assert.False(LogicalPlanOps.containsSubqueries perRow)
        let e = fails "SELECT ss_store, (SELECT count(*) FROM item WHERE i_id = sales.ss_item) AS n FROM sales GROUP BY ss_store"
        Assert.StartsWith("The subquery reads sales.ss_item, which must appear in GROUP BY or be used in an aggregate function", e.Message)
        Assert.StartsWith("The subquery reads sales.ss_item", (fails "SELECT count(*) AS c, (SELECT count(*) FROM item WHERE i_id = ss_item) AS n FROM sales").Message)
        Assert.StartsWith(
            "max() over columns of the query around is that query's aggregate",
            (fails "SELECT ss_store, (SELECT max(i_id) FROM item WHERE i_id < max(ss_qty)) AS m FROM sales GROUP BY ss_store").Message)
        // An aggregate of the subquery's own columns stays refused where it is.
        Assert.StartsWith("Aggregate functions are not allowed in WHERE", (fails "SELECT s_name FROM store WHERE s_id IN (SELECT ss_store FROM sales WHERE max(ss_qty) > 1)").Message)

    [<Fact>]
    let ``A window over the RANGE frame is computed by peers, joined back`` () =
        let plan = (translate "SELECT ss_store, sum(ss_qty) OVER (PARTITION BY ss_store ORDER BY ss_item) AS s FROM sales").Plan
        // Grouped by partition and order keys, joined back with nulls matching.
        Assert.Contains(nodes plan, (function LogicalPlan.GroupBy(_, keys, _, _, _) -> keys.Length = 2 | _ -> false))
        match joinsOf plan with
        | [ (_, _, leftOn, _, JoinKind.Left, true) ] -> Assert.Equal(2, leftOn.Length)
        | other -> failwithf "unexpected %A" other
        Assert.StartsWith("count(DISTINCT ...) OVER (ORDER BY ...) is not supported", (fails "SELECT count(DISTINCT ss_qty) OVER (ORDER BY ss_item) AS c FROM sales").Message)
        Assert.StartsWith("stddev() OVER (ORDER BY ...) is not supported", (fails "SELECT stddev(ss_qty) OVER (ORDER BY ss_item) AS c FROM sales").Message)
        // Frames: refused shapes, each by name.
        let frame text = (fails $"SELECT sum(ss_qty) OVER ({text}) AS c FROM sales").Message
        Assert.StartsWith("A GROUPS offset must be a whole number", frame "ORDER BY ss_item GROUPS BETWEEN 1.5 PRECEDING AND CURRENT ROW")
        Assert.StartsWith("A RANGE frame with an offset needs exactly one ORDER BY key", frame "ORDER BY ss_item, ss_store RANGE 1 PRECEDING")
        // RANGE from the partition's start to an offset: the running values of the last peers in reach, by an as-of
        // join of the groups of peers with themselves.
        let reach = (translate "SELECT sum(ss_qty) OVER (ORDER BY ss_item RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS c FROM sales").Plan
        Assert.Contains(nodes reach, (function LogicalPlan.JoinAsOf _ -> true | _ -> false))
        // first_value IGNORE NULLS over ROWS from the start: a running count of the values, no peers joined back.
        Assert.Empty(joinsOf (translate "SELECT first_value(ss_qty IGNORE NULLS) OVER (ORDER BY ss_item ROWS UNBOUNDED PRECEDING) AS c FROM sales").Plan)
        Assert.StartsWith("IGNORE NULLS is for lag(), lead(), first_value() and last_value(), not sum()", (fails "SELECT sum(ss_qty IGNORE NULLS) OVER (ORDER BY ss_item) AS c FROM sales").Message)
        Assert.StartsWith("IGNORE NULLS is for lag()", (fails "SELECT rank() IGNORE NULLS OVER (ORDER BY ss_item) AS c FROM sales").Message)
        Assert.StartsWith("This frame starts after it ends", frame "ORDER BY ss_item ROWS BETWEEN 1 FOLLOWING AND 1 PRECEDING")
        Assert.StartsWith("A ROWS frame needs ORDER BY", frame "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW")
        Assert.StartsWith("The offset 0.005 has more decimals than the ORDER BY key's 2", frame "ORDER BY ss_price RANGE BETWEEN 0.005 PRECEDING AND CURRENT ROW")
        Assert.StartsWith("A RANGE frame with an offset needs a number, a date or a timestamp", (fails "SELECT sum(s_id) OVER (ORDER BY s_name RANGE 1 PRECEDING) AS c FROM store").Message)
        Assert.StartsWith("lag() needs ORDER BY", (fails "SELECT lag(ss_qty) OVER () AS c FROM sales").Message)
        Assert.StartsWith("lead() needs OVER", (fails "SELECT lead(ss_qty) AS c FROM sales").Message)

    [<Fact>]
    let ``VALUES gives rows of columns col0, col1 in the type their values meet in`` () =
        let sql = translate "SELECT * FROM (VALUES (1, NULL), (2.5, 'b')) v"
        Assert.Equal<string list>([ "col0"; "col1" ], sql.Columns |> List.map fst)
        // INTEGER meets DECIMAL(2,1) in DECIMAL(11,1).
        Assert.Equal<string list>([ "Decimal128(11, 1)"; "StringView" ], sql.Columns |> List.map (snd >> fun t -> t.Display))
        // Each row is given in the column's type: the union's sides have one schema.
        match nodes (translate "VALUES (1, DATE '2000-01-01'), (2.5, '2000-01-02')").Plan |> List.choose (function LogicalPlan.Union(l, r, _) -> Some(l, r) | _ -> None) with
        | [ (l, r) ] ->
            let types (p: LogicalPlan) = (SchemaInference.inferPlanSchema p).Fields |> List.map (fun f -> f.DataType.Display)
            Assert.Equal<string list>(types l, types r)
        | other -> failwithf "unexpected %A" other
        Assert.StartsWith("Each row of VALUES must have 2 values, not 1", (fails "VALUES (1, 2), (3)").Message)
        Assert.StartsWith("Column 1 of VALUES holds values of types", (fails "VALUES (1), ('a')").Message)

    [<Fact>]
    let ``Intervals of months move dates; left and right take a length`` () =
        let sql = translate "SELECT DATE '2000-01-31' + INTERVAL '1' MONTH AS a, INTERVAL '2' YEAR + DATE '2000-02-29' AS b"
        Assert.Equal<string list>([ "Date32"; "Date32" ], sql.Columns |> List.map (snd >> fun t -> t.Display))
        Assert.StartsWith("An interval of months or years is only added to or subtracted from a date", (fails "SELECT ss_qty + INTERVAL '1' MONTH AS a FROM sales").Message)
        Assert.StartsWith("INTERVAL ... WEEK is not supported", (fails "SELECT DATE '2000-01-31' + INTERVAL '1' WEEK AS a").Message)
        Assert.StartsWith("A negative length in left() is not supported", (fails "SELECT left(s_name, -1) AS a FROM store").Message)
        Assert.StartsWith("isnan() takes a number", (fails "SELECT isnan(s_name) AS a FROM store").Message)

    // ----------------------------------------------------------------------------------------------------------
    // Errors
    // ----------------------------------------------------------------------------------------------------------

    [<Fact>]
    let ``Errors name the table, the alias or the clause`` () =
        let e = fails "SELECT * FROM stor"
        Assert.StartsWith("Unknown table stor (line 1, column 15)", e.Message)
        Assert.Contains("Did you mean store?", e.Message)
        Assert.StartsWith("The name s is given to two tables in this FROM", (fails "SELECT * FROM store s, item s").Message)
        Assert.StartsWith("SELECT * cannot be used with GROUP BY", (fails "SELECT * FROM store GROUP BY s_id").Message)
        Assert.StartsWith("The column s_name must appear in GROUP BY", (fails "SELECT s_name FROM store GROUP BY s_id").Message)
        Assert.StartsWith("Aggregate functions are not allowed in WHERE", (fails "SELECT s_name FROM store WHERE count(*) > 1").Message)
        Assert.StartsWith("WHERE needs a condition", (fails "SELECT s_name FROM store WHERE s_id").Message)
