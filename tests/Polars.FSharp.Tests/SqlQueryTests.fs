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
        // INTERSECT: the left's distinct rows with an equal right row, NULL equal to NULL.
        match nodes (translate "SELECT s_id FROM store INTERSECT SELECT ss_store FROM sales").Plan |> List.tryPick (function LogicalPlan.Distinct(LogicalPlan.Join(_, _, _, _, kind, _, _, nullsEqual), _, _, _) -> Some(kind, nullsEqual) | _ -> None) with
        | Some(JoinKind.Semi, true) -> ()
        | other -> failwithf "unexpected %A" other
        Assert.StartsWith("The two sides of UNION give 2 and 1 columns", (fails "SELECT s_id, s_name FROM store UNION SELECT i_id FROM item").Message)

    [<Fact>]
    let ``Each reference to a common table expression is the same subplan`` () =
        let plan = (translate "WITH c AS (SELECT s_id FROM store WHERE s_state = 'CA') SELECT a.s_id FROM c a, c b WHERE a.s_id = b.s_id").Plan
        match joinsOf plan with
        | [ (LogicalPlan.Project(left, _), LogicalPlan.Project(right, _), _, _, JoinKind.Inner, _) ] -> Assert.Equal(left, right)
        | other -> failwithf "unexpected %A" other

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
        Assert.StartsWith("This subquery is not supported", (fails "SELECT s_name FROM store WHERE s_id NOT IN (SELECT ss_store FROM sales)").Message)

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
