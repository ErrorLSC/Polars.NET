namespace Polars.FSharp.Tests

open System
open Xunit
open Kaminari.Samidare.Sql

/// The SQL parser: how it groups what a recursive-descent reader can get wrong, what it keeps of the text for the
/// binder, and its error messages. Grouping is checked through SqlFormat, which parenthesizes every compound
/// expression; the TPC-DS texts are checked against DuckDB (Polars.CSharp.Linq.Tests, TpcdsSqlParseTests).
module SqlParserTests =

    let private query text = SqlParser.parseQuery SqlDialect.Standard text
    let private formatted text = SqlFormat.query (query text)
    let private expr text = SqlFormat.expression (SqlParser.parseExpression SqlDialect.Standard text)

    let private fails (text: string) =
        Assert.Throws<SqlException>(fun () -> query text |> ignore)

    let private selectOf (q: SqlQuery) =
        match q.Body with
        | SqlQueryBody.Select s -> s
        | other -> failwithf "not a SELECT: %A" other

    let private firstItem text =
        match (selectOf (query text)).Items with
        | SqlSelectItem.Expr(e, _) :: _ -> e
        | other -> failwithf "unexpected items %A" other

    // ----------------------------------------------------------------------------------------------------------
    // Precedence
    // ----------------------------------------------------------------------------------------------------------

    [<Fact>]
    let ``Operators group by precedence, loosest first`` () =
        Assert.Equal("(a OR (b AND (NOT (c = ((d + (e * (-f))) || g)))))", expr "a OR b AND NOT c = d + e * -f || g")
        Assert.Equal("((a - b) - c)", expr "a - b - c")
        Assert.Equal("((NOT a) AND b)", expr "NOT a AND b")
        Assert.Equal("((a / b) % c)", expr "a / b % c")
        Assert.Equal("((a + b) * c)", expr "(a + b) * c")

    [<Fact>]
    let ``BETWEEN takes its AND before a logical AND`` () =
        Assert.Equal("((a BETWEEN 1 AND (2 + 3)) AND b)", expr "a BETWEEN 1 AND 2 + 3 AND b")
        Assert.Equal("((a NOT BETWEEN 1 AND 2) OR b)", expr "a NOT BETWEEN 1 AND 2 OR b")

    [<Fact>]
    let ``Prefix NOT negates a comparison, infix NOT belongs to IN, LIKE and BETWEEN`` () =
        Assert.Equal("(NOT (a IN (1, 2)))", expr "NOT a IN (1, 2)")
        Assert.Equal("(a NOT IN (1, 2))", expr "a NOT IN (1, 2)")
        Assert.Equal("(a NOT LIKE 'x!%%' ESCAPE '!')", expr "a NOT LIKE 'x!%%' ESCAPE '!'")
        Assert.Equal("(NOT (EXISTS (SELECT 1)))", expr "NOT EXISTS (SELECT 1)")
        Assert.Equal("((a IS NOT NULL) AND (NOT (b IS NULL)))", expr "a IS NOT NULL AND NOT b IS NULL")
        Assert.Equal("(a IS NOT DISTINCT FROM (b + 1))", expr "a IS NOT DISTINCT FROM b + 1")

    [<Fact>]
    let ``Comparisons do not chain`` () =
        let e = fails "SELECT a = b = c"
        Assert.StartsWith("Comparisons do not chain: '=' follows a comparison (line 1, column 14)", e.Message)
        Assert.Contains("Add parentheses", e.Message)
        Assert.Equal("((a = b) = c)", expr "(a = b) = c")

    // ----------------------------------------------------------------------------------------------------------
    // Clauses
    // ----------------------------------------------------------------------------------------------------------

    [<Fact>]
    let ``An alias without AS stops at a keyword`` () =
        Assert.Equal(
            "(SELECT a AS x FROM (t AS y LEFT JOIN u ON (y.k = u.k)) WHERE (x > 1)) UNION (SELECT b FROM v) ORDER BY x DESC LIMIT 5",
            formatted "select a x from t y left join u on y.k = u.k where x > 1 union select b from v order by x desc limit 5")
        Assert.Equal("SELECT a FROM t AS s GROUP BY a", formatted "SELECT a FROM t s GROUP BY a")

    [<Fact>]
    let ``JOIN binds before the comma`` () =
        Assert.Equal("SELECT * FROM a, (b JOIN c ON (b.x = c.x)), d", formatted "SELECT * FROM a, b JOIN c ON b.x = c.x, d")
        Assert.Equal("SELECT * FROM ((a CROSS JOIN b) FULL JOIN c USING (k))", formatted "SELECT * FROM a CROSS JOIN b FULL OUTER JOIN c USING (k)")

    [<Fact>]
    let ``A leading parenthesis opens a query, a join or an expression`` () =
        Assert.Equal("SELECT * FROM (SELECT 1) AS s, (t JOIN u ON (t.a = u.a))", formatted "SELECT * FROM (SELECT 1) s, (t JOIN u ON t.a = u.a)")
        Assert.Equal("SELECT ((SELECT 1) + 1), ((SELECT 2))", formatted "SELECT ((SELECT 1) + 1), ((SELECT 2))")
        Assert.Equal("SELECT * FROM (((SELECT 1)) UNION ALL ((SELECT 2))) AS z", formatted "SELECT * FROM ((SELECT 1) UNION ALL (SELECT 2)) z")
        Assert.Equal("SELECT (a IN (SELECT b FROM t))", formatted "SELECT a IN (SELECT b FROM t)")
        Assert.Equal("SELECT (a IN (((SELECT b FROM t)) UNION ((SELECT c FROM u))))", formatted "SELECT a IN ((SELECT b FROM t) UNION (SELECT c FROM u))")

    [<Fact>]
    let ``INTERSECT binds before UNION, and ORDER BY and LIMIT end the whole set operation`` () =
        Assert.Equal(
            "(SELECT 1) UNION ((SELECT 2) INTERSECT (SELECT 3)) ORDER BY 1 LIMIT 2",
            formatted "SELECT 1 UNION SELECT 2 INTERSECT SELECT 3 ORDER BY 1 LIMIT 2")
        Assert.Equal("((SELECT 1) EXCEPT ALL (SELECT 2)) UNION (SELECT 3)", formatted "SELECT 1 EXCEPT ALL SELECT 2 UNION DISTINCT SELECT 3")

    [<Fact>]
    let ``GROUP BY reads sets, ROLLUP, CUBE and GROUPING SETS`` () =
        let s = selectOf (query "SELECT a FROM t GROUP BY ROLLUP (a, b), (c, d), (e + 1) * 2, (f), ()")
        match s.GroupBy with
        | [ SqlGroupingItem.Rollup([ SqlGroupingItem.Expr _; SqlGroupingItem.Expr _ ], _)
            SqlGroupingItem.Set([ _; _ ], _)
            SqlGroupingItem.Expr { Kind = SqlExprKind.Binary(SqlBinaryOp.Multiply, _, _) }
            SqlGroupingItem.Expr { Kind = SqlExprKind.Column _ }
            SqlGroupingItem.Set([], _) ] -> ()
        | other -> failwithf "unexpected %A" other
        Assert.Equal(
            "SELECT a FROM t GROUP BY GROUPING SETS ((a, b), a, ()), CUBE (a, b)",
            formatted "SELECT a FROM t GROUP BY GROUPING SETS ((a, b), (a), ()), CUBE (a, b)")

    [<Fact>]
    let ``WITH, derived tables without an alias, OFFSET and FETCH`` () =
        Assert.Equal(
            "WITH c(x) AS (SELECT 1) SELECT * FROM c, (SELECT 2) LIMIT 3 OFFSET 1",
            formatted "WITH c (x) AS (SELECT 1) SELECT * FROM c, (SELECT 2) OFFSET 1 ROWS FETCH FIRST 3 ROWS ONLY")
        match (selectOf (query "SELECT * FROM (SELECT 1)")).From with
        | [ SqlTableRef.Derived(_, None, _) ] -> ()
        | other -> failwithf "unexpected %A" other

    // ----------------------------------------------------------------------------------------------------------
    // What the tree keeps for the binder
    // ----------------------------------------------------------------------------------------------------------

    [<Fact>]
    let ``Numbers keep their text and kind`` () =
        let items = (selectOf (query "SELECT 1, 1.000, .5, 2., 1e3, 2.5E-2")).Items
        let literals =
            items |> List.map (function
                | SqlSelectItem.Expr({ Kind = SqlExprKind.Literal l }, _) -> l
                | other -> failwithf "unexpected %A" other)
        Assert.Equal<SqlLiteral list>(
            [ SqlLiteral.Integer "1"; SqlLiteral.Decimal "1.000"; SqlLiteral.Decimal ".5"; SqlLiteral.Decimal "2."
              SqlLiteral.Float "1e3"; SqlLiteral.Float "2.5E-2" ],
            literals)

    [<Fact>]
    let ``Names remember whether they were quoted`` () =
        match (selectOf (query "SELECT T.\"Mixed \"\"q\"\"\" AS \"30 days\", b x FROM t AS T")).Items with
        | [ SqlSelectItem.Expr({ Kind = SqlExprKind.Column [ t; c ] }, Some quotedAlias); SqlSelectItem.Expr(_, Some plainAlias) ] ->
            Assert.Equal(("T", false), (t.Text, t.Quoted))
            Assert.Equal(("Mixed \"q\"", true), (c.Text, c.Quoted))
            Assert.Equal(("30 days", true), (quotedAlias.Text, quotedAlias.Quoted))
            Assert.Equal(("x", false), (plainAlias.Text, plainAlias.Quoted))
        | other -> failwithf "unexpected %A" other

    [<Fact>]
    let ``Order items keep written and default directions and null orders`` () =
        let q = query "SELECT a FROM t ORDER BY a, b DESC NULLS FIRST, c ASC NULLS LAST"
        Assert.Equal<(SqlSortDirection * SqlNullOrder) list>(
            [ SqlSortDirection.Default, SqlNullOrder.Default
              SqlSortDirection.Descending, SqlNullOrder.First
              SqlSortDirection.Ascending, SqlNullOrder.Last ],
            q.OrderBy |> List.map (fun o -> o.Direction, o.Nulls))

    [<Fact>]
    let ``Stars, DISTINCT aggregates, casts and windows`` () =
        match (selectOf (query "SELECT *, t.*, count(*), count(DISTINCT x) FROM t")).Items with
        | [ SqlSelectItem.Wildcard(None, _)
            SqlSelectItem.Wildcard(Some [ { Text = "t" } ], _)
            SqlSelectItem.Expr({ Kind = SqlExprKind.Function { Star = true; Arguments = [] } }, None)
            SqlSelectItem.Expr({ Kind = SqlExprKind.Function { Distinct = true; Arguments = [ _ ] } }, None) ] -> ()
        | other -> failwithf "unexpected %A" other
        match (firstItem "SELECT TRY_CAST(a AS DECIMAL(7, 2))").Kind with
        | SqlExprKind.Cast(_, { Name = "DECIMAL"; Parameters = [ 7; 2 ] }, true) -> ()
        | other -> failwithf "unexpected %A" other
        match (firstItem "SELECT CAST(a AS double precision)").Kind with
        | SqlExprKind.Cast(_, { Name = "DOUBLE PRECISION"; Parameters = [] }, false) -> ()
        | other -> failwithf "unexpected %A" other
        Assert.Equal(
            "SELECT sum(x) OVER (PARTITION BY a ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), rank() OVER (ORDER BY y DESC)",
            formatted "SELECT sum(x) OVER (PARTITION BY a ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), rank() OVER (ORDER BY y DESC)")

    [<Fact>]
    let ``Typed literals, CASE and EXTRACT`` () =
        Assert.Equal("(DATE '2000-01-01' + INTERVAL '5' DAY)", expr "date '2000-01-01' + interval '5' day")
        Assert.Equal("(CASE a WHEN 1 THEN 'x' ELSE 'y' END)", expr "CASE a WHEN 1 THEN 'x' ELSE 'y' END")
        Assert.Equal("(CASE WHEN (a > 1) THEN 'it''s' END)", expr "CASE WHEN a > 1 THEN 'it''s' END")
        Assert.Equal("EXTRACT(YEAR FROM d)", expr "extract(year from d)")

    [<Fact>]
    let ``Spans cover the text of each node`` () =
        let text = "SELECT  a + b  FROM t"
        let e = firstItem text
        Assert.Equal("a + b", text.Substring(e.Span.Start, e.Span.Length))
        let q = query "  SELECT 1  "
        Assert.Equal({ Start = 2; Length = 8 }, q.Span)

    // ----------------------------------------------------------------------------------------------------------
    // Errors
    // ----------------------------------------------------------------------------------------------------------

    [<Fact>]
    let ``A misspelt keyword is named`` () =
        let e = fails "SELECT a\nFORM t"
        Assert.Equal(
            String.Join(
                "\n",
                [ "Expected the end of the text after the query, found 't' (line 2, column 6)"
                  "  2 | FORM t"
                  "    |      ^"
                  "Did you mean FROM? 'FORM' was read as an alias of what precedes it." ]),
            e.Message.Replace("\r\n", "\n"))
        Assert.Equal((2, 6), (e.Line, e.Column))
        let e = fails "SELECT a FROM t WHER a > 1"
        Assert.Contains("Did you mean WHERE?", e.Message)
        let e = fails "SELECT a FROM t ORDER a"
        Assert.StartsWith("Expected BY after ORDER, found 'a'", e.Message)
        let e = fails "SELECT a FROM t JOIN u USING k"
        Assert.StartsWith("Expected '(' and the joined columns after USING, found 'k' (line 1, column 30)", e.Message)
        let e = fails "SELECT a FROM t JOIN u ON"
        Assert.StartsWith("Expected an expression, found the end of the text", e.Message)

    [<Fact>]
    let ``Unclosed text and parentheses say where they began`` () =
        let e = fails "SELECT 'abc\nFROM t"
        Assert.StartsWith("Unterminated string: no closing ' (line 1, column 8)", e.Message)
        let e = fails "SELECT (a + b FROM t"
        Assert.StartsWith("Expected ')' to close the '(' at line 1, column 8, found 'FROM'", e.Message)
        let e = fails "SELECT a /* note"
        Assert.StartsWith("Unterminated comment", e.Message)
        let e = fails "SELECT 1a"
        Assert.StartsWith("A number cannot run into a name: '1a'", e.Message)

    [<Fact>]
    let ``A keyword cannot be a name unquoted`` () =
        let e = fails "SELECT a FROM select"
        Assert.StartsWith("Expected a name for a table, found the keyword SELECT", e.Message)
        Assert.Contains("\"select\"", e.Message)
        Assert.Equal("SELECT a FROM \"select\"", formatted "SELECT a FROM \"select\"")

    [<Fact>]
    let ``Unsupported forms are refused by name`` () =
        Assert.StartsWith("NATURAL joins are not supported", (fails "SELECT * FROM a NATURAL JOIN b").Message)
        Assert.StartsWith("Named windows are not supported", (fails "SELECT sum(x) OVER w FROM t").Message)
        Assert.StartsWith("Row values are not supported", (fails "SELECT (a, b) FROM t").Message)
        Assert.StartsWith("Table functions are not supported", (fails "SELECT * FROM read_parquet('x')").Message)

    [<Fact>]
    let ``Backticks quote names in the Spark expression dialect only`` () =
        let spark = SqlParser.parseExpression SqlDialect.SparkExpression "`a b` > 0"
        Assert.Equal("(\"a b\" > 0)", SqlFormat.expression spark)
        let e = Assert.Throws<SqlException>(fun () -> SqlParser.parseExpression SqlDialect.Standard "`a b` > 0" |> ignore)
        Assert.StartsWith("Unexpected character '`'", e.Message)
        Assert.Contains("double quotes", e.Message)

    [<Fact>]
    let ``Nesting beyond the limit is refused, not a stack overflow`` () =
        let deep = "SELECT " + String('(', 1000) + "1" + String(')', 1000)
        Assert.StartsWith("The text nests deeper than", (fails deep).Message)
        let deepQuery = String.replicate 300 "SELECT * FROM (" + "SELECT 1" + String(')', 300)
        Assert.StartsWith("The text nests deeper than", (fails deepQuery).Message)
        let rollups = "SELECT a FROM t GROUP BY " + String.replicate 1000 "ROLLUP (" + "a" + String(')', 1000)
        Assert.StartsWith("The text nests deeper than", (fails rollups).Message)

    /// A parenthesis before SELECT is read ahead once per position: nested scalar subqueries in parentheses take
    /// linear time, not time doubling per level.
    [<Fact>]
    let ``Parenthesized subqueries nest in linear time`` () =
        let nested levels = "SELECT " + String.replicate levels "((SELECT " + "1" + String.replicate levels "))"
        let watch = Diagnostics.Stopwatch.StartNew()
        let q = query (nested 40)
        Assert.True(watch.ElapsedMilliseconds < 2000L, $"took {watch.ElapsedMilliseconds} ms")
        Assert.StartsWith("SELECT ((SELECT ((SELECT", SqlFormat.query q)
