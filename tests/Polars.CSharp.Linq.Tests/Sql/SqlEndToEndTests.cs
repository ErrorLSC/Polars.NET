#nullable enable
using DuckDB.NET.Data;
using Kaminari.Samidare.Sql;
using Polars.NET.Core.Linq.Backend;

namespace Polars.CSharp.Linq.Tests.Sql;

/// <summary>
/// SQL semantics TPC-DS does not reach, run through the SQL frontend on Polars against DuckDB over the same small
/// tables (NULLs, NaN, ties, empty groups): window frames, set operations, division by zero, DISTINCT counts, LIKE,
/// ranks, outer joins, grouping sets, casts and string functions. Rows compare as in TpcdsSqlResultTests.
/// </summary>
[Trait("Category", "Sql")]
public class SqlEndToEndTests
{
    /// Two tables written as Parquet by DuckDB, read by both sides.
    static readonly Lazy<(string Dir, ISqlCatalog Catalog)> Data = new(() =>
    {
        var dir = Directory.CreateTempSubdirectory("sql-e2e-").FullName;
        using (var connection = new DuckDBConnection("Data Source=:memory:"))
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = $"""
                CREATE TABLE t AS SELECT * FROM (VALUES
                    (1, 1, 'apple', 1.50::DECIMAL(7,2), 2.0::DOUBLE, 1),
                    (1, NULL, 'banana', NULL, NULL, 2),
                    (1, 2, NULL, 2.25, 5.0, 3),
                    (2, 2, 'apricot', 0.00, 'NaN'::DOUBLE, 1),
                    (2, NULL, 'cherry', 4.10, 1.0, 2),
                    (3, 3, 'a_b%c', NULL, NULL, 1)
                ) v(g, k, s, d, x, o);
                CREATE TABLE u AS SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (NULL, 'none'), (2, 'deux')) v(k, label);
                COPY t TO '{dir}/t.parquet' (FORMAT parquet);
                COPY u TO '{dir}/u.parquet' (FORMAT parquet);
                """;
            command.ExecuteNonQuery();
        }
        var catalog = new SqlCatalog();
        foreach (var path in Directory.GetFiles(dir, "*.parquet"))
            catalog.Register(Path.GetFileNameWithoutExtension(path), PolarsTableSources.OfLazyFrame(LazyFrame.ScanParquet(path).Handle));
        return (dir, catalog);
    });

    /// Division by zero is NULL (sql-frontend Q8), decimal or floating; DuckDB's division of decimals is a double's,
    /// infinity by zero, so this one is not compared with DuckDB.
    [Fact]
    public void Division_by_zero_is_null()
    {
        var rows = TpcdsSqlResultTests.Run(Data.Value.Catalog, "SELECT d / (o - 1) AS q, x / (o - 1) AS f, g % (o - 1) AS r FROM t WHERE o = 1 ORDER BY g");
        Assert.All(rows, row => Assert.All(row, Assert.Null));
        Assert.Equal(3, rows.Count);
    }

    /// A sum of quotients that are all null (each a division by zero) is null: the count of a quotient by a column is
    /// the quotient's own, not its operands', which are not null. Not compared with DuckDB, as above.
    [Fact]
    public void Sum_of_quotients_by_zero_is_null()
    {
        var rows = TpcdsSqlResultTests.Run(Data.Value.Catalog, "SELECT o, sum(d / (o - 1)) AS s, count(d / (o - 1)) AS c FROM t GROUP BY o ORDER BY o");
        Assert.Equal(3, rows.Count);
        Assert.Null(rows[0][1]);
        Assert.Equal(0L, Convert.ToInt64(rows[0][2]));
        Assert.NotNull(rows[1][1]);
    }

    [Theory]
    // Windows: OVER (), running aggregates that skip nulls, ranks over nullable keys in mixed directions.
    [InlineData("SELECT g, o, sum(d) OVER () AS total FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, max(d) OVER (PARTITION BY g ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS m, sum(d) OVER (PARTITION BY g ORDER BY o ROWS UNBOUNDED PRECEDING) AS c FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, rank() OVER (PARTITION BY g ORDER BY k DESC, s) AS r, dense_rank() OVER (ORDER BY k) AS dr, row_number() OVER (ORDER BY g DESC, o) AS rn FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, avg(sum(d)) OVER (PARTITION BY g) AS a FROM t GROUP BY g, o ORDER BY g, o")]
    // Set operations: NULL equals NULL, duplicates removed.
    [InlineData("SELECT k FROM t UNION SELECT k FROM u ORDER BY k")]
    [InlineData("SELECT k FROM t INTERSECT SELECT k FROM u ORDER BY k")]
    [InlineData("SELECT k FROM u EXCEPT SELECT k FROM t ORDER BY k")]
    [InlineData("SELECT k FROM t INTERSECT SELECT k FROM u EXCEPT SELECT o FROM t ORDER BY k")]
    [InlineData("SELECT g, k FROM t INTERSECT SELECT k, k FROM u ORDER BY g, k")]
    // Aggregates: DISTINCT counts skip NULL, empty sums are NULL, MIN and MAX put NaN above every number.
    [InlineData("SELECT g, count(DISTINCT k) AS n, count(k) AS c, count(*) AS r, sum(d) AS s, min(x) AS lo, max(x) AS hi, avg(d) AS a FROM t GROUP BY g ORDER BY g")]
    [InlineData("SELECT g, k, sum(d) AS s, grouping(k) AS gk FROM t GROUP BY ROLLUP (g, k) ORDER BY g, k, gk")]
    [InlineData("SELECT g, sum(d * 2 + o) AS s, sum(k + o) AS t, sum(d * 2 + o) + sum(k * 1) AS u FROM t GROUP BY g ORDER BY g")]
    [InlineData("SELECT avg(d * 2 + o) AS a, sum(d * o) AS s, count(d * o) AS c, count(*) AS n FROM t")]
    [InlineData("SELECT avg(d * 2) AS a FROM t WHERE d IS NULL")]
    // Sums and counts of arithmetic counted from its columns when they give its nulls, not when a divisor may be zero.
    [InlineData("SELECT g, count(d * o) AS c, avg(d + 1) AS a, sum(d / 2) AS h FROM t GROUP BY g ORDER BY g")]
    [InlineData("SELECT g, sum(d * o) AS s, avg(d + o) AS a FROM t GROUP BY ROLLUP (g) ORDER BY g")]
    [InlineData("SELECT o, (SELECT sum(d) FROM t t2 WHERE t2.g = t.g AND t2.k > 5) AS none FROM t ORDER BY g, o")]
    // Expressions: division by zero is NULL, casts round, NULL in || and concat, LIKE with escapes, IN with NULL.
    [InlineData("SELECT CAST(d AS INTEGER) AS i, CAST(-2.5 AS INTEGER) AS m FROM t ORDER BY g, o")]
    [InlineData("SELECT s || '!' AS e, concat(s, '!') AS c, substr(s, 2, 3) AS sub, upper(s) AS up FROM t ORDER BY g, o")]
    [InlineData("SELECT s FROM t WHERE s LIKE 'a%' OR s LIKE '%y' OR s LIKE 'a!_b!%%' ESCAPE '!' OR s LIKE '_an%' ORDER BY s")]
    [InlineData("SELECT o FROM t WHERE k IN (1, NULL) ORDER BY g, o")]
    [InlineData("SELECT g, o FROM t WHERE k NOT IN (2, 3) ORDER BY g, o")]
    [InlineData("SELECT coalesce(k, -1) AS k, nullif(g, 1) AS n FROM t ORDER BY g, o")]
    // Joins: USING, NULL keys never match, an outer join's condition on the joined side.
    [InlineData("SELECT t.g, t.o, u.label FROM t JOIN u USING (k) ORDER BY t.g, t.o, u.label")]
    [InlineData("SELECT t.g, t.o, u.label FROM t LEFT JOIN u ON t.k = u.k AND u.label <> 'deux' ORDER BY t.g, t.o, u.label")]
    [InlineData("SELECT u.label, t.o FROM t RIGHT JOIN u ON t.k = u.k ORDER BY u.label, t.o")]
    // Constant filters: a channel picked out of a union of marked branches, a count over a branch kept empty.
    [InlineData("WITH c AS (SELECT g, sum(d) AS v, 'x' AS ch FROM t GROUP BY g UNION ALL SELECT k AS g, count(*) AS v, 'y' AS ch FROM u GROUP BY k) SELECT a.g, a.v, b.v AS w FROM c a JOIN c b ON a.g = b.g WHERE a.ch = 'y' AND b.ch = 'x' ORDER BY a.g")]
    [InlineData("SELECT count(*) AS n, sum(d) AS s, 'x' AS ch FROM t WHERE 1 = 2 UNION ALL SELECT count(*), sum(d), 'y' FROM t WHERE 1999 + 2 = 2001 ORDER BY ch")]
    [InlineData("SELECT * FROM (SELECT g, 'x' AS ch FROM t UNION ALL SELECT k, 'y' FROM u) v WHERE ch = 'z' ORDER BY g")]
    // Blocks joining the same tables alike share one join (TPC-DS Q88): each its own filter, one with none, one testing
    // both tables; keys with nulls never match.
    [InlineData("SELECT * FROM (SELECT count(*) AS a FROM t, u WHERE t.k = u.k AND t.g = 1) x, (SELECT count(*) AS b FROM t, u WHERE t.k = u.k AND u.label <> 'deux') y, (SELECT sum(t.d) AS c, count(*) AS n FROM t, u WHERE u.k = t.k) z")]
    [InlineData("SELECT * FROM (SELECT count(*) AS a FROM t, u WHERE t.k = u.k AND (t.o = 1 OR u.label = 'deux')) x, (SELECT max(t.s) AS b FROM t, u WHERE t.k = u.k AND t.o > u.k) y")]
    [InlineData("SELECT * FROM (SELECT count(*) AS a, max(q.s) AS m FROM t p, t q WHERE p.g = q.g AND p.o = 1) x, (SELECT count(*) AS b, max(p.s) AS m FROM t p, t q WHERE p.g = q.g AND q.o = 2) y")]
    [InlineData("SELECT * FROM (SELECT count(*) AS a FROM t p, u, t q WHERE p.k = u.k AND q.k = u.k AND p.o = q.o AND p.g = 1) x, (SELECT count(*) AS b FROM t p, u, t q WHERE p.k = u.k AND q.k = u.k AND p.o = q.o AND u.label = 'two') y")]
    // Subqueries over groups: in SELECT, HAVING and ORDER BY a correlated subquery reads the group's keys (with ROLLUP
    // too, where a rolled-up key is null); in an aggregate's argument, the input row's columns.
    [InlineData("SELECT g, (SELECT count(*) FROM u WHERE u.k = t.g) AS n FROM t GROUP BY g ORDER BY g")]
    [InlineData("SELECT g, sum(o) AS s FROM t GROUP BY g HAVING sum(o) > (SELECT count(*) FROM u WHERE u.k = t.g) ORDER BY g")]
    [InlineData("SELECT g FROM t GROUP BY g HAVING EXISTS (SELECT 1 FROM u WHERE u.k = t.g) ORDER BY g")]
    [InlineData("SELECT g, k, (SELECT count(*) FROM u WHERE u.k = t.g) AS n FROM t GROUP BY ROLLUP (g, k) ORDER BY g, k")]
    [InlineData("SELECT g FROM t GROUP BY g ORDER BY (SELECT count(*) FROM u WHERE u.k = t.g) DESC, g")]
    [InlineData("SELECT g AS grp, (SELECT max(label) FROM u WHERE u.k = t.g) AS l, count(*) AS c FROM t GROUP BY grp ORDER BY grp")]
    [InlineData("SELECT g, sum((SELECT count(*) FROM u WHERE u.k = t.k)) AS n, max((SELECT min(label) FROM u WHERE u.k = t.o)) AS m FROM t GROUP BY g ORDER BY g")]
    [InlineData("SELECT count(*) AS c, sum((SELECT count(*) FROM u WHERE u.k = t.k)) AS n FROM t")]
    // SQL's IN and NOT IN as values and under OR: null when no value matches and the target or a value is null, false
    // over no rows; correlated on an expression, the sets are {1, 2}, {NULL, 2} and empty.
    [InlineData("SELECT g, o, k IN (SELECT k FROM u) AS a, k NOT IN (SELECT k FROM u WHERE k IS NOT NULL) AS b FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, o IN (SELECT k FROM u WHERE length(u.label) = t.g + 2) AS a FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o FROM t WHERE o NOT IN (SELECT k FROM u WHERE length(u.label) = t.g + 2) ORDER BY g, o")]
    [InlineData("SELECT g, o FROM t WHERE o = 3 OR k NOT IN (SELECT k FROM u WHERE k IS NOT NULL) ORDER BY g, o")]
    [InlineData("SELECT g, o FROM t WHERE k NOT IN (SELECT k FROM u WHERE k > 10) ORDER BY g, o")]
    [InlineData("SELECT g, o FROM t WHERE k NOT IN (SELECT k FROM u) ORDER BY g, o")]
    // Quantified comparisons: ANY false and ALL true over no rows, null for a null operand or an undecided null
    // value; NaN above every number (MIN and MAX of SQL); correlated.
    [InlineData("SELECT g, o, o > ANY (SELECT k FROM u) AS a, o > ALL (SELECT k FROM u) AS b, o <= ANY (SELECT k FROM u WHERE k IS NOT NULL) AS c, o < ALL (SELECT k FROM u WHERE k > 10) AS d, o >= SOME (SELECT k FROM u WHERE k > 10) AS e FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, o <= ALL (SELECT k FROM u WHERE k IS NOT NULL) AS a, o < ALL (SELECT k FROM u) AS b, o >= ALL (SELECT k FROM u WHERE k IS NOT NULL) AS c, o < ANY (SELECT k FROM u) AS d FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, k = ALL (SELECT k FROM u WHERE k = 2) AS a, k <> ANY (SELECT k FROM u WHERE k IS NOT NULL) AS b, k = ALL (SELECT k FROM u) AS c, k <> ANY (SELECT k FROM u WHERE k = 2) AS d FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, o >= ALL (SELECT t2.o FROM t t2 WHERE t2.g = t.g) AS top, o < ANY (SELECT t2.k FROM t t2 WHERE t2.g = t.g) AS below FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, x > ALL (SELECT x FROM t t2 WHERE t2.g = 1) AS a, x < ANY (SELECT x FROM t t2 WHERE t2.g = 2) AS b, x >= ALL (SELECT x FROM t t2 WHERE t2.g = 2) AS c FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o FROM t WHERE d > ALL (SELECT d FROM t t2 WHERE t2.g = 1 AND t2.d IS NOT NULL) OR o > ANY (SELECT k FROM u) ORDER BY g, o")]
    // RANGE frames (ORDER BY without a frame, or RANGE UNBOUNDED PRECEDING): up to the current row's last peer; peers
    // with null keys together; averages of decimals and of doubles with NaN; several keys and directions.
    [InlineData("SELECT g, o, sum(o) OVER (PARTITION BY g ORDER BY k) AS s, count(k) OVER (PARTITION BY g ORDER BY k) AS c, avg(d) OVER (ORDER BY k) AS a FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, min(d) OVER (ORDER BY g DESC) AS lo, max(x) OVER (ORDER BY k NULLS FIRST) AS hi, avg(x) OVER (ORDER BY g) AS m FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, sum(d) OVER (ORDER BY k DESC NULLS LAST, g) AS s, count(*) OVER (PARTITION BY k ORDER BY g RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c FROM t ORDER BY g, o")]
    [InlineData("SELECT g, sum(o) AS s, sum(sum(o)) OVER (ORDER BY g) AS running, sum(g) OVER (ORDER BY g RANGE UNBOUNDED PRECEDING) * 2 AS twice FROM t GROUP BY g ORDER BY g")]
    [InlineData("SELECT o, count(*) OVER (ORDER BY o) AS c FROM t ORDER BY sum(o) OVER (ORDER BY o) DESC, o")]
    // Running values over keys in mixed directions (the engine orders a window one way: each key ranked in its own).
    [InlineData("SELECT g, o, sum(o) OVER (ORDER BY g DESC, k NULLS LAST ROWS UNBOUNDED PRECEDING) AS r, sum(o) OVER (ORDER BY k DESC NULLS FIRST, o) AS s FROM t ORDER BY g, o")]
    // Running MIN and MAX of doubles keep NaN above every number (a partition starting with NaN, then a number).
    [InlineData("SELECT g, o, min(x) OVER (ORDER BY g DESC, o DESC ROWS UNBOUNDED PRECEDING) AS lo, max(x) OVER (ORDER BY g, o ROWS UNBOUNDED PRECEDING) AS hi, min(x) OVER (PARTITION BY g ORDER BY o ROWS UNBOUNDED PRECEDING) AS plo, max(x) OVER (PARTITION BY g ORDER BY o) AS phi FROM t ORDER BY g, o")]
    // USING: unqualified, the key is the value of the side that has the row (COALESCE for FULL, the right's for RIGHT),
    // in the left key's place for *; qualified, each side's own.
    [InlineData("SELECT * FROM t FULL JOIN u USING (k) ORDER BY k, label, s")]
    [InlineData("SELECT k, t.k AS tk, u.k AS uk, label, count(*) OVER (PARTITION BY k) AS n FROM t FULL JOIN u USING (k) ORDER BY k, label, tk, s")]
    [InlineData("SELECT k, t.k AS tk, u.k AS uk, label FROM t RIGHT JOIN u USING (k) ORDER BY k, label, tk")]
    [InlineData("SELECT * FROM t RIGHT JOIN u USING (k) ORDER BY k, label, s")]
    [InlineData("SELECT k, count(*) AS n FROM t FULL JOIN u USING (k) GROUP BY k ORDER BY k")]
    [InlineData("SELECT k, t.k AS tk, v.k AS vk, label FROM t FULL JOIN (SELECT k + 1 AS k, label FROM u) v USING (k) ORDER BY k, label, tk, o")]
    [InlineData("SELECT * FROM t RIGHT JOIN (SELECT k + 2 AS k, label FROM u) v USING (k) ORDER BY k, label, o")]
    // VALUES: a query, a derived table with column names, a common table expression, an IN list's query, a set
    // operation's side; each column in the type its values meet in.
    [InlineData("VALUES (1, 'a'), (2, NULL), (NULL, 'c') ORDER BY 1")]
    [InlineData("SELECT * FROM (VALUES (1, 2.5), (3, 1)) v ORDER BY col0")]
    [InlineData("WITH v(x, y) AS (VALUES (1, 'one'), (2, 'zwei')) SELECT t.g, v.y FROM t JOIN v ON t.g = v.x ORDER BY t.g, v.y")]
    [InlineData("SELECT g, o FROM t WHERE o IN (VALUES (1), (3)) ORDER BY g, o")]
    [InlineData("SELECT k FROM u UNION ALL VALUES (7) ORDER BY k")]
    [InlineData("SELECT x, count(*) AS n FROM (VALUES (1), (1), (2)) v(x) GROUP BY x ORDER BY x")]
    [InlineData("SELECT col0 FROM (VALUES (DATE '2000-01-01'), ('2000-01-02'), (NULL)) v ORDER BY col0")]
    // Small syntax: SUBSTRING FROM/FOR, IS [NOT] TRUE/FALSE/UNKNOWN, intervals of months and years (a date's day kept,
    // or the month's last; DuckDB gives a timestamp, so both sides cast), unquoted interval numbers, left() and
    // right() (lengths 0 and longer than the text), isnan().
    [InlineData("SELECT g, o, substring(s FROM 2 FOR 3) AS a, substring(s FROM 3) AS b, substring(s, 2) AS c FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, (k > 1) IS TRUE AS a, (k > 1) IS NOT FALSE AS b, (k > 1) IS UNKNOWN AS c, (k > 1) IS NOT TRUE AS d, (k > 1) IS FALSE AS e FROM t ORDER BY g, o")]
    [InlineData("SELECT CAST(CAST('2000-01-31' AS DATE) + INTERVAL '1' MONTH AS DATE) AS a, CAST(CAST('2000-02-29' AS DATE) + INTERVAL '1' YEAR AS DATE) AS b, CAST(CAST('2000-03-31' AS DATE) - INTERVAL '1' MONTH AS DATE) AS c, CAST(INTERVAL 13 MONTH + DATE '2001-01-15' AS DATE) AS d, CAST('2000-01-31' AS DATE) + INTERVAL 30 DAY AS e")]
    [InlineData("SELECT CAST(TIMESTAMP '2000-01-31 10:30:00' + INTERVAL '1' MONTH AS TIMESTAMP) AS a, CAST(TIMESTAMP '2004-02-29 00:00:01' - INTERVAL '4' YEAR AS TIMESTAMP) AS b")]
    [InlineData("SELECT g, o, left(s, 2) AS a, right(s, 2) AS b, left(s, 0) AS c, right(s, 0) AS d, right(s, 10) AS e, left(s, 10) AS f, isnan(x) AS n, isnan(o) AS m FROM t ORDER BY g, o")]
    // Frames bounded at both ends: ROWS (preceding, following, both after or before the row, past the partition) and
    // RANGE over an integer, a decimal (nulls in their own frame), descending; dates and timestamps by intervals.
    [InlineData("SELECT g, o, sum(d) OVER (PARTITION BY g ORDER BY o ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS a, avg(x) OVER (ORDER BY g, o ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS b, count(k) OVER (ORDER BY g, o ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS c, min(s) OVER (ORDER BY g, o ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING) AS e, max(x) OVER (ORDER BY g DESC, o ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS f, count(*) OVER (PARTITION BY g ORDER BY o ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS n FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, sum(o) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS a, count(*) OVER (ORDER BY k DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS b, sum(o) OVER (PARTITION BY g ORDER BY d RANGE BETWEEN 1.5 PRECEDING AND 0.5 FOLLOWING) AS c, max(s) OVER (ORDER BY k NULLS LAST RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS e FROM t ORDER BY g, o")]
    [InlineData("SELECT dt, v, sum(v) OVER (ORDER BY dt RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND CURRENT ROW) AS a, count(*) OVER (ORDER BY dt DESC RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND INTERVAL '3' DAY FOLLOWING) AS b FROM (VALUES (DATE '2000-01-01', 1), (DATE '2000-01-02', 2), (DATE '2000-01-05', 4), (DATE '2000-01-06', 8), (NULL, 16), (DATE '2000-01-06', 32)) v(dt, v) ORDER BY dt, v")]
    [InlineData("SELECT ts, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 2 HOUR PRECEDING AND CURRENT ROW) AS a FROM (VALUES (TIMESTAMP '2000-01-01 10:00:00', 1), (TIMESTAMP '2000-01-01 11:30:00', 2), (TIMESTAMP '2000-01-01 13:00:00', 4)) v(ts, v) ORDER BY ts")]
    // Frames unbounded at one end with an offset at the other (ROWS), and RANGE from the current row's peers to the end.
    [InlineData("SELECT g, o, sum(o) OVER (ORDER BY g, o ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS a, count(k) OVER (ORDER BY g, o ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) AS b, avg(d) OVER (PARTITION BY g ORDER BY o ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS c, max(x) OVER (ORDER BY g, o ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS e, count(*) OVER (ORDER BY g, o ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS n, sum(o) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS r FROM t ORDER BY g, o")]
    // lag and lead (offsets, defaults only outside the partition), first_value and last_value over their frames.
    [InlineData("SELECT g, o, lag(s) OVER (PARTITION BY g ORDER BY o) AS a, lead(d, 2) OVER (ORDER BY g, o) AS b, lag(k, 1, -1) OVER (PARTITION BY g ORDER BY o) AS c, lead(s, 1, 'none') OVER (PARTITION BY g ORDER BY o DESC) AS e, lag(o, 0) OVER (ORDER BY g, o) AS f FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, first_value(s) OVER (PARTITION BY g ORDER BY o) AS a, last_value(s) OVER (PARTITION BY g ORDER BY o) AS b, last_value(s) OVER (PARTITION BY g ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS c, first_value(d) OVER (ORDER BY g, o ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS e, last_value(x) OVER (ORDER BY g, o ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS f, last_value(s) OVER (PARTITION BY g ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS h, first_value(s) OVER (PARTITION BY g ORDER BY o ROWS UNBOUNDED PRECEDING) AS i FROM t ORDER BY g, o")]
    // Over groups, and aggregates the running paths do not compute (stddev, text) over bounded frames.
    [InlineData("SELECT g, sum(o) AS s, sum(sum(o)) OVER (ORDER BY g ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS w, lag(sum(o)) OVER (ORDER BY g) AS p FROM t GROUP BY g ORDER BY g")]
    [InlineData("SELECT g, o, stddev(o) OVER (ORDER BY g, o ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a, max(s) OVER (ORDER BY g, o ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS b, sum(o) OVER (ORDER BY g, o ROWS 2 PRECEDING) AS c FROM t ORDER BY g, o")]
    // GROUPS frames: groups of peers (nulls one group at their end), bounded at both ends, several keys; unbounded at
    // one end with an offset at the other (moved by groups: none before the start, the last past the end).
    [InlineData("SELECT g, o, sum(o) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a, count(*) OVER (ORDER BY k DESC GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS b, sum(d) OVER (PARTITION BY g ORDER BY k GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS c, max(s) OVER (ORDER BY k NULLS LAST GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS e, count(k) OVER (ORDER BY g, k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS f FROM t ORDER BY g, o")]
    [InlineData("SELECT g, o, sum(o) OVER (ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS a, count(k) OVER (ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b, avg(d) OVER (PARTITION BY g ORDER BY k DESC GROUPS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS c, max(x) OVER (ORDER BY k GROUPS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS e, min(x) OVER (ORDER BY g GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS f, count(*) OVER (ORDER BY k NULLS LAST GROUPS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) AS n FROM t ORDER BY g, o")]
    // RANGE unbounded at one end with an offset at the other: up to the last peers in reach (a null key's frame ends at
    // its own peers, wherever nulls go), none in reach counting 0; null partition keys, dates.
    [InlineData("SELECT g, o, sum(o) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS a, count(*) OVER (ORDER BY k NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS b, sum(o) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS c, avg(x) OVER (PARTITION BY g ORDER BY d DESC RANGE BETWEEN UNBOUNDED PRECEDING AND 0.5 FOLLOWING) AS e, count(*) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND 5 PRECEDING) AS f, sum(o) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND 5 PRECEDING) AS j, max(x) OVER (ORDER BY k DESC NULLS FIRST RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS h FROM t ORDER BY g, o")]
    [InlineData("SELECT p, dt, v, sum(v) OVER (PARTITION BY p ORDER BY dt RANGE BETWEEN UNBOUNDED PRECEDING AND INTERVAL 1 DAY FOLLOWING) AS a, count(*) OVER (PARTITION BY p ORDER BY dt DESC RANGE BETWEEN INTERVAL 2 DAY PRECEDING AND UNBOUNDED FOLLOWING) AS b, sum(v) OVER (PARTITION BY p ORDER BY dt GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS c, min(v) OVER (PARTITION BY p ORDER BY dt NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND INTERVAL 1 DAY PRECEDING) AS e FROM (VALUES ('a', DATE '2000-01-01', 1), ('a', DATE '2000-01-02', 2), ('a', DATE '2000-01-05', 4), ('a', NULL, 8), (NULL, DATE '2000-01-01', 16), (NULL, DATE '2000-01-03', 32), (NULL, NULL, 64), ('b', DATE '2000-01-06', 128), ('a', DATE '2000-01-02', 256)) v(p, dt, v) ORDER BY p, dt, v")]
    // Partitions of several keys, nulls in some: each its own partition.
    [InlineData("SELECT p, q, i, sum(v) OVER (PARTITION BY p, q ORDER BY i RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS a, count(*) OVER (PARTITION BY p, q ORDER BY i DESC RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS b FROM (VALUES (NULL, 1, 1, 1), (NULL, 2, 1, 2), (1, NULL, 1, 4), (1, NULL, 2, 8), (NULL, NULL, 1, 16), (NULL, NULL, 3, 32), (1, 1, 1, 64), (NULL, 1, 3, 128), (2, NULL, 2, 256)) v(p, q, i, v) ORDER BY p, q, i")]
    // GROUPS without ORDER BY: the partition is one group of peers, so a frame holds all of it or none.
    [InlineData("SELECT g, o, sum(o) OVER (PARTITION BY g GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a, count(*) OVER (GROUPS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS b, sum(o) OVER (GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS c, max(s) OVER (PARTITION BY g GROUPS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS e, count(k) OVER (GROUPS 2 PRECEDING) AS f, last_value(s IGNORE NULLS) OVER (PARTITION BY g GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS h FROM t ORDER BY g, o")]
    // IGNORE NULLS: lag and lead skip null values (n = 1, n > 1, defaults where too few values, n = 0 the value itself),
    // first_value and last_value over the whole partition, the default frame (by peers), running and bounded frames.
    [InlineData("SELECT p, i, x, lag(x IGNORE NULLS) OVER (PARTITION BY p ORDER BY i) AS a, lag(x, 2 IGNORE NULLS) OVER (PARTITION BY p ORDER BY i) AS b, lead(x, 2, -1 IGNORE NULLS) OVER (PARTITION BY p ORDER BY i) AS c, lead(x IGNORE NULLS) OVER (ORDER BY p, i) AS d, lag(x, 3, 0 IGNORE NULLS) OVER (ORDER BY i DESC, p) AS e, lag(x, 0, -5 IGNORE NULLS) OVER (ORDER BY p, i) AS f, lag(s, 2 IGNORE NULLS) OVER (PARTITION BY p ORDER BY i) AS h, lead(s, 1, '-' IGNORE NULLS) OVER (PARTITION BY p ORDER BY i) AS j FROM (VALUES ('a', 1, 1, 10, 'p'), ('a', 2, 1, NULL, NULL), ('a', 3, 2, NULL, 'q'), ('a', 4, 2, 30, NULL), ('a', 5, NULL, NULL, 'r'), ('a', 6, 4, 60, NULL), ('a', 7, 4, NULL, NULL), (NULL, 1, 1, NULL, NULL), (NULL, 2, 3, 5, 'z'), ('b', 1, NULL, NULL, NULL), ('b', 2, 3, NULL, NULL)) v(p, i, k, x, s) ORDER BY p, i")]
    [InlineData("SELECT p, i, first_value(x IGNORE NULLS) OVER (PARTITION BY p ORDER BY i) AS a, last_value(x IGNORE NULLS) OVER (PARTITION BY p ORDER BY i) AS b, first_value(x IGNORE NULLS) OVER (PARTITION BY p ORDER BY i ROWS UNBOUNDED PRECEDING) AS c, last_value(s IGNORE NULLS) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS e, first_value(x IGNORE NULLS) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS f, last_value(s IGNORE NULLS) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS h, last_value(x IGNORE NULLS) OVER (PARTITION BY p) AS w, first_value(s IGNORE NULLS) OVER (PARTITION BY p) AS z FROM (VALUES ('a', 1, 1, 10, 'p'), ('a', 2, 1, NULL, NULL), ('a', 3, 2, NULL, 'q'), ('a', 4, 2, 30, NULL), ('a', 5, NULL, NULL, 'r'), ('a', 6, 4, 60, NULL), ('a', 7, 4, NULL, NULL), (NULL, 1, 1, NULL, NULL), (NULL, 2, 3, 5, 'z'), ('b', 1, NULL, NULL, NULL), ('b', 2, 3, NULL, NULL)) v(p, i, k, x, s) ORDER BY p, i")]
    [InlineData("SELECT p, i, first_value(x IGNORE NULLS) OVER (ORDER BY k) AS a, last_value(x IGNORE NULLS) OVER (ORDER BY k) AS b, first_value(x IGNORE NULLS) OVER (ORDER BY k DESC NULLS FIRST) AS c, last_value(x IGNORE NULLS) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS e, first_value(x RESPECT NULLS) OVER (ORDER BY k NULLS LAST) AS f FROM (VALUES ('a', 1, 1, 10, 'p'), ('a', 2, 1, NULL, NULL), ('a', 3, 2, NULL, 'q'), ('a', 4, 2, 30, NULL), ('a', 5, NULL, NULL, 'r'), ('a', 6, 4, 60, NULL), ('a', 7, 4, NULL, NULL), (NULL, 1, 1, NULL, NULL), (NULL, 2, 3, 5, 'z'), ('b', 1, NULL, NULL, NULL), ('b', 2, 3, NULL, NULL)) v(p, i, k, x, s) ORDER BY p, i")]
    // Running aggregates whose frame ends past the partition's end on every row of a short partition.
    [InlineData("SELECT g, o, sum(o) OVER (PARTITION BY g ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND 5 FOLLOWING) AS a, count(k) OVER (PARTITION BY g ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) AS b, max(x) OVER (PARTITION BY g ORDER BY o DESC ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) AS c, avg(d) OVER (PARTITION BY g ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) AS e, sum(o) OVER (PARTITION BY g ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND 3 FOLLOWING) AS f, count(*) OVER (PARTITION BY g ORDER BY k DESC GROUPS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS h FROM t ORDER BY g, o")]
    // first_value and last_value over ROWS frames unbounded at one end with an offset at the other (empty frames, past
    // the partition's end, the forward and backward fill idioms), and RANGE / GROUPS from the current row's peers on
    // (peers of equal values: which of them is first is not defined).
    [InlineData("SELECT p, i, first_value(x) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) AS a, first_value(x) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS b, last_value(x) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS c, last_value(x) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) AS e, first_value(x IGNORE NULLS) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS f, first_value(x IGNORE NULLS) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) AS h, last_value(x IGNORE NULLS) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS j, last_value(s IGNORE NULLS) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) AS l FROM (VALUES ('a', 1, 1, 10, 'p'), ('a', 2, 1, NULL, NULL), ('a', 3, 2, NULL, 'q'), ('a', 4, 2, 30, NULL), ('a', 5, NULL, NULL, 'r'), ('a', 6, 4, 60, NULL), ('a', 7, 4, NULL, NULL), (NULL, 1, 1, NULL, NULL), (NULL, 2, 3, 5, 'z'), ('b', 1, NULL, NULL, NULL), ('b', 2, 3, NULL, NULL)) v(p, i, k, x, s) ORDER BY p, i")]
    [InlineData("SELECT p, i, first_value(x IGNORE NULLS) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS a, last_value(x) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS b, first_value(x) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS c, last_value(x IGNORE NULLS) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) AS e, first_value(s) OVER (PARTITION BY p ORDER BY i ROWS BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) AS f, last_value(x) OVER (ORDER BY p, i DESC ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS h, first_value(x) OVER (ORDER BY p, i ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS j, last_value(s IGNORE NULLS) OVER (ORDER BY p, i ROWS BETWEEN 3 FOLLOWING AND UNBOUNDED FOLLOWING) AS l FROM (VALUES ('a', 1, 1, 10, 'p'), ('a', 2, 1, NULL, NULL), ('a', 3, 2, NULL, 'q'), ('a', 4, 2, 30, NULL), ('a', 5, NULL, NULL, 'r'), ('a', 6, 4, 60, NULL), ('a', 7, 4, NULL, NULL), (NULL, 1, 1, NULL, NULL), (NULL, 2, 3, 5, 'z'), ('b', 1, NULL, NULL, NULL), ('b', 2, 3, NULL, NULL)) v(p, i, k, x, s) ORDER BY p, i")]
    [InlineData("SELECT p, k, x, first_value(x) OVER (PARTITION BY p ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS a, last_value(x) OVER (PARTITION BY p ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS b, first_value(x IGNORE NULLS) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS c, last_value(x IGNORE NULLS) OVER (PARTITION BY p ORDER BY k DESC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS e, first_value(x) OVER (ORDER BY k NULLS LAST GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS f, last_value(x IGNORE NULLS) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS h FROM (VALUES ('a', 1, 10), ('a', 1, 10), ('a', 2, NULL), ('a', 3, 30), ('a', NULL, 40), ('a', 4, NULL), ('a', 4, NULL), ('b', 5, NULL), ('b', 6, 60), (NULL, 9, 7)) v(p, k, x) ORDER BY p, k, x")]
    public void Gives_DuckDBs_rows(string sql)
    {
        var (dir, catalog) = Data.Value;
        var expected = TpcdsSqlResultTests.DuckDbRows(dir, sql);
        var actual = TpcdsSqlResultTests.Run(catalog, sql);
        var difference = TpcdsSqlResultTests.Difference(expected, actual);
        Assert.True(difference is null, $"{difference}; {expected.Count} rows expected, {actual.Count} given");
    }

    /// IGNORE NULLS after the parentheses (ISO, Oracle, SQL Server, Spark) means what it means inside them; DuckDB reads
    /// only the inside spelling, so its rows for that text are the expected ones.
    [Fact]
    public void Ignore_nulls_after_the_call_is_the_same_as_inside()
    {
        var (dir, catalog) = Data.Value;
        const string iso = "SELECT g, o, lag(k) IGNORE NULLS OVER (ORDER BY g, o) AS a, last_value(d) IGNORE NULLS OVER (ORDER BY g, o) AS b, lead(s) RESPECT NULLS OVER (ORDER BY g, o) AS c FROM t ORDER BY g, o";
        const string inside = "SELECT g, o, lag(k IGNORE NULLS) OVER (ORDER BY g, o) AS a, last_value(d IGNORE NULLS) OVER (ORDER BY g, o) AS b, lead(s) OVER (ORDER BY g, o) AS c FROM t ORDER BY g, o";
        var expected = TpcdsSqlResultTests.DuckDbRows(dir, inside);
        var actual = TpcdsSqlResultTests.Run(catalog, iso);
        var difference = TpcdsSqlResultTests.Difference(expected, actual);
        Assert.True(difference is null, $"{difference}; {expected.Count} rows expected, {actual.Count} given");
    }
}
