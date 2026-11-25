#nullable enable
using DuckDB.NET.Data;
using Kaminari.Samidare.Sql;
using Polars.NET.Linq.Backend.Polars;

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
            catalog.Register(Path.GetFileNameWithoutExtension(path), PolarsTableSources.ofLazyFrame(LazyFrame.ScanParquet(path).Handle));
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
    public void Gives_DuckDBs_rows(string sql)
    {
        var (dir, catalog) = Data.Value;
        var expected = TpcdsSqlResultTests.DuckDbRows(dir, sql);
        var actual = TpcdsSqlResultTests.Run(catalog, sql);
        var difference = TpcdsSqlResultTests.Difference(expected, actual);
        Assert.True(difference is null, $"{difference}; {expected.Count} rows expected, {actual.Count} given");
    }
}
