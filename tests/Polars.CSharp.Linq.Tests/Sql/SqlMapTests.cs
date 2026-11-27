#nullable enable
using DuckDB.NET.Data;
using Kaminari.Samidare.Sql;
using Polars.NET.Core.Linq.Backend;

namespace Polars.CSharp.Linq.Tests.Sql;

/// <summary>
/// Maps in the SQL frontend (documented extensions to ISO SQL): subscripts, MAP {k: v} and map(k, v, ...), CAST to
/// MAP(K, V), element_at, map_contains_key, cardinality, map_keys / map_values; =, ORDER BY, DISTINCT and GROUP BY on
/// maps entry-order sensitive, as Polars and DuckDB compare them. Rows compared with DuckDB where it means the same.
/// </summary>
[Trait("Category", "Sql")]
public class SqlMapTests
{
    /// A table of maps written as Parquet by DuckDB (Parquet MAP columns), read by both sides.
    static readonly Lazy<(string Dir, ISqlCatalog Catalog)> Data = new(() =>
    {
        var dir = Directory.CreateTempSubdirectory("sql-map-").FullName;
        using (var connection = new DuckDBConnection("Data Source=:memory:"))
        {
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = $$"""
                CREATE TABLE w AS SELECT * FROM (VALUES
                    (1, MAP {'a': 1, 'b': 2}, MAP {1: 'one', 2: 'two'}),
                    (2, MAP {'b': 2, 'a': 1}, MAP {2: 'deux'}),
                    (3, NULL, NULL),
                    (4, MAP {}::MAP(VARCHAR, INTEGER), MAP {}::MAP(INTEGER, VARCHAR)),
                    (5, MAP {'a': 1, 'b': 2}, MAP {3: NULL}),
                    (6, MAP {'c': NULL}, MAP {1: 'uno'})
                ) v(id, m, n);
                COPY w TO '{{dir}}/w.parquet' (FORMAT parquet);
                """;
            command.ExecuteNonQuery();
        }
        var catalog = new SqlCatalog();
        catalog.Register("w", PolarsTableSources.OfLazyFrame(LazyFrame.ScanParquet(Path.Combine(dir, "w.parquet")).Handle));
        return (dir, catalog);
    });

    private static List<object?[]> Run(string sql) => TpcdsSqlResultTests.Run(Data.Value.Catalog, sql);

    [Theory]
    [InlineData("SELECT id, m['a'] AS a, m['zz'] AS z, m['c'] AS c, n[2] AS two, cardinality(m) AS k, map_contains(m, 'a') AS h FROM w ORDER BY id")]
    [InlineData("SELECT id FROM w WHERE m['a'] = 1 AND n[1] IS NOT NULL ORDER BY id")]
    [InlineData("SELECT id, CAST(m AS MAP(VARCHAR, BIGINT))['b'] AS b, CAST(n AS MAP(BIGINT, VARCHAR))[2] AS v FROM w ORDER BY id")]
    [InlineData("SELECT cardinality(MAP {'x': 1, 'y': 2}) AS c, MAP {'x': 1}['x'] AS v, MAP {1: 'a', 2: 'b'}[2] AS i, MAP {'x': NULL}['x'] AS n")]
    // Entry-order sensitive: {a: 1, b: 2} and {b: 2, a: 1} are two values.
    [InlineData("SELECT id FROM w WHERE m = MAP {'a': 1, 'b': 2} ORDER BY id")]
    [InlineData("SELECT cardinality(m) AS c, count(*) AS r FROM w GROUP BY m ORDER BY c, r")]
    [InlineData("SELECT count(*) AS d FROM (SELECT DISTINCT m FROM w) x")]
    [InlineData("SELECT id FROM w ORDER BY m, id")]
    [InlineData("SELECT a.id AS l, b.id AS r FROM w a JOIN w b ON a.m = b.m WHERE a.id < b.id ORDER BY l, r")]
    public void Gives_DuckDBs_rows(string sql)
    {
        var (dir, catalog) = Data.Value;
        var expected = TpcdsSqlResultTests.DuckDbRows(dir, sql);
        var actual = TpcdsSqlResultTests.Run(catalog, sql);
        var difference = TpcdsSqlResultTests.Difference(expected, actual);
        Assert.True(difference is null, $"{difference}; {expected.Count} rows expected, {actual.Count} given");
    }

    /// element_at is a map's value (as Spark and Trino; DuckDB gives a list of it), map_contains_key DuckDB's
    /// map_contains, map() Spark's constructor.
    [Fact]
    public void Element_at_map_contains_key_and_map()
    {
        var rows = Run("SELECT id, element_at(m, 'a') AS a, map_contains_key(m, 'b') AS h, map('k', id, 'j', 2 * id)['j'] AS j FROM w ORDER BY id");
        Assert.Equal([1, 1, null, null, 1, null], rows.Select(static r => r[1] is null ? (long?)null : Convert.ToInt64(r[1])));
        Assert.Equal([true, true, null, false, true, false], rows.Select(static r => (bool?)r[2]));
        Assert.Equal([2L, 4, 6, 8, 10, 12], rows.Select(static r => Convert.ToInt64(r[3])));
    }

    [Fact]
    public void Map_keys_and_values_are_lists()
    {
        var rows = Run("SELECT map_keys(m) AS k, map_values(n) AS v FROM w WHERE id = 1");
        Assert.Equal(["a", "b"], ((System.Collections.IEnumerable)rows[0][0]!).Cast<object>().Select(static k => k.ToString()));
        Assert.Equal(["one", "two"], ((System.Collections.IEnumerable)rows[0][1]!).Cast<object>().Select(static v => v.ToString()));
    }

    [Theory]
    [InlineData("SELECT MAP {} AS e", "MAP {} has no key and value types")]
    [InlineData("SELECT MAP {'a': 1, 'a': 2} AS e", "repeats the key 'a'")]
    [InlineData("SELECT map(NULL, 1) AS e", "cannot be NULL")]
    [InlineData("SELECT m[1] AS e FROM w", "keys are VARCHAR")]
    [InlineData("SELECT id[1] AS e FROM w", "no map")]
    [InlineData("SELECT CAST(m AS VARCHAR) AS e FROM w", "a map casts only to a map")]
    [InlineData("SELECT map('a') AS e", "in pairs")]
    public void Refusals_name_the_problem(string sql, string message)
    {
        var error = Assert.Throws<SqlException>(() => Run(sql));
        Assert.Contains(message, error.Message);
    }
}
