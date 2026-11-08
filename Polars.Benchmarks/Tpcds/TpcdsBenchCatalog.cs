using Polars.Tpcds;

namespace Polars.Benchmarks.Tpcds;

/// <summary>
/// The benchmarked queries. The LINQ queries and the DuckDB SQL are the ones Polars.Tpcds.Tests validates
/// (Polars.Tpcds.Queries); the Linq2DB versions live in <see cref="TpcdsLinq2DbQueries"/>.
/// </summary>
/// <remarks>
/// Chosen to cover the query shapes of TPC-DS: star joins with aggregation (Q3, Q55), correlated subqueries
/// (Q1, Q6), multi-channel UNION ALL (Q5), EXISTS / NOT EXISTS (Q16, Q94), INTERSECT / EXCEPT (Q38, Q87),
/// window functions (Q47, Q51, Q98), ROLLUP (Q36, Q67, Q86), very large plans (Q14, Q64) and a heavy join (Q72),
/// plus <see cref="WideResult"/> for materializing a large result.
/// </remarks>
internal static class TpcdsBenchCatalog
{
    private static readonly Dictionary<string, TpcdsBenchQuery> Queries = new TpcdsBenchQuery[]
    {
        new TpcdsBenchQuery<string>("Q1", TpcdsQueries.Q1, TpcdsSql.Q1, TpcdsLinq2DbQueries.Q1),
        new TpcdsBenchQuery<Q3Row>("Q3", TpcdsQueries.Q3, TpcdsSql.Q3, TpcdsLinq2DbQueries.Q3),
        new TpcdsBenchQuery<Q5Row>("Q5", TpcdsQueries.Q5, TpcdsSql.Q5),
        new TpcdsBenchQuery<Q6Row>("Q6", TpcdsQueries.Q6, TpcdsSql.Q6),
        new TpcdsBenchQuery<Q14Row>("Q14", TpcdsQueries.Q14, TpcdsSql.Q14),
        new TpcdsBenchQuery<Q16Row>("Q16", TpcdsQueries.Q16, TpcdsSql.Q16, TpcdsLinq2DbQueries.Q16),
        new TpcdsBenchQuery<Q36Row>("Q36", TpcdsQueries.Q36, TpcdsSql.Q36, TpcdsLinq2DbQueries.Q36),
        new TpcdsCountBenchQuery<Q38Row>("Q38", TpcdsQueries.Q38, TpcdsSql.Q38),
        new TpcdsBenchQuery<Q47Row>("Q47", TpcdsQueries.Q47, TpcdsSql.Q47),
        new TpcdsBenchQuery<Q51Row>("Q51", TpcdsQueries.Q51, TpcdsSql.Q51),
        new TpcdsBenchQuery<Q55Row>("Q55", TpcdsQueries.Q55, TpcdsSql.Q55),
        new TpcdsBenchQuery<Q64Row>("Q64", TpcdsQueries.Q64, TpcdsSql.Q64),
        new TpcdsBenchQuery<Q67Row>("Q67", TpcdsQueries.Q67, TpcdsSql.Q67),
        new TpcdsBenchQuery<Q72Row>("Q72", TpcdsQueries.Q72, TpcdsSql.Q72),
        new TpcdsBenchQuery<Q86Row>("Q86", TpcdsQueries.Q86, TpcdsSql.Q86),
        new TpcdsCountBenchQuery<Q87Row>("Q87", TpcdsQueries.Q87, TpcdsSql.Q87),
        new TpcdsBenchQuery<Q94Row>("Q94", TpcdsQueries.Q94, TpcdsSql.Q94),
        new TpcdsBenchQuery<Q98Row>("Q98", TpcdsQueries.Q98, TpcdsSql.Q98, TpcdsLinq2DbQueries.Q98),
        new TpcdsBenchQuery<WideRow>("Wide", WideResult.Polars, WideResult.Sql, WideResult.Linq2Db, ordered: false),
    }.ToDictionary(q => q.Name);

    public static TpcdsBenchQuery Get(string name) =>
        Queries.TryGetValue(name, out var query) ? query : throw new ArgumentException($"Unknown TPC-DS benchmark query '{name}'.");

    /// <summary>Validates every selected query (see <see cref="TpcdsBenchQuery.Validate"/>) and reports each outcome.</summary>
    public static void ValidateAll()
    {
        TpcdsBenchEnvironment.UseBenchmarkData();
        Console.WriteLine($"TPC-DS data: {TpcdsData.DataDir}");
        using var duckDb = TpcdsBenchEnvironment.OpenDuckDb();
        using var linq2Db = TpcdsBenchEnvironment.OpenLinq2Db(duckDb);
        int failed = 0;
        foreach (var name in SelectedNames())
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();
            try
            {
                var outcome = Get(name).Validate(duckDb, linq2Db);
                Console.WriteLine($"OK   {outcome} ({watch.Elapsed.TotalSeconds:F1}s)");
                if (Environment.GetEnvironmentVariable("POLARS_TPCDS_SHOW_LINQ2DB_SQL") == "1" && Get(name).HasLinq2Db)
                    Console.WriteLine(Get(name).Linq2DbTranslate(linq2Db));
            }
            catch (Exception e)
            {
                failed++;
                Console.WriteLine($"FAIL {name}: {e.GetType().Name}: {e.Message}");
                if (Environment.GetEnvironmentVariable("POLARS_TPCDS_SHOW_LINQ2DB_SQL") == "1" && Get(name).HasLinq2Db)
                    Console.WriteLine(Get(name).Linq2DbTranslate(linq2Db));
            }
        }

        Console.WriteLine(failed == 0 ? "All queries validated." : $"{failed} queries failed validation.");
        Environment.ExitCode = failed == 0 ? 0 : 1;
    }

    /// <summary>All queries, or those named in <c>POLARS_TPCDS_QUERIES</c> (comma separated).</summary>
    public static IEnumerable<string> SelectedNames()
    {
        var selected = Environment.GetEnvironmentVariable("POLARS_TPCDS_QUERIES");
        return string.IsNullOrWhiteSpace(selected)
            ? Queries.Keys
            : selected.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }
}

/// <summary>The Linq2DB implementations of the benchmarked queries, over the mapping of <see cref="TpcdsLinq2DbMapping"/>.</summary>
internal static partial class TpcdsLinq2DbQueries
{
}
