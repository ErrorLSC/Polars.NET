using BenchmarkDotNet.Attributes;
using DuckDB.NET.Data;
using LinqToDB.Data;
using Polars.NET.Linq;
using Kaminari.Samidare;
using Polars.Tpcds;

namespace Polars.Benchmarks.Tpcds;

/// <summary>
/// TPC-DS queries end to end, from Parquet files to a <c>List&lt;T&gt;</c>: Polars.NET LINQ, DuckDB with
/// hand-written SQL through Dapper, and DuckDB through Linq2DB. Every iteration builds the query, translates it,
/// scans the Parquet files, executes and materializes; plan caches stay on, as in an application that runs the
/// same query repeatedly. Polars_Collect isolates native execution (a prepared plan collected again).
/// </summary>
/// <remarks>
/// Before timing, every implementation's result is compared row by row (see <see cref="TpcdsBenchQuery.Validate"/>).
/// Select queries with <c>POLARS_TPCDS_QUERIES=Q3,Q98</c>; see <see cref="TpcdsBenchEnvironment"/> for the data.
/// Each invocation runs a whole query, so the job runs one invocation per iteration.
/// </remarks>
[MemoryDiagnoser]
[WarmupCount(2)]
[IterationCount(10)]
[InvocationCount(1)]
public class TpcdsExecutionBenchmarks
{
    private static readonly HashSet<string> Validated = [];
    private TpcdsBenchQuery _query = null!;
    private DuckDBConnection _duckDb = null!;
    private DataConnection? _linq2Db;

    [ParamsSource(nameof(QueryNames))]
    public string Query { get; set; } = "";

    public static IEnumerable<string> QueryNames => TpcdsBenchCatalog.SelectedNames();

    [GlobalSetup]
    public void Setup()
    {
        TpcdsBenchEnvironment.UseBenchmarkData();
        _query = TpcdsBenchCatalog.Get(Query);
        _duckDb = TpcdsBenchEnvironment.OpenDuckDb();
        _linq2Db = _query.HasLinq2Db ? TpcdsBenchEnvironment.OpenLinq2Db(_duckDb) : null;
        // Once per query: GlobalSetup runs again for every benchmark method.
        if (Validated.Add(Query))
            Console.WriteLine("// Validation: " + _query.Validate(_duckDb, _linq2Db));
        _query.PreparePhases();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _query.ReleasePhases();
        _linq2Db?.Dispose();
        _duckDb.Dispose();
    }

    [Benchmark(Baseline = true, Description = "DuckDB+Dapper")]
    public int DuckDB_Dapper() => _query.DapperToList(_duckDb);

    [Benchmark(Description = "DuckDB+Linq2DB")]
    public int DuckDB_Linq2DB() => _linq2Db is null ? Unsupported() : _query.Linq2DbToList(_linq2Db);

    [Benchmark(Description = "Polars.NET LINQ")]
    public int Polars_Linq() => _query.PolarsToList();

    /// Native execution only: a plan translated in setup, scanned and executed again.
    [Benchmark(Description = "Polars collect only")]
    public long Polars_Collect() => _query.PolarsCollect();

    // A benchmark that is not applicable reports as an error (NA) rather than a misleading time.
    private int Unsupported() => throw new NotSupportedException($"{Query} has no Linq2DB implementation.");
}

/// <summary>
/// The managed phases of the same queries, which take microseconds to milliseconds and need many invocations:
/// translation (building the expression tree included) with the plan cache cold and warm, for Polars.NET LINQ
/// and Linq2DB, and the materialization of a collected result as a <c>List&lt;T&gt;</c>.
/// </summary>
[MemoryDiagnoser]
[WarmupCount(3)]
[IterationCount(15)]
public class TpcdsTranslationBenchmarks
{
    private TpcdsBenchQuery _query = null!;
    private DuckDBConnection _duckDb = null!;
    private DataConnection? _linq2Db;

    [ParamsSource(nameof(QueryNames))]
    public string Query { get; set; } = "";

    /// False translates from scratch every time; true reuses a cached plan (each library's default).
    [Params(false, true)]
    public bool PlanCache { get; set; }

    public static IEnumerable<string> QueryNames => TpcdsBenchCatalog.SelectedNames();

    [GlobalSetup]
    public void Setup()
    {
        TpcdsBenchEnvironment.UseBenchmarkData();
        _query = TpcdsBenchCatalog.Get(Query);
        _duckDb = TpcdsBenchEnvironment.OpenDuckDb();
        _linq2Db = _query.HasLinq2Db ? TpcdsBenchEnvironment.OpenLinq2Db(_duckDb, disableQueryCache: !PlanCache) : null;
        // Translation alone: the tables' scans are created once, as Linq2DB's tables are mapping metadata only.
        TpcdsData.ReuseSources = true;
        QueryPlanCache.Enabled = PlanCache;
        QueryPlanCache.Clear();
        _query.PreparePhases();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _query.ReleasePhases();
        TpcdsData.ReuseSources = false;
        QueryPlanCache.Enabled = true;
        QueryPlanCache.Clear();
        _linq2Db?.Dispose();
        _duckDb.Dispose();
    }

    [Benchmark(Description = "Polars.NET LINQ translate")]
    public void Polars_Translate() => _query.PolarsTranslate();

    [Benchmark(Description = "Linq2DB translate")]
    public string Linq2DB_Translate() =>
        _linq2Db is null ? throw new NotSupportedException($"{Query} has no Linq2DB implementation.") : _query.Linq2DbTranslate(_linq2Db);

    /// Materialization of the collected result (independent of PlanCache).
    [Benchmark(Description = "Polars materialize")]
    public int Polars_Materialize() => _query.PolarsMaterialize();
}
