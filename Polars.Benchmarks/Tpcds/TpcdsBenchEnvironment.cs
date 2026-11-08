using System.Data;
using Dapper;
using DuckDB.NET.Data;
using LinqToDB;
using LinqToDB.Data;
using Polars.Tpcds;

namespace Polars.Benchmarks.Tpcds;

/// <summary>
/// Data location and the DuckDB side of the TPC-DS benchmarks: one in-memory DuckDB connection whose tables are
/// views over the same Parquet files Polars scans, shared by Dapper (raw SQL) and Linq2DB.
/// </summary>
/// <remarks>
/// Data: <c>Polars.Benchmarks/generate_tpcds_data.sh [sf]</c> writes <c>Data/tpcds-sf&lt;sf&gt;</c>. The scale factor is
/// <c>POLARS_TPCDS_SF</c> (default 2); <c>POLARS_TPCDS_DIR</c> points at another directory instead.
/// </remarks>
internal static class TpcdsBenchEnvironment
{
    private static readonly string[] Tables =
    [
        "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer", "customer_address",
        "customer_demographics", "date_dim", "household_demographics", "income_band", "inventory", "item",
        "promotion", "reason", "ship_mode", "store", "store_returns", "store_sales", "time_dim", "warehouse",
        "web_page", "web_returns", "web_sales", "web_site",
    ];

    private static bool _dapperConfigured;

    public static string ScaleFactor => Environment.GetEnvironmentVariable("POLARS_TPCDS_SF") is { Length: > 0 } sf ? sf : "2";

    /// <summary>Points <see cref="TpcdsData"/> at the benchmark data set.</summary>
    public static void UseBenchmarkData()
    {
        var configured = Environment.GetEnvironmentVariable("POLARS_TPCDS_DIR");
        if (!string.IsNullOrEmpty(configured))
        {
            TpcdsData.DataDir = configured;
            return;
        }

        var name = "tpcds-sf" + ScaleFactor;
        // From the build output (bin/<Configuration>/<tfm>) up to the project directory.
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "Data", name);
            if (File.Exists(Path.Combine(candidate, "store_sales.parquet")))
            {
                TpcdsData.DataDir = candidate;
                return;
            }
        }

        throw new InvalidOperationException(
            $"TPC-DS data '{name}' not found. Run Polars.Benchmarks/generate_tpcds_data.sh {ScaleFactor}, or set POLARS_TPCDS_DIR.");
    }

    /// <summary>
    /// Opens an in-memory DuckDB database with one view per TPC-DS table over its Parquet file, so that DuckDB
    /// scans the files on every query as Polars does. NULL ordering follows LINQ (NULLs first ascending), which
    /// the reference SQL relies on to match the LINQ results.
    /// </summary>
    public static DuckDBConnection OpenDuckDb()
    {
        ConfigureDapper();
        var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        Execute(connection, "SET default_null_order = 'nulls_first_on_asc_last_on_desc'");
        foreach (var table in Tables)
        {
            var path = Path.Combine(TpcdsData.DataDir, table + ".parquet").Replace("'", "''");
            Execute(connection, $"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')");
        }

        return connection;
    }

    /// <summary>A Linq2DB connection over an open DuckDB connection; the caller keeps ownership of the latter.</summary>
    public static DataConnection OpenLinq2Db(DuckDBConnection connection, bool disableQueryCache = false) =>
        new(new DataOptions()
            .UseDuckDB()
            .UseConnection(connection)
            .UseMappingSchema(TpcdsLinq2DbMapping.Schema)
            // SQL NULL semantics for == / != (Linq2DB's default emulates C# null equality with extra IS NULL predicates).
            .UseCompareNulls(LinqToDB.CompareNulls.LikeSql)
            .UseDisableQueryCache(disableQueryCache));

    private static void Execute(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static void ConfigureDapper()
    {
        if (_dapperConfigured) return;
        SqlMapper.AddTypeHandler(new DateOnlyHandler());
        _dapperConfigured = true;
    }

    /// <summary>DuckDB DATE columns read as DateTime through the untyped reader; the row types use DateOnly.</summary>
    private sealed class DateOnlyHandler : SqlMapper.TypeHandler<DateOnly>
    {
        public override DateOnly Parse(object value) => value switch
        {
            DateOnly d => d,
            DateTime dt => DateOnly.FromDateTime(dt),
            _ => DateOnly.FromDateTime(Convert.ToDateTime(value)),
        };

        public override void SetValue(IDbDataParameter parameter, DateOnly value)
        {
            parameter.DbType = DbType.Date;
            parameter.Value = value;
        }
    }
}
