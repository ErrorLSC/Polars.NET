#nullable enable
using System.Globalization;
using DuckDB.NET.Data;
using Kaminari.Samidare.Config;
using Kaminari.Samidare.IR;
using Kaminari.Samidare.IR.Passes;
using Kaminari.Samidare.Sql;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Wrappers;
using Polars.NET.Linq.Backend.Polars;
using Polars.Tpcds.Tests;

namespace Polars.CSharp.Linq.Tests.Sql;

/// <summary>
/// DuckDB's 99 TPC-DS texts run through the SQL frontend on Polars give DuckDB's rows (sql-frontend S6), compared in
/// order: integers, decimals, strings and dates exactly, floating values within 1e-9 relative. Where DuckDB gives a double
/// and the frontend a decimal (decimal division and AVG stay decimals here, Q4), within one unit of the decimal's last
/// digit. NULL only matches NULL. DuckDB runs with LINQ's NULL order, the frontend's default (Q3).
/// </summary>
[Trait("Category", "Sql")]
public class TpcdsSqlResultTests
{
    static readonly Lazy<ISqlCatalog> Catalog = new(() =>
    {
        var catalog = new SqlCatalog();
        foreach (var path in Directory.GetFiles(TpcdsData.DataDir, "*.parquet"))
            catalog.Register(Path.GetFileNameWithoutExtension(path), PolarsTableSources.ofLazyFrame(LazyFrame.ScanParquet(path).Handle));
        return catalog;
    });

    [Theory]
    [MemberData(nameof(TpcdsSqlParseTests.Queries), MemberType = typeof(TpcdsSqlParseTests))]
    public void Query_gives_DuckDBs_rows(int n)
    {
        var text = TpcdsSqlParseTests.Text(n);
        var expected = DuckDbRows(TpcdsData.DataDir, text);
        var actual = Run(Catalog.Value, text);
        var difference = Difference(expected, actual);
        if (difference is not null)
        {
            var unordered = Difference(Sorted(expected), Sorted(actual)) is null ? " (the same rows in another order)" : "";
            Assert.Fail($"Q{n}: {difference}{unordered}; {expected.Count} rows expected, {actual.Count} given");
        }
    }

    /// The query through the SQL frontend, optimized and run as the LINQ provider runs its plans.
    internal static List<object?[]> Run(ISqlCatalog catalog, string text)
    {
        var sql = SqlQueries.translate(catalog, true, text);
        var plan = EnumResolution.resolvePlan(sql.Plan);
        plan = CommonSubplanSharing.apply(PolarsQueryBackend.Instance, sql.Sources, plan);
        plan = LogicalOptimizer.optimizeComplete(SamidareConfig.AllowUnstableOrdering, SamidareConfig.CollectColumnStatistics, sql.Sources, plan);
        using var df = new DataFrame(PolarsWrapper.LazyCollect(PolarsPhysicalLowerer.lowerPlan(sql.Sources, plan), PlEngine.Auto));
        var width = df.ColumnNames.Length;
        var rows = new List<object?[]>((int)df.Height);
        for (var r = 0; r < df.Height; r++)
        {
            var row = new object?[width];
            for (var c = 0; c < width; c++) row[c] = df[r, c];
            rows.Add(row);
        }
        return rows;
    }

    /// The query on DuckDB, a view over each Parquet file of the directory, with LINQ's NULL order.
    internal static List<object?[]> DuckDbRows(string dataDir, string text)
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        using (var setup = connection.CreateCommand())
        {
            var views = Directory.GetFiles(dataDir, "*.parquet")
                .Select(path => $"CREATE VIEW {Path.GetFileNameWithoutExtension(path)} AS SELECT * FROM read_parquet('{path}');");
            setup.CommandText = "SET default_null_order = 'nulls_first_on_asc_last_on_desc';" + string.Concat(views);
            setup.ExecuteNonQuery();
        }
        using var command = connection.CreateCommand();
        command.CommandText = text;
        using var reader = command.ExecuteReader();
        var rows = new List<object?[]>();
        while (reader.Read())
        {
            var row = new object?[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++) row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            rows.Add(row);
        }
        return rows;
    }

    internal static List<object?[]> Sorted(List<object?[]> rows) =>
        [.. rows.OrderBy(r => string.Join("\u0001", r.Select(v => v is null ? "\u0000" : Convert.ToString(v, CultureInfo.InvariantCulture))), StringComparer.Ordinal)];

    /// The first difference, or null when the rows are the same.
    internal static string? Difference(List<object?[]> expected, List<object?[]> actual)
    {
        for (var i = 0; i < Math.Min(expected.Count, actual.Count); i++)
        {
            if (expected[i].Length != actual[i].Length) return $"{expected[i].Length} columns expected, {actual[i].Length} given";
            for (var c = 0; c < expected[i].Length; c++)
                if (!Same(expected[i][c], actual[i][c]))
                    return $"row {i}, column {c}: expected {Show(expected[i][c])}, got {Show(actual[i][c])}";
        }
        return expected.Count == actual.Count ? null : "row count";
    }

    static string Show(object? v) => v is null ? "NULL" : $"{Convert.ToString(v, CultureInfo.InvariantCulture)} ({v.GetType().Name})";

    static bool Same(object? e, object? a)
    {
        if (e is null || a is null) return e is null && a is null;
        if (e is double or float || a is double or float)
        {
            var x = Convert.ToDouble(e, CultureInfo.InvariantCulture);
            var y = Convert.ToDouble(a, CultureInfo.InvariantCulture);
            if (double.IsNaN(x) || double.IsNaN(y)) return double.IsNaN(x) && double.IsNaN(y);
            // A decimal against a double: within one unit of the decimal's last digit.
            var unit = (e, a) switch
            {
                (decimal d, _) => (double)Unit(d),
                (_, decimal d) => (double)Unit(d),
                _ => 0.0,
            };
            return Math.Abs(x - y) <= unit + 1e-9 * Math.Max(1.0, Math.Max(Math.Abs(x), Math.Abs(y)));
        }
        if (IsExact(e) && IsExact(a)) return ToDecimal(e) == ToDecimal(a);
        return Equals(Normalize(e), Normalize(a));
    }

    static decimal ToDecimal(object v) => v switch
    {
        System.Numerics.BigInteger b => (decimal)b,
        Int128 i => (decimal)i,
        UInt128 u => (decimal)u,
        _ => Convert.ToDecimal(v, CultureInfo.InvariantCulture),
    };

    static decimal Unit(decimal d) => 1m / (decimal)Math.Pow(10, (decimal.GetBits(d)[3] >> 16) & 0x7F);

    static bool IsExact(object v) => v is sbyte or short or int or long or byte or ushort or uint or ulong or decimal or System.Numerics.BigInteger or Int128 or UInt128;

    static object Normalize(object v) => v switch
    {
        DateTime d when d.TimeOfDay == TimeSpan.Zero => DateOnly.FromDateTime(d),
        _ => v,
    };
}
