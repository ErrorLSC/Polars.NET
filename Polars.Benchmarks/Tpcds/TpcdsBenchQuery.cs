using System.Data.Common;
using System.Reflection;
using Dapper;
using LinqToDB;
using LinqToDB.Data;
using Polars.NET.Core;
using Polars.NET.Core.Wrappers;
using Polars.NET.Linq;

namespace Polars.Benchmarks.Tpcds;

/// <summary>
/// One benchmarked query in each of its implementations: the Polars.NET LINQ query, the DuckDB SQL run through
/// Dapper, and (optionally) the same query written for Linq2DB over DuckDB. All produce the same row type, so
/// their results are compared before anything is timed.
/// </summary>
public abstract class TpcdsBenchQuery(string name)
{
    public string Name { get; } = name;

    public abstract bool HasLinq2Db { get; }

    // --- End to end ---
    public abstract int PolarsToList();
    public abstract int DapperToList(DbConnection connection);
    public abstract int Linq2DbToList(DataConnection db);

    // --- Translation ---
    /// <summary>Builds the query and translates it to a native Polars plan (IR, optimization, lowering).</summary>
    public abstract void PolarsTranslate();
    /// <summary>Builds the query and translates it to SQL.</summary>
    public abstract string Linq2DbTranslate(DataConnection db);

    // --- Polars phases over one prepared plan (PreparePhases first) ---
    public abstract void PreparePhases();
    public abstract void ReleasePhases();
    /// <summary>Runs the prepared native plan; the collected frame is dropped.</summary>
    public abstract long PolarsCollect();
    /// <summary>Materializes the frame collected by PreparePhases as a List.</summary>
    public abstract int PolarsMaterialize();

    /// <summary>Prints the Polars query's IR and optimized native plan, and times one run of it.</summary>
    public abstract void Explain();

    /// <summary>Row count of the Polars result and whether every implementation produced the same rows.</summary>
    public abstract string Validate(DbConnection connection, DataConnection? db);
}

public sealed class TpcdsBenchQuery<TRow>(
    string name,
    Func<IQueryable<TRow>> polars,
    Func<string> sql,
    Func<DataConnection, IQueryable<TRow>>? linq2Db = null,
    bool ordered = true) : TpcdsBenchQuery(name)
{
    private PreparedQuery<TRow>? _prepared;
    private DataFrameHandle? _collected;

    public override bool HasLinq2Db => linq2Db is not null;

    public override int PolarsToList() => polars().ToList().Count;

    public override int DapperToList(DbConnection connection) =>
        connection.Query<TRow>(sql(), buffered: true, commandTimeout: 0).AsList().Count;

    public override int Linq2DbToList(DataConnection db) => Linq2Db(db).ToList().Count;

    public override void PolarsTranslate()
    {
        using var prepared = Prepare(polars());
    }

    public override string Linq2DbTranslate(DataConnection db) => Linq2Db(db).ToSqlQuery().Sql;

    public override void PreparePhases()
    {
        ReleasePhases();
        _prepared = Prepare(polars());
        _collected = _prepared.Collect();
    }

    public override void ReleasePhases()
    {
        _collected?.Dispose();
        _collected = null;
        (_prepared as IDisposable)?.Dispose();
        _prepared = null;
    }

    public override long PolarsCollect()
    {
        using var df = Prepared.Collect();
        return PolarsWrapper.DataFrameHeight(df);
    }

    public override int PolarsMaterialize()
    {
        // The materializer takes ownership of the frame it is given; a clone shares the column buffers.
        var df = PolarsWrapper.CloneDataFrame(_collected ?? throw new InvalidOperationException("Call PreparePhases first."));
        return Prepared.Materialize(df).Count;
    }

    public override void Explain()
    {
        var query = (IPolarsExplainable)polars();
        Console.WriteLine("=== Logical IR ===");
        Console.WriteLine(query.ExplainLogicalPlan());
        Console.WriteLine("=== Polars plan (optimized) ===");
        Console.WriteLine(query.Explain(true));
        for (int run = 1; run <= 3; run++)
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();
            int rows = PolarsToList();
            Console.WriteLine($"run {run}: {rows} rows in {watch.Elapsed.TotalMilliseconds:F1} ms");
        }
    }

    public override string Validate(DbConnection connection, DataConnection? db)
    {
        var polarsRows = polars().ToList();
        var dapperRows = connection.Query<TRow>(sql(), buffered: true, commandTimeout: 0).AsList();
        RowComparer.AssertSame(Name + " Polars vs DuckDB+Dapper", dapperRows, polarsRows, ordered);
        var checkedAgainst = "DuckDB+Dapper";
        if (linq2Db is not null && db is not null)
        {
            var linq2DbRows = linq2Db(db).ToList();
            RowComparer.AssertSame(Name + " Linq2DB vs DuckDB+Dapper", dapperRows, linq2DbRows, ordered);
            checkedAgainst += ", Linq2DB";
        }

        if (polarsRows.Count == 0)
            throw new InvalidOperationException($"{Name}: empty result; the benchmark would measure nothing.");

        return $"{Name}: {polarsRows.Count} rows, identical in Polars, {checkedAgainst}";
    }

    private PreparedQuery<TRow> Prepared =>
        _prepared ?? throw new InvalidOperationException("Call PreparePhases first.");

    private IQueryable<TRow> Linq2Db(DataConnection db) =>
        (linq2Db ?? throw new NotSupportedException($"{Name} has no Linq2DB implementation."))(db);

    private static PreparedQuery<TRow> Prepare(IQueryable<TRow> query) =>
        query is PolarsQuery<TRow> polarsQuery
            ? polarsQuery.Prepare()
            : throw new InvalidOperationException($"Not a Polars query: {query.GetType()}");
}

/// <summary>
/// Compares two result lists value by value: exact for integers, strings and dates, relative 1e-9 for decimals
/// and floating values (DuckDB and Polars may sum in a different order). NULL must match NULL.
/// </summary>
internal static class RowComparer
{
    public static void AssertSame<TRow>(string what, IReadOnlyList<TRow> expected, IReadOnlyList<TRow> actual, bool ordered)
    {
        if (expected.Count != actual.Count)
            throw new InvalidOperationException($"{what}: {expected.Count} rows expected, {actual.Count} rows found.");

        var (e, a) = ordered ? (expected, actual) : (Sort(expected), Sort(actual));
        var props = IsScalar(typeof(TRow)) ? null : typeof(TRow).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        for (int i = 0; i < e.Count; i++)
        {
            if (props is null)
            {
                Check(what, $"row {i}", e[i], a[i]);
                continue;
            }

            foreach (var p in props)
                Check(what, $"row {i}, {p.Name}", p.GetValue(e[i]), p.GetValue(a[i]));
        }
    }

    private static void Check(string what, string where, object? e, object? a)
    {
        bool same = (e, a) switch
        {
            (null, null) => true,
            (null, _) or (_, null) => false,
            (double or float or decimal, _) or (_, double or float or decimal) => Close(Convert.ToDouble(e), Convert.ToDouble(a)),
            _ => Equals(e, a),
        };
        if (!same)
            throw new InvalidOperationException($"{what}: {where}: expected {e ?? "NULL"}, found {a ?? "NULL"}.");
    }

    private static bool Close(double e, double a) =>
        Math.Abs(e - a) <= 1e-9 * Math.Max(1.0, Math.Max(Math.Abs(e), Math.Abs(a)));

    private static bool IsScalar(Type t) =>
        t.IsPrimitive || t == typeof(string) || t == typeof(decimal) || Nullable.GetUnderlyingType(t) is not null;

    private static List<TRow> Sort<TRow>(IReadOnlyList<TRow> rows)
    {
        if (IsScalar(typeof(TRow))) return [.. rows.OrderBy(r => r)];
        var props = typeof(TRow).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        return [.. rows.OrderBy(r => string.Join('\u0001', props.Select(p => Convert.ToString(p.GetValue(r), System.Globalization.CultureInfo.InvariantCulture))), StringComparer.Ordinal)];
    }
}

/// <summary>
/// A query whose answer is <c>count(*)</c> over a set query (TPC-DS Q38, Q87): Polars runs <c>LongCount()</c>,
/// DuckDB the official SQL as a scalar. The translation and collect phases are those of the counted set query;
/// there is no list to materialize.
/// </summary>
public sealed class TpcdsCountBenchQuery<TRow>(
    string name,
    Func<IQueryable<TRow>> polars,
    Func<string> sql,
    Func<DataConnection, IQueryable<TRow>>? linq2Db = null) : TpcdsBenchQuery(name)
{
    private readonly TpcdsBenchQuery<TRow> _rows = new(name, polars, sql, linq2Db);

    public override bool HasLinq2Db => linq2Db is not null;

    public override int PolarsToList() => (int)polars().LongCount();

    public override int DapperToList(DbConnection connection) =>
        (int)connection.ExecuteScalar<long>(sql(), commandTimeout: 0);

    public override int Linq2DbToList(DataConnection db) =>
        (int)(linq2Db ?? throw new NotSupportedException($"{Name} has no Linq2DB implementation."))(db).LongCount();

    public override void PolarsTranslate() => _rows.PolarsTranslate();
    public override string Linq2DbTranslate(DataConnection db) => _rows.Linq2DbTranslate(db);
    public override void PreparePhases() => _rows.PreparePhases();
    public override void ReleasePhases() => _rows.ReleasePhases();
    public override long PolarsCollect() => _rows.PolarsCollect();
    public override int PolarsMaterialize() => throw new NotSupportedException($"{Name} returns a count; nothing to materialize.");

    public override void Explain() => _rows.Explain();

    public override string Validate(DbConnection connection, DataConnection? db)
    {
        long polarsCount = PolarsToList(), duckDbCount = DapperToList(connection);
        if (polarsCount != duckDbCount)
            throw new InvalidOperationException($"{Name}: DuckDB counts {duckDbCount}, Polars {polarsCount}.");
        if (HasLinq2Db && db is not null && Linq2DbToList(db) is var l2db && l2db != duckDbCount)
            throw new InvalidOperationException($"{Name}: DuckDB counts {duckDbCount}, Linq2DB {l2db}.");
        if (polarsCount == 0)
            throw new InvalidOperationException($"{Name}: empty result; the benchmark would measure nothing.");
        return $"{Name}: count {polarsCount}, identical in Polars, DuckDB+Dapper{(HasLinq2Db && db is not null ? ", Linq2DB" : "")}";
    }
}
