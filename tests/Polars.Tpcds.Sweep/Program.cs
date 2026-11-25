using System.Collections.Frozen;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Kaminari.Samidare.Sql;
using Polars.CSharp;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Wrappers;
using Polars.NET.Linq.Backend.Polars;
using Polars.Tpcds;

// A quick pass over all 99 TPC-DS queries, for finding gaps and checking that a rule helps beyond the queries it was
// written for. The BenchmarkDotNet suite in Polars.Benchmarks stays the place for careful numbers on a few queries.
//
//   DOTNET_ROOT=$HOME/.dotnet ~/.dotnet/dotnet run -c Release --project tests/Polars.Tpcds.Sweep \
//     -p:SkipRust=true -p:RustTargetDir=release -- run --out <dir> [--rounds 5] [--queries Q1,Q2] [--data <dir>]
//   ... -- plans --out <dir> [--queries Q1,Q2] [--data <dir>]
//   ... -- sql --out <dir> [--rounds 5] [--queries Q1,Q2] [--data <dir>] [--texts <dir>]
//   ... -- sql-plans --out <dir> [--queries Q1,Q2] [--data <dir>] [--texts <dir>]
//   ... -- sql-probe --out <dir> --queries Q1,Q2 [--data <dir>] [--texts <dir>]
//   ... -- sql-translate --out <dir> [--rounds 50] [--queries Q1,Q2] [--data <dir>] [--texts <dir>]
//
// `run` first compares each query's rows with DuckDB, then times it end to end (parquet to rows, plan cache on),
// alternating engines, and writes sweep.csv and sweep.md. `plans` writes each optimized plan with generated names
// normalized, so two directories can be compared with diff. `sql` runs DuckDB's own TPC-DS texts (the SQL frontend's test
// texts) through the SQL frontend, checks its rows against DuckDB's on the same text, then times the frontend (translation
// included), the LINQ version and DuckDB in rotating order, and writes sql-sweep.csv and sql-sweep.md. `sql-plans`
// writes each query's optimized plan from the SQL frontend (Qn.sql.txt), the engine's plan of it (Qn.sql.polars.txt)
// and the LINQ version's, as IR (Qn.linq.ir.txt) and the engine's (Qn.linq.txt). `sql-translate` times the SQL
// frontend's translation (with the bytes it allocates) and the execution of its plan apart.

if (args.Length == 0 || args[0] is not ("run" or "plans" or "sql" or "sql-plans" or "sql-probe" or "sql-translate"))
{
    Console.Error.WriteLine("usage: run|plans|sql|sql-plans|sql-probe|sql-translate --out <dir> [--rounds n] [--queries Q1,Q2] [--data <dir>] [--texts <dir>]");
    return 2;
}

var options = new Dictionary<string, string>();
for (int i = 1; i + 1 < args.Length; i += 2)
    options[args[i].TrimStart('-')] = args[i + 1];

if (!options.TryGetValue("out", out var output))
{
    Console.Error.WriteLine("--out is required");
    return 2;
}

TpcdsData.DataDir = options.TryGetValue("data", out var data) ? data : FindData();
Directory.CreateDirectory(output);

var selected = options.TryGetValue("queries", out var names)
    ? names.Split(',').Select(TpcdsCatalog.Get).ToList()
    : TpcdsCatalog.All.ToList();

if (args[0] == "sql-plans")
{
    var catalog = SqlSweep.Catalog();
    var textDir = options.TryGetValue("texts", out var t) ? t : SqlSweep.FindTexts();
    foreach (var query in selected)
    {
        var text = File.ReadAllText(Path.Combine(textDir, $"q{int.Parse(query.Name[1..], CultureInfo.InvariantCulture):00}.sql"));
        File.WriteAllText(Path.Combine(output, query.Name + ".sql.txt"), Kaminari.Samidare.IR.LogicalPlanOps.formatPlan(PolarsSql.plan(catalog, text).Item1));
        using (var lazy = PolarsSql.lazyFrame(catalog, text))
            File.WriteAllText(Path.Combine(output, query.Name + ".sql.polars.txt"), Normalize(PolarsWrapper.Explain(lazy, optimized: true)));
        File.WriteAllText(Path.Combine(output, query.Name + ".linq.txt"), Normalize(query.Explain()));
        File.WriteAllText(Path.Combine(output, query.Name + ".linq.ir.txt"), Normalize(query.ExplainLogicalPlan()));
    }
    Console.WriteLine($"{selected.Count} plan pairs written to {output}");
    return 0;
}

if (args[0] == "sql-translate")
{
    // Translation (text to optimized IR) and execution (lowering and collecting that plan) timed apart: translation in
    // a process with no native op yet (Polars' workers spin after one and slow the managed code that follows), with
    // the bytes it allocates, after a warm-up; execution after it (10 runs after 5), with a pause before each run.
    var catalog = SqlSweep.Catalog();
    var textDir = options.TryGetValue("texts", out var translateTexts) ? translateTexts : SqlSweep.FindTexts();
    int repeats = options.TryGetValue("rounds", out var rr) ? int.Parse(rr, CultureInfo.InvariantCulture) : 50;
    var texts = selected.Select(q => (q.Name, Text: File.ReadAllText(Path.Combine(textDir, $"q{int.Parse(q.Name[1..], CultureInfo.InvariantCulture):00}.sql")))).ToList();
    var translation = new Dictionary<string, string>();
    // Warm up for a few seconds over all the texts: a few dozen translations leave the compiler's code at the first
    // tier (Q88 measured 17 ms after 40 translations, 6 ms after 300).
    for (var warm = Stopwatch.StartNew(); warm.Elapsed < TimeSpan.FromSeconds(4);)
        foreach (var (_, text) in texts)
            PolarsSql.plan(catalog, text);
    foreach (var (name, text) in texts)
    {
        var times = new List<double>();
        long bytes = 0;
        for (int i = 0; i < repeats; i++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            var watch = Stopwatch.StartNew();
            PolarsSql.plan(catalog, text);
            times.Add(watch.Elapsed.TotalMilliseconds);
            bytes = GC.GetAllocatedBytesForCurrentThread() - before;
        }
        times.Sort();
        translation[name] = $"translate median {times[times.Count / 2],6:F2} min {times[0],6:F2} ms, {bytes / 1024.0:F0} KiB";
    }
    foreach (var (name, text) in texts)
    {
        var (plan, sources) = PolarsSql.plan(catalog, text);
        var times = new List<double>();
        for (int i = 0; i < 15; i++)
        {
            Thread.Sleep(250);
            var watch = Stopwatch.StartNew();
            using (var lf = PolarsPhysicalLowerer.lowerPlan(sources, plan))
            using (var df = PolarsWrapper.LazyCollect(lf, PlEngine.Auto)) { }
            if (i >= 5) times.Add(watch.Elapsed.TotalMilliseconds);
        }
        times.Sort();
        Console.WriteLine($"{name,-4} {translation[name]}; execute median {times[times.Count / 2],6:F2} min {times[0],6:F2} ms");
    }
    return 0;
}

if (args[0] == "sql-probe")
{
    // Every join, grouping and shared subplan of a query's plan timed alone (lowered and collected), from the SQL
    // frontend and from the LINQ version, then the whole plan: where the time of a slow query goes. A join's time
    // includes materializing its whole output, which the grouping above it may not need.
    var catalog = SqlSweep.Catalog();
    var textDir = options.TryGetValue("texts", out var probeTexts) ? probeTexts : SqlSweep.FindTexts();
    foreach (var query in selected)
    {
        var text = File.ReadAllText(Path.Combine(textDir, $"q{int.Parse(query.Name[1..], CultureInfo.InvariantCulture):00}.sql"));
        var (sqlPlan, sqlSources) = PolarsSql.plan(catalog, text);
        var linq = ((Kaminari.Samidare.IR.IInternalQueryExplainable)query.CreateQuery()).CompileLogicalQuery();
        Probe.Run($"{query.Name} SQL", sqlPlan, sqlSources);
        Probe.Run($"{query.Name} LINQ", linq.Plan, linq.Sources);
    }
    return 0;
}

if (args[0] == "plans")
{
    foreach (var query in selected)
        File.WriteAllText(Path.Combine(output, query.Name + ".txt"), Normalize(query.Explain()));
    Console.WriteLine($"{selected.Count} plans written to {output}");
    return 0;
}

int rounds = options.TryGetValue("rounds", out var r) ? int.Parse(r, CultureInfo.InvariantCulture) : 5;
using var duckDb = TpcdsVerification.OpenDuckDb();

if (args[0] == "sql")
    return SqlSweep.Run(duckDb, selected, options.TryGetValue("texts", out var texts) ? texts : SqlSweep.FindTexts(), output, rounds);
var results = new List<SweepResult>();
var clock = Stopwatch.StartNew();

foreach (var query in selected)
{
    var result = Sweep(query);
    results.Add(result);
    Console.WriteLine(
        result.Error is null
            ? $"{query.Name,-4} rows={result.Rows,8}  polars={result.PolarsMs,9:F1}  duckdb={result.DuckDbMs,9:F1}  x{result.Ratio:F2}"
            : $"{query.Name,-4} FAILED: {result.Error}");
}

Write(results, output, rounds, clock.Elapsed);
int failed = results.Count(x => x.Error is not null);
Console.WriteLine($"{results.Count - failed} of {results.Count} queries measured in {clock.Elapsed:mm\\:ss}; report in {output}");
return failed == 0 ? 0 : 1;

SweepResult Sweep(TpcdsCase query)
{
    try
    {
        // Doubles as the warm-up round.
        var sql = query.Sql();
        var difference = TpcdsVerification.FindDifference(TpcdsVerification.Rows(duckDb, sql), query.RunPolars());
        if (difference is not null)
            return new SweepResult(query.Name, 0, 0, 0, 0, 0, difference);

        var polars = new List<double>();
        var duck = new List<double>();
        int rows = 0;
        for (int round = 0; round < rounds; round++)
        {
            polars.Add(Time(() => rows = query.RunPolars().Count));
            duck.Add(Time(() => TpcdsVerification.Rows(duckDb, sql)));
        }

        return new SweepResult(query.Name, rows, Median(polars), Median(duck), polars.Min(), duck.Min(), null);
    }
    catch (Exception e)
    {
        return new SweepResult(query.Name, 0, 0, 0, 0, 0, e.GetBaseException().Message.ReplaceLineEndings(" "));
    }
}

static double Time(Action action)
{
    GC.Collect();
    var watch = Stopwatch.StartNew();
    action();
    return watch.Elapsed.TotalMilliseconds;
}

static double Median(List<double> values)
{
    var sorted = values.Order().ToList();
    int middle = sorted.Count / 2;
    return sorted.Count % 2 == 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

static string Normalize(string plan) =>
    Regex.Replace(plan, "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|u?[0-9a-f]{32}", "#");

static string FindData()
{
    for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
    {
        var candidate = Path.Combine(dir.FullName, "Polars.Benchmarks", "Data", "tpcds-sf2");
        if (File.Exists(Path.Combine(candidate, "store_sales.parquet")))
            return candidate;
    }

    throw new InvalidOperationException("TPC-DS SF2 data not found. Run Polars.Benchmarks/generate_tpcds_data.sh 2, or pass --data.");
}

static void Write(List<SweepResult> results, string output, int rounds, TimeSpan elapsed)
{
    var invariant = CultureInfo.InvariantCulture;
    var csv = new StringBuilder("query,rows,polars_ms,duckdb_ms,polars_min_ms,duckdb_min_ms,ratio,error\n");
    foreach (var x in results)
        csv.AppendLine(invariant, $"{x.Name},{x.Rows},{x.PolarsMs:F2},{x.DuckDbMs:F2},{x.PolarsMin:F2},{x.DuckDbMin:F2},{x.Ratio:F3},\"{x.Error?.Replace("\"", "'")}\"");
    File.WriteAllText(Path.Combine(output, "sweep.csv"), csv.ToString());

    var ok = results.Where(x => x.Error is null).ToList();
    double polars = ok.Sum(x => x.PolarsMs), duck = ok.Sum(x => x.DuckDbMs);
    double geometric = ok.Count == 0 ? 0 : Math.Exp(ok.Average(x => Math.Log(x.Ratio)));
    var md = new StringBuilder();
    md.AppendLine(invariant, $"# TPC-DS sweep: {ok.Count} of {results.Count} queries, median of {rounds} rounds ({elapsed:mm\\:ss})");
    md.AppendLine();
    md.AppendLine(invariant, $"Data: {TpcdsData.DataDir}");
    md.AppendLine();
    md.AppendLine(invariant, $"Total Polars / DuckDB: {polars:F0} / {duck:F0} ms = {polars / duck:F2}x; geometric mean of ratios {geometric:F2}x; Polars faster on {ok.Count(x => x.Ratio < 1)}.");
    md.AppendLine();
    md.AppendLine("Sorted by absolute gap. DuckDB reads rows as object arrays; Polars materializes its result objects.");
    md.AppendLine();
    md.AppendLine("| Query | Rows | Polars ms | DuckDB ms | Ratio | Gap ms |");
    md.AppendLine("|---|---:|---:|---:|---:|---:|");
    foreach (var x in ok.OrderByDescending(x => x.PolarsMs - x.DuckDbMs))
        md.AppendLine(invariant, $"| {x.Name} | {x.Rows} | {x.PolarsMs:F1} | {x.DuckDbMs:F1} | {x.Ratio:F2}x | {x.PolarsMs - x.DuckDbMs:+0.0;-0.0} |");
    foreach (var x in results.Where(x => x.Error is not null))
        md.AppendLine(invariant, $"\n**{x.Name} failed:** {x.Error}");
    File.WriteAllText(Path.Combine(output, "sweep.md"), md.ToString());
}

internal sealed record SweepResult(string Name, int Rows, double PolarsMs, double DuckDbMs, double PolarsMin, double DuckDbMin, string? Error)
{
    public double Ratio => DuckDbMs > 0 ? PolarsMs / DuckDbMs : 0;
}

/// The SQL frontend against DuckDB and the LINQ version, on DuckDB's own TPC-DS texts.
internal static class SqlSweep
{
    /// Differences from DuckDB where our answer is the right one: two adjacent rows in the other order, and cells where
    /// we give another value. The query passes only when, with the rows swapped back and those cells set to our values in
    /// DuckDB's rows, every row is equal; any other difference still fails it.
    static readonly FrozenDictionary<string, KnownDifference> KnownDifferences = new Dictionary<string, KnownDifference>
    {
        // SF2: web items 25243 and 17565 both have currency ratio 65/88 (11963.90/16197.28 and 10556.00/14291.20).
        // Our exact decimals tie them (currency_rank 5 for both, then ORDER BY item puts 17565 first); DuckDB divides
        // in doubles, 1 ulp apart, and ranks 17565 6th.
        ["Q49"] = new(SwappedRow: 26, Cells: [(27, 4, 5L)],
            Reason: "DuckDB's double division breaks an exact tie (both currency ratios are 65/88); our decimals keep it"),
    }.ToFrozenDictionary();

    sealed record KnownDifference(int SwappedRow, (int Row, int Column, object Ours)[] Cells, string Reason);

    /// The TPC-DS tables, each a Parquet scan under its name.
    public static ISqlCatalog Catalog()
    {
        var catalog = new SqlCatalog();
        foreach (var path in Directory.GetFiles(TpcdsData.DataDir, "*.parquet"))
            catalog.Register(Path.GetFileNameWithoutExtension(path), PolarsTableSources.ofLazyFrame(LazyFrame.ScanParquet(path).Handle));
        return catalog;
    }

    public static int Run(DuckDB.NET.Data.DuckDBConnection duckDb, List<TpcdsCase> selected, string texts, string output, int rounds)
    {
        var catalog = Catalog();
        var views = new StringBuilder();
        foreach (var path in Directory.GetFiles(TpcdsData.DataDir, "*.parquet"))
            views.Append(CultureInfo.InvariantCulture, $"CREATE VIEW {Path.GetFileNameWithoutExtension(path)} AS SELECT * FROM read_parquet('{path}');");
        using (var setup = duckDb.CreateCommand())
        {
            setup.CommandText = views.ToString();
            setup.ExecuteNonQuery();
        }

        var clock = Stopwatch.StartNew();
        var results = new List<SqlSweepResult>();
        foreach (var query in selected)
        {
            var result = Measure(query);
            results.Add(result);
            Console.WriteLine(
                result.Error is null
                    ? $"{query.Name,-4} rows={result.Rows,8}  sql={result.SqlMs,9:F1}  linq={result.LinqMs,9:F1}  duckdb={result.DuckDbMs,9:F1}  translate={result.TranslateMs,6:F1}  sql/duckdb x{result.SqlRatio:F2}{(result.Note is null ? "" : "  (known difference from DuckDB)")}"
                    : $"{query.Name,-4} FAILED: {result.Error}");
        }
        Write(results, output, rounds, clock.Elapsed);
        int failed = results.Count(x => x.Error is not null);
        Console.WriteLine($"{results.Count - failed} of {results.Count} queries measured in {clock.Elapsed:mm\\:ss}; report in {output}");
        return failed == 0 ? 0 : 1;

        SqlSweepResult Measure(TpcdsCase query)
        {
            try
            {
                var text = File.ReadAllText(Path.Combine(texts, $"q{int.Parse(query.Name[1..], CultureInfo.InvariantCulture):00}.sql"));
                // Doubles as the warm-up round.
                var expected = TpcdsVerification.Rows(duckDb, text);
                var actual = RunSql(catalog, text);
                var difference = TpcdsVerification.FindSqlDifference(expected, actual);
                string? note = null;
                if (difference is not null && KnownDifferences.TryGetValue(query.Name, out var known) && known.SwappedRow + 1 < actual.Count)
                {
                    int row = known.SwappedRow;
                    (actual[row], actual[row + 1]) = (actual[row + 1], actual[row]);
                    foreach (var (cellRow, column, ours) in known.Cells)
                        expected[cellRow][column] = ours;
                    if (TpcdsVerification.FindSqlDifference(expected, actual) is null)
                        note = $"{difference}; rows {row} and {row + 1} in the other order, the rest equal but the known cells ({known.Reason})";
                }
                if (difference is not null && note is null)
                    return new SqlSweepResult(query.Name, 0, 0, 0, 0, 0, difference);
                query.RunPolars();

                var sql = new List<double>();
                var linq = new List<double>();
                var duck = new List<double>();
                var translate = new List<double>();
                int rows = 0;
                Action[] runs =
                [
                    () => sql.Add(Time(() => rows = RunSql(catalog, text).Count)),
                    () => linq.Add(Time(() => query.RunPolars())),
                    () => duck.Add(Time(() => TpcdsVerification.Rows(duckDb, text))),
                ];
                for (int round = 0; round < rounds; round++)
                {
                    for (int i = 0; i < runs.Length; i++) runs[(i + round) % runs.Length]();
                    translate.Add(Time(() => PolarsSql.plan(catalog, text)));
                }
                return new SqlSweepResult(query.Name, rows, Median(sql), Median(linq), Median(duck), Median(translate), null, note);
            }
            catch (Exception e)
            {
                return new SqlSweepResult(query.Name, 0, 0, 0, 0, 0, e.GetBaseException().Message.ReplaceLineEndings(" "));
            }
        }
    }

    static List<object?[]> RunSql(ISqlCatalog catalog, string text)
    {
        using var df = new DataFrame(PolarsWrapper.LazyCollect(PolarsSql.lazyFrame(catalog, text), PlEngine.Auto));
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

    static double Time(Action action)
    {
        GC.Collect();
        var watch = Stopwatch.StartNew();
        action();
        return watch.Elapsed.TotalMilliseconds;
    }

    static double Median(List<double> values)
    {
        var sorted = values.Order().ToList();
        int middle = sorted.Count / 2;
        return sorted.Count % 2 == 1 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
    }

    /// The SQL frontend's test texts (DuckDB's tpcds_queries()).
    public static string FindTexts()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "tests", "Polars.CSharp.Linq.Tests", "Sql", "Tpcds");
            if (File.Exists(Path.Combine(candidate, "q01.sql")))
                return candidate;
        }
        throw new InvalidOperationException("The TPC-DS SQL texts were not found; pass --texts.");
    }

    static void Write(List<SqlSweepResult> results, string output, int rounds, TimeSpan elapsed)
    {
        var invariant = CultureInfo.InvariantCulture;
        var csv = new StringBuilder("query,rows,sql_ms,linq_ms,duckdb_ms,translate_ms,sql_duckdb,sql_linq,error\n");
        foreach (var x in results)
            csv.AppendLine(invariant, $"{x.Name},{x.Rows},{x.SqlMs:F2},{x.LinqMs:F2},{x.DuckDbMs:F2},{x.TranslateMs:F2},{x.SqlRatio:F3},{x.SqlLinqRatio:F3},\"{x.Error?.Replace("\"", "'")}\"");
        File.WriteAllText(Path.Combine(output, "sql-sweep.csv"), csv.ToString());

        var ok = results.Where(x => x.Error is null).ToList();
        double sql = ok.Sum(x => x.SqlMs), linq = ok.Sum(x => x.LinqMs), duck = ok.Sum(x => x.DuckDbMs), translate = ok.Sum(x => x.TranslateMs);
        double Geo(Func<SqlSweepResult, double> ratio) => ok.Count == 0 ? 0 : Math.Exp(ok.Average(x => Math.Log(ratio(x))));
        var md = new StringBuilder();
        md.AppendLine(invariant, $"# TPC-DS SQL sweep: {ok.Count} of {results.Count} queries, median of {rounds} rounds ({elapsed:mm\\:ss})");
        md.AppendLine();
        md.AppendLine(invariant, $"Data: {TpcdsData.DataDir}");
        md.AppendLine();
        md.AppendLine(invariant, $"Total SQL / LINQ / DuckDB: {sql:F0} / {linq:F0} / {duck:F0} ms; SQL/DuckDB {sql / duck:F2}x (geometric {Geo(x => x.SqlRatio):F2}x), SQL/LINQ {sql / linq:F2}x (geometric {Geo(x => x.SqlLinqRatio):F2}x); translation {translate:F0} ms in all.");
        md.AppendLine();
        md.AppendLine("Sorted by SQL's gap to DuckDB. SQL time includes translation (parse, bind, decorrelate, optimize); LINQ uses its plan cache.");
        md.AppendLine();
        md.AppendLine("| Query | Rows | SQL ms | LINQ ms | DuckDB ms | Translate ms | SQL/DuckDB | SQL/LINQ |");
        md.AppendLine("|---|---:|---:|---:|---:|---:|---:|---:|");
        foreach (var x in ok.OrderByDescending(x => x.SqlMs - x.DuckDbMs))
            md.AppendLine(invariant, $"| {x.Name} | {x.Rows} | {x.SqlMs:F1} | {x.LinqMs:F1} | {x.DuckDbMs:F1} | {x.TranslateMs:F1} | {x.SqlRatio:F2}x | {x.SqlLinqRatio:F2}x |");
        foreach (var x in results.Where(x => x.Error is not null))
            md.AppendLine(invariant, $"\n**{x.Name} failed:** {x.Error}");
        foreach (var x in results.Where(x => x.Note is not null))
            md.AppendLine(invariant, $"\n**{x.Name} differs from DuckDB:** {x.Note}");
        File.WriteAllText(Path.Combine(output, "sql-sweep.md"), md.ToString());
    }
}

internal sealed record SqlSweepResult(string Name, int Rows, double SqlMs, double LinqMs, double DuckDbMs, double TranslateMs, string? Error, string? Note = null)
{
    public double SqlRatio => DuckDbMs > 0 ? SqlMs / DuckDbMs : 0;
    public double SqlLinqRatio => LinqMs > 0 ? SqlMs / LinqMs : 0;
}

/// Times the parts of a plan alone: see the sql-probe mode.
internal static class Probe
{
    public static void Run(string label, Kaminari.Samidare.IR.LogicalPlan plan, Kaminari.Samidare.IR.SourceBinding[] sources)
    {
        Console.WriteLine($"== {label}");
        var parts = new List<(string Label, Kaminari.Samidare.IR.LogicalPlan Part)>();
        Find(plan, 0);
        foreach (var (partLabel, part) in parts)
            Console.WriteLine($"{Median(part)}  {partLabel}");
        Console.WriteLine($"{Median(plan)}  whole");

        void Find(Kaminari.Samidare.IR.LogicalPlan p, int depth)
        {
            if (p is Kaminari.Samidare.IR.LogicalPlan.GroupBy or Kaminari.Samidare.IR.LogicalPlan.Shared or Kaminari.Samidare.IR.LogicalPlan.Join)
            {
                var first = Kaminari.Samidare.IR.LogicalPlanOps.formatPlan(p).Split('\n')[0].Trim();
                parts.Add(($"{new string(' ', depth * 2)}{(first.Length > 110 ? first[..110] : first)}", p));
            }
            foreach (var input in Kaminari.Samidare.IR.LogicalPlanOps.getInputs(p))
                Find(input, depth + 1);
        }

        string Median(Kaminari.Samidare.IR.LogicalPlan p)
        {
            for (int w = 0; w < 4; w++) Collect(p);
            var times = new List<double>();
            long rows = 0;
            for (int i = 0; i < 5; i++)
            {
                GC.Collect();
                var watch = Stopwatch.StartNew();
                rows = Collect(p);
                times.Add(watch.Elapsed.TotalMilliseconds);
            }
            times.Sort();
            return $"{times[2]:F1}".PadLeft(8) + $" ms {rows,9} rows";
        }

        long Collect(Kaminari.Samidare.IR.LogicalPlan p)
        {
            using var lf = PolarsPhysicalLowerer.lowerPlan(sources, p);
            using var df = new DataFrame(PolarsWrapper.LazyCollect(lf, PlEngine.Auto));
            return df.Height;
        }
    }
}
