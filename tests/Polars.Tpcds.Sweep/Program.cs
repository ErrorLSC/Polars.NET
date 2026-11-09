using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Polars.Tpcds;

// A quick pass over all 99 TPC-DS queries, for finding gaps and checking that a rule helps beyond the queries it was
// written for. The BenchmarkDotNet suite in Polars.Benchmarks stays the place for careful numbers on a few queries.
//
//   DOTNET_ROOT=$HOME/.dotnet ~/.dotnet/dotnet run -c Release --project tests/Polars.Tpcds.Sweep \
//     -p:SkipRust=true -p:RustTargetDir=release -- run --out <dir> [--rounds 5] [--queries Q1,Q2] [--data <dir>]
//   ... -- plans --out <dir> [--queries Q1,Q2] [--data <dir>]
//
// `run` first compares each query's rows with DuckDB, then times it end to end (parquet to rows, plan cache on),
// alternating engines, and writes sweep.csv and sweep.md. `plans` writes each optimized plan with generated names
// normalized, so two directories can be compared with diff.

if (args.Length == 0 || args[0] is not ("run" or "plans"))
{
    Console.Error.WriteLine("usage: run|plans --out <dir> [--rounds n] [--queries Q1,Q2] [--data <dir>]");
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

if (args[0] == "plans")
{
    foreach (var query in selected)
        File.WriteAllText(Path.Combine(output, query.Name + ".txt"), Normalize(query.Explain()));
    Console.WriteLine($"{selected.Count} plans written to {output}");
    return 0;
}

int rounds = options.TryGetValue("rounds", out var r) ? int.Parse(r, CultureInfo.InvariantCulture) : 5;
using var duckDb = TpcdsVerification.OpenDuckDb();
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
