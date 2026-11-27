#nullable enable
using Xunit.Abstractions;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// Filters the managed CSV and NDJSON readers apply exactly, against the engine's own: the rows of every filter, read
/// through a scan the filter is pushed to, are those the engine keeps over the same rows in memory. Nulls, NaN, signed
/// zeros, infinities, empty and non-ASCII strings, dates and booleans, with each operator the engine pushes. A filter
/// the reader reports applied leaves the plan (no FILTER above the scan, no SELECTION in it).
/// </summary>
public class ManagedScanExactFilterTests(ITestOutputHelper output) : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("exact-filter-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    // i: Int64 with nulls; f: Float64 with NaN, ±0, ±inf, nulls; s: strings with "", non-ASCII, nulls; d: dates; b: bools.
    private static readonly (string? I, string? F, string? S, string? D, string? B)[] Values =
    [
        ("1", "0.5", "a", "2024-01-01", "true"),
        ("2", "-0.0", "b", "2024-01-02", "false"),
        ("3", "0.0", "", "2024-02-29", null),
        ("4", "NaN", "é", "2023-12-31", "true"),
        ("5", "inf", "ä", "2024-01-01", "false"),
        ("6", "-inf", "Z", null, "true"),
        (null, null, null, "2024-03-01", null),
        ("8", "1.5", "ab", "2024-01-15", "true"),
        ("9", "1e300", "a b", "2020-01-01", "false"),
        ("10", "-2.25", "äb", "2024-01-01", "true"),
        ("-3", "1.5", "aé", "2024-12-31", "false"),
        ("0", "NaN", "b", "2024-01-02", null),
        ("7", null, "a", null, "true"),
        ("5", "0.5", null, "2024-01-01", "false"),
    ];

    private string Csv()
    {
        string path = Path.Combine(_dir, "data.csv");
        File.WriteAllLines(path, ["i,f,s,d,b", .. Values.Select(v => $"{v.I},{v.F},{Quoted(v.S)},{v.D},{v.B}")]);
        return path;

        // An empty string quoted (an empty field is null).
        static string Quoted(string? s) => s is null ? "" : $"\"{s}\"";
    }

    private string Ndjson()
    {
        string path = Path.Combine(_dir, "data.ndjson");
        File.WriteAllLines(path, Values.Select(v =>
            $$"""{"i":{{v.I ?? "null"}},"f":{{Json(v.F)}},"s":{{(v.S is null ? "null" : $"\"{v.S}\"")}},"d":{{(v.D is null ? "null" : $"\"{v.D}\"")}},"b":{{v.B ?? "null"}}}"""));
        return path;

        // JSON has no NaN or infinities: those rows hold nulls in NDJSON.
        static string Json(string? f) => f is null or "NaN" or "inf" or "-inf" ? "null" : f;
    }

    private static LazyFrame Scan(string format, string path) => format == "csv"
        ? LazyFrame.ScanCsv(path, dtypeOverride: new Dictionary<string, DataType> { ["d"] = DataType.Date })
        : LazyFrame.ScanNdjson(path, schemaOverrides: new Dictionary<string, DataType> { ["d"] = DataType.Date });

    /// <summary>The filters, by name (made anew for each use: an expression is consumed by the query).</summary>
    private static readonly (string Name, Func<Expr> Make)[] Filters =
    [
        ("i > 5", () => Pl.Col("i") > 5),
        ("i >= 5", () => Pl.Col("i") >= 5),
        ("i < 5", () => Pl.Col("i") < 5),
        ("i <= 5", () => Pl.Col("i") <= 5),
        ("i == 5", () => Pl.Col("i") == 5),
        ("i != 5", () => Pl.Col("i") != 5),
        ("i is null", () => Pl.Col("i").IsNull()),
        ("i is not null", () => Pl.Col("i").IsNotNull()),
        ("i between 2 and 8", () => Pl.Col("i").IsBetween(2, 8)),
        ("i between 2 and 8 (open)", () => Pl.Col("i").IsBetween(2, 8, ClosedInterval.None)),
        ("i in [1, 3, 9]", () => Pl.Col("i").IsIn(new long[] { 1, 3, 9 })),
        ("not i > 5", () => !(Pl.Col("i") > 5)),
        ("i > 5 or s is null", () => (Pl.Col("i") > 5) | Pl.Col("s").IsNull()),
        ("i > 2 and i < 9", () => (Pl.Col("i") > 2) & (Pl.Col("i") < 9)),
        ("f > 0", () => Pl.Col("f") > 0.0),
        ("f >= 0", () => Pl.Col("f") >= 0.0),
        ("f < 0", () => Pl.Col("f") < 0.0),
        ("f == 0", () => Pl.Col("f") == 0.0),
        ("f != 1.5", () => Pl.Col("f") != 1.5),
        ("f > 1e299", () => Pl.Col("f") > 1e299),
        ("f == NaN", () => Pl.Col("f") == double.NaN),
        ("f > NaN", () => Pl.Col("f") > double.NaN),
        ("f < inf", () => Pl.Col("f") < double.PositiveInfinity),
        ("f between -1 and 1", () => Pl.Col("f").IsBetween(-1.0, 1.0)),
        ("s == a", () => Pl.Col("s") == "a"),
        ("s != a", () => Pl.Col("s") != "a"),
        ("s > b", () => Pl.Col("s") > "b"),
        ("s < ä", () => Pl.Col("s") < "ä"),
        ("s == empty", () => Pl.Col("s") == ""),
        ("s starts with a", () => Pl.Col("s").Str.StartsWith("a")),
        ("s starts with ä", () => Pl.Col("s").Str.StartsWith("ä")),
        ("s ends with b", () => Pl.Col("s").Str.EndsWith("b")),
        ("s contains ' '", () => Pl.Col("s").Str.Contains(" ", literal: true)),
        ("s in [a, é]", () => Pl.Col("s").IsIn(new[] { "a", "é" })),
        ("s eq_missing null", () => Pl.Col("s").EqMissing(Pl.Lit((string?)null))),
        ("s neq_missing a", () => Pl.Col("s").NeqMissing(Pl.Lit("a"))),
        ("d > 2024-01-01", () => Pl.Col("d") > Pl.Lit(new DateOnly(2024, 1, 1))),
        ("d == 2024-01-01", () => Pl.Col("d") == Pl.Lit(new DateOnly(2024, 1, 1))),
        ("b", () => Pl.Col("b")),
        ("b == false", () => Pl.Col("b") == false),
        ("i > 3 and s == a", () => (Pl.Col("i") > 3) & (Pl.Col("s") == "a")),
        ("i as f64 > 2.5", () => Pl.Col("i").Cast(DataType.Float64) > 2.5),
        ("s as i64 (non-strict) is null", () => Pl.Col("s").Cast(DataType.Int64, strict: false).IsNull()),
    ];

    public static TheoryData<string> Formats => ["csv", "ndjson"];

    [Theory]
    [MemberData(nameof(Formats))]
    public void Every_Filter_Keeps_The_Engines_Rows(string format)
    {
        string path = format == "csv" ? Csv() : Ndjson();
        using DataFrame all = Scan(format, path).Collect();
        List<string> differing = [], claimed = [];
        foreach (var (name, make) in Filters)
        {
            using LazyFrame memory = all.Lazy();
            using LazyFrame byEngine = memory.Filter(make());
            using DataFrame expected = byEngine.Collect();
            using LazyFrame scan = Scan(format, path);
            using LazyFrame pushed = scan.Filter(make());
            string plan = pushed.Explain();
            // The engine filters what the reader does not apply: a FILTER above the scan, or the scan's SELECTION.
            bool applied = !plan.Contains("FILTER", StringComparison.Ordinal) && !plan.Contains("SELECTION:", StringComparison.Ordinal);
            using DataFrame actual = pushed.Collect();
            bool same = expected.Equals(actual);
            if (!same) differing.Add($"{name}: engine {expected.Height} rows, scan {actual.Height}\n{expected}\n{actual}");
            if (applied) claimed.Add(name);
            output.WriteLine($"{(same ? "OK  " : "DIFF")} {(applied ? "applied by reader" : "engine filters   ")} {name} ({expected.Height} rows)");
        }
        Assert.True(differing.Count == 0, string.Join("\n\n", differing));
        // The plain comparisons are the reader's (the engine leaves them out of its plan).
        Assert.Contains("i > 5", claimed);
        Assert.Contains("s == a", claimed);
        Assert.Contains("i > 3 and s == a", claimed);
        // A cast is the engine's: the document does not say whether it is strict.
        Assert.DoesNotContain("i as f64 > 2.5", claimed);
        Assert.DoesNotContain("s as i64 (non-strict) is null", claimed);
    }
}
