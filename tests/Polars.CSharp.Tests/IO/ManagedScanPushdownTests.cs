#nullable enable
using System.Reflection;
using Apache.Arrow;
using Polars.NET.Core.Csv;
using Xunit.Abstractions;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// The managed CSV and NDJSON scans as I/O plugin resolvers: the engine's filters reach the readers (rows skipped,
/// partitions pruned), the row index and the rows limit keep the engine's meaning (numbered and counted before any
/// filter), and a path scan's fingerprint covers every option that decides its data.
/// </summary>
public class ManagedScanPushdownTests(ITestOutputHelper output) : IDisposable
{
    private const int Rows = 1000;
    private readonly string _dir = Directory.CreateTempSubdirectory("managed-scan-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    /// <summary>Rows id, g ("a"/"b"), v; the last row's v is not a number, so reading it fails.</summary>
    private string Csv(string name = "data.csv")
    {
        string path = Path.Combine(_dir, name);
        using var writer = new StreamWriter(path);
        writer.WriteLine("id,g,v");
        for (int i = 0; i < Rows; i++) writer.WriteLine($"{i},{(i % 2 == 0 ? "a" : "b")},{(i == Rows - 1 ? "oops" : i.ToString())}");
        return path;
    }

    /// <summary>The same rows as NDJSON; the last row's v is a string, so reading it as a number fails.</summary>
    private string Ndjson()
    {
        string path = Path.Combine(_dir, "data.ndjson");
        using var writer = new StreamWriter(path);
        for (int i = 0; i < Rows; i++)
            writer.WriteLine($$"""{"id":{{i}},"g":"{{(i % 2 == 0 ? "a" : "b")}}","v":{{(i == Rows - 1 ? "\"oops\"" : i.ToString())}}}""");
        return path;
    }

    private static int[] Ints(DataFrame df, string column) => [.. Enumerable.Range(0, (int)df.Height).Select(i => Convert.ToInt32(df[i, column]))];

    private static LazyFrame Scan(string format, string path, string? rowIndex = null, ulong? nRows = null) => format switch
    {
        "csv" => LazyFrame.ScanCsv(path, rowIndexName: rowIndex, nRows: nRows),
        _ => LazyFrame.ScanNdjson(path, rowIndexName: rowIndex, nRows: nRows),
    };

    private string File(string format) => format == "csv" ? Csv() : Ndjson();

    public static TheoryData<string> Formats => ["csv", "ndjson"];

    [Theory]
    [MemberData(nameof(Formats))]
    public void A_Filter_Reaches_The_Reader(string format)
    {
        using var lf = Scan(format, File(format));
        using var q = lf.Filter(Pl.Col("id") < 10).Select(Pl.Col("id"));
        string plan = q.Explain();
        output.WriteLine(plan);
        Assert.Contains("skips rows by: Binary (Lt, Column \"id\"", plan);
    }

    [Fact]
    public void A_Csv_Filter_Skips_Rows_Before_Decoding_Them()
    {
        string path = Csv();
        // Read whole, the last row fails; filtered, the reader never decodes its v (CSV decodes the filter's columns
        // first; NDJSON decodes whole objects, so its rows' errors stay).
        using (var all = LazyFrame.ScanCsv(path)) Assert.ThrowsAny<Exception>(() => all.Collect().Dispose());
        using var lf = LazyFrame.ScanCsv(path);
        using var q = lf.Filter(Pl.Col("id") < 10).Select(Pl.Col("id"), Pl.Col("v"));
        using var df = q.Collect();
        Assert.Equal([.. Enumerable.Range(0, 10)], Ints(df, "id"));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void The_Row_Index_Numbers_Rows_Before_The_Filter(string format)
    {
        using var lf = Scan(format, File(format), rowIndex: "i");
        using var q = lf.Filter((Pl.Col("g") == "b") & (Pl.Col("id") < 20)).Select(Pl.Col("i"), Pl.Col("id"));
        using var df = q.Collect();
        Assert.Equal([1, 3, 5, 7, 9, 11, 13, 15, 17, 19], Ints(df, "i"));
        Assert.Equal(Ints(df, "id"), Ints(df, "i"));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void A_Filter_On_The_Row_Index_Is_The_Engines(string format)
    {
        using var lf = Scan(format, File(format), rowIndex: "i");
        using (var q = lf.Filter(Pl.Col("i") < 3).Select(Pl.Col("i")))
        using (var df = q.Collect()) Assert.Equal([0, 1, 2], Ints(df, "i"));
        // Deep in the input, on both sides of a bound: numbers the reader would give a skipping read differ there.
        using (var q = lf.Filter((Pl.Col("i") >= 990) & (Pl.Col("i") < 995) & (Pl.Col("g") == "a")).Select(Pl.Col("i"), Pl.Col("id")))
        using (var df = q.Collect())
        {
            Assert.Equal([990, 992, 994], Ints(df, "i"));
            Assert.Equal(Ints(df, "id"), Ints(df, "i"));
        }
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void N_Rows_Are_Taken_Before_The_Filter(string format)
    {
        using var lf = Scan(format, File(format), nRows: 10);
        using var q = lf.Filter(Pl.Col("id") >= 5).Select(Pl.Col("id"));
        using var df = q.Collect();
        Assert.Equal([5, 6, 7, 8, 9], Ints(df, "id"));
    }

    [Theory]
    [MemberData(nameof(Formats))]
    public void Head_Stops_Before_The_Failing_Row(string format)
    {
        using var lf = Scan(format, File(format));
        using var q = lf.Head(5);
        using var df = q.Collect(Engine.Streaming);
        Assert.Equal([0, 1, 2, 3, 4], Ints(df, "id"));
    }

    [Fact]
    public void A_Partition_Filter_Leaves_Other_Partitions_Unread()
    {
        foreach (string key in (string[])["a", "b"])
        {
            Directory.CreateDirectory(Path.Combine(_dir, "t", $"k={key}"));
            // Partition b's values fail to decode as the column's type: reading it at all fails the query.
            System.IO.File.WriteAllLines(Path.Combine(_dir, "t", $"k={key}", "part.csv"),
                ["x", .. Enumerable.Range(0, 5).Select(i => key == "a" ? i.ToString() : $"bad{i}")]);
        }
        using var lf = LazyFrame.ScanCsv(Path.Combine(_dir, "t"), hivePartitioning: true,
            dtypeOverride: new Dictionary<string, DataType> { ["x"] = DataType.Int64 });
        using var q = lf.Filter(Pl.Col("k") == "a").Select(Pl.Col("x"));
        using var df = q.Collect();
        Assert.Equal([0, 1, 2, 3, 4], Ints(df, "x"));
    }

    [Fact]
    public void Explain_Names_The_Managed_Scan()
    {
        using var lf = LazyFrame.ScanCsv(Csv());
        string plan = lf.Explain(optimized: false);
        output.WriteLine(plan);
        Assert.Contains("seseragi: csv[", plan);
    }

    /// <summary>Options that shape the work or are applied above the scan, not the data the scan reads.</summary>
    private static readonly HashSet<string> NotData =
    [
        nameof(CsvScanArgs.Schema), nameof(CsvScanArgs.DtypeOverride), nameof(CsvScanArgs.NRows), nameof(CsvScanArgs.NThreads),
        nameof(CsvScanArgs.Rechunk),
    ];

    [Fact]
    public void A_Csv_Path_Fingerprint_Covers_Every_Option_Of_The_Data()
    {
        var schema = new Apache.Arrow.Schema([new Apache.Arrow.Field("id", Apache.Arrow.Types.Int64Type.Default, true)], null);
        ulong baseline = ManagedCsv.Fingerprint("a.csv", new CsvScanArgs(), null, schema);
        Assert.Equal(baseline, ManagedCsv.Fingerprint("a.csv", new CsvScanArgs(), null, schema));
        Assert.NotEqual(baseline, ManagedCsv.Fingerprint("b.csv", new CsvScanArgs(), null, schema));
        Assert.NotEqual(baseline, ManagedCsv.Fingerprint("a.csv", new CsvScanArgs(), null,
            new Apache.Arrow.Schema([new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, true)], null)));
        Assert.NotEqual(baseline, ManagedCsv.Fingerprint("a.csv", new CsvScanArgs(), new global::Polars.NET.Core.Delta.CloudArgs(Keys: ["endpoint"], Values: ["x"]), schema));

        // Every option that decides the data, changed alone, changes the fingerprint (a new option must be added).
        foreach (PropertyInfo property in typeof(CsvScanArgs).GetProperties())
        {
            if (NotData.Contains(property.Name)) continue;
            var args = new CsvScanArgs();
            property.SetValue(args, Changed(property.PropertyType, property.GetValue(args)));
            Assert.True(ManagedCsv.Fingerprint("a.csv", args, null, schema) != baseline, $"{property.Name} is not in the fingerprint");
        }

        static object? Changed(Type type, object? value) => type switch
        {
            _ when type == typeof(bool) => !(bool)value!,
            _ when type == typeof(char) => (char)value! == ';' ? ',' : ';',
            _ when type == typeof(char?) => (char?)value == ';' ? ',' : ';',
            _ when type == typeof(ulong) => (ulong)value! + 1,
            _ when type == typeof(ulong?) => value is ulong n ? n + 1 : 7UL,
            _ when type == typeof(string) => (string?)value + "x",
            _ when type == typeof(IReadOnlyList<string>) => (IReadOnlyList<string>)["NA"],
            _ when type == typeof(System.Text.Encoding) => System.Text.Encoding.Latin1,
            _ => throw new InvalidOperationException($"No changed value for {type}: extend the test"),
        };
    }
}
