using System.Text;
using Kaminari.Seseragi.Files;
using Polars.NET.Core;
using Polars.NET.Core.Csv;
using Polars.NET.Core.Enums;
using Xunit.Abstractions;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// The managed CSV entry points of Core (<see cref="ManagedCsv"/>) against the engine's own CSV IO, still behind the
/// public API (C5a: the differential before the switch): reads of files, globs, memory and streams give the frames the
/// engine gives where the decided rules agree (types, nulls, dates, skips, comments, row index, limits, column order,
/// chunks); scans give the same query results; writes and sinks give the engine's bytes; partitioned sinks read back.
/// </summary>
public sealed class ManagedCsvTests(ITestOutputHelper output) : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("managed-csv-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string At(string name) => Path.Combine(_dir, name);

    private string Write(string name, string text)
    {
        string path = At(name);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, text);
        return path;
    }

    private const string Mixed = "id,name,price,when,day,flag\n1,alpha,1.5,2024-01-02 03:04:05,2024-01-02,true\n2,\"b,eta\",,2024-02-03 00:00:00,2024-02-03,false\n" +
        "3,\"quo\"\"te\",-0.25,2024-03-04 05:06:07.123456,2024-03-04,\n4,,1e3,,2024-04-05,true\n";

    public static TheoryData<string, string> Inputs() => new()
    {
        { "mixed", Mixed },
        { "ints-floats", "a,b\n1,2.5\n3,4\n-5,1e-3\n" },
        { "strings", "s,t\n\"x\ny\",plain\n\"\",\"\"\"q\"\"\"\nü,😀\n" },
        { "blank-row", "a,b\n1,x\n\n3,z\n" },
        { "crlf", "a,b\r\n1,x\r\n2,y\r\n" },
        { "times", "t\n12:34:56\n00:00:01.5\n" },
        { "zoned", "t\n2024-01-02T03:04:05Z\n2024-01-02T03:04:05+09:00\n" },
        { "nulls", "a,b\nNA,1\nx,NA\n" },
    };

    private static void Same(DataFrame expected, DataFrame actual, string what)
    {
        if (!actual.Equals(expected))
        {
            Assert.Fail($"{what}\nengine:\n{expected}\nmanaged:\n{actual}");
        }
    }

    /// <summary>Files read as the engine reads them (through the public API, still native): the same frame.</summary>
    [Theory]
    [MemberData(nameof(Inputs))]
    public void AFile_ReadsAsTheEngineReadsIt(string name, string text)
    {
        string path = Write(name + ".csv", text);
        foreach (bool rechunk in (bool[])[false, true])
        {
            using DataFrame engine = DataFrame.ReadCsv(path, rechunk: rechunk, nullValues: name == "nulls" ? ["NA"] : null);
            using var managed = new DataFrame(ManagedCsv.ReadCsv(path, null, new CsvScanArgs { Rechunk = rechunk, NullValues = name == "nulls" ? ["NA"] : null }, null));
            Same(engine, managed, $"{name} rechunk {rechunk}");
        }
    }

    [Fact]
    public void Options_ReadAsTheEngineReadsThem()
    {
        string path = Write("opts.csv", "# comment\nskip me\nid;name;v\n1;a;1,5\n2;b;2,25\n3;c;\n");
        using DataFrame engine = DataFrame.ReadCsv(path, separator: ';', commentPrefix: "#", skipRows: 1, decimalComma: true, rowIndexName: "row",
            rowIndexOffset: 10, nRows: 2);
        using var managed = new DataFrame(ManagedCsv.ReadCsv(path, null, new CsvScanArgs
        {
            Separator = ';', CommentPrefix = "#", SkipRows = 1, DecimalComma = true, RowIndexName = "row", RowIndexOffset = 10, NRows = 2,
        }, null));
        Same(engine, managed, "options");
        // Columns asked in another order than the file's: the frame takes theirs.
        string mixed = Write("mixed.csv", Mixed);
        using DataFrame engineCols = DataFrame.ReadCsv(mixed, columns: ["price", "id"]);
        using var managedCols = new DataFrame(ManagedCsv.ReadCsv(mixed, ["price", "id"], new CsvScanArgs(), null));
        Same(engineCols, managedCols, "columns");
    }

    [Fact]
    public void Memory_AndStreams_ReadAsFiles()
    {
        byte[] bytes = Encoding.UTF8.GetBytes(Mixed);
        using DataFrame engine = DataFrame.ReadCsv(bytes);
        using (var fromMemory = new DataFrame(ManagedCsv.ReadCsv(bytes.AsMemory(), null, new CsvScanArgs()))) Same(engine, fromMemory, "memory");
        using (var fromSpan = new DataFrame(ManagedCsv.ReadCsv(bytes.AsSpan(), null, new CsvScanArgs()))) Same(engine, fromSpan, "span");
        using (var fromStream = new DataFrame(ManagedCsv.ReadCsv(new MemoryStream(bytes), null, new CsvScanArgs()))) Same(engine, fromStream, "stream");
        using (var scanned = new LazyFrame(ManagedCsv.ScanCsv(bytes.AsSpan(), new CsvScanArgs())).Collect()) Same(engine, scanned, "scanned memory");
        using (var scannedStream = new LazyFrame(ManagedCsv.ScanCsv(new MemoryStream(bytes), new CsvScanArgs())).Collect()) Same(engine, scannedStream, "scanned stream");
    }

    /// <summary>Queries over a scan (projection, a filter, a row limit, an aggregate, the row index and the path) give the engine's results; a scan runs again.</summary>
    [Fact]
    public void Scans_GiveTheEnginesQueryResults()
    {
        Write("g/a.csv", Mixed);
        Write("g/b.csv", Mixed.Replace("alpha", "gamma"));
        string glob = At("g/*.csv");
        using LazyFrame engine = LazyFrame.ScanCsv(glob, rowIndexName: "row", includeFilePaths: "file", inferSchemaFiles: 10);
        using var managed = new LazyFrame(ManagedCsv.ScanCsv(glob, new CsvScanArgs { RowIndexName = "row", IncludeFilePaths = "file" }, null));
        Func<LazyFrame, LazyFrame>[] queries =
        [
            q => q,
            q => q.Select("name", "id"),
            q => q.Filter(Col("price") > 0).Select("id", "row", "file"),
            q => q.Head(3),
            q => q.GroupBy("flag").Agg(Col("price").Sum().Alias("s"), Col("id").Count().Alias("n")).Sort("flag"),
        ];
        for (int i = 0; i < queries.Length; i++)
            for (int run = 0; run < 2; run++)
            {
                using DataFrame a = queries[i](engine.Clone()).Collect(), b = queries[i](managed.Clone()).Collect();
                Same(a, b, $"query {i} run {run}");
            }
    }

    /// <summary>A frame written, a query sunk (to a file, a stream, memory), as the engine writes them.</summary>
    [Fact]
    public void Writes_AndSinks_AsTheEngineWritesThem()
    {
        using DataFrame df = DataFrame.ReadCsv(Write("w.csv", Mixed));
        var args = new CsvWriteArgs();
        byte[] engine = df.WriteCsvNativeMemory().ToArray();
        Assert.Equal(Encoding.UTF8.GetString(engine), Encoding.UTF8.GetString(ManagedCsv.WriteCsvToMemory(df.Handle, args)));
        Assert.Equal(Encoding.UTF8.GetString(engine), Encoding.UTF8.GetString(ManagedCsv.SinkCsvToMemory(df.Lazy().CloneHandle(), args, maintainOrder: true)));
        string file = At("out/sunk.csv");
        ManagedCsv.SinkCsv(df.Lazy().CloneHandle(), file, args, checkExtension: true, maintainOrder: true, PlSyncOnClose.None, mkdir: true, null);
        Assert.Equal(Encoding.UTF8.GetString(engine), File.ReadAllText(file));
        var semicolons = new CsvWriteArgs { Separator = ';', QuoteStyle = PlQuoteStyle.Always, NullValue = "NULL", IncludeHeader = false };
        Assert.Equal(Encoding.UTF8.GetString(df.WriteCsvNativeMemory(separator: (byte)';', quoteStyle: QuoteStyle.Always, nullValue: "NULL", includeHeader: false).ToArray()),
            Encoding.UTF8.GetString(ManagedCsv.WriteCsvToMemory(df.Handle, semicolons)));
        // A query without rows: its header alone.
        using DataFrame none = df.Head(0);
        Assert.Equal("id,name,price,when,day,flag\n", Encoding.UTF8.GetString(ManagedCsv.SinkCsvToMemory(none.Lazy().CloneHandle(), args, maintainOrder: true)));
    }

    [Fact]
    public void APartitionedSink_ReadsBack()
    {
        using DataFrame df = DataFrame.ReadCsv(Write("p.csv", Mixed));
        string root = At("parts");
        ManagedCsv.SinkCsvPartitioned(df.Lazy().CloneHandle(), root, ["flag"], includeKeys: true, keysPreGrouped: false, 0, 0, new CsvWriteArgs(),
            maintainOrder: true, PlSyncOnClose.None, Kaminari.Seseragi.Files.ExistingData.Error, null);
        Assert.All(Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories), f => Assert.StartsWith("part-", Path.GetFileName(f)));
        using DataFrame back = new LazyFrame(ManagedCsv.ScanCsv(Path.Combine(root, "**", "*.csv"), new CsvScanArgs(), null)).Collect();
        using DataFrame expected = df.Sort("id"), actual = back.Sort("id");
        output.WriteLine(actual.ToString());
        Same(expected.Select("id", "name", "flag"), actual.Select("id", "name", "flag"), "partitions");
    }
}
