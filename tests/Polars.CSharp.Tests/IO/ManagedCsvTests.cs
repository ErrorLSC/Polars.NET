using System.Text;
using Kaminari.Seseragi.Files;
using Polars.NET.Core;
using Polars.NET.Core.Csv;
using Polars.NET.Core.Enums;
using Xunit.Abstractions;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// The public CSV API, now on the managed reader and writer (<see cref="ManagedCsv"/>), against the engine's own CSV IO
/// (<see cref="NativeCsv"/>: the C5 differential, until native CSV IO is deleted): reads of files, globs, memory and
/// streams give the frames the engine gives where the decided rules agree (types, nulls, dates, skips, comments, row
/// index, limits, column order, chunks); scans give the same query results; writes and sinks give the engine's bytes;
/// partitioned sinks read back.
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
            using DataFrame engine = NativeCsv.Read(path, new CsvScanArgs { Rechunk = rechunk, NullValues = name == "nulls" ? ["NA"] : null });
            using DataFrame managed = DataFrame.ReadCsv(path, rechunk: rechunk, nullValues: name == "nulls" ? ["NA"] : null);
            Same(engine, managed, $"{name} rechunk {rechunk}");
        }
    }

    [Fact]
    public void Options_ReadAsTheEngineReadsThem()
    {
        string path = Write("opts.csv", "# comment\nskip me\nid;name;v\n1;a;1,5\n2;b;2,25\n3;c;\n");
        using DataFrame engine = NativeCsv.Read(path, new CsvScanArgs
        {
            Separator = ';', CommentPrefix = "#", SkipRows = 1, DecimalComma = true, RowIndexName = "row", RowIndexOffset = 10, NRows = 2,
        });
        using DataFrame managed = DataFrame.ReadCsv(path, separator: ';', commentPrefix: "#", skipRows: 1, decimalComma: true, rowIndexName: "row",
            rowIndexOffset: 10, nRows: 2);
        Same(engine, managed, "options");
        // Columns asked in another order than the file's: the frame takes theirs.
        string mixed = Write("mixed.csv", Mixed);
        using DataFrame engineCols = NativeCsv.Read(mixed, new CsvScanArgs(), ["price", "id"]);
        using DataFrame managedCols = DataFrame.ReadCsv(mixed, columns: ["price", "id"]);
        Same(engineCols, managedCols, "columns");
    }

    /// <summary>Records ended by another character than '\n' read as the engine reads them.</summary>
    [Fact]
    public void AnotherLineEnd_ReadsAsTheEngineReadsIt()
    {
        string path = Write("eol.csv", "a,b|1,x|2,\"y|z\"|3,w|");
        using DataFrame engine = NativeCsv.Read(path, new CsvScanArgs { EolChar = '|' });
        using DataFrame managed = DataFrame.ReadCsv(path, eolChar: '|');
        Same(engine, managed, "eol");
    }

    /// <summary>
    /// A schema with a header matches the columns by name (C1 rule 1): the frame in the schema's order with each column's
    /// own values, where the engine (0.55) renamed the file's columns by position.
    /// </summary>
    [Fact]
    public void ASchema_MatchesColumnsByName()
    {
        string path = Write("named.csv", "id,name\n1,a\n2,b\n");
        using DataFrame managed = DataFrame.ReadCsv(path, schema: new Dictionary<string, DataType> { ["name"] = DataType.String, ["id"] = DataType.Int64 });
        Assert.Equal(["name", "id"], managed.Columns);
        Assert.Equal(["a", "b"], managed["name"].ToArray<string>());
        Assert.Equal([1L, 2L], managed["id"].ToArray<long>());
    }

    /// <summary>
    /// The released default of <c>inferSchemaFiles</c> (<see cref="ulong.MaxValue"/>: every file) now means 10 files, as
    /// Polars 2.1 takes: a column that is a float only from the eleventh file on is refused with the fixes named.
    /// </summary>
    [Fact]
    public void InferSchemaFiles_DefaultIsTenFiles()
    {
        for (int i = 0; i < 11; i++) Write($"many/f{i:D2}.csv", i < 10 ? $"v\n{i}\n" : "v\n1.5\n");
        string glob = At("many/*.csv");
        Exception error = Record.Exception(() => DataFrame.ReadCsv(glob).Dispose());
        Assert.NotNull(error);
        output.WriteLine(error.ToString());
        using DataFrame all = DataFrame.ReadCsv(glob, inferSchemaFiles: 11);
        Assert.Equal(DataType.Float64, all["v"].DataType);
    }

    /// <summary>Empty input with raiseIfEmpty false: an empty frame from every scan, with or without types given.</summary>
    [Fact]
    public void EmptyInput_ScansAsAnEmptyFrame()
    {
        string path = Write("empty.csv", "");
        var types = new Dictionary<string, DataType> { ["a"] = DataType.Int8 };
        using (LazyFrame lf = LazyFrame.ScanCsv(path, raiseIfEmpty: false)) using (DataFrame df = lf.Collect()) Assert.Equal(0, df.Height);
        using (LazyFrame lf = LazyFrame.ScanCsv(path, raiseIfEmpty: false, dtypeOverride: types)) using (DataFrame df = lf.Collect()) Assert.Equal(0, df.Height);
        using (LazyFrame lf = LazyFrame.ScanCsv(ReadOnlyMemory<byte>.Empty, raiseIfEmpty: false, dtypeOverride: types))
        using (DataFrame df = lf.Collect())
            Assert.Equal(0, df.Height);
        Assert.Throws<Kaminari.Seseragi.Files.Csv.CsvFormatException>(() => LazyFrame.ScanCsv(path).Dispose());
    }

    [Fact]
    public void Memory_AndStreams_ReadAsFiles()
    {
        byte[] bytes = Encoding.UTF8.GetBytes(Mixed);
        using DataFrame engine = NativeCsv.Read(bytes, new CsvScanArgs());
        using (DataFrame fromBytes = DataFrame.ReadCsv(bytes)) Same(engine, fromBytes, "bytes");
        using (DataFrame fromMemory = DataFrame.ReadCsv(bytes.AsMemory())) Same(engine, fromMemory, "memory");
        using (DataFrame fromSpan = DataFrame.ReadCsv(bytes.AsSpan())) Same(engine, fromSpan, "span");
        using (DataFrame fromStream = DataFrame.ReadCsv(new MemoryStream(bytes))) Same(engine, fromStream, "stream");
        using (LazyFrame lf = LazyFrame.ScanCsv(bytes)) using (DataFrame scanned = lf.Collect()) Same(engine, scanned, "scanned bytes");
        using (LazyFrame lf = LazyFrame.ScanCsv(bytes.AsMemory())) using (DataFrame scanned = lf.Collect()) Same(engine, scanned, "scanned memory");
        using (var scannedStream = new LazyFrame(ManagedCsv.ScanCsv(new MemoryStream(bytes), new CsvScanArgs())).Collect()) Same(engine, scannedStream, "scanned stream");
    }

    /// <summary>Queries over a scan (projection, a filter, a row limit, an aggregate, the row index and the path) give the engine's results; a scan runs again.</summary>
    [Fact]
    public void Scans_GiveTheEnginesQueryResults()
    {
        Write("g/a.csv", Mixed);
        Write("g/b.csv", Mixed.Replace("alpha", "gamma"));
        string glob = At("g/*.csv");
        using LazyFrame engine = NativeCsv.Scan(glob, new CsvScanArgs { RowIndexName = "row", IncludeFilePaths = "file", InferSchemaFiles = 10 });
        using LazyFrame managed = LazyFrame.ScanCsv(glob, rowIndexName: "row", includeFilePaths: "file");
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
        string engine = Encoding.UTF8.GetString(NativeCsv.Write(df, new CsvWriteArgs()));
        Assert.Equal(engine, Encoding.UTF8.GetString(df.WriteCsvMemory()));
        using (NativeBuffer native = df.WriteCsvNativeMemory()) Assert.Equal(engine, Encoding.UTF8.GetString(native.ToArray()));
        using (LazyFrame lf = df.Lazy()) Assert.Equal(engine, Encoding.UTF8.GetString(lf.SinkCsvMemory()));
        string file = At("out/sunk.csv");
        using (LazyFrame lf = df.Lazy()) lf.SinkCsv(file, mkdir: true);
        Assert.Equal(engine, File.ReadAllText(file));
        string written = At("out/written.csv");
        df.WriteCsv(written);
        Assert.Equal(engine, File.ReadAllText(written));
        using (var stream = new MemoryStream())
        {
            df.WriteCsv(stream);
            Assert.Equal(engine, Encoding.UTF8.GetString(stream.ToArray()));
        }
        var semicolons = new CsvWriteArgs { Separator = ';', QuoteStyle = PlQuoteStyle.Always, NullValue = "NULL", IncludeHeader = false };
        Assert.Equal(Encoding.UTF8.GetString(NativeCsv.Write(df, semicolons)),
            Encoding.UTF8.GetString(df.WriteCsvMemory(separator: (byte)';', quoteStyle: QuoteStyle.Always, nullValue: "NULL", includeHeader: false)));
        // A query without rows: its header alone.
        using DataFrame none = df.Head(0);
        using (LazyFrame lf = none.Lazy()) Assert.Equal("id,name,price,when,day,flag\n", Encoding.UTF8.GetString(lf.SinkCsvMemory()));
    }

    [Fact]
    public void APartitionedSink_ReadsBack()
    {
        using DataFrame df = DataFrame.ReadCsv(Write("p.csv", Mixed));
        string root = At("parts");
        using (LazyFrame lf = df.Lazy()) lf.SinkCsvPartitioned(root, global::Polars.CSharp.Polars.Selectors.ByName("flag"));
        Assert.All(Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories), f => Assert.StartsWith("part-", Path.GetFileName(f)));
        using DataFrame back = DataFrame.ReadCsv(Path.Combine(root, "**", "*.csv"));
        using DataFrame expected = df.Sort("id"), actual = back.Sort("id");
        output.WriteLine(actual.ToString());
        Same(expected.Select("id", "name", "flag"), actual.Select("id", "name", "flag"), "partitions");
    }
}
