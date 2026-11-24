using System.Text;
using Kaminari.Seseragi.Files;
using Polars.NET.Core;
using Polars.NET.Core.Csv;
using Xunit.Abstractions;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// The public CSV API on the managed reader and writer (<see cref="ManagedCsv"/>). Native CSV IO was compared with it
/// before it was removed (C5b: these tests ran against the engine, and 45 corpus files read the same); what the engine
/// gave is pinned here (the frame of <see cref="Mixed"/>, its written bytes, option and line-end reads), and every
/// overload — path, bytes, span, memory, stream, scan, rechunked or not — must give the same frame.
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
            Assert.Fail($"{what}\nexpected:\n{expected}\nactual:\n{actual}");
        }
    }

    /// <summary>The engine's frame of <see cref="Mixed"/> (pinned from native Polars 0.55 before its CSV IO was removed).</summary>
    private const string MixedSchema = "Schema: {id: Int64, name: String, price: Float64, when: Datetime, day: Date, flag: Boolean}";

    /// <summary>The engine's bytes of <see cref="Mixed"/> read and written with the defaults (pinned from native Polars 0.55).</summary>
    private const string MixedWritten = """
        id,name,price,when,day,flag
        1,alpha,1.5,2024-01-02T03:04:05.000000,2024-01-02,true
        2,"b,eta",,2024-02-03T00:00:00.000000,2024-02-03,false
        3,"quo""te",-0.25,2024-03-04T05:06:07.123456,2024-03-04,
        4,,1000.0,,2024-04-05,true

        """;

    /// <summary>A file, its bytes, a span, memory and a stream, read and scanned, rechunked or not: one frame.</summary>
    [Theory]
    [MemberData(nameof(Inputs))]
    public void EveryOverload_ReadsTheSameFrame(string name, string text)
    {
        string path = Write(name + ".csv", text);
        string[] nulls = name == "nulls" ? ["NA"] : null;
        byte[] bytes = Encoding.UTF8.GetBytes(text);
        using DataFrame expected = DataFrame.ReadCsv(path, nullValues: nulls);
        using (DataFrame df = DataFrame.ReadCsv(path, rechunk: true, nullValues: nulls)) Same(expected, df, $"{name} rechunked");
        using (DataFrame df = DataFrame.ReadCsv(bytes, nullValues: nulls)) Same(expected, df, $"{name} bytes");
        using (DataFrame df = DataFrame.ReadCsv(bytes.AsSpan(), nullValues: nulls)) Same(expected, df, $"{name} span");
        using (DataFrame df = DataFrame.ReadCsv(bytes.AsMemory(), nullValues: nulls)) Same(expected, df, $"{name} memory");
        using (DataFrame df = DataFrame.ReadCsv(new MemoryStream(bytes), nullValues: nulls)) Same(expected, df, $"{name} stream");
        using (LazyFrame lf = LazyFrame.ScanCsv(path, nullValues: nulls)) using (DataFrame df = lf.Collect()) Same(expected, df, $"{name} scanned");
        using (LazyFrame lf = LazyFrame.ScanCsv(bytes, nullValues: nulls)) using (DataFrame df = lf.Collect()) Same(expected, df, $"{name} scanned bytes");
        using (LazyFrame lf = LazyFrame.ScanCsv(bytes.AsMemory(), nullValues: nulls)) using (DataFrame df = lf.Collect()) Same(expected, df, $"{name} scanned memory");
        using (var lf = new LazyFrame(ManagedCsv.ScanCsv(new MemoryStream(bytes), new CsvScanArgs { NullValues = nulls })))
        using (DataFrame df = lf.Collect())
            Same(expected, df, $"{name} scanned stream");
    }

    /// <summary>Mixed types read as the engine read them: its schema, and its bytes written back.</summary>
    [Fact]
    public void Mixed_ReadsAsTheEngineReadIt()
    {
        using DataFrame df = DataFrame.ReadCsv(Write("mixed.csv", Mixed));
        Assert.Equal(MixedSchema, df.Schema.ToString());
        Assert.Equal(MixedWritten, Encoding.UTF8.GetString(df.WriteCsvMemory()));
    }

    /// <summary>Separator, comments, skips, decimal comma, row index from an offset and a row limit, as the engine read them.</summary>
    [Fact]
    public void Options_ReadAsTheEngineReadThem()
    {
        string path = Write("opts.csv", "# comment\nskip me\nid;name;v\n1;a;1,5\n2;b;2,25\n3;c;\n");
        using DataFrame df = DataFrame.ReadCsv(path, separator: ';', commentPrefix: "#", skipRows: 1, decimalComma: true, rowIndexName: "row",
            rowIndexOffset: 10, nRows: 2);
        Assert.Equal(["row", "id", "name", "v"], df.Columns);
        Assert.Equal(DataType.UInt32, df["row"].DataType);
        Assert.Equal([10u, 11u], df["row"].ToArray<uint>());
        Assert.Equal([1L, 2L], df["id"].ToArray<long>());
        Assert.Equal(["a", "b"], df["name"].ToArray<string>());
        Assert.Equal([1.5, 2.25], df["v"].ToArray<double>());
        // Columns asked in another order than the file's: the frame takes theirs.
        using DataFrame columns = DataFrame.ReadCsv(Write("mixed.csv", Mixed), columns: ["price", "id"]);
        Assert.Equal(["price", "id"], columns.Columns);
        Assert.Equal([1L, 2L, 3L, 4L], columns["id"].ToArray<long>());
    }

    /// <summary>Records ended by another character than '\n' read as the engine read them.</summary>
    [Fact]
    public void AnotherLineEnd_ReadsAsTheEngineReadIt()
    {
        using DataFrame df = DataFrame.ReadCsv(Write("eol.csv", "a,b|1,x|2,\"y|z\"|3,w|"), eolChar: '|');
        Assert.Equal([1L, 2L, 3L], df["a"].ToArray<long>());
        Assert.Equal(["x", "y|z", "w"], df["b"].ToArray<string>());
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

    /// <summary>
    /// Queries over a scan of files (projection, a filter, a row limit, an aggregate, the row index and the path) give
    /// what the same queries give over the frame read eagerly; a scan runs again.
    /// </summary>
    [Fact]
    public void Scans_GiveTheEagerFramesQueryResults()
    {
        Write("g/a.csv", Mixed);
        Write("g/b.csv", Mixed.Replace("alpha", "gamma"));
        string glob = At("g/*.csv");
        using DataFrame read = DataFrame.ReadCsv(glob, rowIndexName: "row");
        using LazyFrame scan = LazyFrame.ScanCsv(glob, rowIndexName: "row");
        Func<LazyFrame, LazyFrame>[] queries =
        [
            q => q,
            q => q.Select("name", "id"),
            q => q.Filter(Col("price") > 0).Select("id", "row"),
            q => q.Head(3),
            q => q.GroupBy("flag").Agg(Col("price").Sum().Alias("s"), Col("id").Count().Alias("n")).Sort("flag"),
        ];
        for (int i = 0; i < queries.Length; i++)
            for (int run = 0; run < 2; run++)
            {
                using LazyFrame eager = read.Lazy();
                using DataFrame a = queries[i](eager).Collect(), b = queries[i](scan.Clone()).Collect();
                Same(a, b, $"query {i} run {run}");
            }
        // Each row's file.
        using LazyFrame withPaths = LazyFrame.ScanCsv(glob, includeFilePaths: "file");
        using DataFrame paths = withPaths.Select("file").Collect();
        Assert.Equal(8, paths.Height);
        Assert.Equal(2, paths["file"].ToArray<string>().Distinct().Count());
    }

    /// <summary>A frame written, a query sunk (to a file, a stream, memory), as the engine wrote them.</summary>
    [Fact]
    public void Writes_AndSinks_AsTheEngineWroteThem()
    {
        using DataFrame df = DataFrame.ReadCsv(Write("w.csv", Mixed));
        Assert.Equal(MixedWritten, Encoding.UTF8.GetString(df.WriteCsvMemory()));
        using (NativeBuffer native = df.WriteCsvNativeMemory()) Assert.Equal(MixedWritten, Encoding.UTF8.GetString(native.ToArray()));
        using (LazyFrame lf = df.Lazy()) Assert.Equal(MixedWritten, Encoding.UTF8.GetString(lf.SinkCsvMemory()));
        string file = At("out/sunk.csv");
        using (LazyFrame lf = df.Lazy()) lf.SinkCsv(file, mkdir: true);
        Assert.Equal(MixedWritten, File.ReadAllText(file));
        string written = At("out/written.csv");
        df.WriteCsv(written);
        Assert.Equal(MixedWritten, File.ReadAllText(written));
        using (var stream = new MemoryStream())
        {
            df.WriteCsv(stream);
            Assert.Equal(MixedWritten, Encoding.UTF8.GetString(stream.ToArray()));
        }
        // Pinned from native Polars 0.55 too.
        const string semicolons = """
            "1";"alpha";"1.5";"2024-01-02T03:04:05.000000";"2024-01-02";"true"
            "2";"b,eta";"NULL";"2024-02-03T00:00:00.000000";"2024-02-03";"false"
            "3";"quo""te";"-0.25";"2024-03-04T05:06:07.123456";"2024-03-04";"NULL"
            "4";"NULL";"1000.0";"NULL";"2024-04-05";"true"

            """;
        Assert.Equal(semicolons, Encoding.UTF8.GetString(df.WriteCsvMemory(separator: (byte)';', quoteStyle: QuoteStyle.Always, nullValue: "NULL", includeHeader: false)));
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
