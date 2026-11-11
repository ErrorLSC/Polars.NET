using System.Data;
using Apache.Arrow;
using Kaminari.Sazanami.Data;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// C# stream sources (ScanDatabase, ScanEnumerable, ScanRecordBatches) on the streaming engine: sinks spool the
/// stream to a temporary IPC file, streaming collects read it into memory first (#38).
/// </summary>
public class AnonymousScanStreamingTests
{
    private const int Rows = 1000;
    private const int BatchSize = 100;

    public sealed class Row
    {
        public int Id { get; set; }
        public string G { get; set; } = "";
        public double? V { get; set; }
    }

    private static IEnumerable<Row> SourceRows() =>
        Enumerable.Range(0, Rows).Select(i => new Row { Id = i, G = i % 2 == 0 ? "a" : "b", V = i % 7 == 0 ? null : i * 0.5 });

    private static DataTable Table()
    {
        var t = new DataTable();
        t.Columns.Add("Id", typeof(int));
        t.Columns.Add("G", typeof(string));
        t.Columns.Add("V", typeof(double));
        foreach (var r in SourceRows()) t.Rows.Add(r.Id, r.G, r.V.HasValue ? r.V.Value : DBNull.Value);
        return t;
    }

    public static TheoryData<string> Sources => ["factory", "reader", "enumerable", "batches"];

    /// <summary>A frame over a fresh source; "reader" and "batches" can be read only once.</summary>
    private static LazyFrame Source(string kind)
    {
        switch (kind)
        {
            case "factory":
                var table = Table();
                return LazyFrame.ScanDatabase(table.CreateDataReader, BatchSize);
            case "reader":
                return LazyFrame.ScanDatabase(Table().CreateDataReader(), BatchSize);
            case "enumerable":
                return LazyFrame.ScanEnumerable(SourceRows(), batchSize: BatchSize);
            case "batches":
                var reader = Table().CreateDataReader();
                Schema schema = reader.GetArrowSchema();
                return LazyFrame.ScanRecordBatches(reader.ToArrowBatches(BatchSize), schema);
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static LazyFrame Query(LazyFrame lf) => lf.Filter(Pl.Col("Id") % 3 != 0);

    private static readonly int[] ExpectedIds = [.. Enumerable.Range(0, Rows).Where(i => i % 3 != 0)];

    private static void AssertIds(DataFrame df)
    {
        Assert.Equal((long)ExpectedIds.Length, df.Height);
        using var sorted = df.Sort("Id");
        var ids = Enumerable.Range(0, (int)sorted.Height).Select(i => Convert.ToInt32(sorted[i, "Id"])).ToArray();
        Assert.Equal(ExpectedIds, ids);
    }

    private static string TempDir() => Directory.CreateTempSubdirectory("anon-scan-").FullName;

    [Theory]
    [MemberData(nameof(Sources))]
    public void SinkParquet_Streams(string kind)
    {
        var dir = TempDir();
        try
        {
            var path = Path.Combine(dir, "out.parquet");
            using (var lf = Source(kind)) Query(lf).SinkParquet(path);
            using var back = DataFrame.ReadParquet(path);
            AssertIds(back);
            Assert.Equal(["Id", "G", "V"], back.Columns);
        }
        finally { Directory.Delete(dir, true); }
    }

    [Theory]
    [MemberData(nameof(Sources))]
    public void SinkCsv_Ipc_NdJson_Stream(string kind)
    {
        var dir = TempDir();
        try
        {
            string csv = Path.Combine(dir, "out.csv"), ipc = Path.Combine(dir, "out.arrow"), nd = Path.Combine(dir, "out.ndjson");
            using (var lf = Source(kind)) Query(lf).SinkCsv(csv);
            using (var lf = Source(kind)) Query(lf).SinkIpc(ipc);
            using (var lf = Source(kind)) Query(lf).SinkNdJson(nd);

            using (var back = LazyFrame.ScanCsv(csv).Collect()) AssertIds(back);
            using (var back = LazyFrame.ScanIpc(ipc).Collect()) AssertIds(back);
            using (var back = LazyFrame.ScanNdjson(nd).Collect()) AssertIds(back);
        }
        finally { Directory.Delete(dir, true); }
    }

    [Theory]
    [MemberData(nameof(Sources))]
    public void SinkParquetPartitioned_Streams(string kind)
    {
        var dir = TempDir();
        try
        {
            using (var lf = Source(kind)) Query(lf).SinkParquetPartitioned(dir, Pl.Col("G"));
            using var back = LazyFrame.ScanParquet(Path.Combine(dir, "**", "*.parquet")).Collect();
            AssertIds(back);
        }
        finally { Directory.Delete(dir, true); }
    }

    [Theory]
    [MemberData(nameof(Sources))]
    public void Collect_Streaming_Engine(string kind)
    {
        using var lf = Source(kind);
        using var df = Query(lf).Collect(Engine.Streaming);
        AssertIds(df);
    }

    [Theory]
    [MemberData(nameof(Sources))]
    public async Task CollectBatchesAsync_Streams(string kind)
    {
        using var lf = Source(kind);
        var ids = new List<int>();
        await foreach (var batch in Query(lf).CollectBatchesAsync(BatchSize, Engine.Streaming))
        {
            using (batch)
                for (int i = 0; i < batch.Height; i++) ids.Add(Convert.ToInt32(batch[i, "Id"]));
        }
        Assert.Equal(ExpectedIds, ids.Order());
    }

    [Theory]
    [MemberData(nameof(Sources))]
    public void CollectAll_Streaming_Reads_A_Shared_Source_Once(string kind)
    {
        // Both plans scan the same source: a single-use reader must be read once for both.
        using var lf = Source(kind);
        using var a = Query(lf);
        using var b = lf.Filter(Pl.Col("Id") < 10);
        var dfs = Pl.CollectAll([a, b], Engine.Streaming);
        try
        {
            AssertIds(dfs[0]);
            Assert.Equal(10, dfs[1].Height);
        }
        finally { foreach (var df in dfs) df.Dispose(); }
    }

    [Theory]
    [MemberData(nameof(Sources))]
    public void SelfJoin_Sink_Reads_The_Source_Once(string kind)
    {
        var dir = TempDir();
        try
        {
            var path = Path.Combine(dir, "joined.parquet");
            using (var lf = Source(kind))
                Query(lf).Join(lf.Select(Pl.Col("Id"), Pl.Col("G").Alias("G2")), "Id").SinkParquet(path);
            using var back = DataFrame.ReadParquet(path);
            AssertIds(back);
        }
        finally { Directory.Delete(dir, true); }
    }

    [Fact]
    public void Sink_After_Reading_The_Schema()
    {
        // Reading the schema caches the converted plan inside the frame; the sink must not run that stale plan.
        var dir = TempDir();
        try
        {
            var path = Path.Combine(dir, "out.csv");
            using var lf = Source("reader");
            using (var schema = lf.Schema) Assert.Equal(3, schema.Count);
            Query(lf).SinkCsv(path);
            using var back = LazyFrame.ScanCsv(path).Collect();
            AssertIds(back);
        }
        finally { Directory.Delete(dir, true); }
    }

    [Fact]
    public void Sql_Subquery_Over_A_Source_Sinks()
    {
        // The IN subquery is a plan inside an expression.
        var dir = TempDir();
        try
        {
            var path = Path.Combine(dir, "sql.csv");
            using var lf = Source("factory");
            using var ctx = new SqlContext();
            ctx.Register("t", lf);
            using var q = ctx.Execute("SELECT * FROM t WHERE Id IN (SELECT Id FROM t WHERE Id < 10) AND Id % 3 <> 0");
            q.SinkCsv(path);
            using var back = LazyFrame.ScanCsv(path).Collect();
            Assert.Equal(6, back.Height);
        }
        finally { Directory.Delete(dir, true); }
    }

    [Fact]
    public void Empty_Source_Sinks_A_Schema_Only_File()
    {
        var dir = TempDir();
        try
        {
            var path = Path.Combine(dir, "empty.parquet");
            var table = Table();
            table.Rows.Clear();
            using (var lf = LazyFrame.ScanDatabase(table.CreateDataReader, BatchSize)) lf.SinkParquet(path);
            using var back = DataFrame.ReadParquet(path);
            Assert.Equal(0, back.Height);
            Assert.Equal(["Id", "G", "V"], back.Columns);
        }
        finally { Directory.Delete(dir, true); }
    }

    [Fact]
    public void ScanDatabaseBuffered_Reads_Now_And_Replays()
    {
        LazyFrame lf;
        using (var reader = Table().CreateDataReader())
            lf = LazyFrame.ScanDatabaseBuffered(reader, BatchSize);
        // The reader is gone; the buffered file serves every execution, streamed or not.
        using (lf)
        {
            using (var df = Query(lf).Collect()) AssertIds(df);
            using (var df = Query(lf).Collect(Engine.Streaming)) AssertIds(df);
            var dir = TempDir();
            try
            {
                var path = Path.Combine(dir, "buffered.parquet");
                Query(lf).SinkParquet(path);
                using var back = DataFrame.ReadParquet(path);
                AssertIds(back);
            }
            finally { Directory.Delete(dir, true); }
        }
    }

    [Fact]
    public void ScanEnumerable_Buffered_Keeps_Types_And_Rows()
    {
        using var lf = LazyFrame.ScanEnumerable(SourceRows(), batchSize: BatchSize, useBuffered: true);
        using var buffered = lf.Collect();
        using var streamed = LazyFrame.ScanEnumerable(SourceRows(), batchSize: BatchSize).Collect();
        Assert.True(buffered.Equals(streamed));
    }

    [Fact]
    public void ScanEnumerable_Buffered_Empty()
    {
        using var lf = LazyFrame.ScanEnumerable(System.Array.Empty<Row>(), useBuffered: true);
        using var df = lf.Collect();
        Assert.Equal(0, df.Height);
        Assert.Equal(["Id", "G", "V"], df.Columns);
    }

    public sealed class Wide
    {
        public int Id { get; set; }
        public decimal Price { get; set; }
        public DateTime At { get; set; }
        public DateOnly Day { get; set; }
        public TimeOnly Clock { get; set; }
        public TimeSpan Span { get; set; }
        public Guid Key { get; set; }
        public byte[] Blob { get; set; } = [];
        public bool Flag { get; set; }
        public long? Maybe { get; set; }
        public string? Text { get; set; }
        public List<int> Items { get; set; } = [];
    }

    private static IEnumerable<Wide> WideRows() => Enumerable.Range(0, 250).Select(i => new Wide
    {
        Id = i,
        Price = 1.25m * i,
        At = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMinutes(i),
        Day = new DateOnly(2026, 1, 1).AddDays(i),
        Clock = new TimeOnly(0, 0).Add(TimeSpan.FromSeconds(i)),
        Span = TimeSpan.FromMilliseconds(i),
        Key = new Guid(i, 0, 0, new byte[8]),
        Blob = [(byte)i, 1, 2],
        Flag = i % 2 == 0,
        Maybe = i % 5 == 0 ? null : i,
        Text = i % 7 == 0 ? null : $"t{i}",
        Items = [.. Enumerable.Range(0, i % 4)],
    });

    private static void AssertSameFrame(DataFrame expected, DataFrame actual)
    {
        Assert.Equal(expected.Schema.ToString(), actual.Schema.ToString());
        Assert.True(expected.Equals(actual), $"expected:\n{expected}\nactual:\n{actual}");
    }

    [Fact]
    public async Task Spool_Materialize_And_Buffered_Keep_Every_Type()
    {
        // The in-memory engine runs the C# stream itself: the reference for every other path.
        using var oracle = LazyFrame.ScanEnumerable(WideRows(), batchSize: 64).Collect(Engine.InMemory);

        using (var lf = LazyFrame.ScanEnumerable(WideRows(), batchSize: 64))
        using (var materialized = lf.Collect(Engine.Streaming))
            AssertSameFrame(oracle, materialized);

        var batches = new List<DataFrame>();
        using (var lf = LazyFrame.ScanEnumerable(WideRows(), batchSize: 64))
            await foreach (var b in lf.CollectBatchesAsync(64, Engine.Streaming, maintainOrder: true)) batches.Add(b);
        using (var spooled = DataFrame.Concat(batches))
            AssertSameFrame(oracle, spooled);
        foreach (var b in batches) b.Dispose();

        using (var lf = LazyFrame.ScanEnumerable(WideRows(), batchSize: 64, useBuffered: true))
        using (var buffered = lf.Collect())
            AssertSameFrame(oracle, buffered);
    }

    [Fact]
    public void Spool_Keeps_Dictionary_Columns_Across_Batches()
    {
        // Two batches whose dictionaries differ: the spool converts each batch as the stream import does.
        var dictType = new Apache.Arrow.Types.DictionaryType(Apache.Arrow.Types.Int32Type.Default, Apache.Arrow.Types.StringType.Default, false);
        var schema = new Schema.Builder().Field(f => f.Name("Cat").DataType(dictType).Nullable(true)).Build();
        RecordBatch Batch(string[] dict, int[] idx)
        {
            var values = new StringArray.Builder().AppendRange(dict).Build();
            var indices = new Int32Array.Builder().AppendRange(idx).Build();
            return new RecordBatch(schema, [new DictionaryArray(dictType, indices, values)], idx.Length);
        }
        IEnumerable<RecordBatch> Stream() { yield return Batch(["x", "y"], [0, 1, 0]); yield return Batch(["z", "x"], [0, 1]); }

        using var oracle = LazyFrame.ScanRecordBatches(Stream(), schema).Collect(Engine.InMemory);
        var dir = TempDir();
        try
        {
            string parquet = Path.Combine(dir, "cat.parquet"), ipc = Path.Combine(dir, "cat.arrow");
            LazyFrame.ScanRecordBatches(Stream(), schema).SinkParquet(parquet);
            LazyFrame.ScanRecordBatches(Stream(), schema).SinkIpc(ipc);
            DataFrame[] backs = [DataFrame.ReadParquet(parquet), DataFrame.ReadIpc(ipc)];
            foreach (var back in backs)
            {
                using (back)
                {
                    Assert.Equal(["x", "y", "x", "z", "x"], Enumerable.Range(0, 5).Select(i => back[i, "Cat"]?.ToString()));
                    Assert.Equal(oracle.Schema.ToString(), back.Schema.ToString());
                }
            }
        }
        finally { Directory.Delete(dir, true); }
    }

    [Theory]
    [MemberData(nameof(Sources))]
    public void SinkBatches_Streams_In_Order(string kind)
    {
        var ids = new List<int>();
        using (var lf = Source(kind))
            Query(lf).SinkBatches(batch =>
            {
                using (batch)
                {
                    var column = (Int32Array)batch.Column("Id");
                    for (int i = 0; i < column.Length; i++) ids.Add(column.GetValue(i)!.Value);
                }
            });
        Assert.Equal(ExpectedIds, ids);
    }

    [Fact]
    public void Spool_Leaves_No_Temp_Files()
    {
        // Polars's temporary directory (polars-io path_utils). On Unix the spool file is unlinked at once; on Windows
        // it is removed when its last handle closes.
        var tempRoot = Environment.GetEnvironmentVariable("POLARS_TEMP_DIR")
                       ?? (OperatingSystem.IsWindows()
                           ? Path.Combine(Path.GetTempPath(), "polars")
                           : Path.Combine(Path.GetTempPath(), $"polars-{Environment.UserName}"));
        int Count() => Directory.Exists(tempRoot) ? Directory.GetFiles(tempRoot, "polars-net-spool-*").Length : 0;

        var dir = TempDir();
        try
        {
            using (var lf = Source("factory")) Query(lf).SinkParquet(Path.Combine(dir, "out.parquet"));
            Assert.Equal(0, Count());
        }
        finally { Directory.Delete(dir, true); }
    }
}
