using System.Runtime.CompilerServices;
using System.Text.Json;
using Kaminari.Seseragi.Files.Parquet;
using Polars.CSharp;
using Polars.NET.Core;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Wrappers;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Arrow.TypeSystem;
using static Polars.CSharp.Polars;
using ArrowField = Apache.Arrow.Field;
using ArrowTypes = Apache.Arrow.Types;
using DeltaSchema = Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Polars.Config.Tests;

/// <summary>
/// Delta data files written by the managed Parquet writer (<see cref="ManagedSink"/>, chosen by
/// <c>POLARS_NET_DELTA_SINK</c>, process-wide: hence here, in the serial project) against the engine's own sink: the
/// same rows come back, the log records the same statistics, and the files have the same Parquet schema, but for the
/// root's name (ours <c>schema</c>, the engine's <c>root</c>: readers ignore it) and the legacy converted type ours adds
/// to UTC timestamps (TIMESTAMP_MICROS, as Spark writes it). Files split by size on disk and by rows; a failure on the
/// way leaves no file and no commit. Partitioned writes (partition values of every type, Hive-escaped directories, an
/// empty string as null), partitioned change data and clustering's buckets alike; rows grouped by their keys already;
/// files closed early to hold the memory budget; a failure with many files open leaves none.
/// </summary>
public sealed class DeltaManagedSinkTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), $"managed_sink_{Guid.NewGuid():N}");

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(ManagedSink.Variable, null);
        if (Directory.Exists(_dir)) Directory.Delete(_dir, true);
    }

    private string Table(string name) => Path.Combine(_dir, name);

    /// <summary>Runs <paramref name="write"/> with the given sink chosen.</summary>
    private static void Sink(string sink, Action write)
    {
        Environment.SetEnvironmentVariable(ManagedSink.Variable, sink);
        try
        {
            write();
        }
        finally
        {
            Environment.SetEnvironmentVariable(ManagedSink.Variable, null);
        }
    }

    private static string[] DataFiles(string table, string under = "") =>
        [.. Directory.GetFiles(Path.Combine(table, under), "*.parquet", SearchOption.AllDirectories)
            .Where(f => !f.Contains("_delta_log") && (under.Length > 0 || !f.Contains("_change_data"))).Order()];

    private static string[] AllFiles(string table) => [.. Directory.GetFiles(table, "*", SearchOption.AllDirectories).Order()];

    /// <summary>The adds of every commit, in commit order.</summary>
    private static List<JsonElement> Adds(string table)
    {
        List<JsonElement> adds = [];
        foreach (string commit in Directory.GetFiles(Path.Combine(table, "_delta_log"), "*.json").Order())
        {
            foreach (string line in File.ReadLines(commit))
            {
                using JsonDocument action = JsonDocument.Parse(line);
                if (action.RootElement.TryGetProperty("add", out JsonElement add)) adds.Add(add.Clone());
            }
        }
        return adds;
    }

    /// <summary>The schema elements of a data file, one line each, the known differences taken out.</summary>
    private static string[] SchemaOf(string file, bool names = true)
    {
        using ParquetFile parquet = ParquetFile.Open(file);
        return [.. parquet.Metadata.Thrift!.Schema.Skip(1).Select(e =>
            $"{(names ? Stable(e.Name) : "")} {e.Type?.ToString() ?? "group"} {e.Repetition} children={e.NumChildren} logical={e.LogicalType?.Kind} " +
            $"converted={(e.ConvertedType is Kaminari.Seseragi.Files.Parquet.Metadata.ConvertedType.TimestampMicros or Kaminari.Seseragi.Files.Parquet.Metadata.ConvertedType.TimestampMillis ? null : e.ConvertedType)} " +
            $"id={e.FieldId} len={e.TypeLength} p={e.Precision} s={e.Scale}")];
    }

    /// <summary>A column name with the random part of the materialized row tracking columns' names cut (each table draws its own).</summary>
    private static string Stable(string name) =>
        name.StartsWith("_row-id-col-", StringComparison.Ordinal) ? "_row-id-col-*"
        : name.StartsWith("_row-commit-version-col-", StringComparison.Ordinal) ? "_row-commit-version-col-*"
        : name;

    private static string AddedFile(string table, JsonElement add) => Path.Combine(table, Uri.UnescapeDataString(add.GetProperty("path").GetString()!));

    private static string CreatedBy(string file)
    {
        using ParquetFile parquet = ParquetFile.Open(file);
        return parquet.Metadata.Thrift!.CreatedBy ?? "";
    }

    private static DataFrame Typed(int rows)
    {
        using DataFrame typed = DataFrame.From<Row>([.. Enumerable.Range(0, rows).Select(Row.Of)]);
        return typed.Lazy().WithColumns(
            Col("Money").Cast(DataType.Decimal(9, 2)).Alias("D9"),
            Col("Money").Cast(DataType.Decimal(18, 4)).Alias("D18"),
            Col("At").Cast(DataType.Datetime(TimeUnit.Microseconds)).Alias("Ntz"),
            When(Col("Id") == 7).Then(double.NaN).Otherwise(Col("Double")).Alias("WithNaN")).Collect();
    }

    /// <summary>The same write through both sinks: the rows, the statistics of each file and the files' schemas agree.</summary>
    private void SameAsTheEnginesSink(Action<string> write, string orderBy = "Id")
    {
        string native = Table("native"), managed = Table("managed");
        Sink("native", () => write(native));
        Sink("managed", () => write(managed));

        using (DataFrame a = DataFrame.ReadDelta(native).Sort(orderBy), b = DataFrame.ReadDelta(managed).Sort(orderBy))
            Assert.True(a.Equals(b), "the tables hold other rows");
        List<JsonElement> nativeAdds = Adds(native), managedAdds = Adds(managed);
        Assert.Equal(nativeAdds.Count, managedAdds.Count);
        for (int i = 0; i < nativeAdds.Count; i++)
            Assert.Equal(nativeAdds[i].GetProperty("stats").GetString(), managedAdds[i].GetProperty("stats").GetString());
        // File by file, in the log's order (their names are random).
        for (int i = 0; i < nativeAdds.Count; i++)
            Assert.Equal(SchemaOf(AddedFile(native, nativeAdds[i])), SchemaOf(AddedFile(managed, managedAdds[i])));
        string[] nativeFiles = DataFiles(native), managedFiles = DataFiles(managed);
        Assert.All(managedFiles, f => Assert.StartsWith("seseragi", CreatedBy(f)));
        Assert.All(nativeFiles, f => Assert.DoesNotContain("seseragi", CreatedBy(f)));
    }

    [Fact]
    public void EveryType_WritesWhatTheEnginesSinkWrites()
    {
        using DataFrame rows = Typed(3000);
        SameAsTheEnginesSink(table => rows.WriteDelta(table));

        // The files' own bounds are whole (the table's statistics cut strings their own way): a reader pruning row
        // groups on them sees the real 80-character maximum.
        using ParquetFile file = ParquetFile.Open(DataFiles(Table("managed"))[0]);
        int text = System.Array.FindIndex(file.Metadata.Thrift!.RowGroups[0].Columns, c => c.MetaData!.PathInSchema.SequenceEqual(["Text"]));
        var statistics = file.Metadata.Thrift.RowGroups[0].Columns[text].MetaData!.Statistics!;
        Assert.True(statistics.MaxValue!.Length > 64);
        Assert.True(statistics.IsMaxValueExact);
    }

    [Fact]
    public void ColumnMapping_FieldIdsAreTheTables_AtAnyDepth()
    {
        using DataFrame rows = Typed(500);
        using PolarsSchema schema = rows.Schema;
        string table = Table("mapped");
        Delta.CreateTable(table, schema, properties: new() { ["delta.columnMapping.mode"] = "id" });
        Sink("managed", () => rows.WriteDelta(table, mode: DeltaSaveMode.Append));

        // The ids of the log's schema by physical name (struct fields inside too), against the file's.
        using JsonDocument metadata = JsonDocument.Parse(File.ReadLines(Directory.GetFiles(Path.Combine(table, "_delta_log"), "*.json").Order().First())
            .First(l => l.Contains("\"metaData\"")));
        Dictionary<string, int> ids = [];
        void Collect(JsonElement fields)
        {
            foreach (JsonElement field in fields.EnumerateArray())
            {
                JsonElement meta = field.GetProperty("metadata");
                ids[meta.GetProperty("delta.columnMapping.physicalName").GetString()!] = meta.GetProperty("delta.columnMapping.id").GetInt32();
                if (field.GetProperty("type") is { ValueKind: JsonValueKind.Object } type && type.GetProperty("type").GetString() == "struct")
                    Collect(type.GetProperty("fields"));
            }
        }
        using (JsonDocument tableSchema = JsonDocument.Parse(metadata.RootElement.GetProperty("metaData").GetProperty("schemaString").GetString()!))
            Collect(tableSchema.RootElement.GetProperty("fields"));

        using ParquetFile file = ParquetFile.Open(DataFiles(table)[0]);
        var named = file.Metadata.Thrift!.Schema.Where(e => e.Name.StartsWith("col-", StringComparison.Ordinal)).ToList();
        Assert.Equal(ids.Count, named.Count);
        Assert.All(named, e => Assert.Equal(ids[e.Name], e.FieldId));
        using DataFrame read = DataFrame.ReadDelta(table).Sort("Id");
        Assert.True(read.Equals(rows), "the table holds other rows");
    }

    [Fact]
    public void RowTrackingAndChangeData_OfAnUpdate_AreWhatTheEnginesSinkWrites()
    {
        using DataFrame rows = Typed(2000);
        Dictionary<string, string> properties = new() { [DeltaTableProperties.EnableRowTracking] = "true", [DeltaTableProperties.EnableChangeDataFeed] = "true" };
        SameAsTheEnginesSink(table =>
        {
            using PolarsSchema schema = rows.Schema;
            Delta.CreateTable(table, schema, properties: properties);
            rows.WriteDelta(table, mode: DeltaSaveMode.Append);
            Delta.Update(table, Col("Id") < 300, new Dictionary<string, Expr> { ["Int"] = Col("Int") + 1 });
        });
        // The change data files hold the same rows.
        // (The materialized row tracking columns' names are each table's own: renamed alike.)
        DataFrame ChangeData(string table)
        {
            using DataFrame read = DataFrame.ReadParquet(Path.Combine(Table(table), "_change_data", "*.parquet"));
            using DataFrame renamed = read.Rename([.. read.Columns], [.. read.Columns.Select(Stable)]);
            return renamed.Sort([(IntoSelector)"Id", "_change_type"]);
        }
        using DataFrame native = ChangeData("native"), managed = ChangeData("managed");
        Assert.Equal(600, managed.Height);
        Assert.True(native.Equals(managed), "the change data differ");
        // The rewritten file carries the materialized row ids: the same ids come back.
        using DataFrame a = LazyFrame.ScanDelta(Table("native"), rowIdColumn: "row_id").Collect().Sort("Id");
        using DataFrame b = LazyFrame.ScanDelta(Table("managed"), rowIdColumn: "row_id").Collect().Sort("Id");
        Assert.True(a.Equals(b), "the row ids differ");
    }

    [Fact]
    public void Files_SplitBySizeOnDisk_AndByRows()
    {
        using DataFrame rows = Typed(200_000);
        const long Target = 1 << 20;
        string bySize = Table("by_size");
        // Pages small against the files: a file's size is known to within its open pages (taken at their plain size).
        Sink("managed", () => rows.WriteDelta(bySize, approxBytesPerFile: Target, dataPageSize: 32 << 10));
        long[] sizes = [.. DataFiles(bySize).Select(f => new FileInfo(f).Length)];
        Assert.True(sizes.Length >= 10, $"{sizes.Length} files");
        Assert.All(Adds(bySize).Select(a => a.GetProperty("size").GetInt64()).SkipLast(1), size => Assert.InRange(size, Target * 85 / 100, Target * 115 / 100));
        Assert.Equal(sizes.Order(), Adds(bySize).Select(a => a.GetProperty("size").GetInt64()).Order());

        string byRows = Table("by_rows");
        Sink("managed", () => rows.WriteDelta(byRows, maxRowsPerFile: 30_000));
        long[] counts = [.. Adds(byRows).Select(a => JsonDocument.Parse(a.GetProperty("stats").GetString()!).RootElement.GetProperty("numRecords").GetInt64())];
        Assert.Equal(200_000, counts.Sum());
        Assert.All(counts, c => Assert.InRange(c, 1, 30_000));
        Assert.Equal(6, counts.Count(c => c == 30_000));

        using DataFrame read = DataFrame.ReadDelta(bySize).Sort("Id");
        Assert.True(read.Equals(rows), "the table holds other rows");
    }

    [Fact]
    public void TheFilesFooters_AreFreed_OnceRead_OrWhenTheWriteFails()
    {
        // The managed sink hands its files' footers (native bytes) to the commit, which reads their statistics and
        // frees them; a failed write frees those of the files it closed.
        using DataFrame rows = Typed(100_000);
        long live = ParquetFooter.Held;
        Sink("managed", () => rows.WriteDelta(Table("footers"), maxRowsPerFile: 10_000));
        Assert.Equal(10, DataFiles(Table("footers")).Length);
        Assert.Equal(live, ParquetFooter.Held);

        // A cast failing on a late batch: files of the earlier ones are closed by then.
        using DataFrame more = Typed(300_000);
        LazyFrame failing = more.Lazy().WithColumns(When(Col("Id") == 250_000).Then(Lit(1000)).Otherwise(Col("Tiny")).Cast(DataType.Int8, strict: true).Alias("Tiny"));
        Sink("managed", () => Assert.ThrowsAny<Exception>(() => failing.SinkDelta(Table("failing"), maxRowsPerFile: 10_000)));
        Assert.Equal(live, ParquetFooter.Held);
    }

    [Fact]
    public void DatasetFilesFooters_AreFreed_CompletedOrAborted()
    {
        var schema = new Apache.Arrow.Schema([new ArrowField("id", ArrowTypes.Int64Type.Default, true)], null);
        void Write(string name, bool complete)
        {
            using ParquetDatasetWriter writer = ParquetDatasetWriter.Create(Table(name), schema, new ParquetDatasetWriteOptions { MaxRowsPerFile = 10 });
            using (var batch = new Apache.Arrow.RecordBatch(schema, [new Apache.Arrow.Int64Array.Builder().AppendRange(Enumerable.Range(0, 50).Select(i => (long)i)).Build()], 50))
                writer.Write(batch);
            if (complete) Assert.Equal(5, writer.Complete().Files.Count);
            else writer.Abort(); // five files closed at their row limit: their footers held by the sink
        }
        long live = ParquetFooter.Held;
        Write("completed", complete: true);
        Assert.Equal(live, ParquetFooter.Held);
        Write("aborted", complete: false);
        Assert.Equal(live, ParquetFooter.Held);
    }

    [Fact]
    public void AFailureOnTheWay_LeavesNoFileAndNoCommit()
    {
        // A cast that fails on a row of the second batch: the first file is open (or written) by then.
        using DataFrame rows = Typed(300_000);
        string table = Table("failed");
        using (DataFrame first = Typed(10)) Sink("managed", () => first.WriteDelta(table));
        string[] before = AllFiles(table);
        LazyFrame failing = rows.Lazy().WithColumns(When(Col("Id") == 250_000).Then(Lit(1000)).Otherwise(Col("Tiny")).Cast(DataType.Int8, strict: true).Alias("Tiny"));
        Sink("managed", () => Assert.ThrowsAny<Exception>(() => failing.SinkDelta(table, mode: DeltaSaveMode.Append)));
        Assert.Equal(before, AllFiles(table)); // no data file, no temporary file, no commit
        Assert.Single(Directory.GetFiles(Path.Combine(table, "_delta_log"), "*.json"));
    }

    [Fact]
    public void TheCodecAndRowGroups_AreTheOnesAskedFor()
    {
        using DataFrame rows = Typed(3000);
        string table = Table("zstd");
        Sink("managed", () => rows.WriteDelta(table, compression: ParquetCompression.ZSTD, rowGroupSize: 1000));
        string file = Assert.Single(DataFiles(table));
        Assert.EndsWith(".zstd.parquet", file);
        using ParquetFile parquet = ParquetFile.Open(file);
        var groups = parquet.Metadata.Thrift!.RowGroups;
        Assert.Equal(3, groups.Length);
        Assert.All(groups.SelectMany(g => g.Columns), c => Assert.Equal(Kaminari.Seseragi.Files.Parquet.Metadata.CompressionCodec.Zstd, c.MetaData!.Codec));
    }

    private static DataFrame WithPartitionColumns(int rows)
    {
        string?[] strings = [" a b ", "x=y/z", "50%", "日本語", "", null, "plain", "a:b#c"];
        // Floating keys: -0.0 is 0.0 and every NaN one value (the IR's grouping keys).
        double?[] floats = [0.0, -0.0, double.NaN, BitConverter.Int64BitsToDouble(0x7FF8000000000001L), 1.5, null];
        using DataFrame keyed = DataFrame.From<Keys>([.. Enumerable.Range(0, rows).Select(i => new Keys(
            i, strings[i % strings.Length], i % 7 - 3, (i % 5) * 10_000_000_000L, DateOnly.FromDayNumber(738_000 + i % 4),
            new DateTime(2024, 1, 1 + i % 3, i % 2, 30, 15, DateTimeKind.Utc), i % 3 == 0, i % 4 * 1.25m, $"value {i}", floats[i % floats.Length]))]);
        return keyed.Lazy().WithColumns(
            Col("PDec").Cast(DataType.Decimal(10, 2)),
            Col("PStr").Cast(DataType.Categorical()).Alias("PCat"),
            Col("PTs").Cast(DataType.Datetime(TimeUnit.Microseconds))).Collect();
    }

    /// <summary>The adds of a table by their partition values (as JSON with sorted keys), each partition's sorted by statistics.</summary>
    private static Dictionary<string, List<JsonElement>> ByPartition(string table) =>
        Adds(table)
            .GroupBy(a => string.Join(",", a.GetProperty("partitionValues").EnumerateObject().OrderBy(p => p.Name, StringComparer.Ordinal)
                .Select(p => $"{p.Name}={(p.Value.ValueKind == JsonValueKind.Null ? "<null>" : p.Value.GetString())}")))
            .ToDictionary(g => g.Key, g => g.OrderBy(a => a.GetProperty("stats").GetString(), StringComparer.Ordinal).ToList());

    /// <summary>
    /// A partitioned write through both sinks: the same rows, the same partitions in the same directories holding as
    /// many rows, and where each sink wrote one file of a partition, the same statistics and file schema (the engine's
    /// sink writes the rows of an empty string and of a null in files of their own; ours, one partition, in one).
    /// </summary>
    private void SamePartitionedAsTheEnginesSink(Action<string> write, string native, string managed)
    {
        Sink("native", () => write(native));
        Sink("managed", () => write(managed));
        using (DataFrame a = DataFrame.ReadDelta(native).Sort("Id"), b = DataFrame.ReadDelta(managed).Sort("Id"))
            Assert.True(a.Equals(b), "the tables hold other rows");
        Dictionary<string, List<JsonElement>> nativeAdds = ByPartition(native), managedAdds = ByPartition(managed);
        Assert.Equal(nativeAdds.Keys.Order(), managedAdds.Keys.Order());
        static long Records(JsonElement add) => JsonDocument.Parse(add.GetProperty("stats").GetString()!).RootElement.GetProperty("numRecords").GetInt64();
        static string? DirectoryOf(JsonElement add) => Path.GetDirectoryName(add.GetProperty("path").GetString());
        foreach (var (partition, adds) in nativeAdds)
        {
            List<JsonElement> ours = managedAdds[partition];
            Assert.Equal(adds.Sum(Records), ours.Sum(Records));
            Assert.Equal(adds.Select(DirectoryOf).Distinct(), ours.Select(DirectoryOf).Distinct());
            if (adds.Count == 1 && ours.Count == 1)
            {
                Assert.Equal(adds[0].GetProperty("stats").GetString(), ours[0].GetProperty("stats").GetString());
                Assert.Equal(SchemaOf(AddedFile(native, adds[0])), SchemaOf(AddedFile(managed, ours[0])));
            }
        }
        Assert.All(DataFiles(managed), f => Assert.StartsWith("seseragi", CreatedBy(f)));
    }

    [Theory]
    [InlineData("PStr")]
    [InlineData("PInt,PBool")]
    [InlineData("PLong,PStr")]
    [InlineData("PDate")]
    [InlineData("PTs")]
    [InlineData("PDec")]
    [InlineData("PFloat")]
    [InlineData("PCat")]
    [InlineData("PCat,PInt")]
    public void PartitionedWrites_WriteWhatTheEnginesSinkWrites(string partitionBy)
    {
        using DataFrame rows = WithPartitionColumns(5000);
        string name = partitionBy.Replace(',', '_');
        SamePartitionedAsTheEnginesSink(table => rows.WriteDelta(table, partitionBy: Selectors.ByName(partitionBy.Split(','))), Table($"native_{name}"), Table($"managed_{name}"));
    }

    [Fact]
    public void FloatKeys_ZeroAndNaN_AreOnePartitionEach_AmongOtherKeys()
    {
        // The IR's grouping keys: -0.0 is 0.0 and every NaN one value, with other key columns too (the engine's own
        // sink keeps -0.0 and 0.0 apart when it groups by several columns).
        using DataFrame rows = WithPartitionColumns(4200);
        string table = Table("float_keys");
        Sink("managed", () => rows.WriteDelta(table, partitionBy: Selectors.ByName("PFloat", "PInt")));
        List<(string? Float, string Int, long Rows)> partitions = [.. Adds(table).Select(a => (
            a.GetProperty("partitionValues").GetProperty("PFloat").GetString(),
            a.GetProperty("partitionValues").GetProperty("PInt").GetString()!,
            JsonDocument.Parse(a.GetProperty("stats").GetString()!).RootElement.GetProperty("numRecords").GetInt64()))];
        foreach (string pInt in partitions.Select(p => p.Int).Distinct())
        {
            // Of every 42 rows, PInt = i % 7 - 3 and PFloat = floats[i % 6]: per PInt value 200 zeros (0.0, -0.0),
            // 200 NaNs (two payloads) and 100 nulls.
            var zeros = partitions.Where(p => p.Int == pInt && p.Float is "0.0" or "-0.0").ToList();
            Assert.Single(zeros);
            Assert.Equal(200, zeros[0].Rows);
            var nans = partitions.Where(p => p.Int == pInt && p.Float == "NaN").ToList();
            Assert.Single(nans);
            Assert.Equal(200, nans[0].Rows);
            Assert.Equal(100, Assert.Single(partitions, p => p.Int == pInt && p.Float is null).Rows);
        }
    }

    [Theory]
    [InlineData("native")]
    [InlineData("managed")]
    public void NonFinitePartitionValues_AreWrittenAsSparkWritesThem(string sink)
    {
        // NaN and the infinities have no JSON number: their partition values are Java's Double.toString, as Spark's.
        double otherNaN = BitConverter.Int64BitsToDouble(0x7FF8000000000001L);
        double?[] doubles = [double.NaN, double.PositiveInfinity, double.NegativeInfinity, 1.5, null, otherNaN];
        using DataFrame rows = DataFrame.From<NonFinite>([.. Enumerable.Range(0, 60).Select(i => new NonFinite(i, doubles[i % 6], (float?)doubles[i % 6]))]);
        foreach (string column in (string[])["D", "F"])
        {
            string table = Table($"non_finite_{sink}_{column}");
            Sink(sink, () => rows.WriteDelta(table, partitionBy: column));
            List<string?> values = [.. Adds(table).Select(a => a.GetProperty("partitionValues").GetProperty(column).GetString()).Distinct().OrderBy(v => v, StringComparer.Ordinal)];
            Assert.Equal([null, "-Infinity", "1.5", "Infinity", "NaN"], values);
            Assert.True(Directory.Exists(Path.Combine(table, $"{column}=NaN")) && Directory.Exists(Path.Combine(table, $"{column}=-Infinity")), "the directories");
            // Read back as written: the NaNs and infinities are values again, the nulls null.
            using DataFrame read = DataFrame.ReadDelta(table).Sort("Id");
            using Apache.Arrow.RecordBatch batch = Polars.NET.Core.Arrow.ArrowFfiBridge.ExportDataFrame(Handle(read));
            Apache.Arrow.IArrowArray got = batch.Column(batch.Schema.GetFieldIndex(column));
            for (int i = 0; i < 60; i++)
            {
                double? expected = doubles[i % 6];
                double? actual = got.IsNull(i) ? null : got is Apache.Arrow.DoubleArray d ? d.GetValue(i) : ((Apache.Arrow.FloatArray)got).GetValue(i);
                Assert.True(expected is null ? actual is null : actual is double a && (double.IsNaN(expected.Value) ? double.IsNaN(a) : a == expected.Value),
                    $"{sink} {column} row {i}: {actual} for {expected}");
            }
        }
    }

    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_Handle")]
    private static extern DataFrameHandle Handle(DataFrame df);

    [Fact]
    public void AnEmptyStringPartition_IsTheNullOne_OneFile()
    {
        // Rows of "" and of null, sorted apart, make one partition: one directory, one file.
        using DataFrame rows = WithPartitionColumns(800);
        string table = Table("empty_is_null");
        Sink("managed", () => rows.WriteDelta(table, partitionBy: "PStr"));
        string[] nulls = [.. Adds(table).Where(a => a.GetProperty("partitionValues").GetProperty("PStr").ValueKind == JsonValueKind.Null)
            .Select(a => a.GetProperty("path").GetString()!)];
        string file = Assert.Single(nulls);
        Assert.StartsWith("PStr=__HIVE_DEFAULT_PARTITION__/", file);
        Assert.Equal(200, JsonDocument.Parse(Adds(table).Single(a => a.GetProperty("path").GetString() == file).GetProperty("stats").GetString()!)
            .RootElement.GetProperty("numRecords").GetInt64());
    }

    [Fact]
    public void PartitionedChangeData_IsWhatTheEnginesSinkWrites()
    {
        using DataFrame rows = WithPartitionColumns(3000);
        Dictionary<string, string> properties = new() { [DeltaTableProperties.EnableChangeDataFeed] = "true" };
        void Write(string table)
        {
            using PolarsSchema schema = rows.Schema;
            Delta.CreateTable(table, schema, partitionBy: ["PStr"], properties: properties);
            rows.WriteDelta(table, mode: DeltaSaveMode.Append);
            Delta.Update(table, Col("Id") % 3 == 0, new Dictionary<string, Expr> { ["PInt"] = Col("PInt") + 100 });
        }
        SamePartitionedAsTheEnginesSink(Write, Table("cdc_native"), Table("cdc_managed"));
        // The change data: under _change_data, in the partitions' directories, the same rows.
        static string[] Directories(string table) =>
            [.. Directory.GetFiles(Path.Combine(table, "_change_data"), "*.parquet", SearchOption.AllDirectories)
                .Select(f => Path.GetRelativePath(table, Path.GetDirectoryName(f)!)).Distinct().Order()];
        Assert.Equal(Directories(Table("cdc_native")), Directories(Table("cdc_managed")));
        DataFrame ChangeData(string table)
        {
            using DataFrame read = DataFrame.ReadParquet(Path.Combine(Table(table), "_change_data", "**", "*.parquet"));
            return read.Sort([(IntoSelector)"Id", "_change_type"]);
        }
        using DataFrame native = ChangeData("cdc_native"), managed = ChangeData("cdc_managed");
        Assert.Equal(2000, managed.Height);
        Assert.True(native.Equals(managed), "the change data differ");
    }

    [Fact]
    public void ClusteringBuckets_SplitTheFiles_AsTheEnginesSinkDoes()
    {
        using DataFrame rows = WithPartitionColumns(20_000);
        void Write(string table)
        {
            using PolarsSchema schema = rows.Schema;
            Delta.CreateTable(table, schema, clusterBy: ["PLong", "PInt"]);
            for (int k = 0; k < 4; k++)
            {
                using DataFrame part = rows.Slice(k * 5000, 5000);
                part.WriteDelta(table, mode: DeltaSaveMode.Append);
            }
            Assert.True(Delta.Optimize(table, targetSizeMb: 1) > 0);
        }
        string native = Table("clustered_native"), managed = Table("clustered_managed");
        Sink("native", () => Write(native));
        Sink("managed", () => Write(managed));
        using (DataFrame a = DataFrame.ReadDelta(native).Sort("Id"), b = DataFrame.ReadDelta(managed).Sort("Id"))
            Assert.True(a.Equals(b), "the tables hold other rows");
        // The optimize's files: as many, each a bucket's rows, the bucket column left out.
        static List<JsonElement> Optimized(string table) =>
            [.. Adds(table).Where(a => a.TryGetProperty("clusteringProvider", out _))];
        List<JsonElement> nativeFiles = Optimized(native), managedFiles = Optimized(managed);
        Assert.Equal(nativeFiles.Count, managedFiles.Count);
        Assert.Equal(nativeFiles.Select(a => a.GetProperty("stats").GetString()).Order(), managedFiles.Select(a => a.GetProperty("stats").GetString()).Order());
        Assert.All(managedFiles, a => Assert.DoesNotContain(SchemaOf(AddedFile(managed, a)), e => e.Contains("bucket", StringComparison.OrdinalIgnoreCase)));
    }

    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "get_Handle")]
    private static extern LazyFrameHandle LazyHandle(LazyFrame lf);

    [Fact]
    public void RowsGroupedAlready_AreNotSorted_AndEachPartitionClosesWhenTheNextComes()
    {
        // Rows grouped by their key (a run per key across batches of 1000): files of the same rows as sorted ones.
        using DataFrame rows = DataFrame.From<Grouped>([.. Enumerable.Range(0, 12_000).Select(i => new Grouped(i / 1500, i))]);
        string Write(bool preGrouped)
        {
            string root = Table(preGrouped ? "grouped" : "sorted");
            Directory.CreateDirectory(root);
            var fileSystem = new Kaminari.Seseragi.Lakehouse.Engine.LocalFileSystem(root);
            DeltaSchema.StructField[] fields = [new() { Name = "Value", Type = new DeltaSchema.PrimitiveType("long") }];
            List<int> opened = [];
            ManagedSink.SinkResult result = ManagedSink.Sink(fileSystem, PolarsWrapper.LazyClone(LazyHandle(rows.Lazy())), ["Key"], preGrouped, fields,
                keys => keys is null ? -1 : KeyOf(keys),
                (key, index) =>
                {
                    opened.Add(key);
                    return $"Key={key}/part-{index:D5}.parquet";
                },
                KernelWriteOptions.Default with { RowGroupSize = 1000, KeysPreGrouped = preGrouped }, approxBytesPerFile: 0);
            Assert.Equal(8, result.Files.Count);
            Assert.Equal([0, 1, 2, 3, 4, 5, 6, 7], opened); // one file per key, in the order the keys came
            return root;
        }
        string grouped = Write(preGrouped: true), sorted = Write(preGrouped: false);
        for (int key = 0; key < 8; key++)
        {
            using DataFrame a = DataFrame.ReadParquet(Path.Combine(grouped, $"Key={key}", "*.parquet"));
            using DataFrame b = DataFrame.ReadParquet(Path.Combine(sorted, $"Key={key}", "*.parquet"));
            Assert.Equal(1500, a.Height);
            Assert.True(a.Equals(b), $"key {key}");
            Assert.Equal(["Value"], a.Columns);
        }
    }

    [Fact]
    public void BatchesOfManyChunks_GroupedOrNot_WriteEachPartitionsRows()
    {
        // The engine hands concatenated frames over as batches of several chunks: some chunks grouped by their key
        // already (written as ranges), some not (gathered); one key spans chunks and batches.
        using DataFrame grouped = DataFrame.From<Grouped>([.. Enumerable.Range(0, 3000).Select(i => new Grouped(i / 700, i))]);
        using DataFrame mixed = DataFrame.From<Grouped>([.. Enumerable.Range(3000, 3000).Select(i => new Grouped(i * 7919 % 11, i))]);
        using DataFrame single = DataFrame.From<Grouped>([.. Enumerable.Range(6000, 1000).Select(i => new Grouped(4, i))]);
        using LazyFrame rows = LazyFrame.Concat([grouped.Lazy(), mixed.Lazy(), single.Lazy(), mixed.Lazy().Filter(Col("Value") % 3 != 0)]);
        string root = Table("chunks");
        Directory.CreateDirectory(root);
        var fileSystem = new Kaminari.Seseragi.Lakehouse.Engine.LocalFileSystem(root);
        DeltaSchema.StructField[] fields = [new() { Name = "Value", Type = new DeltaSchema.PrimitiveType("long") }];
        List<int> described = [];
        ManagedSink.SinkResult result = ManagedSink.Sink(fileSystem, PolarsWrapper.LazyClone(LazyHandle(rows)), ["Key"], false, fields,
            keys =>
            {
                int key = KeyOf(keys!);
                described.Add(key);
                return key;
            },
            (key, index) => $"Key={key}/part-{index:D5}.parquet", KernelWriteOptions.Default with { RowGroupSize = 1000 }, approxBytesPerFile: 0);
        // Each key described once, in the order the keys first came; one file each.
        using DataFrame all = rows.Collect();
        long[] firstCame = [.. ((Apache.Arrow.Int64Array)Polars.NET.Core.Arrow.ArrowFfiBridge.ExportDataFrame(Handle(all)).Column(0)).Values.ToArray().Distinct()];
        Assert.Equal(firstCame.Select(k => (int)k), described);
        Assert.Equal(11, result.Files.Count);
        foreach (int key in described)
        {
            using DataFrame written = DataFrame.ReadParquet(Path.Combine(root, $"Key={key}", "*.parquet"));
            using DataFrame expected = all.Filter(Col("Key") == key).Select(Col("Value"));
            Assert.True(expected.Equals(written), $"key {key}: the rows, in their order"); // a stable grouping keeps the order
        }
    }

    [Fact]
    public void AFailureWithManyFilesOpen_LeavesNoFileAndNoCommit()
    {
        using DataFrame rows = Typed(300_000);
        string table = Table("failed_partitioned");
        using (DataFrame first = Typed(10)) Sink("managed", () => first.Lazy().WithColumns((Col("Id") % 50).Alias("Part")).SinkDelta(table, partitionBy: "Part"));
        string[] before = AllFiles(table);
        LazyFrame failing = rows.Lazy()
            .WithColumns((Col("Id") % 50).Alias("Part"), When(Col("Id") == 250_000).Then(Lit(1000)).Otherwise(Col("Tiny")).Cast(DataType.Int8, strict: true).Alias("Tiny"));
        Sink("managed", () => Assert.ThrowsAny<Exception>(() => failing.SinkDelta(table, partitionBy: "Part", mode: DeltaSaveMode.Append)));
        Assert.Equal(before, AllFiles(table)); // no data file, no temporary file, no commit
        Assert.Single(Directory.GetFiles(Path.Combine(table, "_delta_log"), "*.json"));
    }

    [Fact]
    public void FilesBeyondTheMemoryBudget_EndTheirRowGroupsEarly()
    {
        // 40 partitions, each file holding its pages until its row group ends: a budget of 2 MiB ends row groups early
        // (on a local disk, that is enough: the pages go to the file); the rows are all there.
        using DataFrame rows = Typed(200_000);
        string table = Table("budget");
        WithBudget(2 << 20, () => Sink("managed", () => rows.Lazy().WithColumns((Col("Id") % 40).Alias("Part")).SinkDelta(table, partitionBy: "Part")));
        Assert.Equal(40, Adds(table).Count);
        using DataFrame read = DataFrame.ReadDelta(table).Drop("Part").Sort("Id");
        Assert.True(read.Equals(rows), "the table holds other rows");
        // The largest files' row groups were ended (until the rest fit).
        Assert.Contains(DataFiles(table), f =>
        {
            using ParquetFile file = ParquetFile.Open(f);
            return file.Metadata.RowGroups.Count > 1;
        });
    }

    private static void WithBudget(long bytes, Action write)
    {
        ManagedSink.MemoryBudgetOverride = bytes;
        try
        {
            write();
        }
        finally
        {
            ManagedSink.MemoryBudgetOverride = null;
        }
    }

    /// <summary>
    /// A local directory whose files are held in memory until committed (as a cloud upload holds its parts); creating a
    /// file whose path holds <paramref name="refused"/> fails.
    /// </summary>
    private sealed class BufferingFileSystem(string root, string? refused = null) : Kaminari.Seseragi.Lakehouse.Engine.IWritableFileSystem
    {
        private readonly Kaminari.Seseragi.Lakehouse.Engine.LocalFileSystem _disk = new(root);

        public IReadOnlyList<Kaminari.Seseragi.Lakehouse.Engine.FileEntry> List(string directory, string? startFrom = null) => _disk.List(directory, startFrom);
        public bool TryReadAll(string path, System.Buffers.IBufferWriter<byte> into) => _disk.TryReadAll(path, into);
        public int ReadRange(string path, long offset, Span<byte> destination) => _disk.ReadRange(path, offset, destination);
        public bool TryCreate(string path, ReadOnlyMemory<byte> content) =>
            refused is not null && path.Contains(refused, StringComparison.Ordinal) ? throw new IOException($"Refused to create {path}.") : _disk.TryCreate(path, content);
        public void Overwrite(string path, ReadOnlyMemory<byte> content) => _disk.Overwrite(path, content);
        public bool Delete(string path) => _disk.Delete(path);
        public IReadOnlyList<Kaminari.Seseragi.Lakehouse.Engine.FileEntry> ListAll(string directory) => _disk.ListAll(directory);
    }

    [Fact]
    public void FilesBeyondTheMemoryBudget_CloseEarly_WhenTheirStreamsHoldTheBytes()
    {
        // Streams holding whole files: ending row groups frees nothing, so the largest files close early (more files
        // than partitions, every row in one of them once).
        using DataFrame rows = DataFrame.From<Grouped>([.. Enumerable.Range(0, 100_000).Select(i => new Grouped(i % 20, i))]);
        string root = Table("buffered");
        Directory.CreateDirectory(root);
        DeltaSchema.StructField[] fields = [new() { Name = "Value", Type = new DeltaSchema.PrimitiveType("long") }];
        ManagedSink.SinkResult result = null!;
        WithBudget(256 << 10, () => result = ManagedSink.Sink(new BufferingFileSystem(root), PolarsWrapper.LazyClone(LazyHandle(rows.Lazy())), ["Key"], false, fields,
            keys => KeyOf(keys!), (key, index) => $"Key={key}/part-{index:D5}.parquet", KernelWriteOptions.Default with { RowGroupSize = 5000 }, approxBytesPerFile: 0));
        // Every early close but those of partitions with no rows after it opened another file.
        Assert.True(result.ClosedForMemory > 0);
        Assert.InRange(result.Files.Count, 21, 20 + result.ClosedForMemory);
        using DataFrame read = DataFrame.ReadParquet(Path.Combine(root, "**", "*.parquet"));
        Assert.Equal(100_000, read.Height);
        Assert.Equal(100_000, read["Value"].NUnique());
    }

    [Fact]
    public void AFileThatFailsToBePublished_FailsTheSink()
    {
        // Files are published while the next rows are written: a failure among them is not lost.
        using DataFrame rows = DataFrame.From<Grouped>([.. Enumerable.Range(0, 20_000).Select(i => new Grouped(i % 4, i))]);
        string root = Table("refused");
        Directory.CreateDirectory(root);
        DeltaSchema.StructField[] fields = [new() { Name = "Value", Type = new DeltaSchema.PrimitiveType("long") }];
        var e = Assert.Throws<IOException>(() => ManagedSink.Sink(new BufferingFileSystem(root, refused: "Key=2/"), PolarsWrapper.LazyClone(LazyHandle(rows.Lazy())),
            ["Key"], false, fields, keys => KeyOf(keys!), (key, index) => $"Key={key}/part-{index:D5}.parquet", KernelWriteOptions.Default, approxBytesPerFile: 0));
        Assert.Contains("Key=2/part-00000.parquet", e.Message);
        Assert.False(Directory.Exists(Path.Combine(root, "Key=2")));
    }

    [Fact]
    public void TheMemoryBudget_IsAnEighthOfTheMemoryAvailable_OrWhatTheVariableSays()
    {
        const long Available = 32L << 30;
        Assert.Equal(4L << 30, ManagedSink.Budget(null, Available));
        Assert.Equal(4L << 30, ManagedSink.Budget(" ", Available));
        Assert.Equal(256L << 20, ManagedSink.Budget(null, 1L << 30)); // at least 256 MiB
        Assert.Equal(2L << 30, ManagedSink.Budget("2GiB", Available));
        Assert.Equal(512L << 20, ManagedSink.Budget("512 MiB", Available));
        Assert.Equal(8L << 30, ManagedSink.Budget("25%", Available));
        Assert.Equal(4L << 30, ManagedSink.Budget(" 12.5 % ", Available));
        foreach (string wrong in (string[])["0", "0%", "150%", "lots", "-1GiB"])
            Assert.Contains(ManagedSink.MemoryVariable, Assert.Throws<ArgumentException>(() => ManagedSink.Budget(wrong, Available)).Message);

        // The variable, read when a sink starts (this project runs its tests one at a time: the process environment).
        string? before = Environment.GetEnvironmentVariable(ManagedSink.MemoryVariable);
        try
        {
            Environment.SetEnvironmentVariable(ManagedSink.MemoryVariable, "3GiB");
            Assert.Equal(3L << 30, ManagedSink.MemoryBudget);
            Environment.SetEnvironmentVariable(ManagedSink.MemoryVariable, null);
            Assert.Equal(ManagedSink.Budget(null, GC.GetGCMemoryInfo().TotalAvailableMemoryBytes), ManagedSink.MemoryBudget);
        }
        finally
        {
            Environment.SetEnvironmentVariable(ManagedSink.MemoryVariable, before);
        }
    }

    [Fact]
    public void TheSinkVariable_ChoosesTheManagedSink_UnlessItSaysNative()
    {
        foreach (string? setting in (string?[])[null, "", " ", "managed", " Managed "]) Assert.True(ManagedSink.Chosen(setting), setting);
        Assert.False(ManagedSink.Chosen("native"));
        Assert.False(ManagedSink.Chosen("NATIVE "));
        Assert.Contains(ManagedSink.Variable, Assert.Throws<ArgumentException>(() => ManagedSink.Chosen("auto")).Message);

        // By default on a local disk; the engine's sink when asked, which refuses maps and says how to write them.
        string root = Table("choice");
        Directory.CreateDirectory(root);
        var disk = new Kaminari.Seseragi.Lakehouse.Engine.LocalFileSystem(root);
        Assert.True(ManagedSink.Applies(disk));
        Sink("native", () => Assert.False(ManagedSink.Applies(disk)));
        DeltaSchema.StructField[] fields =
        [
            new() { Name = "id", Type = new DeltaSchema.PrimitiveType("long") },
            new() { Name = "tags", Type = new DeltaSchema.MapType(new DeltaSchema.PrimitiveType("string"), new DeltaSchema.PrimitiveType("long"), true) },
        ];
        string message = Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(() => DeltaKernelWrite.EnsureNoMaps(fields)).Message;
        Assert.Contains("(tags)", message);
        Assert.Contains($"{ManagedSink.Variable}=native", message);
    }

    [Fact]
    public void DataMaps_AreListsOfTwoFieldStructs_WhereTheTableHasMaps()
    {
        static DeltaSchema.PrimitiveType P(string name) => new(name);
        static DeltaSchema.StructField F(string name, DeltaSchema.DeltaType type) => new() { Name = name, Type = type };
        static DeltaSchema.ArrayType Entries(DeltaSchema.DeltaType key, DeltaSchema.DeltaType value) =>
            new(new DeltaSchema.StructType([F("key", key), F("value", value)]), true);
        var table = new DeltaSchema.StructType(
        [
            F("m", new DeltaSchema.MapType(P("string"), P("long"), true)),
            F("s", new DeltaSchema.StructType([F("inner", new DeltaSchema.MapType(P("integer"), P("string"), false))])),
            F("a", new DeltaSchema.ArrayType(new DeltaSchema.MapType(P("string"), new DeltaSchema.MapType(P("string"), P("double"), true), true), true)),
            F("pairs", new DeltaSchema.ArrayType(new DeltaSchema.StructType([F("x", P("long")), F("y", P("long"))]), true)),
        ]);
        var data = new DeltaSchema.StructType(
        [
            F("m", new DeltaSchema.ArrayType(new DeltaSchema.StructType([F("k", P("string")), F("v", P("integer"))]), true)), // names by position
            F("s", new DeltaSchema.StructType([F("inner", Entries(P("integer"), P("string")))])),
            F("a", new DeltaSchema.ArrayType(Entries(P("string"), Entries(P("string"), P("double"))), true)),
            F("pairs", new DeltaSchema.ArrayType(new DeltaSchema.StructType([F("x", P("long")), F("y", P("long"))]), true)),
            F("extra", Entries(P("string"), P("long"))), // not in the table: left as it is
        ]);
        DeltaSchema.StructType shaped = DeltaKernelWrite.WithTableMaps(data, table);
        Assert.Equal("map<string,integer>", shaped.Field("m")!.Type.ToString());
        Assert.Equal("struct<inner:map<integer,string>>", shaped.Field("s")!.Type.ToString());
        Assert.Equal("array<map<string,map<string,double>>>", shaped.Field("a")!.Type.ToString());
        Assert.Equal("array<struct<x:long,y:long>>", shaped.Field("pairs")!.Type.ToString());
        Assert.Equal("array<struct<key:string,value:long>>", shaped.Field("extra")!.Type.ToString());
        // A list of structs of other widths stays a list: the schema check refuses it against the map.
        var wide = new DeltaSchema.StructType([F("m", new DeltaSchema.ArrayType(new DeltaSchema.StructType([F("k", P("string")), F("v", P("long")), F("w", P("long"))]), true))]);
        Assert.IsType<DeltaSchema.ArrayType>(DeltaKernelWrite.WithTableMaps(wide, table).Field("m")!.Type);
    }

    private static int KeyOf(DataFrameHandle keys)
    {
        using Apache.Arrow.RecordBatch batch = Polars.NET.Core.Arrow.ArrowFfiBridge.ExportDataFrame(keys);
        return (int)((Apache.Arrow.Int64Array)batch.Column(batch.Schema.GetFieldIndex("Key"))).GetValue(0)!.Value;
    }

    [Fact]
    public void TheWriterSchema_NamesTheMaps_AndCarriesTheFieldIds()
    {
        // As the engine exports a table's columns: maps as large lists of key/value structs, at the top, in a struct,
        // as a list's element and as a map's value.
        static ArrowField Entries(ArrowTypes.IArrowType value) =>
            new("entries", new ArrowTypes.StructType([new ArrowField("key", ArrowTypes.StringType.Default, true), new ArrowField("value", value, true)]), false);
        static ArrowTypes.LargeListType MapLike(ArrowTypes.IArrowType value) => new(Entries(value));
        var exported = new Apache.Arrow.Schema(
        [
            new ArrowField("col-m", MapLike(ArrowTypes.Int32Type.Default), true),
            new ArrowField("col-s", new ArrowTypes.StructType([new ArrowField("col-sm", MapLike(ArrowTypes.Int32Type.Default), true), new ArrowField("col-x", ArrowTypes.Int64Type.Default, true)]), true),
            new ArrowField("col-a", new ArrowTypes.LargeListType(new ArrowField("item", MapLike(ArrowTypes.Int32Type.Default), true)), true),
            new ArrowField("col-mm", MapLike(MapLike(ArrowTypes.Int32Type.Default)), true),
            new ArrowField("_change_type", ArrowTypes.StringType.Default, true),
        ], null);
        static DeltaSchema.MapType Map(DeltaSchema.DeltaType value) => new(new DeltaSchema.PrimitiveType("string"), value, true);
        static DeltaSchema.StructField Field(string name, string physical, int id, DeltaSchema.DeltaType type) => new()
        {
            Name = name, Type = type,
            Metadata = new Dictionary<string, string> { [DeltaSchema.StructField.PhysicalNameKey] = physical, [DeltaSchema.StructField.ColumnIdKey] = $"{id}" },
        };
        var integer = new DeltaSchema.PrimitiveType("integer");
        DeltaSchema.StructField[] fields =
        [
            Field("m", "col-m", 1, Map(integer)),
            Field("s", "col-s", 2, new DeltaSchema.StructType([Field("sm", "col-sm", 3, Map(integer)), Field("x", "col-x", 4, new DeltaSchema.PrimitiveType("long"))])),
            Field("a", "col-a", 5, new DeltaSchema.ArrayType(Map(integer), true)),
            Field("mm", "col-mm", 6, Map(Map(integer))),
        ];
        var (schema, maps) = ManagedSink.WriterSchema(exported, fields);
        Assert.Equal(["col-a[]", "col-m", "col-mm", "col-mm.value", "col-s.col-sm"], maps!.Order());
        Assert.Equal([1, 2, 5, 6, null], schema.FieldsList.Select(Kaminari.Sazanami.Arrow.TypeSystem.ArrowFieldIds.Of));
        var nested = (ArrowTypes.StructType)schema.FieldsList[1].DataType;
        Assert.Equal([3, 4], nested.Fields.Select(Kaminari.Sazanami.Arrow.TypeSystem.ArrowFieldIds.Of));

        // Written with them: maps in the file, ids on their fields.
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions { MapColumns = maps, LeaveOpen = true })) writer.Complete();
        stream.Position = 0;
        using ParquetFile file = ParquetFile.Open(stream);
        var elements = file.Metadata.Thrift!.Schema;
        Assert.Equal(5, elements.Count(e => e.LogicalType?.Kind == Kaminari.Seseragi.Files.Parquet.Metadata.LogicalTypeKind.Map));
        Assert.Equal(3, elements.Single(e => e.Name == "col-sm").FieldId);
    }

    private sealed record Inner(int A, string? B);

    private sealed record Keys(long Id, string? PStr, int PInt, long PLong, DateOnly PDate, DateTime PTs, bool PBool, decimal PDec, string Text, double? PFloat);

    private sealed record Grouped(long Key, long Value);

    private sealed record NonFinite(long Id, double? D, float? F);

    private sealed record Row(long Id, sbyte Tiny, short Small, int Int, float Single, double Double, decimal Money, string? Text,
        bool Flag, DateOnly Day, DateTimeOffset At, List<int>? Numbers, Inner? Nested)
    {
        public static Row Of(int i) => new(i, (sbyte)(i % 100), (short)(i % 30_000), i * 7, i / 3f, i / 7d, i * 1.25m,
            i % 5 == 0 ? null : i % 7 == 0 ? $"{new string('w', 80)}-{i}" : $"text-{i}",
            i % 2 == 0, DateOnly.FromDayNumber(738_000 + i % 5000), DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000 + i * 1000L),
            i % 6 == 0 ? null : [i, i + 1], i % 9 == 0 ? null : new Inner(i, i % 3 == 0 ? null : $"b{i}"));
    }
}
