using System.Text.Json;
using Kaminari.Seseragi.Files.Parquet;
using Polars.CSharp;
using Polars.NET.Core.Delta;
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
/// way leaves no file and no commit; partitioned writes still go through the engine's sink.
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

    [Fact]
    public void PartitionedWrites_StillGoThroughTheEnginesSink()
    {
        using DataFrame rows = Typed(1000);
        string table = Table("partitioned");
        Sink("managed", () => rows.WriteDelta(table, partitionBy: "Flag"));
        Assert.All(DataFiles(table), f => Assert.DoesNotContain("seseragi", CreatedBy(f)));
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
        Assert.Equal([1, 2, 5, 6, null], schema.FieldsList.Select(Kaminari.Sazanami.Arrow.ArrowFieldIds.Of));
        var nested = (ArrowTypes.StructType)schema.FieldsList[1].DataType;
        Assert.Equal([3, 4], nested.Fields.Select(Kaminari.Sazanami.Arrow.ArrowFieldIds.Of));

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

    private sealed record Row(long Id, sbyte Tiny, short Small, int Int, float Single, double Double, decimal Money, string? Text,
        bool Flag, DateOnly Day, DateTimeOffset At, List<int>? Numbers, Inner? Nested)
    {
        public static Row Of(int i) => new(i, (sbyte)(i % 100), (short)(i % 30_000), i * 7, i / 3f, i / 7d, i * 1.25m,
            i % 5 == 0 ? null : i % 7 == 0 ? $"{new string('w', 80)}-{i}" : $"text-{i}",
            i % 2 == 0, DateOnly.FromDayNumber(738_000 + i % 5000), DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000 + i * 1000L),
            i % 6 == 0 ? null : [i, i + 1], i % 9 == 0 ? null : new Inner(i, i % 3 == 0 ? null : $"b{i}"));
    }
}
