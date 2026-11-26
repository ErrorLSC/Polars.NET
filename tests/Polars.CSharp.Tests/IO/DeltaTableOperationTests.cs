#nullable enable
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Exceptions;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// Table operations of the public Delta API, as Spark's: ADD COLUMNS, UNSET TBLPROPERTIES, DESCRIBE DETAIL, REPLACE
/// TABLE, SHALLOW CLONE and CONVERT TO DELTA (each checked against what delta-spark 4.4 commits for it).
/// </summary>
public class DeltaTableOperationTests
{
    private sealed record Row(int Id, string? S);
    private sealed record Wide(int Id, string? S, long? N);
    private sealed record Replaced(long K, string? V);
    private sealed record IdOnly(int Id);
    private sealed record IdLong(long Id);

    private static void InDirectory(string name, Action<string> body)
    {
        string path = Path.Combine(Path.GetTempPath(), $"delta_ops_{name}_{Guid.NewGuid():N}");
        try
        {
            body(path);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }

    private static void Write<T>(string path, IEnumerable<T> rows)
    {
        using DataFrame df = DataFrame.From(rows);
        df.WriteDelta(path, mode: DeltaSaveMode.Append, mkdir: true);
    }

    private static List<T> Rows<T>(string path, ulong? version = null)
    {
        using DataFrame read = DataFrame.ReadDelta(path, version: version).Sort("Id");
        return [.. read.AsEnumerable<T>()];
    }

    private static Snapshot Latest(string path) => Snapshot.Load(new LocalFileSystem(path));

    private static JsonElement CommitInfo(string path, long version) => Action(path, version, "commitInfo");

    private static JsonElement Action(string path, long version, string kind) => Actions(path, version, kind).First();

    private static List<JsonElement> Actions(string path, long version, string kind)
    {
        List<JsonElement> found = [];
        foreach (string line in File.ReadLines(Path.Combine(path, "_delta_log", $"{version:D20}.json")))
        {
            using JsonDocument doc = JsonDocument.Parse(line);
            if (doc.RootElement.TryGetProperty(kind, out JsonElement action)) found.Add(action.Clone());
        }
        return found;
    }

    private static string Parameter(JsonElement info, string name) =>
        info.GetProperty("operationParameters").GetProperty(name) is { ValueKind: JsonValueKind.String } text ? text.GetString()! : info.GetProperty("operationParameters").GetProperty(name).GetRawText();

    private static string Refusal(Action action) => Assert.Throws<PolarsException>(action).Message;

    // ---- ADD COLUMNS ----

    [Fact]
    public void Added_columns_go_where_asked_read_null_for_old_rows_and_are_recorded_as_Spark_records_them() => InDirectory("add", path =>
    {
        Write(path, [new Row(1, "a")]);
        Delta.AddColumns(path,
        [
            new DeltaNewColumn("N", DataType.Int64, Comment: "count"),
            new DeltaNewColumn("First", DataType.Int32, First: true),
            new DeltaNewColumn("Mid", DataType.String, After: "id"),
        ]);
        StructType schema = Latest(path).Schema;
        Assert.Equal(["First", "Id", "Mid", "S", "N"], schema.Fields.Select(f => f.Name));
        Assert.Equal("count", schema.Field("N")!.Metadata["comment"]);
        Assert.Equal(new PrimitiveType(PrimitiveType.Long).Name, ((PrimitiveType)schema.Field("N")!.Type).Name);
        JsonElement info = CommitInfo(path, 1);
        Assert.Equal("ADD COLUMNS", info.GetProperty("operation").GetString());
        Assert.Equal(
            """[{"column":{"name":"N","type":"long","nullable":true,"metadata":{"comment":"count"}}},{"column":{"name":"First","type":"integer","nullable":true,"metadata":{}},"position":"FIRST"},{"column":{"name":"Mid","type":"string","nullable":true,"metadata":{}},"position":"AFTER Id"}]""",
            Parameter(info, "columns"));

        Write(path, [new Wide(2, "b", 7)]);
        Assert.Equal([new Wide(1, "a", null), new Wide(2, "b", 7)], Rows<Wide>(path));
    });

    [Fact]
    public void Nested_fields_are_added_inside_structs_and_array_elements() => InDirectory("nested", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From([
            ("Id", DataType.Int32),
            ("St", DataType.Struct([new Field("X", DataType.Int32)])),
            ("Items", DataType.List(DataType.Struct([new Field("P", DataType.Int32)]))),
        ]));
        Delta.AddColumns(path, [new DeltaNewColumn("st.Y", DataType.String, After: "x"), new DeltaNewColumn("items.element.Q", DataType.Int32)]);
        StructType schema = Latest(path).Schema;
        Assert.Equal(["X", "Y"], ((StructType)schema.Field("St")!.Type).Fields.Select(f => f.Name));
        Assert.Equal(["P", "Q"], ((StructType)((ArrayType)schema.Field("Items")!.Type).ElementType).Fields.Select(f => f.Name));
        Assert.Contains("\"name\":\"St.Y\"", Parameter(CommitInfo(path, 1), "columns"), StringComparison.Ordinal);
    });

    [Fact]
    public void Under_column_mapping_added_columns_get_ids_and_physical_names() => InDirectory("mapped", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From<Row>(), properties: new() { ["delta.columnMapping.mode"] = "name" });
        Write(path, [new Row(1, "a")]);
        Delta.AddColumns(path, [new DeltaNewColumn("N", DataType.Int64)]);
        Snapshot table = Latest(path);
        StructField added = table.Schema.Field("N")!;
        Assert.Equal(3, added.FieldId);
        Assert.StartsWith("col-", added.PhysicalName, StringComparison.Ordinal);
        Assert.Equal("3", table.Metadata.Configuration["delta.columnMapping.maxColumnId"]);
        Write(path, [new Wide(2, "b", 9)]);
        Assert.Equal([new Wide(1, "a", null), new Wide(2, "b", 9)], Rows<Wide>(path));
    });

    [Fact]
    public void A_timestamp_without_time_zone_brings_its_feature() => InDirectory("ntz", path =>
    {
        Write(path, [new Row(1, "a")]);
        Delta.AddColumns(path, [new DeltaNewColumn("At", DataType.Datetime())]);
        Assert.Contains(TableFeatures.TimestampNtz, Latest(path).Protocol.WriterFeatures!);
    });

    [Theory]
    [InlineData("s", null, false, "Cannot add column 's': 'S' already exists.")]
    [InlineData("S.x", null, false, "Cannot add 'S.x': S is not a struct.")]
    [InlineData("Nope.x", null, false, "The column 'Nope.x' is not in the table's schema.")]
    [InlineData("N", "nope", false, "Cannot add 'N' after 'nope': the table has no column 'nope'.")]
    public void A_column_that_cannot_be_added_is_refused_and_nothing_committed(string column, string? after, bool first, string refusal) => InDirectory("addbad", path =>
    {
        Write(path, [new Row(1, "a")]);
        Assert.Equal(refusal, Refusal(() => Delta.AddColumns(path, [new DeltaNewColumn(column, DataType.Int32, After: after, First: first)])));
        Assert.Equal("Cannot add column 'f': 'F' already exists.",
            Refusal(() => Delta.AddColumns(path, [new DeltaNewColumn("F", DataType.Int32), new DeltaNewColumn("f", DataType.Int32)])));
        Assert.Equal(0, Latest(path).Version);
    });

    // ---- UNSET TBLPROPERTIES ----

    [Fact]
    public void Unset_removes_properties_in_any_case_of_a_delta_key_and_skips_missing_ones_only_when_asked() => InDirectory("unset", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From<Row>(), properties: new()
        {
            ["foo"] = "1", [DeltaTableProperties.LogRetentionDuration] = "interval 40 days", [DeltaTableProperties.EnableChangeDataFeed] = "true",
        });
        Delta.UnsetTableProperties(path, ["foo", "DELTA.LOGRETENTIONDURATION"]);
        Snapshot table = Latest(path);
        Assert.Equal([DeltaTableProperties.EnableChangeDataFeed], table.Metadata.Configuration.Keys);
        JsonElement info = CommitInfo(path, 1);
        Assert.Equal("UNSET TBLPROPERTIES", info.GetProperty("operation").GetString());
        Assert.Equal("""["foo","DELTA.LOGRETENTIONDURATION"]""", Parameter(info, "properties"));

        Assert.Equal("The table has no property 'foo'. To skip properties the table does not have, pass ifExists.",
            Refusal(() => Delta.UnsetTableProperties(path, ["foo"])));
        Delta.UnsetTableProperties(path, ["foo"], ifExists: true);
        Assert.Equal(1, Latest(path).Version);
        // The feature stays in the protocol, unused.
        Delta.UnsetTableProperties(path, [DeltaTableProperties.EnableChangeDataFeed]);
        Assert.Empty(Latest(path).Metadata.Configuration);
        Assert.Contains(TableFeatures.ChangeDataFeed, TableFeatures.WriterFeaturesOf(Latest(path).Protocol));
    });

    [Theory]
    [InlineData("delta.columnMapping.mode", "drop column mapping with Delta.DropFeature(\"columnMapping\")")]
    [InlineData("delta.constraints.c", "drop CHECK constraints with Delta.DropConstraint")]
    [InlineData("delta.feature.deletionVectors", "drop table features with Delta.DropFeature")]
    [InlineData("delta.minWriterVersion", "drop table features with Delta.DropFeature")]
    [InlineData("delta.columnMapping.maxColumnId", "the table keeps it itself")]
    [InlineData("delta.rowTracking.materializedRowIdColumnName", "the table keeps it itself")]
    public void Properties_only_another_operation_changes_are_refused(string key, string why) => InDirectory("unsetbad", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From<Row>(), properties: new() { ["delta.columnMapping.mode"] = "name", [DeltaTableProperties.EnableRowTracking] = "true" });
        Delta.AddConstraint(path, "c", "Id > 0");
        string message = Refusal(() => Delta.UnsetTableProperties(path, [key]));
        Assert.StartsWith($"The property {key} cannot be removed: ", message);
        Assert.Contains(why, message);
        Assert.Equal(1, Latest(path).Version);
    });

    // ---- DESCRIBE DETAIL ----

    [Fact]
    public void Detail_tells_what_DESCRIBE_DETAIL_tells() => InDirectory("detail", path =>
    {
        DateTimeOffset before = DateTimeOffset.UtcNow.AddSeconds(-1);
        Delta.CreateTable(path, PolarsSchema.From<Row>(), clusterBy: ["Id"], properties: new() { ["foo"] = "1" });
        Write(path, [new Row(1, "a"), new Row(2, "b")]);
        DeltaTableDetail detail = Delta.Detail(path);
        Snapshot table = Latest(path);
        Assert.Equal(("delta", table.Metadata.Id, Path.GetFullPath(path), 1L), (detail.Format, detail.Id, detail.Location, detail.Version));
        Assert.Equal((table.Files.Count, table.Files.Sum(f => f.Size)), (detail.NumFiles, detail.SizeInBytes));
        Assert.Empty(detail.PartitionColumns);
        Assert.Equal(["Id"], detail.ClusteringColumns);
        Assert.Equal("1", detail.Properties["foo"]);
        Assert.Equal((1, 7), (detail.MinReaderVersion, detail.MinWriterVersion));
        Assert.Equal(["appendOnly", "clustering", "domainMetadata", "invariants"], detail.TableFeatures);
        Assert.InRange(detail.CreatedAt!.Value, before, DateTimeOffset.UtcNow);
        Assert.InRange(detail.LastModified, detail.CreatedAt.Value.AddSeconds(-1), DateTimeOffset.UtcNow.AddSeconds(1));
        Assert.Equal("No Delta table at " + path + "_none.", Refusal(() => Delta.Detail(path + "_none")));
    });

    // ---- REPLACE TABLE ----

    [Fact]
    public void Replace_gives_a_new_definition_removes_the_rows_and_keeps_history_and_what_Spark_keeps() => InDirectory("replace", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From<Row>(), partitionBy: ["S"], properties: new()
        {
            ["delta.enableDeletionVectors"] = "true", [DeltaTableProperties.EnableChangeDataFeed] = "true", ["foo"] = "1",
            [DeltaTableProperties.EnableRowTracking] = "true",
        });
        Write(path, [new Row(1, "a"), new Row(2, "b")]);
        Delta.AddConstraint(path, "c", "Id > 0");
        Snapshot old = Latest(path);

        Delta.ReplaceTable(path, PolarsSchema.From<Replaced>(), properties: new() { ["bar"] = "2" });
        Snapshot table = Latest(path);
        Assert.Equal(3, table.Version);
        Assert.NotEqual(old.Metadata.Id, table.Metadata.Id);
        Assert.Equal(["K", "V"], table.Schema.Fields.Select(f => f.Name));
        Assert.Empty(table.PartitionColumns);
        Assert.Empty(table.Files);
        Assert.Equal(
            ["bar", "delta.enableDeletionVectors", "delta.enableRowTracking", RowTracking.MaterializedRowCommitVersionKey, RowTracking.MaterializedRowIdKey],
            table.Metadata.Configuration.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(old.Metadata.Configuration[RowTracking.MaterializedRowIdKey], table.Metadata.Configuration[RowTracking.MaterializedRowIdKey]);
        Assert.Equal(old.Protocol.WriterFeatures!.Order(), table.Protocol.WriterFeatures!.Order()); // not lowered
        Assert.Equal("REPLACE TABLE", CommitInfo(path, 3).GetProperty("operation").GetString());
        Assert.Equal(2, Actions(path, 3, "remove").Count);

        // Time travel reads the old definition and rows.
        Assert.Equal([new Row(1, "a"), new Row(2, "b")], Rows<Row>(path, version: 2));
        // The row ids go on after the old ones.
        Write(path, [new Replaced(5, "x")]);
        Assert.True(Latest(path).Files.Single().BaseRowId >= 2);
    });

    [Fact]
    public void Replace_needs_a_table_unless_orCreate() => InDirectory("replacenone", path =>
    {
        Assert.Equal($"No Delta table at {path} to replace; use orCreate to create one.", Refusal(() => Delta.ReplaceTable(path, PolarsSchema.From<Row>())));
        Delta.ReplaceTable(path, PolarsSchema.From<Row>(), orCreate: true);
        Assert.Equal("CREATE OR REPLACE TABLE", CommitInfo(path, 0).GetProperty("operation").GetString());
        Write(path, [new Row(1, "a")]);
        Delta.ReplaceTable(path, PolarsSchema.From<Replaced>(), orCreate: true);
        Assert.Equal("CREATE OR REPLACE TABLE", CommitInfo(path, 2).GetProperty("operation").GetString());
        Assert.Empty(Latest(path).Files);
    });

    [Fact]
    public void A_clustered_table_replaced_unclustered_keeps_an_empty_clustering_domain() => InDirectory("replacecluster", path =>
    {
        Delta.CreateTable(path, PolarsSchema.From<Row>(), clusterBy: ["Id"]);
        Delta.ReplaceTable(path, PolarsSchema.From<Row>());
        Assert.Equal("""{"clusteringColumns":[],"domainName":"delta.clustering"}""", Action(path, 1, "domainMetadata").GetProperty("configuration").GetString());
        Assert.Empty(Delta.Detail(path).ClusteringColumns);
    });

    // ---- SHALLOW CLONE ----

    [Fact]
    public void A_clone_refers_to_the_sources_files_and_lives_on_its_own() => InDirectory("clone", root =>
    {
        string source = Path.Combine(root, "source"), clone = Path.Combine(root, "clone");
        Delta.CreateTable(source, PolarsSchema.From<Row>(), properties: new() { ["delta.enableDeletionVectors"] = "true", ["foo"] = "1" });
        Write(source, [new Row(1, "a"), new Row(2, "b"), new Row(3, "c")]);
        Delta.Delete(source, Col("Id") == 2);
        Delta.Clone(source, clone, properties: new() { ["bar"] = "2" });

        Snapshot table = Latest(clone);
        Assert.Equal(0, table.Version);
        Assert.NotEqual(Latest(source).Metadata.Id, table.Metadata.Id);
        Assert.Equal(("1", "2"), (table.Metadata.Configuration["foo"], table.Metadata.Configuration["bar"]));
        Assert.All(table.Files, f => Assert.StartsWith("file:/" + Path.GetFullPath(source).TrimStart('/'), f.Path, StringComparison.Ordinal));
        Assert.Contains(table.Files, f => f.DeletionVector is { StorageType: "p" });
        JsonElement info = CommitInfo(clone, 0);
        Assert.Equal("CLONE", info.GetProperty("operation").GetString());
        Assert.Equal(($"delta.`{Path.GetFullPath(source)}`", "2"), (Parameter(info, "source"), Parameter(info, "sourceVersion")));
        Assert.Equal([new Row(1, "a"), new Row(3, "c")], Rows<Row>(clone));

        // Its writes stay in it, and its vacuum leaves the source's files alone.
        Write(clone, [new Row(9, "z")]);
        Delta.Delete(clone, Col("Id") == 1);
        Delta.Vacuum(clone, retentionHours: 0, enforceRetention: false);
        Assert.Equal([new Row(3, "c"), new Row(9, "z")], Rows<Row>(clone));
        Assert.Equal([new Row(1, "a"), new Row(3, "c")], Rows<Row>(source));

        // A clone of the clone refers to the original files; a clone of an older version.
        string again = Path.Combine(root, "again"), old = Path.Combine(root, "old");
        Delta.Clone(clone, again);
        Assert.Equal(Rows<Row>(clone), Rows<Row>(again));
        Delta.Clone(source, old, version: 1);
        Assert.Equal([new Row(1, "a"), new Row(2, "b"), new Row(3, "c")], Rows<Row>(old));
    });

    [Fact]
    public void A_clone_needs_an_empty_location_outside_the_source_on_its_store() => InDirectory("clonebad", root =>
    {
        string source = Path.Combine(root, "source");
        Write(source, [new Row(1, "a")]);
        string taken = Path.Combine(root, "taken");
        Directory.CreateDirectory(taken);
        File.WriteAllText(Path.Combine(taken, "x.txt"), "x");
        Assert.Equal($"Cannot clone into {taken}: the location is not empty.", Refusal(() => Delta.Clone(source, taken)));
        Assert.StartsWith("Cannot clone", Refusal(() => Delta.Clone(source, Path.Combine(source, "inner"))));
        Assert.StartsWith("Cannot clone", Refusal(() => Delta.Clone(source, "s3://bucket/clone")));
        Assert.Throws<ArgumentException>(() => Delta.Clone(source, Path.Combine(root, "both"), version: 0, timestamp: DateTimeOffset.UtcNow));
    });

    // ---- CONVERT TO DELTA ----

    private static void Parquet<T>(string file, IEnumerable<T> rows)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(file)!);
        using DataFrame df = DataFrame.From(rows);
        df.WriteParquet(file, statistics: true);
    }

    [Fact]
    public void Convert_lists_the_files_with_their_schema_and_statistics() => InDirectory("convert", path =>
    {
        Parquet(Path.Combine(path, "a.parquet"), [new Row(1, "a")]);
        Parquet(Path.Combine(path, "b.parquet"), [new Wide(2, "b", 5)]);
        Parquet(Path.Combine(path, "_skipped", "c.parquet"), [new Row(3, "c")]);
        Parquet(Path.Combine(path, ".hidden.parquet"), [new Row(4, "d")]);
        Delta.ConvertToDelta(path);

        Snapshot table = Latest(path);
        Assert.Equal(["Id", "S", "N"], table.Schema.Fields.Select(f => f.Name));
        Assert.Equal(["a.parquet", "b.parquet"], table.Files.Select(f => f.Path).Order(StringComparer.Ordinal));
        Assert.All(table.Files, f => Assert.NotNull(f.Stats));
        JsonElement info = CommitInfo(path, 0);
        Assert.Equal("CONVERT", info.GetProperty("operation").GetString());
        Assert.Equal(("2", "[]", "true", "parquet"), (Parameter(info, "numFiles"), Parameter(info, "partitionedBy"), Parameter(info, "collectStats"), Parameter(info, "sourceFormat")));
        Assert.Equal([new Wide(1, "a", null), new Wide(2, "b", 5)], Rows<Wide>(path));

        // Converting a table again does nothing.
        Delta.ConvertToDelta(path);
        Assert.Equal(0, Latest(path).Version);
    });

    [Fact]
    public void Convert_reads_partition_values_from_Hive_directories() => InDirectory("convertparts", path =>
    {
        Parquet(Path.Combine(path, "S=a b", "f.parquet"), [new IdOnly(1)]);
        Parquet(Path.Combine(path, "S=x%2Fy", "f.parquet"), [new IdOnly(2)]);
        Parquet(Path.Combine(path, "S=__HIVE_DEFAULT_PARTITION__", "f.parquet"), [new IdOnly(3)]);
        Delta.ConvertToDelta(path, partitionBy: PolarsSchema.From([("S", DataType.String)]), collectStatistics: false);

        Snapshot table = Latest(path);
        Assert.Equal(["S"], table.PartitionColumns);
        Assert.Equal(["S=__HIVE_DEFAULT_PARTITION__/f.parquet", "S=a%20b/f.parquet", "S=x%252Fy/f.parquet"], table.Files.Select(f => f.Path).Order(StringComparer.Ordinal));
        Assert.All(table.Files, f => Assert.Null(f.Stats));
        Assert.Equal([new Row(1, "a b"), new Row(2, "x/y"), new Row(3, null)], Rows<Row>(path));
    });

    [Fact]
    public void Convert_reads_the_types_as_Spark_reads_them() => InDirectory("converttypes", root =>
    {
        string path = Path.Combine(root, "types");
        Directory.CreateDirectory(path);
        using (DataFrame df = DataFrame.FromSeries(
            Series.From("U8", [(byte)1]), Series.From("U16", [(ushort)1]), Series.From("U32", [uint.MaxValue]),
            Series.From("Local", [new DateTime(2020, 1, 1, 10, 0, 0)])))
            df.WriteParquet(Path.Combine(path, "f.parquet"), statistics: true);
        Delta.ConvertToDelta(path);
        Snapshot table = Latest(path);
        Assert.Equal(
            """{"type":"struct","fields":[{"name":"U8","type":"short","nullable":true,"metadata":{}},{"name":"U16","type":"integer","nullable":true,"metadata":{}},{"name":"U32","type":"long","nullable":true,"metadata":{}},{"name":"Local","type":"timestamp_ntz","nullable":true,"metadata":{}}]}""",
            table.Metadata.SchemaString);
        Assert.Contains(TableFeatures.TimestampNtz, table.Protocol.WriterFeatures!);
        using DataFrame read = DataFrame.ReadDelta(path);
        Assert.Equal((long)uint.MaxValue, read["U32"].ToArray<long>()[0]);

        string unsigned = Path.Combine(root, "unsigned");
        Directory.CreateDirectory(unsigned);
        using (DataFrame df = DataFrame.FromSeries(Series.From("U64", [ulong.MaxValue])))
            df.WriteParquet(Path.Combine(unsigned, "f.parquet"));
        Assert.StartsWith("The column 'U64' of f.parquet is of a type Delta has no counterpart for (UINT64", Refusal(() => Delta.ConvertToDelta(unsigned)));

        string nanos = Path.Combine(root, "nanos");
        Directory.CreateDirectory(nanos);
        using (DataFrame df = DataFrame.FromSeries(Series.From("T", [new DateTime(2020, 1, 1)])).WithColumns(Col("T").Cast(DataType.Datetime(TimeUnit.Nanoseconds))))
            df.WriteParquet(Path.Combine(nanos, "f.parquet"));
        Assert.StartsWith("The column 'T' of f.parquet is of a type Delta has no counterpart for (TIMESTAMP(NANOS))", Refusal(() => Delta.ConvertToDelta(nanos)));
    });

    [Fact]
    public void What_Spark_cannot_convert_is_refused() => InDirectory("convertbad", root =>
    {
        string empty = Path.Combine(root, "empty");
        Directory.CreateDirectory(empty);
        Assert.Equal("No file found in the directory: there is nothing to convert.", Refusal(() => Delta.ConvertToDelta(empty)));

        string conflict = Path.Combine(root, "conflict");
        Parquet(Path.Combine(conflict, "a.parquet"), [new IdOnly(1)]);
        Parquet(Path.Combine(conflict, "b.parquet"), [new IdLong(1)]);
        Assert.StartsWith("Failed to merge the column 'Id': b.parquet holds it as long, earlier files as integer.", Refusal(() => Delta.ConvertToDelta(conflict)));

        string text = Path.Combine(root, "text");
        Parquet(Path.Combine(text, "a.parquet"), [new IdOnly(1)]);
        File.WriteAllText(Path.Combine(text, "notes.txt"), "hi");
        Assert.StartsWith("Could not read the footers of the files", Refusal(() => Delta.ConvertToDelta(text)));

        string parts = Path.Combine(root, "parts");
        Parquet(Path.Combine(parts, "P=1", "a.parquet"), [new IdOnly(1)]);
        Assert.StartsWith("Expecting 0 partition column(s)", Refusal(() => Delta.ConvertToDelta(parts)));
        Assert.StartsWith("The partition value 'x' of", Refusal(() =>
        {
            Parquet(Path.Combine(parts, "P=x", "a.parquet"), [new IdOnly(1)]);
            Delta.ConvertToDelta(parts, partitionBy: PolarsSchema.From([("P", DataType.Int32)]));
        }));
        Assert.False(Directory.Exists(Path.Combine(parts, "_delta_log")) && Directory.EnumerateFiles(Path.Combine(parts, "_delta_log")).Any());
    });
}
