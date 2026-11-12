using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Delta.Schema;

namespace Seseragi.Lakehouse.Tests;

/// <summary>RENAME COLUMN, DROP COLUMNS and ALTER COLUMN TYPE, checked against what delta-spark 4.4 commits.</summary>
public class AlterColumnTests
{
    private static string F(string name, string type, int id, string physical) =>
        "{\"name\":\"" + name + "\",\"type\":" + type + ",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":" + id +
        ",\"delta.columnMapping.physicalName\":\"" + physical + "\"}}";

    // a INT, b STRING, s STRUCT<x INT, y STRING>, arr ARRAY<STRUCT<f INT, g INT>>, m MAP<STRING, STRUCT<v INT>>,
    // ia ARRAY<INT>, dt DATE, pc STRING (partition column when partitioned), mapped by name.
    private static readonly string MappedSchema = "{\"type\":\"struct\",\"fields\":[" + string.Join(',',
        F("a", "\"integer\"", 1, "col-a"),
        F("b", "\"string\"", 2, "col-b"),
        F("s", "{\"type\":\"struct\",\"fields\":[" + F("x", "\"integer\"", 4, "col-x") + "," + F("y", "\"string\"", 5, "col-y") + "]}", 3, "col-s"),
        F("arr", "{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":[" + F("f", "\"integer\"", 7, "col-f") + "," +
            F("g", "\"integer\"", 8, "col-g") + "]},\"containsNull\":true}", 6, "col-arr"),
        F("m", "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":{\"type\":\"struct\",\"fields\":[" + F("v", "\"integer\"", 10, "col-v") +
            "]},\"valueContainsNull\":true}", 9, "col-m"),
        F("ia", "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true}", 11, "col-ia"),
        F("dt", "\"date\"", 12, "col-dt"),
        F("pc", "\"string\"", 13, "col-pc")) + "]}";

    private static MemoryFileSystem Table(bool partitioned = true, bool mapped = true, bool widening = true, string? statsColumns = "a,b,s.x")
    {
        var fs = new MemoryFileSystem();
        Dictionary<string, string> configuration = [];
        List<string> readerFeatures = [];
        List<string> writerFeatures = ["appendOnly", "invariants"];
        if (mapped)
        {
            configuration["delta.columnMapping.mode"] = "name";
            configuration["delta.columnMapping.maxColumnId"] = "13";
            readerFeatures.Add("columnMapping");
            writerFeatures.Add("columnMapping");
        }
        if (widening)
        {
            configuration["delta.enableTypeWidening"] = "true";
            readerFeatures.Add("typeWidening");
            writerFeatures.Add("typeWidening");
        }
        if (statsColumns is not null) configuration["delta.dataSkippingStatsColumns"] = statsColumns;
        string schema = mapped ? MappedSchema : Unmapped(StructType.Parse(MappedSchema)).ToJson();
        var metadata = new MetadataAction
        {
            Id = "t", SchemaString = schema, Configuration = configuration, PartitionColumns = partitioned ? ["pc"] : [],
        };
        var protocol = new ProtocolAction { MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = readerFeatures, WriterFeatures = writerFeatures };
        Transaction.Create(fs, protocol, metadata).Commit(DateTimeOffset.FromUnixTimeMilliseconds(1_000));
        return fs;
    }

    private static StructType Unmapped(StructType schema) => new([.. schema.Fields.Select(Plain)]);

    private static StructField Plain(StructField f) => new()
    {
        Name = f.Name, Nullable = f.Nullable,
        Type = f.Type switch
        {
            StructType s => Unmapped(s),
            ArrayType { ElementType: StructType e } a => new ArrayType(Unmapped(e), a.ContainsNull),
            MapType { ValueType: StructType v } m => new MapType(m.KeyType, Unmapped(v), m.ValueContainsNull),
            _ => f.Type,
        },
    };

    private static JsonElement CommitInfo(MemoryFileSystem fs, long version)
    {
        string text = Encoding.UTF8.GetString(fs.ReadAll(LogFile.CommitPath(version))!);
        foreach (string line in text.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            using JsonDocument doc = JsonDocument.Parse(line);
            if (doc.RootElement.TryGetProperty("commitInfo", out JsonElement info)) return info.Clone();
        }
        throw new InvalidOperationException("No commitInfo.");
    }

    private static string Parameter(MemoryFileSystem fs, long version, string name) =>
        CommitInfo(fs, version).GetProperty("operationParameters").GetProperty(name).GetString()!;

    private static StructField Field(StructType schema, params string[] path)
    {
        DeltaType type = schema;
        StructField? field = null;
        foreach (string part in path)
        {
            type = type switch { ArrayType a => a.ElementType, MapType m => m.ValueType, _ => type };
            field = ((StructType)type).Field(part) ?? throw new InvalidOperationException($"No field {part}.");
            type = field.Type;
        }
        return field!;
    }

    // ------------------------------------------------------------------ rename

    [Fact]
    public void RenameColumn_KeepsThePhysicalName_AndRenamesWhereSparkDoes()
    {
        MemoryFileSystem fs = Table();
        Assert.Equal(1, AlterTable.RenameColumn(fs, Snapshot.Load(fs), "B", "b2"));
        Assert.Equal("RENAME COLUMN", CommitInfo(fs, 1).GetProperty("operation").GetString());
        Assert.Equal(("b", "b2"), (Parameter(fs, 1, "oldColumnPath"), Parameter(fs, 1, "newColumnPath"))); // as resolved, any case given
        Snapshot table = Snapshot.Load(fs);
        StructField b2 = Field(table.Schema, "b2");
        Assert.Equal(("col-b", 2), (b2.PhysicalName, b2.FieldId));
        Assert.Null(table.Schema.Field("b"));
        Assert.Equal("a,b2,s.x", table.Metadata.Configuration["delta.dataSkippingStatsColumns"]);
        Assert.Equal(["a", "b2", "s", "arr", "m", "ia", "dt", "pc"], table.Schema.Fields.Select(f => f.Name)); // order kept

        // Nested fields; a name with a dot is backticked where Spark backticks it.
        AlterTable.RenameColumn(fs, Snapshot.Load(fs), "s.x", "x.y");
        Assert.Equal(("s.x", "s.`x.y`"), (Parameter(fs, 2, "oldColumnPath"), Parameter(fs, 2, "newColumnPath")));
        Assert.Equal("a,b2,s.`x.y`", Snapshot.Load(fs).Metadata.Configuration["delta.dataSkippingStatsColumns"]);
        // Renaming the struct renames the entries below it; Spark quotes a part that is not a plain name.
        AlterTable.RenameColumn(fs, Snapshot.Load(fs), "s", "s s");
        Assert.Equal(("s", "s s"), (Parameter(fs, 3, "oldColumnPath"), Parameter(fs, 3, "newColumnPath")));
        Assert.Equal("a,b2,`s s`.`x.y`", Snapshot.Load(fs).Metadata.Configuration["delta.dataSkippingStatsColumns"]);
        Assert.Equal("col-x", Field(Snapshot.Load(fs).Schema, "s s", "x.y").PhysicalName);

        // Through arrays and maps.
        AlterTable.RenameColumn(fs, Snapshot.Load(fs), "arr.element.f", "f1");
        Assert.Equal(("arr.element.f", "arr.element.f1"), (Parameter(fs, 4, "oldColumnPath"), Parameter(fs, 4, "newColumnPath")));
        AlterTable.RenameColumn(fs, Snapshot.Load(fs), "m.value.v", "v1");
        table = Snapshot.Load(fs);
        Assert.Equal("col-f", Field(table.Schema, "arr", "f1").PhysicalName);
        Assert.Equal("col-v", Field(table.Schema, "m", "v1").PhysicalName);

        // A partition column: partitionColumns (logical names) follows.
        AlterTable.RenameColumn(fs, table, "pc", "pc2");
        Assert.Equal(["pc2"], Snapshot.Load(fs).PartitionColumns);
        Assert.Equal("13", Snapshot.Load(fs).Metadata.Configuration["delta.columnMapping.maxColumnId"]);
    }

    [Fact]
    public void RenameColumn_RefusesWhatSparkRefuses()
    {
        MemoryFileSystem plain = Table(mapped: false);
        var e = Assert.Throws<DeltaException>(() => AlterTable.RenameColumn(plain, Snapshot.Load(plain), "a", "a2"));
        Assert.Contains("column mapping", e.Message);

        MemoryFileSystem fs = Table();
        Snapshot table = Snapshot.Load(fs);
        Assert.Throws<DeltaException>(() => AlterTable.RenameColumn(fs, table, "a", "B")); // taken, any case
        Assert.Throws<DeltaException>(() => AlterTable.RenameColumn(fs, table, "a", "A")); // Spark: FIELD_ALREADY_EXISTS
        Assert.Throws<DeltaException>(() => AlterTable.RenameColumn(fs, table, "s.x", "Y"));
        Assert.Throws<DeltaException>(() => AlterTable.RenameColumn(fs, table, "nope", "z"));
        Assert.Throws<DeltaException>(() => AlterTable.RenameColumn(fs, table, "arr.f", "z")); // arr is not a struct
        Assert.Throws<DeltaException>(() => AlterTable.RenameColumn(fs, table, "ia.element", "z"));
        Assert.Equal(0, Snapshot.Load(fs).Version);
    }

    // ------------------------------------------------------------------ drop

    [Fact]
    public void DropColumns_InOneCommit_AsSpark()
    {
        MemoryFileSystem fs = Table();
        Assert.Equal(1, AlterTable.DropColumns(fs, Snapshot.Load(fs), ["B", "s.y", "arr.element.g"]));
        Assert.Equal("DROP COLUMNS", CommitInfo(fs, 1).GetProperty("operation").GetString());
        Assert.Equal("""["b","s.y","arr.element.g"]""", Parameter(fs, 1, "columns"));
        Snapshot table = Snapshot.Load(fs);
        Assert.Equal(["a", "s", "arr", "m", "ia", "dt", "pc"], table.Schema.Fields.Select(f => f.Name));
        Assert.Equal(["x"], ((StructType)Field(table.Schema, "s").Type).Fields.Select(f => f.Name));
        Assert.Equal("a,s.x", table.Metadata.Configuration["delta.dataSkippingStatsColumns"]);
        Assert.Equal("13", table.Metadata.Configuration["delta.columnMapping.maxColumnId"]); // ids are never reused

        // Dropping the only named stats column leaves the property empty (Spark): no column statistics.
        AlterTable.DropColumns(fs, table, ["s"]);
        table = Snapshot.Load(fs);
        Assert.Equal("a", table.Metadata.Configuration["delta.dataSkippingStatsColumns"]);
        AlterTable.DropColumns(fs, table, ["a"]);
        Assert.Equal("", Snapshot.Load(fs).Metadata.Configuration["delta.dataSkippingStatsColumns"]);
    }

    [Fact]
    public void DropColumns_RefusesWhatWouldBreakTheTable()
    {
        MemoryFileSystem plain = Table(mapped: false);
        Assert.Contains("column mapping",
            Assert.Throws<DeltaException>(() => AlterTable.DropColumns(plain, Snapshot.Load(plain), ["a"])).Message);

        MemoryFileSystem fs = Table();
        Snapshot table = Snapshot.Load(fs);
        Assert.Contains("partition", Assert.Throws<DeltaException>(() => AlterTable.DropColumns(fs, table, ["pc"])).Message);
        Assert.Throws<DeltaException>(() => AlterTable.DropColumns(fs, table, ["nope"]));
        Assert.Throws<DeltaException>(() => AlterTable.DropColumns(fs, table, ["s", "s.x"])); // s.x is gone with s (Spark: not found)
        Assert.Throws<DeltaException>(() => AlterTable.DropColumns(fs, table, ["s.x", "s.y"])); // would leave s without fields
        Assert.Throws<DeltaException>(() => AlterTable.DropColumns(fs, table, ["m.value.v"]));
        Assert.Throws<DeltaException>(() => AlterTable.DropColumns(fs, table, ["ia.element"]));
        Assert.Equal(0, Snapshot.Load(fs).Version);
        MemoryFileSystem unpartitioned = Table(partitioned: false);
        Assert.Contains("only column", Assert.Throws<DeltaException>(() => AlterTable.DropColumns(
            unpartitioned, Snapshot.Load(unpartitioned), ["a", "b", "s", "arr", "m", "ia", "dt", "pc"])).Message); // no column would remain

        // IF EXISTS: missing columns skipped; nothing to drop, no commit.
        Assert.Null(AlterTable.DropColumns(fs, table, ["nope", "s.nope"], ifExists: true));
        Assert.Equal(1, AlterTable.DropColumns(fs, table, ["nope", "b"], ifExists: true));
        Assert.Equal("""["b"]""", Parameter(fs, 1, "columns"));
    }

    [Fact]
    public void DropColumns_RefusesClusteringColumns_AndStructsHoldingThem()
    {
        MemoryFileSystem fs = Table(partitioned: false);
        AlterTable.ClusterBy(fs, Snapshot.Load(fs), ["a", "s.x"]);
        Snapshot table = Snapshot.Load(fs);
        Assert.Contains("clustering", Assert.Throws<DeltaException>(() => AlterTable.DropColumns(fs, table, ["a"])).Message);
        Assert.Contains("clustering", Assert.Throws<DeltaException>(() => AlterTable.DropColumns(fs, table, ["s.x"])).Message);
        // Spark lets this one through and leaves a table its later ALTERs fail on.
        Assert.Contains("clustering", Assert.Throws<DeltaException>(() => AlterTable.DropColumns(fs, table, ["s"])).Message);
        AlterTable.DropColumns(fs, table, ["s.y", "b"]);

        // A renamed clustering column stays clustering (the domain holds physical names).
        AlterTable.RenameColumn(fs, Snapshot.Load(fs), "s", "s2");
        table = Snapshot.Load(fs);
        Assert.Equal(["a", "s2.x"], Clustering.Fields(table.Schema, Clustering.PhysicalColumns(table.DomainMetadata)!).Select(Clustering.LogicalName));
    }

    // ------------------------------------------------------------------ type

    [Fact]
    public void ChangeColumnType_RecordsTheChangeWhereSparkDoes()
    {
        MemoryFileSystem fs = Table();
        Assert.Equal(1, AlterTable.ChangeColumnType(fs, Snapshot.Load(fs), "A", new PrimitiveType(PrimitiveType.Long)));
        Assert.Equal("CHANGE COLUMN", CommitInfo(fs, 1).GetProperty("operation").GetString());
        Assert.Equal(
            """{"name":"a","type":"long","nullable":true,"metadata":{"delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-a"}}""",
            Parameter(fs, 1, "column"));
        StructField a = Field(Snapshot.Load(fs).Schema, "a");
        Assert.Equal("long", a.Type.ToString());
        Assert.Equal("""[{"toType":"long","fromType":"integer"}]""", a.Metadata["delta.typeChanges"]);

        // An array element: recorded on the array's field with the path to it (Spark).
        AlterTable.ChangeColumnType(fs, Snapshot.Load(fs), "ia.element", new PrimitiveType(PrimitiveType.Long));
        Assert.Equal("""{"name":"ia.element","type":"long","nullable":true,"metadata":{}}""", Parameter(fs, 2, "column"));
        Assert.Equal("""[{"toType":"long","fromType":"integer","fieldPath":"element"}]""", Field(Snapshot.Load(fs).Schema, "ia").Metadata["delta.typeChanges"]);

        // A field of a struct inside an array: on the field itself.
        AlterTable.ChangeColumnType(fs, Snapshot.Load(fs), "arr.element.f", new PrimitiveType(PrimitiveType.Double));
        Assert.Equal("arr.element.f", JsonDocument.Parse(Parameter(fs, 3, "column")).RootElement.GetProperty("name").GetString());
        StructField f = Field(Snapshot.Load(fs).Schema, "arr", "f");
        Assert.Equal(("double", """[{"toType":"double","fromType":"integer"}]"""), (f.Type.ToString(), f.Metadata["delta.typeChanges"]));
        Assert.False(Field(Snapshot.Load(fs).Schema, "arr").Metadata.ContainsKey("delta.typeChanges"));

        // A nested field, then a second change of the same column (history kept).
        AlterTable.ChangeColumnType(fs, Snapshot.Load(fs), "s.x", new PrimitiveType(PrimitiveType.Long));
        Assert.Equal("long", Field(Snapshot.Load(fs).Schema, "s", "x").Type.ToString());
        AlterTable.ChangeColumnType(fs, Snapshot.Load(fs), "a", new DecimalType(25, 2));
        Assert.Equal("""[{"toType":"long","fromType":"integer"},{"toType":"decimal(25,2)","fromType":"long"}]""",
            Field(Snapshot.Load(fs).Schema, "a").Metadata["delta.typeChanges"]);

        // Already that type: no commit.
        long version = Snapshot.Load(fs).Version;
        Assert.Null(AlterTable.ChangeColumnType(fs, Snapshot.Load(fs), "b", new PrimitiveType(PrimitiveType.String)));
        Assert.Equal(version, Snapshot.Load(fs).Version);
    }

    [Fact]
    public void ChangeColumnType_WorksWithoutColumnMapping()
    {
        MemoryFileSystem fs = Table(mapped: false);
        AlterTable.ChangeColumnType(fs, Snapshot.Load(fs), "a", new PrimitiveType(PrimitiveType.Long));
        Assert.Equal("""{"name":"a","type":"long","nullable":true,"metadata":{}}""", Parameter(fs, 1, "column"));
        Assert.Equal("long", Field(Snapshot.Load(fs).Schema, "a").Type.ToString());
    }

    [Fact]
    public void ChangeColumnType_RefusesWhatIsNotAWidening()
    {
        MemoryFileSystem fs = Table();
        Snapshot table = Snapshot.Load(fs);
        var longType = new PrimitiveType(PrimitiveType.Long);
        Assert.Throws<DeltaException>(() => AlterTable.ChangeColumnType(fs, table, "b", longType)); // string → long
        Assert.Throws<DeltaException>(() => AlterTable.ChangeColumnType(fs, table, "a", new PrimitiveType(PrimitiveType.Short))); // narrowing
        Assert.Throws<DeltaException>(() => AlterTable.ChangeColumnType(fs, table, "s", longType)); // a struct
        Assert.Throws<DeltaException>(() => AlterTable.ChangeColumnType(fs, table, "ia", longType));
        Assert.Throws<DeltaException>(() => AlterTable.ChangeColumnType(fs, table, "nope", longType));
        // date → timestamp_ntz is a widening, but needs the timestampNtz feature (Spark: manual enablement).
        Assert.Contains("timestampNtz",
            Assert.Throws<DeltaException>(() => AlterTable.ChangeColumnType(fs, table, "dt", new PrimitiveType(PrimitiveType.TimestampNtz))).Message);
        Assert.Equal(0, Snapshot.Load(fs).Version);

        MemoryFileSystem off = Table(widening: false);
        Assert.Contains("delta.enableTypeWidening",
            Assert.Throws<DeltaException>(() => AlterTable.ChangeColumnType(off, Snapshot.Load(off), "a", longType)).Message);
    }
}
