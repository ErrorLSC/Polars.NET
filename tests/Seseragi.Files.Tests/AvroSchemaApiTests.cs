using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files.Avro;

namespace Seseragi.Files.Tests;

/// <summary>The public schema model: attributes the package does not interpret survive parse → JSON, and ids
/// travel between Avro attributes and Arrow field metadata.</summary>
public class AvroSchemaApiTests
{
    // Shapes taken from Iceberg manifests written by Spark 4.1 / Iceberg 1.12 (partition tuple r102, data_file r2,
    // map-as-array k117_v118), plus a real Avro map with key-id / value-id and attributes we must not lose.
    private const string IcebergLike = """
        {"type": "record", "name": "manifest_entry", "fields": [
          {"name": "status", "type": "int", "field-id": 0},
          {"name": "data_file", "field-id": 2, "type": {"type": "record", "name": "r2", "fields": [
            {"name": "file_path", "type": "string", "doc": "Location URI with FS scheme", "field-id": 100},
            {"name": "partition", "field-id": 102, "type": {"type": "record", "name": "r102", "fields": [
              {"name": "tstz", "type": ["null", {"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": true}], "default": null, "field-id": 1000},
              {"name": "tsntz", "type": ["null", {"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": false}], "default": null, "field-id": 1001},
              {"name": "uid", "type": ["null", {"type": "fixed", "name": "uuid_fixed", "size": 16, "logicalType": "uuid"}], "default": null, "field-id": 1002},
              {"name": "dec", "type": ["null", {"type": "fixed", "name": "decimal_9_2", "size": 4, "logicalType": "decimal", "precision": 9, "scale": 2}], "default": null, "field-id": 1003}
            ]}},
            {"name": "column_sizes", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record", "name": "k117_v118", "fields": [
              {"name": "key", "type": "int", "field-id": 117},
              {"name": "value", "type": "long", "field-id": 118}
            ]}}], "default": null, "field-id": 108},
            {"name": "split_offsets", "type": ["null", {"type": "array", "items": "long", "element-id": 133}], "default": null, "field-id": 132},
            {"name": "props", "type": {"type": "map", "values": "string", "key-id": 7, "value-id": 8}, "field-id": 6, "order": "ignore", "aliases": ["properties"]},
            {"name": "odd", "type": {"type": "long", "logicalType": "not-a-real-type", "x-extra": {"a": [1, 2]}}, "field-id": 9},
            {"name": "baddec", "type": {"type": "bytes", "logicalType": "decimal", "precision": 0}, "field-id": 10},
            {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B"], "doc": "kinds", "aliases": ["K"], "x-enum": 1}, "field-id": 11}
          ]}}
        ]}
        """;

    [Fact]
    public void Parse_then_ToJson_keeps_every_attribute()
    {
        AvroSchema schema = AvroSchema.Parse(IcebergLike);
        AssertSameJson(IcebergLike, schema.ToJson());
    }

    [Fact]
    public void Ids_and_properties_are_exposed()
    {
        var entry = (AvroRecordSchema)AvroSchema.Parse(IcebergLike);
        Assert.Equal(0, entry.Fields[0].FieldId);
        AvroField dataFile = entry.Fields[1];
        Assert.Equal(2, dataFile.FieldId);
        var r2 = (AvroRecordSchema)dataFile.Schema;
        Assert.Equal("Location URI with FS scheme", r2.Fields[0].Doc);

        var partition = (AvroRecordSchema)r2.Fields[1].Schema;
        AvroSchema tstz = ((AvroUnionSchema)partition.Fields[0].Schema).Branches[1];
        Assert.Equal(AvroLogicalKind.TimestampMicros, tstz.Logical);
        Assert.True(tstz.Properties["adjust-to-utc"].GetBoolean());
        Assert.False(((AvroUnionSchema)partition.Fields[1].Schema).Branches[1].Properties["adjust-to-utc"].GetBoolean());
        var uid = (AvroFixedSchema)((AvroUnionSchema)partition.Fields[2].Schema).Branches[1];
        Assert.Equal((AvroLogicalKind.Uuid, 16, "uuid_fixed"), (uid.Logical, uid.Size, uid.Name));
        Assert.Empty(uid.Properties); // logicalType was interpreted

        var columnSizes = (AvroArraySchema)((AvroUnionSchema)r2.Fields[2].Schema).Branches[1];
        Assert.Equal(AvroLogicalKind.Map, columnSizes.Logical); // interpreted, so not kept as a property
        Assert.False(columnSizes.Properties.ContainsKey("logicalType"));
        var kv = (AvroRecordSchema)columnSizes.Items;
        Assert.Equal([117, 118], kv.Fields.Select(f => f.FieldId));

        var splits = (AvroArraySchema)((AvroUnionSchema)r2.Fields[3].Schema).Branches[1];
        Assert.Equal(133, splits.ElementId);
        Assert.Equal(132, r2.Fields[3].FieldId);

        AvroField props = r2.Fields[4];
        var map = (AvroMapSchema)props.Schema;
        Assert.Equal((7, 8), (map.KeyId, map.ValueId));
        Assert.Equal("ignore", props.Properties["order"].GetString());
        Assert.Equal(["properties"], props.Aliases);

        AvroSchema odd = r2.Fields[5].Schema;
        Assert.Equal((AvroType.Long, AvroLogicalKind.None), (odd.Type, odd.Logical));
        Assert.Equal("not-a-real-type", odd.Properties["logicalType"].GetString());
        Assert.Equal(2, odd.Properties["x-extra"].GetProperty("a")[1].GetInt32());

        AvroSchema badDecimal = r2.Fields[6].Schema;
        Assert.Equal(AvroLogicalKind.None, badDecimal.Logical);
        Assert.Equal(0, badDecimal.Properties["precision"].GetInt32());

        var kind = (AvroEnumSchema)r2.Fields[7].Schema;
        Assert.Equal(("kinds", "K", 1), (kind.Doc, kind.Aliases[0], kind.Properties["x-enum"].GetInt32()));
    }

    [Fact]
    public async Task Reading_puts_ids_into_arrow_metadata()
    {
        // Our writer turns Arrow ids into Avro ids; reading turns them back. Nested levels: struct child, list item,
        // map key and value.
        Schema arrow = Arrow(
            Id(new Field("id", Int64Type.Default, false), 1),
            Id(new Field("file", new StructType([Id(new Field("path", StringType.Default, true), 100)]), true), 2),
            Id(new Field("splits", new ListType(Id(new Field("item", Int64Type.Default, true), 133)), true), 132),
            Id(new Field("props", new MapType(Id(new Field("key", StringType.Default, false), 7),
                Id(new Field("value", StringType.Default, true), 8)), true), 6),
            new Field("plain", Int32Type.Default, true));

        var ms = new MemoryStream();
        string written;
        await using (var writer = AvroWriter.Create(ms, arrow, new() { LeaveOpen = true }))
        {
            written = writer.WriterSchemaJson;
            await writer.CompleteAsync();
        }
        using (JsonDocument doc = JsonDocument.Parse(written))
        {
            JsonElement fields = doc.RootElement.GetProperty("fields");
            Assert.Equal(1, fields[0].GetProperty("field-id").GetInt32());
            Assert.Equal(100, fields[1].GetProperty("type")[1].GetProperty("fields")[0].GetProperty("field-id").GetInt32());
            Assert.Equal(133, fields[2].GetProperty("type")[1].GetProperty("element-id").GetInt32());
            Assert.Equal(7, fields[3].GetProperty("type")[1].GetProperty("key-id").GetInt32());
            Assert.Equal(8, fields[3].GetProperty("type")[1].GetProperty("value-id").GetInt32());
            Assert.False(fields[4].TryGetProperty("field-id", out _));
        }

        ms.Position = 0;
        await using var reader = await AvroReader.OpenAsync(ms);
        Schema read = reader.Schema;
        Assert.Equal(1, ArrowFieldIds.Of(read.GetFieldByName("id")));
        Assert.Equal(100, ArrowFieldIds.Of(((StructType)read.GetFieldByName("file").DataType).Fields[0]));
        Assert.Equal(133, ArrowFieldIds.Of(((ListType)read.GetFieldByName("splits").DataType).ValueField));
        var map = (MapType)read.GetFieldByName("props").DataType;
        Assert.Equal((7, 8), (ArrowFieldIds.Of(map.KeyField), ArrowFieldIds.Of(map.ValueField)));
        Assert.Null(ArrowFieldIds.Of(read.GetFieldByName("plain")));
        Assert.Equal(132, ((AvroRecordSchema)reader.WriterSchema).Fields[2].FieldId);
    }

    [Fact]
    public async Task Ids_reach_the_row_view()
    {
        Schema arrow = Arrow(
            Id(new Field("n", Int64Type.Default, false), 5),
            Id(new Field("file", new StructType([Id(new Field("path", StringType.Default, true), 100)]), false), 2));
        var paths = new StringArray.Builder().Append("a.parquet").Append("b.parquet").Build();
        var file = new StructArray(arrow.FieldsList[1].DataType, 2, [paths], ArrowBuffer.Empty, 0, 0);
        RecordBatch batch = new(arrow, [new Int64Array.Builder().Append(10).Append(20).Build(), file], 2);

        var ms = new MemoryStream();
        await AvroWriter.WriteAsync(ms, arrow, AsAsync(batch), new() { LeaveOpen = true });
        ms.Position = 0;
        await using var reader = await AvroReader.OpenAsync(ms);
        List<string> seen = [];
        await foreach (RecordBatch b in reader.ReadBatchesAsync())
        {
            ArrowRows rows = ArrowRows.Over(b, ArrowFieldIds.MetadataKey);
            foreach (ArrowRow row in rows)
            {
                ArrowRow f = row.GetStruct(rows.OrdinalOfId(2));
                seen.Add($"{row.Get<long>(rows.OrdinalOfId(5))}:{f.Get<string>(f.Rows.OrdinalOfId(100))}");
            }
        }
        Assert.Equal(["10:a.parquet", "20:b.parquet"], seen);
    }

    [Fact]
    public async Task Files_without_ids_have_no_metadata()
    {
        var (schema, _) = await AvroReadTests.ReadAll("complex");
        Assert.All(schema.FieldsList, f => Assert.False(f.HasMetadata));
    }

    private static Field Id(Field field, int id) => ArrowFieldIds.With(field, id);

    private static Schema Arrow(params Field[] fields) => new(fields, []);

    private static async IAsyncEnumerable<RecordBatch> AsAsync(RecordBatch batch)
    {
        await Task.Yield();
        yield return batch;
    }

    private static void AssertSameJson(string expected, string actual)
    {
        using JsonDocument e = JsonDocument.Parse(expected), a = JsonDocument.Parse(actual);
        Assert.True(JsonElement.DeepEquals(e.RootElement, a.RootElement), $"expected {expected}\nactual   {actual}");
    }
}
