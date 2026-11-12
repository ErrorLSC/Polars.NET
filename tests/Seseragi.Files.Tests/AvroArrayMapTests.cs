using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files.Avro;
using static Seseragi.Files.Tests.AvroEdgeCaseTests;

namespace Seseragi.Files.Tests;

/// <summary>Maps encoded as an array of key/value records with <c>logicalType: map</c> (Iceberg, DuckDB): read as
/// Arrow maps, written for maps whose keys are not strings.</summary>
public class AvroArrayMapTests
{
    // The shape DuckDB 2.0 writes for MAP(INTEGER, VARCHAR): nullable key and value unions, record "record0".
    private const string DuckDbSchema = """
        {"type": "record", "name": "root", "fields": [
          {"name": "m", "type": ["null", {"logicalType": "map", "type": "array", "items": {"type": "record", "name": "record0",
            "fields": [{"name": "key", "type": ["null", {"type": "int"}]}, {"name": "value", "type": ["null", {"type": "string"}]}]}}]}]}
        """;

    // An entry (key branch 1 = int, value branch 1 = string or 0 = null).
    private static Bytes Entry(Bytes b, int key, string? value) =>
        value is null ? b.Long(1).Long(key).Long(0) : b.Long(1).Long(key).Long(1).Str(value);

    [Fact]
    public async Task DuckDb_shaped_map_reads_as_an_arrow_map()
    {
        var rows = new Bytes();
        Entry(Entry(rows.Long(1).Long(2), 1, "a"), 2, null).Long(0); // {1: "a", 2: null}
        rows.Long(0);                                                // null map
        rows.Long(1).Long(0);                                        // empty map
        byte[] file = Container(DuckDbSchema, [(3, rows.ToArray())]);

        await using var reader = await AvroReader.OpenAsync(new MemoryStream(file));
        var map = Assert.IsType<MapType>(reader.Schema.GetFieldByName("m").DataType);
        Assert.Equal((ArrowTypeId.Int32, false), (map.KeyField.DataType.TypeId, map.KeyField.IsNullable));
        Assert.Equal(("key", "value"), (map.KeyField.Name, map.ValueField.Name));
        Assert.True(reader.Schema.GetFieldByName("m").IsNullable);
        Assert.Equal(["{1: \"a\", 2: null}", "null", "{}"], await Read(file));
    }

    [Fact]
    public async Task A_key_union_with_null_last_is_a_map_too()
    {
        string schema = DuckDbSchema.Replace("""["null", {"type": "int"}]""", """[{"type": "int"}, "null"]""");
        var rows = new Bytes().Long(1).Long(1).Long(0).Long(7).Long(1).Str("x").Long(0); // one entry: key branch 0 = 7, value "x"
        byte[] file = Container(schema, [(1, rows.ToArray())]);
        Assert.Equal(["{7: \"x\"}"], await Read(file));
        var bad = new Bytes().Long(1).Long(1).Long(1).Long(1).Str("x").Long(0); // one entry: key branch 1 = null
        await Assert.ThrowsAsync<AvroFormatException>(() => Read(Container(schema, [(1, bad.ToArray())])));
    }

    [Fact]
    public async Task A_null_map_key_is_refused()
    {
        var rows = new Bytes().Long(1).Long(1).Long(0).Long(1).Str("x").Long(0); // key branch 0 = null
        byte[] file = Container(DuckDbSchema, [(1, rows.ToArray())]);
        var e = await Assert.ThrowsAsync<AvroFormatException>(() => Read(file));
        Assert.Contains("map key", e.Message);
    }

    [Fact]
    public async Task Iceberg_shaped_map_keeps_its_ids_and_reads_through_the_row_view()
    {
        const string schema = """
            {"type": "record", "name": "r2", "fields": [
              {"name": "column_sizes", "type": ["null", {"type": "array", "logicalType": "map", "items": {"type": "record",
                "name": "k117_v118", "fields": [{"name": "key", "type": "int", "field-id": 117}, {"name": "value", "type": "long", "field-id": 118}]}}],
               "default": null, "field-id": 108}]}
            """;
        var rows = new Bytes().Long(1).Long(2).Long(1).Long(100).Long(2).Long(200).Long(0); // {1: 100, 2: 200}
        byte[] file = Container(schema, [(1, rows.ToArray())]);

        await using var reader = await AvroReader.OpenAsync(new MemoryStream(file));
        Field field = reader.Schema.GetFieldByName("column_sizes");
        var map = (MapType)field.DataType;
        Assert.Equal((108, 117, 118), (ArrowFieldIds.Of(field), ArrowFieldIds.Of(map.KeyField), ArrowFieldIds.Of(map.ValueField)));
        var array = (AvroArraySchema)((AvroUnionSchema)((AvroRecordSchema)reader.WriterSchema).Fields[0].Schema).Branches[1];
        Assert.Equal(AvroLogicalKind.Map, array.Logical);
        Assert.False(array.Properties.ContainsKey("logicalType"));

        await foreach (RecordBatch batch in reader.ReadBatchesAsync())
        {
            ArrowRows view = ArrowRows.Over(batch, ArrowFieldIds.MetadataKey);
            ArrowMap<int, long> sizes = view[0].GetMap<int, long>(view.OrdinalOfId(108));
            Assert.Equal(200L, sizes[2]);
        }
        AssertSameJson(schema, reader.WriterSchema.ToJson());
    }

    [Theory]
    [InlineData("""{"type": "record", "name": "e", "fields": [{"name": "a", "type": "int"}, {"name": "b", "type": "int"}, {"name": "c", "type": "int"}]}""")]
    [InlineData("\"int\"")]
    [InlineData("""{"type": "record", "name": "e", "fields": [{"name": "k", "type": ["int", "string"]}, {"name": "v", "type": "int"}]}""")]
    public void Logical_map_on_anything_but_key_value_records_is_a_plain_list(string items)
    {
        string json = $$$"""{"type": "record", "name": "r", "fields": [{"name": "x", "type": {"type": "array", "logicalType": "map", "items": {{{items}}}}}]}""";
        var array = (AvroArraySchema)((AvroRecordSchema)AvroSchema.Parse(json)).Fields[0].Schema;
        Assert.Equal(AvroLogicalKind.None, array.Logical);
        Assert.Equal("map", array.Properties["logicalType"].GetString());
        Assert.IsType<ListType>(AvroReadSchema.ToField("x", array).DataType);
        AssertSameJson(json, AvroSchema.Parse(json).ToJson());
    }

    [Fact]
    public async Task Non_string_keys_write_as_an_array_map_and_round_trip()
    {
        var type = new MapType(ArrowFieldIds.With(new Field("key", Int32Type.Default, false), 117),
            ArrowFieldIds.With(new Field("value", StringType.Default, true), 118));
        var schema = new Schema([new Field("m", type, true), new Field("n", Int32Type.Default, false)], null);
        var b = new MapArray.Builder(type);
        var keys = (Int32Array.Builder)b.KeyBuilder;
        var values = (StringArray.Builder)b.ValueBuilder;
        b.Append(); keys.Append(9); values.Append("skipped by the slice");
        b.Append(); keys.Append(1); values.Append("a"); keys.Append(2); values.AppendNull();
        b.AppendNull();
        b.Append();
        b.Append(); keys.Append(-3); values.Append("z");
        var batch = new RecordBatch(schema, [b.Build(), new Int32Array.Builder().AppendRange([0, 1, 2, 3, 4]).Build()], 5);

        var ms = new MemoryStream();
        string written;
        await using (var writer = AvroWriter.Create(ms, schema, new() { LeaveOpen = true }))
        {
            written = writer.WriterSchemaJson;
            await writer.WriteAsync(batch.Slice(1, 4));
            await writer.CompleteAsync();
        }
        using (JsonDocument doc = JsonDocument.Parse(written))
        {
            JsonElement array = doc.RootElement.GetProperty("fields")[0].GetProperty("type")[1];
            Assert.Equal(("array", "map"), (array.GetProperty("type").GetString(), array.GetProperty("logicalType").GetString()));
            JsonElement entry = array.GetProperty("items");
            Assert.Equal("record", entry.GetProperty("type").GetString());
            Assert.Equal("int", entry.GetProperty("fields")[0].GetProperty("type").GetString()); // a required key: no union
            Assert.Equal(117, entry.GetProperty("fields")[0].GetProperty("field-id").GetInt32());
            Assert.Equal(118, entry.GetProperty("fields")[1].GetProperty("field-id").GetInt32());
        }

        ms.Position = 0;
        Assert.Equal(["{1: \"a\", 2: null} | 1", "null | 2", "{} | 3", "{-3: \"z\"} | 4"], await Read(ms.ToArray()));
        await using var reader = await AvroReader.OpenAsync(new MemoryStream(ms.ToArray()));
        var read = (MapType)reader.Schema.GetFieldByName("m").DataType;
        Assert.Equal((117, 118), (ArrowFieldIds.Of(read.KeyField), ArrowFieldIds.Of(read.ValueField)));
    }

    private static void AssertSameJson(string expected, string actual)
    {
        using JsonDocument e = JsonDocument.Parse(expected), a = JsonDocument.Parse(actual);
        Assert.True(JsonElement.DeepEquals(e.RootElement, a.RootElement), $"expected {expected}\nactual   {actual}");
    }
}
