using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Avro;
using static Seseragi.Files.Tests.AvroEdgeCaseTests;

namespace Seseragi.Files.Tests;

/// <summary>Iceberg's <c>adjust-to-utc</c> attribute: <c>timestamp-*</c> with <c>false</c> is a wall-clock
/// timestamp (no time zone), with <c>true</c> or without the attribute an instant in UTC.</summary>
public class AvroAdjustToUtcTests
{
    private const long Micros = 1_709_296_496_123_456; // 2024-03-01T12:34:56.123456

    private static string Schema(string type) =>
        $$$"""{"type": "record", "name": "r", "fields": [{"name": "t", "type": {{{type}}}}]}""";

    private static async Task<(Field Field, long Value)> ReadOne(string type)
    {
        byte[] file = Container(Schema(type), [(1, new Bytes().Long(Micros).ToArray())]);
        await using var reader = await AvroReader.OpenAsync(new MemoryStream(file));
        Field field = reader.Schema.FieldsList[0];
        long value = 0;
        await foreach (RecordBatch b in reader.ReadBatchesAsync()) value = ((TimestampArray)b.Column(0)).Values[0];
        return (field, value);
    }

    [Theory]
    [InlineData("""{"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": false}""", null)]
    [InlineData("""{"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": true}""", "UTC")]
    [InlineData("""{"type": "long", "logicalType": "timestamp-micros"}""", "UTC")]
    // Only a JSON boolean counts.
    [InlineData("""{"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": "false"}""", "UTC")]
    // A wall-clock timestamp has no zone, whatever kaminari.timezone says.
    [InlineData("""{"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": false, "kaminari.timezone": "Asia/Tokyo"}""", null)]
    [InlineData("""{"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": true, "kaminari.timezone": "Asia/Tokyo"}""", "Asia/Tokyo")]
    // local-timestamp-* is wall-clock already; the attribute changes nothing.
    [InlineData("""{"type": "long", "logicalType": "local-timestamp-micros", "adjust-to-utc": true}""", null)]
    public async Task Adjust_to_utc_decides_the_time_zone(string type, string? zone)
    {
        var (field, value) = await ReadOne(type);
        var ts = Assert.IsType<TimestampType>(field.DataType);
        Assert.Equal((TimeUnit.Microsecond, zone), (ts.Unit, ts.Timezone));
        Assert.Equal(Micros, value); // the stored value is never shifted
        // The attribute is kept as written.
        AssertSameJson(Schema(type), AvroSchema.Parse(Schema(type)).ToJson());
    }

    [Fact]
    public void The_attribute_elsewhere_is_ignored_and_kept()
    {
        string json = Schema("""{"type": "long", "adjust-to-utc": false}""");
        var t = ((AvroRecordSchema)AvroSchema.Parse(json)).Fields[0].Schema;
        Assert.Equal((AvroType.Long, AvroLogicalKind.None, (bool?)false), (t.Type, t.Logical, t.AdjustToUtc));
        Assert.IsType<Int64Type>(AvroReadSchema.ToField("t", t).DataType);
        AssertSameJson(json, AvroSchema.Parse(json).ToJson());
    }

    [Fact]
    public async Task A_wall_clock_timestamp_is_written_back_as_local_timestamp()
    {
        // The default Arrow → Avro mapping writes zone-less timestamps as local-timestamp-* (the Avro standard);
        // reproducing Iceberg's exact schema is the job of writing against a given schema.
        byte[] file = Container(Schema("""{"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": false}"""),
            [(1, new Bytes().Long(Micros).ToArray())]);
        await using var reader = await AvroReader.OpenAsync(new MemoryStream(file));
        List<RecordBatch> batches = [];
        await foreach (RecordBatch b in reader.ReadBatchesAsync()) batches.Add(b);

        var ms = new MemoryStream();
        string written;
        await using (var writer = AvroWriter.Create(ms, reader.Schema, new() { LeaveOpen = true }))
        {
            written = writer.WriterSchemaJson;
            foreach (var b in batches) await writer.WriteAsync(b);
            await writer.CompleteAsync();
        }
        using JsonDocument doc = JsonDocument.Parse(written);
        Assert.Equal("local-timestamp-micros", doc.RootElement.GetProperty("fields")[0].GetProperty("type").GetProperty("logicalType").GetString());
        ms.Position = 0;
        await using var back = await AvroReader.OpenAsync(ms);
        await foreach (RecordBatch b in back.ReadBatchesAsync()) Assert.Equal(Micros, ((TimestampArray)b.Column(0)).Values[0]);
    }

    private static void AssertSameJson(string expected, string actual)
    {
        using JsonDocument e = JsonDocument.Parse(expected), a = JsonDocument.Parse(actual);
        Assert.True(JsonElement.DeepEquals(e.RootElement, a.RootElement), $"expected {expected}\nactual   {actual}");
    }
}
