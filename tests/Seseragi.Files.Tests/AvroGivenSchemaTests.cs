using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files.Avro;
using static Kaminari.Seseragi.Files.Avro.AvroField;
using static Seseragi.Files.Tests.TestBatches;
using S = Kaminari.Seseragi.Files.Avro.AvroSchema;

namespace Seseragi.Files.Tests;

/// <summary>Writing Arrow batches as a given Avro schema (<see cref="AvroWriteOptions.WriterSchema"/>).</summary>
public class AvroGivenSchemaTests
{
    private static async Task<(string Json, List<string> Rows)> RoundTrip(RecordBatch batch, AvroSchema schema,
        AvroFieldMatching matching = AvroFieldMatching.ByName)
    {
        var ms = new MemoryStream();
        await using (var writer = AvroWriter.Create(ms, batch.Schema, new() { WriterSchema = schema, FieldMatching = matching, LeaveOpen = true }))
        {
            Assert.Same(schema, writer.WriterSchema);
            await writer.WriteAsync(batch);
            await writer.CompleteAsync();
        }
        await using var reader = await AvroReader.OpenAsync(new MemoryStream(ms.ToArray()));
        List<RecordBatch> batches = [];
        await foreach (var b in reader.ReadBatchesAsync()) batches.Add(b);
        return (reader.WriterSchemaJson, ArrowRender.Rows(batches));
    }

    private static Task Write(RecordBatch batch, AvroSchema schema) => RoundTrip(batch, schema);

    private static Schema WithIds(params (string Name, IArrowType Type, bool Nullable, int Id)[] fields) =>
        new([.. fields.Select(f => ArrowFieldIds.With(new Field(f.Name, f.Type, f.Nullable), f.Id))], null);

    private static Decimal128Array Decimals(Decimal128Type type, params decimal?[] values)
    {
        var b = new Decimal128Array.Builder(type);
        foreach (decimal? v in values)
        {
            if (v is decimal d) b.Append(d);
            else b.AppendNull();
        }
        return b.Build();
    }

    // ---- Iceberg shapes ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task An_iceberg_shaped_schema_is_written_exactly_and_reads_back()
    {
        AvroRecordSchema schema = S.Record("r2",
        [
            Required("content", S.Int, 134, defaultJson: "0"),
            Required("file_path", S.String, 100),
            Required("partition", S.Record("r102",
            [
                Optional("ts", S.TimestampMicros(adjustToUtc: false), 1000),
                Optional("dec", S.DecimalFixed("decimal_9_2", 9, 2), 1001),
                Optional("uid", S.UuidFixed("uuid_fixed"), 1002),
            ]), 102),
            Optional("column_sizes", S.ArrayMap("k117_v118", S.Int, S.Long, 117, 118), 108),
            Optional("key_metadata", S.Bytes, 131),
            Required("spark_order", S.Union(S.Long, S.Null), 999, defaultJson: "0"), // [T, "null"], as Spark writes
        ]);
        Schema arrow = schema.ToArrowSchema();
        Assert.Equal(["content", "file_path", "partition", "column_sizes", "key_metadata", "spark_order"], arrow.FieldsList.Select(f => f.Name));

        // A batch of the exact Arrow types, without content and key_metadata (their defaults are written).
        var partType = (StructType)arrow.GetFieldByName("partition").DataType;
        byte[] uuid = Convert.FromHexString("00112233445566778899aabbccddeeff");
        var partition = Struct(partType,
        [
            Fixed(partType.Fields[0].DataType, [(long?)1_709_296_496_123_456, null]),
            Decimals((Decimal128Type)partType.Fields[1].DataType, 12.34m, -0.01m),
            new FixedSizeBinaryArray(new ArrayData(new FixedSizeBinaryType(16), 2, 1, 0,
                [Validity([true, false]), new ArrowBuffer((byte[])[.. uuid, .. new byte[16]])])),
        ], [true, true]);
        var mapType = (MapType)arrow.GetFieldByName("column_sizes").DataType;
        var sizes = new MapArray.Builder(mapType);
        sizes.Append(); ((Int32Array.Builder)sizes.KeyBuilder).Append(1).Append(2); ((Int64Array.Builder)sizes.ValueBuilder).Append(10).Append(20);
        sizes.AppendNull();
        RecordBatch batch = new(new Schema(
        [
            arrow.GetFieldByName("file_path"), arrow.GetFieldByName("partition"), arrow.GetFieldByName("column_sizes"),
            arrow.GetFieldByName("spark_order"),
        ], null), [Strings(["a.parquet", "b.parquet"]), partition, sizes.Build(), Fixed(Int64Type.Default, [(long?)5, null])], 2);

        var (json, rows) = await RoundTrip(batch, schema);
        using (JsonDocument e = JsonDocument.Parse(schema.ToJson()), a = JsonDocument.Parse(json))
            Assert.True(JsonElement.DeepEquals(e.RootElement, a.RootElement));
        Assert.Equal(
        [
            "0 | \"a.parquet\" | {ts: 1709296496123456, dec: 12.34, uid: 0x00112233445566778899AABBCCDDEEFF} | {1: 10, 2: 20} | null | 5",
            "0 | \"b.parquet\" | {ts: null, dec: -0.01, uid: null} | null | null | null",
        ], rows);
    }

    [Fact]
    public async Task Columns_match_by_field_id_whatever_their_names()
    {
        var schema = S.Record("r", [Required("a", S.Long, 1), Required("b", S.String, 2)]);
        var arrow = WithIds(("second", StringType.Default, false, 2), ("first", Int64Type.Default, false, 1));
        var batch = new RecordBatch(arrow, [Strings(["x"]), Fixed(Int64Type.Default, [(long?)7])], 1);
        Assert.Equal(["7 | \"x\""], (await RoundTrip(batch, schema, AvroFieldMatching.ByFieldId)).Rows);
        await Assert.ThrowsAsync<ArgumentException>(() => RoundTrip(batch, schema)); // by name nothing matches
    }

    // ---- fields -------------------------------------------------------------------------------------------------

    [Fact]
    public async Task Missing_fields_write_their_default_and_extra_columns_are_refused()
    {
        var batch = Batch(("a", Fixed(Int32Type.Default, [(int?)1]), false));
        var withDefaults = S.Record("r", [Required("a", S.Int), Required("seven", S.Int, defaultJson: "7"), Optional("none", S.String)]);
        Assert.Equal(["1 | 7 | null"], (await RoundTrip(batch, withDefaults)).Rows);

        var e = await Assert.ThrowsAsync<ArgumentException>(() => Write(batch, S.Record("r", [Required("a", S.Int), Required("b", S.Int)])));
        Assert.Contains("'b'", e.Message);
        var wide = Batch(("a", Fixed(Int32Type.Default, [(int?)1]), false), ("extra", Fixed(Int32Type.Default, [(int?)2]), false));
        e = await Assert.ThrowsAsync<ArgumentException>(() => Write(wide, S.Record("r", [Required("a", S.Int)])));
        Assert.Contains("'extra'", e.Message);
    }

    [Fact]
    public async Task A_null_in_a_required_field_is_refused_when_written()
    {
        var batch = Batch(("a", Fixed(Int32Type.Default, [(int?)1, null]), true));
        await Assert.ThrowsAsync<NotSupportedException>(() => Write(batch, S.Record("r", [Required("a", S.Int)])));
        Assert.Equal(["1"], (await RoundTrip(Batch(("a", Fixed(Int32Type.Default, [(int?)1]), true)), S.Record("r", [Required("a", S.Int)]))).Rows);
    }

    [Fact]
    public async Task Options_that_conflict_with_a_given_schema_are_refused()
    {
        var batch = Batch(("a", Fixed(Int32Type.Default, [(int?)1]), false));
        var schema = S.Record("r", [Required("a", S.Int)]);
        Assert.Throws<ArgumentException>(() => AvroWriter.Create(new MemoryStream(), batch.Schema, new() { WriterSchema = schema, RecordName = "x" }));
        Assert.Throws<ArgumentException>(() => AvroWriter.Create(new MemoryStream(), batch.Schema, new() { WriterSchema = schema, RecordNamespace = "n" }));
        var general = S.Record("r", [Required("a", S.Union(S.Int, S.String))]);
        Assert.Throws<NotSupportedException>(() => AvroWriter.Create(new MemoryStream(), batch.Schema, new() { WriterSchema = general }));
    }

    // ---- the type matrix ------------------------------------------------------------------------------------------

    public static TheoryData<string, IArrowArray, AvroSchema, string?> Conversions => new()
    {
        // widening, free
        { "int32 → long", Fixed(Int32Type.Default, [(int?)-5]), S.Long, "-5" },
        { "uint16 → int", Fixed(UInt16Type.Default, [(ushort?)65535]), S.Int, "65535" },
        { "float → double", Fixed(FloatType.Default, [(float?)1.5f]), S.Double, "1.5" },
        { "string → bytes", Strings(["ab"]), S.Bytes, "0x6162" },
        { "decimal(9,2) → decimal(12,2) bytes", Decimals(new Decimal128Type(9, 2), 1.25m), S.DecimalBytes(12, 2), "1.25" },
        { "decimal(9,2) → decimal(12,2) fixed", Decimals(new Decimal128Type(9, 2), -1.25m), S.DecimalFixed("d", 12, 2), "-1.25" },
        { "decimal(9,2) → decimal(12,2) fixed(20), sign-extended", Decimals(new Decimal128Type(9, 2), -1.25m), S.DecimalFixed("d", 12, 2, size: 20), "-1.25" },
        { "int64 → timestamp (a plain number takes the reader's meaning)", Fixed(Int64Type.Default, [(long?)1000]), S.TimestampMicros(), "1000" },
        // checked per value, fitting
        { "int64 → int in range", Fixed(Int64Type.Default, [(long?)int.MaxValue]), S.Int, "2147483647" },
        { "double → float, exact", Fixed(DoubleType.Default, [(double?)0.5]), S.Float, "0.5" },
        { "int64 → double, exact", Fixed(Int64Type.Default, [(long?)(1L << 53)]), S.Double, "9007199254740992" },
        { "decimal(12,2) → decimal(4,2), fitting", Decimals(new Decimal128Type(12, 2), 99.99m), S.DecimalBytes(4, 2), "99.99" },
        { "ns → micros, whole", Fixed(new TimestampType(TimeUnit.Nanosecond, "UTC"), [(long?)5_000]), S.TimestampMicros(), "5" },
        // checked per value, refused
        { "int64 → int out of range", Fixed(Int64Type.Default, [(long?)int.MaxValue + 1]), S.Int, null },
        { "double → float, inexact", Fixed(DoubleType.Default, [(double?)0.1]), S.Float, null },
        { "int64 → double, inexact", Fixed(Int64Type.Default, [(long?)((1L << 53) + 1)]), S.Double, null },
        { "uint64 → long beyond range", Fixed(UInt64Type.Default, [(ulong?)ulong.MaxValue]), S.Long, null },
        { "decimal(12,2) → decimal(4,2), too many digits", Decimals(new Decimal128Type(12, 2), 100m), S.DecimalBytes(4, 2), null },
        { "ns → micros, not whole", Fixed(new TimestampType(TimeUnit.Nanosecond, "UTC"), [(long?)5_001]), S.TimestampMicros(), null },
        { "binary → string, not UTF-8", new BinaryArray.Builder().Append((ReadOnlySpan<byte>)[0xFF]).Build(), S.String, null },
        { "binary → fixed(2), 3 bytes", new BinaryArray.Builder().Append((ReadOnlySpan<byte>)[1, 2, 3]).Build(), S.Fixed("f", 2), null },
        { "string → enum, unknown symbol", Strings(["Z"]), S.Enum("E", ["A", "B"]), null },
    };

    [Theory]
    [MemberData(nameof(Conversions))]
    public async Task Types_widen_freely_and_narrow_only_when_the_value_fits(string what, IArrowArray values, AvroSchema type, string? expected)
    {
        var batch = Batch(("v", values, false));
        var schema = S.Record("r", [Required("v", type)]);
        if (expected is null)
        {
            await Assert.ThrowsAsync<NotSupportedException>(() => Write(batch, schema));
            return;
        }
        Assert.True(Assert.Single((await RoundTrip(batch, schema)).Rows) == expected, what);
    }

    public static TheoryData<string, IArrowType, AvroSchema, bool> Meanings => new()
    {
        { "zoned → timestamp-micros", new TimestampType(TimeUnit.Microsecond, "UTC"), S.TimestampMicros(), true },
        { "zoned → timestamp-micros adjust true", new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo"), S.TimestampMicros(adjustToUtc: true), true },
        { "zone-less → adjust false", new TimestampType(TimeUnit.Microsecond, (string?)null), S.TimestampMicros(adjustToUtc: false), true },
        { "zone-less → local-timestamp", new TimestampType(TimeUnit.Millisecond, (string?)null), S.LocalTimestampMicros, true },
        { "zoned → local-timestamp", new TimestampType(TimeUnit.Microsecond, "UTC"), S.LocalTimestampMicros, false },
        { "zoned → adjust false", new TimestampType(TimeUnit.Microsecond, "UTC"), S.TimestampMicros(adjustToUtc: false), false },
        { "zone-less → timestamp-micros", new TimestampType(TimeUnit.Microsecond, (string?)null), S.TimestampMicros(), false },
        { "date → timestamp", Date32Type.Default, S.TimestampMicros(), false },
        { "timestamp → date", new TimestampType(TimeUnit.Microsecond, "UTC"), S.Date, false },
        { "timestamp → long (its stored number)", new TimestampType(TimeUnit.Microsecond, "UTC"), S.Long, true },
        { "decimal scale change", new Decimal128Type(9, 2), S.DecimalBytes(9, 3), false },
        { "string → int", StringType.Default, S.Int, false },
        { "fixed(16) → uuid string", new FixedSizeBinaryType(16), S.Uuid, false },
    };

    [Theory]
    [MemberData(nameof(Meanings))]
    public void Different_meanings_are_refused_before_writing(string what, IArrowType arrowType, AvroSchema type, bool accepted)
    {
        var arrow = new Schema([new Field("v", arrowType, false)], null);
        var options = new AvroWriteOptions { WriterSchema = S.Record("r", [Required("v", type)]) };
        if (accepted) AvroWriter.Create(new MemoryStream(), arrow, options).Dispose();
        else Assert.True(Assert.Throws<ArgumentException>(() => AvroWriter.Create(new MemoryStream(), arrow, options)) is not null, what);
    }

    [Fact]
    public async Task A_failed_write_to_a_path_leaves_no_file()
    {
        string path = Path.Combine(Path.GetTempPath(), $"given-{Guid.NewGuid():N}.avro");
        var batch = Batch(("v", Fixed(DoubleType.Default, [(double?)0.1]), false));
        await Assert.ThrowsAsync<NotSupportedException>(() => AvroWriter.WriteAsync(path, batch.Schema, ((RecordBatch[])[batch]).ToAsyncEnumerable(),
            new() { WriterSchema = S.Record("r", [Required("v", S.Float)]) }));
        Assert.False(System.IO.File.Exists(path));
    }
}
