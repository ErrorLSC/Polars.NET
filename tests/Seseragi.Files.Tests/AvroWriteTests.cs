using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Scalars;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Avro;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests;

public class AvroWriteTests
{
    internal static readonly StructType PointType = new([new Field("a", Int32Type.Default, false), new Field("b", StringType.Default, true)]);
    internal static readonly MapType TagsType = new(new Field("key", StringType.Default, false), new Field("value", Int64Type.Default, true));
    internal static readonly ListType IntListType = new(new Field("item", Int32Type.Default, true));

    /// <summary>A batch with one column per supported type; row k's values derive from k, with nulls sprinkled in.</summary>
    internal static RecordBatch Wide(int k0, int n)
    {
        int[] ks = [.. Enumerable.Range(k0, n)];
        T? Maybe<T>(int k, T v) where T : struct => k % 5 == 0 ? null : v;

        var listOffsets = new int[n + 1];
        List<int?> listValues = [];
        for (int i = 0; i < n; i++)
        {
            for (int j = 0; j < ks[i] % 4; j++) listValues.Add(j == 1 ? null : ks[i] * 10 + j);
            listOffsets[i + 1] = listValues.Count;
        }
        var mapOffsets = new int[n + 1];
        List<string?> mapKeys = [];
        List<long?> mapValues = [];
        for (int i = 0; i < n; i++)
        {
            for (int j = 0; j < ks[i] % 3; j++)
            {
                mapKeys.Add($"k{j}");
                mapValues.Add(j == 2 ? null : ks[i] + j);
            }
            mapOffsets[i + 1] = mapKeys.Count;
        }
        var entries = Struct(new StructType([TagsType.KeyField, TagsType.ValueField]),
            [Strings([.. mapKeys]), Fixed(Int64Type.Default, mapValues.ToArray())], [.. mapKeys.Select(_ => true)]);
        var dictValues = Strings(["red", "green", "blue"]);
        var dictType = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var indices = (Int32Array)Fixed(Int32Type.Default, ks.Select(k => Maybe(k, k % 3)).ToArray());
        var dict = new DictionaryArray(dictType, indices, dictValues);

        var lstr = new LargeStringArray.Builder();
        foreach (int k in ks) lstr.Append($"large{k}");
        var view = new StringViewArray.Builder();
        foreach (int k in ks)
            if (k % 7 == 0) view.AppendNull();
            else view.Append(k % 2 == 0 ? $"v{k}" : $"a longer view string that is not inlined {k}");

        return Batch(
            ("i8", Fixed(Int8Type.Default, ks.Select(k => Maybe(k, (sbyte)(k % 256 - 128))).ToArray()), true),
            ("i16", Fixed(Int16Type.Default, ks.Select(k => (short?)(k - 30000)).ToArray()), false),
            ("i32", Fixed(Int32Type.Default, ks.Select(k => (int?)(k * 7 - 1)).ToArray()), false),
            ("i64", Fixed(Int64Type.Default, ks.Select(k => Maybe(k, (long)k * 1_000_000_000_007)).ToArray()), true),
            ("u8", Fixed(UInt8Type.Default, ks.Select(k => (byte?)(k % 256)).ToArray()), false),
            ("u16", Fixed(UInt16Type.Default, ks.Select(k => (ushort?)(k * 3)).ToArray()), false),
            ("u32", Fixed(UInt32Type.Default, ks.Select(k => (uint?)(uint.MaxValue - (uint)k)).ToArray()), false),
            ("u64", Fixed(UInt64Type.Default, ks.Select(k => (ulong?)((ulong)long.MaxValue - (ulong)k)).ToArray()), false),
            ("f16", Fixed(HalfFloatType.Default, ks.Select(k => (Half?)(k / 8.0)).ToArray()), false),
            ("f32", Fixed(FloatType.Default, ks.Select(k => Maybe(k, k / 3f)).ToArray()), true),
            ("f64", Fixed(DoubleType.Default, ks.Select(k => (double?)(k / 7.0)).ToArray()), false),
            ("b", new BooleanArray.Builder().AppendRange(ks.Select(k => k % 3 == 0)).Build(), false),
            ("s", Strings([.. ks.Select(k => k % 4 == 0 ? null : $"s{k}✓")]), true),
            ("ls", lstr.Build(), false),
            ("sv", view.Build(), true),
            ("bin", BinaryOf(ks), false),
            ("fsb", Fixed(new FixedSizeBinaryType(4), ks.Select(k => (int?)k).ToArray()), false),
            ("dec", new Decimal128Array.Builder(new Decimal128Type(12, 3)).AppendRange(ks.Select(k => (k - 500) * 1.125m)).Build(), false),
            ("dec256", new Decimal256Array.Builder(new Decimal256Type(60, 4)).AppendRange(ks.Select(k => (k - 500) * 12345.6789m)).Build(), false),
            ("date", Fixed(Date32Type.Default, ks.Select(k => (int?)(k - 1000)).ToArray()), false),
            ("tms", Fixed(TimeType.Millisecond, ks.Select(k => (int?)(k * 1000 + 7)).ToArray()), false),
            ("tus", Fixed(TimeType.Microsecond, ks.Select(k => (long?)(k * 1_000_000L + 7)).ToArray()), false),
            ("ts_utc", Fixed(new TimestampType(TimeUnit.Microsecond, "UTC"), ks.Select(k => (long?)(1_600_000_000_000_000L + k)).ToArray()), false),
            ("ts_ms", Fixed(new TimestampType(TimeUnit.Millisecond, "UTC"), ks.Select(k => (long?)(1_600_000_000_000L - k)).ToArray()), false),
            ("ts_ns_local", Fixed(new TimestampType(TimeUnit.Nanosecond, (string?)null), ks.Select(k => Maybe(k, 1_600_000_000_000_000_000L + k)).ToArray()), true),
            ("dur", Fixed(DurationType.Microsecond, ks.Select(k => (long?)(k * -1000L)).ToArray()), false),
            ("list", List(IntListType, listOffsets, Fixed(Int32Type.Default, listValues.ToArray()), [.. ks.Select(k => k % 6 != 0)]), true),
            ("point", Struct(PointType, [Fixed(Int32Type.Default, ks.Select(k => (int?)k).ToArray()), Strings([.. ks.Select(k => k % 2 == 0 ? null : $"p{k}")])], [.. ks.Select(k => k % 9 != 0)]), true),
            ("tags", List(TagsType, mapOffsets, entries, [.. ks.Select(_ => true)]), false),
            ("color", dict, true));
    }

    private static IArrowArray BinaryOf(int[] ks)
    {
        var b = new BinaryArray.Builder();
        foreach (int k in ks) b.Append([(byte)k, (byte)(k >> 8)]);
        return b.Build();
    }

    private static async Task<(List<RecordBatch> Batches, AvroReader Reader)> RoundTrip(IEnumerable<RecordBatch> batches, Schema schema, AvroWriteOptions options, AvroReadOptions? read = null)
    {
        var ms = new MemoryStream();
        await AvroWriter.WriteAsync(ms, schema, batches.ToAsyncEnumerable(), options with { LeaveOpen = true });
        ms.Position = 0;
        var reader = await AvroReader.OpenAsync(ms, read);
        List<RecordBatch> result = [];
        await foreach (var b in reader.ReadBatchesAsync()) result.Add(b);
        return (result, reader);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("deflate")]
    [InlineData("snappy")]
    [InlineData("zstandard")]
    public async Task Round_trip_every_type_through_many_blocks(string codec)
    {
        IAvroCompression compression = codec switch
        {
            "null" => NoCompression.Instance,
            "deflate" => new DeflateCompression(),
            "snappy" => SnappyCompression.Instance,
            _ => new ZstandardCompression(),
        };
        RecordBatch[] input = [Wide(0, 700), Wide(700, 1), Wide(701, 1299)];
        var (output, reader) = await RoundTrip(input, input[0].Schema, new() { Compression = compression, BlockSize = 4096 }, new() { BatchSize = 500 });
        await using var _ = reader;
        Assert.Equal(ArrowRender.Rows(input), ArrowRender.Rows(output));
        Assert.Equal([500, 500, 500, 500], output.Select(b => b.Length));

        IArrowType T(string n) => reader.Schema.GetFieldByName(n).DataType;
        Assert.IsType<Int32Type>(T("i8"));
        Assert.IsType<Int64Type>(T("u32"));
        Assert.IsType<FloatType>(T("f16"));
        Assert.IsType<StringType>(T("ls"));
        Assert.IsType<StringType>(T("sv"));
        Assert.IsType<StringType>(T("color"));
        Assert.Equal("UTC", ((TimestampType)T("ts_utc")).Timezone);
        Assert.Null(((TimestampType)T("ts_ns_local")).Timezone);
        Assert.Equal(TimeUnit.Microsecond, ((DurationType)T("dur")).Unit);
        Assert.IsType<Decimal256Type>(T("dec256"));
        Assert.Equal(4, ((FixedSizeBinaryType)T("fsb")).ByteWidth);
        Assert.IsType<MapType>(T("tags"));
        Assert.True(reader.Schema.GetFieldByName("i8").IsNullable);
        Assert.False(reader.Schema.GetFieldByName("i32").IsNullable);
        Assert.Equal(codec, reader.Codec);
    }

    [Fact]
    public void Writer_schema_names_and_unions_follow_the_rules()
    {
        var batch = Wide(0, 3);
        using var writer = AvroWriter.Create(new MemoryStream(), batch.Schema);
        using var doc = JsonDocument.Parse(writer.WriterSchemaJson);
        var root = doc.RootElement;
        Assert.Equal("topLevelRecord", root.GetProperty("name").GetString());
        JsonElement Type(string field) => root.GetProperty("fields").EnumerateArray().First(f => f.GetProperty("name").GetString() == field).GetProperty("type");

        Assert.Equal("""["null","int"]""", Type("i8").GetRawText());
        Assert.Equal("\"int\"", Type("i32").GetRawText());
        Assert.Equal("\"long\"", Type("u32").GetRawText());
        Assert.Equal("""{"type":"bytes","logicalType":"decimal","precision":12,"scale":3}""", Type("dec").GetRawText());
        Assert.Equal("""{"type":"long","logicalType":"timestamp-micros"}""", Type("ts_utc").GetRawText());
        Assert.Equal("""["null",{"type":"long","logicalType":"local-timestamp-nanos"}]""", Type("ts_ns_local").GetRawText());
        Assert.Equal("""{"type":"long","spark.sql.catalyst.type":"interval day to second"}""", Type("dur").GetRawText());
        Assert.Equal("""{"type":"fixed","name":"fsb","namespace":"topLevelRecord","size":4}""", Type("fsb").GetRawText());
        Assert.Equal("""["null",{"type":"record","name":"point","namespace":"topLevelRecord","fields":[{"name":"a","type":"int"},{"name":"b","type":["null","string"]}]}]""", Type("point").GetRawText());
        Assert.Equal("""{"type":"map","values":["null","long"]}""", Type("tags").GetRawText());
        Assert.Equal("""["null",{"type":"array","items":["null","int"]}]""", Type("list").GetRawText());
        Assert.Equal("""["null","string"]""", Type("color").GetRawText());
    }

    [Fact]
    public async Task Sliced_batches_write_only_their_rows()
    {
        var full = Wide(0, 100);
        var slice = full.Slice(37, 20);
        var (output, reader) = await RoundTrip([slice], full.Schema, new());
        await using var _ = reader;
        Assert.Equal(ArrowRender.Rows(full).Skip(37).Take(20), ArrowRender.Rows(output));
    }

    [Fact]
    public async Task Empty_input_writes_a_valid_file()
    {
        var schema = Wide(0, 1).Schema;
        var (output, reader) = await RoundTrip([], schema, new() { Compression = SnappyCompression.Instance });
        await using var _ = reader;
        Assert.Empty(output);
        Assert.Equal(schema.FieldsList.Count, reader.Schema.FieldsList.Count);
    }

    [Fact]
    public async Task Projection_and_limit_on_written_file()
    {
        var input = Wide(0, 300);
        var (output, reader) = await RoundTrip([input], input.Schema, new() { BlockSize = 512 }, new() { Columns = ["point", "s"], RowLimit = 123 });
        await using var _ = reader;
        var expected = ArrowRender.Rows(new RecordBatch(new Schema([input.Schema.GetFieldByName("s"), input.Schema.GetFieldByName("point")], null),
            [input.Column("s"), input.Column("point")], 300)).Take(123);
        Assert.Equal(expected, ArrowRender.Rows(output));
    }

    [Fact]
    public async Task Metadata_and_record_name_are_written()
    {
        var input = Wide(0, 2);
        var options = new AvroWriteOptions
        {
            RecordName = "Row",
            RecordNamespace = "com.example",
            Metadata = new Dictionary<string, byte[]> { ["origin"] = "test"u8.ToArray() },
        };
        var (_, reader) = await RoundTrip([input], input.Schema, options);
        await using var _ = reader;
        Assert.Equal("test"u8.ToArray(), reader.Metadata["origin"]);
        using var doc = JsonDocument.Parse(reader.WriterSchemaJson);
        Assert.Equal("Row", doc.RootElement.GetProperty("name").GetString());
        Assert.Equal("com.example", doc.RootElement.GetProperty("namespace").GetString());
        Assert.Throws<ArgumentException>(() => AvroWriter.Create(new MemoryStream(), input.Schema,
            new() { Metadata = new Dictionary<string, byte[]> { ["avro.x"] = [] } }));
        Assert.Throws<ArgumentException>(() => AvroWriter.Create(new MemoryStream(), input.Schema, new() { RecordName = "1bad" }));
    }

    public static TheoryData<string, IArrowArray> LossyValues => new()
    {
        { "UInt64 beyond long", Fixed(UInt64Type.Default, [(ulong?)long.MaxValue + 1]) },
        { "Time64 ns below micros", Fixed(TimeType.Nanosecond, [(long?)1_001]) },
        { "Duration ns below micros", Fixed(DurationType.Nanosecond, [(long?)1_500]) },
        { "Date64 not at midnight", Fixed(Date64Type.Default, [(long?)86_400_001]) },
        { "Timestamp seconds overflow", Fixed(new TimestampType(TimeUnit.Second, "UTC"), [(long?)long.MaxValue / 10]) },
        { "Negative day-time interval", Fixed(IntervalType.DayTime, [(long?)-1]) },
    };

    [Theory]
    [MemberData(nameof(LossyValues))]
    public void Values_avro_cannot_hold_are_refused(string _, IArrowArray column)
    {
        var batch = Batch(("c", column, false));
        using var writer = AvroWriter.Create(new MemoryStream(), batch.Schema);
        Assert.Throws<NotSupportedException>(() => writer.Write(batch));
    }

    [Fact]
    public void Exact_conversions_are_accepted()
    {
        var batch = Batch(
            ("t", Fixed(TimeType.Nanosecond, [(long?)5_000]), false),
            ("d", Fixed(DurationType.Nanosecond, [(long?)-3_000]), false),
            ("d64", Fixed(Date64Type.Default, [(long?)(86_400_000L * 3)]), false),
            ("ts", Fixed(new TimestampType(TimeUnit.Second, (string?)null), [(long?)12]), false),
            ("t32s", Fixed(TimeType.Second, [(int?)61]), false));
        var ms = new MemoryStream();
        AvroWriter.Write(ms, batch.Schema, [batch], new() { LeaveOpen = true });
        ms.Position = 0;
        using var reader = AvroReader.Open(ms);
        var b = reader.ReadBatches().Single();
        Assert.Equal(5L, ((Time64Array)b.Column("t")).Values[0]);
        Assert.Equal(-3L, ((DurationArray)b.Column("d")).Values[0]);
        Assert.Equal(3, ((Date32Array)b.Column("d64")).Values[0]);
        Assert.Equal(12_000L, ((TimestampArray)b.Column("ts")).Values[0]);
        Assert.Equal(61_000, ((Time32Array)b.Column("t32s")).Values[0]);
    }

    [Fact]
    public void Map_with_non_string_keys_is_refused()
    {
        var type = new MapType(new Field("key", Int32Type.Default, false), new Field("value", Int32Type.Default, true));
        var schema = new Schema([new Field("m", type, true)], null);
        Assert.Throws<NotSupportedException>(() => AvroWriter.Create(new MemoryStream(), schema));
    }

    [Fact]
    public void Batch_schema_mismatch_is_refused()
    {
        var a = Batch(("c", Fixed(Int32Type.Default, [(int?)1]), false));
        var b = Batch(("c", Strings(["x"]), false));
        using var writer = AvroWriter.Create(new MemoryStream(), a.Schema);
        Assert.Throws<ArgumentException>(() => writer.Write(b));
    }

    [Fact]
    public async Task Intervals_round_trip_as_avro_duration()
    {
        var mdn = new MonthDayNanosecondIntervalArray.Builder()
            .Append(new MonthDayNanosecondInterval(1, 2, 3_000_000)).AppendNull().Build();
        var ym = Fixed(IntervalType.YearMonth, [(int?)14, null]);
        var batch = Batch(("mdn", mdn, true), ("ym", ym, true));
        var (output, reader) = await RoundTrip([batch], batch.Schema, new());
        await using var _ = reader;
        Assert.Equal(ArrowRender.Rows(batch), ArrowRender.Rows(output));
        Assert.Equal(IntervalUnit.MonthDayNanosecond, ((IntervalType)reader.Schema.GetFieldByName("mdn").DataType).Unit);
        Assert.Equal(IntervalUnit.YearMonth, ((IntervalType)reader.Schema.GetFieldByName("ym").DataType).Unit);
    }
}

public class AvroWriteSafetyTests
{
    private static RecordBatch Ints(int n) =>
        Batch(("c", Fixed(Int64Type.Default, [.. Enumerable.Range(0, n).Select(i => (long?)i)]), false));

    public static TheoryData<string> CodecLevels => ["deflate:0", "deflate:1", "deflate:6", "deflate:9", "snappy", "zstandard:1", "zstandard:19", "null"];

    private static IAvroCompression Codec(string spec) => spec.Split(':') switch
    {
        ["deflate", var level] => new DeflateCompression(int.Parse(level)),
        ["zstandard", var level] => new ZstandardCompression(int.Parse(level)),
        ["snappy"] => SnappyCompression.Instance,
        _ => NoCompression.Instance,
    };

    [Theory]
    [MemberData(nameof(CodecLevels))]
    public async Task Incompressible_blocks_round_trip(string codec)
    {
        var random = new Random(42);
        var b = new BinaryArray.Builder();
        for (int i = 0; i < 40; i++)
        {
            var bytes = new byte[64 * 1024];
            random.NextBytes(bytes);
            b.Append(bytes);
        }
        var batch = Batch(("r", b.Build(), false));
        var ms = new MemoryStream();
        await AvroWriter.WriteAsync(ms, batch.Schema, ((RecordBatch[])[batch]).ToAsyncEnumerable(),
            new() { Compression = Codec(codec), BlockSize = 2 << 20, LeaveOpen = true });
        ms.Position = 0;
        await using var reader = await AvroReader.OpenAsync(ms);
        Assert.Equal(ArrowRender.Rows(batch), ArrowRender.Rows(await reader.ReadBatchesAsync().ToListAsync()));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(10)]
    public void Deflate_level_out_of_range_is_refused(int level) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new DeflateCompression(level));

    [Theory]
    [InlineData(0)]
    [InlineData(23)]
    public void Zstandard_level_out_of_range_is_refused(int level) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new ZstandardCompression(level));

    [Fact]
    public void Codecs_compare_by_value()
    {
        Assert.Equal(new ZstandardCompression(9), new ZstandardCompression(9));
        Assert.NotEqual(new ZstandardCompression(9), new ZstandardCompression(3));
        Assert.Equal(new DeflateCompression(), new DeflateCompression(6));
    }

    [Fact]
    public void Batch_with_a_different_timestamp_unit_is_refused()
    {
        var schema = new Schema([new Field("t", new TimestampType(TimeUnit.Millisecond, "UTC"), false)], null);
        var batch = Batch(("t", Fixed(new TimestampType(TimeUnit.Nanosecond, "UTC"), [(long?)1]), false));
        using var writer = AvroWriter.Create(new MemoryStream(), schema);
        Assert.Throws<ArgumentException>(() => writer.Write(batch));
    }

    [Fact]
    public void Batch_with_a_different_nested_decimal_scale_is_refused()
    {
        StructType S(int scale) => new([new Field("d", new Decimal128Type(10, scale), false)]);
        var schema = new Schema([new Field("s", S(2), false)], null);
        var dec = new Decimal128Array.Builder(new Decimal128Type(10, 3)).Append(1.5m).Build();
        var batch = Batch(("s", Struct(S(3), [dec], [true]), false));
        using var writer = AvroWriter.Create(new MemoryStream(), schema);
        Assert.Throws<ArgumentException>(() => writer.Write(batch));
    }

    [Fact]
    public void Null_in_a_non_nullable_field_is_refused()
    {
        var schema = new Schema([new Field("c", Int64Type.Default, false)], null);
        var batch = new RecordBatch(schema, [Fixed(Int64Type.Default, [(long?)1, null])], 2);
        using var writer = AvroWriter.Create(new MemoryStream(), schema);
        Assert.Throws<NotSupportedException>(() => writer.Write(batch));
    }

    private static async IAsyncEnumerable<RecordBatch> FailingSource()
    {
        yield return Ints(1000);
        await Task.Yield();
        throw new IOException("source broke");
    }

    [Fact]
    public async Task Failing_source_deletes_the_partial_file()
    {
        string path = Path.Combine(Path.GetTempPath(), $"seseragi-fail-{Guid.NewGuid():N}.avro");
        var e = await Assert.ThrowsAsync<IOException>(() =>
            AvroWriter.WriteAsync(path, Ints(1).Schema, FailingSource(), new() { BlockSize = 64 }));
        Assert.Equal("source broke", e.Message);
        Assert.False(File.Exists(path));
    }

    [Fact]
    public async Task Failing_source_on_a_stream_does_not_write_the_last_block()
    {
        var ms = new MemoryStream();
        await Assert.ThrowsAsync<IOException>(() =>
            AvroWriter.WriteAsync(ms, Ints(1).Schema, FailingSource(), new() { LeaveOpen = true }));
        Assert.Equal(0, ms.Length); // one default-size block was pending: nothing, not even the header, was written
    }
}

public class AvroTimezoneTests
{
    [Fact]
    public async Task Time_zone_survives_in_a_schema_attribute()
    {
        var tokyo = new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo");
        var batch = Batch(
            ("tokyo", Fixed(tokyo, [(long?)1_700_000_000_000_000, null]), true),
            ("utc", Fixed(new TimestampType(TimeUnit.Nanosecond, "UTC"), [(long?)1, 2]), false),
            ("naive", Fixed(new TimestampType(TimeUnit.Millisecond, (string?)null), [(long?)3, 4]), false));
        var ms = new MemoryStream();
        AvroWriter.Write(ms, batch.Schema, [batch], new() { LeaveOpen = true });
        ms.Position = 0;
        await using var reader = await AvroReader.OpenAsync(ms);

        using var doc = System.Text.Json.JsonDocument.Parse(reader.WriterSchemaJson);
        var fields = doc.RootElement.GetProperty("fields");
        Assert.Equal("""["null",{"type":"long","logicalType":"timestamp-micros","kaminari.timezone":"Asia/Tokyo"}]""", fields[0].GetProperty("type").GetRawText());
        Assert.Equal("""{"type":"long","logicalType":"timestamp-nanos"}""", fields[1].GetProperty("type").GetRawText());

        Assert.Equal("Asia/Tokyo", ((TimestampType)reader.Schema.GetFieldByName("tokyo").DataType).Timezone);
        Assert.Equal("UTC", ((TimestampType)reader.Schema.GetFieldByName("utc").DataType).Timezone);
        Assert.Null(((TimestampType)reader.Schema.GetFieldByName("naive").DataType).Timezone);
        Assert.Equal(ArrowRender.Rows(batch), ArrowRender.Rows(await reader.ReadBatchesAsync().ToListAsync()));
    }

    [Fact]
    public void Batch_in_another_zone_is_refused()
    {
        var schema = new Schema([new Field("t", new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo"), false)], null);
        var batch = Batch(("t", Fixed(new TimestampType(TimeUnit.Microsecond, "Europe/Paris"), [(long?)1]), false));
        using var writer = AvroWriter.Create(new MemoryStream(), schema);
        Assert.Throws<ArgumentException>(() => writer.Write(batch));
    }
}
