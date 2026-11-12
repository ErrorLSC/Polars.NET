using Apache.Arrow;
using ArrowField = Apache.Arrow.Field;
using ArrowSchema = Apache.Arrow.Schema;
using ArrowTypes = Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Avro;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// Avro on the managed Seseragi.Files reader and writer: types beyond what the engine's own Avro reader handled,
/// the Polars type adaptations, ScanAvro / SinkAvro, parallel decoding.
/// </summary>
public class AvroManagedTests
{
    private static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "avro", name + ".avro");

    private static string Dtype(DataFrame df, string column) => df.Schema[column].ToString()!;

    [Fact]
    public void Unions_maps_enums_and_nulls_read()
    {
        using var df = DataFrame.ReadAvro(Fixture("complex"));
        Assert.Equal(3, df.Height);
        Assert.Equal("list[struct[key: str, value: i64]]", Dtype(df, "m"));
        Assert.Equal("cat", Dtype(df, "e"));
        Assert.Equal("binary", Dtype(df, "fx"));
        Assert.Equal("i64", Dtype(df, "il"));
        Assert.Equal("f64", Dtype(df, "fd"));
        Assert.Equal("struct[member0: str, member1: i32]", Dtype(df, "u"));
        Assert.Equal("null", Dtype(df, "nul"));
        Assert.Equal(1L << 40, df.GetValue<long>(1, "il"));
        Assert.Equal("BLUE", df.GetValue<string>(0, "e"));
    }

    [Fact]
    public void Files_with_field_ids_read_and_scan()
    {
        // Iceberg-style ids (field-id, element-id, key-id, value-id) arrive as Arrow field metadata; the engine
        // import must accept them at every level.
        static ArrowField Id(ArrowField f, int id) => Kaminari.Sazanami.Arrow.ArrowFieldIds.With(f, id);
        ArrowSchema schema = new([
            Id(new ArrowField("n", ArrowTypes.Int64Type.Default, false), 1),
            Id(new ArrowField("splits", new ArrowTypes.ListType(Id(new ArrowField("item", ArrowTypes.Int64Type.Default, true), 133)), true), 132),
            Id(new ArrowField("props", new ArrowTypes.MapType(
                Id(new ArrowField("key", ArrowTypes.StringType.Default, false), 7),
                Id(new ArrowField("value", ArrowTypes.StringType.Default, true), 8)), true), 6),
        ], []);
        var splits = new ListArray.Builder(ArrowTypes.Int64Type.Default);
        splits.Append();
        ((Int64Array.Builder)splits.ValueBuilder).Append(4).Append(8);
        var props = new MapArray.Builder((ArrowTypes.MapType)schema.FieldsList[2].DataType);
        props.Append();
        ((StringArray.Builder)props.KeyBuilder).Append("k");
        ((StringArray.Builder)props.ValueBuilder).Append("v");
        RecordBatch batch = new(schema, [new Int64Array.Builder().Append(42).Build(), splits.Build(), props.Build()], 1);

        string path = Path.Combine(Path.GetTempPath(), $"avro-ids-{Guid.NewGuid():N}.avro");
        try
        {
            AvroWriter.Write(path, schema, [batch]);
            using var df = DataFrame.ReadAvro(path);
            Assert.Equal(42L, df.GetValue<long>(0, "n"));
            Assert.Equal("list[i64]", Dtype(df, "splits"));
            using var scanned = LazyFrame.ScanAvro(path).Collect();
            Assert.True(df.Equals(scanned));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Maps_with_integer_keys_round_trip_through_the_engine()
    {
        // Written as an Avro array of key/value records (logicalType map), read back as an Arrow map, which the
        // engine imports as a list of key/value structs (as for string-key maps).
        var type = new ArrowTypes.MapType(new ArrowField("key", ArrowTypes.Int32Type.Default, false),
            new ArrowField("value", ArrowTypes.StringType.Default, true));
        var m = new MapArray.Builder(type);
        m.Append(); ((Int32Array.Builder)m.KeyBuilder).Append(1).Append(2); ((StringArray.Builder)m.ValueBuilder).Append("a").AppendNull();
        m.AppendNull();
        ArrowSchema schema = new([new ArrowField("m", type, true)], []);
        RecordBatch batch = new(schema, [m.Build()], 2);

        string path = Path.Combine(Path.GetTempPath(), $"avro-intmap-{Guid.NewGuid():N}.avro");
        try
        {
            AvroWriter.Write(path, schema, [batch]);
            using var df = DataFrame.ReadAvro(path);
            Assert.Equal("list[struct[key: i32, value: str]]", Dtype(df, "m"));
            using var keys = df.Select(Col("m").List.Eval(Element().Struct.Field("key")).Alias("k"));
            Assert.Equal([1, 2], keys.GetValue<List<int>>(0, "k"));
            Assert.Null(keys.GetValue<List<int>?>(1, "k"));
            using var scanned = LazyFrame.ScanAvro(path).Collect();
            Assert.True(df.Equals(scanned));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Wall_clock_timestamps_read_without_a_time_zone()
    {
        // local-timestamp-* and Iceberg's timestamp-* with "adjust-to-utc": false both become a zone-less Arrow
        // timestamp; the engine keeps it a Datetime without a time zone.
        using var df = DataFrame.ReadAvro(Fixture("logical"), columns: ["ltsms", "ltsus", "ltsns"]);
        Assert.Equal("datetime[ms]", Dtype(df, "ltsms"));
        Assert.Equal("datetime[μs]", Dtype(df, "ltsus"));
        Assert.Equal("datetime[ns]", Dtype(df, "ltsns"));
    }

    [Fact]
    public void Decimal_wider_than_38_digits_is_refused_with_its_column_name()
    {
        var e = Assert.Throws<NotSupportedException>(() => DataFrame.ReadAvro(Fixture("logical")));
        Assert.Contains("dec50", e.Message);
    }

    [Fact]
    public void Avro_duration_becomes_a_months_days_nanoseconds_struct()
    {
        using var df = DataFrame.ReadAvro(Fixture("logical"), columns: ["dur", "decb", "tsns", "uuid"]);
        Assert.Equal("struct[months: i32, days: i32, nanoseconds: duration[ns]]", Dtype(df, "dur"));
        Assert.Equal("decimal[10,2]", Dtype(df, "decb"));
        Assert.Equal("datetime[ns, UTC]", Dtype(df, "tsns"));
        using var parts = df.Select(Col("dur").Struct.Field("months"), Col("dur").Struct.Field("days"));
        Assert.Equal(1, parts.GetValue<int>(0, "months"));
        Assert.Equal(2, parts.GetValue<int>(0, "days"));
    }

    [Fact]
    public void Parallel_and_sequential_reads_are_equal()
    {
        foreach (var codec in (string[])["null", "deflate", "snappy", "zstandard"])
        {
            using var sequential = DataFrame.ReadAvro(Fixture("many_" + codec), parallel: false);
            using var parallel = DataFrame.ReadAvro(Fixture("many_" + codec), parallel: true);
            Assert.Equal(5000, parallel.Height);
            Assert.True(sequential.Equals(parallel));
        }
    }

    [Fact]
    public void Row_limit_and_projections_match_the_old_reader_contract()
    {
        using var limited = DataFrame.ReadAvro(Fixture("many_null"), nRows: 10, columns: ["s", "i"]);
        Assert.Equal(10, limited.Height);
        Assert.Equal(["i", "s"], limited.Columns); // file order, as before
        using var byIndex = DataFrame.ReadAvro(Fixture("many_null"), nRows: 3, projection: [5]);
        Assert.Equal(["s"], byIndex.Columns);
        Assert.Equal("s2", byIndex.GetValue<string>(2, "s"));
    }

    /// <summary>A stream that cannot seek and reports no length: the reader must not need either.</summary>
    private sealed class ForwardOnlyStream(Stream inner) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, Math.Min(count, 1000));
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    [Fact]
    public void Stream_overload_reads_forward_only_streams_and_leaves_them_open()
    {
        using var file = File.OpenRead(Fixture("many_snappy"));
        var stream = new ForwardOnlyStream(file);
        using var df = DataFrame.ReadAvro(stream);
        Assert.Equal(5000, df.Height);
        Assert.True(file.CanRead); // not disposed
    }

    [Fact]
    public void ScanAvro_streams_with_filters_projection_and_limit()
    {
        using var lf = LazyFrame.ScanAvro(Fixture("many_zstandard"));
        using var filtered = lf.Filter(Col("i") >= 4990).Select(Col("s"), Col("i")).Collect(Engine.Streaming);
        Assert.Equal(10, filtered.Height);
        Assert.Equal("s4995", filtered.Filter(Col("i") == 4995).GetValue<string>(0, "s"));

        using var limited = LazyFrame.ScanAvro(Fixture("many_null"), nRows: 7, parallel: false).Collect();
        Assert.Equal(7, limited.Height);

        // Each run reads the file again.
        using var twice = LazyFrame.ScanAvro(Fixture("complex"));
        using var a = twice.Collect();
        using var b = twice.Collect(Engine.Streaming);
        Assert.True(a.Equals(b));
    }

    private static DataFrame Sample() => DataFrame.FromColumns(new
    {
        id = new long[] { 1, 2, 3 },
        name = new[] { "Alice", null, "雷" },
        score = new double?[] { 1.5, null, -2 },
    });

    [Theory]
    [InlineData(AvroCompression.Uncompressed)]
    [InlineData(AvroCompression.Deflate)]
    [InlineData(AvroCompression.Snappy)]
    [InlineData(AvroCompression.Zstandard)]
    public void Write_round_trips_with_every_codec(AvroCompression compression)
    {
        using var df = Sample();
        var bytes = df.WriteAvroMemory(compression);
        using var back = DataFrame.ReadAvro(bytes);
        Assert.True(df.Equals(back));
    }

    [Fact]
    public void Empty_record_name_writes_the_default_name()
    {
        using var df = Sample();
        using var reader = AvroReader.Open(new MemoryStream(df.WriteAvroMemory()));
        Assert.Contains("\"name\":\"topLevelRecord\"", reader.WriterSchemaJson);
        using var reader2 = AvroReader.Open(new MemoryStream(df.WriteAvroMemory(name: "Person")));
        Assert.Contains("\"name\":\"Person\"", reader2.WriterSchemaJson);
    }

    [Fact]
    public void Zoned_datetimes_keep_their_time_zone()
    {
        using var df = DataFrame.FromColumns(new { t = new[] { new DateTime(2024, 1, 2, 3, 4, 5) } })
            .WithColumns(Col("t").Dt.ReplaceTimeZone("Asia/Tokyo"));
        using var back = DataFrame.ReadAvro(df.WriteAvroMemory());
        Assert.Equal(Dtype(df, "t"), Dtype(back, "t"));
        Assert.True(df.Equals(back));
    }

    [Fact]
    public void Categoricals_are_written_as_strings()
    {
        using var df = DataFrame.FromColumns(new { c = new[] { "x", "y", "x" } }).WithColumns(Col("c").Cast(DataType.Categorical()));
        using var back = DataFrame.ReadAvro(df.WriteAvroMemory());
        Assert.Equal("str", Dtype(back, "c"));
        Assert.Equal("y", back.GetValue<string>(1, "c"));
    }

    [Fact]
    public void SinkAvro_writes_the_streamed_result()
    {
        string path = Path.Combine(Path.GetTempPath(), $"sink-{Guid.NewGuid():N}.avro");
        try
        {
            using var lf = LazyFrame.ScanAvro(Fixture("many_snappy"));
            lf.Filter(Col("i") < 100).Select(Col("i"), Col("s")).SinkAvro(path, AvroCompression.Zstandard);
            using var back = DataFrame.ReadAvro(path);
            Assert.Equal(100, back.Height);
            Assert.Equal("s99", back.GetValue<string>(99, "s"));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void SinkAvro_of_an_empty_result_writes_its_schema()
    {
        var ms = new MemoryStream();
        using var lf = LazyFrame.ScanAvro(Fixture("many_null"));
        lf.Filter(Col("i") < 0).Select(Col("i"), Col("s")).SinkAvro(ms);
        using var back = DataFrame.ReadAvro(ms.ToArray());
        Assert.Equal(0, back.Height);
        Assert.Equal(["i", "s"], back.Columns);
    }

    [Fact]
    public void WriteAvroNativeMemory_returns_a_readable_native_buffer()
    {
        using var df = Sample();
        using var buffer = df.WriteAvroNativeMemory(AvroCompression.Snappy);
        using var back = DataFrame.ReadAvro(buffer.Memory);
        Assert.True(df.Equals(back));
    }

    [Fact]
    public void ScanRecordBatches_converts_standard_arrow_layouts()
    {
        // Utf8 (not Utf8View) and Time32[ms] (not Time64[ns]) used to be taken as the engine's own layouts.
        var schema = new Schema(
        [
            new Apache.Arrow.Field("s", Apache.Arrow.Types.StringType.Default, true),
            new Apache.Arrow.Field("t", new Apache.Arrow.Types.Time32Type(Apache.Arrow.Types.TimeUnit.Millisecond), true),
        ], null);
        var batch = new RecordBatch(schema,
        [
            new StringArray.Builder().Append("Alice").Append("Bob").Build(),
            new Time32Array.Builder(new Apache.Arrow.Types.Time32Type(Apache.Arrow.Types.TimeUnit.Millisecond)).Append(1000).Append(2000).Build(),
        ], 2);
        using var df = LazyFrame.ScanRecordBatches([batch], schema).Collect();
        Assert.Equal("Bob", df.GetValue<string>(1, "s"));
        Assert.Equal(new TimeOnly(0, 0, 2), df.GetValue<TimeOnly>(1, "t"));
    }
}
