using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Avro;

namespace Seseragi.Files.Tests;

public class AvroReadTests
{
    internal static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "avro", name + ".avro");

    internal static async Task<(Schema Schema, List<RecordBatch> Batches)> ReadAll(string name, AvroReadOptions? options = null)
    {
        await using var reader = await AvroReader.OpenAsync(Fixture(name), options);
        List<RecordBatch> batches = [];
        await foreach (var b in reader.ReadBatchesAsync()) batches.Add(b);
        return (reader.Schema, batches);
    }

    private static T Col<T>(RecordBatch b, string name) where T : IArrowArray => (T)b.Column(name);

    [Fact]
    public async Task Primitives_read_with_types_and_values()
    {
        var (schema, batches) = await ReadAll("primitives");
        var b = Assert.Single(batches);
        Assert.Equal(3, b.Length);

        Assert.Equal(["b", "i", "l", "f", "d", "s", "by", "n"], schema.FieldsList.Select(f => f.Name));
        Assert.IsType<BooleanType>(schema.GetFieldByName("b").DataType);
        Assert.False(schema.GetFieldByName("i").IsNullable);
        Assert.True(schema.GetFieldByName("n").IsNullable);

        Assert.Equal([true, false, true], Col<BooleanArray>(b, "b").Select(x => x!.Value));
        Assert.Equal([1, int.MinValue, int.MaxValue], Col<Int32Array>(b, "i").Values.ToArray());
        Assert.Equal([1L, long.MinValue, long.MaxValue], Col<Int64Array>(b, "l").Values.ToArray());
        Assert.Equal([1.5f, -0.0f, 3.25f], Col<FloatArray>(b, "f").Values.ToArray());
        Assert.True(float.IsNegative(Col<FloatArray>(b, "f").Values[1]));
        Assert.Equal([2.25, 1e300, -0.5], Col<DoubleArray>(b, "d").Values.ToArray());
        var s = Col<StringArray>(b, "s");
        Assert.Equal(["a", "", "雷 ⚡"], Enumerable.Range(0, 3).Select(i => s.GetString(i)));
        var by = Col<BinaryArray>(b, "by");
        Assert.Equal([0, 1], by.GetBytes(0).ToArray());
        Assert.Equal(0, by.GetBytes(1).Length);
        Assert.Equal([0xFF], by.GetBytes(2).ToArray());
        var n = Col<Int64Array>(b, "n");
        Assert.Equal([7L, null, -1L], n.ToList(includeNulls: true));
        Assert.Equal(1, n.NullCount);
        Assert.Equal(0, Col<Int32Array>(b, "i").NullCount);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("deflate")]
    [InlineData("snappy")]
    [InlineData("zstandard")]
    public async Task Many_blocks_are_rebatched_for_every_codec(string codec)
    {
        await using var reader = await AvroReader.OpenAsync(Fixture("many_" + codec), new() { BatchSize = 777 });
        Assert.Equal(codec, reader.Codec);
        int k = 0;
        List<int> lengths = [];
        await foreach (var b in reader.ReadBatchesAsync())
        {
            using var _ = b;
            lengths.Add(b.Length);
            var i = Col<Int32Array>(b, "i");
            var l = Col<Int64Array>(b, "l");
            var s = Col<StringArray>(b, "s");
            var n = Col<Int64Array>(b, "n");
            var bo = Col<BooleanArray>(b, "b");
            for (int r = 0; r < b.Length; r++, k++)
            {
                Assert.Equal(k, i.GetValue(r));
                Assert.Equal(k * 1_000_000_007L, l.GetValue(r));
                Assert.Equal($"s{k}", s.GetString(r));
                Assert.Equal(k % 3 == 0 ? null : k, n.GetValue(r));
                Assert.Equal(k % 2 == 0, bo.GetValue(r));
            }
        }
        Assert.Equal(5000, k);
        Assert.All(lengths[..^1], len => Assert.Equal(777, len));
        Assert.Equal(5000 % 777, lengths[^1]);
    }

    [Fact]
    public async Task Sync_reading_matches_async()
    {
        using var reader = AvroReader.Open(Fixture("many_snappy"), new() { BatchSize = 1000 });
        var batches = reader.ReadBatches().ToList();
        Assert.Equal(5, batches.Count);
        Assert.Equal(4999, ((Int32Array)batches[^1].Column("i")).GetValue(999));
    }

    [Fact]
    public async Task Projection_skips_columns_and_keeps_file_order()
    {
        var (schema, batches) = await ReadAll("many_deflate", new() { Columns = ["s", "i"] });
        Assert.Equal(["i", "s"], schema.FieldsList.Select(f => f.Name));
        var b = batches[0];
        Assert.Equal(2, b.ColumnCount);
        Assert.Equal("s10", ((StringArray)b.Column("s")).GetString(10));

        var (schema2, _) = await ReadAll("many_deflate", new() { ColumnIndices = [7, 0] });
        Assert.Equal(["b", "n"], schema2.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public async Task Unknown_column_is_an_error()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => ReadAll("primitives", new() { Columns = ["nope"] }));
    }

    [Fact]
    public async Task Row_limit_stops_mid_block()
    {
        var (_, batches) = await ReadAll("many_null", new() { BatchSize = 100, RowLimit = 250 });
        Assert.Equal([100, 100, 50], batches.Select(b => b.Length));
        Assert.Equal(249, ((Int32Array)batches[^1].Column("i")).GetValue(49));

        var (_, none) = await ReadAll("many_null", new() { RowLimit = 0 });
        Assert.Empty(none);
    }

    [Fact]
    public async Task Empty_file_has_schema_and_no_batches()
    {
        var (schema, batches) = await ReadAll("empty");
        Assert.Equal(8, schema.FieldsList.Count);
        Assert.Empty(batches);
    }

    [Fact]
    public async Task Logical_types_map_to_arrow_types()
    {
        var (schema, batches) = await ReadAll("logical");
        var b = Assert.Single(batches);
        IArrowType T(string n) => schema.GetFieldByName(n).DataType;

        Assert.IsType<Date32Type>(T("date"));
        Assert.Equal(18262, ((Date32Array)b.Column("date")).GetValue(0));
        Assert.Equal(TimeUnit.Millisecond, Assert.IsType<Time32Type>(T("tms")).Unit);
        Assert.Equal(3723004, ((Time32Array)b.Column("tms")).GetValue(0));
        Assert.Equal(TimeUnit.Microsecond, Assert.IsType<Time64Type>(T("tus")).Unit);
        Assert.Equal(3723000004L, ((Time64Array)b.Column("tus")).GetValue(0));

        (string Name, TimeUnit Unit, string? Tz, long Value)[] timestamps =
        [
            ("tsms", TimeUnit.Millisecond, "UTC", 1577836800123), ("tsus", TimeUnit.Microsecond, "UTC", 1577836800123456),
            ("tsns", TimeUnit.Nanosecond, "UTC", 1577836800123456789), ("ltsms", TimeUnit.Millisecond, null, 1),
            ("ltsus", TimeUnit.Microsecond, null, 2), ("ltsns", TimeUnit.Nanosecond, null, 3),
        ];
        foreach (var (name, unit, tz, value) in timestamps)
        {
            var t = Assert.IsType<TimestampType>(T(name));
            Assert.Equal(unit, t.Unit);
            Assert.Equal(tz, t.Timezone);
            Assert.Equal(value, ((TimestampArray)b.Column(name)).Values[0]);
        }

        var decb = Assert.IsType<Decimal128Type>(T("decb"));
        Assert.Equal((10, 2), (decb.Precision, decb.Scale));
        Assert.Equal(-12345678.91m, ((Decimal128Array)b.Column("decb")).GetValue(0));
        Assert.Equal(123456789012345.678m, ((Decimal128Array)b.Column("decf")).GetValue(0));
        var d50 = Assert.IsType<Decimal256Type>(T("dec50"));
        Assert.Equal(50, d50.Precision);
        Assert.Equal("-" + new string('9', 50), ((Decimal256Array)b.Column("dec50")).GetString(0));

        Assert.IsType<StringType>(T("uuid"));
        Assert.Equal("5c3d8448-a0b1-4265-83c4-0d24427e8fc2", ((StringArray)b.Column("uuid")).GetString(0));

        Assert.Equal(IntervalUnit.MonthDayNanosecond, Assert.IsType<IntervalType>(T("dur")).Unit);
        var dur = ((MonthDayNanosecondIntervalArray)b.Column("dur")).GetValue(0)!.Value;
        Assert.Equal((1, 2, 3_000_000L), (dur.Months, dur.Days, dur.Nanoseconds));

        Assert.IsType<Int64Type>(T("badlt"));
        Assert.Equal(42L, ((Int64Array)b.Column("badlt")).GetValue(0));
    }

    [Fact]
    public async Task Complex_types_follow_spark_union_rules()
    {
        var (schema, batches) = await ReadAll("complex");
        var b = Assert.Single(batches);
        IArrowType T(string n) => schema.GetFieldByName(n).DataType;

        var arr = (ListArray)b.Column("arr");
        Assert.Equal([1, 2, 3], ((Int32Array)arr.GetSlicedValues(0)).Values.ToArray());
        Assert.Equal(0, arr.GetValueLength(1));
        Assert.Equal([-1], ((Int32Array)arr.GetSlicedValues(2)).Values.ToArray());

        var arrn = (ListArray)b.Column("arrn");
        Assert.True(((ListType)T("arrn")).ValueField.IsNullable);
        var arrn0 = (StringArray)arrn.GetSlicedValues(0);
        Assert.Equal("a", arrn0.GetString(0));
        Assert.True(arrn0.IsNull(1));

        var m = (MapArray)b.Column("m");
        var keys = (StringArray)m.Keys;
        var values = (Int64Array)m.Values;
        Assert.Equal(2, m.GetValueLength(0));
        Assert.Equal(["k", "j"], [keys.GetString(0), keys.GetString(1)]);
        Assert.Equal([1L, 2L, -5L], values.Values.ToArray());
        Assert.Equal(0, m.GetValueLength(1));

        var rec = (StructArray)b.Column("rec");
        Assert.Equal([1, 2, 4], ((Int32Array)rec.Fields[0]).Values.ToArray());
        Assert.True(((StringArray)rec.Fields[1]).IsNull(1));

        var recn = (StructArray)b.Column("recn");
        Assert.True(recn.IsNull(0));
        Assert.False(recn.IsNull(1));
        Assert.True(recn.IsNull(2));
        Assert.Equal(3, ((Int32Array)recn.Fields[0]).GetValue(1));

        var e = (DictionaryArray)b.Column("e");
        var dict = (StringArray)e.Dictionary;
        Assert.Equal(["RED", "GREEN", "BLUE"], Enumerable.Range(0, 3).Select(i => dict.GetString(i)));
        Assert.Equal([2, 0, 1], ((Int32Array)e.Indices).Values.ToArray());

        Assert.Equal(3, Assert.IsType<FixedSizeBinaryType>(T("fx")).ByteWidth);
        Assert.Equal("xyz"u8.ToArray(), ((FixedSizeBinaryArray)b.Column("fx")).GetBytes(1).ToArray());

        Assert.IsType<Int64Type>(T("il"));
        Assert.Equal([5L, 1L << 40, -7L], ((Int64Array)b.Column("il")).Values.ToArray());
        Assert.False(schema.GetFieldByName("il").IsNullable);

        Assert.IsType<DoubleType>(T("fd"));
        Assert.Equal([1.5, null, 2.5e100], ((DoubleArray)b.Column("fd")).ToList(includeNulls: true));

        var u = (StructArray)b.Column("u");
        Assert.Equal(["member0", "member1"], ((StructType)T("u")).Fields.Select(f => f.Name));
        Assert.Equal("s", ((StringArray)u.Fields[0]).GetString(0));
        Assert.True(u.Fields[1].IsNull(0));
        Assert.True(u.IsNull(1));
        Assert.True(u.Fields[0].IsNull(2));
        Assert.Equal(9, ((Int32Array)u.Fields[1]).GetValue(2));

        Assert.IsType<NullType>(T("nonly"));
        Assert.IsType<NullType>(T("nul"));
        Assert.Equal(3, b.Column("nul").NullCount);
    }

    [Fact]
    public async Task Recursive_record_is_refused_unless_projected_away()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => ReadAll("recursive"));
        var (schema, batches) = await ReadAll("recursive", new() { Columns = ["id", "tail"] });
        Assert.Equal(["id", "tail"], schema.FieldsList.Select(f => f.Name));
        var b = Assert.Single(batches);
        Assert.Equal("b", ((StringArray)b.Column("tail")).GetString(1));
    }

    [Fact]
    public async Task Top_level_non_record_reads_as_value_column()
    {
        var (schema, batches) = await ReadAll("toplevel_long");
        Assert.Equal("value", Assert.Single(schema.FieldsList).Name);
        Assert.Equal([1L, -2L, 3L], ((Int64Array)batches[0].Column(0)).Values.ToArray());
    }
}
