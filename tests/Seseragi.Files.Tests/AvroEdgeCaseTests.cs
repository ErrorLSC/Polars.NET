using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Avro;

namespace Seseragi.Files.Tests;

/// <summary>Apache Avro sample files and hand-built containers for encodings the reference writers do not
/// produce (negative block counts, corrupt or truncated data).</summary>
public class AvroEdgeCaseTests
{
    private static string Apache(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "avro", "apache", name);

    [Theory]
    [InlineData("weather.avro", "null")]
    [InlineData("weather-deflate.avro", "deflate")]
    [InlineData("weather-snappy.avro", "snappy")]
    [InlineData("weather-zstd.avro", "zstandard")]
    public async Task Apache_weather_samples(string file, string codec)
    {
        await using var reader = await AvroReader.OpenAsync(Apache(file));
        Assert.Equal(codec, reader.Codec);
        var rows = ArrowRender.Rows(await reader.ReadBatchesAsync().ToListAsync());
        Assert.Equal(
        [
            "\"011990-99999\" | -619524000000 | 0",
            "\"011990-99999\" | -619506000000 | 22",
            "\"011990-99999\" | -619484400000 | -11",
            "\"012650-99999\" | -655531200000 | 111",
            "\"012650-99999\" | -655509600000 | 78",
        ], rows);
    }

    [Fact]
    public async Task Apache_sync_in_metadata_with_binary_value()
    {
        await using var reader = await AvroReader.OpenAsync(Apache("syncInMeta.avro"));
        Assert.Equal(16, reader.Metadata["avro.sync"].Length);
        long rows = 0;
        await foreach (var b in reader.ReadBatchesAsync()) rows += b.Length;
        Assert.True(rows > 0);
        Assert.Equal(["ID", "First", "Last", "Phone", "Age"], reader.Schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public async Task Apache_pre_1_3_container_is_refused()
    {
        await Assert.ThrowsAsync<AvroFormatException>(() => AvroReader.OpenAsync(Apache("test.avro12")));
    }

    [Fact]
    public async Task Apache_interop_schema()
    {
        // The recursive Node record cannot become Arrow; everything else reads.
        await Assert.ThrowsAsync<NotSupportedException>(() => AvroReader.OpenAsync(Apache("interop.avro")));
        await using var reader = await AvroReader.OpenAsync(Apache("interop.avro"),
            new() { ColumnIndices = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] });
        var b = Assert.Single(await reader.ReadBatchesAsync().ToListAsync());
        var rows = ArrowRender.Rows(b);
        Assert.Equal(
            "12 | 15234324 | \"hey\" | true | 1234 | -1234 | 0x3132333132616466 | null | [5, 0, 12] | " +
            "{\"a\": {label: \"a\"}, \"bee\": {label: \"cee\"}} | {member0: null, member1: 12, member2: null} | \"C\" | " +
            "0x31303139313831373136313531343133", rows[0]);
        Assert.Contains("{member0: null, member1: null, member2: [0x78, 0x]}", rows[1]);
    }

    // ------------------------------------------------------------ hand-built containers

    private static readonly byte[] Sync = [.. Enumerable.Range(1, 16).Select(i => (byte)i)];

    internal sealed class Bytes
    {
        public readonly List<byte> Data = [];

        public Bytes Long(long v)
        {
            ulong n = (ulong)((v << 1) ^ (v >> 63));
            while (n >= 0x80)
            {
                Data.Add((byte)(n | 0x80));
                n >>= 7;
            }
            Data.Add((byte)n);
            return this;
        }

        public Bytes Raw(ReadOnlySpan<byte> b)
        {
            Data.AddRange(b.ToArray());
            return this;
        }

        public Bytes Str(string s) => Long(Encoding.UTF8.GetByteCount(s)).Raw(Encoding.UTF8.GetBytes(s));
        public byte[] ToArray() => [.. Data];
    }

    internal static byte[] Container(string schema, IEnumerable<(long Count, byte[] Data)> blocks, string? codec = null, bool includeCodec = true)
    {
        var b = new Bytes().Raw("Obj\u0001"u8);
        List<(string, string)> meta = includeCodec ? [("avro.schema", schema), ("avro.codec", codec ?? "null")] : [("avro.schema", schema)];
        b.Long(meta.Count);
        foreach (var (k, v) in meta) b.Str(k).Str(v);
        b.Long(0).Raw(Sync);
        foreach (var (count, data) in blocks) b.Long(count).Long(data.Length).Raw(data).Raw(Sync);
        return b.ToArray();
    }

    internal static async Task<List<string>> Read(byte[] file, AvroReadOptions? options = null)
    {
        await using var reader = await AvroReader.OpenAsync(new MemoryStream(file), options);
        return ArrowRender.Rows(await reader.ReadBatchesAsync().ToListAsync());
    }

    private const string ArrayMapSchema = """
        {"type":"record","name":"r","fields":[
          {"name":"a","type":{"type":"array","items":"int"}},
          {"name":"m","type":{"type":"map","values":"string"}},
          {"name":"z","type":["int","null"]}]}
        """;

    /// <summary>One row: a = [1, 2, 3] in two blocks (the second with a negative count and byte size),
    /// m = {x: "y"} as a negative-count block, z = 7 then null via the branch order [int, null].</summary>
    private static byte[] NegativeBlockRows()
    {
        var row1 = new Bytes()
            .Long(2).Long(1).Long(2)                     // a: block of 2
            .Long(-1).Long(1).Long(3).Long(0)            // a: block of -1 with size 1, item 3, end
            .Long(-1).Long(4).Str("x").Str("y").Long(0)  // m: block of -1, size 4, x -> y, end
            .Long(0).Long(7);                            // z: branch 0 (int) = 7
        var row2 = new Bytes().Long(0).Long(0).Long(1); // empty array, empty map, z = null (branch 1)
        return [.. row1.Data, .. row2.Data];
    }

    [Fact]
    public async Task Negative_block_counts_decode_and_skip()
    {
        var file = Container(ArrayMapSchema, [(2, NegativeBlockRows())]);
        Assert.Equal(["[1, 2, 3] | {\"x\": \"y\"} | 7", "[] | {} | null"], await Read(file));
        // Skipping a and m must use the recorded byte sizes and land on z.
        Assert.Equal(["7", "null"], await Read(file, new() { Columns = ["z"] }));
    }

    [Fact]
    public async Task Primitive_arrays_across_blocks()
    {
        const string schema = """
            {"type":"record","name":"r","fields":[
              {"name":"d","type":{"type":"array","items":"double"}},
              {"name":"f","type":{"type":"array","items":"float"}},
              {"name":"l","type":{"type":"array","items":"long"}},
              {"name":"t","type":{"type":"array","items":{"type":"long","logicalType":"timestamp-micros"}}}]}
            """;
        static byte[] D(double v) => BitConverter.GetBytes(v);
        static byte[] F(float v) => BitConverter.GetBytes(v);
        var row = new Bytes()
            .Long(2).Raw(D(1.5)).Raw(D(-2)).Long(-1).Long(8).Raw(D(1e300)).Long(0)   // d: [1.5, -2] + sized block [1e300]
            .Long(1).Raw(F(0.25f)).Long(1).Raw(F(-8f)).Long(0)                        // f: two blocks
            .Long(3).Long(1).Long(-1).Long(long.MaxValue).Long(-2).Long(11).Long(0).Long(long.MinValue).Long(0) // l: 3 + sized block of 2
            .Long(1).Long(1_700_000_000_000_000).Long(0);                             // t: 8-byte varint
        var empty = new Bytes().Long(0).Long(0).Long(0).Long(0);
        Assert.Equal(
        [
            "[1.5, -2, 1E+300] | [0.25, -8] | [1, -1, 9223372036854775807, 0, -9223372036854775808] | [1700000000000000]",
            "[] | [] | [] | []",
        ], await Read(Container(schema, [(2, [.. row.Data, .. empty.Data])])));
    }

    [Fact]
    public async Task Missing_codec_means_null()
    {
        var file = Container(ArrayMapSchema, [(2, NegativeBlockRows())], includeCodec: false);
        Assert.Equal(2, (await Read(file)).Count);
    }

    [Fact]
    public async Task Corrupt_sync_marker_is_detected()
    {
        var file = Container(ArrayMapSchema, [(2, NegativeBlockRows())]);
        file[^1] ^= 0xFF;
        await Assert.ThrowsAsync<AvroFormatException>(() => Read(file));
    }

    [Fact]
    public async Task Truncated_file_is_detected()
    {
        var file = Container(ArrayMapSchema, [(2, NegativeBlockRows())]);
        await Assert.ThrowsAsync<AvroFormatException>(() => Read(file[..^20]));
    }

    [Fact]
    public async Task Trailing_bytes_in_a_block_are_detected()
    {
        var file = Container(ArrayMapSchema, [(1, NegativeBlockRows())]); // two rows' bytes, count says one
        await Assert.ThrowsAsync<AvroFormatException>(() => Read(file));
    }

    [Fact]
    public async Task Bad_union_branch_is_detected()
    {
        var data = new Bytes().Long(0).Long(0).Long(5).ToArray(); // z branch 5 of 2
        await Assert.ThrowsAsync<AvroFormatException>(() => Read(Container(ArrayMapSchema, [(1, data)])));
    }

    [Fact]
    public async Task Huge_array_count_on_little_data_fails_fast()
    {
        var data = new Bytes().Long(long.MaxValue / 4).Long(1).ToArray();
        await Assert.ThrowsAsync<AvroFormatException>(() => Read(Container(ArrayMapSchema, [(1, data)])));
    }

    [Fact]
    public async Task Huge_count_on_a_skipped_array_fails_fast()
    {
        const string schema = """
            {"type":"record","name":"r","fields":[
              {"name":"a","type":{"type":"array","items":"null"}},
              {"name":"z","type":"int"}]}
            """;
        // a claims long.MaxValue / 4 zero-width items without a byte size; only z is projected, so a is skipped.
        var data = new Bytes().Long(long.MaxValue / 4).Long(0).Long(5).ToArray();
        await Assert.ThrowsAsync<AvroFormatException>(() => Read(Container(schema, [(1, data)]), new() { Columns = ["z"] }));
    }

    [Theory]
    [InlineData("""{"type":"record","name":"r","fields":[{"name":"s","type":"string"}]}""")]
    [InlineData("""{"type":"record","name":"r","fields":[{"name":"m","type":{"type":"map","values":"int"}}]}""")]
    public async Task Invalid_utf8_in_strings_and_map_keys_is_refused(string schema)
    {
        bool isMap = schema.Contains("map");
        var bad = new Bytes();
        if (isMap) bad.Long(1);
        bad.Long(2).Raw([0xC3, 0x28]); // overlong lead byte followed by a non-continuation byte
        if (isMap) bad.Long(0).Long(0);
        await Assert.ThrowsAsync<AvroFormatException>(() => Read(Container(schema, [(1, bad.ToArray())])));

        // bytes columns carry arbitrary bytes and are not checked
        const string bytesSchema = """{"type":"record","name":"r","fields":[{"name":"b","type":"bytes"}]}""";
        Assert.Equal(["0xC328"], await Read(Container(bytesSchema, [(1, new Bytes().Long(2).Raw([0xC3, 0x28]).ToArray())])));
    }

    [Fact]
    public async Task Snappy_crc_mismatch_is_detected()
    {
        var batch = TestBatches.Batch(("c", TestBatches.Fixed(Int64Type.Default, [(long?)1, 2, 3]), false));
        var ms = new MemoryStream();
        AvroWriter.Write(ms, batch.Schema, [batch], new() { Compression = SnappyCompression.Instance, LeaveOpen = true });
        var file = ms.ToArray();
        file[^17] ^= 0x01; // last CRC byte, just before the trailing sync marker
        await Assert.ThrowsAsync<AvroFormatException>(() => Read(file));
    }

    [Theory]
    [InlineData("bzip2")]
    [InlineData("xz")]
    [InlineData("lz4")]
    public async Task Unsupported_codecs_are_named(string codec)
    {
        var file = Container(ArrayMapSchema, [], codec);
        var e = await Assert.ThrowsAsync<NotSupportedException>(() => Read(file));
        Assert.Contains(codec, e.Message);
    }

    [Fact]
    public async Task Invalid_logical_types_fall_back_to_the_base_type()
    {
        const string schema = """
            {"type":"record","name":"r","fields":[
              {"name":"d","type":{"type":"bytes","logicalType":"decimal","precision":2,"scale":5}},
              {"name":"t","type":{"type":"string","logicalType":"date"}},
              {"name":"f","type":{"type":"fixed","name":"F2","size":2,"logicalType":"decimal","precision":9}}]}
            """;
        var data = new Bytes().Long(1).Raw([0x05]).Str("x").Raw([0, 1]).ToArray();
        await using var reader = await AvroReader.OpenAsync(new MemoryStream(Container(schema, [(1, data)])));
        Assert.IsType<BinaryType>(reader.Schema.GetFieldByName("d").DataType);
        Assert.IsType<StringType>(reader.Schema.GetFieldByName("t").DataType);
        Assert.IsType<FixedSizeBinaryType>(reader.Schema.GetFieldByName("f").DataType);
        Assert.Single(await reader.ReadBatchesAsync().ToListAsync());
    }

    [Fact]
    public async Task Named_type_references_resolve_through_namespaces()
    {
        const string schema = """
            {"type":"record","name":"r","namespace":"a.b","fields":[
              {"name":"x","type":{"type":"fixed","name":"F","size":1}},
              {"name":"y","type":"F"},
              {"name":"z","type":"a.b.F"},
              {"name":"inner","type":{"type":"record","name":"I","namespace":"c","fields":[{"name":"w","type":"a.b.F"}]}}]}
            """;
        var data = new Bytes().Raw([1, 2, 3, 4]).ToArray();
        Assert.Equal(["0x01 | 0x02 | 0x03 | {w: 0x04}"], await Read(Container(schema, [(1, data)])));
    }

    [Fact]
    public async Task Batches_outlive_the_reader_and_free_on_dispose()
    {
        List<RecordBatch> batches;
        await using (var reader = await AvroReader.OpenAsync(AvroReadTests.Fixture("many_zstandard"), new() { BatchSize = 1000 }))
            batches = await reader.ReadBatchesAsync().ToListAsync();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        Assert.Equal("s4999", ((StringArray)batches[^1].Column("s")).GetString(999));
        foreach (var b in batches) b.Dispose();
        foreach (var b in batches) b.Dispose(); // idempotent
    }

    [Fact]
    public async Task Undisposed_batches_are_released_by_the_finalizer()
    {
        for (int i = 0; i < 3; i++)
        {
            await using var reader = await AvroReader.OpenAsync(AvroReadTests.Fixture("many_snappy"));
            await foreach (var _ in reader.ReadBatchesAsync()) { }
        }
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }
}
