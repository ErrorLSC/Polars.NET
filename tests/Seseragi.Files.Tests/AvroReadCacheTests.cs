using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow;
using Kaminari.Seseragi.Files.Avro;
using static Kaminari.Seseragi.Files.Avro.AvroField;
using S = Kaminari.Seseragi.Files.Avro.AvroSchema;

namespace Seseragi.Files.Tests;

/// <summary>Reading many files of one schema through a shared <see cref="AvroReadCache"/>.</summary>
public class AvroReadCacheTests
{
    private static readonly string[] Fixtures = ["primitives", "logical", "complex", "many_deflate", "many_snappy", "empty", "toplevel_long"];

    private static async Task<(string Schema, string Json, List<string> Rows)> Read(byte[] file, AvroReadOptions options)
    {
        await using var reader = await AvroReader.OpenAsync(new MemoryStream(file), options);
        List<RecordBatch> batches = [];
        await foreach (var b in reader.ReadBatchesAsync()) batches.Add(b);
        return (reader.Schema.ToString() ?? "", reader.WriterSchemaJson, ArrowRender.Rows(batches));
    }

    private static byte[] Fixture(string name) => File.ReadAllBytes(AvroReadTests.Fixture(name));

    // A manifest-like file with field ids, written with a given schema.
    private static byte[] Manifest(long start, int rows)
    {
        var schema = Iceberg();
        var paths = new StringArray.Builder();
        var counts = new Int64Array.Builder();
        for (int i = 0; i < rows; i++)
        {
            paths.Append($"f{start + i}.parquet");
            counts.Append(start + i);
        }
        var batch = new RecordBatch(new Schema([new Field("file_path", StringType.Default, false), new Field("record_count", Int64Type.Default, false)], null),
            [paths.Build(), counts.Build()], rows);
        var ms = new MemoryStream();
        AvroWriter.Write(ms, batch.Schema, [batch], new() { WriterSchema = schema, LeaveOpen = true });
        return ms.ToArray();
    }

    private static AvroRecordSchema Iceberg() => S.Record("r2",
    [
        Required("file_path", S.String, 100), Required("record_count", S.Long, 103), Optional("key_metadata", S.Bytes, 131),
    ]);

    [Fact]
    public async Task Cached_reads_equal_uncached_ones()
    {
        var cache = new AvroReadCache();
        foreach (string name in Fixtures)
        {
            byte[] file = Fixture(name);
            var plain = await Read(file, new());
            Same(plain, await Read(file, new() { Cache = cache }));
            Same(plain, await Read(file, new() { Cache = cache })); // the second read hits
            AvroSchema writer = (await AvroReader.OpenAsync(new MemoryStream(file))).WriterSchema;
            Assert.Equal(plain.Rows, (await Read(file, new() { Cache = cache, ReaderSchema = writer })).Rows);
        }
        // Fixtures sharing a writer schema share entries: one without and one with the reader schema per schema.
        int schemas = Fixtures.Select(n => AvroReader.Open(new MemoryStream(Fixture(n))).WriterSchemaJson).Distinct().Count();
        Assert.True(schemas < Fixtures.Length); // empty and many_* reuse the primitives schema
        Assert.Equal(2 * schemas, cache.Count);

        byte[] manifest = Manifest(0, 5);
        var reader = Iceberg();
        var byId = await Read(manifest, new() { ReaderSchema = reader, FieldMatching = AvroFieldMatching.ByFieldId });
        Same(byId, await Read(manifest, new() { ReaderSchema = reader, FieldMatching = AvroFieldMatching.ByFieldId, Cache = cache }));
        Same(byId, await Read(manifest, new() { ReaderSchema = reader, FieldMatching = AvroFieldMatching.ByFieldId, Cache = cache }));
    }

    private static void Same((string Schema, string Json, List<string> Rows) expected, (string Schema, string Json, List<string> Rows) actual)
    {
        Assert.Equal(expected.Schema, actual.Schema);
        Assert.Equal(expected.Json, actual.Json);
        Assert.Equal(expected.Rows, actual.Rows);
    }

    [Fact]
    public async Task Entries_are_keyed_by_content_and_options()
    {
        var cache = new AvroReadCache();
        // Different files, same schema: one entry. A reader schema rebuilt for each file: still one entry.
        for (int i = 0; i < 4; i++)
            await Read(Manifest(i * 10, 3), new() { Cache = cache, ReaderSchema = Iceberg(), FieldMatching = AvroFieldMatching.ByFieldId });
        Assert.Equal(1, cache.Count);
        // Each option that changes the outcome is part of the key.
        await Read(Manifest(0, 3), new() { Cache = cache, ReaderSchema = Iceberg() });                              // by name
        await Read(Manifest(0, 3), new() { Cache = cache, ReaderSchema = Iceberg(), FieldMatching = AvroFieldMatching.ByFieldId, Columns = ["record_count"] });
        await Read(Manifest(0, 3), new() { Cache = cache });                                                        // no reader schema
        await Read(Manifest(0, 3), new() { Cache = cache, ColumnIndices = [1] });
        Assert.Equal(5, cache.Count);
        var projected = await Read(Manifest(0, 3), new() { Cache = cache, Columns = ["record_count"] });
        Assert.Equal(["0", "1", "2"], projected.Rows);
        Assert.Equal(6, cache.Count);
        cache.Clear();
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public async Task The_least_recently_used_entry_is_dropped_and_failures_are_not_cached()
    {
        var cache = new AvroReadCache(capacity: 2);
        byte[] a = Fixture("primitives"), b = Fixture("complex"), c = Fixture("logical");
        await Read(a, new() { Cache = cache });
        await Read(b, new() { Cache = cache });
        await Read(a, new() { Cache = cache }); // a is now the most recent
        await Read(c, new() { Cache = cache }); // drops b
        Assert.Equal(2, cache.Count);
        // A hit never calls the preparation; a miss does.
        static byte[] SchemaBytes(byte[] file) => AvroReader.Open(new MemoryStream(file)).Metadata["avro.schema"];
        PreparedRead Fail() => throw new InvalidOperationException("miss");
        cache.GetOrAdd(SchemaBytes(a), new(), Fail);
        cache.GetOrAdd(SchemaBytes(c), new(), Fail);
        Assert.Throws<InvalidOperationException>(() => cache.GetOrAdd(SchemaBytes(b), new(), Fail));

        var empty = new AvroReadCache();
        await Assert.ThrowsAsync<ArgumentException>(() => Read(a, new() { Cache = empty, Columns = ["nope"] }));
        await Assert.ThrowsAsync<AvroResolutionException>(() => Read(a, new() { Cache = empty, ReaderSchema = S.Record("r", [Required("missing", S.Int)]) }));
        Assert.Equal(0, empty.Count);
        Assert.Throws<ArgumentOutOfRangeException>(() => new AvroReadCache(0));
    }

    [Fact]
    public async Task One_cache_serves_many_threads_and_parallel_decoding()
    {
        byte[][] files = [.. Enumerable.Range(0, 8).Select(i => Manifest(i * 1000, 50 + i))];
        var reader = Iceberg();
        var expected = await Task.WhenAll(files.Select(f => Read(f, new() { ReaderSchema = reader, FieldMatching = AvroFieldMatching.ByFieldId })));
        var cache = new AvroReadCache(capacity: 1);
        await Parallel.ForAsync(0, 400, async (i, _) =>
        {
            int k = i % files.Length;
            var options = new AvroReadOptions
            {
                ReaderSchema = i % 2 == 0 ? reader : Iceberg(), FieldMatching = AvroFieldMatching.ByFieldId, Cache = cache,
                BatchSize = 7, MaxDegreeOfParallelism = i % 3 == 0 ? 2 : 1,
            };
            Assert.Equal(expected[k].Rows, (await Read(files[k], options)).Rows);
        });
        Assert.Equal(1, cache.Count);
        // Field ids survive the cache.
        await using var cached = await AvroReader.OpenAsync(new MemoryStream(files[0]), new() { ReaderSchema = reader, Cache = cache, FieldMatching = AvroFieldMatching.ByFieldId });
        Assert.Equal(100, ArrowFieldIds.Of(cached.Schema.GetFieldByName("file_path")));
        Assert.Equal(3, ((AvroRecordSchema)cached.WriterSchema).Fields.Count);
    }
}
