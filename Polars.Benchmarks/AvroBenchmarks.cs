using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Avro;

namespace Polars.Benchmarks;

// Avro read and write throughput of Seseragi.Files (managed, no native library involved).
// The data mixes what real files carry: small and 8-byte varints (ids, microsecond timestamps), doubles,
// short strings, a nullable int, a boolean and a short list. Files are written once per codec in the setup.
//
// Filter: --filter '*AvroBenchmarks*'

[MemoryDiagnoser]
public class AvroBenchmarks
{
    private const int Rows = 1_000_000;

    private string _dir = "";
    private List<RecordBatch> _batches = [];

    [Params("null", "snappy", "zstandard")]
    public string Codec { get; set; } = "null";

    private string FilePath => Path.Combine(_dir, $"bench_{Codec}.avro");

    private static IAvroCompression CompressionOf(string codec) => codec switch
    {
        "snappy" => SnappyCompression.Instance,
        "zstandard" => new ZstandardCompression(),
        _ => NoCompression.Instance,
    };

    [GlobalSetup]
    public void Setup()
    {
        _dir = Directory.CreateTempSubdirectory("seseragi-avro-bench").FullName;
        _batches = [.. Enumerable.Range(0, Rows / 100_000).Select(i => Batch(i * 100_000, 100_000))];
        AvroWriter.Write(FilePath, _batches[0].Schema, _batches, new() { Compression = CompressionOf(Codec) });
    }

    [GlobalCleanup]
    public void Cleanup() => Directory.Delete(_dir, true);

    [Benchmark]
    public long Read()
    {
        long rows = 0;
        using var reader = AvroReader.Open(FilePath);
        foreach (var b in reader.ReadBatches())
        {
            rows += b.Length;
            b.Dispose();
        }
        return rows;
    }

    [Benchmark]
    public long ReadParallel()
    {
        long rows = 0;
        using var reader = AvroReader.Open(FilePath, new() { MaxDegreeOfParallelism = -1 });
        foreach (var b in reader.ReadBatches())
        {
            rows += b.Length;
            b.Dispose();
        }
        return rows;
    }

    /// <summary>Reading through schema resolution with the writer's own schema: the plan collapses to the direct
    /// decoders, so this should cost what <see cref="Read"/> costs.</summary>
    [Benchmark]
    public long ReadWithReaderSchema()
    {
        AvroSchema writer;
        using (var probe = AvroReader.Open(FilePath)) writer = probe.WriterSchema;
        long rows = 0;
        using var reader = AvroReader.Open(FilePath, new() { ReaderSchema = writer });
        foreach (var b in reader.ReadBatches())
        {
            rows += b.Length;
            b.Dispose();
        }
        return rows;
    }

    [Benchmark]
    public long ReadProjected()
    {
        long rows = 0;
        using var reader = AvroReader.Open(FilePath, new() { Columns = ["ts", "price"] });
        foreach (var b in reader.ReadBatches())
        {
            rows += b.Length;
            b.Dispose();
        }
        return rows;
    }

    [Benchmark]
    public void Write() =>
        AvroWriter.Write(Stream.Null, _batches[0].Schema, _batches, new() { Compression = CompressionOf(Codec) });

    private static RecordBatch Batch(int start, int n)
    {
        var id = new Int64Array.Builder();
        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var price = new DoubleArray.Builder();
        var name = new StringArray.Builder();
        var qty = new Int32Array.Builder();
        var flag = new BooleanArray.Builder();
        var tags = new ListArray.Builder(Int64Type.Default);
        var tagValues = (Int64Array.Builder)tags.ValueBuilder;
        var epoch = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        for (int k = start; k < start + n; k++)
        {
            id.Append(k);
            ts.Append(epoch.AddTicks(k * 13_337L));
            price.Append(k * 0.25);
            name.Append($"customer_{k % 50_000}");
            if (k % 7 == 0) qty.AppendNull();
            else qty.Append(k % 1000);
            flag.Append(k % 3 == 0);
            tags.Append();
            for (int j = 0; j < k % 4; j++) tagValues.Append(k * 31L + j);
        }
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, false),
            new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), false),
            new Field("price", DoubleType.Default, false),
            new Field("name", StringType.Default, false),
            new Field("qty", Int32Type.Default, true),
            new Field("flag", BooleanType.Default, false),
            new Field("tags", new ListType(new Field("item", Int64Type.Default, false)), false),
        ], null);
        return new RecordBatch(schema,
            [id.Build(), ts.Build(), price.Build(), name.Build(), qty.Build(), flag.Build(), tags.Build()], n);
    }
}
