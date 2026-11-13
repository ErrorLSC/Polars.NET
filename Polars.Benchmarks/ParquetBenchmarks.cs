using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using Kaminari.Seseragi.Files.Parquet;
using Polars.CSharp;
using Field = Apache.Arrow.Field;
using Schema = Apache.Arrow.Schema;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace Polars.Benchmarks;

// Parquet reads: the managed reader of Seseragi.Files (single-threaded, to Arrow batches; and imported into Polars)
// against Polars' native reader. 1M rows of flat columns as a typical file holds them: ids, microsecond timestamps,
// doubles, a repeated string (dictionary pages), a nullable int and a boolean, written by Polars in each codec.
//
// The managed reader decodes on one thread; Polars reads with its thread pool (Auto) and, for a fairer row, with
// ParallelStrategy.None (no row-group or column parallelism). For Polars confined to one thread throughout, run the
// whole benchmark with POLARS_MAX_THREADS=1 in the environment (the pool is sized once per process).
//
// Filter: --filter '*ParquetBenchmarks*'

[MemoryDiagnoser]
[WarmupCount(1)]
[IterationCount(8)]
public class ParquetBenchmarks
{
    private const int Rows = 1_000_000;

    private string _dir = "";

    [Params("none", "snappy", "zstd")]
    public string Codec { get; set; } = "snappy";

    private string FilePath => Path.Combine(_dir, $"bench_{Codec}.parquet");

    [GlobalSetup]
    public void Setup()
    {
        _dir = Directory.CreateTempSubdirectory("seseragi-parquet-bench").FullName;
        using DataFrame frame = DataFrame.FromArrow(Batch(Rows));
        frame.WriteParquet(FilePath, Codec switch
        {
            "none" => ParquetCompression.None,
            "zstd" => ParquetCompression.ZSTD,
            _ => ParquetCompression.Snappy,
        }, statistics: true);
    }

    [GlobalCleanup]
    public void Cleanup() => Directory.Delete(_dir, true);

    [Benchmark(Baseline = true)]
    public long ManagedRead()
    {
        long rows = 0;
        using var file = ParquetFile.Open(FilePath);
        using var reader = file.OpenReader();
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            rows += batch.Length;
            batch.Dispose();
        }
        return rows;
    }

    [Benchmark]
    public long ManagedReadIntoPolars()
    {
        using var file = ParquetFile.Open(FilePath);
        using var reader = file.OpenReader();
        using DataFrame frame = DataFrame.FromArrowStream(new BatchStream(reader.Schema, reader.ReadBatches()));
        return frame.Height;
    }

    [Benchmark]
    public long PolarsRead()
    {
        using DataFrame frame = DataFrame.ReadParquet(FilePath);
        return frame.Height;
    }

    [Benchmark]
    public long PolarsReadNoParallelism()
    {
        using DataFrame frame = DataFrame.ReadParquet(FilePath, parallel: ParallelStrategy.None);
        return frame.Height;
    }

    private sealed class BatchStream(Schema schema, IEnumerable<RecordBatch> batches) : IArrowArrayStream
    {
        private readonly IEnumerator<RecordBatch> _batches = batches.GetEnumerator();
        public Schema Schema => schema;
        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default) =>
            ValueTask.FromResult(_batches.MoveNext() ? _batches.Current : null);
        public void Dispose() => _batches.Dispose();
    }

    private static RecordBatch Batch(int n)
    {
        var id = new Int64Array.Builder();
        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var price = new DoubleArray.Builder();
        var name = new StringArray.Builder();
        var qty = new Int32Array.Builder();
        var flag = new BooleanArray.Builder();
        var epoch = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);
        for (int k = 0; k < n; k++)
        {
            id.Append(k);
            ts.Append(epoch.AddTicks(k * 13_337L));
            price.Append(k * 0.25);
            name.Append($"customer_{k % 5_000}");
            if (k % 7 == 0) qty.AppendNull();
            else qty.Append(k % 1000);
            flag.Append(k % 3 == 0);
        }
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, false),
            new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), false),
            new Field("price", DoubleType.Default, false),
            new Field("name", StringType.Default, false),
            new Field("qty", Int32Type.Default, true),
            new Field("flag", BooleanType.Default, false),
        ], null);
        return new RecordBatch(schema, [id.Build(), ts.Build(), price.Build(), name.Build(), qty.Build(), flag.Build()], n);
    }
}
