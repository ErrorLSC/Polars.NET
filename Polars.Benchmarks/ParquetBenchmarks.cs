using Apache.Arrow;
using Apache.Arrow.Types;
using BenchmarkDotNet.Attributes;
using Kaminari.Seseragi.Files.Parquet;
using Polars.CSharp;
using Field = Apache.Arrow.Field;
using Schema = Apache.Arrow.Schema;
using TimeUnit = Apache.Arrow.Types.TimeUnit;

namespace Polars.Benchmarks;

// Parquet reads: the managed reader of Seseragi.Files (to Arrow batches, on all cores and on one; and imported into
// Polars) against Polars' native reader, over files of several shapes written by Polars in each codec:
//   flat      1M rows: ids, microsecond timestamps, doubles, a repeated string (dictionary pages), a nullable int, a bool
//   text      1M rows: ids, a long distinct string (PLAIN pages), a slowly changing int (dictionary)
//   nullable  1M rows: six columns of the flat kinds, 30 % nulls each
//   nested    1M rows: list<int32> (0-9 items), struct{a: int64, b: string}, list<string>
//   wide      200k rows × 200 columns (100 int64, 50 double, 50 dictionary strings)
//
// Polars reads with its thread pool (PolarsRead) and with ParallelStrategy.None (no row-group or column parallelism,
// still its pool for decompression). For Polars confined to one thread, run with POLARS_MAX_THREADS=1 in the
// environment (the pool is sized once per process) and compare with ManagedReadOneThread.
//
// Filter: --filter '*ParquetBenchmarks*'

[MemoryDiagnoser]
[WarmupCount(2)]
[IterationCount(10)]
public class ParquetBenchmarks
{
    private string _dir = "";

    [Params("flat", "text", "nullable", "nested", "wide")]
    public string Shape { get; set; } = "flat";

    [Params("none", "snappy", "zstd")]
    public string Codec { get; set; } = "snappy";

    private string FilePath => Path.Combine(_dir, $"bench_{Shape}_{Codec}.parquet");

    [GlobalSetup]
    public void Setup()
    {
        _dir = Directory.CreateTempSubdirectory("seseragi-parquet-bench").FullName;
        using RecordBatch batch = Shape switch
        {
            "text" => Text(1_000_000),
            "nullable" => Nullable(1_000_000),
            "nested" => Nested(1_000_000),
            "wide" => Wide(200_000),
            _ => Flat(1_000_000),
        };
        using DataFrame frame = DataFrame.FromArrow(batch);
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
    public long ManagedRead() => Read(Environment.ProcessorCount);

    [Benchmark]
    public long ManagedReadOneThread() => Read(1);

    private long Read(int parallelism)
    {
        long rows = 0;
        using var file = ParquetFile.Open(FilePath);
        using var reader = file.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = parallelism });
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
        using DataFrame frame = DataFrame.FromArrowStream(reader.ReadArrowStream());
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

    // ------------------------------------------------------------------ data

    private static readonly DateTimeOffset Epoch = new(2024, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static RecordBatch Flat(int n)
    {
        var id = new Int64Array.Builder();
        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var price = new DoubleArray.Builder();
        var name = new StringArray.Builder();
        var qty = new Int32Array.Builder();
        var flag = new BooleanArray.Builder();
        for (int k = 0; k < n; k++)
        {
            id.Append(k);
            ts.Append(Epoch.AddTicks(k * 13_337L));
            price.Append(k * 0.25);
            name.Append($"customer_{k % 5_000}");
            if (k % 7 == 0) qty.AppendNull();
            else qty.Append(k % 1000);
            flag.Append(k % 3 == 0);
        }
        return Batch(n, ("id", id.Build(), false), ("ts", ts.Build(), false), ("price", price.Build(), false),
            ("name", name.Build(), false), ("qty", qty.Build(), true), ("flag", flag.Build(), false));
    }

    private static RecordBatch Text(int n)
    {
        var id = new Int64Array.Builder();
        var text = new StringArray.Builder();
        var slow = new Int64Array.Builder();
        for (int k = 0; k < n; k++)
        {
            id.Append(k);
            text.Append($"user-{k % 50_000} lorem ipsum dolor sit amet, consectetur adipiscing elit #{k}");
            slow.Append(k / 100);
        }
        return Batch(n, ("id", id.Build(), false), ("text", text.Build(), false), ("slow", slow.Build(), false));
    }

    private static RecordBatch Nullable(int n)
    {
        var random = new Random(1);
        var id = new Int64Array.Builder();
        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var price = new DoubleArray.Builder();
        var name = new StringArray.Builder();
        var qty = new Int32Array.Builder();
        var flag = new BooleanArray.Builder();
        for (int k = 0; k < n; k++)
        {
            if (random.Next(10) < 3) id.AppendNull(); else id.Append(k);
            if (random.Next(10) < 3) ts.AppendNull(); else ts.Append(Epoch.AddTicks(k * 13_337L));
            if (random.Next(10) < 3) price.AppendNull(); else price.Append(k * 0.25);
            if (random.Next(10) < 3) name.AppendNull(); else name.Append($"customer_{k % 5_000}");
            if (random.Next(10) < 3) qty.AppendNull(); else qty.Append(k % 1000);
            if (random.Next(10) < 3) flag.AppendNull(); else flag.Append(k % 3 == 0);
        }
        return Batch(n, ("id", id.Build(), true), ("ts", ts.Build(), true), ("price", price.Build(), true),
            ("name", name.Build(), true), ("qty", qty.Build(), true), ("flag", flag.Build(), true));
    }

    private static RecordBatch Nested(int n)
    {
        var ints = new ListArray.Builder(Int32Type.Default);
        var intValues = (Int32Array.Builder)ints.ValueBuilder;
        var strings = new ListArray.Builder(StringType.Default);
        var stringValues = (StringArray.Builder)strings.ValueBuilder;
        var a = new Int64Array.Builder();
        var b = new StringArray.Builder();
        for (int k = 0; k < n; k++)
        {
            ints.Append();
            for (int i = 0; i < k % 10; i++) intValues.Append(k + i);
            strings.Append();
            for (int i = 0; i < k % 4; i++) stringValues.Append($"tag{(k + i) % 300}");
            a.Append(k);
            b.Append($"b{k % 1000}");
        }
        var structType = new StructType([new Field("a", Int64Type.Default, false), new Field("b", StringType.Default, false)]);
        var record = new StructArray(structType, n, [a.Build(), b.Build()], ArrowBuffer.Empty);
        return Batch(n, ("ints", ints.Build(), false), ("record", record, false), ("tags", strings.Build(), false));
    }

    private static RecordBatch Wide(int n)
    {
        List<(string, IArrowArray, bool)> columns = [];
        for (int c = 0; c < 100; c++)
        {
            var values = new Int64Array.Builder();
            for (int k = 0; k < n; k++) values.Append((long)k * (c + 1));
            columns.Add(($"i{c}", values.Build(), false));
        }
        for (int c = 0; c < 50; c++)
        {
            var values = new DoubleArray.Builder();
            for (int k = 0; k < n; k++) values.Append(k * 0.5 + c);
            columns.Add(($"d{c}", values.Build(), false));
        }
        for (int c = 0; c < 50; c++)
        {
            var values = new StringArray.Builder();
            for (int k = 0; k < n; k++) values.Append($"v{c}_{k % 2_000}");
            columns.Add(($"s{c}", values.Build(), false));
        }
        return Batch(n, [.. columns]);
    }

    private static RecordBatch Batch(int n, params (string Name, IArrowArray Array, bool Nullable)[] columns) =>
        new(new Schema([.. columns.Select(c => new Field(c.Name, c.Array.Data.DataType, c.Nullable))], null), [.. columns.Select(c => c.Array)], n);
}
