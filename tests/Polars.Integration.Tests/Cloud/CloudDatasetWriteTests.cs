using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Json;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.Integration.Tests.Fixtures;

namespace Polars.Integration.Tests.Cloud;

/// <summary>
/// Dataset writes (Parquet, Arrow IPC files and streams, NDJSON) to object stores through <see cref="CloudFileStore"/>:
/// Hive-partitioned files listed and read back, a file large enough to go up in parts, files there already refused,
/// added to or replaced, and a failed write deleting what it published.
/// </summary>
public abstract class CloudDatasetWriteContract
{
    /// <summary>A fresh directory URL of the store (for one test).</summary>
    protected abstract string NewRoot();

    protected abstract IReadOnlyDictionary<string, string> Keys { get; }

    /// <summary>The root of the store's bucket (or container) the directories are below.</summary>
    protected abstract string Bucket { get; }

    private static readonly Schema TheSchema = new(
        [new Field("id", Int64Type.Default, false), new Field("k", StringType.Default, true), new Field("s", StringType.Default, true)], null);

    private static readonly string?[] KeyValues = ["a b", "50%", "x/y", null, ""];

    private static RecordBatch Batch(long first, int rows) => new(TheSchema,
    [
        new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => first + i)).Build(),
        new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => KeyValues[(first + i) % KeyValues.Length])).Build(),
        new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => $"value number {first + i} of the dataset")).Build(),
    ], rows);

    private static DatasetWriteOptions Options(string format, IWritableFileStore store) => format switch
    {
        "parquet" => new ParquetDatasetWriteOptions { Store = store },
        "ipc" => new IpcDatasetWriteOptions { Store = store },
        "json" => new JsonDatasetWriteOptions { Store = store },
        _ => new IpcDatasetWriteOptions { Store = store, Writer = new IpcWriteOptions { Format = IpcFormat.Stream } },
    };

    private static List<DatasetWrittenFile> Write(string path, IEnumerable<RecordBatch> batches, DatasetWriteOptions options) => options switch
    {
        ParquetDatasetWriteOptions parquet => [.. ParquetDatasetWriter.Write(path, TheSchema, batches, parquet).Files],
        JsonDatasetWriteOptions json => [.. JsonDatasetWriter.Write(path, TheSchema, batches, json).Files],
        _ => [.. IpcDatasetWriter.Write(path, TheSchema, batches, (IpcDatasetWriteOptions)options).Files],
    };

    /// <summary>(id, k) of every row read back from <paramref name="path"/> (k from the partitions when partitioned), sorted.</summary>
    private static List<string> Read(string format, string path, IFileStore store, bool partitioned)
    {
        Schema? partitions = partitioned ? new Schema([TheSchema.GetFieldByIndex(1)], null) : null;
        using IDisposable dataset = format switch
        {
            "parquet" => ParquetDataset.Open(path, new ParquetDatasetOptions { Store = store, PartitionSchema = partitions }),
            "json" => JsonDataset.Open(path, new JsonDatasetOptions { Store = store, PartitionSchema = partitions }),
            _ => IpcDataset.Open(path, new IpcDatasetOptions { Store = store, PartitionSchema = partitions }),
        };
        List<string> rows = [];
        IDisposable reader;
        IEnumerable<RecordBatch> batches;
        if (dataset is JsonDataset j)
        {
            JsonDatasetReader r = j.OpenReader(new JsonReadOptions { MaxDegreeOfParallelism = 4 });
            (reader, batches) = (r, r.ReadBatches());
        }
        else if (dataset is ParquetDataset p)
        {
            ParquetDatasetReader r = p.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 4 });
            (reader, batches) = (r, r.ReadBatches());
        }
        else
        {
            IpcDatasetReader r = ((IpcDataset)dataset).OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = 4 });
            (reader, batches) = (r, r.ReadBatches());
        }
        using IDisposable open = reader;
        foreach (RecordBatch batch in batches)
        {
            using (batch)
            {
                var ids = (Int64Array)batch.Column("id");
                IArrowArray k = batch.Column(batch.Schema.GetFieldIndex("k"));
                for (int i = 0; i < batch.Length; i++)
                    rows.Add($"{ids.GetValue(i)}|{(k.IsNull(i) ? "null" : k is StringViewArray v ? v.GetString(i) : ((StringArray)k).GetString(i))}");
            }
        }
        return [.. rows.Order(StringComparer.Ordinal)];
    }

    private static List<string> Expected(long first, int rows) =>
        [.. Enumerable.Range(0, rows).Select(i => $"{first + i}|{KeyValues[(first + i) % KeyValues.Length] ?? "null"}").Order(StringComparer.Ordinal)];

    private static async Task<List<string>> List(IFileStore store, string prefix)
    {
        List<string> paths = [];
        await foreach (StoredFile file in store.ListAsync(prefix, recursive: true)) paths.Add(file.Path);
        return [.. paths.Order(StringComparer.Ordinal)];
    }

    [Theory]
    [MemberData(nameof(DatasetFormats.All), MemberType = typeof(DatasetFormats))]
    public async Task APartitionedDataset_IsWrittenAndReadBack(string format)
    {
        string root = NewRoot();
        FileSystemStore store = CloudFileStore.Open(Bucket, Keys);
        List<RecordBatch> batches = [Batch(0, 20_000), Batch(20_000, 7), Batch(20_007, 13_000)];
        List<DatasetWrittenFile> files = Write($"{root}/w", batches, Options(format, store) with { PartitionBy = ["k"], MaxRowsPerFile = 3000 });
        Assert.Equal(files.Select(f => f.Path).Order(StringComparer.Ordinal), await List(store, $"{root}/w/"));
        Assert.Contains(files, f => f.Path.Contains("/w/k=x%2Fy/", StringComparison.Ordinal));
        Assert.Contains(files, f => f.Path.Contains("/w/k=__HIVE_DEFAULT_PARTITION__/", StringComparison.Ordinal));
        Assert.Contains(files, f => f.Path.Contains("/w/k=/", StringComparison.Ordinal));
        Assert.Equal(Expected(0, 33_007), Read(format, $"{root}/w/", store, partitioned: true));
    }

    [Theory]
    [MemberData(nameof(DatasetFormats.All), MemberType = typeof(DatasetFormats))]
    public async Task ALargeFile_GoesUpInParts(string format)
    {
        string root = NewRoot();
        FileSystemStore store = CloudFileStore.Open(Bucket, Keys);
        // Some 30 MB, uncompressed: three upload parts of 10 MiB.
        List<RecordBatch> batches = [.. Enumerable.Range(0, 8).Select(b => Batch(b * 100_000L, 100_000))];
        DatasetWriteOptions options = format == "parquet"
            ? new ParquetDatasetWriteOptions { Store = store, Writer = new ParquetWriteOptions { Compression = NoCompression.Instance, StoreArrowSchema = false } }
            : Options(format, store);
        DatasetWrittenFile file = Assert.Single(Write($"{root}/big", batches, options));
        Assert.True(file.Bytes > 25L << 20, $"{file.Bytes} bytes");
        Assert.Equal(file.Bytes, (await store.StatAsync(file.Path))!.Value.Size);
        Assert.Equal(Expected(0, 800_000), Read(format, $"{root}/big/", store, partitioned: false));
    }

    [Theory]
    [MemberData(nameof(DatasetFormats.All), MemberType = typeof(DatasetFormats))]
    public async Task FilesThere_AreRefusedAddedToOrReplaced(string format)
    {
        string root = NewRoot();
        FileSystemStore store = CloudFileStore.Open(Bucket, Keys);
        Write($"{root}/t2", [Batch(0, 10)], Options(format, store)); // a sibling prefix is another dataset
        Write($"{root}/t", [Batch(0, 100)], Options(format, store) with { PartitionBy = ["k"] });
        Assert.Throws<IOException>(() => Write($"{root}/t", [Batch(0, 10)], Options(format, store)));
        Write($"{root}/t", [Batch(100, 5)], Options(format, store) with { PartitionBy = ["k"], ExistingData = ExistingData.Append });
        Assert.Equal(Expected(0, 105), Read(format, $"{root}/t/", store, partitioned: true));
        // Rows of partition 'a b' only: its old files go, the other partitions stay.
        var fresh = new RecordBatch(TheSchema, [.. Enumerable.Range(0, 3).Select(c => Batch(1000, 1).Column(c))], 1);
        Write($"{root}/t", [fresh], Options(format, store) with { PartitionBy = ["k"], ExistingData = ExistingData.ReplacePartitions });
        List<string> rows = Read(format, $"{root}/t/", store, partitioned: true);
        Assert.Equal(105 - 21 + 1, rows.Count);
        Assert.Contains("1000|a b", rows);
        Assert.DoesNotContain("0|a b", rows);
        Assert.Single(await List(store, $"{root}/t2/"));
    }

    [Theory]
    [MemberData(nameof(DatasetFormats.All), MemberType = typeof(DatasetFormats))]
    public async Task AFailedWrite_DeletesWhatItPublished(string format)
    {
        string root = NewRoot();
        FileSystemStore store = CloudFileStore.Open(Bucket, Keys);
        // The third file's name fails: the two published are deleted.
        DatasetWriteOptions options = Options(format, store) with
        {
            MaxRowsPerFile = 100,
            FileName = n => n.Index < 2 ? $"part-{n.Index}{n.Extension}" : throw new InvalidOperationException("no name"),
        };
        var error = Assert.Throws<InvalidOperationException>(() => Write($"{root}/f", [Batch(0, 250)], options));
        Assert.Equal("no name", error.Message);
        Assert.Empty(await List(store, $"{root}/f/"));
    }

    private sealed record JsonRow(long Id, long K, string S);

    /// <summary>
    /// The engine's JSON entry points on the store (Core's managed JSON): a query sunk as one compressed file and as Hive
    /// partitions, scanned back (a row index, the file path, the partitions as columns).
    /// </summary>
    [Fact]
    public void ManagedJson_SinksAndScansOnTheStore()
    {
        string root = NewRoot();
        Polars.NET.Core.Delta.CloudArgs cloud = Polars.NET.Core.Delta.CloudArgs.ForLocation(root, [.. Keys.Keys], [.. Keys.Values]);
        using var df = Polars.CSharp.DataFrame.From<JsonRow>([.. Enumerable.Range(0, 3000).Select(i => new JsonRow(i, i % 3, $"value {i}"))]);
        Polars.NET.Core.Json.ManagedJson.SinkJson(df.Lazy().CloneHandle(), $"{root}/one.jsonl.gz", Polars.NET.Core.Enums.PlExternalCompression.Gzip, -1,
            true, true, Polars.NET.Core.Enums.PlSyncOnClose.None, false, cloud);
        using (var one = new Polars.CSharp.LazyFrame(Polars.NET.Core.Json.ManagedJson.ScanNdjson($"{root}/one.jsonl.gz", null, null, null, 100, null, false,
                   "row", 0, "file", false, cloud)).Collect())
        {
            Assert.Equal(["row", "Id", "K", "S", "file"], one.Columns);
            Assert.Equal(3000, one.Height);
            Assert.Equal(Enumerable.Range(0, 3000).Select(i => (long)i), one.Column("Id").ToArray<long>());
            Assert.Equal($"{root}/one.jsonl.gz", one.Column("file").ToArray<string>()[0]);
        }
        Polars.NET.Core.Json.ManagedJson.SinkJsonPartitioned(df.Lazy().CloneHandle(), $"{root}/parts", ["K"], false, false, 700, 0,
            Polars.NET.Core.Enums.PlExternalCompression.Uncompressed, -1, true, Polars.NET.Core.Enums.PlSyncOnClose.None, ExistingData.Error, cloud);
        using var parts = new Polars.CSharp.LazyFrame(Polars.NET.Core.Json.ManagedJson.ScanNdjson($"{root}/parts/", null, null, null, 100, null, false,
            null, 0, null, true, cloud)).Collect();
        Assert.Equal(3000, parts.Height);
        Assert.Equal(3000L, parts.Column("K").ToArray<long>().Sum());
        Assert.Equal(Enumerable.Range(0, 3000).Sum(i => (long)i), parts.Column("Id").ToArray<long>().Sum());
    }
}

[Trait("Category", "Integration")]
public sealed class S3CloudDatasetWriteTests(MinioFixture minio) : CloudDatasetWriteContract, IClassFixture<MinioFixture>
{
    protected override string Bucket => $"s3://{minio.BucketName}";

    protected override string NewRoot() => $"{Bucket}/write_{Guid.NewGuid():N}";

    protected override IReadOnlyDictionary<string, string> Keys => MinioKeys.Of(minio);
}

[Trait("Category", "Integration")]
public sealed class AzureCloudDatasetWriteTests(AzuriteFixture azurite) : CloudDatasetWriteContract, IClassFixture<AzuriteFixture>
{
    protected override string Bucket => $"az://{azurite.ContainerName}";

    protected override string NewRoot() => $"{Bucket}/write_{Guid.NewGuid():N}";

    protected override IReadOnlyDictionary<string, string> Keys => AzuriteKeys.Of(azurite);
}

[Trait("Category", "Integration")]
public sealed class GcsCloudDatasetWriteTests(FakeGcsFixture gcs) : CloudDatasetWriteContract, IClassFixture<FakeGcsFixture>
{
    protected override string Bucket => $"gs://{FakeGcsFixture.BucketName}";

    protected override string NewRoot() => $"{Bucket}/write_{Guid.NewGuid():N}";

    protected override IReadOnlyDictionary<string, string> Keys => GcsKeys.Of(gcs);
}
