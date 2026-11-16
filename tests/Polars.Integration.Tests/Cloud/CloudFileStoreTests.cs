using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Storage;
using Kaminari.Seseragi.Lakehouse.Storage.Http;
using Polars.Integration.Tests.Fixtures;

namespace Polars.Integration.Tests.Cloud;

/// <summary>
/// Datasets on object stores through <see cref="CloudFileStore"/>: globs and Hive directories listed, files read at
/// one and several threads, read again from kept footers with fewer requests, keys with spaces and percent signs, and
/// an object replaced between reads failing with its name.
/// </summary>
public abstract class CloudDatasetContract
{
    protected const int Rows = 20_000;

    /// <summary>A fresh directory URL of the store (for one test).</summary>
    protected abstract string NewRoot();

    protected abstract IReadOnlyDictionary<string, string> Keys { get; }

    /// <summary>The root of the store's bucket (or container) the directories are below.</summary>
    protected abstract string Bucket { get; }

    private CloudFileSystem FileSystem(string root) =>
        CloudStorage.TryOpen(root, Keys, null, out string? reason) ?? throw new InvalidOperationException(reason);

    protected static byte[] File(long first, int rows, bool large)
    {
        var schema = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, true), new Field("s", StringType.Default, true)], null);
        var ids = new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => first + i)).Build();
        var s = new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => $"value {first + i}")).Build();
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions
        {
            RowGroupRows = Math.Max(1, rows / 4),
            Compression = large ? NoCompression.Instance : new ZstandardCompression(),
        }))
        {
            writer.Write(new RecordBatch(schema, [ids, s], rows));
            writer.Complete();
        }
        byte[] bytes = stream.ToArray();
        if (large) Assert.True(bytes.Length > ParquetMetadata.TailGuess);
        return bytes;
    }

    private static List<long> Ids(ParquetDataset dataset, int threads)
    {
        using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads });
        List<long> ids = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) ids.AddRange(((Int64Array)batch.Column(0)).Values.ToArray());
        }
        return ids;
    }

    [Fact]
    public void AGlobOfFiles_ReadsAtOneAndManyThreads_AndAgainWithFewerRequests()
    {
        string root = NewRoot();
        CloudFileSystem fs = FileSystem(root);
        for (int f = 0; f < 4; f++) fs.Overwrite($"data/f{f}.parquet", File(f * Rows, Rows, large: f % 2 == 0));
        fs.Overwrite("data/notes.txt", "not parquet"u8.ToArray());
        IFileStore store = CloudFileStore.Open(Bucket, Keys);
        using var dataset = ParquetDataset.Open($"{root}/data/*.parquet", new ParquetDatasetOptions { Store = store });
        Assert.Equal(4, dataset.Files.Count);
        long[] expected = [.. Enumerable.Range(0, 4 * Rows).Select(i => (long)i)];
        int first = HttpTransport.CountRequests(() => Assert.Equal(expected, Ids(dataset, 1)));
        int again = HttpTransport.CountRequests(() => Assert.Equal(expected, Ids(dataset, 8)));
        Assert.True(again < first, $"read again with {again} requests, first with {first}");
        Console.WriteLine($"{GetType().Name}: 4 files (2 large), first read {first} requests, again {again}");
    }

    [Fact]
    public void AHiveDirectory_WithSpacesAndPercents_ReadsItsPartitions()
    {
        string root = NewRoot();
        CloudFileSystem fs = FileSystem(root);
        fs.Overwrite("t/k=a b/part.parquet", File(0, 10, large: false));
        fs.Overwrite("t/k=c%20d/part.parquet", File(10, 10, large: false)); // a value written percent-encoded: decoded
        fs.Overwrite("t/k=e%25f/odd name.parquet", File(20, 10, large: false));
        using var dataset = ParquetDataset.Open($"{root}/t/", new ParquetDatasetOptions { Store = CloudFileStore.Open(Bucket, Keys) });
        using var reader = dataset.OpenReader();
        List<(long, string)> rows = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch)
            {
                var ids = (Int64Array)batch.Column(0);
                var k = (StringViewArray)batch.Column(batch.Schema.GetFieldIndex("k"));
                for (int i = 0; i < batch.Length; i++) rows.Add((ids.GetValue(i)!.Value, k.GetString(i)!));
            }
        }
        Assert.Equal(30, rows.Count);
        Assert.Equal(["a b", "c d", "e%f"], rows.Select(r => r.Item2).Distinct().Order(StringComparer.Ordinal));
    }

    [Fact]
    public void AnObjectReplacedBetweenReads_Fails_NamingIt()
    {
        string root = NewRoot();
        CloudFileSystem fs = FileSystem(root);
        fs.Overwrite("r/a.parquet", File(0, Rows, large: true));
        fs.Overwrite("r/b.parquet", File(Rows, Rows, large: true));
        using var dataset = ParquetDataset.Open($"{root}/r/*.parquet", new ParquetDatasetOptions { Store = CloudFileStore.Open(Bucket, Keys) });
        Assert.Equal(2 * Rows, Ids(dataset, 4).Count);
        fs.Overwrite("r/b.parquet", File(7 * Rows, Rows, large: true)); // another version (other content: S3's ETag is its digest)
        var error = Assert.ThrowsAny<IOException>(() => Ids(dataset, 4));
        Assert.Contains($"{root}/r/b.parquet", error.Message);
        Assert.Contains("changed while it was read", error.Message);
    }

    [Fact]
    public void APathWithoutATrailingSlash_IsAFile()
    {
        string root = NewRoot();
        FileSystem(root).Overwrite("dir/x.parquet", File(0, 10, large: false));
        IFileStore store = CloudFileStore.Open(Bucket, Keys);
        Assert.ThrowsAny<FileNotFoundException>(() => ParquetDataset.Open($"{root}/dir", new ParquetDatasetOptions { Store = store }));
        using var dataset = ParquetDataset.Open($"{root}/dir/", new ParquetDatasetOptions { Store = store });
        Assert.Single(dataset.Files);
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open("s3://elsewhere/x.parquet", new ParquetDatasetOptions { Store = store }));
    }
}

[Trait("Category", "Integration")]
public sealed class S3CloudFileStoreTests(MinioFixture minio) : CloudDatasetContract, IClassFixture<MinioFixture>
{
    protected override string Bucket => $"s3://{minio.BucketName}";

    protected override string NewRoot() => $"{Bucket}/store_{Guid.NewGuid():N}";

    protected override IReadOnlyDictionary<string, string> Keys => MinioKeys.Of(minio);
}

[Trait("Category", "Integration")]
public sealed class AzureCloudFileStoreTests(AzuriteFixture azurite) : CloudDatasetContract, IClassFixture<AzuriteFixture>
{
    protected override string Bucket => $"az://{azurite.ContainerName}";

    protected override string NewRoot() => $"{Bucket}/store_{Guid.NewGuid():N}";

    protected override IReadOnlyDictionary<string, string> Keys => AzuriteKeys.Of(azurite);
}

[Trait("Category", "Integration")]
public sealed class GcsCloudFileStoreTests(FakeGcsFixture gcs) : CloudDatasetContract, IClassFixture<FakeGcsFixture>
{
    protected override string Bucket => $"gs://{FakeGcsFixture.BucketName}";

    protected override string NewRoot() => $"{Bucket}/store_{Guid.NewGuid():N}";

    protected override IReadOnlyDictionary<string, string> Keys => GcsKeys.Of(gcs);
}
