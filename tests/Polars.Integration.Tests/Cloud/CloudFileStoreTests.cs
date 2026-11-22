using Apache.Arrow;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Storage;
using Kaminari.Seseragi.Lakehouse.Storage.Http;
using Polars.Integration.Tests.Fixtures;

namespace Polars.Integration.Tests.Cloud;

/// <summary>
/// Datasets (Parquet and Arrow IPC files) on object stores through <see cref="CloudFileStore"/>: globs and Hive
/// directories listed, files read at one and several threads, read again from kept footers with fewer requests, keys
/// with spaces and percent signs, and an object replaced between reads failing with its name.
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

    [Theory]
    [MemberData(nameof(DatasetFormats.All), MemberType = typeof(DatasetFormats))]
    public void AGlobOfFiles_ReadsAtOneAndManyThreads_AndAgainWithFewerRequests(string format)
    {
        string root = NewRoot(), ext = DatasetFormats.Extension(format);
        CloudFileSystem fs = FileSystem(root);
        for (int f = 0; f < 4; f++) fs.Overwrite($"data/f{f}.{ext}", DatasetFormats.File(format, f * Rows, Rows, large: f % 2 == 0));
        fs.Overwrite("data/notes.txt", "not a data file"u8.ToArray());
        IFileStore store = CloudFileStore.Open(Bucket, Keys);
        using DatasetFormats.Opened dataset = DatasetFormats.Open(format, $"{root}/data/*.{ext}", store);
        Assert.Equal(4, dataset.Files);
        long[] expected = [.. Enumerable.Range(0, 4 * Rows).Select(i => (long)i)];
        int first = HttpTransport.CountRequests(() => Assert.Equal(expected, dataset.Ids(1)));
        int again = HttpTransport.CountRequests(() => Assert.Equal(expected, dataset.Ids(8)));
        // A footer kept saves requests; NDJSON has none (its files are read whole again).
        if (format == "json") Assert.True(again <= first, $"read again with {again} requests, first with {first}");
        else Assert.True(again < first, $"read again with {again} requests, first with {first}");
        Console.WriteLine($"{GetType().Name} ({format}): 4 files (2 large), first read {first} requests, again {again}");
    }

    [Theory]
    [MemberData(nameof(DatasetFormats.All), MemberType = typeof(DatasetFormats))]
    public void AHiveDirectory_WithSpacesAndPercents_ReadsItsPartitions(string format)
    {
        string root = NewRoot(), ext = DatasetFormats.Extension(format);
        CloudFileSystem fs = FileSystem(root);
        fs.Overwrite($"t/k=a b/part.{ext}", DatasetFormats.File(format, 0, 10, large: false));
        fs.Overwrite($"t/k=c%20d/part.{ext}", DatasetFormats.File(format, 10, 10, large: false)); // a value written percent-encoded: decoded
        fs.Overwrite($"t/k=e%25f/odd name.{ext}", DatasetFormats.File(format, 20, 10, large: false));
        using DatasetFormats.Opened dataset = DatasetFormats.Open(format, $"{root}/t/", CloudFileStore.Open(Bucket, Keys));
        List<(long, string)> rows = [];
        foreach (RecordBatch batch in dataset.Read(4))
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

    [Theory]
    [MemberData(nameof(DatasetFormats.All), MemberType = typeof(DatasetFormats))]
    public void AnObjectReplacedBetweenReads_Fails_NamingIt(string format)
    {
        string root = NewRoot(), ext = DatasetFormats.Extension(format);
        CloudFileSystem fs = FileSystem(root);
        fs.Overwrite($"r/a.{ext}", DatasetFormats.File(format, 0, Rows, large: true));
        fs.Overwrite($"r/b.{ext}", DatasetFormats.File(format, Rows, Rows, large: true));
        using DatasetFormats.Opened dataset = DatasetFormats.Open(format, $"{root}/r/*.{ext}", CloudFileStore.Open(Bucket, Keys));
        Assert.Equal(2 * Rows, dataset.Ids(4).Count);
        fs.Overwrite($"r/b.{ext}", DatasetFormats.File(format, 7 * Rows, Rows, large: true)); // another version (other content: S3's ETag is its digest)
        var error = Assert.ThrowsAny<IOException>(() => dataset.Ids(4));
        Assert.Contains($"{root}/r/b.{ext}", error.Message);
        Assert.Contains("changed while it was read", error.Message);
    }

    [Theory]
    [MemberData(nameof(DatasetFormats.All), MemberType = typeof(DatasetFormats))]
    public void APathWithoutATrailingSlash_IsAFile(string format)
    {
        string root = NewRoot(), ext = DatasetFormats.Extension(format);
        FileSystem(root).Overwrite($"dir/x.{ext}", DatasetFormats.File(format, 0, 10, large: false));
        IFileStore store = CloudFileStore.Open(Bucket, Keys);
        Assert.ThrowsAny<FileNotFoundException>(() => DatasetFormats.Open(format, $"{root}/dir", store));
        using DatasetFormats.Opened dataset = DatasetFormats.Open(format, $"{root}/dir/", store);
        Assert.Equal(1, dataset.Files);
        Assert.Throws<ArgumentException>(() => DatasetFormats.Open(format, $"s3://elsewhere/x.{ext}", store));
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
