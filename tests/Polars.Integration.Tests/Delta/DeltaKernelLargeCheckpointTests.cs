using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Storage;
using Polars.Integration.Tests.Fixtures;

namespace Polars.Integration.Tests;

/// <summary>
/// A checkpoint larger than an upload part, written by the kernel straight to an object store through the managed
/// client (in parts, as it is built): the bytes are the reference ones, and the table loads from the checkpoint alone.
/// fake-gcs takes no XML multipart uploads (classic checkpoints replace), so GCS is covered on the real service.
/// </summary>
public class DeltaKernelLargeCheckpointTests(MinioFixture minio, AzuriteFixture azurite) : IClassFixture<MinioFixture>, IClassFixture<AzuriteFixture>
{
    private const int Files = 180_000; // a checkpoint of about 13 MiB

    [Fact]
    [Trait("DeltaKernel", "Cloud")]
    public void ALargeCheckpoint_GoesInParts_OnS3() =>
        Check(MinioKeys.Open($"s3://{minio.BucketName}/large_checkpoint_{Guid.NewGuid():N}", MinioKeys.Of(minio)));

    [Fact]
    [Trait("DeltaKernel", "Cloud")]
    public void ALargeCheckpoint_GoesInParts_OnAzure() =>
        Check(AzuriteKeys.Open($"az://{azurite.ContainerName}/large_checkpoint_{Guid.NewGuid():N}", AzuriteKeys.Of(azurite)));

    /// <summary>Writes a table of many files and its checkpoint to the store, then checks the checkpoint (also used on the real clouds).</summary>
    internal static void Check(CloudFileSystem fs)
    {
        fs.UploadPartSize = 5 * 1024 * 1024; // the stores' minimum: two parts and the rest
        var protocol = new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 };
        var metadata = new MetadataAction
        {
            Id = "large",
            SchemaString = """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"region","type":"string","nullable":true,"metadata":{}}]}""",
            PartitionColumns = ["region"],
        };
        Transaction transaction = Transaction.Create(fs, protocol, metadata);
        for (int i = 0; i < Files; i++)
        {
            string region = (i % 7).ToString(System.Globalization.CultureInfo.InvariantCulture);
            transaction.Add(new AddAction
            {
                Path = $"region={region}/part-{i:D7}-{Guid.NewGuid()}.c000.snappy.parquet",
                PartitionValues = new Dictionary<string, string?> { ["region"] = region },
                Size = 100_000 + i,
                ModificationTime = 1_700_000_000_000 + i,
                Stats = $$$"""{"numRecords":{{{1000 + i % 100}}},"minValues":{"id":{{{i * 1000L}}}},"maxValues":{"id":{{{i * 1000L + 999}}}},"nullCount":{"id":0}}""",
            });
        }
        transaction.Commit();
        Snapshot snapshot = Snapshot.Load(fs);
        var now = DateTimeOffset.UtcNow;

        int requests = Kaminari.Seseragi.Lakehouse.Storage.Http.HttpTransport.CountRequests(() => Checkpoints.Write(fs, snapshot, now));
        byte[] written = fs.ReadAll("_delta_log/00000000000000000000.checkpoint.parquet")!;
        Assert.True(written.Length > 2 * fs.UploadPartSize, $"the checkpoint has {written.Length} bytes");
        Assert.True(requests >= 4, $"{requests} requests: the checkpoint did not go in parts"); // start, two parts, complete
        Assert.True(Checkpoints.ReferenceParquet(snapshot, now).AsSpan().SequenceEqual(written));

        // The table as the checkpoint alone holds it.
        Assert.True(fs.Delete("_delta_log/00000000000000000000.json"));
        Snapshot fromCheckpoint = Snapshot.Load(fs);
        Assert.Equal(Files, fromCheckpoint.Files.Count);
        Assert.Equal(snapshot.Files.Select(f => f.Path).Order(StringComparer.Ordinal), fromCheckpoint.Files.Select(f => f.Path).Order(StringComparer.Ordinal));
    }
}
