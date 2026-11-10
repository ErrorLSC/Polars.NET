using Azure.Storage.Blobs;
using Kaminari.Seseragi.Lakehouse.Delta;
using Polars.CSharp;
using Polars.Integration.Tests.Fixtures;
using Polars.NET.Core.Delta;

namespace Polars.Integration.Tests;

/// <summary>
/// The managed Delta kernel on Azure and Google Cloud Storage (emulators): tables written by Spark and Databricks
/// are copied to the store, and reading them there must give what reading the local copy gives.
/// </summary>
public static class KernelObjectStoreCheck
{
    public static CloudArgs ToArgs(CloudOptions options)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) =
            CloudOptions.ParseCloudOptions(options);
        return new CloudArgs(provider.ToNative(), retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values);
    }

    /// <summary>Snapshot and rows of the table at <paramref name="url"/> equal those of the local copy.</summary>
    public static void SameAsLocal(string table, string url, CloudArgs args)
    {
        string local = Path.Combine(SampleTables.Root, table);

        using var store = new ObjectStoreFileSystem(url, args);
        Snapshot viaStore = Snapshot.Load(store, parquet: new EngineParquetReader(url, args));
        Snapshot viaLocal = Snapshot.Load(new Kaminari.Seseragi.Lakehouse.Engine.LocalFileSystem(local), parquet: new EngineParquetReader(local));
        Assert.Equal(viaLocal.Version, viaStore.Version);
        Assert.Equal(viaLocal.Files.Select(f => f.Path + "|" + f.DeletionVector?.UniqueId).Order(),
            viaStore.Files.Select(f => f.Path + "|" + f.DeletionVector?.UniqueId).Order());

        using var expected = new LazyFrame(DeltaKernelScan.Scan(local)).Collect();
        using var actual = new LazyFrame(DeltaKernelScan.Scan(url, cloud: args)).Collect();
        Assert.Equal(expected.Height, actual.Height);
        string[] columns = [.. expected.Columns];
        Assert.Equal(columns, actual.Columns);
        using var left = expected.Sort([.. columns]);
        using var right = actual.Sort([.. columns]);
        Assert.True(left.Equals(right), $"{table} differs\nlocal:\n{left}\nstore:\n{right}");
    }
}

public class DeltaKernelAzureTests(AzuriteFixture azurite) : IClassFixture<AzuriteFixture>
{
    private CloudOptions Options()
    {
        var options = CloudOptions.Azure(
            accountName: AzuriteFixture.AccountName, accessKey: AzuriteFixture.AccountKey, endpoint: azurite.BlobEndpoint);
        options.Credentials!["azure_allow_http"] = "true";
        options.Credentials!["azure_use_emulator"] = "true";
        return options;
    }

    [Fact]
    [Trait("DeltaKernel", "Cloud")]
    public void KernelReads_SampleTables_OnAzure()
    {
        var container = new BlobServiceClient(azurite.ConnectionString, new BlobClientOptions(BlobClientOptions.ServiceVersion.V2025_11_05))
            .GetBlobContainerClient(azurite.ContainerName);
        string prefix = $"kernel_{Guid.NewGuid():N}";
        foreach (string table in SampleTables.Names)
        {
            string root = Path.Combine(SampleTables.Root, table);
            foreach (string file in Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories))
            {
                string name = $"{prefix}/{table}/{Path.GetRelativePath(root, file).Replace('\\', '/')}";
                container.GetBlobClient(name).Upload(file, overwrite: true);
            }
        }

        CloudArgs args = KernelObjectStoreCheck.ToArgs(Options());
        foreach (string table in SampleTables.Names)
            KernelObjectStoreCheck.SameAsLocal(table, $"az://{azurite.ContainerName}/{prefix}/{table}", args);
    }
}

public class DeltaKernelGcsTests(FakeGcsFixture gcs) : IClassFixture<FakeGcsFixture>
{
    [Fact]
    [Trait("DeltaKernel", "Cloud")]
    public void KernelReads_SampleTables_OnGcs()
    {
        var options = CloudOptions.Gcp();
        options.Credentials!["google_base_url"] = gcs.BaseUrl;
        options.Credentials!["google_skip_signature"] = "true";

        CloudArgs args = KernelObjectStoreCheck.ToArgs(options);
        foreach (string table in SampleTables.Names)
            KernelObjectStoreCheck.SameAsLocal(table, $"gs://{FakeGcsFixture.BucketName}/{FakeGcsFixture.Prefix}/{table}", args);
    }
}
