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

    /// <summary>
    /// Writers racing to create one file: exactly one wins and its content is what is stored. Then a kernel commit
    /// on the store, read back.
    /// </summary>
    public static void PutIfAbsentHolds(string url, CloudArgs args)
    {
        string path = $"race/{Guid.NewGuid():N}.json";
        var wins = new bool[8];
        Parallel.For(0, wins.Length, i =>
        {
            using var fs = new ObjectStoreFileSystem(url, args);
            wins[i] = fs.TryCreate(path, System.Text.Encoding.UTF8.GetBytes($"writer {i}"));
        });
        Assert.Equal(1, wins.Count(w => w));
        using var reader = new ObjectStoreFileSystem(url, args);
        Assert.Equal($"writer {Array.IndexOf(wins, true)}", System.Text.Encoding.UTF8.GetString(reader.ReadAll(path)!));

        reader.Overwrite(path, "replaced"u8.ToArray());
        Assert.Equal("replaced", System.Text.Encoding.UTF8.GetString(reader.ReadAll(path)!));
        Assert.True(reader.Delete(path));
        Assert.Null(reader.ReadAll(path));
        Assert.False(reader.Delete(path));

        // A table created and appended to by the kernel on the store.
        string table = $"kernel_commit_{Guid.NewGuid():N}";
        using var tableFs = new ObjectStoreFileSystem($"{url}/{table}", args);
        var metadata = new Kaminari.Seseragi.Lakehouse.Delta.Actions.MetadataAction
        {
            Id = table,
            SchemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}",
        };
        var protocol = new Kaminari.Seseragi.Lakehouse.Delta.Actions.ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 };
        Assert.Equal(0, Transaction.Create(tableFs, protocol, metadata).Commit());
        Snapshot created = Snapshot.Load(tableFs);
        Assert.Equal(1, Transaction.Start(tableFs, created, "WRITE").Commit());
        // Without retries the taken version is a conflict; with them the commit lands after the other.
        Assert.Throws<DeltaCommitConflictException>(() => Transaction.Start(tableFs, created, "WRITE").Commit(maxRetries: 0));
        Assert.Equal(1, Snapshot.Load(tableFs).Version);
        Assert.Equal(2, Transaction.Start(tableFs, created, "WRITE").Commit());
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
        KernelObjectStoreCheck.PutIfAbsentHolds($"az://{azurite.ContainerName}/{prefix}", args);
    }
}

public class DeltaKernelGcsTests(FakeGcsFixture gcs) : IClassFixture<FakeGcsFixture>
{
    [Fact]
    [Trait("DeltaKernel", "Cloud")]
    public void KernelReads_SampleTables_OnGcs()
    {
        // The emulator checks no credentials, but the client needs some to write: a service account key signs its
        // own tokens (a self-signed JWT), with no token server involved. A throwaway key will do.
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        string key = System.Text.Json.JsonSerializer.Serialize(new Dictionary<string, string>
        {
            ["type"] = "service_account",
            ["project_id"] = "test",
            ["private_key_id"] = "test",
            ["private_key"] = rsa.ExportPkcs8PrivateKeyPem(),
            ["client_email"] = "test@test.iam.gserviceaccount.com",
        });
        var options = CloudOptions.Gcp();
        options.Credentials!["google_base_url"] = gcs.BaseUrl;
        options.Credentials!["google_service_account_key"] = key;

        CloudArgs args = KernelObjectStoreCheck.ToArgs(options);
        foreach (string table in SampleTables.Names)
            KernelObjectStoreCheck.SameAsLocal(table, $"gs://{FakeGcsFixture.BucketName}/{FakeGcsFixture.Prefix}/{table}", args);
        // Writes are not checked here: the object store client uploads through the XML API (PUT /bucket/object with
        // x-goog-if-generation-match: 0), which fake-gcs-server does not implement ("invalid uploadType").
        // Conditional creates on GCS are therefore untested until a real bucket or another emulator is at hand.
    }
}
