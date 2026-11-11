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

    /// <summary>
    /// Kernel writes, a rewrite, a compaction and an orphan on the store; recursive listing (escaped partition
    /// names); a full vacuum with no retention deletes exactly what the table no longer needs.
    /// </summary>
    public static void VacuumWorks(string url, CloudArgs args)
    {

        // Partition values with characters that need escaping, a rewrite, a compaction and an orphan.
        using (var first = DataFrame.FromColumns(new { Id = new[] { 1, 2, 3 }, Region = new[] { "a b", "a b", "x/y" } }))
            DeltaKernelWrite.Write(url, Polars.NET.Core.Wrappers.PolarsWrapper.LazyClone(first.Lazy().Handle), KernelWriteMode.ErrorIfExists, ["Region"], args);
        using (var second = DataFrame.FromColumns(new { Id = new[] { 4 }, Region = new[] { "a b" } }))
            DeltaKernelWrite.Write(url, Polars.NET.Core.Wrappers.PolarsWrapper.LazyClone(second.Lazy().Handle), KernelWriteMode.Append, null, args);
        DeltaKernelDelete.Delete(url, Polars.NET.Core.Wrappers.PolarsWrapper.Eq(Polars.NET.Core.Wrappers.PolarsWrapper.Col("Id"), Polars.NET.Core.Wrappers.PolarsWrapper.Lit(2)), cloud: args);
        DeltaKernelOptimize.Optimize(url, cloud: args);
        using (var fs = new ObjectStoreFileSystem(url, args))
            Assert.True(fs.TryCreate("Region=a b/orphan.parquet", new byte[] { 1, 2, 3 }));

        using (var fs = new ObjectStoreFileSystem(url, args))
        {
            var all = fs.ListAll("");
            Assert.Contains(all, f => f.Path == "_delta_log/00000000000000000000.json");
            Assert.Contains(all, f => f.Path == "Region=a b/orphan.parquet");
            Assert.Contains(all, f => f.Path.StartsWith("Region=x%2Fy/", StringComparison.Ordinal));
            Assert.Equal(all.OrderBy(f => f.Path, StringComparer.Ordinal).Select(f => f.Path), all.Select(f => f.Path));
            Assert.Equal(fs.List("_delta_log").Select(f => f.Path), fs.ListAll("_delta_log").Select(f => f.Path));
            Assert.Empty(fs.ListAll("no_such_directory"));
        }

        Thread.Sleep(50); // the orphan's modification time strictly before the cutoff of a zero retention
        var dry = DeltaKernelVacuum.Vacuum(url, 0, enforceRetention: false, dryRun: true, full: true, cloud: args);
        var done = DeltaKernelVacuum.Vacuum(url, 0, enforceRetention: false, full: true, cloud: args);

        Assert.Equal(dry.Files, done.Files);
        Assert.Contains("Region=a b/orphan.parquet", done.Files);
        Assert.True(done.Files.Count >= 3, string.Join(", ", done.Files)); // the rewritten file, the compacted ones, the orphan
        using var latest = new LazyFrame(DeltaKernelScan.Scan(url, cloud: args)).Collect();
        Assert.Equal([1, 3, 4], latest.Sort(["Id"])["Id"].ToArray<int>());
        Assert.Empty(DeltaKernelVacuum.Vacuum(url, 0, enforceRetention: false, dryRun: true, full: true, cloud: args).Files);
    }

    /// <summary>
    /// Streamed kernel writes on the store: files split by row count within escaped partitions, statistics read back
    /// from the footers on the store, and a failing write leaves no data file behind.
    /// </summary>
    public static void StreamedWritesWork(string url, CloudArgs args)
    {
        int[] ids = [.. Enumerable.Range(1, 12)];
        using (var df = DataFrame.FromColumns(new { Id = ids, Region = ids.Select(i => (i % 3) switch { 0 => "a b", 1 => "x/y", _ => "東京 50%" }).ToArray() }))
            DeltaKernelWrite.Write(url, Polars.NET.Core.Wrappers.PolarsWrapper.LazyClone(df.Lazy().Handle), KernelWriteMode.ErrorIfExists,
                ["Region"], args, options: new KernelWriteOptions(MaxRowsPerFile: 2, Compression: Polars.NET.Core.Enums.PlParquetCompression.ZSTD));

        using var fs = new ObjectStoreFileSystem(url, args);
        Snapshot table = Snapshot.Load(fs, parquet: new EngineParquetReader(url, args));
        Assert.Equal(6, table.Files.Count); // 4 rows per partition, 2 per file
        Assert.Contains(table.Files, f => f.PartitionValues["Region"] == "東京 50%");
        Assert.All(table.Files, f => Assert.EndsWith(".zstd.parquet", f.Path));
        long rows = table.Files.Sum(f => System.Text.Json.JsonDocument.Parse(f.Stats!).RootElement.GetProperty("numRecords").GetInt64());
        Assert.Equal(12, rows);
        Assert.Contains(table.Files, f => f.Stats!.Contains("\"minValues\":{\"Id\":", StringComparison.Ordinal));
        using (var latest = new LazyFrame(DeltaKernelScan.Scan(url, cloud: args)).Collect())
            Assert.Equal(ids, latest.Sort(["Id"])["Id"].ToArray<int>());

        // A NOT NULL partition column: a null value fails the write, and nothing it wrote stays.
        string strict = url + "_strict";
        using (var strictFs = new ObjectStoreFileSystem(strict, args))
        {
            var metadata = new Kaminari.Seseragi.Lakehouse.Delta.Actions.MetadataAction
            {
                Id = "strict",
                SchemaString = "{\"type\":\"struct\",\"fields\":[{\"name\":\"Id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"Region\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}",
            };
            Transaction.Create(strictFs, new Kaminari.Seseragi.Lakehouse.Delta.Actions.ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 }, metadata).Commit();
        }
        using (var bad = DataFrame.FromColumns(new { Id = new int?[] { 1, null, 3 }, Region = new[] { "a", "b", "c" } }))
            Assert.ThrowsAny<Exception>(() => DeltaKernelWrite.Write(strict, Polars.NET.Core.Wrappers.PolarsWrapper.LazyClone(bad.Lazy().Handle),
                KernelWriteMode.Append, null, args, options: new KernelWriteOptions(MaxRowsPerFile: 1)));
        using (var strictFs = new ObjectStoreFileSystem(strict, args))
            Assert.DoesNotContain(strictFs.ListAll(""), f => f.Path.EndsWith(".parquet", StringComparison.Ordinal));
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
        using (var fs = new ObjectStoreFileSystem($"az://{azurite.ContainerName}/{prefix}", args))
            Assert.Equal("managed", fs.Implementation);
        foreach (string table in SampleTables.Names)
            KernelObjectStoreCheck.SameAsLocal(table, $"az://{azurite.ContainerName}/{prefix}/{table}", args);
        KernelObjectStoreCheck.PutIfAbsentHolds($"az://{azurite.ContainerName}/{prefix}", args);
    }

    [Fact]
    [Trait("DeltaKernel", "Cloud")]
    public void KernelVacuum_OnAzure_ListsRecursively_AndDeletesWhatTheTableNoLongerNeeds() =>
        KernelObjectStoreCheck.VacuumWorks(
            $"az://{azurite.ContainerName}/kernel_vacuum_{Guid.NewGuid():N}", KernelObjectStoreCheck.ToArgs(Options()));
}

public class DeltaKernelAzureStreamingTests(AzuriteFixture azurite) : IClassFixture<AzuriteFixture>
{
    [Fact]
    [Trait("DeltaKernel", "Cloud")]
    public void KernelStreamedWrites_OnAzure()
    {
        var options = CloudOptions.Azure(
            accountName: AzuriteFixture.AccountName, accessKey: AzuriteFixture.AccountKey, endpoint: azurite.BlobEndpoint);
        options.Credentials!["azure_allow_http"] = "true";
        options.Credentials!["azure_use_emulator"] = "true";
        KernelObjectStoreCheck.StreamedWritesWork(
            $"az://{azurite.ContainerName}/kernel_stream_{Guid.NewGuid():N}", KernelObjectStoreCheck.ToArgs(options));
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
