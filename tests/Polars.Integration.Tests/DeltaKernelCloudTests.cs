using Polars.CSharp;
using Polars.Integration.Tests.Fixtures;
using Polars.NET.Core.Delta;
using Pl = Polars.CSharp.Polars;

namespace Polars.Integration.Tests;

/// <summary>
/// The managed Delta kernel against an object store: the log is listed and read through the store the engine
/// uses, and the data files are scanned with the same cloud settings.
/// </summary>
public class DeltaKernelCloudTests(MinioFixture minio) : IClassFixture<MinioFixture>
{
    private CloudOptions Options()
    {
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var options = CloudOptions.Aws(
            region: minio.Region,
            accessKey: minio.AccessKey,
            secretKey: minio.SecretKey,
            endpoint: $"http://{rawEndpoint}");
        options.Credentials!["aws_allow_http"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";
        return options;
    }

    private static CloudArgs ToArgs(CloudOptions options)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) =
            CloudOptions.ParseCloudOptions(options);
        return new CloudArgs(provider.ToNative(), retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values);
    }

    [Fact]
    [Trait("DeltaKernel", "Cloud")]
    public void KernelScan_OnS3_MatchesTheEngine_AtEveryVersion()
    {
        var url = $"s3://{minio.BucketName}/kernel_scan_{Guid.NewGuid():N}";
        var options = Options();

        using (var first = DataFrame.FromColumns(new { Id = new[] { 1, 2, 3 }, Region = new[] { "east", "east", "west" } }))
            first.WriteDelta(url, partitionBy: Pl.Selectors.ByName("Region"), mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        using (var second = DataFrame.FromColumns(new { Id = new[] { 4, 5 }, Region = new[] { "west", "north" } }))
            second.WriteDelta(url, partitionBy: Pl.Selectors.ByName("Region"), mode: DeltaSaveMode.Append, cloudOptions: options);
        Delta.Delete(url, Pl.Col("Id") < Pl.Lit(2), cloudOptions: options);

        CloudArgs args = ToArgs(options);
        using var latestEngine = LazyFrame.ScanDelta(url, cloudOptions: options).Collect();
        Assert.Equal(4, latestEngine.Height);

        ulong latestVersion = 0;
        for (ulong v = 0; v < 10; v++)
        {
            DataFrame expected;
            try
            {
                expected = LazyFrame.ScanDelta(url, version: v, cloudOptions: options).Collect();
            }
            catch (Polars.NET.Core.Exceptions.PolarsException)
            {
                break; // past the latest version
            }
            latestVersion = v;

            using (expected)
            using (var viaKernel = new LazyFrame(DeltaKernelScan.Scan(url, (long)v, cloud: args)).Collect())
            {
                Assert.Equal(expected.Height, viaKernel.Height);
                using var left = viaKernel.Select(Pl.Col("Id"), Pl.Col("Region")).Sort(["Id"]);
                using var right = expected.Select(Pl.Col("Id"), Pl.Col("Region")).Sort(["Id"]);
                Assert.Equal(right["Id"].ToArray<int>(), left["Id"].ToArray<int>());
                Assert.Equal(right["Region"].ToArray<string>(), left["Region"].ToArray<string>());
            }
        }
        Assert.True(latestVersion >= 2, $"expected at least three versions, saw up to {latestVersion}");

        // The latest version without naming it, and the file system on its own.
        using var latest = new LazyFrame(DeltaKernelScan.Scan(url, cloud: args)).Collect();
        Assert.Equal(4, latest.Height);

        using var fs = new ObjectStoreFileSystem(url, args);
        var log = fs.List("_delta_log");
        Assert.Contains(log, f => f.Path == "_delta_log/00000000000000000000.json");
        Assert.NotNull(fs.ReadAll("_delta_log/00000000000000000000.json"));
        Assert.Equal(10, fs.ReadRange("_delta_log/00000000000000000000.json", 0, 10)!.Length);
        Assert.Null(fs.ReadAll("_delta_log/99999999999999999999.json"));
        Assert.Empty(fs.List("no_such_directory"));

        // Listing from an offset (S3 start-after): a version prefix, and an exact name, which is included.
        string[] all = [.. log.Select(f => f.Path)];
        Assert.Equal(all.Where(p => string.CompareOrdinal(p, "_delta_log/00000000000000000001") >= 0),
            fs.List("_delta_log", "00000000000000000001").Select(f => f.Path));
        Assert.Equal(all.Where(p => string.CompareOrdinal(p, "_delta_log/00000000000000000001.json") >= 0),
            fs.List("_delta_log", "00000000000000000001.json").Select(f => f.Path));
        Assert.Empty(fs.List("_delta_log", "zzz"));

        // Absolute URIs, as the log may hold: inside the table, and in another location of the bucket.
        byte[] commit0 = fs.ReadAll("_delta_log/00000000000000000000.json")!;
        Assert.Equal(commit0, fs.ReadAll($"{url}/_delta_log/00000000000000000000.json"));
        var other = $"s3://{minio.BucketName}/kernel_other_{Guid.NewGuid():N}";
        using (var third = DataFrame.FromColumns(new { Id = new[] { 9 }, Region = new[] { "south" } }))
            third.WriteDelta(other, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        using var otherFs = new ObjectStoreFileSystem(other, args);
        Assert.Equal(otherFs.ReadAll("_delta_log/00000000000000000000.json"), fs.ReadAll($"{other}/_delta_log/00000000000000000000.json"));
        Assert.Equal(otherFs.ReadRange("_delta_log/00000000000000000000.json", 3, 7), fs.ReadRange($"{other}/_delta_log/00000000000000000000.json", 3, 7));

        // Conditional writes (If-None-Match) and kernel commits on S3.
        KernelObjectStoreCheck.PutIfAbsentHolds($"s3://{minio.BucketName}/writes", args);
    }

    [Fact]
    [Trait("DeltaKernel", "Cloud")]
    public void KernelVacuum_OnS3_ListsRecursively_AndDeletesWhatTheTableNoLongerNeeds()
    {
        var url = $"s3://{minio.BucketName}/kernel_vacuum_{Guid.NewGuid():N}";
        CloudArgs args = ToArgs(Options());

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
}
