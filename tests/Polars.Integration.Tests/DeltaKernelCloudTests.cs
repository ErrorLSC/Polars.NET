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
    }
}
