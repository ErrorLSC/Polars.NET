using System.Collections.Concurrent;
using Polars.CSharp;
using Polars.Integration.Tests.Fixtures;
using Seseragi.Lakehouse.Tests;
using Xunit.Abstractions;

namespace Polars.Integration.Tests;

/// <summary>
/// The requests a Delta scan sends to an object store, counted by a forwarding proxy in front of MinIO: the engine
/// scans the table's explicit file list (its scan args expand paths, as Polars' own do) without listing or probing the
/// data files, so a large table on a store costs no request per file beyond reading it.
/// </summary>
[Trait("DeltaKernel", "Cloud")]
public sealed class DeltaNativeScanRequestTests(MinioFixture minio, ITestOutputHelper output) : IClassFixture<MinioFixture>
{
    private static readonly HttpClient Upstream = new(new SocketsHttpHandler { UseProxy = false, AllowAutoRedirect = false });

    /// <summary>Forwards a proxied request (absolute target) as it came, and answers with the store's answer.</summary>
    private static FakeAnswer Forward(SeenRequest request)
    {
        var message = new HttpRequestMessage(new HttpMethod(request.Method), request.PathAndQuery);
        if (request.Content.Length > 0) message.Content = new ByteArrayContent(request.Content);
        foreach (var (name, value) in request.Headers)
        {
            if (name is "Host" or "Content-Length" or "Connection" or "Proxy-Connection") continue;
            if (!message.Headers.TryAddWithoutValidation(name, value)) message.Content?.Headers.TryAddWithoutValidation(name, value);
        }
        using HttpResponseMessage response = Upstream.Send(message);
        byte[] body = response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult();
        List<string> headers = [.. response.Headers.Concat(response.Content.Headers)
            .Where(h => h.Key is not ("Transfer-Encoding" or "Connection" or "Content-Length"))
            .Select(h => $"{h.Key}: {string.Join(", ", h.Value)}")];
        if (response.Content.Headers.ContentLength is long length) headers.Add($"Content-Length: {length}");
        return new FakeAnswer((int)response.StatusCode, "", headers) { Bytes = request.Method == "HEAD" ? [] : body };
    }

    private CloudOptions Options()
    {
        string endpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var options = CloudOptions.Aws(region: minio.Region, accessKey: minio.AccessKey, secretKey: minio.SecretKey, endpoint: $"http://{endpoint}");
        options.Credentials!["aws_allow_http"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        return options;
    }

    [Fact]
    public void A_Scan_Neither_Lists_Nor_Probes_The_Data_Files()
    {
        const int files = 12;
        string table = $"requests_{Guid.NewGuid():N}";
        string url = $"s3://{minio.BucketName}/{table}";
        for (int i = 0; i < files; i++)
        {
            using var batch = DataFrame.FromColumns(new { Id = new[] { i * 10L, i * 10L + 1 }, Name = new[] { $"a{i}", $"b{i}" } });
            batch.WriteDelta(url, mode: i == 0 ? DeltaSaveMode.Overwrite : DeltaSaveMode.Append, cloudOptions: Options());
        }

        using var proxy = new FakeHttpServer(Forward);
        CloudOptions proxied = Options();
        proxied.Credentials!["proxy_url"] = proxy.Url;
        using (DataFrame read = LazyFrame.ScanDelta(url, cloudOptions: proxied).Collect())
            Assert.Equal(2 * files, read.Height);

        SeenRequest[] seen = [.. proxy.Requests];
        bool IsList(SeenRequest r) => r.Method == "GET" && r.PathAndQuery.Contains("list-type=2");
        string KeyOf(SeenRequest r) => new Uri(r.PathAndQuery).AbsolutePath;
        var data = seen.Where(r => !IsList(r) && KeyOf(r).EndsWith(".parquet") && !KeyOf(r).Contains("/_delta_log/")).ToList();
        var lists = seen.Where(IsList).ToList();
        foreach (var group in seen.GroupBy(r => (IsList(r) ? "LIST" : r.Method, KeyOf(r).Contains("/_delta_log/") || IsList(r) ? "log" : "data")))
            output.WriteLine($"{group.Key.Item1,-5} {group.Key.Item2,-4} {group.Count()}");

        Assert.All(lists, r => Assert.Contains("_delta_log", Uri.UnescapeDataString(r.PathAndQuery)));
        var perFile = data.GroupBy(KeyOf).ToDictionary(g => g.Key, g => g.ToList());
        Assert.Equal(files, perFile.Count);
        Assert.All(perFile.Values, requests =>
        {
            Assert.True(requests.Count(r => r.Method == "HEAD") <= 1, "a data file was probed more than once");
            Assert.True(requests.Count <= 3, $"{requests.Count} requests for one data file");
        });
    }
}
