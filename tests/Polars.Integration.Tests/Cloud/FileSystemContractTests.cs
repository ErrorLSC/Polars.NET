using System.Text;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Storage;
using Kaminari.Seseragi.Lakehouse.Storage.Azure;
using Kaminari.Seseragi.Lakehouse.Storage.Gcs;
using Kaminari.Seseragi.Lakehouse.Storage.S3;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Polars.CSharp;
using Polars.Integration.Tests.Fixtures;
using Polars.NET.Core.Delta;

namespace Polars.Integration.Tests;

/// <summary>
/// What the Delta kernel relies on from a file system, independent of where the files live. Every implementation
/// (local disk, the native object store, the managed object store per provider) runs the same checks, so a
/// replacement is accepted only when it behaves as the one it replaces.
/// </summary>
public abstract class FileSystemContract
{
    /// <summary>A fresh, empty root no other test uses.</summary>
    protected abstract string NewRoot();

    /// <summary>A file system rooted at <paramref name="root"/>.</summary>
    protected abstract IWritableFileSystem Open(string root);

    /// <summary>The absolute URI of a file below <paramref name="root"/>, as a table log would write it (percent-encoded).</summary>
    protected virtual string AbsoluteUri(string root, string relative) =>
        root.TrimEnd('/') + "/" + string.Join('/', relative.Split('/').Select(Uri.EscapeDataString));

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string Text(byte[]? bytes) => bytes is null ? "<null>" : Encoding.UTF8.GetString(bytes);

    private static void Dispose(IWritableFileSystem fs) => (fs as IDisposable)?.Dispose();

    private void With(Action<IWritableFileSystem, string> check)
    {
        string root = NewRoot();
        IWritableFileSystem fs = Open(root);
        try { check(fs, root); }
        finally { Dispose(fs); }
    }

    [Fact]
    public void List_GivesDirectChildren_InOrdinalOrder_WithSizeAndTime() => With((fs, _) =>
    {
        DateTimeOffset before = DateTimeOffset.UtcNow;
        string[] names = ["b.json", "a.json", "B.json", "a.json.tmp", "_sub/x.json", "dir/inner.json"];
        foreach (string name in names)
            Assert.True(fs.TryCreate($"log/{name}", Bytes(name)));

        IReadOnlyList<FileEntry> entries = fs.List("log");
        Assert.Equal(["log/B.json", "log/a.json", "log/a.json.tmp", "log/b.json"], entries.Select(e => e.Path));
        Assert.All(entries, e => Assert.Equal(e.Path.Length - "log/".Length, e.Size));
        Assert.All(entries, e => Assert.InRange(e.LastModified, before.AddMinutes(-5), DateTimeOffset.UtcNow.AddMinutes(5)));

        // A trailing slash names the same directory; the root lists as "".
        Assert.Equal(entries.Select(e => e.Path), fs.List("log/").Select(e => e.Path));
        Assert.Empty(fs.List(""));
        Assert.True(fs.TryCreate("top.json", Bytes("top")));
        Assert.Equal(["top.json"], fs.List("").Select(e => e.Path));
    });

    [Fact]
    public void List_FromAnOffset_IsInclusive() => With((fs, _) =>
    {
        string[] names = ["00.json", "01.json", "02.json", "02.checkpoint.parquet", "03.json"];
        foreach (string name in names)
            Assert.True(fs.TryCreate($"_delta_log/{name}", Bytes(name)));

        Assert.Equal(["_delta_log/02.checkpoint.parquet", "_delta_log/02.json", "_delta_log/03.json"],
            fs.List("_delta_log", "02").Select(e => e.Path));
        Assert.Equal(["_delta_log/02.json", "_delta_log/03.json"], fs.List("_delta_log", "02.json").Select(e => e.Path));
        Assert.Equal(["_delta_log/03.json"], fs.List("_delta_log", "02.zzz").Select(e => e.Path));
        Assert.Empty(fs.List("_delta_log", "99"));
        Assert.Equal(5, fs.List("_delta_log", "").Count);

        // The entry of the file named exactly the offset reports the same size and time as a plain listing.
        FileEntry listed = fs.List("_delta_log").Single(e => e.Path == "_delta_log/02.json");
        FileEntry fromOffset = fs.List("_delta_log", "02.json")[0];
        Assert.Equal(listed.Path, fromOffset.Path);
        Assert.Equal(listed.Size, fromOffset.Size);
        Assert.InRange((fromOffset.LastModified - listed.LastModified).Duration(), TimeSpan.Zero, TimeSpan.FromSeconds(1));
    });

    [Fact]
    public void List_OfAMissingDirectory_IsEmpty_AndPrefixesStopAtSegments() => With((fs, _) =>
    {
        Assert.Empty(fs.List("missing"));
        Assert.Empty(fs.ListAll("missing"));
        Assert.True(fs.TryCreate("dir2/x.json", Bytes("x")));
        Assert.True(fs.TryCreate("dirty.json", Bytes("x")));
        Assert.Empty(fs.List("dir"));
        Assert.Empty(fs.ListAll("dir"));
        Assert.Empty(fs.List("dir", "x"));
    });

    [Fact]
    public void ListAll_IsRecursive_Ordered_AndRelativeToTheRoot() => With((fs, _) =>
    {
        string[] files = ["t/a=1/p.parquet", "t/a=1/q.parquet", "t/a=2/b=x/r.parquet", "t/_delta_log/0.json", "t/z.parquet", "other.json"];
        foreach (string file in files) Assert.True(fs.TryCreate(file, Bytes(file)));

        IReadOnlyList<FileEntry> all = fs.ListAll("t");
        Assert.Equal(files.Where(f => f.StartsWith("t/", StringComparison.Ordinal)).Order(StringComparer.Ordinal), all.Select(e => e.Path));
        Assert.All(all, e => Assert.Equal(e.Path.Length, e.Size));
        Assert.Equal(files.Order(StringComparer.Ordinal), fs.ListAll("").Select(e => e.Path));
        Assert.Equal(["t/a=2/b=x/r.parquet"], fs.ListAll("t/a=2/").Select(e => e.Path));
    });

    [Fact]
    public void Reads_WholeFiles_Ranges_AndMissingFiles() => With((fs, _) =>
    {
        byte[] content = [.. Enumerable.Range(0, 1000).Select(i => (byte)i)];
        Assert.True(fs.TryCreate("data.bin", content));

        Assert.Equal(content, fs.ReadAll("data.bin"));
        Assert.Equal(content[10..30], fs.ReadRange("data.bin", 10, 20));
        Assert.Equal(content[990..], fs.ReadRange("data.bin", 990, 100)); // cut where the file ends
        Assert.Equal(content[..], fs.ReadRange("data.bin", 0, 1000));
        Assert.Empty(fs.ReadRange("data.bin", 1000, 8)!);
        Assert.Empty(fs.ReadRange("data.bin", 5000, 8)!);
        Assert.Empty(fs.ReadRange("data.bin", 3, 0)!);

        Assert.Null(fs.ReadAll("missing.bin"));
        Assert.Null(fs.ReadRange("missing.bin", 0, 4));
        Assert.Null(fs.ReadAll("missing_dir/missing.bin"));
    });

    [Fact]
    public void EmptyAndLargeFiles_RoundTrip() => With((fs, _) =>
    {
        Assert.True(fs.TryCreate("empty", ReadOnlyMemory<byte>.Empty));
        Assert.Empty(fs.ReadAll("empty")!);
        Assert.Equal(0, Assert.Single(fs.List("")).Size);

        // Larger than common single-request buffers and multipart part sizes.
        var large = new byte[12 * 1024 * 1024 + 17];
        new Random(42).NextBytes(large);
        fs.Overwrite("large.bin", large);
        Assert.Equal(large, fs.ReadAll("large.bin"));
        Assert.Equal(large[^8..], fs.ReadRange("large.bin", large.Length - 8, 8));
        Assert.Equal(large.Length, fs.List("").Single(e => e.Path == "large.bin").Size);
    });

    [Fact]
    public void Tails_GiveTheEndAndTheSize_InOneCall() => With((fs, root) =>
    {
        var tails = Assert.IsAssignableFrom<ITailReadable>(fs);
        Assert.True(tails.CanReadTails);

        byte[] content = [.. Enumerable.Range(0, 100).Select(i => (byte)i)];
        fs.Overwrite("dir/data.bin", content);
        Assert.True(fs.TryCreate("empty", ReadOnlyMemory<byte>.Empty));

        FileTail end = tails.ReadTail("dir/data.bin", 10)!.Value;
        Assert.Equal(100, end.Size);
        Assert.Equal(content[^10..], end.Bytes);
        FileTail all = tails.ReadTail("dir/data.bin", 1000)!.Value;
        Assert.Equal(100, all.Size);
        Assert.Equal(content, all.Bytes);
        FileTail empty = tails.ReadTail("empty", 10)!.Value;
        Assert.Equal(0, empty.Size);
        Assert.Empty(empty.Bytes);
        Assert.Null(tails.ReadTail("missing.bin", 10));
        Assert.Equal(content[^4..], tails.ReadTail(AbsoluteUri(root, "dir/data.bin"), 4)!.Value.Bytes);
    });

    [Fact]
    public void TryCreate_RefusesAnExistingFile_AndOverwriteReplacesIt() => With((fs, _) =>
    {
        Assert.True(fs.TryCreate("dir/one.json", Bytes("first")));
        Assert.False(fs.TryCreate("dir/one.json", Bytes("second")));
        Assert.Equal("first", Text(fs.ReadAll("dir/one.json")));

        fs.Overwrite("dir/one.json", Bytes("third"));
        Assert.Equal("third", Text(fs.ReadAll("dir/one.json")));
        fs.Overwrite("dir/two.json", Bytes("created"));
        Assert.Equal("created", Text(fs.ReadAll("dir/two.json")));
    });

    [Fact]
    public void TryCreate_HasOneWinner_AmongRacingWriters() => With((_, root) =>
    {
        string path = "_delta_log/00000000000000000001.json";
        var wins = new bool[8];
        Parallel.For(0, wins.Length, i =>
        {
            IWritableFileSystem fs = Open(root);
            try { wins[i] = fs.TryCreate(path, Bytes($"writer {i}")); }
            finally { Dispose(fs); }
        });
        Assert.Equal(1, wins.Count(w => w));
        IWritableFileSystem reader = Open(root);
        try { Assert.Equal($"writer {Array.IndexOf(wins, true)}", Text(reader.ReadAll(path))); }
        finally { Dispose(reader); }
    });

    [Fact]
    public void Delete_SaysWhetherThereWasAFile() => With((fs, _) =>
    {
        Assert.True(fs.TryCreate("dir/gone.json", Bytes("x")));
        Assert.True(fs.TryCreate("dir/kept.json", Bytes("x")));
        Assert.True(fs.Delete("dir/gone.json"));
        Assert.False(fs.Delete("dir/gone.json"));
        Assert.False(fs.Delete("never/was.json"));
        Assert.Null(fs.ReadAll("dir/gone.json"));
        Assert.Equal(["dir/kept.json"], fs.List("dir").Select(e => e.Path));
        Assert.True(fs.TryCreate("dir/gone.json", Bytes("again"))); // a deleted name can be created again
    });

    [Fact]
    public void Names_NeedingEscapes_RoundTrip() => With((fs, _) =>
    {
        // Hive-escaped partition values keep their literal %XX; spaces, '=', '+', unicode and '%' as they are.
        string[] files = ["p=a b/x.parquet", "p=x%2Fy/x.parquet", "p=東京 50%/x.parquet", "p=1+1=2/x.parquet", "p=a&b@c;d/x.parquet"];
        foreach (string file in files) Assert.True(fs.TryCreate(file, Bytes(file)));

        Assert.Equal(files.Order(StringComparer.Ordinal), fs.ListAll("").Select(e => e.Path));
        foreach (string file in files)
        {
            Assert.Equal(file, Text(fs.ReadAll(file)));
            string directory = file[..file.LastIndexOf('/')];
            Assert.Equal([file], fs.List(directory).Select(e => e.Path));
        }
        Assert.True(fs.Delete("p=x%2Fy/x.parquet"));
        Assert.Null(fs.ReadAll("p=x%2Fy/x.parquet"));
    });

    [Fact]
    public void AbsoluteUris_AreRead_InsideAndOutsideTheRoot() => With((fs, root) =>
    {
        Assert.True(fs.TryCreate("dv/deletion vector 1.bin", Bytes("inside")));
        Assert.Equal("inside", Text(fs.ReadAll(AbsoluteUri(root, "dv/deletion vector 1.bin"))));
        Assert.Equal("side", Text(fs.ReadRange(AbsoluteUri(root, "dv/deletion vector 1.bin"), 2, 4)));
        Assert.Null(fs.ReadAll(AbsoluteUri(root, "dv/missing.bin")));

        // A file of another location (a shallow clone's source, say), read through this file system.
        string other = NewRoot();
        IWritableFileSystem otherFs = Open(other);
        try
        {
            Assert.True(otherFs.TryCreate("data/part 1=a.parquet", Bytes("outside")));
            Assert.Equal("outside", Text(fs.ReadAll(AbsoluteUri(other, "data/part 1=a.parquet"))));
            Assert.Equal("outs", Text(fs.ReadRange(AbsoluteUri(other, "data/part 1=a.parquet"), 0, 4)));
        }
        finally { Dispose(otherFs); }
    });
}

[Trait("FileSystemContract", "Local")]
public sealed class LocalFileSystemContractTests : FileSystemContract, IDisposable
{
    private readonly string _base = Path.Combine(Path.GetTempPath(), $"fs_contract_{Guid.NewGuid():N}");

    protected override string NewRoot() => Path.Combine(_base, Guid.NewGuid().ToString("N"));

    protected override IWritableFileSystem Open(string root) => new LocalFileSystem(root);

    protected override string AbsoluteUri(string root, string relative) =>
        new Uri(Path.Combine(root, relative.Replace('/', Path.DirectorySeparatorChar))).AbsoluteUri;

    public void Dispose()
    {
        if (Directory.Exists(_base)) Directory.Delete(_base, recursive: true);
    }
}

[Trait("FileSystemContract", "Cloud")]
public sealed class NativeS3FileSystemContractTests(MinioFixture minio) : FileSystemContract, IClassFixture<MinioFixture>
{
    private CloudArgs Args()
    {
        var rawEndpoint = minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var options = CloudOptions.Aws(region: minio.Region, accessKey: minio.AccessKey, secretKey: minio.SecretKey, endpoint: $"http://{rawEndpoint}");
        options.Credentials!["aws_allow_http"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        return KernelObjectStoreCheck.ToArgs(options);
    }

    protected override string NewRoot() => $"s3://{minio.BucketName}/contract_{Guid.NewGuid():N}";

    protected override IWritableFileSystem Open(string root) => new ObjectStoreFileSystem(root, Args(), ObjectStoreBackend.Native);
}

[Trait("FileSystemContract", "Cloud")]
public sealed class NativeAzureFileSystemContractTests(AzuriteFixture azurite) : FileSystemContract, IClassFixture<AzuriteFixture>
{
    private CloudArgs Args()
    {
        var options = CloudOptions.Azure(accountName: AzuriteFixture.AccountName, accessKey: AzuriteFixture.AccountKey, endpoint: azurite.BlobEndpoint);
        options.Credentials!["azure_allow_http"] = "true";
        options.Credentials!["azure_use_emulator"] = "true";
        return KernelObjectStoreCheck.ToArgs(options);
    }

    protected override string NewRoot() => $"az://{azurite.ContainerName}/contract_{Guid.NewGuid():N}";

    protected override IWritableFileSystem Open(string root) => new ObjectStoreFileSystem(root, Args(), ObjectStoreBackend.Native);
}

/// <summary>The settings of the MinIO container as configuration keys.</summary>
internal static class MinioKeys
{
    public static Dictionary<string, string> Of(MinioFixture minio) => new()
    {
        ["aws_access_key_id"] = minio.AccessKey,
        ["aws_secret_access_key"] = minio.SecretKey,
        ["aws_region"] = minio.Region,
        ["aws_endpoint"] = "http://" + minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/'),
        ["aws_allow_http"] = "true",
    };

    public static CloudFileSystem Open(string root, IReadOnlyDictionary<string, string> keys, int? pageSize = null)
    {
        CloudFileSystem fs = CloudStorage.TryOpen(root, keys, RetrySettings.Default, out string? reason)
            ?? throw new InvalidOperationException(reason);
        if (pageSize is { } size) ((S3Client)fs.Client).PageSize = size;
        return fs;
    }
}

[Trait("FileSystemContract", "Cloud")]
public sealed class ManagedS3FileSystemContractTests(MinioFixture minio) : FileSystemContract, IClassFixture<MinioFixture>
{
    protected override string NewRoot() => $"s3://{minio.BucketName}/managed_{Guid.NewGuid():N}";

    protected override IWritableFileSystem Open(string root) => MinioKeys.Open(root, MinioKeys.Of(minio));

    [Fact]
    public void TheFacade_ServesMinio_WithTheManagedClient()
    {
        using var fs = new ObjectStoreFileSystem(NewRoot(), CloudArgs.ForLocation("s3://x", [.. MinioKeys.Of(minio).Keys], [.. MinioKeys.Of(minio).Values]));
        Assert.Equal("managed", fs.Implementation);
    }

    [Fact]
    public void TheFacade_FallsBack_ForSettingsTheManagedClientLacks_UnlessManagedIsRequired()
    {
        string root = NewRoot();
        Assert.True(MinioKeys.Open(root, MinioKeys.Of(minio)).TryCreate("a.json", Encoding.UTF8.GetBytes("a")));

        var keys = MinioKeys.Of(minio);
        keys["aws_checksum_algorithm"] = "sha256"; // MinIO takes the checksums; the managed client does not send them
        keys["aws_s3_force_path_style"] = "true";
        CloudArgs args = CloudArgs.ForLocation(root, [.. keys.Keys], [.. keys.Values]);
        using (var fs = new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Auto))
        {
            Assert.Equal("native", fs.Implementation);
            Assert.Equal("a", Encoding.UTF8.GetString(fs.ReadAll("a.json")!));
        }
        var refused = Assert.Throws<NotSupportedException>(() => new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Managed));
        Assert.Contains("checksum", refused.Message);
    }

    [Fact]
    public void ATableUnderARootWithEscapes_KeepsLogAndDataTogether()
    {
        // The engine writes the data files, the managed client the log: both must read the root the same way.
        string root = $"s3://{minio.BucketName}/esc {Guid.NewGuid():N}/t%41 b";
        var keys = MinioKeys.Of(minio);
        var options = CloudOptions.Aws(region: minio.Region, accessKey: minio.AccessKey, secretKey: minio.SecretKey, endpoint: keys["aws_endpoint"]);
        options.Credentials!["aws_allow_http"] = "true";
        using (var df = DataFrame.FromColumns(new { Id = new[] { 1, 2, 3 } }))
            df.WriteDelta(root, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        using (var read = LazyFrame.ScanDelta(root, cloudOptions: options).Collect())
            Assert.Equal(3, read.Height);

        var fs = MinioKeys.Open(root, keys);
        var all = fs.ListAll("");
        Assert.Contains(all, f => f.Path == "_delta_log/00000000000000000000.json");
        Assert.Contains(all, f => f.Path.EndsWith(".parquet", StringComparison.Ordinal) && !f.Path.Contains('/'));
    }

    [Fact]
    public void TemporaryCredentials_WithASessionToken_Work()
    {
        // MinIO's STS hands out temporary credentials for its own user, as a catalog vends them for a table.
        var (keyId, secret, token) = AssumeRole(minio);
        var keys = MinioKeys.Of(minio);
        keys["aws_access_key_id"] = keyId;
        keys["aws_secret_access_key"] = secret;
        keys["aws_session_token"] = token;

        string root = NewRoot();
        var fs = MinioKeys.Open(root, keys);
        Assert.True(fs.TryCreate("_delta_log/0.json", Encoding.UTF8.GetBytes("{}")));
        Assert.Equal("{}", Encoding.UTF8.GetString(fs.ReadAll("_delta_log/0.json")!));
        Assert.Single(fs.List("_delta_log"));

        // Without the token the temporary key is refused.
        keys.Remove("aws_session_token");
        var refused = Assert.Throws<StorageException>(() => MinioKeys.Open(root, keys).ReadAll("_delta_log/0.json"));
        Assert.Equal(System.Net.HttpStatusCode.Forbidden, refused.Status);
    }

    [Fact]
    public void InstanceCredentials_FromAMetadataService_ServeBothStores()
    {
        // A stand-in for EC2's metadata service hands out MinIO's temporary credentials; no keys are configured.
        AmbientCredentials.AssertNone("AWS_");
        var (keyId, secret, token) = AssumeRole(minio);
        string expiration = DateTime.UtcNow.AddMinutes(14).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);
        string json = $$"""{"Code":"Success","AccessKeyId":"{{keyId}}","SecretAccessKey":"{{secret}}","Token":"{{token}}","Expiration":"{{expiration}}"}""";
        using var imds = new Seseragi.Lakehouse.Tests.FakeHttpServer(r => (r.Method, r.PathAndQuery) switch
        {
            ("PUT", "/latest/api/token") => (200, "session"),
            ("GET", "/latest/meta-data/iam/security-credentials/") => (200, "test-role"),
            ("GET", "/latest/meta-data/iam/security-credentials/test-role") => (200, json),
            _ => (404, ""),
        });

        var keys = MinioKeys.Of(minio);
        keys.Remove("aws_access_key_id");
        keys.Remove("aws_secret_access_key");
        keys["aws_metadata_endpoint"] = imds.Url;
        string root = NewRoot();
        CloudArgs args = CloudArgs.ForLocation(root, [.. keys.Keys], [.. keys.Values]);
        using (var managed = new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Managed))
        {
            Assert.True(managed.TryCreate("_delta_log/0.json", Encoding.UTF8.GetBytes("{}")));
            Assert.Single(managed.List("_delta_log"));
        }
        using (var native = new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Native))
            Assert.Equal("{}", Encoding.UTF8.GetString(native.ReadAll("_delta_log/0.json")!));
        Assert.Contains(imds.Requests, r => r.Method == "PUT");
    }

    [Fact]
    public void PodIdentityCredentials_ServeBothStores()
    {
        // A stand-in for the EKS Pod Identity agent: a full URI and a token file, answering with MinIO's credentials.
        AmbientCredentials.AssertNone("AWS_");
        var (keyId, secret, token) = AssumeRole(minio);
        string expiration = DateTime.UtcNow.AddMinutes(14).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);
        string json = $$"""{"AccessKeyId":"{{keyId}}","SecretAccessKey":"{{secret}}","Token":"{{token}}","Expiration":"{{expiration}}"}""";
        using var agent = new Seseragi.Lakehouse.Tests.FakeHttpServer(r =>
            r.PathAndQuery == "/v1/credentials" && r.Headers.GetValueOrDefault("Authorization") == "pod-token" ? (200, json) : (401, ""));
        string tokenFile = Path.GetTempFileName();
        try
        {
            File.WriteAllText(tokenFile, "pod-token");
            var keys = MinioKeys.Of(minio);
            keys.Remove("aws_access_key_id");
            keys.Remove("aws_secret_access_key");
            keys["aws_container_credentials_full_uri"] = agent.Url + "/v1/credentials";
            keys["aws_container_authorization_token_file"] = tokenFile;
            string root = NewRoot();
            CloudArgs args = CloudArgs.ForLocation(root, [.. keys.Keys], [.. keys.Values]);
            using (var managed = new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Managed))
                Assert.True(managed.TryCreate("x.json", Encoding.UTF8.GetBytes("x")));
            using (var native = new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Native))
                Assert.Equal("x", Encoding.UTF8.GetString(native.ReadAll("x.json")!));
        }
        finally
        {
            File.Delete(tokenFile);
        }
    }

    [Fact]
    public void WithoutAnyCredentials_TheManagedStoreFailsQuickly_OnTheMetadataService()
    {
        // Off the cloud nothing answers at the metadata address; the request fails within seconds (as the engine's).
        // On an EC2 machine the instance's own credentials would be found instead.
        AmbientCredentials.AssertNone("AWS_");
        var keys = MinioKeys.Of(minio);
        keys.Remove("aws_access_key_id");
        keys.Remove("aws_secret_access_key");
        string root = NewRoot();
        using var fs = new ObjectStoreFileSystem(root, CloudArgs.ForLocation(root, [.. keys.Keys], [.. keys.Values]), ObjectStoreBackend.Managed);
        var watch = System.Diagnostics.Stopwatch.StartNew();
        var failure = Assert.Throws<StorageException>(() => fs.List("_delta_log"));
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(20), $"took {watch.Elapsed}: {failure.Message}");
        Assert.Contains("No AWS credentials are configured", failure.Message);
    }

    private static (string KeyId, string Secret, string Token) AssumeRole(MinioFixture minio)
    {
        string endpoint = MinioKeys.Of(minio)["aws_endpoint"];
        var uri = new Uri(endpoint + "/");
        byte[] body = Encoding.UTF8.GetBytes("Action=AssumeRole&Version=2011-06-15&DurationSeconds=900");
        DateTimeOffset now = DateTimeOffset.UtcNow;
        string hash = SigV4.Hash(body);
        var headers = new Dictionary<string, string>
        {
            ["host"] = $"{uri.Host}:{uri.Port}",
            ["content-type"] = "application/x-www-form-urlencoded",
            ["x-amz-date"] = SigV4.AmzDate(now),
            ["x-amz-content-sha256"] = hash,
        };
        string authorization = SigV4.Authorization("POST", "/", "", headers, hash,
            new AwsCredential(minio.AccessKey, minio.SecretKey, null), minio.Region, "sts", now);
        using var client = new HttpClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, uri) { Content = new ByteArrayContent(body) };
        request.Content.Headers.TryAddWithoutValidation("Content-Type", "application/x-www-form-urlencoded");
        request.Headers.TryAddWithoutValidation("x-amz-date", headers["x-amz-date"]);
        request.Headers.TryAddWithoutValidation("x-amz-content-sha256", hash);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);
        using HttpResponseMessage response = client.Send(request);
        string xml = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        Assert.True(response.IsSuccessStatusCode, xml);
        var doc = System.Xml.Linq.XDocument.Parse(xml);
        string Value(string name) => doc.Descendants().First(e => e.Name.LocalName == name).Value;
        return (Value("AccessKeyId"), Value("SecretAccessKey"), Value("SessionToken"));
    }
}

/// <summary>The contract with two keys per listing page, so every listing crosses page boundaries.</summary>
[Trait("FileSystemContract", "Cloud")]
public sealed class ManagedS3SmallPagesContractTests(MinioFixture minio) : FileSystemContract, IClassFixture<MinioFixture>
{
    protected override string NewRoot() => $"s3://{minio.BucketName}/pages_{Guid.NewGuid():N}";

    protected override IWritableFileSystem Open(string root) => MinioKeys.Open(root, MinioKeys.Of(minio), pageSize: 2);
}

/// <summary>The settings of the Azurite container as configuration keys.</summary>
internal static class AzuriteKeys
{
    public static Dictionary<string, string> Of(AzuriteFixture azurite) => new()
    {
        ["azure_storage_account_name"] = AzuriteFixture.AccountName,
        ["azure_storage_account_key"] = AzuriteFixture.AccountKey,
        ["azure_storage_endpoint"] = azurite.BlobEndpoint,
        ["azure_allow_http"] = "true",
    };

    public static CloudFileSystem Open(string root, IReadOnlyDictionary<string, string> keys, int? pageSize = null)
    {
        CloudFileSystem fs = CloudStorage.TryOpen(root, keys, RetrySettings.Default, out string? reason)
            ?? throw new InvalidOperationException(reason);
        if (pageSize is { } size) ((AzureClient)fs.Client).PageSize = size;
        return fs;
    }

    public static BlobContainerClient Container(AzuriteFixture azurite) =>
        new BlobServiceClient(azurite.ConnectionString, new BlobClientOptions(BlobClientOptions.ServiceVersion.V2025_11_05))
            .GetBlobContainerClient(azurite.ContainerName);
}

[Trait("FileSystemContract", "Cloud")]
public sealed class ManagedAzureFileSystemContractTests(AzuriteFixture azurite) : FileSystemContract, IClassFixture<AzuriteFixture>
{
    protected override string NewRoot() => $"az://{azurite.ContainerName}/managed_{Guid.NewGuid():N}";

    protected override IWritableFileSystem Open(string root) => AzuriteKeys.Open(root, AzuriteKeys.Of(azurite));

    private static void RoundTrip(IWritableFileSystem fs)
    {
        Assert.True(fs.TryCreate("_delta_log/0.json", Encoding.UTF8.GetBytes("{}")));
        Assert.False(fs.TryCreate("_delta_log/0.json", Encoding.UTF8.GetBytes("{}")));
        Assert.Equal("{}", Encoding.UTF8.GetString(fs.ReadAll("_delta_log/0.json")!));
        Assert.Single(fs.List("_delta_log"));
        Assert.True(fs.Delete("_delta_log/0.json"));
        Assert.Empty(fs.List("_delta_log"));
    }

    [Fact]
    public void TheFacade_ServesAzurite_WithTheManagedClient()
    {
        var keys = AzuriteKeys.Of(azurite);
        using var fs = new ObjectStoreFileSystem(NewRoot(), CloudArgs.ForLocation("az://x", [.. keys.Keys], [.. keys.Values]));
        Assert.Equal("managed", fs.Implementation);
        RoundTrip(fs);
    }

    [Fact]
    public void TheFacade_FallsBack_ForSettingsTheManagedClientLacks_UnlessManagedIsRequired()
    {
        string root = NewRoot();
        Assert.True(AzuriteKeys.Open(root, AzuriteKeys.Of(azurite)).TryCreate("a.json", Encoding.UTF8.GetBytes("a")));

        var keys = AzuriteKeys.Of(azurite);
        keys["azure_use_fabric_endpoint"] = "true"; // the endpoint key wins in the engine; the managed client refuses Fabric
        CloudArgs args = CloudArgs.ForLocation(root, [.. keys.Keys], [.. keys.Values]);
        using (var fs = new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Auto))
        {
            Assert.Equal("native", fs.Implementation);
            Assert.Equal("a", Encoding.UTF8.GetString(fs.ReadAll("a.json")!));
        }
        var refused = Assert.Throws<NotSupportedException>(() => new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Managed));
        Assert.Contains("Fabric", refused.Message);
    }

    [Fact]
    public void ASasToken_FromTheSdk_Works_AndItsPermissionsHold()
    {
        BlobContainerClient container = AzuriteKeys.Container(azurite);
        string Sas(BlobContainerSasPermissions permissions) =>
            container.GenerateSasUri(permissions, DateTimeOffset.UtcNow.AddHours(1)).Query;

        var keys = AzuriteKeys.Of(azurite);
        keys.Remove("azure_storage_account_key");
        keys["azure_storage_sas_token"] = Sas(BlobContainerSasPermissions.All);
        string root = NewRoot();
        RoundTrip(AzuriteKeys.Open(root, keys));

        keys["azure_storage_sas_token"] = Sas(BlobContainerSasPermissions.Read | BlobContainerSasPermissions.List);
        var readOnly = AzuriteKeys.Open(root, keys);
        Assert.Empty(readOnly.List("_delta_log"));
        var refused = Assert.Throws<StorageException>(() => readOnly.TryCreate("_delta_log/1.json", Encoding.UTF8.GetBytes("{}")));
        Assert.Equal(System.Net.HttpStatusCode.Forbidden, refused.Status);
    }

    [Fact]
    public void TheEmulatorSetting_TakesItsUrlFromTheEnvironment_AndTheWellKnownKey()
    {
        var blob = new Uri(azurite.BlobEndpoint);
        var environment = new Dictionary<string, string> { ["AZURITE_BLOB_STORAGE_URL"] = $"http://{blob.Host}:{blob.Port}" };
        var keys = new Dictionary<string, string> { ["azure_storage_use_emulator"] = "true", ["azure_storage_endpoint"] = "http://unused.invalid" };
        CloudFileSystem fs = CloudStorage.TryOpen(NewRoot(), keys, RetrySettings.Default, environment, null, out string? reason)
            ?? throw new InvalidOperationException(reason);
        RoundTrip(fs);
    }

    [Fact]
    public void ATableUnderAnAbfssRootWithEscapes_KeepsLogAndDataTogether()
    {
        // The engine writes the data files, the managed client the log: both must read the root the same way.
        string root = $"abfss://{azurite.ContainerName}@{AzuriteFixture.AccountName}.dfs.core.windows.net/esc {Guid.NewGuid():N}/t%41 b";
        var options = CloudOptions.Azure(accountName: AzuriteFixture.AccountName, accessKey: AzuriteFixture.AccountKey, endpoint: azurite.BlobEndpoint);
        options.Credentials!["azure_allow_http"] = "true";
        using (var fs = new ObjectStoreFileSystem(root, KernelObjectStoreCheck.ToArgs(options)))
            Assert.Equal("managed", fs.Implementation);
        using (var df = DataFrame.FromColumns(new { Id = new[] { 1, 2, 3 } }))
            df.WriteDelta(root, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        using (var read = LazyFrame.ScanDelta(root, cloudOptions: options).Collect())
            Assert.Equal(3, read.Height);

        var all = AzuriteKeys.Open(root, AzuriteKeys.Of(azurite)).ListAll("");
        Assert.Contains(all, f => f.Path == "_delta_log/00000000000000000000.json");
        Assert.Contains(all, f => f.Path.EndsWith(".parquet", StringComparison.Ordinal) && !f.Path.Contains('/'));
    }

    [Fact]
    public void AnAccountInTheLocation_IsUsed_AndItsFilesAreTheSameAsUnderTheContainer()
    {
        string folder = $"abfs_{Guid.NewGuid():N}";
        var keys = AzuriteKeys.Of(azurite);
        keys["azure_storage_account_name"] = "someoneelse"; // the location's account wins (and signs)
        var hadoop = AzuriteKeys.Open($"abfss://{azurite.ContainerName}@{AzuriteFixture.AccountName}.dfs.core.windows.net/{folder}", keys);
        Assert.True(hadoop.TryCreate("x.json", Encoding.UTF8.GetBytes("x")));

        var plain = AzuriteKeys.Open($"az://{azurite.ContainerName}/{folder}", AzuriteKeys.Of(azurite));
        Assert.Equal("x", Encoding.UTF8.GetString(plain.ReadAll("x.json")!));
        Assert.Equal("x", Encoding.UTF8.GetString(plain.ReadAll($"abfss://{azurite.ContainerName}@{AzuriteFixture.AccountName}.dfs.core.windows.net/{folder}/x.json")!));
    }
}

/// <summary>The contract with two blobs per listing page, so every listing crosses page boundaries.</summary>
[Trait("FileSystemContract", "Cloud")]
public sealed class ManagedAzureSmallPagesContractTests(AzuriteFixture azurite) : FileSystemContract, IClassFixture<AzuriteFixture>
{
    protected override string NewRoot() => $"az://{azurite.ContainerName}/pages_{Guid.NewGuid():N}";

    protected override IWritableFileSystem Open(string root) => AzuriteKeys.Open(root, AzuriteKeys.Of(azurite), pageSize: 2);
}

/// <summary>The settings of the fake-gcs-server container as configuration keys.</summary>
internal static class GcsKeys
{
    /// <summary>
    /// A throwaway service account key: the emulator checks no tokens, but clients sign their own (a self-signed
    /// JWT) and need a key for it.
    /// </summary>
    public static string ThrowawayKey()
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        using var buffer = new MemoryStream();
        using (var writer = new System.Text.Json.Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("type", "service_account");
            writer.WriteString("project_id", "test");
            writer.WriteString("private_key_id", "test");
            writer.WriteString("private_key", rsa.ExportPkcs8PrivateKeyPem());
            writer.WriteString("client_email", "test@test.iam.gserviceaccount.com");
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    private static readonly Lazy<string> Key = new(ThrowawayKey);

    public static Dictionary<string, string> Of(FakeGcsFixture gcs) => new()
    {
        ["google_base_url"] = gcs.BaseUrl,
        ["google_service_account_key"] = Key.Value,
    };

    public static CloudArgs Args(FakeGcsFixture gcs)
    {
        var options = CloudOptions.Gcp();
        foreach (var (key, value) in Of(gcs)) options.Credentials![key] = value;
        return KernelObjectStoreCheck.ToArgs(options);
    }

    public static CloudFileSystem Open(string root, IReadOnlyDictionary<string, string> keys, int? pageSize = null)
    {
        CloudFileSystem fs = CloudStorage.TryOpen(root, keys, RetrySettings.Default, out string? reason)
            ?? throw new InvalidOperationException(reason);
        if (pageSize is { } size) ((GcsClient)fs.Client).PageSize = size;
        return fs;
    }
}

[Trait("FileSystemContract", "Cloud")]
public sealed class ManagedGcsFileSystemContractTests(FakeGcsFixture gcs) : FileSystemContract, IClassFixture<FakeGcsFixture>
{
    protected override string NewRoot() => $"gs://{FakeGcsFixture.BucketName}/managed_{Guid.NewGuid():N}";

    protected override IWritableFileSystem Open(string root) => GcsKeys.Open(root, GcsKeys.Of(gcs));

    [Fact]
    public void TheFacade_ServesTheEmulator_WithTheManagedClient()
    {
        using var fs = new ObjectStoreFileSystem(NewRoot(), GcsKeys.Args(gcs));
        Assert.Equal("managed", fs.Implementation);
        Assert.True(fs.TryCreate("_delta_log/0.json", Encoding.UTF8.GetBytes("{}")));
        Assert.False(fs.TryCreate("_delta_log/0.json", Encoding.UTF8.GetBytes("{}")));
        Assert.Single(fs.List("_delta_log"));
    }

    [Fact]
    public void TheFacade_FallsBack_ForSettingsTheManagedClientLacks_UnlessManagedIsRequired()
    {
        var keys = GcsKeys.Of(gcs);
        // An https proxy (bypassed for the emulator): the engine connects directly, the managed client takes http proxies only.
        keys["google_proxy_url"] = "https://127.0.0.1:1";
        keys["google_proxy_excludes"] = "127.0.0.1";
        string root = $"gs://{FakeGcsFixture.BucketName}/{FakeGcsFixture.Prefix}/{SampleTables.Names[0]}";
        CloudArgs args = CloudArgs.ForLocation(root, [.. keys.Keys], [.. keys.Values]);
        using (var fs = new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Auto))
        {
            Assert.Equal("native", fs.Implementation);
            Assert.NotEmpty(fs.List("_delta_log"));
        }
        var refused = Assert.Throws<NotSupportedException>(() => new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Managed));
        Assert.Contains("http:// proxies", refused.Message);
    }

    [Fact]
    public void AVendedOAuthToken_AloneIsEnough_ForTheEngineAndBothStores()
    {
        // Catalogs vend a bare token (google_oauth_token). Without a service account the engine used to look for the
        // metadata server; the emulator checks no tokens, so success shows the token was taken instead.
        var options = CloudOptions.Gcp();
        options.Credentials!["google_base_url"] = gcs.BaseUrl;
        options.Credentials!["google_oauth_token"] = "ya29.vended";
        CloudArgs args = KernelObjectStoreCheck.ToArgs(options);
        string table = SampleTables.Names[0];
        string root = $"gs://{FakeGcsFixture.BucketName}/{FakeGcsFixture.Prefix}/{table}";

        using (var native = new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Native))
            Assert.NotEmpty(native.List("_delta_log"));
        using (var managed = new ObjectStoreFileSystem(root, args))
            Assert.Equal("managed", managed.Implementation);
        KernelObjectStoreCheck.SameAsLocal(table, root, args);
    }

    [Fact]
    public void TheNativeStore_ReadsTails_OnGcs()
    {
        // The emulator takes no native writes, but reads: the end of a preloaded data file, as the local copy has it.
        string table = SampleTables.Names[0];
        string local = Path.Combine(SampleTables.Root, table);
        string file = Path.GetRelativePath(local, Directory.EnumerateFiles(local, "*.parquet").First()).Replace('\\', '/');
        byte[] bytes = File.ReadAllBytes(Path.Combine(local, file));
        using var native = new ObjectStoreFileSystem($"gs://{FakeGcsFixture.BucketName}/{FakeGcsFixture.Prefix}/{table}", GcsKeys.Args(gcs), ObjectStoreBackend.Native);
        FileTail tail = native.ReadTail(file, 100)!.Value;
        Assert.Equal(bytes.Length, tail.Size);
        Assert.Equal(bytes[^100..], tail.Bytes);
        Assert.Null(native.ReadTail("missing.parquet", 100));
    }

    [Fact]
    public void ATableUnderARootWithEscapes_IsReadTheSameByTheEngine()
    {
        // The emulator takes no engine writes (XML uploads), so the managed client uploads a table and the engine
        // reads its data files: both must read the root the same way.
        string table = SampleTables.Names[0];
        string root = $"gs://{FakeGcsFixture.BucketName}/esc {Guid.NewGuid():N}/t%41 b";
        var fs = GcsKeys.Open(root, GcsKeys.Of(gcs));
        string local = Path.Combine(SampleTables.Root, table);
        foreach (string file in Directory.EnumerateFiles(local, "*", SearchOption.AllDirectories))
            fs.Overwrite(Path.GetRelativePath(local, file).Replace('\\', '/'), File.ReadAllBytes(file));
        KernelObjectStoreCheck.SameAsLocal(table, root, GcsKeys.Args(gcs));
    }
}

/// <summary>The contract with two objects per listing page, so every listing crosses page boundaries.</summary>
[Trait("FileSystemContract", "Cloud")]
public sealed class ManagedGcsSmallPagesContractTests(FakeGcsFixture gcs) : FileSystemContract, IClassFixture<FakeGcsFixture>
{
    protected override string NewRoot() => $"gs://{FakeGcsFixture.BucketName}/pages_{Guid.NewGuid():N}";

    protected override IWritableFileSystem Open(string root) => GcsKeys.Open(root, GcsKeys.Of(gcs), pageSize: 2);
}

/// <summary>
/// Azure token credentials against stand-ins (Azurite takes OAuth only over https): one server plays Microsoft
/// Entra ID or the managed identity endpoint, and the blob service, which lists only for the token it issued.
/// Both stores must get the token and use it.
/// </summary>
[Trait("FileSystemContract", "Cloud")]
public sealed class AzureTokenCredentialTests
{
    private const string EmptyListing =
        """<?xml version="1.0" encoding="utf-8"?><EnumerationResults ContainerName="data"><Blobs /><NextMarker /></EnumerationResults>""";

    private static (int, string) Blobs(Seseragi.Lakehouse.Tests.SeenRequest r, string token) =>
        r.PathAndQuery.StartsWith("/acct/data?", StringComparison.Ordinal) && r.PathAndQuery.Contains("comp=list")
            ? r.Headers.GetValueOrDefault("Authorization") == "Bearer " + token ? (200, EmptyListing) : (403, "")
            : (404, "");

    private static void BothStoresList(string serverUrl, Dictionary<string, string> keys)
    {
        AmbientCredentials.AssertNone("AZURE_");
        keys["azure_storage_account_name"] = "acct";
        keys["azure_storage_endpoint"] = serverUrl + "/acct";
        keys["azure_allow_http"] = "true";
        string root = $"az://data/t_{Guid.NewGuid():N}";
        CloudArgs args = CloudArgs.ForLocation(root, [.. keys.Keys], [.. keys.Values]);
        using (var managed = new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Managed))
            Assert.Empty(managed.List("_delta_log"));
        using (var native = new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Native))
            Assert.Empty(native.List("_delta_log"));
    }

    [Fact]
    public void AClientSecret_ServesBothStores()
    {
        using var server = new Seseragi.Lakehouse.Tests.FakeHttpServer(r =>
            r is { Method: "POST", PathAndQuery: "/tenant-1/oauth2/v2.0/token" } && r.Body.Contains("client_secret=s3cret")
                ? (200, """{"token_type":"Bearer","expires_in":3599,"access_token":"from-secret"}""")
                : Blobs(r, "from-secret"));
        BothStoresList(server.Url, new()
        {
            ["azure_client_id"] = "cid", ["azure_client_secret"] = "s3cret", ["azure_tenant_id"] = "tenant-1",
            ["azure_authority_host"] = server.Url,
        });
        Assert.Equal(2, server.Requests.Count(r => r.Method == "POST"));
    }

    [Fact]
    public void WorkloadIdentity_ServesBothStores()
    {
        string tokenFile = Path.GetTempFileName();
        try
        {
            File.WriteAllText(tokenFile, "federated-jwt");
            using var server = new Seseragi.Lakehouse.Tests.FakeHttpServer(r =>
                r is { Method: "POST", PathAndQuery: "/tenant-2/oauth2/v2.0/token" } && r.Body.Contains("client_assertion=federated-jwt")
                    ? (200, """{"token_type":"Bearer","expires_in":3599,"access_token":"from-federation"}""")
                    : Blobs(r, "from-federation"));
            BothStoresList(server.Url, new()
            {
                ["azure_client_id"] = "cid", ["azure_tenant_id"] = "tenant-2", ["azure_federated_token_file"] = tokenFile,
                ["azure_authority_host"] = server.Url,
            });
            Assert.Equal(2, server.Requests.Count(r => r.Method == "POST"));
        }
        finally
        {
            File.Delete(tokenFile);
        }
    }

    [Fact]
    public void AManagedIdentity_ServesBothStores()
    {
        string expiresOn = DateTimeOffset.UtcNow.AddHours(1).ToUnixTimeSeconds().ToString(System.Globalization.CultureInfo.InvariantCulture);
        using var server = new Seseragi.Lakehouse.Tests.FakeHttpServer(r =>
            r.PathAndQuery.StartsWith("/msi?", StringComparison.Ordinal) && r.Headers.GetValueOrDefault("metadata") == "true"
                ? (200, $$"""{"access_token":"from-msi","expires_on":"{{expiresOn}}","token_type":"Bearer"}""")
                : Blobs(r, "from-msi"));
        BothStoresList(server.Url, new() { ["azure_msi_endpoint"] = server.Url + "/msi" });
        Assert.Equal(2, server.Requests.Count(r => r.PathAndQuery.StartsWith("/msi?", StringComparison.Ordinal)));
    }
}

/// <summary>
/// S3 request headers against a stand-in that records them: encryption with a customer key and requester pays must
/// reach the store the same way from both stores.
/// </summary>
[Trait("FileSystemContract", "Cloud")]
public sealed class S3RequestHeaderTests
{
    [Fact]
    public void EncryptionAndRequesterPays_AreSentAsTheEngineSendsThem()
    {
        using var s3 = new Seseragi.Lakehouse.Tests.FakeHttpServer(r => r.Method switch
        {
            "PUT" => (200, ""),
            "GET" => (200, "x"),
            _ => (405, ""),
        })
        {
            ResponseHeaders = ["ETag: \"e\"", "Last-Modified: Wed, 11 Nov 2026 09:00:00 GMT"],
        };
        string customerKey = Convert.ToBase64String(Enumerable.Range(0, 32).Select(i => (byte)i).ToArray());
        var keys = new Dictionary<string, string>
        {
            ["aws_access_key_id"] = "id", ["aws_secret_access_key"] = "secret", ["aws_region"] = "us-east-1",
            ["aws_endpoint"] = s3.Url, ["aws_allow_http"] = "true", ["aws_request_payer"] = "true",
            ["aws_server_side_encryption"] = "sse-c", ["aws_sse_customer_key_base64"] = customerKey,
        };
        string root = "s3://bucket/t";
        CloudArgs args = CloudArgs.ForLocation(root, [.. keys.Keys], [.. keys.Values]);

        Dictionary<string, string> Sent(ObjectStoreBackend backend, string method)
        {
            while (s3.Requests.TryDequeue(out _)) { }
            using (var fs = new ObjectStoreFileSystem(root, args, backend))
            {
                fs.Overwrite("a.json", Encoding.UTF8.GetBytes("x"));
                Assert.Equal("x", Encoding.UTF8.GetString(fs.ReadAll("a.json")!));
            }
            var request = s3.Requests.First(r => r.Method == method);
            Assert.Contains("x-amz-request-payer", request.Headers["Authorization"]); // signed
            return request.Headers.Where(h => h.Key.StartsWith("x-amz-server-side", StringComparison.OrdinalIgnoreCase)
                    || h.Key.Equals("x-amz-request-payer", StringComparison.OrdinalIgnoreCase))
                .ToDictionary(h => h.Key.ToLowerInvariant(), h => h.Value);
        }

        string[] methods = ["PUT", "GET"];
        foreach (string method in methods)
        {
            var native = Sent(ObjectStoreBackend.Native, method);
            Assert.Equal(4, native.Count);
            Assert.Equal(native, Sent(ObjectStoreBackend.Managed, method));
        }
    }
}

/// <summary>
/// Credential-chain tests remove the configured keys and expect the chain to reach a stand-in. Credentials in the
/// environment or in <c>~/.aws/credentials</c> come first in that chain (for both stores, as in the engine), so the
/// tests need a machine without them; this says which one is in the way.
/// </summary>
internal static class AmbientCredentials
{
    public static void AssertNone(string variablePrefix)
    {
        var found = Environment.GetEnvironmentVariables().Keys.Cast<string>()
            .Where(name => name.StartsWith(variablePrefix, StringComparison.Ordinal) && name is not ("AWS_REGION" or "AWS_DEFAULT_REGION"))
            .ToList();
        if (variablePrefix == "AWS_")
        {
            string file = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".aws", "credentials");
            if (File.Exists(file)) found.Add(file);
        }
        Assert.True(found.Count == 0,
            "This test needs a machine without ambient cloud credentials (they come first in the credential chain): " + string.Join(", ", found));
    }
}

/// <summary>
/// Connection options against stand-ins (an S3 that answers anything, served over https with certificates from a
/// throwaway authority, and a proxy): certificate checks, a trusted extra authority, a proxy with its bypass list
/// and https through a proxy's tunnel. Every case runs on both stores, with its negative.
/// </summary>
[Trait("FileSystemContract", "Cloud")]
public sealed class S3ConnectionOptionTests : IDisposable
{
    private readonly Seseragi.Lakehouse.Tests.TestCertificates _certificates = new();

    public void Dispose() => _certificates.Dispose();

    private static (int, string) AnyS3(Seseragi.Lakehouse.Tests.SeenRequest r) => r.Method switch
    {
        "PUT" => (200, ""),
        "GET" => (200, "x"),
        _ => (405, ""),
    };

    private static readonly string[] S3Headers = ["ETag: \"e\"", "Last-Modified: Wed, 11 Nov 2026 09:00:00 GMT"];

    private static Seseragi.Lakehouse.Tests.FakeHttpServer S3(System.Security.Cryptography.X509Certificates.X509Certificate2? certificate = null) =>
        new(AnyS3, certificate) { ResponseHeaders = S3Headers };

    /// <summary>Writes and reads one file through each store; the failures, by store (none: it worked).</summary>
    private static Dictionary<ObjectStoreBackend, string?> RoundTrip(string endpoint, Dictionary<string, string> extra)
    {
        var keys = new Dictionary<string, string>(extra)
        {
            ["aws_access_key_id"] = "id", ["aws_secret_access_key"] = "secret", ["aws_region"] = "us-east-1",
            ["aws_endpoint"] = endpoint, ["aws_allow_http"] = "true",
        };
        string root = "s3://bucket/t";
        var cloud = new CloudOptions { Provider = CloudProvider.Aws, Credentials = keys, MaxRetries = 0 };
        CloudArgs args = KernelObjectStoreCheck.ToArgs(cloud);
        var outcome = new Dictionary<ObjectStoreBackend, string?>();
        ObjectStoreBackend[] backends = [ObjectStoreBackend.Managed, ObjectStoreBackend.Native];
        foreach (var backend in backends)
        {
            try
            {
                using var fs = new ObjectStoreFileSystem(root, args, backend);
                fs.Overwrite("a.json", Encoding.UTF8.GetBytes("x"));
                Assert.Equal("x", Encoding.UTF8.GetString(fs.ReadAll("a.json")!));
                outcome[backend] = null;
            }
            catch (Exception e) when (e is not Xunit.Sdk.XunitException)
            {
                outcome[backend] = e.Message;
            }
        }
        return outcome;
    }

    private static void BothWork(Dictionary<ObjectStoreBackend, string?> outcome) =>
        Assert.True(outcome.Values.All(v => v is null), "expected both stores to work: " + string.Join(" | ", outcome.Select(o => $"{o.Key}: {o.Value}")));

    private static void BothFail(Dictionary<ObjectStoreBackend, string?> outcome) =>
        Assert.True(outcome.Values.All(v => v is not null), "expected both stores to fail: " + string.Join(" | ", outcome.Select(o => $"{o.Key}: {o.Value ?? "worked"}")));

    [Fact]
    public void UncheckedCertificates_AreAccepted_OnlyWhenAllowed()
    {
        using var s3 = S3(_certificates.Server());
        BothFail(RoundTrip(s3.Url, []));
        BothWork(RoundTrip(s3.Url, new() { ["aws_allow_invalid_certificates"] = "true" }));
    }

    [Fact]
    public void TheProxyCaCertificate_IsTrusted_ButNamesAreStillChecked()
    {
        // With a proxy configured (bypassed for this host), its CA certificate is trusted for every connection.
        using var s3 = S3(_certificates.Server());
        var trusted = new Dictionary<string, string>
        {
            ["proxy_url"] = "http://127.0.0.1:1", ["proxy_excludes"] = "127.0.0.1", ["proxy_ca_certificate"] = _certificates.AuthorityPem,
        };
        BothWork(RoundTrip(s3.Url, trusted));
        BothFail(RoundTrip(s3.Url, new() { ["proxy_url"] = "http://127.0.0.1:1", ["proxy_excludes"] = "127.0.0.1" }));

        using var other = S3(_certificates.Server("other.invalid"));
        BothFail(RoundTrip(other.Url, trusted));
    }

    [Fact]
    public void AProxy_CarriesTheRequests_UnlessTheHostIsExcluded()
    {
        // The store's host does not resolve: only the proxy can reach it.
        using var proxy = new Seseragi.Lakehouse.Tests.FakeHttpServer(AnyS3) { ResponseHeaders = S3Headers };
        BothWork(RoundTrip("http://s3.invalid:9000", new() { ["aws_proxy_url"] = proxy.Url }));
        Assert.Contains(proxy.Requests, r => r is { Method: "PUT", PathAndQuery: "http://s3.invalid:9000/bucket/t/a.json" });

        int seen = proxy.Requests.Count;
        BothFail(RoundTrip("http://s3.invalid:9000", new() { ["aws_proxy_url"] = proxy.Url, ["aws_proxy_excludes"] = "example.org, .invalid" }));
        Assert.Equal(seen, proxy.Requests.Count);
    }

    [Fact]
    public void Https_GoesThroughTheProxysTunnel()
    {
        using var proxy = new Seseragi.Lakehouse.Tests.FakeHttpServer(AnyS3)
        {
            ResponseHeaders = S3Headers,
            TunnelCertificate = _certificates.Server("s3.invalid"),
        };
        BothWork(RoundTrip("https://s3.invalid:9443", new() { ["proxy_url"] = proxy.Url, ["proxy_ca_certificate"] = _certificates.AuthorityPem }));
        Assert.Contains(proxy.Requests, r => r is { Method: "CONNECT", PathAndQuery: "s3.invalid:9443" });
        Assert.Contains(proxy.Requests, r => r is { Method: "PUT", PathAndQuery: "/bucket/t/a.json" });
    }
}

/// <summary>A file's end and size take one request on stores with suffix ranges (S3, GCS), two on Azure.</summary>
[Trait("FileSystemContract", "Cloud")]
public sealed class ManagedTailRequestTests(MinioFixture minio, AzuriteFixture azurite, FakeGcsFixture gcs)
    : IClassFixture<MinioFixture>, IClassFixture<AzuriteFixture>, IClassFixture<FakeGcsFixture>
{
    private static int Requests(CloudFileSystem fs)
    {
        fs.Overwrite("t.bin", new byte[1000]);
        int count = Kaminari.Seseragi.Lakehouse.Storage.Http.HttpTransport.CountRequests(() =>
            Assert.Equal(1000, fs.ReadTail("t.bin", 100)!.Value.Size));
        return count;
    }

    [Fact]
    public void S3_AndGcs_AnswerInOneRequest_AzureInTwo()
    {
        Assert.Equal(1, Requests(MinioKeys.Open($"s3://{minio.BucketName}/tail_{Guid.NewGuid():N}", MinioKeys.Of(minio))));
        Assert.Equal(1, Requests(GcsKeys.Open($"gs://{FakeGcsFixture.BucketName}/tail_{Guid.NewGuid():N}", GcsKeys.Of(gcs))));
        Assert.Equal(2, Requests(AzuriteKeys.Open($"az://{azurite.ContainerName}/tail_{Guid.NewGuid():N}", AzuriteKeys.Of(azurite))));
    }
}
