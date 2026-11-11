using System.Text;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Storage;
using Kaminari.Seseragi.Lakehouse.Storage.S3;
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
        foreach (string name in new[] { "b.json", "a.json", "B.json", "a.json.tmp", "_sub/x.json", "dir/inner.json" })
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
        foreach (string name in new[] { "00.json", "01.json", "02.json", "02.checkpoint.parquet", "03.json" })
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
        keys["aws_request_payer"] = "true"; // MinIO ignores it; the managed client does not implement it
        keys["aws_s3_force_path_style"] = "true";
        CloudArgs args = CloudArgs.ForLocation(root, [.. keys.Keys], [.. keys.Values]);
        using (var fs = new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Auto))
        {
            Assert.Equal("native", fs.Implementation);
            Assert.Equal("a", Encoding.UTF8.GetString(fs.ReadAll("a.json")!));
        }
        var refused = Assert.Throws<NotSupportedException>(() => new ObjectStoreFileSystem(root, args, ObjectStoreBackend.Managed));
        Assert.Contains("request_payer", refused.Message);
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
