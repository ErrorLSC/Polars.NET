using System.Reflection;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.CSharp;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Wrappers;
using Pl = Polars.CSharp.Polars;

namespace Polars.Integration.Tests;

/// <summary>
/// A real S3 bucket for smoke tests: SESERAGI_AWS_BUCKET, SESERAGI_AWS_ACCESS_KEY_ID, SESERAGI_AWS_SECRET_ACCESS_KEY
/// (and optionally SESERAGI_AWS_REGION), else the three lines of <c>.secrets/AWS</c> at the repository root (bucket,
/// key id, secret). Never the AWS_* variables or ~/.aws: the credential chain tests need the machine clean. The
/// values are never written to any output.
/// </summary>
public static class RealAws
{
    private static readonly Lazy<(string Bucket, string KeyId, string Secret, string? Region, string? RoleArn)?> s_settings = new(Load);

    /// <summary>A role the key may assume (SESERAGI_AWS_ROLE_ARN, or the fifth line of .secrets/AWS), for temporary credentials.</summary>
    public static string? RoleArn => s_settings.Value?.RoleArn;

    /// <summary>The bucket's region, when given.</summary>
    public static string? Region => s_settings.Value?.Region;

    public static bool Available => s_settings.Value is not null;

    public static string Bucket => s_settings.Value!.Value.Bucket;

    /// <summary>The prefix of this test run; everything below it is deleted when the run ends.</summary>
    public static string RunPrefix { get; } = $"seseragi-tests/{DateTime.UtcNow:yyyyMMdd-HHmmss}-{Guid.NewGuid().ToString("N")[..8]}";

    /// <summary>A fresh URL below the run prefix.</summary>
    public static string NewUrl(string name) => $"s3://{Bucket}/{RunPrefix}/{name}_{Guid.NewGuid():N}";

    /// <summary>Explicit keys; the region only when given (else the stores find the bucket's region themselves).</summary>
    public static Dictionary<string, string> Keys(bool withRegion = true)
    {
        var (_, keyId, secret, region, _) = s_settings.Value!.Value;
        var keys = new Dictionary<string, string> { ["aws_access_key_id"] = keyId, ["aws_secret_access_key"] = secret };
        if (withRegion && region is not null) keys["aws_region"] = region;
        return keys;
    }

    public static CloudArgs Args(bool withRegion = true)
    {
        var keys = Keys(withRegion);
        return CloudArgs.ForLocation("s3://" + Bucket, [.. keys.Keys], [.. keys.Values]);
    }

    private static (string, string, string, string?, string?)? Load()
    {
        string? role = Environment.GetEnvironmentVariable("SESERAGI_AWS_ROLE_ARN");
        string? bucket = Environment.GetEnvironmentVariable("SESERAGI_AWS_BUCKET");
        string? keyId = Environment.GetEnvironmentVariable("SESERAGI_AWS_ACCESS_KEY_ID");
        string? secret = Environment.GetEnvironmentVariable("SESERAGI_AWS_SECRET_ACCESS_KEY");
        string? region = Environment.GetEnvironmentVariable("SESERAGI_AWS_REGION");
        if (bucket is null || keyId is null || secret is null)
        {
            string? file = SecretsFile("AWS");
            if (file is null) return null;
            string[] lines = [.. File.ReadAllLines(file).Select(l => l.Trim()).Where(l => l.Length > 0)];
            if (lines.Length < 3) return null;
            (bucket, keyId, secret) = (lines[0], lines[1], lines[2]);
            if (lines.Length > 3) region = lines[3];
            if (lines.Length > 4) role ??= lines[4];
        }
        return (bucket, keyId, secret, string.IsNullOrWhiteSpace(region) ? null : region, string.IsNullOrWhiteSpace(role) ? null : role);
    }

    /// <summary>The file <c>.secrets/name</c> of the repository this test runs from, if there is one.</summary>
    internal static string? SecretsFile(string name)
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            string candidate = Path.Combine(dir.FullName, ".secrets", name);
            if (File.Exists(candidate)) return candidate;
        }
        return null;
    }
}

/// <summary>A fact that runs only where a real S3 bucket is configured (see <see cref="RealAws"/>).</summary>
public sealed class AwsFactAttribute : FactAttribute
{
    public AwsFactAttribute()
    {
        if (!RealAws.Available) Skip = "No real S3 bucket: set SESERAGI_AWS_BUCKET / _ACCESS_KEY_ID / _SECRET_ACCESS_KEY or write .secrets/AWS.";
    }
}

/// <summary>A fact that runs only where a real S3 bucket and a role to assume are configured.</summary>
public sealed class AwsRoleFactAttribute : FactAttribute
{
    public AwsRoleFactAttribute()
    {
        if (!RealAws.Available || RealAws.RoleArn is null)
            Skip = "No role to assume: set SESERAGI_AWS_ROLE_ARN or the fifth line of .secrets/AWS.";
    }
}

/// <summary>Temporary credentials from STS AssumeRole, signed with the test key (the managed SigV4 signer).</summary>
public static class RealAwsSts
{
    public static (string KeyId, string Secret, string Token) AssumeRole(TimeSpan duration)
    {
        string region = RealAws.Region ?? "us-east-1";
        var keys = RealAws.Keys();
        var credential = new Kaminari.Seseragi.Lakehouse.Storage.S3.AwsCredential(keys["aws_access_key_id"], keys["aws_secret_access_key"], null);
        string host = $"sts.{region}.amazonaws.com";
        string body = string.Join('&', new Dictionary<string, string>
        {
            ["Action"] = "AssumeRole",
            ["DurationSeconds"] = ((int)duration.TotalSeconds).ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["RoleArn"] = RealAws.RoleArn!,
            ["RoleSessionName"] = "seseragi-test",
            ["Version"] = "2011-06-15",
        }.Select(p => $"{Uri.EscapeDataString(p.Key)}={Uri.EscapeDataString(p.Value)}"));
        byte[] payload = System.Text.Encoding.UTF8.GetBytes(body);
        DateTimeOffset now = DateTimeOffset.UtcNow;
        var signed = new Dictionary<string, string>
        {
            ["content-type"] = "application/x-www-form-urlencoded; charset=utf-8",
            ["host"] = host,
            ["x-amz-date"] = Kaminari.Seseragi.Lakehouse.Storage.S3.SigV4.AmzDate(now),
        };
        string authorization = Kaminari.Seseragi.Lakehouse.Storage.S3.SigV4.Authorization(
            "POST", "/", "", signed, Kaminari.Seseragi.Lakehouse.Storage.S3.SigV4.Hash(payload), credential, region, "sts", now);

        using var http = new HttpClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, $"https://{host}/") { Content = new ByteArrayContent(payload) };
        request.Content.Headers.TryAddWithoutValidation("Content-Type", signed["content-type"]);
        request.Headers.TryAddWithoutValidation("x-amz-date", signed["x-amz-date"]);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);
        using HttpResponseMessage response = http.Send(request);
        byte[] answer = response.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult();
        Assert.True(response.IsSuccessStatusCode, $"AssumeRole: HTTP {(int)response.StatusCode}");
        var temporary = Kaminari.Seseragi.Lakehouse.Storage.S3.AwsCredentialSources.FromStsXml(answer);
        Assert.NotNull(temporary.Value.Token);
        return (temporary.Value.KeyId, temporary.Value.Secret, temporary.Value.Token!);
    }
}

/// <summary>Deletes everything the run wrote below its prefix.</summary>
public sealed class RealAwsCleanup : IDisposable
{
    public void Dispose()
    {
        if (!RealAws.Available) return;
        using var fs = new ObjectStoreFileSystem($"s3://{RealAws.Bucket}/{RealAws.RunPrefix}", RealAws.Args(), ObjectStoreBackend.Managed);
        foreach (FileEntry file in fs.ListAll("")) fs.Delete(file.Path);
    }
}

/// <summary>
/// Smoke tests on a real S3 bucket, run once with the default store (managed) and once with
/// <c>POLARS_NET_OBJECT_STORE=native</c>: the file system contract, put-if-absent races, region discovery, and the
/// Delta kernel end to end (streamed writes, DML, optimize, vacuum, identity columns, row tracking).
/// </summary>
[Trait("RealCloud", "Aws")]
public sealed class RealCloudAwsTests : IClassFixture<RealAwsCleanup>
{
    private static string Backend => ObjectStoreFileSystem.ConfiguredBackend == ObjectStoreBackend.Native ? "native" : "managed";

    /// <summary>The file system contract, each of its checks on its own root.</summary>
    private sealed class Contract(bool withRegion) : FileSystemContract
    {
        protected override string NewRoot() => RealAws.NewUrl("contract");

        protected override IWritableFileSystem Open(string root)
        {
            var fs = new ObjectStoreFileSystem(root, RealAws.Args(withRegion));
            Assert.Equal(Backend, fs.Implementation);
            return fs;
        }
    }

    private static void RunContract(bool withRegion)
    {
        var contract = new Contract(withRegion);
        var checks = typeof(FileSystemContract).GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Where(m => m.GetCustomAttribute<FactAttribute>() is not null).ToList();
        Assert.True(checks.Count >= 10);
        var failures = new List<string>();
        foreach (MethodInfo check in checks)
        {
            try
            {
                check.Invoke(contract, null);
            }
            catch (TargetInvocationException e)
            {
                failures.Add($"{check.Name}: {e.InnerException?.GetType().Name}: {e.InnerException?.Message}");
            }
        }
        Assert.True(failures.Count == 0, $"{Backend}: " + string.Join("\n", failures));
    }

    [AwsFact]
    public void TheFileSystemContract_Holds() => RunContract(withRegion: true);

    [AwsFact]
    public void WithoutARegion_TheBucketsRegionIsFound() => RunContract(withRegion: false);

    [AwsFact]
    public void PutIfAbsent_HasOneWinner_AndCommitsConflict() =>
        KernelObjectStoreCheck.PutIfAbsentHolds(RealAws.NewUrl("race"), RealAws.Args());

    [AwsFact]
    public void StreamedWrites_SplitFiles_AndReadFootersFromTheStore() =>
        KernelObjectStoreCheck.StreamedWritesWork(RealAws.NewUrl("streamed"), RealAws.Args());

    [AwsFact]
    public void Vacuum_DeletesExactlyWhatTheTableNoLongerNeeds() =>
        KernelObjectStoreCheck.VacuumWorks(RealAws.NewUrl("vacuum"), RealAws.Args());

    [AwsRoleFact]
    public void TemporaryCredentials_FromAssumeRole_WriteAndReadATable()
    {
        var (keyId, secret, token) = RealAwsSts.AssumeRole(TimeSpan.FromMinutes(15));
        var keys = new Dictionary<string, string>
        {
            ["aws_access_key_id"] = keyId,
            ["aws_secret_access_key"] = secret,
            ["aws_session_token"] = token,
            ["aws_region"] = RealAws.Region ?? "us-east-1",
        };
        string url = RealAws.NewUrl("session");
        CloudArgs args = CloudArgs.ForLocation(url, [.. keys.Keys], [.. keys.Values]);

        // The kernel's own access (log, footers) and the engine's (data files) both sign with the session token.
        using (var fs = new ObjectStoreFileSystem(url, args))
        {
            Assert.Equal(Backend, fs.Implementation);
            Assert.True(fs.TryCreate("probe.json", "{}"u8.ToArray()));
            Assert.Single(fs.List(""));
            Assert.True(fs.Delete("probe.json"));
        }
        using (var rows = DataFrame.FromColumns(new { Id = new[] { 1, 2, 3 } }))
            DeltaKernelWrite.Write(url, PolarsWrapper.LazyClone(rows.Lazy().Handle), KernelWriteMode.ErrorIfExists, null, args);
        DeltaKernelDelete.Delete(url, PolarsWrapper.Eq(PolarsWrapper.Col("Id"), PolarsWrapper.Lit(2)), cloud: args);
        using (var latest = new LazyFrame(DeltaKernelScan.Scan(url, cloud: args)).Collect())
            Assert.Equal([1, 3], latest.Sort(["Id"])["Id"].ToArray<int>());

        // Without its session token the temporary key is refused.
        keys.Remove("aws_session_token");
        using var withoutToken = new ObjectStoreFileSystem(url, CloudArgs.ForLocation(url, [.. keys.Keys], [.. keys.Values]));
        Assert.ThrowsAny<Exception>(() => withoutToken.List("_delta_log"));
    }

    [AwsFact]
    public void ATable_WithIdentityAndRowTracking_LivesThroughEveryOperation()
    {
        string url = RealAws.NewUrl("lifecycle");
        CloudArgs args = RealAws.Args();
        var keys = RealAws.Keys();
        var options = new CloudOptions { Credentials = keys };

        Delta.CreateTable(url,
            new PolarsSchema().Add("Id", DataType.Int64).Add("Region", DataType.String).Add("Price", DataType.Float64),
            partitionBy: ["Region"],
            properties: new() { [DeltaTableProperties.EnableRowTracking] = "true", [DeltaTableProperties.EnableChangeDataFeed] = "true" },
            identityColumns: [new DeltaIdentityColumn("Id")],
            cloudOptions: options);
        using (var rows = DataFrame.FromColumns(new { Region = new[] { "east", "west", "east", "west" }, Price = new[] { 1.0, 2.0, 3.0, 4.0 } }))
            rows.WriteDelta(url, mode: DeltaSaveMode.Append, cloudOptions: options);
        Dictionary<long, long> RowIds()
        {
            using var df = DataFrame.ReadDelta(url, cloudOptions: options, rowIdColumn: "row_id");
            return df["Id"].ToArray<long>().Zip(df["row_id"].ToArray<long>()).ToDictionary(p => p.First, p => p.Second);
        }
        var written = RowIds();
        Delta.Delete(url, Pl.Col("Id") == 2L, cloudOptions: options);
        DeltaKernelUpdate.Update(url, PolarsWrapper.Eq(PolarsWrapper.Col("Id"), PolarsWrapper.Lit(3L)), [("Price", PolarsWrapper.Lit(30.0))], cloud: args);
        using (var source = DataFrame.FromColumns(new { Region = new[] { "north" }, Price = new[] { 5.0 } }))
        {
            DeltaKernelMerge.Merge(url, PolarsWrapper.LazyClone(source.Lazy().Handle), ["Region"],
                [new(MergeClauseKind.NotMatchedInsert, PolarsWrapper.Lit(true))], cloud: args);
        }
        DeltaKernelOptimize.Optimize(url, cloud: args);

        using var latest = DataFrame.ReadDelta(url, cloudOptions: options, rowIdColumn: "row_id", rowCommitVersionColumn: "row_version").Sort(["Id"]);
        Assert.Equal([1L, 3L, 4L, 5L], latest["Id"].ToArray<long>());
        Assert.Equal([1.0, 30.0, 4.0, 5.0], latest["Price"].ToArray<double>());
        // Rows that lived through the delete, update, merge and optimize kept their ids; the inserted one has a new id.
        var now = RowIds();
        Assert.All(new[] { 1L, 3L, 4L }, id => Assert.Equal(written[id], now[id]));
        Assert.DoesNotContain(now[5], written.Values);
        Assert.Equal(4, now.Values.Distinct().Count());

        // Time travel, history and the change feed read from the store.
        using (var v1 = DataFrame.ReadDelta(url, version: 1, cloudOptions: options))
            Assert.Equal(4, v1.Height);
        using (var history = Delta.History(url, cloudOptions: options))
            // Optimize had nothing to compact (one file per partition), so it committed nothing.
            Assert.Equal(["CREATE TABLE", "WRITE", "DELETE", "UPDATE", "MERGE"], history["operation"].ToArray<string>().Reverse());
        using (var changes = Delta.ReadChangeDataFeed(url, startVersion: 1, endVersion: 4, cloudOptions: options).Collect())
            Assert.Contains("delete", changes["_change_type"].ToArray<string>());
    }
}
