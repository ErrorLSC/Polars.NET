using System.Reflection;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.CSharp;
using Polars.NET.Core.Delta;
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

/// <summary>AWS query APIs (STS, EC2) and presigned S3 URLs, signed with the test key by the managed SigV4 signer.</summary>
public static class RealAwsApi
{
    private static Kaminari.Seseragi.Lakehouse.Storage.S3.AwsCredential Credential()
    {
        var keys = RealAws.Keys();
        return new(keys["aws_access_key_id"], keys["aws_secret_access_key"], null);
    }

    public static string Region => RealAws.Region ?? "us-east-1";

    /// <summary>A query API call (form POST); the answer's XML. Fails with the service's error text.</summary>
    public static System.Xml.Linq.XDocument Query(string service, string version, string action, IEnumerable<KeyValuePair<string, string>> parameters)
    {
        string host = $"{service}.{Region}.amazonaws.com";
        var form = new List<KeyValuePair<string, string>> { new("Action", action), new("Version", version) };
        form.AddRange(parameters);
        byte[] payload = System.Text.Encoding.UTF8.GetBytes(string.Join('&', form.Select(p => $"{Uri.EscapeDataString(p.Key)}={Uri.EscapeDataString(p.Value)}")));
        DateTimeOffset now = DateTimeOffset.UtcNow;
        var signed = new Dictionary<string, string>
        {
            ["content-type"] = "application/x-www-form-urlencoded; charset=utf-8",
            ["host"] = host,
            ["x-amz-date"] = Kaminari.Seseragi.Lakehouse.Storage.S3.SigV4.AmzDate(now),
        };
        string authorization = Kaminari.Seseragi.Lakehouse.Storage.S3.SigV4.Authorization(
            "POST", "/", "", signed, Kaminari.Seseragi.Lakehouse.Storage.S3.SigV4.Hash(payload), Credential(), Region, service, now);

        using var http = new HttpClient();
        using var request = new HttpRequestMessage(HttpMethod.Post, $"https://{host}/") { Content = new ByteArrayContent(payload) };
        request.Content.Headers.TryAddWithoutValidation("Content-Type", signed["content-type"]);
        request.Headers.TryAddWithoutValidation("x-amz-date", signed["x-amz-date"]);
        request.Headers.TryAddWithoutValidation("Authorization", authorization);
        using HttpResponseMessage response = http.Send(request);
        string answer = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        if (!response.IsSuccessStatusCode) throw new InvalidOperationException($"{service} {action}: HTTP {(int)response.StatusCode}: {answer}");
        return System.Xml.Linq.XDocument.Parse(answer);
    }

    /// <summary>Temporary credentials of the configured role (STS AssumeRole).</summary>
    public static (string KeyId, string Secret, string Token) AssumeRole(TimeSpan duration)
    {
        System.Xml.Linq.XDocument answer = Query("sts", "2011-06-15", "AssumeRole",
        [
            new("DurationSeconds", ((int)duration.TotalSeconds).ToString(System.Globalization.CultureInfo.InvariantCulture)),
            new("RoleArn", RealAws.RoleArn!),
            new("RoleSessionName", "seseragi-test"),
        ]);
        var temporary = Kaminari.Seseragi.Lakehouse.Storage.S3.AwsCredentialSources.FromStsXml(
            System.Text.Encoding.UTF8.GetBytes(answer.ToString()));
        Assert.NotNull(temporary.Value.Token);
        return (temporary.Value.KeyId, temporary.Value.Secret, temporary.Value.Token!);
    }

    /// <summary>The values of the elements of this local name, anywhere in the answer.</summary>
    public static IEnumerable<System.Xml.Linq.XElement> All(System.Xml.Linq.XDocument doc, string name) =>
        doc.Descendants().Where(e => e.Name.LocalName == name);

    public static string? Value(System.Xml.Linq.XElement e, string name) =>
        e.Elements().FirstOrDefault(c => c.Name.LocalName == name)?.Value;

    /// <summary>A presigned (query-signed, unsigned payload) URL of an object of the test bucket.</summary>
    public static string Presign(string method, string key, TimeSpan expires)
    {
        var credential = Credential();
        string host = $"{RealAws.Bucket}.s3.{Region}.amazonaws.com";
        string path = "/" + Kaminari.Seseragi.Lakehouse.Storage.S3.SigV4.Encode(key, encodeSlash: false);
        DateTimeOffset now = DateTimeOffset.UtcNow;
        string amzDate = Kaminari.Seseragi.Lakehouse.Storage.S3.SigV4.AmzDate(now);
        string scope = $"{amzDate[..8]}/{Region}/s3/aws4_request";
        string query = Kaminari.Seseragi.Lakehouse.Storage.S3.SigV4.CanonicalQuery(new Dictionary<string, string>
        {
            ["X-Amz-Algorithm"] = "AWS4-HMAC-SHA256",
            ["X-Amz-Credential"] = $"{credential.KeyId}/{scope}",
            ["X-Amz-Date"] = amzDate,
            ["X-Amz-Expires"] = ((int)expires.TotalSeconds).ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["X-Amz-SignedHeaders"] = "host",
        });
        string canonical = $"{method}\n{path}\n{query}\nhost:{host}\n\nhost\nUNSIGNED-PAYLOAD";
        string toSign = $"AWS4-HMAC-SHA256\n{amzDate}\n{scope}\n{Kaminari.Seseragi.Lakehouse.Storage.S3.SigV4.Hash(System.Text.Encoding.UTF8.GetBytes(canonical))}";
        static byte[] Hmac(byte[] key, string data) => System.Security.Cryptography.HMACSHA256.HashData(key, System.Text.Encoding.UTF8.GetBytes(data));
        byte[] signingKey = Hmac(Hmac(Hmac(Hmac(System.Text.Encoding.UTF8.GetBytes("AWS4" + credential.Secret), amzDate[..8]), Region), "s3"), "aws4_request");
        string signature = Convert.ToHexString(Hmac(signingKey, toSign)).ToLowerInvariant();
        return $"https://{host}{path}?{query}&X-Amz-Signature={signature}";
    }
}

/// <summary>
/// A fact that runs only on request (SESERAGI_AWS_EC2=1): it rents a small EC2 machine for a few minutes.
/// </summary>
public sealed class AwsEc2FactAttribute : FactAttribute
{
    public AwsEc2FactAttribute()
    {
        if (!RealAws.Available) Skip = "No real S3 bucket configured.";
        else if (Environment.GetEnvironmentVariable("SESERAGI_AWS_EC2") != "1") Skip = "Rents an EC2 machine: set SESERAGI_AWS_EC2=1 to run it.";
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
        var (keyId, secret, token) = RealAwsApi.AssumeRole(TimeSpan.FromMinutes(15));
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
        var options = new CloudOptions { Credentials = keys };
        using (var rows = DataFrame.FromColumns(new { Id = new[] { 1, 2, 3 } }))
            rows.WriteDelta(url, mode: DeltaSaveMode.ErrorIfExists, cloudOptions: options);
        Delta.Delete(url, Pl.Col("Id") == 2, options);
        using (var latest = DataFrame.ReadDelta(url, cloudOptions: options))
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
        Delta.Update(url, Pl.Col("Id") == 3L, new Dictionary<string, Expr> { ["Price"] = Pl.Lit(30.0) }, options);
        using (var source = DataFrame.FromColumns(new { Region = new[] { "north" }, Price = new[] { 5.0 } }))
            source.MergeDeltaOrdered(url, ["Region"], cloudOptions: options).WhenNotMatchedInsert().Execute();
        Delta.Optimize(url, cloudOptions: options);

        using var latest = DataFrame.ReadDelta(url, cloudOptions: options, rowIdColumn: "row_id", rowCommitVersionColumn: "row_version").Sort(["Id"]);
        Assert.Equal([1L, 3L, 4L, 5L], latest["Id"].ToArray<long>());
        Assert.Equal([1.0, 30.0, 4.0, 5.0], latest["Price"].ToArray<double>());
        // Rows that lived through the delete, update, merge and optimize kept their ids; the inserted one has a new id.
        var now = RowIds();
        Assert.All([1L, 3L, 4L], id => Assert.Equal(written[id], now[id]));
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

/// <summary>
/// The credentials of a machine (instance metadata, IMDSv2), on a real EC2 machine rented for the test and destroyed
/// afterwards: the probe (tests/RealCloudProbe, published self-contained) runs there with no keys at all, once on the
/// managed store and once on the native one, and reports back through a presigned URL.
/// </summary>
/// <remarks>
/// The machine cannot outlive the test: it is terminated in <c>finally</c>; it powers itself off when done, and
/// powering off terminates it; its first act is a 20 minute shutdown timer; and every run first terminates test
/// machines older than 30 minutes. Fedora 44, since the native library needs glibc 2.43.
/// </remarks>
[Trait("RealCloud", "AwsEc2")]
public sealed class RealCloudAwsMachineTests : IClassFixture<RealAwsCleanup>
{
    private const string Tag = "seseragi-test";
    private const string InstanceProfile = "seseragi-test-instance";
    private const string InstanceType = "t3.small";
    private const string FedoraOwner = "125523088429";

    [AwsEc2Fact]
    public void InstanceCredentials_ServeBothStores_OnARealMachine()
    {
        SweepOldMachines();

        // The probe, published for linux-x64 with the release native library, goes to the bucket.
        string package = PublishProbe();
        string prefix = $"{RealAws.RunPrefix}/ec2";
        using (var fs = new ObjectStoreFileSystem($"s3://{RealAws.Bucket}", RealAws.Args(), ObjectStoreBackend.Managed))
            fs.Overwrite($"{prefix}/probe.tgz", File.ReadAllBytes(package));

        string get = RealAwsApi.Presign("GET", $"{prefix}/probe.tgz", TimeSpan.FromMinutes(30));
        string put = RealAwsApi.Presign("PUT", $"{prefix}/result.txt", TimeSpan.FromMinutes(30));
        string tables = $"s3://{RealAws.Bucket}/{prefix}/table"; // one table per store: table_managed, table_native
        string script = $$"""
            #!/bin/bash
            shutdown -h +20
            export HOME=/root
            cd /root
            {
              if curl -sSf -o probe.tgz '{{get}}' && tar xzf probe.tgz; then
                for b in managed native; do
                  POLARS_NET_OBJECT_STORE=$b ./probe-publish/RealCloudProbe "{{tables}}_$b" '{{RealAwsApi.Region}}' 2>&1 | tail -n 1
                done
              else
                echo '{"ok":false,"step":"download the probe"}'
              fi
            } > result.txt 2>&1
            curl -sSf -X PUT --upload-file result.txt '{{put}}'
            shutdown -h now
            """;

        string? instance = null;
        try
        {
            instance = Launch(LatestFedora(), Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(script.Replace("\r", ""))));
            string result = AwaitResult($"{prefix}/result.txt", instance, TimeSpan.FromMinutes(15));

            string[] lines = [.. result.Split('\n', StringSplitOptions.RemoveEmptyEntries)];
            Assert.True(lines.Length == 2, result);
            foreach (var (line, backend) in lines.Zip(["managed", "native"]))
            {
                using var doc = System.Text.Json.JsonDocument.Parse(line);
                var root = doc.RootElement;
                Assert.True(root.GetProperty("ok").GetBoolean(), line);
                Assert.Equal(backend, root.GetProperty("implementation").GetString());
                Assert.Equal([1L, 3L], root.GetProperty("ids").EnumerateArray().Select(e => e.GetInt64()));
            }
        }
        finally
        {
            if (instance is not null) Terminate(instance);
        }
    }

    /// <summary>Publishes the probe self-contained and packs it; returns the package path.</summary>
    internal static string PublishProbe()
    {
        string repo = new DirectoryInfo(AppContext.BaseDirectory).EnumerateAncestors()
            .First(d => File.Exists(Path.Combine(d.FullName, "Polars.NET.slnx"))).FullName;
        string work = Path.Combine(Path.GetTempPath(), $"seseragi_probe_{Guid.NewGuid():N}");
        string output = Path.Combine(work, "probe-publish");
        // No reused build nodes: they would outlive the publish and keep its output pipes open.
        Run("dotnet", $"publish \"{Path.Combine(repo, "tests", "RealCloudProbe")}\" -c Release -r linux-x64 --self-contained -p:SkipRust=true --disable-build-servers -nodeReuse:false -o \"{output}\"", repo);
        Assert.True(File.Exists(Path.Combine(output, "libnative_shim.so")), "The release native library is not built (cargo build --release).");
        string package = Path.Combine(work, "probe.tgz");
        Run("tar", $"czf \"{package}\" -C \"{work}\" probe-publish", work);
        return package;
    }

    internal static void Run(string file, string arguments, string directory)
    {
        var start = new System.Diagnostics.ProcessStartInfo(file, arguments)
        {
            WorkingDirectory = directory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        start.Environment["MSBUILDDISABLENODEREUSE"] = "1";
        start.Environment["DOTNET_CLI_USE_MSBUILD_SERVER"] = "0";
        using var process = new System.Diagnostics.Process { StartInfo = start };
        var output = new System.Text.StringBuilder();
        process.OutputDataReceived += (_, e) => { lock (output) output.AppendLine(e.Data); };
        process.ErrorDataReceived += (_, e) => { lock (output) output.AppendLine(e.Data); };
        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
        // Waits for the process itself, not for its output pipes (a child holding them must not hang the test).
        if (!process.WaitForExit(TimeSpan.FromMinutes(10)))
        {
            process.Kill(entireProcessTree: true);
            throw new TimeoutException($"{file} {arguments} did not finish within 10 minutes.");
        }
        Assert.True(process.ExitCode == 0, $"{file} {arguments}\n{output}");
    }

    /// <summary>The newest Fedora 44 Cloud Base image for x86_64 in the region.</summary>
    private static string LatestFedora()
    {
        var doc = RealAwsApi.Query("ec2", "2016-11-15", "DescribeImages",
        [
            new("Owner.1", FedoraOwner),
            new("Filter.1.Name", "name"), new("Filter.1.Value.1", "Fedora-Cloud-Base-AmazonEC2.x86_64-44*"),
            new("Filter.2.Name", "architecture"), new("Filter.2.Value.1", "x86_64"),
            new("Filter.3.Name", "state"), new("Filter.3.Value.1", "available"),
        ]);
        var images = RealAwsApi.All(doc, "item")
            .Where(i => RealAwsApi.Value(i, "imageId") is not null && RealAwsApi.Value(i, "creationDate") is not null)
            .OrderByDescending(i => RealAwsApi.Value(i, "creationDate"))
            .ToList();
        Assert.True(images.Count > 0, "No Fedora 44 Cloud Base image for x86_64 in the region.");
        return RealAwsApi.Value(images[0], "imageId")!;
    }

    private static string Launch(string image, string userData)
    {
        KeyValuePair<string, string>[] parameters =
        [
            new("ImageId", image), new("InstanceType", InstanceType), new("MinCount", "1"), new("MaxCount", "1"),
            new("IamInstanceProfile.Name", InstanceProfile),
            new("InstanceInitiatedShutdownBehavior", "terminate"),
            new("MetadataOptions.HttpTokens", "required"), // IMDSv2 only
            new("UserData", userData),
            new("TagSpecification.1.ResourceType", "instance"),
            new("TagSpecification.1.Tag.1.Key", Tag), new("TagSpecification.1.Tag.1.Value", "true"),
            new("TagSpecification.1.Tag.2.Key", "Name"), new("TagSpecification.1.Tag.2.Value", $"seseragi-test {RealAws.RunPrefix}"),
        ];
        // A role just created takes a few seconds to reach EC2.
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                var doc = RealAwsApi.Query("ec2", "2016-11-15", "RunInstances", parameters);
                return RealAwsApi.All(doc, "instanceId").First().Value;
            }
            catch (InvalidOperationException e) when (attempt < 6 && e.Message.Contains("Invalid IAM Instance Profile", StringComparison.Ordinal))
            {
                Thread.Sleep(TimeSpan.FromSeconds(10));
            }
        }
    }

    /// <summary>Waits for the probe's report; fails early when the machine is gone without one.</summary>
    private static string AwaitResult(string key, string instance, TimeSpan timeout)
    {
        using var fs = new ObjectStoreFileSystem($"s3://{RealAws.Bucket}", RealAws.Args(), ObjectStoreBackend.Managed);
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (clock.Elapsed < timeout)
        {
            if (fs.ReadAll(key) is byte[] bytes) return System.Text.Encoding.UTF8.GetString(bytes);
            string state = State(instance);
            Assert.True(state is "pending" or "running" or "stopping" or "shutting-down",
                $"The machine is {state} after {clock.Elapsed:mm\\:ss} and reported nothing.");
            Thread.Sleep(TimeSpan.FromSeconds(15));
        }
        throw new TimeoutException($"No report from the machine within {timeout}.");
    }

    private static string State(string instance)
    {
        var doc = RealAwsApi.Query("ec2", "2016-11-15", "DescribeInstances", [new("InstanceId.1", instance)]);
        return RealAwsApi.All(doc, "instanceState").Select(s => RealAwsApi.Value(s, "name")).FirstOrDefault() ?? "unknown";
    }

    private static void Terminate(string instance)
    {
        try
        {
            RealAwsApi.Query("ec2", "2016-11-15", "TerminateInstances", [new("InstanceId.1", instance)]);
        }
        catch (InvalidOperationException e)
        {
            Console.Error.WriteLine($"Terminating {instance} failed: {e.Message}");
        }
    }

    /// <summary>Terminates test machines still alive more than 30 minutes after launch (from runs that crashed).</summary>
    private static void SweepOldMachines()
    {
        var doc = RealAwsApi.Query("ec2", "2016-11-15", "DescribeInstances",
        [
            new("Filter.1.Name", $"tag:{Tag}"), new("Filter.1.Value.1", "true"),
            new("Filter.2.Name", "instance-state-name"),
            new("Filter.2.Value.1", "pending"), new("Filter.2.Value.2", "running"), new("Filter.2.Value.3", "stopped"),
        ]);
        foreach (var item in RealAwsApi.All(doc, "instancesSet").SelectMany(s => s.Elements()))
        {
            string? id = RealAwsApi.Value(item, "instanceId");
            if (id is not null && DateTimeOffset.TryParse(RealAwsApi.Value(item, "launchTime"), out var launched)
                && DateTimeOffset.UtcNow - launched > TimeSpan.FromMinutes(30))
                Terminate(id);
        }
    }
}

internal static class DirectoryInfoExtensions
{
    public static IEnumerable<DirectoryInfo> EnumerateAncestors(this DirectoryInfo directory)
    {
        for (DirectoryInfo? d = directory; d is not null; d = d.Parent) yield return d;
    }
}
