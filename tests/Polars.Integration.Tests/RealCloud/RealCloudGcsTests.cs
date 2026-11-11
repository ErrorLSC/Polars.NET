using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.CSharp;
using Polars.NET.Core.Delta;
using Pl = Polars.CSharp.Polars;

namespace Polars.Integration.Tests;

/// <summary>How a test signs in to the real bucket.</summary>
public enum GcsSignIn
{
    /// <summary>The path of the service account's JSON key (<c>google_service_account</c>).</summary>
    KeyFile,
    /// <summary>The JSON key itself (<c>google_service_account_key</c>).</summary>
    KeyJson,
    /// <summary>An access token the test fetched with the key (<c>google_oauth_token</c>), passed as is.</summary>
    OAuthToken,
}

/// <summary>
/// A real Google Cloud Storage bucket for smoke tests: SESERAGI_GCS_BUCKET, SESERAGI_GCS_PROJECT and
/// SESERAGI_GCS_KEY_FILE, else <c>.secrets/GCS</c> (bucket, project id) with the service account key in
/// <c>.secrets/gcs-key.json</c>. Never GOOGLE_APPLICATION_CREDENTIALS or gcloud: the credential chain tests need the
/// machine clean. Secret values are never written to any output.
/// </summary>
public static class RealGcs
{
    private sealed record Settings(string Bucket, string? Project, string KeyFile);

    private static readonly Lazy<Settings?> s_settings = new(Load);

    public static bool Available => s_settings.Value is not null;

    public static string Bucket => s_settings.Value!.Bucket;

    public static string? Project => s_settings.Value!.Project;

    public static string KeyFile => s_settings.Value!.KeyFile;

    /// <summary>The prefix of this test run; everything below it is deleted when the run ends.</summary>
    public static string RunPrefix { get; } = $"seseragi-tests/{DateTime.UtcNow:yyyyMMdd-HHmmss}-{Guid.NewGuid().ToString("N")[..8]}";

    /// <summary>A fresh URL below the run prefix.</summary>
    public static string NewUrl(string name) => $"gs://{Bucket}/{RunPrefix}/{name}_{Guid.NewGuid():N}";

    public static Dictionary<string, string> Keys(GcsSignIn signIn = GcsSignIn.KeyFile) => signIn switch
    {
        GcsSignIn.KeyFile => new() { ["google_service_account"] = KeyFile },
        GcsSignIn.KeyJson => new() { ["google_service_account_key"] = File.ReadAllText(KeyFile) },
        _ => new() { ["google_oauth_token"] = AccessToken() },
    };

    public static CloudArgs Args(GcsSignIn signIn = GcsSignIn.KeyFile)
    {
        var keys = Keys(signIn);
        return CloudArgs.ForLocation($"gs://{Bucket}", [.. keys.Keys], [.. keys.Values]);
    }

    private static string? s_token;

    /// <summary>
    /// An access token of the service account, fetched once per run by the test itself (a self-signed RS256 JWT
    /// exchanged at the key's token URI), independently of the stores under test.
    /// </summary>
    public static string AccessToken() => s_token ??= Token("https://www.googleapis.com/auth/devstorage.read_write");

    /// <summary>The service account's key: its email and RSA private key (the caller disposes the key).</summary>
    public static (string Email, RSA Key) SigningKey()
    {
        using var key = JsonDocument.Parse(File.ReadAllText(KeyFile));
        var rsa = RSA.Create();
        rsa.ImportFromPem(key.RootElement.GetProperty("private_key").GetString());
        return (key.RootElement.GetProperty("client_email").GetString()!, rsa);
    }

    /// <summary>An access token of the service account for a scope (self-signed JWT exchanged at the token URI).</summary>
    public static string Token(string scope)
    {
        using var key = JsonDocument.Parse(File.ReadAllText(KeyFile));
        string email = key.RootElement.GetProperty("client_email").GetString()!;
        string tokenUri = key.RootElement.GetProperty("token_uri").GetString()!;
        using var rsa = RSA.Create();
        rsa.ImportFromPem(key.RootElement.GetProperty("private_key").GetString());

        static string B64(byte[] bytes) => Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
        long now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        string header = B64(Encoding.UTF8.GetBytes("""{"alg":"RS256","typ":"JWT"}"""));
        var claims = new MemoryStream();
        using (var json = new Utf8JsonWriter(claims))
        {
            json.WriteStartObject();
            json.WriteString("iss", email);
            json.WriteString("scope", scope);
            json.WriteString("aud", tokenUri);
            json.WriteNumber("iat", now);
            json.WriteNumber("exp", now + 3600);
            json.WriteEndObject();
        }
        string unsigned = header + "." + B64(claims.ToArray());
        string jwt = unsigned + "." + B64(rsa.SignData(Encoding.ASCII.GetBytes(unsigned), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));

        using var http = new HttpClient();
        using var form = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "urn:ietf:params:oauth:grant-type:jwt-bearer",
            ["assertion"] = jwt,
        });
        using HttpResponseMessage response = http.PostAsync(tokenUri, form).GetAwaiter().GetResult();
        string body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        using var answer = JsonDocument.Parse(body);
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException($"Token request: HTTP {(int)response.StatusCode}: {answer.RootElement.GetProperty("error").GetString()}");
        return answer.RootElement.GetProperty("access_token").GetString()!;
    }

    private static Settings? Load()
    {
        string? bucket = Environment.GetEnvironmentVariable("SESERAGI_GCS_BUCKET");
        string? project = Environment.GetEnvironmentVariable("SESERAGI_GCS_PROJECT");
        string? keyFile = Environment.GetEnvironmentVariable("SESERAGI_GCS_KEY_FILE");
        if (bucket is null)
        {
            string? file = RealAws.SecretsFile("GCS");
            if (file is null) return null;
            string[] lines = [.. File.ReadAllLines(file).Select(l => l.Trim())];
            if (lines.Length == 0 || lines[0].Length == 0) return null;
            bucket = lines[0];
            if (lines.Length > 1 && lines[1].Length > 0) project ??= lines[1];
        }
        keyFile ??= RealAws.SecretsFile("gcs-key.json");
        return keyFile is null || !File.Exists(keyFile) ? null : new Settings(bucket, project, Path.GetFullPath(keyFile));
    }
}

/// <summary>A fact that runs only where a real bucket is configured (see <see cref="RealGcs"/>).</summary>
public sealed class GcsFactAttribute : FactAttribute
{
    public GcsFactAttribute()
    {
        if (!RealGcs.Available) Skip = "No real GCS bucket: set SESERAGI_GCS_* or write .secrets/GCS and .secrets/gcs-key.json.";
    }
}

/// <summary>A theory that runs only where a real bucket is configured.</summary>
public sealed class GcsTheoryAttribute : TheoryAttribute
{
    public GcsTheoryAttribute()
    {
        if (!RealGcs.Available) Skip = "No real GCS bucket: set SESERAGI_GCS_* or write .secrets/GCS and .secrets/gcs-key.json.";
    }
}

/// <summary>Deletes everything the run wrote below its prefix.</summary>
public sealed class RealGcsCleanup : IDisposable
{
    public void Dispose()
    {
        if (!RealGcs.Available) return;
        try
        {
            using var fs = new ObjectStoreFileSystem($"gs://{RealGcs.Bucket}/{RealGcs.RunPrefix}", RealGcs.Args(), ObjectStoreBackend.Managed);
            foreach (FileEntry file in fs.ListAll("")) fs.Delete(file.Path);
        }
        catch (Exception e)
        {
            Console.Error.WriteLine($"Cleaning up GCS failed: {e.Message}");
        }
    }
}

/// <summary>
/// Smoke tests on a real GCS bucket, run once with the default store (managed, JSON API) and once with
/// <c>POLARS_NET_OBJECT_STORE=native</c> (object_store, XML API): the file system contract under every sign-in (key
/// file, key JSON, access token), put-if-absent races, and the Delta kernel end to end.
/// </summary>
[Trait("RealCloud", "Gcs")]
public sealed class RealCloudGcsTests : IClassFixture<RealGcsCleanup>
{
    private static string Backend => ObjectStoreFileSystem.ConfiguredBackend == ObjectStoreBackend.Native ? "native" : "managed";

    private sealed class Contract(CloudArgs args) : FileSystemContract
    {
        protected override string NewRoot() => RealGcs.NewUrl("contract");

        protected override IWritableFileSystem Open(string root)
        {
            var fs = new ObjectStoreFileSystem(root, args);
            Assert.Equal(Backend, fs.Implementation);
            return fs;
        }
    }

    [GcsTheory]
    [InlineData(GcsSignIn.KeyFile)]
    [InlineData(GcsSignIn.KeyJson)]
    [InlineData(GcsSignIn.OAuthToken)]
    public void TheFileSystemContract_Holds(GcsSignIn signIn)
    {
        var contract = new Contract(RealGcs.Args(signIn));
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
        Assert.True(failures.Count == 0, $"{Backend} {signIn}: " + string.Join("\n", failures));
    }

    [GcsFact]
    public void PutIfAbsent_HasOneWinner_AndCommitsConflict() =>
        KernelObjectStoreCheck.PutIfAbsentHolds(RealGcs.NewUrl("race"), RealGcs.Args());

    [GcsFact]
    public void StreamedWrites_SplitFiles_AndReadFootersFromTheStore() =>
        KernelObjectStoreCheck.StreamedWritesWork(RealGcs.NewUrl("streamed"), RealGcs.Args());

    [GcsFact]
    public void Vacuum_DeletesExactlyWhatTheTableNoLongerNeeds() =>
        KernelObjectStoreCheck.VacuumWorks(RealGcs.NewUrl("vacuum"), RealGcs.Args());

    /// <summary>A token is all it takes: no key is needed, and a wrong token is refused.</summary>
    [GcsFact]
    public void AnAccessToken_Signs_AndAWrongOneIsRefused()
    {
        string url = RealGcs.NewUrl("token");
        using (var fs = new ObjectStoreFileSystem(url, RealGcs.Args(GcsSignIn.OAuthToken)))
        {
            Assert.Equal(Backend, fs.Implementation);
            Assert.True(fs.TryCreate("probe.json", "{}"u8.ToArray()));
            Assert.Single(fs.List(""));
        }
        var wrong = new Dictionary<string, string> { ["google_oauth_token"] = "ya29.not-a-token" };
        using var refused = new ObjectStoreFileSystem(url, CloudArgs.ForLocation(url, [.. wrong.Keys], [.. wrong.Values]));
        Assert.ThrowsAny<Exception>(() => refused.ReadAll("probe.json"));
    }

    [GcsTheory]
    [InlineData(GcsSignIn.KeyFile)]
    [InlineData(GcsSignIn.OAuthToken)]
    public void ATable_WithIdentityAndRowTracking_LivesThroughEveryOperation(GcsSignIn signIn)
    {
        string url = RealGcs.NewUrl("lifecycle");
        var keys = RealGcs.Keys(signIn);
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

        using var latest = DataFrame.ReadDelta(url, cloudOptions: options).Sort(["Id"]);
        Assert.Equal([1L, 3L, 4L, 5L], latest["Id"].ToArray<long>());
        Assert.Equal([1.0, 30.0, 4.0, 5.0], latest["Price"].ToArray<double>());
        var now = RowIds();
        Assert.All(new[] { 1L, 3L, 4L }, id => Assert.Equal(written[id], now[id]));
        Assert.DoesNotContain(now[5], written.Values);

        using (var v1 = DataFrame.ReadDelta(url, version: 1, cloudOptions: options))
            Assert.Equal(4, v1.Height);
        using (var history = Delta.History(url, cloudOptions: options))
            Assert.Equal(["CREATE TABLE", "WRITE", "DELETE", "UPDATE", "MERGE"], history["operation"].ToArray<string>().Reverse());
        using (var changes = Delta.ReadChangeDataFeed(url, startVersion: 1, endVersion: 4, cloudOptions: options).Collect())
            Assert.Contains("delete", changes["_change_type"].ToArray<string>());
    }
}
