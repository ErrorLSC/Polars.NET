using System.Text;
using Kaminari.Seseragi.Lakehouse.Storage;
using Kaminari.Seseragi.Lakehouse.Storage.S3;

namespace Seseragi.Lakehouse.Tests;

public class AwsCredentialTests
{
    private static readonly RetrySettings NoRetry = new(0, TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(10));

    private const string CredentialsJson = """
        {"Code":"Success","Type":"AWS-HMAC","AccessKeyId":"AKID","SecretAccessKey":"SECRET","Token":"TOKEN","Expiration":"2099-01-01T00:00:00Z"}
        """;

    // ---- the cache ----

    [Fact]
    public void TheCache_RefreshesWhenLessThanTheMinimumLifeIsLeft_AndForgetsFailures()
    {
        var now = new DateTimeOffset(2026, 11, 11, 0, 0, 0, TimeSpan.Zero);
        int fetches = 0;
        bool fail = false;
        var cache = new CachedCredential<string>(() =>
        {
            fetches++;
            if (fail) throw new StorageException("down");
            return new Temporary<string>($"v{fetches}", now.AddMinutes(10));
        }, clock: () => now);

        Assert.Equal("v1", cache.Get());
        now = now.AddMinutes(4);
        Assert.Equal("v1", cache.Get()); // six minutes left
        now = now.AddMinutes(2);
        Assert.Equal("v2", cache.Get()); // four minutes left: refreshed
        now = now.AddMinutes(6);
        fail = true;
        Assert.Throws<StorageException>(cache.Get);
        fail = false;
        Assert.Equal("v4", cache.Get()); // the failure was not remembered
    }

    [Fact]
    public void TheCache_UsesAShortLivedCredentialJustFetched_AndCredentialsWithoutExpiry()
    {
        var now = new DateTimeOffset(2026, 11, 11, 0, 0, 0, TimeSpan.Zero);
        int fetches = 0;
        var shortLived = new CachedCredential<string>(() => new Temporary<string>($"v{++fetches}", now.AddMinutes(1)), clock: () => now);
        Assert.Equal("v1", shortLived.Get());
        Assert.Equal("v1", shortLived.Get()); // within the fetch backoff
        now = now.AddSeconds(1);
        Assert.Equal("v2", shortLived.Get());

        int forever = 0;
        var noExpiry = new CachedCredential<string>(() => new Temporary<string>($"f{++forever}", null), clock: () => now);
        noExpiry.Get();
        now = now.AddYears(1);
        Assert.Equal("f1", noExpiry.Get());
    }

    // ---- the sources ----

    [Fact]
    public void InstanceMetadata_UsesAnImdsV2Session()
    {
        using var server = new FakeHttpServer(r => (r.Method, r.PathAndQuery) switch
        {
            ("PUT", "/latest/api/token") => r.Headers.GetValueOrDefault("X-aws-ec2-metadata-token-ttl-seconds") == "600" ? (200, "session") : (400, ""),
            (_, _) when r.Headers.GetValueOrDefault("X-aws-ec2-metadata-token") != "session" => (401, ""),
            ("GET", "/latest/meta-data/iam/security-credentials/") => (200, "my-role"),
            ("GET", "/latest/meta-data/iam/security-credentials/my-role") => (200, CredentialsJson),
            _ => (404, ""),
        });
        AwsCredential credential = AwsCredentialSources.Instance(server.Url + "/", imdsv1Fallback: false, NoRetry).Get();
        Assert.Equal(new AwsCredential("AKID", "SECRET", "TOKEN"), credential);
        Assert.Equal(3, server.Requests.Count);
    }

    [Fact]
    public void InstanceMetadata_FallsBackToImdsV1_OnlyWhenAllowed()
    {
        using var server = new FakeHttpServer(r => (r.Method, r.PathAndQuery) switch
        {
            ("PUT", _) => (403, ""),
            (_, _) when r.Headers.ContainsKey("X-aws-ec2-metadata-token") => (400, ""),
            ("GET", "/latest/meta-data/iam/security-credentials/") => (200, "my-role"),
            ("GET", "/latest/meta-data/iam/security-credentials/my-role") => (200, CredentialsJson),
            _ => (404, ""),
        });
        Assert.Equal("AKID", AwsCredentialSources.Instance(server.Url, imdsv1Fallback: true, NoRetry).Get().KeyId);
        var refused = Assert.Throws<StorageException>(() => AwsCredentialSources.Instance(server.Url, imdsv1Fallback: false, NoRetry).Get());
        Assert.Equal(System.Net.HttpStatusCode.Forbidden, refused.Status);
    }

    [Fact]
    public void InstanceMetadata_OffTheCloud_FailsQuickly()
    {
        // Nothing listens on this port: the metadata connect timeout is short, and the failure is a storage error.
        var watch = System.Diagnostics.Stopwatch.StartNew();
        var failure = Assert.Throws<StorageException>(() => AwsCredentialSources.Instance("http://127.0.0.1:1", false, NoRetry).Get());
        Assert.Contains("No AWS credentials are configured", failure.Message);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(10), $"took {watch.Elapsed}");
    }

    [Fact]
    public void ContainerCredentials_TaskAndPod()
    {
        string tokenFile = Path.GetTempFileName();
        try
        {
            File.WriteAllText(tokenFile, "pod-token\n");
            using var server = new FakeHttpServer(r => r.PathAndQuery switch
            {
                "/v2/credentials/abc" => (200, CredentialsJson),
                "/pod" => r.Headers.GetValueOrDefault("Authorization") == "pod-token" ? (200, CredentialsJson) : (401, ""),
                _ => (404, ""),
            });
            Assert.Equal("TOKEN", AwsCredentialSources.Task(server.Url + "/v2/credentials/abc", NoRetry).Get().Token);
            Assert.Equal("TOKEN", AwsCredentialSources.Pod(server.Url + "/pod", tokenFile, NoRetry).Get().Token);

            var missing = Assert.Throws<StorageException>(() => AwsCredentialSources.Pod(server.Url + "/pod", tokenFile + ".missing", NoRetry).Get());
            Assert.Contains("token file", missing.Message);
        }
        finally
        {
            File.Delete(tokenFile);
        }
    }

    [Fact]
    public void WebIdentity_AssumesTheRole_WithTheTokenReadForEveryFetch()
    {
        string tokenFile = Path.GetTempFileName();
        try
        {
            File.WriteAllText(tokenFile, "jwt+1/=");
            using var server = new FakeHttpServer(r => r.Method == "POST"
                ? (200, """
                    <AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
                      <AssumeRoleWithWebIdentityResult>
                        <SubjectFromWebIdentityToken>sub</SubjectFromWebIdentityToken>
                        <AssumedRoleUser><Arn>arn:aws:sts::1:assumed-role/r/s</Arn><AssumedRoleId>id:s</AssumedRoleId></AssumedRoleUser>
                        <Credentials>
                          <SessionToken>STS-TOKEN</SessionToken><SecretAccessKey>STS-SECRET</SecretAccessKey>
                          <Expiration>2099-01-01T00:00:00Z</Expiration><AccessKeyId>ASIA</AccessKeyId>
                        </Credentials>
                      </AssumeRoleWithWebIdentityResult>
                    </AssumeRoleWithWebIdentityResponse>
                    """)
                : (405, ""));
            var source = AwsCredentialSources.WebIdentity(tokenFile, "arn:aws:iam::1:role/r", "sess", server.Url + "/", NoRetry, allowHttp: true);
            Assert.Equal(new AwsCredential("ASIA", "STS-SECRET", "STS-TOKEN"), source.Get());

            SeenRequest seen = Assert.Single(server.Requests);
            Assert.Contains("Action=AssumeRoleWithWebIdentity", seen.PathAndQuery);
            Assert.Contains("DurationSeconds=3600", seen.PathAndQuery);
            Assert.Contains("RoleArn=arn%3Aaws%3Aiam%3A%3A1%3Arole%2Fr", seen.PathAndQuery);
            Assert.Contains("RoleSessionName=sess", seen.PathAndQuery);
            Assert.Contains("WebIdentityToken=jwt%2B1%2F%3D", seen.PathAndQuery);
            Assert.False(seen.Headers.ContainsKey("Authorization"));

            // The token must not travel in clear.
            var plain = AwsCredentialSources.WebIdentity(tokenFile, "arn", "s", server.Url, NoRetry);
            Assert.Contains("https", Assert.Throws<StorageException>(plain.Get).Message);
        }
        finally
        {
            File.Delete(tokenFile);
        }
    }

    // ---- the choice ----

    private static readonly Dictionary<string, string> NoEnvironment = [];

    private static string Origin(Dictionary<string, string> options, Dictionary<string, string>? environment = null)
    {
        options.TryAdd("aws_region", "eu-west-1");
        S3Settings settings = S3Settings.Resolve("bucket", options, environment ?? NoEnvironment, null, out string? reason)
            ?? throw new InvalidOperationException(reason);
        return settings.Credential switch
        {
            CachedCredential<AwsCredential> cached => cached.Origin,
            StaticCredential<AwsCredential> => "static",
            null => "unsigned",
            _ => "?",
        };
    }

    [Fact]
    public void Credentials_AreChosenInTheLibrarysOrder()
    {
        var all = new Dictionary<string, string>
        {
            ["aws_access_key_id"] = "id", ["aws_secret_access_key"] = "s", ["aws_web_identity_token_file"] = "/t",
            ["aws_role_arn"] = "arn:r", ["aws_container_credentials_relative_uri"] = "/v2/c",
            ["aws_container_credentials_full_uri"] = "http://pod/creds", ["aws_container_authorization_token_file"] = "/pt",
        };
        Assert.Equal("static", Origin(new(all)));
        all.Remove("aws_access_key_id");
        all.Remove("aws_secret_access_key");
        Assert.Equal("web identity arn:r at https://sts.eu-west-1.amazonaws.com", Origin(new(all)));
        Assert.Equal("web identity arn:r at http://sts.local", Origin(new(all) { ["aws_endpoint_url_sts"] = "http://sts.local" }));
        all.Remove("aws_role_arn");
        Assert.Equal("task role http://169.254.170.2/v2/c", Origin(new(all)));
        all.Remove("aws_container_credentials_relative_uri");
        Assert.Equal("pod identity http://pod/creds", Origin(new(all)));
        all.Remove("aws_container_authorization_token_file");
        Assert.Equal("instance metadata http://169.254.169.254", Origin(new(all)));
        Assert.Equal("instance metadata http://imds:1 (IMDSv1 fallback)",
            Origin(new() { ["aws_metadata_endpoint"] = "http://imds:1", ["aws_imdsv1_fallback"] = "yes" }));
        Assert.Equal("unsigned", Origin(new() { ["aws_skip_signature"] = "true" }));

        // The environment takes part in the same order (as AWS_* variables), below explicit keys.
        Assert.Equal("web identity arn:env at https://sts.eu-west-1.amazonaws.com",
            Origin([], new() { ["AWS_WEB_IDENTITY_TOKEN_FILE"] = "/t", ["AWS_ROLE_ARN"] = "arn:env" }));
        Assert.Equal("static", Origin(new() { ["aws_access_key_id"] = "id", ["aws_secret_access_key"] = "s" },
            new() { ["AWS_WEB_IDENTITY_TOKEN_FILE"] = "/t", ["AWS_ROLE_ARN"] = "arn:env" }));
        // Stores are opened per table operation: one configuration shares one source in the process.
        S3Settings Settings() => S3Settings.Resolve("bucket", new Dictionary<string, string> { ["aws_region"] = "r", ["aws_metadata_endpoint"] = "http://shared:1" },
            NoEnvironment, null, out _)!;
        Assert.Same(Settings().Credential, Settings().Credential);
        // An empty key still counts as given.
        Assert.Equal("static", Origin([], new() { ["AWS_ACCESS_KEY_ID"] = "", ["AWS_SECRET_ACCESS_KEY"] = "" }));
    }

    [Fact]
    public void TheCredentialsFile_FillsInBelowExplicitKeys_InTheEnginesWay()
    {
        const string file = """
            [default]
            aws_access_key_id = FILEKEY
            aws_secret_access_key = FILESECRET
            [other]
            aws_session_token = FILETOKEN
            """;
        var environment = new Dictionary<string, string> { ["AWS_ACCESS_KEY_ID"] = "ENVKEY" };
        Assert.Equal(new() { ["secret_access_key"] = "FILESECRET", ["token"] = "FILETOKEN" },
            S3Settings.CredentialsFile(file, new Dictionary<string, string> { ["access_key_id"] = "ENVKEY" }));
        // The first key missing from the file ends the reading.
        Assert.Equal(new() { ["access_key_id"] = "K" }, S3Settings.CredentialsFile("aws_access_key_id = K\naws_session_token = T", new Dictionary<string, string>()));
        Assert.Empty(S3Settings.CredentialsFile(file, new Dictionary<string, string> { ["access_key_id"] = "a", ["secret_access_key"] = "b", ["token"] = "c" }));

        // Explicit keys win, but a token from the file joins them (as in the engine).
        S3Settings settings = S3Settings.Resolve("bucket",
            new Dictionary<string, string> { ["aws_region"] = "r", ["aws_access_key_id"] = "X", ["aws_secret_access_key"] = "Y" },
            environment, null, out _, awsCredentialsFile: file)!;
        Assert.Equal(new AwsCredential("X", "Y", "FILETOKEN"), settings.Credential!.Get());
        S3Settings fromFile = S3Settings.Resolve("bucket", new Dictionary<string, string> { ["aws_region"] = "r" },
            NoEnvironment, null, out _, awsCredentialsFile: file)!;
        Assert.Equal(new AwsCredential("FILEKEY", "FILESECRET", "FILETOKEN"), fromFile.Credential!.Get());
    }
}
