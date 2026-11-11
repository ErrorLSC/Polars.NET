using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Storage;
using Kaminari.Seseragi.Lakehouse.Storage.Gcs;

namespace Seseragi.Lakehouse.Tests;

public class GcsClientTests
{
    private static readonly RSA Rsa = RSA.Create(2048);

    private static string Key(string? baseUrl = null, bool disableOAuth = false, string type = "service_account", string keyId = "kid1")
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("type", type);
            writer.WriteString("private_key_id", keyId);
            writer.WriteString("private_key", Rsa.ExportPkcs8PrivateKeyPem());
            writer.WriteString("client_email", "sa@p.iam.gserviceaccount.com");
            if (baseUrl is not null) writer.WriteString("gcs_base_url", baseUrl);
            if (disableOAuth) writer.WriteBoolean("disable_oauth", true);
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    private static byte[] FromBase64Url(string text)
    {
        string padded = text.Replace('-', '+').Replace('_', '/');
        return Convert.FromBase64String(padded.PadRight(padded.Length + (4 - padded.Length % 4) % 4, '='));
    }

    [Fact]
    public void Tokens_AreSelfSignedJwts_ThatTheKeyVerifies_AndAreRenewedBeforeTheyExpire()
    {
        var now = new DateTimeOffset(2026, 11, 11, 9, 0, 0, TimeSpan.Zero);
        ServiceAccountKey key = ServiceAccountKey.Parse(Key(), out _)!;
        using ServiceAccountToken tokens = ServiceAccountToken.Create(key, out _, () => now)!;

        string token = tokens.Get();
        string[] parts = token.Split('.');
        Assert.Equal(3, parts.Length);
        Assert.True(Rsa.VerifyData(Encoding.ASCII.GetBytes(parts[0] + "." + parts[1]), FromBase64Url(parts[2]),
            HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1));

        using var header = JsonDocument.Parse(FromBase64Url(parts[0]));
        Assert.Equal("RS256", header.RootElement.GetProperty("alg").GetString());
        Assert.Equal("kid1", header.RootElement.GetProperty("kid").GetString());
        using var claims = JsonDocument.Parse(FromBase64Url(parts[1]));
        Assert.Equal("sa@p.iam.gserviceaccount.com", claims.RootElement.GetProperty("iss").GetString());
        Assert.Equal("sa@p.iam.gserviceaccount.com", claims.RootElement.GetProperty("sub").GetString());
        Assert.Equal(ServiceAccountToken.Scope, claims.RootElement.GetProperty("scope").GetString());
        Assert.Equal(now.ToUnixTimeSeconds(), claims.RootElement.GetProperty("iat").GetInt64());
        Assert.Equal(now.ToUnixTimeSeconds() + 3600, claims.RootElement.GetProperty("exp").GetInt64());

        now = now.AddMinutes(50);
        Assert.Same(token, tokens.Get());
        now = now.AddMinutes(6);
        Assert.NotEqual(token, tokens.Get());
    }

    [Fact]
    public void ObjectNames_AreOnePathSegment_SlashesIncluded()
    {
        Assert.Equal("/storage/v1/b/bk/o/a%2Fx%20y%25.json", GcsClient.ObjectPath("bk", "a/x y%.json"));
    }

    private static GcsSettings? Resolve(
        Dictionary<string, string> options, Dictionary<string, string>? environment = null,
        Dictionary<string, string>? files = null, string? wellKnown = null) =>
        GcsSettings.Resolve(options, environment ?? [], path => files?.GetValueOrDefault(path), wellKnown, out _);

    private static string? Reason(
        Dictionary<string, string> options, Dictionary<string, string>? environment = null,
        Dictionary<string, string>? files = null, string? wellKnown = null)
    {
        Assert.Null(GcsSettings.Resolve(options, environment ?? [], path => files?.GetValueOrDefault(path), wellKnown, out string? reason));
        return reason;
    }

    [Fact]
    public void ServiceAccounts_ComeFromExplicitKeys_ThenGoogleVariables_ThenServiceAccount()
    {
        // Each key file names its own base URL, which shows which one was taken.
        var files = new Dictionary<string, string> { ["/plain"] = Key("http://plain"), ["/google"] = Key("http://google") };
        var environment = new Dictionary<string, string> { ["SERVICE_ACCOUNT"] = "/plain" };
        Assert.Equal("http://plain", Resolve([], environment, files)!.BaseUrl);
        environment["GOOGLE_SERVICE_ACCOUNT_PATH"] = "/google";
        Assert.Equal("http://google", Resolve([], environment, files)!.BaseUrl);
        files["/explicit"] = Key("http://explicit");
        Assert.Equal("http://explicit", Resolve(new() { ["service_account"] = "/explicit" }, environment, files)!.BaseUrl);
    }

    [Fact]
    public void APathAndAKey_FromAnySource_AreRefused()
    {
        var files = new Dictionary<string, string> { ["/plain"] = Key() };
        Assert.Contains("both", Reason(new() { ["service_account_key"] = Key() }, new() { ["SERVICE_ACCOUNT"] = "/plain" }, files));
    }

    [Fact]
    public void TheBaseUrlKey_WinsOverTheKeyFiles_AndDisabledOAuthSendsNoToken()
    {
        GcsSettings settings = Resolve(new() { ["google_base_url"] = "http://emulator/", ["service_account_key"] = Key("http://file", disableOAuth: true) })!;
        Assert.Equal("http://emulator", settings.BaseUrl);
        Assert.Null(settings.Token);
        Assert.Equal(GcsSettings.DefaultBaseUrl, Resolve(new() { ["service_account_key"] = Key() })!.BaseUrl);
        Assert.NotNull(Resolve(new() { ["service_account_key"] = Key() })!.Token);
    }

    [Fact]
    public void ApplicationDefaultCredentials_AreUsed_WhenTheyHoldAServiceAccount()
    {
        // The token's key id shows which file was taken; their emulator fields are not used (as in the engine).
        var files = new Dictionary<string, string>
        {
            ["/home/.adc"] = Key("http://adc", disableOAuth: true, keyId: "gcloud"), ["/named"] = Key(keyId: "named"),
            ["/user"] = Key(type: "authorized_user"),
        };
        static string KeyId(GcsSettings settings)
        {
            using var header = JsonDocument.Parse(FromBase64Url(settings.Token!.Get().Split('.')[0]));
            return header.RootElement.GetProperty("kid").GetString()!;
        }
        GcsSettings gcloud = Resolve([], files: files, wellKnown: "/home/.adc")!;
        Assert.Equal("gcloud", KeyId(gcloud));
        Assert.Equal(GcsSettings.DefaultBaseUrl, gcloud.BaseUrl);
        Assert.Equal("named", KeyId(Resolve(new() { ["google_application_credentials"] = "/named" }, files: files, wellKnown: "/home/.adc")!));
        // gcloud user credentials need their OAuth client and refresh token.
        Assert.Contains("refresh_token", Reason(new() { ["application_credentials"] = "/user" }, files: files));
        files["/user-ok"] = """{"type":"authorized_user","client_id":"cid.apps","client_secret":"cs","refresh_token":"1//rt"}""";
        Assert.Equal("gcloud user of client cid.apps",
            Assert.IsType<CachedCredential<string>>(Resolve(new() { ["application_credentials"] = "/user-ok" }, files: files)!.Token).Origin);
        Assert.Contains("cannot be read", Reason(new() { ["application_credentials"] = "/missing" }, files: files));
    }

    [Fact]
    public void WithoutCredentials_TheMetadataServerIsAsked_AtTheHostTheEnvironmentNames()
    {
        static string Origin(GcsSettings settings) => Assert.IsType<CachedCredential<string>>(settings.Token).Origin;
        Assert.Equal("metadata server metadata.google.internal / 169.254.169.254", Origin(Resolve([])!));
        Assert.Equal("metadata server meta:8080 / 10.0.0.1", Origin(Resolve([], new() { ["GCE_METADATA_HOST"] = "meta:8080", ["GCE_METADATA_IP"] = "10.0.0.1" })!));
        Assert.Equal("metadata server root:1 / 169.254.169.254", Origin(Resolve([], new() { ["GCE_METADATA_ROOT"] = "root:1" })!));
        Assert.Same(Resolve([])!.Token, Resolve([])!.Token);
        // Unsigned stores still sign uploads and deletes (with the metadata server's token), as in the engine.
        GcsSettings unsigned = Resolve(new() { ["google_skip_signature"] = "true" })!;
        Assert.True(unsigned.SkipSignature);
        Assert.NotNull(unsigned.Token);
        // With a key, reads go unsigned but uploads and deletes keep the token, as in the engine.
        GcsSettings keyed = Resolve(new() { ["google_skip_signature"] = "true", ["service_account_key"] = Key() })!;
        Assert.True(keyed.SkipSignature);
        Assert.NotNull(keyed.Token);
    }

    [Fact]
    public void TheMetadataServer_GivesTokens_ByNameOrElseByAddress()
    {
        using var server = new FakeHttpServer(r =>
            r.PathAndQuery == "/computeMetadata/v1/instance/service-accounts/default/token?audience=https%3A%2F%2Fwww.googleapis.com%2Foauth2%2Fv4%2Ftoken"
            && r.Headers.GetValueOrDefault("Metadata-Flavor") == "Google"
                ? (200, """{"access_token":"ya29.meta","expires_in":3599,"token_type":"Bearer"}""")
                : (404, ""));
        RetrySettings noRetry = new(0, TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(10));
        string address = server.Url["http://".Length..];
        Assert.Equal("ya29.meta", GcsCredentialSources.MetadataServer(address, "127.0.0.1:1", noRetry).Get());
        Assert.Equal("ya29.meta", GcsCredentialSources.MetadataServer("127.0.0.1:1", address, noRetry).Get());
        var failure = Assert.Throws<StorageException>(() => GcsCredentialSources.MetadataServer("127.0.0.1:1", "127.0.0.1:2", noRetry).Get());
        Assert.Contains("No Google Cloud credentials are configured", failure.Message);
    }

    [Fact]
    public void AGcloudUser_ExchangesTheRefreshToken()
    {
        using var server = new FakeHttpServer(r => r.Method == "POST"
            ? (200, """{"access_token":"ya29.user","expires_in":3599,"scope":"x","token_type":"Bearer","id_token":"a.b.c"}""")
            : (405, ""));
        RetrySettings noRetry = new(0, TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(10));
        Assert.Equal("ya29.user", GcsCredentialSources.AuthorizedUser("cid", "cs", "1//rt", noRetry, server.Url + "/token").Get());
        var form = System.Web.HttpUtility.ParseQueryString(Assert.Single(server.Requests).Body);
        Assert.Equal(("refresh_token", "cid", "cs", "1//rt"), (form["grant_type"], form["client_id"], form["client_secret"], form["refresh_token"]));
    }

    [Theory]
    [InlineData("google_proxy_url", "https://proxy")]
    [InlineData("allow_invalid_certificates", "perhaps")]
    [InlineData("skip_signature", "perhaps")]
    public void SetupsTheClientLacks_AreLeftToTheEngine(string key, string value)
    {
        Assert.NotNull(Reason(new() { ["service_account_key"] = Key(), [key] = value }));
    }

    [Fact]
    public void AVendedToken_IsUsedAsItIs_AndLeavesTheEnvironmentAndGcloudOut()
    {
        // As the engine hands such a token over: a credential provider, with a builder that reads no environment.
        var files = new Dictionary<string, string> { ["/home/.adc"] = Key(type: "authorized_user") };
        var environment = new Dictionary<string, string> { ["GOOGLE_BASE_URL"] = "http://fromenv", ["SERVICE_ACCOUNT"] = "/missing" };
        GcsSettings settings = Resolve(new() { ["Google_OAuth_Token"] = "ya29.t" }, environment, files, "/home/.adc")!;
        Assert.Equal("ya29.t", settings.Token!.Get());
        Assert.Equal(GcsSettings.DefaultBaseUrl, settings.BaseUrl);
        Assert.Equal("http://emulator", Resolve(new() { ["google_oauth_token"] = "t", ["google_base_url"] = "http://emulator" })!.BaseUrl);
    }

    [Fact]
    public void OnlyGsLocations_AreServed()
    {
        var keys = new Dictionary<string, string> { ["service_account_key"] = Key() };
        Assert.NotNull(CloudStorage.TryOpen("gs://bucket/t", keys, RetrySettings.Default, new Dictionary<string, string>(), null, out _));
        Assert.Null(CloudStorage.TryOpen("gcs://bucket/t", keys, RetrySettings.Default, new Dictionary<string, string>(), null, out string? reason));
        Assert.Contains("scheme", reason);
    }

    [Fact]
    public void Listing_ReadsItems_SizesAsText_AndThePageToken()
    {
        var (objects, next) = GcsClient.ParseListing(Encoding.UTF8.GetBytes("""
            {"kind":"storage#objects","prefixes":["t/sub/"],
             "items":[{"name":"t/a.json","size":"12","updated":"2026-11-11T09:00:00.123Z"}],
             "nextPageToken":"abc"}
            """));
        Assert.Equal("t/a.json", Assert.Single(objects).Key);
        Assert.Equal(12, objects[0].Size);
        Assert.Equal(new DateTimeOffset(2026, 11, 11, 9, 0, 0, 123, TimeSpan.Zero), objects[0].LastModified);
        Assert.Equal("abc", next);

        var (none, last) = GcsClient.ParseListing(Encoding.UTF8.GetBytes("""{"kind":"storage#objects","prefixes":["p/"]}"""));
        Assert.Empty(none);
        Assert.Null(last);
    }
}
