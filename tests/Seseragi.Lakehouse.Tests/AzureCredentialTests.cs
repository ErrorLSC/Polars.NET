using System.Globalization;
using System.Web;
using Kaminari.Seseragi.Lakehouse.Storage;
using Kaminari.Seseragi.Lakehouse.Storage.Azure;

namespace Seseragi.Lakehouse.Tests;

public class AzureCredentialTests
{
    private static readonly RetrySettings NoRetry = new(0, TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(10));

    private static readonly DateTimeOffset Now = new(2026, 11, 11, 9, 0, 0, TimeSpan.Zero);

    private static Dictionary<string, string> Form(string body)
    {
        var parsed = HttpUtility.ParseQueryString(body);
        return parsed.AllKeys.ToDictionary(k => k!, k => parsed[k]!);
    }

    [Fact]
    public void AClientSecret_IsExchangedForAStorageToken()
    {
        using var entra = new FakeHttpServer(r => r is { Method: "POST", PathAndQuery: "/tenant/oauth2/v2.0/token" }
            ? (200, """{"token_type":"Bearer","expires_in":3599,"ext_expires_in":3599,"access_token":"eyJ.secret"}""")
            : (404, ""));
        var source = AzureCredentialSources.ClientSecret(AzureCredentialSources.TokenUrl(entra.Url, "tenant"), "cid", "s3cr+t", NoRetry, () => Now);
        Assert.Equal("eyJ.secret", source.Get());

        SeenRequest seen = Assert.Single(entra.Requests);
        Assert.Equal("application/json", seen.Headers["Accept"]);
        Assert.Equal(new Dictionary<string, string>
        {
            ["client_id"] = "cid", ["client_secret"] = "s3cr+t", ["scope"] = "https://storage.azure.com/.default", ["grant_type"] = "client_credentials",
        }, Form(seen.Body));
    }

    [Fact]
    public void WorkloadIdentity_SendsTheFederatedToken_ReadForEveryFetch()
    {
        string tokenFile = Path.GetTempFileName();
        try
        {
            var now = Now;
            using var entra = new FakeHttpServer(_ => (200, """{"token_type":"Bearer","expires_in":600,"access_token":"wi"}"""));
            var source = AzureCredentialSources.WorkloadIdentity(AzureCredentialSources.TokenUrl(entra.Url, "t"), "cid", tokenFile, NoRetry, () => now);
            File.WriteAllText(tokenFile, "fed-1");
            Assert.Equal("wi", source.Get());
            now = now.AddMinutes(6); // four minutes left: refreshed
            File.WriteAllText(tokenFile, "fed-2");
            source.Get();

            Assert.Equal(["fed-1", "fed-2"], entra.Requests.Select(r => Form(r.Body)["client_assertion"]));
            Assert.Equal("urn:ietf:params:oauth:client-assertion-type:jwt-bearer", Form(entra.Requests.First().Body)["client_assertion_type"]);
            Assert.False(Form(entra.Requests.First().Body).ContainsKey("client_secret"));
        }
        finally
        {
            File.Delete(tokenFile);
        }
    }

    [Fact]
    public void AManagedIdentity_AsksTheEndpoint_WithTheChosenIdentity_AndTheAppServiceHeader()
    {
        long expiresOn = Now.AddHours(1).ToUnixTimeSeconds();
        using var msi = new FakeHttpServer(r => r.Headers.GetValueOrDefault("metadata") == "true"
            ? (200, $$"""{"access_token":"mi","expires_on":"{{expiresOn}}","resource":"https://storage.azure.com","token_type":"Bearer"}""")
            : (400, ""));
        var source = AzureCredentialSources.ManagedIdentity(msi.Url + "/token", "cid", null, "/sub/res", "header-secret", NoRetry, () => Now);
        Assert.Equal("mi", source.Get());

        SeenRequest seen = Assert.Single(msi.Requests);
        Assert.Equal("/token?api-version=2019-08-01&resource=https%3A%2F%2Fstorage.azure.com&msi_res_id=%2Fsub%2Fres", seen.PathAndQuery);
        Assert.Equal("header-secret", seen.Headers["x-identity-header"]);
    }

    [Fact]
    public void AManagedIdentity_OffTheCloud_FailsQuickly_SayingWhy()
    {
        var watch = System.Diagnostics.Stopwatch.StartNew();
        var failure = Assert.Throws<StorageException>(() =>
            AzureCredentialSources.ManagedIdentity("http://127.0.0.1:1/token", null, null, null, null, NoRetry).Get());
        Assert.Contains("No Azure credentials are configured", failure.Message);
        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(10));
    }

    [Fact]
    public void Refusals_CarryEntrasDescription()
    {
        using var entra = new FakeHttpServer(_ => (401, """{"error":"invalid_client","error_description":"AADSTS7000215: Invalid client secret provided.\r\nTrace ID: x"}"""));
        var failure = Assert.Throws<StorageException>(() =>
            AzureCredentialSources.ClientSecret(AzureCredentialSources.TokenUrl(entra.Url, "t"), "cid", "bad", NoRetry).Get());
        Assert.Contains("AADSTS7000215: Invalid client secret provided.", failure.Message);
        Assert.DoesNotContain("bad", failure.Message);
    }

    [Fact]
    public void TheAzureCli_GivesItsToken_WithTheExpiryInLocalTime()
    {
        if (OperatingSystem.IsWindows()) return; // the stand-in is a shell script
        string script = Path.Combine(Path.GetTempPath(), $"az-{Guid.NewGuid():N}");
        string expires = DateTime.Now.AddHours(1).ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture);
        File.WriteAllText(script, $$"""
            #!/bin/sh
            [ "$*" = "account get-access-token --output json --scope https://storage.azure.com/.default" ] || { echo "bad arguments: $*" >&2; exit 2; }
            echo '{"accessToken":"cli-token","expiresOn":"{{expires}}","subscription":"s","tenant":"t","tokenType":"Bearer"}'
            """);
        try
        {
            File.SetUnixFileMode(script, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            Assert.Equal("cli-token", AzureCredentialSources.AzureCli(script).Get());

            File.WriteAllText(script, "#!/bin/sh\necho 'Please run az login' >&2\nexit 1\n");
            Assert.Contains("Please run az login", Assert.Throws<StorageException>(AzureCredentialSources.AzureCli(script).Get).Message);
            Assert.Contains("not installed", Assert.Throws<StorageException>(AzureCredentialSources.AzureCli(script + ".missing").Get).Message);
        }
        finally
        {
            File.Delete(script);
        }
    }
}
