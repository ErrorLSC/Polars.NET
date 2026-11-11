using System.Text;
using Kaminari.Seseragi.Lakehouse.Storage;
using Kaminari.Seseragi.Lakehouse.Storage.Azure;

namespace Seseragi.Lakehouse.Tests;

public class AzureClientTests
{
    private const string Date = "Wed, 11 Nov 2026 09:00:00 GMT";
    private static readonly byte[] EmulatorKey = Convert.FromBase64String(AzureSettings.EmulatorKey);

    private static readonly Dictionary<string, string> NoEnvironment = [];

    // Expected signatures were computed independently (Python hmac/hashlib) over the strings asserted here.

    [Fact]
    public void SharedKey_OfAListing_SortsAndLowerCasesTheQuery_WithDecodedValues()
    {
        var headers = new Dictionary<string, string> { ["x-ms-version"] = "2023-11-03", ["x-ms-date"] = Date };
        List<KeyValuePair<string, string>> query =
            [new("restype", "container"), new("comp", "list"), new("maxresults", "2"), new("prefix", "a b/"), new("startFrom", "a b/x")];
        string toSign = SharedKey.StringToSign("GET", "devstoreaccount1", "/devstoreaccount1/polars", query, headers);

        Assert.Equal(
            "GET\n\n\n\n\n\n\n\n\n\n\n\n"
            + $"x-ms-date:{Date}\nx-ms-version:2023-11-03\n"
            + "/devstoreaccount1/devstoreaccount1/polars\ncomp:list\nmaxresults:2\nprefix:a b/\nrestype:container\nstartfrom:a b/x",
            toSign);
        Assert.Equal("SharedKey devstoreaccount1:0Rl62Lz77pwL9XjehJh4sz1i87rC3/X7v3U1paaqDlE=",
            SharedKey.Authorization("devstoreaccount1", EmulatorKey, toSign));
    }

    [Fact]
    public void SharedKey_OfAConditionalPut_SignsLengthAndCondition_AndThePathAsSent()
    {
        var headers = new Dictionary<string, string>
        {
            ["x-ms-version"] = "2023-11-03", ["x-ms-date"] = Date, ["x-ms-blob-type"] = "BlockBlob",
            ["if-none-match"] = "*", ["content-length"] = "2",
        };
        string toSign = SharedKey.StringToSign("PUT", "acct", "/polars/a%20b/0.json", [], headers);

        Assert.Equal(
            "PUT\n\n\n2\n\n\n\n\n\n*\n\n\n"
            + $"x-ms-blob-type:BlockBlob\nx-ms-date:{Date}\nx-ms-version:2023-11-03\n/acct/polars/a%20b/0.json",
            toSign);
        Assert.Equal("SharedKey acct:elNKvsm/0rEoOy//ry9xs4G7ZceZCMxXz5HZdfCkJEk=", SharedKey.Authorization("acct", EmulatorKey, toSign));
    }

    [Fact]
    public void SharedKey_LeavesAZeroContentLengthEmpty()
    {
        var headers = new Dictionary<string, string> { ["content-length"] = "0" };
        Assert.StartsWith("PUT\n\n\n\n", SharedKey.StringToSign("PUT", "a", "/c/k", [], headers));
    }

    private static AzureSettings? Resolve(
        Dictionary<string, string> options, Dictionary<string, string>? environment = null, string? urlAccount = null) =>
        AzureSettings.Resolve(urlAccount, options, environment ?? NoEnvironment, out _);

    private static string? Reason(Dictionary<string, string> options, Dictionary<string, string>? environment = null)
    {
        Assert.Null(AzureSettings.Resolve(null, options, environment ?? NoEnvironment, out string? reason));
        return reason;
    }

    [Fact]
    public void Credentials_AreChosenInTheLibrarysOrder_WhateverTheirSource()
    {
        var all = new Dictionary<string, string>
        {
            ["account_name"] = "acct", ["bearer_token"] = "t", ["azure_storage_account_key"] = AzureSettings.EmulatorKey, ["sas_token"] = "sv=1&sig=x",
        };
        Assert.IsType<AzureAuth.Bearer>(Resolve(all)!.Auth);
        all.Remove("bearer_token");
        Assert.IsType<AzureAuth.SharedKey>(Resolve(all)!.Auth);
        all.Remove("azure_storage_account_key");
        Assert.IsType<AzureAuth.Sas>(Resolve(all)!.Auth);

        // A client secret in the environment comes before an explicit SAS: the library uses OAuth.
        var secret = new Dictionary<string, string> { ["AZURE_CLIENT_ID"] = "id", ["AZURE_CLIENT_SECRET"] = "s", ["AZURE_TENANT_ID"] = "t" };
        Assert.Equal("client secret of id at https://login.microsoftonline.com/t/oauth2/v2.0/token", Origin(all, secret));
        // ... but not before an account key.
        all["access_key"] = AzureSettings.EmulatorKey;
        Assert.IsType<AzureAuth.SharedKey>(Resolve(all, secret)!.Auth);
    }

    [Fact]
    public void ExplicitKeys_WinOverTheEnvironment()
    {
        var environment = new Dictionary<string, string> { ["AZURE_STORAGE_ACCOUNT_NAME"] = "fromenv", ["AZURE_STORAGE_TOKEN"] = "t" };
        Assert.Equal("https://fromenv.blob.core.windows.net", Resolve([], environment)!.ServiceUrl);
        Assert.Equal("explicit", Resolve(new() { ["Azure_Storage_Account_Name"] = "explicit" }, environment)!.Account);
        Assert.Equal("fromurl", Resolve(new() { ["account_name"] = "explicit" }, environment, urlAccount: "fromurl")!.Account);
    }

    [Fact]
    public void TheEmulator_IgnoresTheEndpoint_AndPutsTheAccountIntoThePath()
    {
        var options = new Dictionary<string, string> { ["use_emulator"] = "yes", ["endpoint"] = "http://ignored" };
        AzureSettings settings = Resolve(options)!;
        Assert.Equal("http://127.0.0.1:10000/devstoreaccount1", settings.ServiceUrl);
        Assert.Equal(EmulatorKey, Assert.IsType<AzureAuth.SharedKey>(settings.Auth).Key);

        var environment = new Dictionary<string, string> { ["AZURITE_BLOB_STORAGE_URL"] = "http://azurite:1234/" };
        options["account_name"] = "other";
        Assert.Equal("http://azurite:1234/other", Resolve(options, environment)!.ServiceUrl);
    }

    [Fact]
    public void UnsignedRequests_NeedNoCredentials()
    {
        AzureSettings settings = Resolve(new() { ["account_name"] = "acct", ["azure_skip_signature"] = "1" })!;
        Assert.Null(settings.Auth);
    }

    private static string Origin(Dictionary<string, string> options, Dictionary<string, string>? environment = null) =>
        Resolve(options, environment)!.Auth switch
        {
            AzureAuth.Bearer { Token: CachedCredential<string> cached } => cached.Origin,
            AzureAuth.Bearer => "static bearer",
            AzureAuth.SharedKey => "shared key",
            AzureAuth.Sas => "sas",
            null => "unsigned",
            _ => "?",
        };

    [Fact]
    public void TokenCredentials_FollowTheLibrarysOrder_DownToTheManagedIdentity()
    {
        var keys = new Dictionary<string, string>
        {
            ["account_name"] = "acct", ["client_id"] = "cid", ["tenant_id"] = "ten", ["federated_token_file"] = "/f",
            ["client_secret"] = "s", ["azure_authority_host"] = "http://login.local", ["use_azure_cli"] = "true",
        };
        Assert.Equal("workload identity of cid at http://login.local/ten/oauth2/v2.0/token", Origin(new(keys)));
        keys.Remove("federated_token_file");
        Assert.Equal("client secret of cid at http://login.local/ten/oauth2/v2.0/token", Origin(new(keys)));
        keys.Remove("client_secret");
        Assert.Equal("Azure CLI", Origin(new(keys)));
        keys.Remove("use_azure_cli");
        // A client id alone picks the managed identity it names.
        Assert.Equal("managed identity at http://169.254.169.254/metadata/identity/oauth2/token (client_id)", Origin(new(keys)));
        Assert.Equal("managed identity at http://msi.local/token (msi_res_id)",
            Origin(new() { ["account_name"] = "acct", ["object_id"] = "o", ["msi_resource_id"] = "r" }, new() { ["IDENTITY_ENDPOINT"] = "http://msi.local/token" }));
        Assert.Equal("managed identity at http://explicit/token",
            Origin(new() { ["account_name"] = "acct", ["msi_endpoint"] = "http://explicit/token" }, new() { ["IDENTITY_ENDPOINT"] = "http://msi.local/token" }));
        // One configuration shares one source in the process.
        Assert.Same(((AzureAuth.Bearer)Resolve(new() { ["account_name"] = "acct" })!.Auth!).Token,
            ((AzureAuth.Bearer)Resolve(new() { ["account_name"] = "acct" })!.Auth!).Token);
    }

    [Theory]
    [InlineData("use_azure_cli", "maybe", "use_azure_cli")]
    [InlineData("azure_use_fabric_endpoint", "true", "Fabric")]
    [InlineData("azure_proxy_url", "socks5://proxy", "http:// proxies")]
    [InlineData("allow_invalid_certificates", "perhaps", "allow_invalid_certificates")]
    [InlineData("use_emulator", "maybe", "use_emulator")]
    public void SetupsTheClientLacks_AreLeftToTheEngine(string key, string value, string reason)
    {
        var options = new Dictionary<string, string> { ["account_name"] = "acct", ["client_id"] = "id", ["tenant_id"] = "t", [key] = value };
        Assert.Contains(reason, Reason(options));
    }

    [Fact]
    public void UnknownKeys_AreIgnored()
    {
        // "azure_use_emulator" is not one of the library's spellings.
        var options = new Dictionary<string, string> { ["account_name"] = "acct", ["access_key"] = AzureSettings.EmulatorKey, ["azure_use_emulator"] = "true", ["what"] = "ever" };
        Assert.Equal("https://acct.blob.core.windows.net", Resolve(options)!.ServiceUrl);
    }

    [Fact]
    public void ASasToken_IsDecodedOnce_AndSplitAtTheFirstEquals()
    {
        var sas = Assert.IsType<AzureAuth.Sas>(AzureSettings.SasAuth("?sv=2025-11-05&sig=a%2Bb%3D%3D&se=2026-11-11T10%3A00%3A00Z", out _));
        Assert.Equal([new("sv", "2025-11-05"), new("sig", "a+b=="), new("se", "2026-11-11T10:00:00Z")], sas.Pairs);
        Assert.Null(AzureSettings.SasAuth("sv=1&broken", out string? reason));
        Assert.NotNull(reason);
    }

    [Theory]
    [InlineData("az", "c", "c", null)]
    [InlineData("abfss", "c@acct.dfs.core.windows.net", "c", "acct")]
    [InlineData("abfs", "c@Acct.blob.core.windows.net", "c", "acct")]
    [InlineData("adl", "c", "c", null)]
    public void Locations_NameTheContainer_AndPerhapsTheAccount(string scheme, string authority, string container, string? account)
    {
        Assert.Equal((container, account), CloudStorage.ParseAzureAuthority(scheme, authority, out _));
    }

    [Theory]
    [InlineData("abfss", "c@ws.dfs.fabric.microsoft.com")]
    [InlineData("abfss", "c@acct.example.com")]
    [InlineData("az", "c.d")]
    [InlineData("adl", "c@acct.dfs.core.windows.net")]
    public void OtherLocations_AreLeftToTheEngine(string scheme, string authority)
    {
        Assert.Null(CloudStorage.ParseAzureAuthority(scheme, authority, out string? reason));
        Assert.NotNull(reason);
    }

    [Fact]
    public void Listing_SkipsDirectoriesAndPrefixes_DecodesEncodedNames_AndReadsTheMarker()
    {
        const string xml = """
            <?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults ServiceEndpoint="http://x/" ContainerName="c">
              <Prefix>t/</Prefix><MaxResults>3</MaxResults>
              <Blobs>
                <Blob><Name>t/a.json</Name><Properties><Last-Modified>Wed, 11 Nov 2026 09:00:00 GMT</Last-Modified><Content-Length>12</Content-Length><BlobType>BlockBlob</BlobType></Properties><Metadata><k>v</k></Metadata></Blob>
                <BlobPrefix><Name>t/sub/</Name></BlobPrefix>
                <Blob><Name>t/dir</Name><Properties><Content-Length>0</Content-Length><ResourceType>directory</ResourceType></Properties></Blob>
                <Blob><Name Encoded="true">t/%EF%BF%BEx</Name><Properties><Content-Length>3</Content-Length><Last-Modified>Wed, 11 Nov 2026 09:00:01 GMT</Last-Modified></Properties></Blob>
              </Blobs>
              <NextMarker>2!abc</NextMarker>
            </EnumerationResults>
            """;
        var (blobs, next) = AzureClient.ParseListing(Encoding.UTF8.GetBytes(xml));
        Assert.Equal(["t/a.json", "t/￾x"], blobs.Select(b => b.Key));
        Assert.Equal(12, blobs[0].Size);
        Assert.Equal(new DateTimeOffset(2026, 11, 11, 9, 0, 0, TimeSpan.Zero), blobs[0].LastModified);
        Assert.Equal("2!abc", next);

        var (none, last) = AzureClient.ParseListing(Encoding.UTF8.GetBytes(
            "<EnumerationResults><Blobs><BlobPrefix><Name>p/</Name></BlobPrefix></Blobs><NextMarker /></EnumerationResults>"));
        Assert.Empty(none);
        Assert.Null(last);
    }
}

public class AzureDirectoryRangeTests
{
    private static CloudFileSystem Open(FakeHttpServer server)
    {
        var keys = new Dictionary<string, string>
        {
            ["account_name"] = "acct", ["access_key"] = AzureSettings.EmulatorKey, ["endpoint"] = server.Url, ["allow_http"] = "true",
        };
        return CloudStorage.TryOpen("az://c/t", keys, RetrySettings.Default, new Dictionary<string, string>(), null, out string? reason)
            ?? throw new InvalidOperationException(reason);
    }

    // A real ADLS Gen2 account answers a range of a directory with 416 and no x-ms-resource-type; only its HEAD says
    // it is a directory.
    [Fact]
    public void ARangeOfADirectory_IsNoFile()
    {
        using var server = new FakeHttpServer(r => r.Method == "HEAD" ? (200, "") : (416, ""))
        {
            ResponseHeaders = ["x-ms-resource-type: directory"],
        };
        var fs = Open(server);
        Assert.Null(fs.ReadRange("dir", 0, 1));
        Assert.Equal(["GET", "HEAD"], server.Requests.Select(r => r.Method));
    }

    [Fact]
    public void ARangePastTheEndOfAFile_IsEmpty()
    {
        using var server = new FakeHttpServer(r => r.Method == "HEAD" ? (200, "") : (416, ""));
        var fs = Open(server);
        Assert.Equal([], fs.ReadRange("empty.json", 0, 1)!);
    }
}
