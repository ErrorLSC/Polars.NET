using System.Reflection;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.CSharp;
using Polars.NET.Core.Delta;
using Pl = Polars.CSharp.Polars;

namespace Polars.Integration.Tests;

/// <summary>The two kinds of real storage account the Azure smoke tests use.</summary>
public enum AzureAccountKind
{
    /// <summary>A flat blob namespace (StorageV2), addressed as <c>az://container/…</c> with the account name in the options.</summary>
    Blob,
    /// <summary>ADLS Gen2 (hierarchical namespace: real directories), addressed as <c>abfss://container@account.dfs.core.windows.net/…</c>.</summary>
    Adls,
}

/// <summary>How a test signs in to a real storage account.</summary>
public enum AzureSignIn
{
    /// <summary>The account key (SharedKey).</summary>
    AccountKey,
    /// <summary>A container SAS token, signed locally with the account key.</summary>
    Sas,
    /// <summary>A service principal's client secret (the store fetches its tokens).</summary>
    ClientSecret,
    /// <summary>A bearer token fetched by the test with the client secret and passed as is.</summary>
    BearerToken,
}

/// <summary>
/// Real Azure storage accounts for smoke tests: SESERAGI_AZURE_BLOB_ACCOUNT / _BLOB_KEY / _ADLS_ACCOUNT / _ADLS_KEY /
/// _CONTAINER / _TENANT_ID / _CLIENT_ID / _CLIENT_SECRET (and, for the VM test, _SUBSCRIPTION_ID / _RESOURCE_GROUP),
/// else the lines of <c>.secrets/AZURE</c> (or <c>.secrets/Azure</c>) in that order. Never the AZURE_* variables or <c>az login</c>: the credential chain tests need
/// the machine clean. The values are never written to any output.
/// </summary>
public static class RealAzure
{
    private sealed record Settings(
        string BlobAccount, string BlobKey, string AdlsAccount, string AdlsKey, string Container,
        string? TenantId, string? ClientId, string? ClientSecret, string? SubscriptionId, string? ResourceGroup);

    private static readonly Lazy<Settings?> s_settings = new(Load);

    public static bool Available => s_settings.Value is not null;

    /// <summary>A service principal (tenant, client id, client secret) is configured too.</summary>
    public static bool HasServicePrincipal => s_settings.Value is { TenantId: not null, ClientId: not null, ClientSecret: not null };

    public static string Container => s_settings.Value!.Container;

    /// <summary>The subscription and resource group the service principal may deploy into (lines 9 and 10), for the VM test.</summary>
    public static (string Subscription, string Group)? Deployment =>
        s_settings.Value is { SubscriptionId: { } subscription, ResourceGroup: { } group } && HasServicePrincipal ? (subscription, group) : null;

    /// <summary>The tenant, client id and secret of the service principal.</summary>
    public static (string Tenant, string Client, string Secret) ServicePrincipal =>
        (s_settings.Value!.TenantId!, s_settings.Value.ClientId!, s_settings.Value.ClientSecret!);

    public static string Account(AzureAccountKind kind) => kind == AzureAccountKind.Blob ? s_settings.Value!.BlobAccount : s_settings.Value!.AdlsAccount;

    private static string AccountKey(AzureAccountKind kind) => kind == AzureAccountKind.Blob ? s_settings.Value!.BlobKey : s_settings.Value!.AdlsKey;

    /// <summary>The prefix of this test run; everything below it is deleted when the run ends.</summary>
    public static string RunPrefix { get; } = $"seseragi-tests/{DateTime.UtcNow:yyyyMMdd-HHmmss}-{Guid.NewGuid().ToString("N")[..8]}";

    /// <summary>The URL of a path of the container (no trailing slash): az:// for the flat account, abfss:// for ADLS.</summary>
    public static string Url(AzureAccountKind kind, string path) => kind == AzureAccountKind.Blob
        ? $"az://{Container}/{path}"
        : $"abfss://{Container}@{Account(kind)}.dfs.core.windows.net/{path}";

    /// <summary>A fresh URL below the run prefix.</summary>
    public static string NewUrl(AzureAccountKind kind, string name) => Url(kind, $"{RunPrefix}/{name}_{Guid.NewGuid():N}");

    /// <summary>The options of a sign-in: the account name always, plus that sign-in's keys only.</summary>
    public static Dictionary<string, string> Keys(AzureAccountKind kind, AzureSignIn signIn)
    {
        Settings s = s_settings.Value!;
        var keys = new Dictionary<string, string> { ["azure_storage_account_name"] = Account(kind) };
        switch (signIn)
        {
            case AzureSignIn.AccountKey:
                keys["azure_storage_account_key"] = AccountKey(kind);
                break;
            case AzureSignIn.Sas:
                keys["azure_storage_sas_token"] = ContainerClient(kind)
                    .GenerateSasUri(BlobContainerSasPermissions.All, DateTimeOffset.UtcNow.AddHours(2)).Query.TrimStart('?');
                break;
            case AzureSignIn.ClientSecret:
                keys["azure_storage_tenant_id"] = s.TenantId!;
                keys["azure_storage_client_id"] = s.ClientId!;
                keys["azure_storage_client_secret"] = s.ClientSecret!;
                break;
            case AzureSignIn.BearerToken:
                keys["azure_storage_token"] = BearerToken();
                break;
        }
        return keys;
    }

    public static CloudArgs Args(AzureAccountKind kind, AzureSignIn signIn = AzureSignIn.AccountKey)
    {
        var keys = Keys(kind, signIn);
        return CloudArgs.ForLocation(Url(kind, ""), [.. keys.Keys], [.. keys.Values]);
    }

    /// <summary>The container, through the SDK with the account key (for cleanup and for making directories).</summary>
    public static BlobContainerClient ContainerClient(AzureAccountKind kind) => new(
        new Uri($"https://{Account(kind)}.blob.core.windows.net/{Container}"),
        new StorageSharedKeyCredential(Account(kind), AccountKey(kind)));

    private static string? s_token;

    /// <summary>A storage token of the service principal (client credentials grant), fetched once per run.</summary>
    public static string BearerToken() => s_token ??= Token("https://storage.azure.com/.default");

    /// <summary>A token of the service principal for a scope (client credentials grant).</summary>
    public static string Token(string scope)
    {
        Settings s = s_settings.Value!;
        using var http = new HttpClient();
        using var form = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = s.ClientId!,
            ["client_secret"] = s.ClientSecret!,
            ["scope"] = scope,
        });
        using HttpResponseMessage response = http.PostAsync($"https://login.microsoftonline.com/{s.TenantId}/oauth2/v2.0/token", form).GetAwaiter().GetResult();
        string body = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
        if (!response.IsSuccessStatusCode)
        {
            // The error text names the problem (wrong secret, unknown client) without echoing the secret.
            using var error = System.Text.Json.JsonDocument.Parse(body);
            throw new InvalidOperationException($"Token request: HTTP {(int)response.StatusCode}: {error.RootElement.GetProperty("error_description").GetString()}");
        }
        using var doc = System.Text.Json.JsonDocument.Parse(body);
        return doc.RootElement.GetProperty("access_token").GetString()!;
    }

    private static Settings? Load()
    {
        static string? Env(string name) => Environment.GetEnvironmentVariable("SESERAGI_AZURE_" + name) is { Length: > 0 } v ? v.Trim() : null;
        string?[] values =
        [
            Env("BLOB_ACCOUNT"), Env("BLOB_KEY"), Env("ADLS_ACCOUNT"), Env("ADLS_KEY"), Env("CONTAINER"),
            Env("TENANT_ID"), Env("CLIENT_ID"), Env("CLIENT_SECRET"), Env("SUBSCRIPTION_ID"), Env("RESOURCE_GROUP"),
        ];
        if (values[..5].Any(v => v is null))
        {
            string? file = RealAws.SecretsFile("AZURE") ?? RealAws.SecretsFile("Azure");
            if (file is null) return null;
            string[] lines = [.. File.ReadAllLines(file).Select(l => l.Trim())];
            values = [.. Enumerable.Range(0, 10).Select(i => i < lines.Length && lines[i].Length > 0 ? lines[i] : null)];
            if (values[..5].Any(v => v is null)) return null;
        }
        return new Settings(values[0]!, values[1]!, values[2]!, values[3]!, values[4]!, values[5], values[6], values[7], values[8], values[9]);
    }
}

/// <summary>A fact that runs only where real storage accounts are configured (see <see cref="RealAzure"/>).</summary>
public sealed class AzureFactAttribute : FactAttribute
{
    public AzureFactAttribute()
    {
        if (!RealAzure.Available) Skip = "No real Azure storage accounts: set SESERAGI_AZURE_* or write .secrets/AZURE.";
    }
}

/// <summary>A theory that runs only where real storage accounts are configured.</summary>
public sealed class AzureTheoryAttribute : TheoryAttribute
{
    public AzureTheoryAttribute()
    {
        if (!RealAzure.Available) Skip = "No real Azure storage accounts: set SESERAGI_AZURE_* or write .secrets/AZURE.";
    }
}

/// <summary>A theory that runs only where a service principal is configured too (Microsoft Entra sign-in).</summary>
public sealed class AzureEntraTheoryAttribute : TheoryAttribute
{
    public AzureEntraTheoryAttribute()
    {
        if (!RealAzure.Available || !RealAzure.HasServicePrincipal)
            Skip = "No service principal: set SESERAGI_AZURE_TENANT_ID / _CLIENT_ID / _CLIENT_SECRET or lines 6-8 of .secrets/AZURE.";
    }
}

/// <summary>Deletes everything the run wrote below its prefix, in both accounts (directories too, deepest first).</summary>
public sealed class RealAzureCleanup : IDisposable
{
    public void Dispose()
    {
        if (!RealAzure.Available) return;
        foreach (AzureAccountKind kind in Enum.GetValues<AzureAccountKind>())
        {
            try
            {
                BlobContainerClient container = RealAzure.ContainerClient(kind);
                // On ADLS the listing holds the directories as well; children go before their directory.
                var names = container.GetBlobs(BlobTraits.None, BlobStates.None, RealAzure.RunPrefix + "/", default).Select(b => b.Name)
                    .Append(RealAzure.RunPrefix).Append("seseragi-tests")
                    .OrderByDescending(n => n.Count(c => c == '/')).ThenByDescending(n => n.Length).ToList();
                foreach (string name in names)
                {
                    try { container.GetBlobClient(name).DeleteIfExists(); }
                    catch (Azure.RequestFailedException) { } // a directory still holding another run's files
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"Cleaning up {kind} failed: {e.Message}");
            }
        }
    }
}

/// <summary>
/// Smoke tests on real Azure storage (a flat blob account and an ADLS Gen2 one), run once with the default store
/// (managed) and once with <c>POLARS_NET_OBJECT_STORE=native</c>: the file system contract under every sign-in
/// (account key, SAS, client secret, bearer token), put-if-absent races, directories of a hierarchical namespace, and
/// the Delta kernel end to end.
/// </summary>
[Trait("RealCloud", "Azure")]
public sealed class RealCloudAzureTests : IClassFixture<RealAzureCleanup>
{
    private static string Backend => ObjectStoreFileSystem.ConfiguredBackend == ObjectStoreBackend.Native ? "native" : "managed";

    /// <summary>The file system contract, each of its checks on its own root.</summary>
    private sealed class Contract(AzureAccountKind kind, CloudArgs args) : FileSystemContract
    {
        protected override string NewRoot() => RealAzure.NewUrl(kind, "contract");

        protected override IWritableFileSystem Open(string root)
        {
            var fs = new ObjectStoreFileSystem(root, args);
            Assert.Equal(Backend, fs.Implementation);
            return fs;
        }
    }

    private static void RunContract(AzureAccountKind kind, AzureSignIn signIn)
    {
        var contract = new Contract(kind, RealAzure.Args(kind, signIn));
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
        Assert.True(failures.Count == 0, $"{Backend} {kind} {signIn}: " + string.Join("\n", failures));
    }

    [AzureTheory]
    [InlineData(AzureAccountKind.Blob, AzureSignIn.AccountKey)]
    [InlineData(AzureAccountKind.Adls, AzureSignIn.AccountKey)]
    [InlineData(AzureAccountKind.Blob, AzureSignIn.Sas)]
    [InlineData(AzureAccountKind.Adls, AzureSignIn.Sas)]
    public void TheFileSystemContract_Holds(AzureAccountKind kind, AzureSignIn signIn) => RunContract(kind, signIn);

    [AzureEntraTheory]
    [InlineData(AzureAccountKind.Blob, AzureSignIn.ClientSecret)]
    [InlineData(AzureAccountKind.Adls, AzureSignIn.ClientSecret)]
    [InlineData(AzureAccountKind.Blob, AzureSignIn.BearerToken)]
    [InlineData(AzureAccountKind.Adls, AzureSignIn.BearerToken)]
    public void TheFileSystemContract_Holds_SignedInWithEntra(AzureAccountKind kind, AzureSignIn signIn) => RunContract(kind, signIn);

    [AzureTheory]
    [InlineData(AzureAccountKind.Blob)]
    [InlineData(AzureAccountKind.Adls)]
    public void PutIfAbsent_HasOneWinner_AndCommitsConflict(AzureAccountKind kind) =>
        KernelObjectStoreCheck.PutIfAbsentHolds(RealAzure.NewUrl(kind, "race"), RealAzure.Args(kind));

    [AzureTheory]
    [InlineData(AzureAccountKind.Blob)]
    [InlineData(AzureAccountKind.Adls)]
    public void StreamedWrites_SplitFiles_AndReadFootersFromTheStore(AzureAccountKind kind) =>
        KernelObjectStoreCheck.StreamedWritesWork(RealAzure.NewUrl(kind, "streamed"), RealAzure.Args(kind));

    [AzureTheory]
    [InlineData(AzureAccountKind.Blob)]
    [InlineData(AzureAccountKind.Adls)]
    public void Vacuum_DeletesExactlyWhatTheTableNoLongerNeeds(AzureAccountKind kind) =>
        KernelObjectStoreCheck.VacuumWorks(RealAzure.NewUrl(kind, "vacuum"), RealAzure.Args(kind));

    /// <summary>
    /// A hierarchical namespace has real directories: they are not files to list, read or size, whether the kernel's
    /// own writes made them or someone made one empty.
    /// </summary>
    [AzureFact]
    public void Directories_OfAHierarchicalNamespace_AreNotFiles()
    {
        const AzureAccountKind kind = AzureAccountKind.Adls;
        string name = $"directories_{Guid.NewGuid():N}";
        string url = RealAzure.Url(kind, $"{RealAzure.RunPrefix}/{name}");
        CloudArgs args = RealAzure.Args(kind);
        var options = new CloudOptions { Credentials = RealAzure.Keys(kind, AzureSignIn.AccountKey) };

        // An empty directory, made as Azure tools make one: a blob marked hdi_isfolder.
        BlobContainerClient container = RealAzure.ContainerClient(kind);
        container.GetBlobClient($"{RealAzure.RunPrefix}/{name}/empty").Upload(BinaryData.FromBytes([]),
            new BlobUploadOptions { Metadata = new Dictionary<string, string> { ["hdi_isfolder"] = "true" } });
        Assert.Contains(container.GetBlobs(BlobTraits.Metadata, BlobStates.None, $"{RealAzure.RunPrefix}/{name}/", default),
            b => b.Name.EndsWith("/empty", StringComparison.Ordinal));

        using (var rows = DataFrame.FromColumns(new { Id = new[] { 1, 2, 3 }, Region = new[] { "a", "b", "a" } }))
            rows.WriteDelta(url, partitionBy: "Region", mode: DeltaSaveMode.ErrorIfExists, cloudOptions: options);

        using var fs = new ObjectStoreFileSystem(url, args);
        Assert.Equal(Backend, fs.Implementation);
        var files = fs.ListAll("").Select(f => f.Path).ToList();
        // The log and one data file per partition; no "_delta_log", "Region=a", "Region=b" or "empty" entries.
        Assert.Equal(3, files.Count);
        Assert.Contains(files, f => f.StartsWith("_delta_log/", StringComparison.Ordinal) && f.EndsWith(".json", StringComparison.Ordinal));
        Assert.Equal(2, files.Count(f => f.StartsWith("Region=", StringComparison.Ordinal) && f.EndsWith(".parquet", StringComparison.Ordinal)));
        Assert.DoesNotContain(fs.List(""), f => f.Path is "empty" or "_delta_log" or "Region=a" or "Region=b");
        string[] directories = ["empty", "_delta_log", "Region=a"];
        foreach (string directory in directories)
        {
            Assert.Null(fs.ReadAll(directory));
            Assert.Null(fs.ReadRange(directory, 0, 1));
        }

        using (var table = DataFrame.ReadDelta(url, cloudOptions: options))
            Assert.Equal([1, 2, 3], table.Sort(["Id"])["Id"].ToArray<int>());
    }

    [AzureTheory]
    [InlineData(AzureAccountKind.Blob, AzureSignIn.AccountKey)]
    [InlineData(AzureAccountKind.Adls, AzureSignIn.AccountKey)]
    public void ATable_WithIdentityAndRowTracking_LivesThroughEveryOperation(AzureAccountKind kind, AzureSignIn signIn) =>
        TableLifecycle(kind, signIn);

    [AzureEntraTheory]
    [InlineData(AzureAccountKind.Adls, AzureSignIn.ClientSecret)]
    public void ATable_WithIdentityAndRowTracking_LivesThroughEveryOperation_SignedInWithEntra(AzureAccountKind kind, AzureSignIn signIn) =>
        TableLifecycle(kind, signIn);

    /// <summary>A table created, written, deleted from, updated, merged into and optimized through the public API.</summary>
    private static void TableLifecycle(AzureAccountKind kind, AzureSignIn signIn)
    {
        string url = RealAzure.NewUrl(kind, "lifecycle");
        var keys = RealAzure.Keys(kind, signIn);
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
        // Rows that lived through the delete, update, merge and optimize kept their ids; the inserted one has a new id.
        var now = RowIds();
        Assert.All([1L, 3L, 4L], id => Assert.Equal(written[id], now[id]));
        Assert.DoesNotContain(now[5], written.Values);

        using (var v1 = DataFrame.ReadDelta(url, version: 1, cloudOptions: options))
            Assert.Equal(4, v1.Height);
        using (var history = Delta.History(url, cloudOptions: options))
            Assert.Equal(["CREATE TABLE", "WRITE", "DELETE", "UPDATE", "MERGE"], history["operation"].ToArray<string>().Reverse());
        using (var changes = Delta.ReadChangeDataFeed(url, startVersion: 1, endVersion: 4, cloudOptions: options).Collect())
            Assert.Contains("delete", changes["_change_type"].ToArray<string>());
    }

    /// <summary>A SAS that may only read and list: reads work, writes are refused.</summary>
    [AzureFact]
    public void AReadOnlySas_ReadsButCannotWrite()
    {
        const AzureAccountKind kind = AzureAccountKind.Blob;
        string url = RealAzure.NewUrl(kind, "readonly");
        using (var fs = new ObjectStoreFileSystem(url, RealAzure.Args(kind)))
            Assert.True(fs.TryCreate("probe.json", "{}"u8.ToArray()));

        string sas = RealAzure.ContainerClient(kind)
            .GenerateSasUri(BlobContainerSasPermissions.Read | BlobContainerSasPermissions.List, DateTimeOffset.UtcNow.AddHours(1)).Query.TrimStart('?');
        var keys = new Dictionary<string, string> { ["azure_storage_account_name"] = RealAzure.Account(kind), ["azure_storage_sas_token"] = sas };
        using var readOnly = new ObjectStoreFileSystem(url, CloudArgs.ForLocation(url, [.. keys.Keys], [.. keys.Values]));
        Assert.Equal(Backend, readOnly.Implementation);
        Assert.Equal("{}", System.Text.Encoding.UTF8.GetString(readOnly.ReadAll("probe.json")!));
        Assert.Single(readOnly.List(""));
        Assert.ThrowsAny<Exception>(() => readOnly.TryCreate("other.json", "{}"u8.ToArray()));
    }
}
