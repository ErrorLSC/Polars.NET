using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Storage.Http;

namespace Kaminari.Seseragi.Lakehouse.Storage.Azure;

/// <summary>
/// OAuth tokens for Azure Storage as the object_store library fetches them: Microsoft Entra ID with a client secret
/// or a federated token (workload identity), the managed identity endpoint, or the Azure CLI. Tokens are fetched
/// when first needed, never when a store is opened.
/// </summary>
internal static class AzureCredentialSources
{
    /// <summary>The scope of storage tokens.</summary>
    internal const string StorageScope = "https://storage.azure.com/.default";

    /// <summary>The Entra ID host of the public cloud.</summary>
    internal const string PublicAuthorityHost = "https://login.microsoftonline.com";

    /// <summary>The managed identity endpoint of Azure VMs.</summary>
    internal const string DefaultMsiEndpoint = "http://169.254.169.254/metadata/identity/oauth2/token";

    /// <summary>The token endpoint of a tenant.</summary>
    internal static string TokenUrl(string? authorityHost, string tenantId) =>
        $"{authorityHost ?? PublicAuthorityHost}/{tenantId}/oauth2/v2.0/token";

    /// <summary>A service principal with a client secret (client credentials grant).</summary>
    public static CachedCredential<string> ClientSecret(string tokenUrl, string clientId, string clientSecret, RetrySettings retry, Func<DateTimeOffset>? clock = null) =>
        new(() => OAuth(tokenUrl, [new("client_id", clientId), new("client_secret", clientSecret)], retry, clock), clock: clock)
        {
            Origin = $"client secret of {clientId} at {tokenUrl}",
        };

    /// <summary>Workload identity: the federated token the file holds (read for every fetch) as the client assertion.</summary>
    public static CachedCredential<string> WorkloadIdentity(string tokenUrl, string clientId, string tokenFile, RetrySettings retry, Func<DateTimeOffset>? clock = null) =>
        new(() =>
        {
            string assertion;
            try
            {
                assertion = File.ReadAllText(tokenFile);
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException)
            {
                throw new StorageException($"Cannot read the federated token file '{tokenFile}': {e.Message}", inner: e);
            }
            return OAuth(tokenUrl,
            [
                new("client_id", clientId),
                new("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"),
                new("client_assertion", assertion),
            ], retry, clock);
        }, clock: clock)
        {
            Origin = $"workload identity of {clientId} at {tokenUrl}",
        };

    private static Temporary<string> OAuth(
        string tokenUrl, List<KeyValuePair<string, string>> form, RetrySettings retry, Func<DateTimeOffset>? clock)
    {
        form.Add(new("scope", StorageScope));
        form.Add(new("grant_type", "client_credentials"));
        StoreResponse response = HttpTransport.Send(() =>
        {
            var request = new HttpRequestMessage(HttpMethod.Post, tokenUrl) { Content = new FormUrlEncodedContent(form) };
            request.Headers.TryAddWithoutValidation("Accept", "application/json");
            return request;
        }, retry);
        if (!response.IsSuccess) throw Failure(response, "get a token from Microsoft Entra ID");
        DateTimeOffset now = clock?.Invoke() ?? DateTimeOffset.UtcNow;
        return Parse(response.Body, "Microsoft Entra ID", root =>
            (root.GetProperty("access_token").GetString()!, now.AddSeconds(Number(root.GetProperty("expires_in")))));
    }

    /// <summary>
    /// A managed identity: the endpoint of Azure VMs (or App Service's, with its identity header). An explicit
    /// identity is chosen by client id, object id or resource id (the last one given wins).
    /// </summary>
    public static CachedCredential<string> ManagedIdentity(
        string endpoint, string? clientId, string? objectId, string? resourceId, string? identityHeader, RetrySettings retry,
        Func<DateTimeOffset>? clock = null)
    {
        var query = new List<KeyValuePair<string, string>> { new("api-version", "2019-08-01"), new("resource", "https://storage.azure.com") };
        KeyValuePair<string, string>? identity = null;
        if (clientId is not null) identity = new("client_id", clientId);
        if (objectId is not null) identity = new("object_id", objectId);
        if (resourceId is not null) identity = new("msi_res_id", resourceId);
        if (identity is { } chosen) query.Add(chosen);
        string url = endpoint + (endpoint.Contains('?') ? "&" : "?") + string.Join('&', query.Select(p =>
            PercentEncoding.Encode(p.Key, encodeSlash: true) + "=" + PercentEncoding.Encode(p.Value, encodeSlash: true)));

        return new(() =>
        {
            StoreResponse response;
            try
            {
                response = HttpTransport.Send(() =>
                {
                    var request = new HttpRequestMessage(HttpMethod.Get, url);
                    request.Headers.TryAddWithoutValidation("metadata", "true");
                    if (identityHeader is not null) request.Headers.TryAddWithoutValidation("x-identity-header", identityHeader);
                    return request;
                }, retry, metadata: true);
            }
            catch (StorageException e) when (e.Status is null)
            {
                throw new StorageException(
                    $"No Azure credentials are configured, and the managed identity endpoint {endpoint} cannot be reached " +
                    $"(not on Azure?): {e.Message}", inner: e);
            }
            if (!response.IsSuccess) throw Failure(response, "get a token for the managed identity");
            return Parse(response.Body, "the managed identity endpoint", root =>
                (root.GetProperty("access_token").GetString()!, DateTimeOffset.FromUnixTimeSeconds(Number(root.GetProperty("expires_on")))));
        }, clock: clock)
        {
            Origin = $"managed identity at {endpoint}" + (identity is { } id ? $" ({id.Key})" : ""),
        };
    }

    /// <summary>The Azure CLI's token (<c>az account get-access-token</c>), its expiry read in local time.</summary>
    /// <param name="program">The <c>az</c> program (tests use a stand-in).</param>
    /// <param name="clock">The current time.</param>
    public static CachedCredential<string> AzureCli(string? program = null, Func<DateTimeOffset>? clock = null) =>
        new(() =>
        {
            var start = new ProcessStartInfo
            {
                FileName = program ?? (OperatingSystem.IsWindows() ? "cmd" : "az"),
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            if (program is null && OperatingSystem.IsWindows())
            {
                start.ArgumentList.Add("/C");
                start.ArgumentList.Add("az");
            }
            foreach (string argument in new[] { "account", "get-access-token", "--output", "json", "--scope", StorageScope })
                start.ArgumentList.Add(argument);

            string output, error;
            int exitCode;
            try
            {
                using Process process = Process.Start(start) ?? throw new StorageException("The Azure CLI could not be started.");
                Task<string> errorText = process.StandardError.ReadToEndAsync();
                output = process.StandardOutput.ReadToEnd();
                error = errorText.GetAwaiter().GetResult();
                process.WaitForExit();
                exitCode = process.ExitCode;
            }
            catch (System.ComponentModel.Win32Exception e)
            {
                throw new StorageException("The Azure CLI is not installed.", inner: e);
            }
            if (exitCode != 0) throw new StorageException($"The Azure CLI failed: {error.Trim()}");

            return Parse(System.Text.Encoding.UTF8.GetBytes(output), "the Azure CLI", root =>
            {
                string type = root.GetProperty("tokenType").GetString() ?? "";
                if (!type.Equals("bearer", StringComparison.OrdinalIgnoreCase))
                    throw new StorageException($"The Azure CLI gave a token of type '{type}', not a bearer token.");
                DateTime local = DateTime.ParseExact(root.GetProperty("expiresOn").GetString()!, "yyyy-MM-dd HH:mm:ss.ffffff",
                    CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal);
                return (root.GetProperty("accessToken").GetString()!, new DateTimeOffset(local));
            });
        }, clock: clock)
        {
            Origin = "Azure CLI",
        };

    private static long Number(JsonElement value) =>
        value.ValueKind == JsonValueKind.String ? long.Parse(value.GetString()!, CultureInfo.InvariantCulture) : value.GetInt64();

    private static Temporary<string> Parse(byte[] body, string source, Func<JsonElement, (string Token, DateTimeOffset Expires)> read)
    {
        try
        {
            using var document = JsonDocument.Parse(body);
            var (token, expires) = read(document.RootElement);
            return new Temporary<string>(token, expires);
        }
        catch (Exception e) when (e is JsonException or KeyNotFoundException or InvalidOperationException or FormatException)
        {
            throw new StorageException($"The answer of {source} is not a valid token.", inner: e);
        }
    }

    private static StorageException Failure(StoreResponse response, string what)
    {
        string? detail = null;
        try
        {
            using var document = JsonDocument.Parse(response.Body);
            if (document.RootElement.ValueKind == JsonValueKind.Object
                && document.RootElement.TryGetProperty("error_description", out JsonElement description)
                && description.ValueKind == JsonValueKind.String)
                detail = description.GetString()?.Split('\n')[0].Trim();
        }
        catch (JsonException)
        {
            // No JSON error; the status says enough.
        }
        return new StorageException($"Failed to {what}: HTTP {(int)response.Status}" + (detail is null ? "." : $": {detail}"), response.Status);
    }
}
