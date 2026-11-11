using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Storage.Http;

namespace Kaminari.Seseragi.Lakehouse.Storage.Gcs;

/// <summary>
/// The Google Cloud Storage settings of a set of configuration keys, read as the widely used Rust
/// <c>object_store</c> reads them (same key names and aliases; <c>SERVICE_ACCOUNT</c>, then <c>GOOGLE_*</c>
/// environment variables below explicit keys; a service account first, then application default credentials).
/// </summary>
/// <param name="BaseUrl">The storage service, without a trailing slash.</param>
/// <param name="Token">The token source; null sends requests without a token.</param>
/// <param name="SkipSignature">Whether reads (and listings) go without a token.</param>
/// <param name="Http">Connection options (proxy, certificate checks).</param>
internal sealed record GcsSettings(string BaseUrl, CredentialSource<string>? Token, bool SkipSignature = false, HttpOptions? Http = null)
{
    internal const string DefaultBaseUrl = "https://storage.googleapis.com";

    /// <summary>A static OAuth token, as catalogs vend them (handled by the engine's native layer, not the library).</summary>
    internal const string OAuthTokenKey = "google_oauth_token";

    /// <summary>Where gcloud keeps application default credentials, below the home directory.</summary>
    internal static readonly string WellKnownCredentials = Path.Combine(".config", "gcloud", "application_default_credentials.json");

    private static readonly Dictionary<string, string> Aliases = BuildAliases();

    private static readonly HashSet<string> Ignored = [.. ConfigValues.HarmlessClientKeys, "bucket"];

    private static Dictionary<string, string> BuildAliases()
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        void Add(string canonical, params string[] names)
        {
            foreach (string name in names) map[name] = canonical;
        }
        Add("service_account", "google_service_account", "service_account", "google_service_account_path", "service_account_path");
        Add("service_account_key", "google_service_account_key", "service_account_key");
        Add("bucket", "google_bucket", "google_bucket_name", "bucket", "bucket_name");
        Add("base_url", "google_base_url", "base_url");
        Add("application_credentials", "google_application_credentials", "application_credentials");
        Add("skip_signature", "google_skip_signature", "skip_signature");
        foreach (string client in ConfigValues.ClientKeys)
        {
            map.TryAdd(client, client);
            map.TryAdd("google_" + client, client);
        }
        return map;
    }

    /// <summary>The canonical key of a configuration key, or null when object_store would not know it.</summary>
    internal static string? Canonical(string key) => Aliases.GetValueOrDefault(key.ToLowerInvariant());

    /// <summary>
    /// The settings, or null with the reason when they need something this client does not implement (the metadata
    /// server, user credentials from gcloud, proxies, ...).
    /// </summary>
    /// <param name="options">Explicit configuration keys.</param>
    /// <param name="environment">Environment variables (the process environment in production).</param>
    /// <param name="readFile">Reads a file, null when there is none (key files and application default credentials).</param>
    /// <param name="wellKnownCredentials">The path of gcloud's application default credentials, or null.</param>
    /// <param name="reason">Why the settings cannot be served, when null is returned.</param>
    /// <param name="retry">Retry settings for fetching tokens.</param>
    internal static GcsSettings? Resolve(
        IReadOnlyDictionary<string, string> options, IReadOnlyDictionary<string, string> environment,
        Func<string, string?> readFile, string? wellKnownCredentials, out string? reason, RetrySettings? retry = null)
    {
        // A static token (Unity Catalog vends them) is not a key of the object store library: the engine hands it
        // over as a credential provider, and then reads no GOOGLE_* variables and no application default credentials.
        string? staticToken = options.FirstOrDefault(o => o.Key.Equals(OAuthTokenKey, StringComparison.OrdinalIgnoreCase)).Value;
        if (staticToken is not null) environment = new Dictionary<string, string>();

        var config = new Dictionary<string, string>(StringComparer.Ordinal);
        if (environment.GetValueOrDefault("SERVICE_ACCOUNT") is { } path) config["service_account"] = path;
        foreach (var (name, value) in environment)
            if (name.StartsWith("GOOGLE_", StringComparison.Ordinal) && Canonical(name) is { } key) config[key] = value;
        foreach (var (name, value) in options)
            if (Canonical(name) is { } key) config[key] = value;

        foreach (var (key, value) in config)
        {
            bool served = key switch
            {
                "skip_signature" => ConfigValues.Bool(value) is not null,
                _ when HttpOptions.Keys.Contains(key) => true,
                "service_account" or "service_account_key" or "base_url" or "application_credentials" => true,
                _ => Ignored.Contains(key),
            };
            if (!served)
            {
                reason = $"the setting '{key}' (with this value) is not supported by the managed GCS client";
                return null;
            }
        }

        ServiceAccountKey? account = null;
        CredentialSource<string>? userToken = null;
        RetrySettings tokenRetry = retry ?? RetrySettings.Default;
        string? accountPath = config.GetValueOrDefault("service_account");
        string? accountKey = config.GetValueOrDefault("service_account_key");
        if (accountPath is not null && accountKey is not null)
        {
            reason = "both a service account path and a service account key are given";
            return null;
        }
        if (accountPath is not null || accountKey is not null)
        {
            string? json = accountKey ?? readFile(accountPath!);
            if (json is null)
            {
                reason = $"the service account file '{accountPath}' cannot be read";
                return null;
            }
            if ((account = ServiceAccountKey.Parse(json, out reason)) is null) return null;
        }
        else if (staticToken is null)
        {
            // Application default credentials: the named file, else gcloud's.
            string? adcPath = config.GetValueOrDefault("application_credentials") ?? wellKnownCredentials;
            string? adc = adcPath is null ? null : readFile(adcPath);
            if (adc is null && config.ContainsKey("application_credentials"))
            {
                reason = $"the application credentials file '{adcPath}' cannot be read";
                return null;
            }
            if (adc is not null)
            {
                string? type = CredentialType(adc);
                if (type == "authorized_user")
                {
                    if (AuthorizedUser(adc, tokenRetry, ConnectionOptions(config), out reason) is not { } user) return null;
                    userToken = user;
                }
                else if (type != "service_account")
                {
                    reason = $"application default credentials of type '{type}' are not supported";
                    return null;
                }
                else if ((account = ServiceAccountKey.Parse(adc, out reason)) is null) return null;
                else
                {
                    // The engine takes the emulator fields (base URL, no OAuth) from a named service account only.
                    account = account with { BaseUrl = null, DisableOAuth = false };
                }
            }
        }

        bool skipSignature = ConfigValues.IsTrue(config.GetValueOrDefault("skip_signature"));
        if (HttpOptions.From(config, out reason) is not { } http) return null;
        if (staticToken is not null)
        {
            reason = null;
            string tokenBaseUrl = config.GetValueOrDefault("base_url") ?? account?.BaseUrl ?? DefaultBaseUrl;
            return new GcsSettings(tokenBaseUrl.TrimEnd('/'), new StaticCredential<string>(staticToken), skipSignature, http);
        }
        CredentialSource<string>? token = userToken;
        if (account is null && token is null)
        {
            // Nothing configured: the metadata server of a Google Cloud machine (also for unsigned stores, whose
            // uploads and deletes still carry a token in the engine's object store).
            string host = environment.GetValueOrDefault("GCE_METADATA_HOST") ?? environment.GetValueOrDefault("GCE_METADATA_ROOT")
                ?? GcsCredentialSources.DefaultMetadataHost;
            string ip = environment.GetValueOrDefault("GCE_METADATA_IP") ?? GcsCredentialSources.DefaultMetadataIp;
            token = SharedCredentials<string>.Get(["gcp-metadata", host, ip, http.Key], () => GcsCredentialSources.MetadataServer(host, ip, tokenRetry, http: http));
        }

        // Unsigned requests are reads only: as in the engine's object store, uploads and deletes keep the token.
        if (account is not null && !account.DisableOAuth)
        {
            token = ServiceAccountToken.Create(account, out reason);
            if (token is null) return null;
        }
        else if (account is not null)
        {
            token = null;
        }
        string baseUrl = config.GetValueOrDefault("base_url") ?? account?.BaseUrl ?? DefaultBaseUrl;
        reason = null;
        return new GcsSettings(baseUrl.TrimEnd('/'), token, skipSignature, http);
    }

    /// <summary>The token source of gcloud user credentials (client id, client secret, refresh token).</summary>
    private static CredentialSource<string>? AuthorizedUser(string json, RetrySettings retry, HttpOptions http, out string? reason)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            JsonElement root = document.RootElement;
            string? Text(string name) =>
                root.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;
            if (Text("client_id") is not { } id || Text("client_secret") is not { } secret || Text("refresh_token") is not { } refresh)
            {
                reason = "the gcloud user credentials lack client_id, client_secret or refresh_token";
                return null;
            }
            reason = null;
            return SharedCredentials<string>.Get(["gcp-user", id, refresh, http.Key], () => GcsCredentialSources.AuthorizedUser(id, secret, refresh, retry, http: http));
        }
        catch (JsonException)
        {
            reason = "the application default credentials are not JSON";
            return null;
        }
    }

    // The connection options while reading the credentials (invalid ones are refused later, before any request).
    private static HttpOptions ConnectionOptions(Dictionary<string, string> config) => HttpOptions.From(config, out _) ?? HttpOptions.Default;

    private static string? CredentialType(string json)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            return document.RootElement.TryGetProperty("type", out JsonElement type) && type.ValueKind == JsonValueKind.String
                ? type.GetString()
                : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
