using System.Text.Json;

namespace Kaminari.Seseragi.Lakehouse.Storage.Gcs;

/// <summary>
/// The Google Cloud Storage settings of a set of configuration keys, read as the widely used Rust
/// <c>object_store</c> reads them (same key names and aliases; <c>SERVICE_ACCOUNT</c>, then <c>GOOGLE_*</c>
/// environment variables below explicit keys; a service account first, then application default credentials).
/// </summary>
/// <param name="BaseUrl">The storage service, without a trailing slash.</param>
/// <param name="Token">The token source; null sends requests without a token.</param>
/// <param name="SkipSignature">Whether reads (and listings) go without a token.</param>
internal sealed record GcsSettings(string BaseUrl, GcsTokenSource? Token, bool SkipSignature = false)
{
    internal const string DefaultBaseUrl = "https://storage.googleapis.com";

    /// <summary>A static OAuth token, as catalogs vend them (handled by the engine's native layer, not the library).</summary>
    internal const string OAuthTokenKey = "google_oauth_token";

    /// <summary>Where gcloud keeps application default credentials, below the home directory.</summary>
    internal static readonly string WellKnownCredentials = Path.Combine(".config", "gcloud", "application_default_credentials.json");

    private static readonly Dictionary<string, string> Aliases = BuildAliases();

    private static readonly HashSet<string> Ignored = new(ConfigValues.HarmlessClientKeys) { "bucket" };

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
    internal static GcsSettings? Resolve(
        IReadOnlyDictionary<string, string> options, IReadOnlyDictionary<string, string> environment,
        Func<string, string?> readFile, string? wellKnownCredentials, out string? reason)
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
                "allow_invalid_certificates" => ConfigValues.IsFalse(value),
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
                if (type != "service_account")
                {
                    reason = $"application default credentials of type '{type}' are not supported by the managed GCS client yet";
                    return null;
                }
                if ((account = ServiceAccountKey.Parse(adc, out reason)) is null) return null;
                // The engine takes the emulator fields (base URL, no OAuth) from a named service account only.
                account = account with { BaseUrl = null, DisableOAuth = false };
            }
        }

        bool skipSignature = ConfigValues.IsTrue(config.GetValueOrDefault("skip_signature"));
        if (staticToken is not null)
        {
            reason = null;
            string tokenBaseUrl = config.GetValueOrDefault("base_url") ?? account?.BaseUrl ?? DefaultBaseUrl;
            return new GcsSettings(tokenBaseUrl.TrimEnd('/'), new StaticToken(staticToken), skipSignature);
        }
        if (account is null && !skipSignature)
        {
            reason = "no service account is given (the metadata server is not supported by the managed GCS client yet)";
            return null;
        }

        ServiceAccountToken? token = null;
        // Unsigned requests are reads only: as in the engine's object store, uploads and deletes keep the token.
        if (account is not null && !account.DisableOAuth)
        {
            token = ServiceAccountToken.Create(account, out reason);
            if (token is null) return null;
        }
        string baseUrl = config.GetValueOrDefault("base_url") ?? account?.BaseUrl ?? DefaultBaseUrl;
        reason = null;
        return new GcsSettings(baseUrl.TrimEnd('/'), token, skipSignature);
    }

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
