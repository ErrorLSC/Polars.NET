namespace Kaminari.Seseragi.Lakehouse.Storage.Azure;

/// <summary>How requests to Azure Blob Storage are authorized.</summary>
internal abstract record AzureAuth
{
    /// <summary>The storage account key (SharedKey signatures).</summary>
    public sealed record SharedKey(byte[] Key) : AzureAuth;

    /// <summary>A shared access signature, as query pairs (decoded).</summary>
    public sealed record Sas(IReadOnlyList<KeyValuePair<string, string>> Pairs) : AzureAuth;

    /// <summary>An OAuth bearer token.</summary>
    public sealed record Bearer(CredentialSource<string> Token) : AzureAuth;
}

/// <summary>
/// The Azure settings of a set of configuration keys, read as the widely used Rust <c>object_store</c> reads them
/// (same key names and aliases, <c>AZURE_*</c> environment variables below explicit keys, the same choice among
/// credentials), so one set of keys means the same here as in an engine built on that library.
/// </summary>
/// <param name="ServiceUrl">The blob service, without a trailing slash; the emulator's includes the account.</param>
/// <param name="Account">The storage account.</param>
/// <param name="Auth">The credentials; null sends unsigned requests.</param>
internal sealed record AzureSettings(string ServiceUrl, string Account, AzureAuth? Auth)
{
    /// <summary>The well-known account of Azurite and the storage emulator.</summary>
    internal const string EmulatorAccount = "devstoreaccount1";

    /// <summary>The well-known key of <see cref="EmulatorAccount"/>.</summary>
    internal const string EmulatorKey =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private const string EmulatorUrlVariable = "AZURITE_BLOB_STORAGE_URL";

    private static readonly Dictionary<string, string> Aliases = BuildAliases();

    // Keys that configure credentials (read where the credential is chosen), or change nothing for plain requests.
    private static readonly HashSet<string> Ignored = new(ConfigValues.HarmlessClientKeys)
    {
        "container_name", "disable_tagging", "msi_endpoint", "object_id", "msi_resource_id", "authority_host",
        "client_id", "client_secret", "tenant_id", "federated_token_file", "use_azure_cli", "fabric_token_service_url",
        "fabric_workload_host", "fabric_session_token", "fabric_cluster_identifier",
    };

    private static Dictionary<string, string> BuildAliases()
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        void Add(string canonical, params string[] names)
        {
            foreach (string name in names) map[name] = canonical;
        }
        Add("access_key", "azure_storage_account_key", "azure_storage_access_key", "azure_storage_master_key",
            "master_key", "account_key", "access_key");
        Add("account_name", "azure_storage_account_name", "account_name");
        Add("client_id", "azure_storage_client_id", "azure_client_id", "client_id");
        Add("client_secret", "azure_storage_client_secret", "azure_client_secret", "client_secret");
        Add("tenant_id", "azure_storage_tenant_id", "azure_storage_authority_id", "azure_tenant_id",
            "azure_authority_id", "tenant_id", "authority_id");
        Add("authority_host", "azure_storage_authority_host", "azure_authority_host", "authority_host");
        Add("sas_key", "azure_storage_sas_key", "azure_storage_sas_token", "sas_key", "sas_token");
        Add("token", "azure_storage_token", "bearer_token", "token");
        Add("use_emulator", "azure_storage_use_emulator", "use_emulator");
        Add("endpoint", "azure_storage_endpoint", "azure_endpoint", "endpoint");
        Add("msi_endpoint", "azure_msi_endpoint", "azure_identity_endpoint", "identity_endpoint", "msi_endpoint");
        Add("object_id", "azure_object_id", "object_id");
        Add("msi_resource_id", "azure_msi_resource_id", "msi_resource_id");
        Add("federated_token_file", "azure_federated_token_file", "federated_token_file");
        Add("use_fabric_endpoint", "azure_use_fabric_endpoint", "use_fabric_endpoint");
        Add("use_azure_cli", "azure_use_azure_cli", "use_azure_cli");
        Add("skip_signature", "azure_skip_signature", "skip_signature");
        Add("container_name", "azure_container_name", "container_name");
        Add("disable_tagging", "azure_disable_tagging", "disable_tagging");
        Add("fabric_token_service_url", "azure_fabric_token_service_url", "fabric_token_service_url");
        Add("fabric_workload_host", "azure_fabric_workload_host", "fabric_workload_host");
        Add("fabric_session_token", "azure_fabric_session_token", "fabric_session_token");
        Add("fabric_cluster_identifier", "azure_fabric_cluster_identifier", "fabric_cluster_identifier");
        foreach (string client in ConfigValues.ClientKeys)
        {
            map.TryAdd(client, client);
            map.TryAdd("azure_" + client, client);
        }
        return map;
    }

    /// <summary>The canonical Azure key of a configuration key, or null when object_store would not know it.</summary>
    internal static string? Canonical(string key) => Aliases.GetValueOrDefault(key.ToLowerInvariant());

    /// <summary>
    /// The settings for a container, or null with the reason when they need something this client does not
    /// implement (OAuth flows such as managed identity, workload identity, client secrets or the Azure CLI; Fabric;
    /// proxies).
    /// </summary>
    /// <param name="urlAccount">The account the location names (<c>container@account.dfs.core.windows.net</c>), which wins over the keys.</param>
    /// <param name="options">Explicit configuration keys.</param>
    /// <param name="environment">Environment variables (the process environment in production).</param>
    /// <param name="reason">Why the settings cannot be served, when null is returned.</param>
    /// <param name="retry">Retry settings for fetching tokens.</param>
    internal static AzureSettings? Resolve(
        string? urlAccount, IReadOnlyDictionary<string, string> options, IReadOnlyDictionary<string, string> environment,
        out string? reason, RetrySettings? retry = null)
    {
        var config = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, value) in environment)
            if (name.StartsWith("AZURE_", StringComparison.Ordinal) && Canonical(name) is { } key) config[key] = value;
        // App Service names its managed identity endpoint so (over AZURE_MSI_ENDPOINT, below explicit keys).
        if (environment.GetValueOrDefault("IDENTITY_ENDPOINT") is { } identityEndpoint) config["msi_endpoint"] = identityEndpoint;
        foreach (var (name, value) in options)
            if (Canonical(name) is { } key) config[key] = value;

        foreach (var (key, value) in config)
        {
            bool served = key switch
            {
                "use_emulator" or "skip_signature" or "use_fabric_endpoint" or "use_azure_cli" => ConfigValues.Bool(value) is not null,
                "allow_invalid_certificates" => ConfigValues.IsFalse(value),
                "access_key" or "account_name" or "sas_key" or "token" or "endpoint" => true,
                _ => Ignored.Contains(key),
            };
            if (!served)
            {
                reason = $"the setting '{key}' (with this value) is not supported by the managed Azure client";
                return null;
            }
        }
        if (ConfigValues.IsTrue(config.GetValueOrDefault("use_fabric_endpoint")))
        {
            reason = "Microsoft Fabric endpoints are not supported by the managed Azure client";
            return null;
        }

        reason = null;
        bool skipSignature = ConfigValues.IsTrue(config.GetValueOrDefault("skip_signature"));
        string? account = urlAccount ?? config.GetValueOrDefault("account_name");
        string? accessKey = config.GetValueOrDefault("access_key");
        string? token = config.GetValueOrDefault("token");
        string? sas = config.GetValueOrDefault("sas_key");

        if (ConfigValues.IsTrue(config.GetValueOrDefault("use_emulator")))
        {
            // The emulator ignores the endpoint key: its URL comes from the environment, the account goes into the path.
            account ??= EmulatorAccount;
            string url = environment.GetValueOrDefault(EmulatorUrlVariable) ?? "http://127.0.0.1:10000";
            AzureAuth? emulatorAuth = null;
            if (!skipSignature)
            {
                emulatorAuth = accessKey is not null ? KeyAuth(accessKey, out reason)
                    : token is not null ? new AzureAuth.Bearer(new StaticCredential<string>(token))
                    : sas is not null ? SasAuth(sas, out reason)
                    : KeyAuth(EmulatorKey, out reason);
                if (emulatorAuth is null) return null;
            }
            reason = null;
            return new AzureSettings(url.TrimEnd('/') + "/" + account, account, emulatorAuth);
        }

        if (string.IsNullOrEmpty(account))
        {
            reason = "no storage account is named";
            return null;
        }

        AzureAuth? auth = null;
        if (!skipSignature)
        {
            // object_store's order: the first credential that is fully given wins, whatever its source.
            bool Has(string key) => config.ContainsKey(key);
            string? Value(string key) => config.GetValueOrDefault(key);
            RetrySettings tokenRetry = retry ?? RetrySettings.Default;
            string TokenUrl() => AzureCredentialSources.TokenUrl(Value("authority_host"), Value("tenant_id")!);
            AzureAuth Bearer(string[] parts, Func<CachedCredential<string>> create) =>
                new AzureAuth.Bearer(SharedCredentials<string>.Get(parts, create));

            if (Has("fabric_token_service_url") && Has("fabric_workload_host") && Has("fabric_session_token")
                && Has("fabric_cluster_identifier"))
                reason = "Fabric token credentials are not supported by the managed Azure client";
            else if (token is not null)
                (auth, reason) = (new AzureAuth.Bearer(new StaticCredential<string>(token)), null);
            else if (accessKey is not null)
                auth = KeyAuth(accessKey, out reason);
            else if (Has("client_id") && Has("tenant_id") && Has("federated_token_file"))
            {
                string url = TokenUrl(), client = Value("client_id")!, file = Value("federated_token_file")!;
                auth = Bearer(["workload", url, client, file], () => AzureCredentialSources.WorkloadIdentity(url, client, file, tokenRetry));
            }
            else if (Has("client_id") && Has("client_secret") && Has("tenant_id"))
            {
                string url = TokenUrl(), client = Value("client_id")!, secret = Value("client_secret")!;
                auth = Bearer(["secret", url, client, secret], () => AzureCredentialSources.ClientSecret(url, client, secret, tokenRetry));
            }
            else if (sas is not null)
                auth = SasAuth(sas, out reason);
            else if (ConfigValues.IsTrue(Value("use_azure_cli")))
                auth = Bearer(["cli"], () => AzureCredentialSources.AzureCli());
            else
            {
                string msi = Value("msi_endpoint") ?? AzureCredentialSources.DefaultMsiEndpoint;
                string? header = environment.GetValueOrDefault("IDENTITY_HEADER");
                string?[] identity = [Value("client_id"), Value("object_id"), Value("msi_resource_id")];
                auth = Bearer(["msi", msi, identity[0] ?? "", identity[1] ?? "", identity[2] ?? "", header ?? ""],
                    () => AzureCredentialSources.ManagedIdentity(msi, identity[0], identity[1], identity[2], header, tokenRetry));
            }
            if (auth is null) return null;
        }

        string endpoint = config.GetValueOrDefault("endpoint") ?? $"https://{account}.blob.core.windows.net";
        reason = null;
        return new AzureSettings(endpoint.TrimEnd('/'), account, auth);
    }

    private static AzureAuth? KeyAuth(string key, out string? reason)
    {
        try
        {
            reason = null;
            return new AzureAuth.SharedKey(Convert.FromBase64String(key));
        }
        catch (FormatException)
        {
            reason = "the account key is not base64";
            return null;
        }
    }

    /// <summary>A SAS token as query pairs: the whole token is percent-decoded, then split at <c>&amp;</c> and the first <c>=</c>.</summary>
    internal static AzureAuth? SasAuth(string sas, out string? reason)
    {
        string decoded = Uri.UnescapeDataString(sas);
        var pairs = new List<KeyValuePair<string, string>>();
        foreach (string part in decoded.TrimStart('?').Split('&'))
        {
            if (string.IsNullOrWhiteSpace(part)) continue;
            string trimmed = part.Trim();
            int equals = trimmed.IndexOf('=');
            if (equals < 0)
            {
                reason = "a component of the SAS token has no value";
                return null;
            }
            pairs.Add(new(trimmed[..equals], trimmed[(equals + 1)..]));
        }
        reason = null;
        return new AzureAuth.Sas(pairs);
    }
}
