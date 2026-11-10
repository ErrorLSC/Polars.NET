namespace Kaminari.Seseragi.Catalog.Unity;

/// <summary>Which cloud the temporary credentials belong to.</summary>
public enum CredentialKind
{
    /// <summary>AWS access key, secret key and optional session token.</summary>
    Aws,
    /// <summary>Azure user delegation SAS token.</summary>
    Azure,
    /// <summary>Google Cloud OAuth token.</summary>
    Gcp,
}

/// <summary>
/// Short-lived storage credentials the catalog vends for one table.
/// They are fetched once and do not renew themselves: work that outlives <see cref="ExpiresAt"/> must ask again.
/// </summary>
public sealed class TemporaryCredentials
{
    /// <summary>The cloud these credentials are for.</summary>
    public required CredentialKind Kind { get; init; }

    /// <summary>AWS access key id.</summary>
    public string? AwsAccessKeyId { get; init; }
    /// <summary>AWS secret access key.</summary>
    public string? AwsSecretAccessKey { get; init; }
    /// <summary>AWS session token, when the service sends one.</summary>
    public string? AwsSessionToken { get; init; }
    /// <summary>Azure SAS token.</summary>
    public string? AzureSasToken { get; init; }
    /// <summary>Google Cloud OAuth token.</summary>
    public string? GcpOauthToken { get; init; }

    /// <summary>When the credentials stop working, if the service says so.</summary>
    public DateTimeOffset? ExpiresAt { get; init; }

    /// <summary>
    /// The credentials as object-store configuration keys
    /// (<c>aws_access_key_id</c>, <c>azure_storage_sas_token</c>, <c>google_oauth_token</c>, ...).
    /// </summary>
    public Dictionary<string, string> ToStorageOptions()
    {
        var options = new Dictionary<string, string>(StringComparer.Ordinal);
        switch (Kind)
        {
            case CredentialKind.Aws:
                options["aws_access_key_id"] = AwsAccessKeyId!;
                options["aws_secret_access_key"] = AwsSecretAccessKey!;
                if (AwsSessionToken is not null) options["aws_session_token"] = AwsSessionToken;
                break;
            case CredentialKind.Azure:
                options["azure_storage_sas_token"] = AzureSasToken!;
                break;
            case CredentialKind.Gcp:
                options["google_oauth_token"] = GcpOauthToken!;
                break;
        }
        return options;
    }
}

/// <summary>A table's location together with credentials to reach it.</summary>
public sealed class TableAccess
{
    /// <summary>The table as the catalog describes it.</summary>
    public required TableInfo Table { get; init; }
    /// <summary>The storage location of the table.</summary>
    public required string Location { get; init; }
    /// <summary>The temporary credentials vended for the table.</summary>
    public required TemporaryCredentials Credentials { get; init; }

    /// <summary>
    /// Storage options for the table: <paramref name="baseOptions"/> with the vended credentials laid over them.
    /// A vended key replaces a base key of exactly the same spelling.
    /// </summary>
    public Dictionary<string, string> MergeStorageOptions(IEnumerable<KeyValuePair<string, string>>? baseOptions = null)
    {
        var merged = new Dictionary<string, string>(StringComparer.Ordinal);
        if (baseOptions is not null)
        {
            foreach (var (key, value) in baseOptions) merged[key] = value;
        }
        foreach (var (key, value) in Credentials.ToStorageOptions()) merged[key] = value;
        return merged;
    }
}
