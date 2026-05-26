namespace Polars.CSharp;

/// <summary>
/// Configuration options for Cloud IO (S3, Azure, GCP, etc).
/// </summary>
public sealed class CloudOptions
{
    /// <summary>
    /// The cloud provider to use.
    /// </summary>
    public CloudProvider Provider { get; set; } = CloudProvider.None;

    /// <summary>
    /// Maximum number of retries for cloud requests. Default is 2.
    /// </summary>
    public uint MaxRetries { get; set; } = 2;

    /// <summary>
    /// The timeout for a single cloud request in milliseconds.
    /// <br/>Default is <b>30,000ms (30 seconds)</b>.
    /// </summary>
    public ulong RetryTimeoutMs { get; set; } = 30_000;

    /// <summary>
    /// The initial backoff time for retries in milliseconds.
    /// <br/>Default is <b>500ms</b>.
    /// </summary>
    public ulong RetryInitBackoffMs { get; set; } = 500;

    /// <summary>
    /// The maximum backoff time for retries in milliseconds.
    /// <br/>Default is <b>10,000ms (10 seconds)</b>.
    /// </summary>
    public ulong RetryMaxBackoffMs { get; set; } = 10_000;

    /// <summary>
    /// Time-to-live for the file cache in seconds. Default is 0 (no cache/default).
    /// </summary>
    public ulong FileCacheTtl { get; set; } = 0;

    /// <summary>
    /// Key-value pairs for authentication and configuration (e.g., "aws_access_key_id", "endpoint_url").
    /// <para>
    /// See Polars documentation for supported keys for each provider.
    /// </para>
    /// </summary>
    public Dictionary<string, string>? Credentials { get; set; } = [];

    // ==========================================
    // AWS S3
    // ==========================================

    /// <summary>
    /// Creates options for Amazon S3 or S3-compatible services (like MinIO).
    /// </summary>
    /// <param name="region">The AWS region (e.g., "us-east-1").</param>
    /// <param name="accessKey">AWS Access Key ID.</param>
    /// <param name="secretKey">AWS Secret Access Key.</param>
    /// <param name="endpoint">Custom endpoint URL (required for MinIO/R2 etc).</param>
    /// <param name="bucket">Optional bucket name (usually inferred from path).</param>
    /// <param name="sessionToken">Optional session token for temporary credentials.</param>
    public static CloudOptions Aws(
        string region, 
        string? accessKey = null, 
        string? secretKey = null, 
        string? endpoint = null,
        string? bucket = null,
        string? sessionToken = null)
    {
        var creds = new Dictionary<string, string>
        {
            { "aws_region", region }
        };

        if (accessKey != null) creds["aws_access_key_id"] = accessKey;
        if (secretKey != null) creds["aws_secret_access_key"] = secretKey;
        if (endpoint != null) creds["aws_endpoint_url"] = endpoint;
        if (bucket != null) creds["aws_bucket"] = bucket;
        if (sessionToken != null) creds["aws_session_token"] = sessionToken;

        return new CloudOptions
        {
            Provider = CloudProvider.Aws,
            Credentials = creds
        };
    }

    // ==========================================
    // Azure
    // ==========================================

    /// <summary>
    /// Creates options for Azure Blob Storage or Azure Data Lake Gen2 (ADLS).
    /// </summary>
    /// <param name="accountName">The storage account name.</param>
    /// <param name="accessKey">The storage account access key (Shared Key).</param>
    /// <param name="sasToken">Shared Access Signature (SAS) token. (Mutually exclusive with accessKey).</param>
    /// <param name="endpoint">Custom endpoint URL.</param>
    public static CloudOptions Azure(
        string accountName, 
        string? accessKey = null, 
        string? sasToken = null,
        string? endpoint = null)
    {
        var creds = new Dictionary<string, string>
        {
            { "azure_storage_account_name", accountName }
        };

        if (accessKey != null) creds["azure_storage_access_key"] = accessKey;
        if (sasToken != null) creds["azure_storage_sas_token"] = sasToken;
        if (endpoint != null) creds["azure_storage_endpoint"] = endpoint;

        return new CloudOptions
        {
            Provider = CloudProvider.Azure,
            Credentials = creds
        };
    }

    // ==========================================
    // Google Cloud Platform (GCP)
    // ==========================================

    /// <summary>
    /// Creates options for Google Cloud Storage (GCS).
    /// </summary>
    /// <param name="serviceAccountPath">Path to the Service Account JSON file. If null, uses default authentication chain.</param>
    public static CloudOptions Gcp(string? serviceAccountPath = null)
    {
        var creds = new Dictionary<string, string>();

        if (serviceAccountPath != null)
        {
            // Can be a path or the actual JSON content
            creds["google_service_account"] = serviceAccountPath;
        }

        return new CloudOptions
        {
            Provider = CloudProvider.Gcp,
            Credentials = creds
        };
    }

    // ==========================================
    // HTTP / HTTPS
    // ==========================================

    /// <summary>
    /// Creates options for generic HTTP/HTTPS sources.
    /// </summary>
    /// <param name="headers">Custom headers (e.g. Authorization) to include in requests.</param>
    public static CloudOptions Http(Dictionary<string, string> headers)
    {
        return new CloudOptions
        {
            Provider = CloudProvider.Http,
            Credentials = headers
        };
    }

    /// <summary>
    /// Creates options for generic HTTP/HTTPS with a Bearer token.
    /// </summary>
    /// <param name="bearerToken">The bearer token string.</param>
    public static CloudOptions Http(string bearerToken)
    {
        return new CloudOptions
        {
            Provider = CloudProvider.Http,
            Credentials = new Dictionary<string, string>
            {
                { "Authorization", $"Bearer {bearerToken}" }
            }
        };
    }

    // ==========================================
    // Hugging Face
    // ==========================================

    /// <summary>
    /// Creates options for Hugging Face datasets (hf://).
    /// </summary>
    /// <param name="token">Hugging Face API token. If null, Polars will look for HF_TOKEN env var.</param>
    public static CloudOptions HuggingFace(string? token = null)
    {
        var creds = new Dictionary<string, string>();
        
        if (token != null)
        {
            creds["token"] = token;
        }

        return new CloudOptions
        {
            Provider = CloudProvider.HuggingFace,
            Credentials = creds
        };
    }

    internal static (
        CloudProvider Provider,
        nuint Retries,
        ulong RetryTimeoutMs,
        ulong RetryInitBackoffMs,
        ulong RetryMaxBackoffMs,
        ulong CacheTtl,
        string[]? Keys,
        string[]? Values
    ) ParseCloudOptions(CloudOptions? options)
    {
        if (options == null)
        {
            return (CloudProvider.None, 0, 0, 0, 0, 0, null, null);
        }

        string[]? keys = null;
        string[]? values = null;

        if (options.Credentials != null && options.Credentials.Count > 0)
        {
            keys = options.Credentials.Keys.ToArray();
            values = options.Credentials.Values.ToArray();
        }

        return (
            options.Provider,
            options.MaxRetries,
            options.RetryTimeoutMs,
            options.RetryInitBackoffMs,
            options.RetryMaxBackoffMs,
            options.FileCacheTtl,
            keys,
            values
        );
    }
}
