using System.Text.RegularExpressions;

namespace Kaminari.Seseragi.Lakehouse.Storage.S3;

/// <summary>
/// The S3 settings of a set of configuration keys, read as the widely used Rust <c>object_store</c> reads them (same
/// key names and aliases, case-insensitive, <c>AWS_*</c> environment variables below explicit keys), so one set of
/// keys means the same here as in an engine built on that library. Settings this client does not implement leave
/// the location to another implementation (see <see cref="Resolve"/>).
/// </summary>
internal sealed record S3Settings(string BucketEndpoint, string Region, AwsCredential? Credential)
{
    /// <summary>Canonical names by every spelling object_store accepts (after lower-casing).</summary>
    private static readonly Dictionary<string, string> Aliases = BuildAliases();

    // Keys that change nothing for static credentials and plain requests.
    private static readonly HashSet<string> Ignored =
    [
        "bucket", "imdsv1_fallback", "metadata_endpoint", "unsigned_payload", "container_credentials_relative_uri",
        "container_credentials_full_uri", "container_authorization_token_file", "web_identity_token_file", "role_arn",
        "role_session_name", "sts_endpoint", "copy_if_not_exists", "disable_tagging", "allow_http", "user_agent",
        "timeout", "connect_timeout", "default_content_type", "http1_only", "http2_only", "http2_keep_alive_interval",
        "http2_keep_alive_timeout", "http2_keep_alive_while_idle", "http2_max_frame_size", "pool_idle_timeout",
        "pool_max_idle_per_host", "randomize_addresses",
    ];

    private static Dictionary<string, string> BuildAliases()
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        void Add(string canonical, params string[] names)
        {
            foreach (string name in names) map[name] = canonical;
        }
        Add("access_key_id", "aws_access_key_id", "access_key_id");
        Add("secret_access_key", "aws_secret_access_key", "secret_access_key");
        Add("default_region", "aws_default_region", "default_region");
        Add("region", "aws_region", "region");
        Add("bucket", "aws_bucket", "aws_bucket_name", "bucket_name", "bucket");
        Add("endpoint", "aws_endpoint_url", "aws_endpoint", "endpoint_url", "endpoint");
        Add("s3_endpoint", "aws_endpoint_url_s3");
        Add("token", "aws_session_token", "aws_token", "session_token", "token");
        Add("virtual_hosted_style_request", "aws_virtual_hosted_style_request", "virtual_hosted_style_request");
        Add("s3_express", "aws_s3_express", "s3_express");
        Add("imdsv1_fallback", "aws_imdsv1_fallback", "imdsv1_fallback");
        Add("metadata_endpoint", "aws_metadata_endpoint", "metadata_endpoint");
        Add("unsigned_payload", "aws_unsigned_payload", "unsigned_payload");
        Add("checksum", "aws_checksum_algorithm", "checksum_algorithm");
        Add("container_credentials_relative_uri", "aws_container_credentials_relative_uri", "container_credentials_relative_uri");
        Add("container_credentials_full_uri", "aws_container_credentials_full_uri", "container_credentials_full_uri");
        Add("container_authorization_token_file", "aws_container_authorization_token_file", "container_authorization_token_file");
        Add("web_identity_token_file", "aws_web_identity_token_file", "web_identity_token_file");
        Add("role_arn", "aws_role_arn", "role_arn");
        Add("role_session_name", "aws_role_session_name", "role_session_name");
        Add("sts_endpoint", "aws_endpoint_url_sts", "endpoint_url_sts");
        Add("skip_signature", "aws_skip_signature", "skip_signature");
        Add("copy_if_not_exists", "aws_copy_if_not_exists", "copy_if_not_exists");
        Add("conditional_put", "aws_conditional_put", "conditional_put");
        Add("disable_tagging", "aws_disable_tagging", "disable_tagging");
        Add("request_payer", "aws_request_payer", "request_payer");
        Add("allow_http", "aws_allow_http");
        Add("sse", "aws_server_side_encryption", "server_side_encryption");
        Add("sse_kms_key_id", "aws_sse_kms_key_id", "sse_kms_key_id");
        Add("sse_bucket_key_enabled", "aws_sse_bucket_key_enabled", "sse_bucket_key_enabled");
        Add("sse_customer_key", "aws_sse_customer_key_base64", "sse_customer_key_base64");
        foreach (string client in new[]
        {
            "allow_http", "allow_invalid_certificates", "connect_timeout", "default_content_type", "http1_only",
            "http2_only", "http2_keep_alive_interval", "http2_keep_alive_timeout", "http2_keep_alive_while_idle",
            "http2_max_frame_size", "pool_idle_timeout", "pool_max_idle_per_host", "proxy_url", "proxy_ca_certificate",
            "proxy_excludes", "randomize_addresses", "timeout", "user_agent",
        })
        {
            map.TryAdd(client, client);
            map.TryAdd("aws_" + client, client);
        }
        return map;
    }

    /// <summary>The canonical S3 key of a configuration key, or null when object_store would ignore it.</summary>
    internal static string? Canonical(string key) => Aliases.GetValueOrDefault(key.ToLowerInvariant());

    /// <summary>
    /// The settings for bucket <paramref name="bucket"/>, or null with the reason when they need something this
    /// client does not implement (credential chains such as instance metadata or web identity, server-side
    /// encryption, requester pays, proxies, ...).
    /// </summary>
    /// <param name="bucket">The bucket.</param>
    /// <param name="options">Explicit configuration keys.</param>
    /// <param name="environment">Environment variables (the process environment in production).</param>
    /// <param name="awsConfigFile">The text of <c>~/.aws/config</c>, if there is one.</param>
    /// <param name="reason">Why the settings cannot be served, when null is returned.</param>
    /// <param name="lookupRegion">
    /// Finds a bucket's region when nothing names one and no endpoint is given (null: the location is not served).
    /// </param>
    internal static S3Settings? Resolve(
        string bucket, IReadOnlyDictionary<string, string> options, IReadOnlyDictionary<string, string> environment,
        string? awsConfigFile, out string? reason, Func<string, string?>? lookupRegion = null)
    {
        // Environment first, explicit keys over it.
        var fromEnvironment = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, value) in environment)
            if (name.StartsWith("AWS_", StringComparison.Ordinal) && Canonical(name) is { } key) fromEnvironment[key] = value;
        var explicitKeys = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (name, value) in options)
            if (Canonical(name) is { } key) explicitKeys[key] = value;
        var config = new Dictionary<string, string>(fromEnvironment, StringComparer.Ordinal);
        foreach (var (key, value) in explicitKeys) config[key] = value;

        foreach (var (key, value) in config)
        {
            if (Ignored.Contains(key) || key is "access_key_id" or "secret_access_key" or "token" or "region"
                or "default_region" or "endpoint" or "s3_endpoint" or "virtual_hosted_style_request" or "skip_signature")
                continue;
            bool harmless = key switch
            {
                "s3_express" or "request_payer" or "allow_invalid_certificates" => IsFalse(value),
                "conditional_put" => value.Equals("etag", StringComparison.OrdinalIgnoreCase),
                _ => false,
            };
            if (!harmless)
            {
                reason = $"the setting '{key}' is not supported by the managed S3 client";
                return null;
            }
        }

        bool skipSignature = config.TryGetValue("skip_signature", out string? skip) && IsTrue(skip);
        AwsCredential? credential = null;
        if (!skipSignature)
        {
            config.TryGetValue("access_key_id", out string? keyId);
            config.TryGetValue("secret_access_key", out string? secret);
            if (string.IsNullOrEmpty(keyId) || string.IsNullOrEmpty(secret))
            {
                reason = "no static credentials (access key and secret) are given";
                return null;
            }
            credential = new AwsCredential(keyId, secret, config.GetValueOrDefault("token") is { Length: > 0 } token ? token : null);
        }

        string? endpoint = config.GetValueOrDefault("s3_endpoint") ?? config.GetValueOrDefault("endpoint");
        // A default region never replaces a region set before it; the config file fills in after the environment.
        string? region = explicitKeys.GetValueOrDefault("region")
            ?? fromEnvironment.GetValueOrDefault("region")
            ?? fromEnvironment.GetValueOrDefault("default_region");
        if (region is null && awsConfigFile is not null)
        {
            Match match = Regex.Match(awsConfigFile, @"region\s*=\s*([^\r\n]*)");
            if (match.Success) region = match.Groups[1].Value;
        }
        region ??= explicitKeys.GetValueOrDefault("default_region");
        if (region is null)
        {
            if (endpoint is null)
            {
                region = lookupRegion?.Invoke(bucket);
                if (region is null)
                {
                    reason = "no region is given and the bucket's region could not be looked up";
                    return null;
                }
            }
            else
            {
                region = "us-east-1";
            }
        }

        bool virtualHosted = config.TryGetValue("virtual_hosted_style_request", out string? hosted) && IsTrue(hosted);
        string bucketEndpoint = (endpoint, virtualHosted) switch
        {
            ({ } e, true) => e,
            ({ } e, false) => e.TrimEnd('/') + "/" + bucket,
            (null, true) => $"https://{bucket}.s3.{region}.amazonaws.com",
            (null, false) => $"https://s3.{region}.amazonaws.com/{bucket}",
        };
        reason = null;
        return new S3Settings(bucketEndpoint, region, credential);
    }

    private static bool IsTrue(string value) => value.Trim().Equals("true", StringComparison.OrdinalIgnoreCase);

    private static bool IsFalse(string value) => value.Trim().Equals("false", StringComparison.OrdinalIgnoreCase);
}
