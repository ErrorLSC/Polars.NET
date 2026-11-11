using System.Security.Cryptography;

namespace Kaminari.Seseragi.Lakehouse.Storage.S3;

/// <summary>
/// Headers some S3 requests carry, as the object_store library sends them: server-side encryption on uploads (and,
/// with customer-provided keys, on reads), and requester pays on every signed request.
/// </summary>
/// <param name="Put">Headers of uploads (lower-case names).</param>
/// <param name="Read">Headers of reads and properties (customer-provided keys only).</param>
/// <param name="RequesterPays">Whether signed requests say that the requester pays.</param>
internal sealed record S3Headers(IReadOnlyDictionary<string, string> Put, IReadOnlyDictionary<string, string> Read, bool RequesterPays)
{
    public static readonly S3Headers None = new(new Dictionary<string, string>(), new Dictionary<string, string>(), false);

    /// <summary>
    /// The headers of the settings (canonical keys), or null with the reason when they are invalid (as the library's
    /// builder refuses them).
    /// </summary>
    public static S3Headers? From(IReadOnlyDictionary<string, string> config, out string? reason)
    {
        reason = null;
        bool requesterPays = false;
        if (config.TryGetValue("request_payer", out string? payer))
        {
            if (ConfigValues.Bool(payer) is not { } pays)
            {
                reason = "the setting 'request_payer' is not a boolean";
                return null;
            }
            requesterPays = pays;
        }

        var put = new Dictionary<string, string>(StringComparer.Ordinal);
        var read = new Dictionary<string, string>(StringComparer.Ordinal);
        if (config.TryGetValue("sse", out string? type))
        {
            switch (type)
            {
                case "AES256" or "aws:kms" or "aws:kms:dsse":
                    put["x-amz-server-side-encryption"] = type;
                    if (config.TryGetValue("sse_kms_key_id", out string? keyId)) put["x-amz-server-side-encryption-aws-kms-key-id"] = keyId;
                    if (config.TryGetValue("sse_bucket_key_enabled", out string? bucketKey))
                    {
                        if (ConfigValues.Bool(bucketKey) is not { } enabled)
                        {
                            reason = "the setting 'sse_bucket_key_enabled' is not a boolean";
                            return null;
                        }
                        put["x-amz-server-side-encryption-bucket-key-enabled"] = enabled ? "true" : "false";
                    }
                    break;
                case "sse-c":
                    if (config.GetValueOrDefault("sse_customer_key") is not { } key)
                    {
                        reason = "customer-provided encryption (sse-c) needs a customer key";
                        return null;
                    }
                    byte[] decoded;
                    try
                    {
                        decoded = Convert.FromBase64String(key);
                    }
                    catch (FormatException)
                    {
                        reason = "the customer encryption key is not base64";
                        return null;
                    }
                    foreach (var headers in new[] { put, read })
                    {
                        headers["x-amz-server-side-encryption-customer-algorithm"] = "AES256";
                        headers["x-amz-server-side-encryption-customer-key"] = key;
                        headers["x-amz-server-side-encryption-customer-key-md5"] = Convert.ToBase64String(MD5.HashData(decoded));
                    }
                    break;
                default:
                    reason = $"'{type}' is not a server-side encryption type (AES256, aws:kms, aws:kms:dsse, sse-c)";
                    return null;
            }
        }
        return put.Count == 0 && !requesterPays ? None : new S3Headers(put, read, requesterPays);
    }
}
