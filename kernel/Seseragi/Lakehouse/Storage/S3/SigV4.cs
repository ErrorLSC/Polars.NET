using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Kaminari.Seseragi.Lakehouse.Storage.S3;

/// <summary>AWS credentials: an access key, its secret, and the session token of temporary credentials.</summary>
internal sealed record AwsCredential(string KeyId, string Secret, string? Token);

/// <summary>AWS Signature Version 4 (header-based), as S3 and S3-compatible stores check it.</summary>
internal static class SigV4
{
    /// <summary>The SHA-256 of an empty body.</summary>
    public const string EmptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private const string Algorithm = "AWS4-HMAC-SHA256";

    /// <summary>
    /// Percent-encodes as SigV4 wants: letters, digits and <c>-_.~</c> stay, every other UTF-8 byte becomes
    /// <c>%XX</c> (upper case); <c>/</c> stays unless <paramref name="encodeSlash"/>.
    /// </summary>
    public static string Encode(string value, bool encodeSlash)
    {
        var builder = new StringBuilder(value.Length + 8);
        foreach (byte b in Encoding.UTF8.GetBytes(value))
        {
            char c = (char)b;
            if (c is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or >= '0' and <= '9' or '-' or '_' or '.' or '~'
                || (c == '/' && !encodeSlash))
                builder.Append(c);
            else
                builder.Append('%').Append(b.ToString("X2", CultureInfo.InvariantCulture));
        }
        return builder.ToString();
    }

    /// <summary>The query string in canonical form: encoded, sorted by name then value.</summary>
    public static string CanonicalQuery(IEnumerable<KeyValuePair<string, string>> query) =>
        string.Join('&', query
            .Select(p => (Key: Encode(p.Key, true), Value: Encode(p.Value, true)))
            .OrderBy(p => p.Key, StringComparer.Ordinal)
            .ThenBy(p => p.Value, StringComparer.Ordinal)
            .Select(p => p.Key + "=" + p.Value));

    /// <summary>Lower-case hex SHA-256.</summary>
    public static string Hash(ReadOnlySpan<byte> data) => Convert.ToHexString(SHA256.HashData(data)).ToLowerInvariant();

    /// <summary>
    /// The <c>Authorization</c> header of a request. <paramref name="headers"/> are the headers to sign, host
    /// included, by lower-case name; <paramref name="canonicalPath"/> and <paramref name="canonicalQuery"/> are
    /// exactly what is sent.
    /// </summary>
    public static string Authorization(
        string method, string canonicalPath, string canonicalQuery, IReadOnlyDictionary<string, string> headers,
        string payloadHash, AwsCredential credential, string region, string service, DateTimeOffset time)
    {
        string amzDate = time.UtcDateTime.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
        string date = amzDate[..8];

        var sorted = headers.OrderBy(h => h.Key, StringComparer.Ordinal).ToList();
        string signedHeaders = string.Join(';', sorted.Select(h => h.Key));
        var canonical = new StringBuilder()
            .Append(method).Append('\n')
            .Append(canonicalPath).Append('\n')
            .Append(canonicalQuery).Append('\n');
        foreach (var (name, value) in sorted) canonical.Append(name).Append(':').Append(Trim(value)).Append('\n');
        canonical.Append('\n').Append(signedHeaders).Append('\n').Append(payloadHash);

        string scope = $"{date}/{region}/{service}/aws4_request";
        string stringToSign = $"{Algorithm}\n{amzDate}\n{scope}\n{Hash(Encoding.UTF8.GetBytes(canonical.ToString()))}";

        byte[] key = Hmac(Encoding.UTF8.GetBytes("AWS4" + credential.Secret), date);
        key = Hmac(key, region);
        key = Hmac(key, service);
        key = Hmac(key, "aws4_request");
        string signature = Convert.ToHexString(Hmac(key, stringToSign)).ToLowerInvariant();
        return $"{Algorithm} Credential={credential.KeyId}/{scope}, SignedHeaders={signedHeaders}, Signature={signature}";
    }

    /// <summary>The <c>x-amz-date</c> value of a time.</summary>
    public static string AmzDate(DateTimeOffset time) =>
        time.UtcDateTime.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);

    private static byte[] Hmac(byte[] key, string data) => HMACSHA256.HashData(key, Encoding.UTF8.GetBytes(data));

    private static string Trim(string value)
    {
        value = value.Trim();
        if (!value.Contains("  ", StringComparison.Ordinal)) return value;
        var builder = new StringBuilder(value.Length);
        foreach (char c in value)
            if (c != ' ' || builder.Length == 0 || builder[^1] != ' ') builder.Append(c);
        return builder.ToString();
    }
}
