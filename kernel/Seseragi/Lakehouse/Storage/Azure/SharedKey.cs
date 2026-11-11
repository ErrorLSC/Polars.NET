using System.Security.Cryptography;
using System.Text;

namespace Kaminari.Seseragi.Lakehouse.Storage.Azure;

/// <summary>Azure Storage SharedKey signatures (blob service).</summary>
internal static class SharedKey
{
    /// <summary>
    /// The string a SharedKey signature covers. <paramref name="headers"/> are the request's headers by lower-case
    /// name (standard ones that are not sent are simply absent); <paramref name="path"/> is the path exactly as
    /// sent; <paramref name="query"/> holds the query pairs, not encoded.
    /// </summary>
    public static string StringToSign(
        string method, string account, string path, IReadOnlyList<KeyValuePair<string, string>> query,
        IReadOnlyDictionary<string, string> headers)
    {
        string Header(string name) => headers.GetValueOrDefault(name) ?? "";

        // Content-Length is left empty when it is zero (service versions from 2015-02-21).
        string contentLength = Header("content-length") is "0" ? "" : Header("content-length");
        var builder = new StringBuilder()
            .Append(method).Append('\n')
            .Append(Header("content-encoding")).Append('\n')
            .Append(Header("content-language")).Append('\n')
            .Append(contentLength).Append('\n')
            .Append(Header("content-md5")).Append('\n')
            .Append(Header("content-type")).Append('\n')
            .Append(Header("date")).Append('\n')
            .Append(Header("if-modified-since")).Append('\n')
            .Append(Header("if-match")).Append('\n')
            .Append(Header("if-none-match")).Append('\n')
            .Append(Header("if-unmodified-since")).Append('\n')
            .Append(Header("range")).Append('\n');

        foreach (var (name, value) in headers.Where(h => h.Key.StartsWith("x-ms-", StringComparison.Ordinal))
                     .OrderBy(h => h.Key, StringComparer.Ordinal))
            builder.Append(name).Append(':').Append(value.Trim()).Append('\n');

        builder.Append('/').Append(account).Append(path);
        foreach (var group in query.GroupBy(p => p.Key, StringComparer.Ordinal).OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            builder.Append('\n').Append(group.Key.ToLowerInvariant()).Append(':')
                .AppendJoin(',', group.Select(p => p.Value).Order(StringComparer.Ordinal));
        }
        return builder.ToString();
    }

    /// <summary>The <c>Authorization</c> header value for a string to sign.</summary>
    public static string Authorization(string account, byte[] key, string stringToSign) =>
        $"SharedKey {account}:{Convert.ToBase64String(HMACSHA256.HashData(key, Encoding.UTF8.GetBytes(stringToSign)))}";
}
