using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Xml;
using Kaminari.Seseragi.Lakehouse.Storage.Http;

namespace Kaminari.Seseragi.Lakehouse.Storage.Gcs;

/// <summary>
/// One Google Cloud Storage bucket through the JSON API (the XML API's host serves it too, so a base URL means the
/// same for both), authorized with OAuth bearer tokens.
/// </summary>
internal sealed class GcsClient : ObjectStoreClient
{
    private static readonly UriCreationOptions Verbatim = new() { DangerousDisablePathAndQueryCanonicalization = true };

    private readonly string _baseUrl;
    private readonly string _bucket;
    private readonly GcsTokenSource? _token;
    private readonly bool _unsignedReads;
    private readonly RetrySettings _retry;

    /// <param name="settings">The service and the credentials.</param>
    /// <param name="bucket">The bucket.</param>
    /// <param name="retry">Retry settings.</param>
    public GcsClient(GcsSettings settings, string bucket, RetrySettings retry)
    {
        _baseUrl = settings.BaseUrl;
        _bucket = bucket;
        _token = settings.Token;
        _unsignedReads = settings.SkipSignature;
        _retry = retry;
    }

    /// <summary>Objects per listing page; small values let tests cross page boundaries.</summary>
    internal int PageSize { get; set; } = 1000;

    private static string Encode(string value) => PercentEncoding.Encode(value, encodeSlash: true);

    /// <summary>The JSON API path of an object: its whole name is one path segment, slashes included.</summary>
    internal static string ObjectPath(string bucket, string key) => $"/storage/v1/b/{Encode(bucket)}/o/{Encode(key)}";

    private static string Query(IEnumerable<KeyValuePair<string, string>> query) =>
        string.Join('&', query.Select(p => Encode(p.Key) + "=" + Encode(p.Value)));

    private StoreResponse Send(
        HttpMethod method, string pathAndQuery, Dictionary<string, string>? headers = null, ReadOnlyMemory<byte>? body = null)
    {
        string url = _baseUrl + pathAndQuery;
        return HttpTransport.Send(() =>
        {
            var request = new HttpRequestMessage(method, new Uri(url, Verbatim));
            bool read = method == HttpMethod.Get || method == HttpMethod.Head;
            if (_token is not null && !(read && _unsignedReads)) request.Headers.TryAddWithoutValidation("Authorization", "Bearer " + _token.Token());
            if (headers is not null)
                foreach (var (name, value) in headers) request.Headers.TryAddWithoutValidation(name, value);
            if (body is { } bytes)
            {
                request.Content = new ReadOnlyMemoryContent(bytes);
                request.Content.Headers.TryAddWithoutValidation("Content-Type", "application/octet-stream");
            }
            return request;
        }, _retry);
    }

    private static StorageException Failure(StoreResponse response, string what)
    {
        string? reason = null, message = null;
        if (response.Body.Length > 0)
        {
            // JSON from the JSON API; some answers (ranges) come as XML.
            try
            {
                using var document = JsonDocument.Parse(response.Body);
                if (document.RootElement.ValueKind == JsonValueKind.Object
                    && document.RootElement.TryGetProperty("error", out JsonElement error) && error.ValueKind == JsonValueKind.Object)
                {
                    if (error.TryGetProperty("message", out JsonElement m) && m.ValueKind == JsonValueKind.String) message = m.GetString();
                    if (error.TryGetProperty("errors", out JsonElement errors) && errors.ValueKind == JsonValueKind.Array
                        && errors.GetArrayLength() > 0 && errors[0].ValueKind == JsonValueKind.Object
                        && errors[0].TryGetProperty("reason", out JsonElement r) && r.ValueKind == JsonValueKind.String)
                        reason = r.GetString();
                }
            }
            catch (JsonException)
            {
                try
                {
                    using var reader = XmlReader.Create(new MemoryStream(response.Body), new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit });
                    while (!reader.EOF)
                    {
                        if (reader.NodeType == XmlNodeType.Element && reader.LocalName == "Code") reason = reader.ReadElementContentAsString();
                        else if (reader.NodeType == XmlNodeType.Element && reader.LocalName == "Message") message = reader.ReadElementContentAsString();
                        else reader.Read();
                    }
                }
                catch (XmlException)
                {
                    // Neither; the status says enough.
                }
            }
        }
        string detail = reason is null && message is null ? "" : $": {reason ?? "error"}" + (message is null ? "" : $" ({message})");
        return new StorageException($"Failed to {what}: HTTP {(int)response.Status}{detail}", response.Status, reason);
    }

    /// <inheritdoc/>
    public override ObjectInfo? Head(string key)
    {
        StoreResponse response = Send(HttpMethod.Get, ObjectPath(_bucket, key));
        if (response.Status == HttpStatusCode.NotFound) return null;
        if (!response.IsSuccess) throw Failure(response, $"read the properties of '{key}'");
        using var document = JsonDocument.Parse(response.Body);
        return ReadObject(document.RootElement);
    }

    /// <inheritdoc/>
    public override byte[]? Get(string key)
    {
        StoreResponse response = Send(HttpMethod.Get, ObjectPath(_bucket, key) + "?alt=media");
        if (response.Status == HttpStatusCode.NotFound) return null;
        if (!response.IsSuccess) throw Failure(response, $"read '{key}'");
        return response.Body;
    }

    /// <inheritdoc/>
    public override byte[]? GetRange(string key, long offset, int length)
    {
        var range = new Dictionary<string, string> { ["Range"] = $"bytes={offset}-{offset + length - 1}" };
        StoreResponse response = Send(HttpMethod.Get, ObjectPath(_bucket, key) + "?alt=media", range);
        switch (response.Status)
        {
            case HttpStatusCode.NotFound:
                return null;
            case HttpStatusCode.RequestedRangeNotSatisfiable:
                return []; // the object ends before the offset
            case HttpStatusCode.PartialContent:
                return response.Body;
            case HttpStatusCode.OK:
            {
                byte[] all = response.Body;
                if (offset >= all.Length) return [];
                return all.AsSpan((int)offset, (int)Math.Min(length, all.Length - offset)).ToArray();
            }
            default:
                throw Failure(response, $"read '{key}'");
        }
    }

    private string UploadPath(string key, bool ifAbsent)
    {
        var query = new List<KeyValuePair<string, string>> { new("uploadType", "media"), new("name", key) };
        if (ifAbsent) query.Add(new("ifGenerationMatch", "0"));
        return $"/upload/storage/v1/b/{Encode(_bucket)}/o?{Query(query)}";
    }

    /// <inheritdoc/>
    public override bool PutIfAbsent(string key, ReadOnlyMemory<byte> content)
    {
        StoreResponse response = Send(HttpMethod.Post, UploadPath(key, ifAbsent: true), body: content);
        if (response.IsSuccess) return true;
        if (response.Status == HttpStatusCode.PreconditionFailed)
        {
            // After a retry the object may be our own earlier attempt, whose answer was lost: it is ours when it
            // holds exactly what we wrote.
            return response.Retried && Get(key) is { } stored && content.Span.SequenceEqual(stored);
        }
        throw Failure(response, $"create '{key}'");
    }

    /// <inheritdoc/>
    public override void Put(string key, ReadOnlyMemory<byte> content)
    {
        StoreResponse response = Send(HttpMethod.Post, UploadPath(key, ifAbsent: false), body: content);
        if (!response.IsSuccess) throw Failure(response, $"write '{key}'");
    }

    /// <inheritdoc/>
    public override void Delete(string key)
    {
        StoreResponse response = Send(HttpMethod.Delete, ObjectPath(_bucket, key));
        if (!response.IsSuccess && response.Status != HttpStatusCode.NotFound) throw Failure(response, $"delete '{key}'");
    }

    /// <inheritdoc/>
    public override IEnumerable<ObjectInfo> List(string prefix, bool delimited, string? startAfter)
    {
        string? pageToken = null;
        do
        {
            var query = new List<KeyValuePair<string, string>> { new("maxResults", PageSize.ToString(CultureInfo.InvariantCulture)) };
            if (prefix.Length > 0) query.Add(new("prefix", prefix));
            if (delimited) query.Add(new("delimiter", "/"));
            // startOffset is inclusive: keys up to the offset are dropped below.
            if (startAfter is not null) query.Add(new("startOffset", startAfter));
            if (pageToken is not null) query.Add(new("pageToken", pageToken));

            StoreResponse response = Send(HttpMethod.Get, $"/storage/v1/b/{Encode(_bucket)}/o?{Query(query)}");
            if (!response.IsSuccess) throw Failure(response, $"list '{prefix}'");
            var (objects, next) = ParseListing(response.Body);
            foreach (ObjectInfo info in objects)
                if (startAfter is null || string.CompareOrdinal(info.Key, startAfter) > 0) yield return info;
            pageToken = next;
        }
        while (pageToken is not null);
    }

    /// <summary>The objects of a listing page (prefixes left out) and the token of the next page, if any.</summary>
    internal static (List<ObjectInfo> Objects, string? NextPageToken) ParseListing(byte[] json)
    {
        using var document = JsonDocument.Parse(json);
        JsonElement root = document.RootElement;
        var objects = new List<ObjectInfo>();
        if (root.TryGetProperty("items", out JsonElement items) && items.ValueKind == JsonValueKind.Array)
            foreach (JsonElement item in items.EnumerateArray()) objects.Add(ReadObject(item));
        string? next = root.TryGetProperty("nextPageToken", out JsonElement token) && token.ValueKind == JsonValueKind.String
            ? token.GetString()
            : null;
        return (objects, string.IsNullOrEmpty(next) ? null : next);
    }

    private static ObjectInfo ReadObject(JsonElement item)
    {
        string name = item.GetProperty("name").GetString() ?? "";
        long size = item.TryGetProperty("size", out JsonElement s)
            ? s.ValueKind == JsonValueKind.String ? long.Parse(s.GetString()!, CultureInfo.InvariantCulture) : s.GetInt64()
            : 0;
        DateTimeOffset updated = item.TryGetProperty("updated", out JsonElement u) && u.ValueKind == JsonValueKind.String
            ? DateTimeOffset.Parse(u.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal)
            : DateTimeOffset.UnixEpoch;
        return new ObjectInfo(name, size, updated);
    }
}
