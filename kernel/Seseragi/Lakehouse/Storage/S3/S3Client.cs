using System.Globalization;
using System.Net;
using System.Xml;
using Kaminari.Seseragi.Lakehouse.Storage.Http;

namespace Kaminari.Seseragi.Lakehouse.Storage.S3;

/// <summary>
/// One S3 bucket (or a bucket of an S3-compatible store) through its REST API, signed with SigV4.
/// </summary>
internal sealed class S3Client : ObjectStoreClient
{
    private static readonly UriCreationOptions Verbatim = new() { DangerousDisablePathAndQueryCanonicalization = true };

    private readonly string _origin;     // scheme://host[:port]
    private readonly string _basePath;   // "/bucket" (path style) or "" (virtual-hosted)
    private readonly string _host;       // the Host header
    private readonly string _region;
    private readonly CredentialSource<AwsCredential>? _credential;
    private readonly RetrySettings _retry;
    private readonly S3Headers _headers;
    private readonly HttpOptions? _http;

    /// <param name="bucketEndpoint">The bucket's URL: <c>https://s3.region.amazonaws.com/bucket</c>, <c>http://localhost:9000/bucket</c> or <c>https://bucket.s3.region.amazonaws.com</c>.</param>
    /// <param name="region">The signing region.</param>
    /// <param name="credential">Where the credentials come from; null sends unsigned requests.</param>
    /// <param name="retry">Retry settings.</param>
    /// <param name="headers">Encryption and requester-pays headers.</param>
    /// <param name="http">Connection options.</param>
    public S3Client(
        string bucketEndpoint, string region, CredentialSource<AwsCredential>? credential, RetrySettings retry, S3Headers? headers = null,
        HttpOptions? http = null)
    {
        _http = http;
        _headers = headers ?? S3Headers.None;
        var uri = new Uri(bucketEndpoint.TrimEnd('/'));
        _origin = uri.GetLeftPart(UriPartial.Authority);
        _basePath = uri.AbsolutePath.TrimEnd('/');
        _host = uri.IsDefaultPort ? uri.Host : $"{uri.Host}:{uri.Port}";
        _region = region;
        _credential = credential;
        _retry = retry;
    }

    /// <summary>Keys per listing page; small values let tests cross page boundaries.</summary>
    internal int PageSize { get; set; } = 1000;

    private string ObjectPath(string key) => _basePath + "/" + SigV4.Encode(key, encodeSlash: false);

    private StoreResponse Send(
        HttpMethod method, string path, IReadOnlyList<KeyValuePair<string, string>>? query = null,
        IReadOnlyDictionary<string, string>? extraHeaders = null, ReadOnlyMemory<byte>? body = null,
        Func<HttpStatusCode, bool>? alsoRetry = null)
    {
        string canonicalQuery = query is null ? "" : SigV4.CanonicalQuery(query);
        string payloadHash = body is { } content ? SigV4.Hash(content.Span) : SigV4.EmptyPayloadHash;
        string url = _origin + path + (canonicalQuery.Length == 0 ? "" : "?" + canonicalQuery);

        // Fetched once per request, outside the retried part: a credential source reports its own failures.
        AwsCredential? credential = _credential?.Get();
        return HttpTransport.Send(() =>
        {
            var request = new HttpRequestMessage(method, new Uri(url, Verbatim));
            var signed = new Dictionary<string, string>(StringComparer.Ordinal) { ["host"] = _host };
            if (extraHeaders is not null)
                foreach (var (name, value) in extraHeaders) signed[name] = value;
            if (credential is not null)
            {
                if (_headers.RequesterPays) signed["x-amz-request-payer"] = "requester";
                DateTimeOffset now = DateTimeOffset.UtcNow;
                signed["x-amz-date"] = SigV4.AmzDate(now);
                signed["x-amz-content-sha256"] = payloadHash;
                if (credential.Token is not null) signed["x-amz-security-token"] = credential.Token;
                string authorization = SigV4.Authorization(
                    method.Method, path, canonicalQuery, signed, payloadHash, credential, _region, "s3", now);
                request.Headers.TryAddWithoutValidation("Authorization", authorization);
            }
            foreach (var (name, value) in signed)
                if (name != "host") request.Headers.TryAddWithoutValidation(name, value);
            if (body is { } bytes) request.Content = new ReadOnlyMemoryContent(bytes);
            return request;
        }, _retry, alsoRetry, http: _http);
    }

    private static Dictionary<string, string> With(IReadOnlyDictionary<string, string> headers, params (string Name, string Value)[] more)
    {
        var all = new Dictionary<string, string>(headers, StringComparer.Ordinal);
        foreach (var (name, value) in more) all[name] = value;
        return all;
    }

    private static StorageException Failure(StoreResponse response, string what)
    {
        string? code = null, message = null;
        if (response.Body.Length > 0)
        {
            try
            {
                using var reader = XmlReader.Create(new MemoryStream(response.Body), new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit });
                while (!reader.EOF)
                {
                    if (reader.NodeType == XmlNodeType.Element && reader.LocalName == "Code") code = reader.ReadElementContentAsString();
                    else if (reader.NodeType == XmlNodeType.Element && reader.LocalName == "Message") message = reader.ReadElementContentAsString();
                    else reader.Read();
                }
            }
            catch (XmlException)
            {
                // Not an S3 error document; the status says enough.
            }
        }
        string detail = code is null ? "" : $": {code}" + (message is null ? "" : $" ({message})");
        return new StorageException($"Failed to {what}: HTTP {(int)response.Status}{detail}", response.Status, code);
    }

    /// <inheritdoc/>
    public override ObjectInfo? Head(string key)
    {
        StoreResponse response = Send(HttpMethod.Head, ObjectPath(key), extraHeaders: With(_headers.Read));
        if (response.Status == HttpStatusCode.NotFound) return null;
        if (!response.IsSuccess) throw Failure(response, $"read the properties of '{key}'");
        return new ObjectInfo(key, response.ContentHeaders.ContentLength ?? 0, response.ContentHeaders.LastModified ?? DateTimeOffset.UnixEpoch);
    }

    /// <inheritdoc/>
    public override byte[]? Get(string key)
    {
        StoreResponse response = Send(HttpMethod.Get, ObjectPath(key), extraHeaders: With(_headers.Read));
        if (response.Status == HttpStatusCode.NotFound) return null;
        if (!response.IsSuccess) throw Failure(response, $"read '{key}'");
        return response.Body;
    }

    /// <inheritdoc/>
    public override (byte[] Tail, long Size)? GetTail(string key, int length)
    {
        if (length == 0) return base.GetTail(key, length);
        // A suffix range: the end of the object and, in Content-Range, its size, in one request.
        StoreResponse response = Send(HttpMethod.Get, ObjectPath(key), extraHeaders: With(_headers.Read, ("range", $"bytes=-{length}")));
        if (response.Status == HttpStatusCode.NotFound) return null;
        if (response.Status == HttpStatusCode.RequestedRangeNotSatisfiable) return base.GetTail(key, length); // an empty object
        if (!response.IsSuccess) throw Failure(response, $"read the end of '{key}'");
        // Read raw: stores write an empty object's range in forms the header parser refuses ("bytes 0--1/0").
        string? contentRange = response.ContentHeaders.NonValidated.TryGetValues("Content-Range", out var values) ? values.ToString() : null;
        return SuffixAnswer(response.Status, contentRange, response.Body, length) ?? base.GetTail(key, length);
    }

    /// <inheritdoc/>
    public override byte[]? GetRange(string key, long offset, int length)
    {
        var range = With(_headers.Read, ("range", $"bytes={offset}-{offset + length - 1}"));
        StoreResponse response = Send(HttpMethod.Get, ObjectPath(key), extraHeaders: range);
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
                // The store ignored the range and sent everything.
                byte[] all = response.Body;
                if (offset >= all.Length) return [];
                return all.AsSpan((int)offset, (int)Math.Min(length, all.Length - offset)).ToArray();
            }
            default:
                throw Failure(response, $"read '{key}'");
        }
    }

    /// <inheritdoc/>
    public override bool PutIfAbsent(string key, ReadOnlyMemory<byte> content)
    {
        var condition = With(_headers.Put, ("if-none-match", "*"));
        // 409: S3 is completing another conditional write of the key; asking again settles who won.
        StoreResponse response = Send(HttpMethod.Put, ObjectPath(key), extraHeaders: condition, body: content,
            alsoRetry: static status => status == HttpStatusCode.Conflict);
        if (response.IsSuccess) return true;
        if (response.Status is HttpStatusCode.PreconditionFailed or HttpStatusCode.NotModified)
        {
            // After a retry the object may be our own earlier attempt, whose answer was lost: it is ours when it
            // holds exactly what we wrote.
            return response.Retried && Get(key) is { } stored && content.Span.SequenceEqual(stored);
        }
        // Still 409 when the retries ran out: another writer's write of the key is in progress and ours did not
        // land. A refused create says exactly that (a table commit then rereads the log and tries again).
        if (response.Status == HttpStatusCode.Conflict) return false;
        if (response.Status == HttpStatusCode.NotImplemented)
            throw new StorageException(
                $"The store does not support conditional writes (If-None-Match), which commits to a table need: {Failure(response, $"create '{key}'").Message}",
                response.Status);
        throw Failure(response, $"create '{key}'");
    }

    /// <inheritdoc/>
    public override void Put(string key, ReadOnlyMemory<byte> content)
    {
        StoreResponse response = Send(HttpMethod.Put, ObjectPath(key), extraHeaders: With(_headers.Put), body: content);
        if (!response.IsSuccess) throw Failure(response, $"write '{key}'");
    }

    /// <inheritdoc/>
    public override void Delete(string key)
    {
        StoreResponse response = Send(HttpMethod.Delete, ObjectPath(key));
        if (!response.IsSuccess && response.Status != HttpStatusCode.NotFound) throw Failure(response, $"delete '{key}'");
    }

    /// <inheritdoc/>
    public override IEnumerable<ObjectInfo> List(string prefix, bool delimited, string? startAfter)
    {
        string? token = null;
        do
        {
            var query = new List<KeyValuePair<string, string>>
            {
                new("list-type", "2"),
                new("max-keys", PageSize.ToString(CultureInfo.InvariantCulture)),
            };
            if (prefix.Length > 0) query.Add(new("prefix", prefix));
            if (delimited) query.Add(new("delimiter", "/"));
            if (startAfter is not null) query.Add(new("start-after", startAfter));
            if (token is not null) query.Add(new("continuation-token", token));

            StoreResponse response = Send(HttpMethod.Get, _basePath.Length == 0 ? "/" : _basePath, query);
            if (!response.IsSuccess) throw Failure(response, $"list '{prefix}'");
            var (objects, next) = ParseListing(response.Body);
            foreach (ObjectInfo info in objects) yield return info;
            token = next;
        }
        while (token is not null);
    }

    /// <summary>The objects of a ListObjectsV2 page and the token of the next page, if any.</summary>
    internal static (List<ObjectInfo> Objects, string? NextToken) ParseListing(byte[] xml)
    {
        var objects = new List<ObjectInfo>();
        bool truncated = false;
        string? next = null;
        using var reader = XmlReader.Create(new MemoryStream(xml), new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit, IgnoreWhitespace = true });
        reader.MoveToContent();
        reader.Read();
        while (!reader.EOF)
        {
            if (reader.NodeType != XmlNodeType.Element || reader.Depth != 1)
            {
                reader.Read();
                continue;
            }
            switch (reader.LocalName)
            {
                case "Contents":
                    using (XmlReader item = reader.ReadSubtree()) objects.Add(ReadContents(item));
                    reader.Read(); // past </Contents>
                    break;
                case "IsTruncated":
                    truncated = reader.ReadElementContentAsString() == "true";
                    break;
                case "NextContinuationToken":
                    next = reader.ReadElementContentAsString();
                    break;
                default:
                    reader.Skip();
                    break;
            }
        }
        return (objects, truncated ? next : null);
    }

    private static ObjectInfo ReadContents(XmlReader item)
    {
        string key = "";
        long size = 0;
        DateTimeOffset modified = DateTimeOffset.UnixEpoch;
        item.Read(); // <Contents>
        item.Read();
        while (!item.EOF)
        {
            if (item.NodeType != XmlNodeType.Element)
            {
                item.Read();
                continue;
            }
            switch (item.LocalName)
            {
                case "Key":
                    key = item.ReadElementContentAsString();
                    break;
                case "Size":
                    size = long.Parse(item.ReadElementContentAsString(), CultureInfo.InvariantCulture);
                    break;
                case "LastModified":
                    modified = DateTimeOffset.Parse(item.ReadElementContentAsString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
                    break;
                default:
                    item.Skip();
                    break;
            }
        }
        return new ObjectInfo(key, size, modified);
    }
}
