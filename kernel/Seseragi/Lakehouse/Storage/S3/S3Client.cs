using System.Buffers;
using System.Globalization;
using System.Net;
using System.Xml;
using Kaminari.Sazanami.Network;
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
        // Fetched once per request, outside the retried part: a credential source reports its own failures.
        AwsCredential? credential = _credential?.Get();
        return HttpTransport.Send(Request(method, path, query, extraHeaders, body, credential), _retry, alsoRetry, http: _http);
    }

    /// <summary>Sends a request asynchronously and hands its answer (body not read) to <paramref name="read"/>.</summary>
    private async Task<T> SendAsync<T>(
        HttpMethod method, string path, IReadOnlyDictionary<string, string>? extraHeaders, Func<HttpResponseMessage, CancellationToken, Task<T>> read,
        CancellationToken cancellationToken)
    {
        AwsCredential? credential = _credential is null ? null : await _credential.GetAsync(cancellationToken).ConfigureAwait(false);
        return await HttpTransport.SendAsync(Request(method, path, null, extraHeaders, null, credential), read, _retry, _http, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Makes the request (afresh for every attempt: each is signed at its own time).</summary>
    private Func<HttpRequestMessage> Request(
        HttpMethod method, string path, IReadOnlyList<KeyValuePair<string, string>>? query, IReadOnlyDictionary<string, string>? extraHeaders,
        ReadOnlyMemory<byte>? body, AwsCredential? credential)
    {
        string canonicalQuery = query is null ? "" : SigV4.CanonicalQuery(query);
        string payloadHash = body is { } content ? SigV4.Hash(content.Span) : SigV4.EmptyPayloadHash;
        string url = _origin + path + (canonicalQuery.Length == 0 ? "" : "?" + canonicalQuery);
        return () =>
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
        };
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
                using var reader = XmlReader.Create(response.OpenBody(), new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit });
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
        using StoreResponse response = Send(HttpMethod.Head, ObjectPath(key), extraHeaders: With(_headers.Read));
        if (response.Status == HttpStatusCode.NotFound) return null;
        if (!response.IsSuccess) throw Failure(response, $"read the properties of '{key}'");
        return new ObjectInfo(key, response.ContentHeaders.ContentLength ?? 0, response.ContentHeaders.LastModified ?? DateTimeOffset.UnixEpoch);
    }

    /// <inheritdoc/>
    public override bool Get(string key, IBufferWriter<byte> into)
    {
        using StoreResponse response = Send(HttpMethod.Get, ObjectPath(key), extraHeaders: With(_headers.Read));
        if (response.Status == HttpStatusCode.NotFound) return false;
        if (!response.IsSuccess) throw Failure(response, $"read '{key}'");
        into.Write(response.Body);
        return true;
    }

    /// <inheritdoc/>
    public override (byte[] Tail, long Size)? GetTail(string key, int length)
    {
        if (length == 0) return base.GetTail(key, length);
        // A suffix range: the end of the object and, in Content-Range, its size, in one request.
        using StoreResponse response = Send(HttpMethod.Get, ObjectPath(key), extraHeaders: With(_headers.Read, ("range", $"bytes=-{length}")));
        if (response.Status == HttpStatusCode.NotFound) return null;
        if (response.Status == HttpStatusCode.RequestedRangeNotSatisfiable) return base.GetTail(key, length); // an empty object
        if (!response.IsSuccess) throw Failure(response, $"read the end of '{key}'");
        // Read raw: stores write an empty object's range in forms the header parser refuses ("bytes 0--1/0").
        string? contentRange = response.ContentHeaders.NonValidated.TryGetValues("Content-Range", out var values) ? values.ToString() : null;
        return SuffixAnswer(response.Status, contentRange, response.Body, length) ?? base.GetTail(key, length);
    }

    /// <inheritdoc/>
    public override Task<RangeRead?> ReadRangeAsync(string key, long offset, Memory<byte> destination, string? version, CancellationToken cancellationToken)
    {
        if (destination.IsEmpty) return base.ReadRangeAsync(key, offset, destination, version, cancellationToken);
        var headers = With(_headers.Read, ("range", $"bytes={offset}-{offset + destination.Length - 1}"));
        if (version is not null) headers["if-match"] = version;
        return SendAsync(HttpMethod.Get, ObjectPath(key), headers, (response, ct) =>
            ReadRangeAnswer(response, offset, destination, response.Headers.ETag?.ToString() ?? version, version, r => Failure(r, $"read '{key}'"), ct), cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<ObjectTail?> GetTailAsync(string key, int length, CancellationToken cancellationToken)
    {
        if (length == 0) return await base.GetTailAsync(key, length, cancellationToken).ConfigureAwait(false);
        var (answer, etag) = await SendAsync(HttpMethod.Get, ObjectPath(key), With(_headers.Read, ("range", $"bytes=-{length}")),
            static async (r, ct) => (await HttpTransport.Buffer(r, retried: false, ct).ConfigureAwait(false), r.Headers.ETag?.ToString()),
            cancellationToken).ConfigureAwait(false);
        using StoreResponse response = answer;
        if (response.Status == HttpStatusCode.NotFound) return null;
        if (response.Status == HttpStatusCode.RequestedRangeNotSatisfiable) return await base.GetTailAsync(key, length, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccess) throw Failure(response, $"read the end of '{key}'");
        string? contentRange = response.ContentHeaders.NonValidated.TryGetValues("Content-Range", out var values) ? values.ToString() : null;
        return SuffixAnswer(response.Status, contentRange, response.Body, length) is var (tail, size)
            ? new ObjectTail(tail, size, etag)
            : await base.GetTailAsync(key, length, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override int GetRange(string key, long offset, Span<byte> destination)
    {
        if (destination.IsEmpty) return Head(key) is null ? -1 : 0;
        var range = With(_headers.Read, ("range", $"bytes={offset}-{offset + destination.Length - 1}"));
        using StoreResponse response = Send(HttpMethod.Get, ObjectPath(key), extraHeaders: range);
        return RangeAnswer(response, offset, destination) ?? throw Failure(response, $"read '{key}'");
    }

    /// <inheritdoc/>
    public override bool PutIfAbsent(string key, ReadOnlyMemory<byte> content)
    {
        var condition = With(_headers.Put, ("if-none-match", "*"));
        // 409: S3 is completing another conditional write of the key; asking again settles who won.
        using StoreResponse response = Send(HttpMethod.Put, ObjectPath(key), extraHeaders: condition, body: content,
            alsoRetry: static status => status == HttpStatusCode.Conflict);
        if (response.IsSuccess) return true;
        if (response.Status is HttpStatusCode.PreconditionFailed or HttpStatusCode.NotModified)
        {
            // After a retry the object may be our own earlier attempt, whose answer was lost: it is ours when it
            // holds exactly what we wrote.
            return response.Retried && Holds(key, content.Span);
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
        using StoreResponse response = Send(HttpMethod.Put, ObjectPath(key), extraHeaders: With(_headers.Put), body: content);
        if (!response.IsSuccess) throw Failure(response, $"write '{key}'");
    }

    /// <summary>
    /// Sends a request asynchronously and reads its whole answer. An answer <paramref name="retryAnswer"/> rejects is
    /// sent again like a network failure (S3 can answer a completion with 200 and an error document).
    /// </summary>
    private async Task<StoreResponse> SendBufferedAsync(
        HttpMethod method, string path, IReadOnlyList<KeyValuePair<string, string>>? query, IReadOnlyDictionary<string, string>? extraHeaders,
        ReadOnlyMemory<byte>? body, CancellationToken cancellationToken, Func<HttpStatusCode, bool>? alsoRetry = null,
        Func<StoreResponse, bool>? retryAnswer = null)
    {
        AwsCredential? credential = _credential is null ? null : await _credential.GetAsync(cancellationToken).ConfigureAwait(false);
        return await HttpTransport.SendAsync(Request(method, path, query, extraHeaders, body, credential), async (response, retried, ct) =>
        {
            StoreResponse answer = await HttpTransport.Buffer(response, retried, ct).ConfigureAwait(false);
            if (retryAnswer is not null && retryAnswer(answer))
            {
                using (answer) throw new IOException($"The store answered {(int)answer.Status} with an error: {Failure(answer, "complete the upload").Message}");
            }
            return answer;
        }, _retry, alsoRetry, _http, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Whether an answer's body is an S3 error document.</summary>
    private static bool IsErrorDocument(StoreResponse answer)
    {
        if (answer.Body.IsEmpty) return false;
        try
        {
            using var reader = XmlReader.Create(answer.OpenBody(), new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit });
            return reader.MoveToContent() == XmlNodeType.Element && reader.LocalName == "Error";
        }
        catch (XmlException)
        {
            return false;
        }
    }

    /// <summary>The text of the first element named <paramref name="name"/> in an XML answer, or null.</summary>
    internal static string? ElementText(Stream xml, string name)
    {
        try
        {
            using var reader = XmlReader.Create(xml, new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit });
            while (reader.Read())
            {
                if (reader.NodeType == XmlNodeType.Element && reader.LocalName == name) return reader.ReadElementContentAsString();
            }
        }
        catch (XmlException)
        {
            // Not XML: no such element.
        }
        return null;
    }

    /// <inheritdoc/>
    public override async Task<ObjectUpload> StartUploadAsync(string key, bool create, CancellationToken cancellationToken)
    {
        // Encryption settings are given when the upload starts (customer keys again with every part).
        using StoreResponse response = await SendBufferedAsync(HttpMethod.Post, ObjectPath(key), [new("uploads", "")], With(_headers.Put),
            ReadOnlyMemory<byte>.Empty, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccess) throw Failure(response, $"start an upload of '{key}'");
        string uploadId = ElementText(response.OpenBody(), "UploadId")
            ?? throw new StorageException($"Failed to start an upload of '{key}': the answer names no upload.", response.Status);
        return new S3Upload(this, key, create, uploadId);
    }

    /// <summary>
    /// An S3 multipart upload: parts are numbered from 1; completing names every part by its ETag. With create, the
    /// completion carries <c>If-None-Match: *</c>.
    /// </summary>
    private sealed class S3Upload(S3Client client, string key, bool create, string uploadId) : ObjectUpload(key, create)
    {
        private string Path => client.ObjectPath(Key);

        private List<KeyValuePair<string, string>> Query(int? index = null) => index is { } i
            ? [new("partNumber", (i + 1).ToString(CultureInfo.InvariantCulture)), new("uploadId", uploadId)]
            : [new("uploadId", uploadId)];

        public override async Task<UploadedPart> PutPartAsync(int index, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            using StoreResponse response = await client.SendBufferedAsync(HttpMethod.Put, Path, Query(index), With(client._headers.Read), content,
                cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccess) throw Failure(response, $"upload part {index + 1} of '{Key}'");
            string etag = response.Header("ETag")
                ?? throw new StorageException($"Failed to upload part {index + 1} of '{Key}': the answer has no ETag.", response.Status);
            return new UploadedPart(index, etag);
        }

        public override async Task<bool> CompleteAsync(IReadOnlyList<UploadedPart> parts, ReadOnlyMemory<byte> last, CancellationToken cancellationToken)
        {
            CheckOrder(parts);
            // A completion needs one part at least: an empty object is one empty part.
            if (!last.IsEmpty || parts.Count == 0)
                parts = [.. parts, await PutPartAsync(parts.Count, last, cancellationToken).ConfigureAwait(false)];

            var body = new System.Text.StringBuilder("<CompleteMultipartUpload>");
            foreach (UploadedPart part in parts)
            {
                body.Append("<Part><ETag>").Append(System.Security.SecurityElement.Escape(part.Id)).Append("</ETag><PartNumber>")
                    .Append(part.Index + 1).Append("</PartNumber></Part>");
            }
            body.Append("</CompleteMultipartUpload>");

            var headers = new Dictionary<string, string>(StringComparer.Ordinal);
            if (Create) headers["if-none-match"] = "*";
            // 409: S3 is completing another conditional write of the key; asking again settles who won.
            using StoreResponse response = await client.SendBufferedAsync(HttpMethod.Post, Path, Query(), headers,
                System.Text.Encoding.UTF8.GetBytes(body.ToString()), cancellationToken,
                alsoRetry: Create ? static status => status == HttpStatusCode.Conflict : null,
                retryAnswer: static answer => answer.Status == HttpStatusCode.OK && IsErrorDocument(answer)).ConfigureAwait(false);
            if (response.IsSuccess) return true;

            // After a retry, the first attempt may have landed with its answer lost: the store then refuses the
            // condition (MinIO) or no longer knows the upload (404). The upload is gone only once completed (it was
            // not aborted), so its absence says the object is ours. ETags cannot tell: encrypted parts have no MD5s.
            if (response.Retried && response.Status is HttpStatusCode.PreconditionFailed or HttpStatusCode.NotFound
                && await GoneAsync(cancellationToken).ConfigureAwait(false))
                return true;
            if (Create && response.Status is HttpStatusCode.PreconditionFailed or HttpStatusCode.Conflict)
            {
                // The key exists (or another writer's create of it is still in progress after the retries): the
                // upload stays open on the store until it is aborted.
                await AbortAsync(cancellationToken).ConfigureAwait(false);
                return false;
            }
            if (Create && response.Status == HttpStatusCode.NotImplemented)
            {
                await AbortAsync(cancellationToken).ConfigureAwait(false);
                throw new StorageException(
                    $"The store does not support conditional writes (If-None-Match) of multipart uploads: {Failure(response, $"create '{Key}'").Message}",
                    response.Status);
            }
            throw Failure(response, $"complete the upload of '{Key}'");
        }

        /// <summary>Whether the store no longer knows the upload (completed or aborted).</summary>
        private async Task<bool> GoneAsync(CancellationToken cancellationToken)
        {
            using StoreResponse response = await client.SendBufferedAsync(HttpMethod.Get, Path, [.. Query(), new("max-parts", "1")], With(client._headers.Read),
                null, cancellationToken).ConfigureAwait(false);
            if (response.Status == HttpStatusCode.NotFound) return true;
            if (response.IsSuccess) return false;
            throw Failure(response, $"read the state of the upload of '{Key}'");
        }

        public override async Task AbortAsync(CancellationToken cancellationToken)
        {
            using StoreResponse response = await client.SendBufferedAsync(HttpMethod.Delete, Path, Query(), null, null, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccess && response.Status != HttpStatusCode.NotFound) throw Failure(response, $"abort the upload of '{Key}'");
        }
    }

    /// <inheritdoc/>
    public override void Delete(string key)
    {
        using StoreResponse response = Send(HttpMethod.Delete, ObjectPath(key));
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

            List<ObjectInfo> objects;
            string? next;
            using (StoreResponse response = Send(HttpMethod.Get, _basePath.Length == 0 ? "/" : _basePath, query))
            {
                if (!response.IsSuccess) throw Failure(response, $"list '{prefix}'");
                (objects, next) = ParseListing(response.OpenBody());
            }
            foreach (ObjectInfo info in objects) yield return info;
            token = next;
        }
        while (token is not null);
    }

    /// <summary>The objects of a ListObjectsV2 page and the token of the next page, if any.</summary>
    internal static (List<ObjectInfo> Objects, string? NextToken) ParseListing(Stream xml)
    {
        var objects = new List<ObjectInfo>();
        bool truncated = false;
        string? next = null;
        using var reader = XmlReader.Create(xml, new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit, IgnoreWhitespace = true });
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
