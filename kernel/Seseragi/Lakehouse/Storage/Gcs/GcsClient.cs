using System.Globalization;
using System.Net;
using System.Text.Json;
using System.Xml;
using Kaminari.Sazanami.Network;
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
    private readonly CredentialSource<string>? _token;
    private readonly bool _unsignedReads;
    private readonly RetrySettings _retry;
    private readonly HttpOptions? _http;

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
        _http = settings.Http;
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
        // Fetched once per request, outside the retried part: a token source reports its own failures.
        bool read = method == HttpMethod.Get || method == HttpMethod.Head;
        string? token = _token is not null && !(read && _unsignedReads) ? _token.Get() : null;
        return HttpTransport.Send(Request(method, pathAndQuery, headers, body, token), _retry, http: _http);
    }

    /// <summary>Sends a read asynchronously and hands its answer (body not read) to <paramref name="read"/>.</summary>
    private async Task<T> SendAsync<T>(string pathAndQuery, Dictionary<string, string>? headers, Func<HttpResponseMessage, CancellationToken, Task<T>> read,
        CancellationToken cancellationToken)
    {
        string? token = _token is not null && !_unsignedReads ? await _token.GetAsync(cancellationToken).ConfigureAwait(false) : null;
        return await HttpTransport.SendAsync(Request(HttpMethod.Get, pathAndQuery, headers, null, token), read, _retry, _http, cancellationToken)
            .ConfigureAwait(false);
    }

    private Func<HttpRequestMessage> Request(HttpMethod method, string pathAndQuery, Dictionary<string, string>? headers, ReadOnlyMemory<byte>? body, string? token) =>
        RequestTo(method, _baseUrl + pathAndQuery, headers, body, token);

    /// <summary>
    /// Makes a request to <paramref name="url"/>. Headers of the content (<c>Content-*</c>) go with the body; a body is
    /// <c>application/octet-stream</c> unless the headers name another type (an empty one: none).
    /// </summary>
    private static Func<HttpRequestMessage> RequestTo(HttpMethod method, string url, Dictionary<string, string>? headers, ReadOnlyMemory<byte>? body, string? token) =>
        () =>
        {
            var request = new HttpRequestMessage(method, new Uri(url, Verbatim));
            if (token is not null) request.Headers.TryAddWithoutValidation("Authorization", "Bearer " + token);
            if (body is { } bytes)
            {
                request.Content = new ReadOnlyMemoryContent(bytes);
                if (headers?.ContainsKey("Content-Type") != true)
                    request.Content.Headers.TryAddWithoutValidation("Content-Type", "application/octet-stream");
            }
            if (headers is not null)
            {
                foreach (var (name, value) in headers)
                {
                    if (!name.StartsWith("Content-", StringComparison.OrdinalIgnoreCase)) request.Headers.TryAddWithoutValidation(name, value);
                    else if (request.Content is not null && value.Length > 0) request.Content.Headers.TryAddWithoutValidation(name, value);
                }
            }
            return request;
        };

    /// <summary>An object's version: its generation (sent with its content).</summary>
    private static string? Generation(HttpResponseMessage response) =>
        response.Headers.TryGetValues("x-goog-generation", out var values) ? values.FirstOrDefault() : null;

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
    public override (byte[] Tail, long Size)? GetTail(string key, int length)
    {
        if (length == 0) return base.GetTail(key, length);
        // A suffix range: the end of the object and, in Content-Range, its size, in one request.
        var range = new Dictionary<string, string> { ["Range"] = $"bytes=-{length}" };
        StoreResponse response = Send(HttpMethod.Get, ObjectPath(_bucket, key) + "?alt=media", range);
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
        var range = new Dictionary<string, string> { ["Range"] = $"bytes={offset}-{offset + destination.Length - 1}" };
        string path = ObjectPath(_bucket, key) + "?alt=media" + (version is null ? "" : "&ifGenerationMatch=" + Encode(version));
        return SendAsync(path, range, (response, ct) =>
            ReadRangeAnswer(response, offset, destination, Generation(response) ?? version, version, r => Failure(r, $"read '{key}'"), ct), cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<ObjectTail?> GetTailAsync(string key, int length, CancellationToken cancellationToken)
    {
        if (length == 0) return await base.GetTailAsync(key, length, cancellationToken).ConfigureAwait(false);
        var range = new Dictionary<string, string> { ["Range"] = $"bytes=-{length}" };
        var (response, generation) = await SendAsync(ObjectPath(_bucket, key) + "?alt=media", range,
            static async (r, ct) => (new StoreResponse(r.StatusCode, r.Headers, r.Content.Headers, await r.Content.ReadAsByteArrayAsync(ct).ConfigureAwait(false), false),
                Generation(r)), cancellationToken).ConfigureAwait(false);
        if (response.Status == HttpStatusCode.NotFound) return null;
        if (response.Status == HttpStatusCode.RequestedRangeNotSatisfiable) return await base.GetTailAsync(key, length, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccess) throw Failure(response, $"read the end of '{key}'");
        string? contentRange = response.ContentHeaders.NonValidated.TryGetValues("Content-Range", out var values) ? values.ToString() : null;
        return SuffixAnswer(response.Status, contentRange, response.Body, length) is var (tail, size)
            ? new ObjectTail(tail, size, generation)
            : await base.GetTailAsync(key, length, cancellationToken).ConfigureAwait(false);
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

    /// <summary>Sends a request to <paramref name="url"/> asynchronously and reads its whole answer.</summary>
    private async Task<StoreResponse> SendBufferedAsync(
        HttpMethod method, string url, Dictionary<string, string>? headers, ReadOnlyMemory<byte>? body, CancellationToken cancellationToken)
    {
        string? token = _token is null ? null : await _token.GetAsync(cancellationToken).ConfigureAwait(false);
        return await HttpTransport.SendAsync(RequestTo(method, url, headers, body, token), HttpTransport.Buffer, _retry, alsoRetry: null, _http,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The XML API address of an object (its name a path below the bucket).</summary>
    private string XmlObjectUrl(string key) => $"{_baseUrl}/{Encode(_bucket)}/{PercentEncoding.Encode(key, encodeSlash: false)}";

    /// <inheritdoc/>
    /// <remarks>
    /// Replacing uploads use the XML API's multipart uploads (parts in parallel, as the object_store library does).
    /// Creating uploads use a resumable upload of the JSON API: multipart uploads take no preconditions (refused when
    /// they start, ignored when they complete), resumable ones check <c>ifGenerationMatch=0</c> when they finish.
    /// </remarks>
    public override async Task<ObjectUpload> StartUploadAsync(string key, bool create, CancellationToken cancellationToken)
    {
        if (create)
        {
            List<KeyValuePair<string, string>> query = [new("uploadType", "resumable"), new("name", key), new("ifGenerationMatch", "0")];
            StoreResponse started = await SendBufferedAsync(HttpMethod.Post, $"{_baseUrl}/upload/storage/v1/b/{Encode(_bucket)}/o?{Query(query)}",
                new() { ["Content-Type"] = "", ["X-Upload-Content-Type"] = "application/octet-stream" }, ReadOnlyMemory<byte>.Empty,
                cancellationToken).ConfigureAwait(false);
            if (!started.IsSuccess) throw Failure(started, $"start an upload of '{key}'");
            string session = started.Header("Location")
                ?? throw new StorageException($"Failed to start an upload of '{key}': the answer names no upload session.", started.Status);
            return new GcsResumableUpload(this, key, session);
        }

        StoreResponse response = await SendBufferedAsync(HttpMethod.Post, XmlObjectUrl(key) + "?uploads", null, ReadOnlyMemory<byte>.Empty,
            cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccess) throw Failure(response, $"start an upload of '{key}'");
        string uploadId = S3.S3Client.ElementText(response.Body, "UploadId")
            ?? throw new StorageException($"Failed to start an upload of '{key}': the answer names no upload.", response.Status);
        return new GcsMultipartUpload(this, key, uploadId);
    }

    /// <summary>A multipart upload of the XML API (S3's protocol): parts numbered from 1, completed by their ETags.</summary>
    private sealed class GcsMultipartUpload(GcsClient client, string key, string uploadId) : ObjectUpload(key, create: false)
    {
        private string Url(int? index = null) => client.XmlObjectUrl(Key) + (index is { } i
            ? $"?partNumber={(i + 1).ToString(CultureInfo.InvariantCulture)}&uploadId={Encode(uploadId)}"
            : $"?uploadId={Encode(uploadId)}");

        public override async Task<UploadedPart> PutPartAsync(int index, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            StoreResponse response = await client.SendBufferedAsync(HttpMethod.Put, Url(index), null, content, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccess) throw Failure(response, $"upload part {index + 1} of '{Key}'");
            string etag = response.Header("ETag")
                ?? throw new StorageException($"Failed to upload part {index + 1} of '{Key}': the answer has no ETag.", response.Status);
            return new UploadedPart(index, etag);
        }

        public override async Task<bool> CompleteAsync(IReadOnlyList<UploadedPart> parts, ReadOnlyMemory<byte> last, CancellationToken cancellationToken)
        {
            CheckOrder(parts);
            if (!last.IsEmpty) parts = [.. parts, await PutPartAsync(parts.Count, last, cancellationToken).ConfigureAwait(false)];
            if (parts.Count == 0)
            {
                // The service refuses a completion without parts: an empty object is written plainly.
                await AbortAsync(cancellationToken).ConfigureAwait(false);
                client.Put(Key, ReadOnlyMemory<byte>.Empty);
                return true;
            }

            // ETags go in as they came, quotes and all (the service does not take them escaped).
            var body = new System.Text.StringBuilder("<CompleteMultipartUpload>");
            foreach (UploadedPart part in parts)
            {
                body.Append("<Part><PartNumber>").Append(part.Index + 1).Append("</PartNumber><ETag>").Append(part.Id).Append("</ETag></Part>");
            }
            body.Append("</CompleteMultipartUpload>");
            // A completion sent again after its answer was lost is answered as the first one was.
            StoreResponse response = await client.SendBufferedAsync(HttpMethod.Post, Url(), new() { ["Content-Type"] = "application/xml" },
                System.Text.Encoding.UTF8.GetBytes(body.ToString()), cancellationToken).ConfigureAwait(false);
            if (response.IsSuccess) return true;
            throw Failure(response, $"complete the upload of '{Key}'");
        }

        public override async Task AbortAsync(CancellationToken cancellationToken)
        {
            StoreResponse response = await client.SendBufferedAsync(HttpMethod.Delete, Url(), null, null, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccess && response.Status != HttpStatusCode.NotFound) throw Failure(response, $"abort the upload of '{Key}'");
        }
    }

    /// <summary>
    /// A resumable upload of the JSON API, created only when no object exists: chunks go in order to the session's
    /// address (every one but the last a multiple of 256 KiB), the last one with the total size, where the service
    /// checks the precondition. The service says how much it kept after each chunk; the rest is sent again.
    /// </summary>
    private sealed class GcsResumableUpload(GcsClient client, string key, string session) : ObjectUpload(key, create: true)
    {
        /// <summary>Answers in a row that keep nothing more (or do not finish) before giving up.</summary>
        private const int MaxStalls = 3;

        private long _offset; // bytes the service has kept
        private int _next;    // the index of the next part

        public override int MaxConcurrency => 1;

        public override async Task<UploadedPart> PutPartAsync(int index, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
        {
            if (index != _next) throw new InvalidOperationException($"A resumable upload takes its parts in order: part {_next} is next, not {index}.");
            await SendAsync(content, total: null, cancellationToken).ConfigureAwait(false);
            _next++;
            return new UploadedPart(index, "");
        }

        public override async Task<bool> CompleteAsync(IReadOnlyList<UploadedPart> parts, ReadOnlyMemory<byte> last, CancellationToken cancellationToken)
        {
            CheckOrder(parts);
            if (parts.Count != _next) throw new ArgumentException($"The upload has {_next} parts, not {parts.Count}.", nameof(parts));
            return await SendAsync(last, _offset + last.Length, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Sends bytes from the current offset, and with <paramref name="total"/> finishes the object: true when it was
        /// created, false when the precondition failed (an object exists).
        /// </summary>
        private async Task<bool> SendAsync(ReadOnlyMemory<byte> data, long? total, CancellationToken cancellationToken)
        {
            string size = total?.ToString(CultureInfo.InvariantCulture) ?? "*";
            for (int stalls = 0; ;)
            {
                string range = data.IsEmpty ? $"bytes */{size}" : $"bytes {_offset}-{_offset + data.Length - 1}/{size}";
                StoreResponse response = await client.SendBufferedAsync(HttpMethod.Put, session, new() { ["Content-Range"] = range }, data,
                    cancellationToken).ConfigureAwait(false);
                switch ((int)response.Status)
                {
                    case 200 or 201 when total is not null:
                        return true;
                    case 412 when total is not null:
                        await AbortQuietlyAsync().ConfigureAwait(false);
                        return false;
                    case 308:
                    {
                        long kept = Kept(response.Header("Range"));
                        if (kept < _offset || kept > _offset + data.Length)
                            throw new StorageException($"Failed to upload '{Key}': the service kept {kept} bytes, {_offset + data.Length} were sent.", response.Status);
                        stalls = kept == _offset ? stalls + 1 : 0; // a finish answered 308 makes no progress either
                        if (stalls > MaxStalls) throw new StorageException($"Failed to upload '{Key}': the service keeps no more bytes past {kept}.", response.Status);
                        data = data[(int)(kept - _offset)..];
                        _offset = kept;
                        if (total is null && data.IsEmpty) return true;
                        break;
                    }
                    default:
                        throw Failure(response, $"upload '{Key}'");
                }
            }
        }

        /// <summary>Ends the session after a refused finish (best effort: the service ends it too).</summary>
        private async Task AbortQuietlyAsync()
        {
            try
            {
                await AbortAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (StorageException)
            {
                // The object exists; the session is the service's to drop.
            }
        }

        /// <summary>The bytes kept, from a 308's <c>Range: bytes=0-N</c> (none without the header).</summary>
        private static long Kept(string? range)
        {
            if (range is null) return 0;
            int dash = range.LastIndexOf('-');
            return dash > 0 && long.TryParse(range.AsSpan(dash + 1), NumberStyles.None, CultureInfo.InvariantCulture, out long end)
                ? end + 1
                : throw new StorageException($"The upload service answered with an unreadable range '{range}'.");
        }

        public override async Task AbortAsync(CancellationToken cancellationToken)
        {
            // A cancelled session answers 499; one that finished or expired, 404 or 410.
            StoreResponse response = await client.SendBufferedAsync(HttpMethod.Delete, session, null, null, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccess && (int)response.Status is not (499 or 404 or 410)) throw Failure(response, $"abort the upload of '{Key}'");
        }
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
