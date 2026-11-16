using System.Globalization;
using System.Net;
using System.Xml;
using Kaminari.Sazanami.Network;
using Kaminari.Seseragi.Lakehouse.Storage.Http;

namespace Kaminari.Seseragi.Lakehouse.Storage.Azure;

/// <summary>One container of Azure Blob Storage (or of Azurite) through the blob REST API.</summary>
internal sealed class AzureClient : ObjectStoreClient
{
    /// <summary>The service version requests ask for (as the object_store library does).</summary>
    internal const string ServiceVersion = "2023-11-03";

    private static readonly UriCreationOptions Verbatim = new() { DangerousDisablePathAndQueryCanonicalization = true };

    private readonly string _origin;        // scheme://host[:port]
    private readonly string _containerPath; // "/container", or "/account/container" on the emulator
    private readonly string _account;
    private readonly AzureAuth? _auth;
    private readonly RetrySettings _retry;
    private readonly HttpOptions? _http;

    /// <param name="settings">The service, account and credentials.</param>
    /// <param name="container">The container.</param>
    /// <param name="retry">Retry settings.</param>
    public AzureClient(AzureSettings settings, string container, RetrySettings retry)
    {
        var uri = new Uri(settings.ServiceUrl + "/" + container);
        _origin = uri.GetLeftPart(UriPartial.Authority);
        _containerPath = uri.AbsolutePath.TrimEnd('/');
        _account = settings.Account;
        _auth = settings.Auth;
        _retry = retry;
        _http = settings.Http;
    }

    /// <summary>Blobs per listing page; small values let tests cross page boundaries.</summary>
    internal int PageSize { get; set; } = 5000;

    private string BlobPath(string key) => _containerPath + "/" + PercentEncoding.Encode(key, encodeSlash: false);

    private StoreResponse Send(
        HttpMethod method, string path, List<KeyValuePair<string, string>>? query = null,
        Dictionary<string, string>? extraHeaders = null, ReadOnlyMemory<byte>? body = null)
    {
        // Fetched once per request, outside the retried part: a token source reports its own failures.
        IReadOnlyList<KeyValuePair<string, string>>? sas = _auth is AzureAuth.RenewedSas renewed ? renewed.Pairs.Get() : null;
        string? bearerToken = _auth is AzureAuth.Bearer bearer ? bearer.Token.Get() : null;
        return HttpTransport.Send(Request(method, path, query ?? [], extraHeaders, body, sas, bearerToken), _retry, http: _http);
    }

    /// <summary>Sends a request asynchronously and hands its answer (body not read) to <paramref name="read"/>.</summary>
    private async Task<T> SendAsync<T>(
        HttpMethod method, string path, Dictionary<string, string>? extraHeaders, Func<HttpResponseMessage, CancellationToken, Task<T>> read,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<KeyValuePair<string, string>>? sas = _auth is AzureAuth.RenewedSas renewed
            ? await renewed.Pairs.GetAsync(cancellationToken).ConfigureAwait(false) : null;
        string? bearerToken = _auth is AzureAuth.Bearer bearer ? await bearer.Token.GetAsync(cancellationToken).ConfigureAwait(false) : null;
        return await HttpTransport.SendAsync(Request(method, path, [], extraHeaders, null, sas, bearerToken), read, _retry, _http, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Makes the request (afresh for every attempt: each is dated and signed at its own time).</summary>
    private Func<HttpRequestMessage> Request(
        HttpMethod method, string path, List<KeyValuePair<string, string>> query, Dictionary<string, string>? extraHeaders,
        ReadOnlyMemory<byte>? body, IReadOnlyList<KeyValuePair<string, string>>? renewedSas, string? bearerToken)
    {
        IEnumerable<KeyValuePair<string, string>> sent = _auth switch
        {
            AzureAuth.Sas sas => query.Concat(sas.Pairs),
            AzureAuth.RenewedSas => query.Concat(renewedSas!),
            _ => query,
        };
        string queryText = string.Join('&', sent.Select(p =>
            PercentEncoding.Encode(p.Key, encodeSlash: true) + "=" + PercentEncoding.Encode(p.Value, encodeSlash: true)));
        string url = _origin + path + (queryText.Length == 0 ? "" : "?" + queryText);
        return () =>
        {
            var request = new HttpRequestMessage(method, new Uri(url, Verbatim));
            var headers = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["x-ms-date"] = DateTimeOffset.UtcNow.ToString("R", CultureInfo.InvariantCulture),
                ["x-ms-version"] = ServiceVersion,
            };
            if (extraHeaders is not null)
                foreach (var (name, value) in extraHeaders) headers[name] = value;
            foreach (var (name, value) in headers) request.Headers.TryAddWithoutValidation(name, value);

            switch (_auth)
            {
                case AzureAuth.SharedKey key:
                {
                    var signed = new Dictionary<string, string>(headers, StringComparer.Ordinal);
                    if (body is { } content) signed["content-length"] = content.Length.ToString(CultureInfo.InvariantCulture);
                    string toSign = SharedKey.StringToSign(method.Method, _account, path, query, signed);
                    request.Headers.TryAddWithoutValidation("Authorization", SharedKey.Authorization(_account, key.Key, toSign));
                    break;
                }
                case AzureAuth.Bearer:
                    request.Headers.TryAddWithoutValidation("Authorization", "Bearer " + bearerToken);
                    break;
            }
            if (body is { } bytes) request.Content = new ReadOnlyMemoryContent(bytes);
            return request;
        };
    }

    private static string? ErrorCode(StoreResponse response) => response.Header("x-ms-error-code");

    private static StorageException Failure(StoreResponse response, string what)
    {
        string? code = ErrorCode(response), message = null;
        if (response.Body.Length > 0)
        {
            try
            {
                using var reader = XmlReader.Create(new MemoryStream(response.Body), new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit });
                while (!reader.EOF)
                {
                    if (reader.NodeType == XmlNodeType.Element && reader.LocalName == "Code")
                    {
                        string inBody = reader.ReadElementContentAsString(); // always read: it advances the reader
                        code ??= inBody;
                    }
                    else if (reader.NodeType == XmlNodeType.Element && reader.LocalName == "Message") message = reader.ReadElementContentAsString();
                    else reader.Read();
                }
            }
            catch (XmlException)
            {
                // Not an error document; the status says enough.
            }
        }
        string detail = code is null ? "" : $": {code}" + (message is null ? "" : $" ({message.Split('\n')[0].Trim()})");
        return new StorageException($"Failed to {what}: HTTP {(int)response.Status}{detail}", response.Status, code);
    }

    // With a hierarchical namespace, directories are blobs too; they are not files.
    private static bool IsDirectory(StoreResponse response) =>
        response.Header("x-ms-resource-type") is { } type && type != "file";

    /// <inheritdoc/>
    public override ObjectInfo? Head(string key)
    {
        StoreResponse response = Send(HttpMethod.Head, BlobPath(key));
        if (response.Status == HttpStatusCode.NotFound) return null;
        if (!response.IsSuccess) throw Failure(response, $"read the properties of '{key}'");
        if (IsDirectory(response)) return null;
        return new ObjectInfo(key, response.ContentHeaders.ContentLength ?? 0, response.ContentHeaders.LastModified ?? DateTimeOffset.UnixEpoch);
    }

    /// <inheritdoc/>
    public override byte[]? Get(string key)
    {
        StoreResponse response = Send(HttpMethod.Get, BlobPath(key));
        if (response.Status == HttpStatusCode.NotFound) return null;
        if (!response.IsSuccess) throw Failure(response, $"read '{key}'");
        return IsDirectory(response) ? null : response.Body;
    }

    /// <inheritdoc/>
    public override Task<RangeRead?> ReadRangeAsync(string key, long offset, Memory<byte> destination, string? version, CancellationToken cancellationToken)
    {
        if (destination.IsEmpty) return base.ReadRangeAsync(key, offset, destination, version, cancellationToken);
        var headers = new Dictionary<string, string> { ["range"] = $"bytes={offset}-{offset + destination.Length - 1}" };
        if (version is not null) headers["if-match"] = version;
        return SendAsync(HttpMethod.Get, BlobPath(key), headers, async (response, ct) =>
        {
            // A directory (hierarchical namespace) is no file; Azure answers its range with 416 and no resource type.
            if (response.Headers.TryGetValues("x-ms-resource-type", out var type) && type.First() != "file") return null;
            if (response.StatusCode == HttpStatusCode.RequestedRangeNotSatisfiable && Head(key) is null) return null;
            return await ReadRangeAnswer(response, offset, destination, response.Headers.ETag?.ToString() ?? version, version, r => Failure(r, $"read '{key}'"), ct)
                .ConfigureAwait(false);
        }, cancellationToken);
    }

    /// <summary>
    /// The end of a blob: its properties (Azure has no suffix ranges), then a ranged read of that version (the size
    /// and version both from the first request).
    /// </summary>
    public override async Task<ObjectTail?> GetTailAsync(string key, int length, CancellationToken cancellationToken)
    {
        ObjectInfo? info = null;
        string? etag = await SendAsync(HttpMethod.Head, BlobPath(key), null, (response, _) =>
        {
            var answer = new StoreResponse(response.StatusCode, response.Headers, response.Content.Headers, [], retried: false);
            if (answer.Status == HttpStatusCode.NotFound || answer.IsSuccess && IsDirectory(answer)) return Task.FromResult<string?>(null);
            if (!answer.IsSuccess) throw Failure(answer, $"read the properties of '{key}'");
            info = new ObjectInfo(key, response.Content.Headers.ContentLength ?? 0, response.Content.Headers.LastModified ?? DateTimeOffset.UnixEpoch);
            return Task.FromResult(response.Headers.ETag?.ToString());
        }, cancellationToken).ConfigureAwait(false);
        if (info is not { } blob) return null;
        int count = (int)Math.Min(length, blob.Size);
        byte[] tail = new byte[count];
        if (count > 0)
        {
            if (await ReadRangeAsync(key, blob.Size - count, tail, etag, cancellationToken).ConfigureAwait(false) is not { } read) return null;
            if (read.Count != count)
                throw new StorageException($"Failed to read the end of '{key}': the blob changed while it was read.", HttpStatusCode.PreconditionFailed);
        }
        return new ObjectTail(tail, blob.Size, etag);
    }

    /// <inheritdoc/>
    public override byte[]? GetRange(string key, long offset, int length)
    {
        var range = new Dictionary<string, string> { ["range"] = $"bytes={offset}-{offset + length - 1}" };
        StoreResponse response = Send(HttpMethod.Get, BlobPath(key), extraHeaders: range);
        switch (response.Status)
        {
            case HttpStatusCode.NotFound:
                return null;
            case HttpStatusCode.RequestedRangeNotSatisfiable:
                // The blob ends before the offset, or (hierarchical namespace) it is a directory: Azure answers a
                // range of a directory with 416 and no x-ms-resource-type, so only a HEAD tells them apart.
                return Head(key) is null ? null : [];
            case HttpStatusCode.PartialContent:
                return IsDirectory(response) ? null : response.Body;
            case HttpStatusCode.OK:
            {
                if (IsDirectory(response)) return null;
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
        var headers = new Dictionary<string, string> { ["if-none-match"] = "*", ["x-ms-blob-type"] = "BlockBlob" };
        StoreResponse response = Send(HttpMethod.Put, BlobPath(key), extraHeaders: headers, body: content);
        if (response.IsSuccess) return true;
        // 409 (BlobAlreadyExists) and 412 both mean the blob is there, as the engine's object store reads them.
        if (response.Status is HttpStatusCode.Conflict or HttpStatusCode.PreconditionFailed)
        {
            // After a retry the blob may be our own earlier attempt, whose answer was lost: it is ours when it holds
            // exactly what we wrote.
            return response.Retried && Get(key) is { } stored && content.Span.SequenceEqual(stored);
        }
        throw Failure(response, $"create '{key}'");
    }

    /// <inheritdoc/>
    public override void Put(string key, ReadOnlyMemory<byte> content)
    {
        var headers = new Dictionary<string, string> { ["x-ms-blob-type"] = "BlockBlob" };
        StoreResponse response = Send(HttpMethod.Put, BlobPath(key), extraHeaders: headers, body: content);
        if (!response.IsSuccess) throw Failure(response, $"write '{key}'");
    }

    /// <summary>Sends a request asynchronously and reads its whole answer.</summary>
    private async Task<StoreResponse> SendBufferedAsync(
        HttpMethod method, string path, List<KeyValuePair<string, string>> query, Dictionary<string, string>? extraHeaders,
        ReadOnlyMemory<byte>? body, CancellationToken cancellationToken)
    {
        IReadOnlyList<KeyValuePair<string, string>>? sas = _auth is AzureAuth.RenewedSas renewed
            ? await renewed.Pairs.GetAsync(cancellationToken).ConfigureAwait(false) : null;
        string? bearerToken = _auth is AzureAuth.Bearer bearer ? await bearer.Token.GetAsync(cancellationToken).ConfigureAwait(false) : null;
        return await HttpTransport.SendAsync(Request(method, path, query, extraHeaders, body, sas, bearerToken), HttpTransport.Buffer,
            _retry, alsoRetry: null, _http, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    /// <remarks>Blocks need no request to start; the blob appears when its block list is committed.</remarks>
    public override Task<ObjectUpload> StartUploadAsync(string key, bool create, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult<ObjectUpload>(new AzureUpload(this, key, create));
    }

    /// <summary>
    /// A block blob written block by block: every block gets a random id (all of one length, as the service requires),
    /// and committing the block list makes the blob. With create, the commit carries <c>If-None-Match: *</c>.
    /// </summary>
    private sealed class AzureUpload(AzureClient client, string key, bool create) : ObjectUpload(key, create)
    {
        private string Path => client.BlobPath(Key);

        private static string NewBlockId() => Convert.ToBase64String(System.Text.Encoding.ASCII.GetBytes(Guid.NewGuid().ToString("N")));

        public override async Task<UploadedPart> PutPartAsync(int index, ReadOnlyMemory<byte> content, CancellationToken cancellationToken)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            string id = NewBlockId(); // one id for every attempt: a retry rewrites the same block
            StoreResponse response = await client.SendBufferedAsync(HttpMethod.Put, Path, [new("comp", "block"), new("blockid", id)], null, content,
                cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccess) throw Failure(response, $"upload block {index} of '{Key}'");
            return new UploadedPart(index, id);
        }

        public override async Task<bool> CompleteAsync(IReadOnlyList<UploadedPart> parts, ReadOnlyMemory<byte> last, CancellationToken cancellationToken)
        {
            CheckOrder(parts);
            if (!last.IsEmpty) parts = [.. parts, await PutPartAsync(parts.Count, last, cancellationToken).ConfigureAwait(false)];

            // "Latest" takes the uncommitted block, or the committed one of that id: a commit sent again after its
            // answer was lost commits the same blocks again instead of failing.
            var body = new System.Text.StringBuilder("""<?xml version="1.0" encoding="utf-8"?><BlockList>""");
            foreach (UploadedPart part in parts) body.Append("<Latest>").Append(part.Id).Append("</Latest>");
            body.Append("</BlockList>");
            Dictionary<string, string>? headers = Create ? new() { ["if-none-match"] = "*" } : null;
            StoreResponse response = await client.SendBufferedAsync(HttpMethod.Put, Path, [new("comp", "blocklist")], headers,
                System.Text.Encoding.UTF8.GetBytes(body.ToString()), cancellationToken).ConfigureAwait(false);
            if (response.IsSuccess) return true;

            if (Create && response.Status is HttpStatusCode.Conflict or HttpStatusCode.PreconditionFailed)
            {
                // After a retry the blob may be our own commit, whose answer was lost: it is ours when it is made
                // of exactly our blocks (their ids are random).
                return response.Retried && await CommittedAsync(cancellationToken).ConfigureAwait(false) is { } committed
                    && committed.SequenceEqual(parts.Select(static p => p.Id));
            }
            if (Create && response.Status == HttpStatusCode.BadRequest && ErrorCode(response) == "InvalidBlockList"
                && await client.HeadAsync(Key, cancellationToken).ConfigureAwait(false))
            {
                // Another writer committed the blob first, which discards every uncommitted block, ours included.
                return false;
            }
            throw Failure(response, $"commit the blocks of '{Key}'");
        }

        /// <summary>The ids of the blob's committed blocks, or null when there is no blob.</summary>
        private async Task<List<string>?> CommittedAsync(CancellationToken cancellationToken)
        {
            StoreResponse response = await client.SendBufferedAsync(HttpMethod.Get, Path, [new("comp", "blocklist"), new("blocklisttype", "committed")],
                null, null, cancellationToken).ConfigureAwait(false);
            if (response.Status == HttpStatusCode.NotFound) return null;
            if (!response.IsSuccess) throw Failure(response, $"read the blocks of '{Key}'");
            var ids = new List<string>();
            using var reader = XmlReader.Create(new MemoryStream(response.Body), new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit });
            while (!reader.EOF)
            {
                if (reader.NodeType == XmlNodeType.Element && reader.LocalName == "Name") ids.Add(reader.ReadElementContentAsString());
                else reader.Read();
            }
            return ids;
        }

        /// <summary>Uncommitted blocks cannot be deleted; the service drops them after a week, or at the blob's next commit.</summary>
        public override Task AbortAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    /// <summary>Whether a blob exists at the key.</summary>
    private async Task<bool> HeadAsync(string key, CancellationToken cancellationToken)
    {
        StoreResponse response = await SendBufferedAsync(HttpMethod.Head, BlobPath(key), [], null, null, cancellationToken).ConfigureAwait(false);
        if (response.Status == HttpStatusCode.NotFound) return false;
        if (!response.IsSuccess) throw Failure(response, $"read the properties of '{key}'");
        return true;
    }

    /// <inheritdoc/>
    public override void Delete(string key)
    {
        StoreResponse response = Send(HttpMethod.Delete, BlobPath(key));
        if (!response.IsSuccess && response.Status != HttpStatusCode.NotFound) throw Failure(response, $"delete '{key}'");
    }

    /// <inheritdoc/>
    public override IEnumerable<ObjectInfo> List(string prefix, bool delimited, string? startAfter)
    {
        string? marker = null;
        do
        {
            var query = new List<KeyValuePair<string, string>>
            {
                new("restype", "container"),
                new("comp", "list"),
                new("maxresults", PageSize.ToString(CultureInfo.InvariantCulture)),
            };
            if (prefix.Length > 0) query.Add(new("prefix", prefix));
            if (delimited) query.Add(new("delimiter", "/"));
            if (marker is not null) query.Add(new("marker", marker));
            // startFrom is inclusive, and a service that does not know it lists from the start: keys up to the
            // offset are dropped below either way.
            else if (startAfter is not null) query.Add(new("startFrom", startAfter));

            StoreResponse response = Send(HttpMethod.Get, _containerPath, query);
            if (!response.IsSuccess) throw Failure(response, $"list '{prefix}'");
            var (blobs, next) = ParseListing(response.Body);
            foreach (ObjectInfo info in blobs)
                if (startAfter is null || string.CompareOrdinal(info.Key, startAfter) > 0) yield return info;
            marker = next;
        }
        while (marker is not null);
    }

    /// <summary>The files of a List Blobs page (directories left out) and the marker of the next page, if any.</summary>
    internal static (List<ObjectInfo> Blobs, string? NextMarker) ParseListing(byte[] xml)
    {
        var blobs = new List<ObjectInfo>();
        string? next = null;
        using var reader = XmlReader.Create(new MemoryStream(xml), new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit, IgnoreWhitespace = true });
        reader.MoveToContent();
        while (!reader.EOF)
        {
            if (reader.NodeType != XmlNodeType.Element)
            {
                reader.Read();
                continue;
            }
            switch (reader.LocalName)
            {
                case "EnumerationResults" or "Blobs":
                    reader.Read(); // into it
                    break;
                case "Blob":
                    using (XmlReader item = reader.ReadSubtree())
                        if (ReadBlob(item) is { } blob) blobs.Add(blob);
                    reader.Read(); // past </Blob>
                    break;
                case "NextMarker":
                    next = reader.ReadElementContentAsString();
                    break;
                default:
                    reader.Skip();
                    break;
            }
        }
        return (blobs, string.IsNullOrEmpty(next) ? null : next);
    }

    private static ObjectInfo? ReadBlob(XmlReader item)
    {
        string name = "";
        long size = 0;
        DateTimeOffset modified = DateTimeOffset.UnixEpoch;
        bool directory = false;
        item.Read(); // <Blob>
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
                case "Name":
                    // Names that XML cannot carry come percent-encoded.
                    bool encoded = item.GetAttribute("Encoded") == "true";
                    name = item.ReadElementContentAsString();
                    if (encoded) name = Uri.UnescapeDataString(name);
                    break;
                case "Properties":
                    item.Read(); // into it
                    break;
                case "Content-Length":
                    size = long.Parse(item.ReadElementContentAsString(), CultureInfo.InvariantCulture);
                    break;
                case "Last-Modified":
                    modified = DateTimeOffset.Parse(item.ReadElementContentAsString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
                    break;
                case "ResourceType":
                    directory = item.ReadElementContentAsString() == "directory";
                    break;
                default:
                    item.Skip();
                    break;
            }
        }
        return directory ? null : new ObjectInfo(name, size, modified);
    }
}
