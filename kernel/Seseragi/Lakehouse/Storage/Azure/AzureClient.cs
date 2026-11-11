using System.Globalization;
using System.Net;
using System.Xml;
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
    }

    /// <summary>Blobs per listing page; small values let tests cross page boundaries.</summary>
    internal int PageSize { get; set; } = 5000;

    private string BlobPath(string key) => _containerPath + "/" + PercentEncoding.Encode(key, encodeSlash: false);

    private StoreResponse Send(
        HttpMethod method, string path, List<KeyValuePair<string, string>>? query = null,
        Dictionary<string, string>? extraHeaders = null, ReadOnlyMemory<byte>? body = null)
    {
        query ??= [];
        IEnumerable<KeyValuePair<string, string>> sent = _auth is AzureAuth.Sas sas ? query.Concat(sas.Pairs) : query;
        string queryText = string.Join('&', sent.Select(p =>
            PercentEncoding.Encode(p.Key, encodeSlash: true) + "=" + PercentEncoding.Encode(p.Value, encodeSlash: true)));
        string url = _origin + path + (queryText.Length == 0 ? "" : "?" + queryText);

        return HttpTransport.Send(() =>
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
                case AzureAuth.Bearer bearer:
                    request.Headers.TryAddWithoutValidation("Authorization", "Bearer " + bearer.Token);
                    break;
            }
            if (body is { } bytes) request.Content = new ReadOnlyMemoryContent(bytes);
            return request;
        }, _retry);
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
    public override byte[]? GetRange(string key, long offset, int length)
    {
        var range = new Dictionary<string, string> { ["range"] = $"bytes={offset}-{offset + length - 1}" };
        StoreResponse response = Send(HttpMethod.Get, BlobPath(key), extraHeaders: range);
        switch (response.Status)
        {
            case HttpStatusCode.NotFound:
                return null;
            case HttpStatusCode.RequestedRangeNotSatisfiable:
                return []; // the blob ends before the offset
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
