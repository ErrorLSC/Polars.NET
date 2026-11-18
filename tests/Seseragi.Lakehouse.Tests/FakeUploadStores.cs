using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Xml.Linq;

namespace Seseragi.Lakehouse.Tests;

/// <summary>
/// An in-memory object store behind a <see cref="FakeHttpServer"/>, playing one store's upload protocol, with injected
/// failures: <see cref="Refuse"/> answers 503 without acting, <see cref="Lose"/> acts and then answers 503 (an answer
/// lost on its way back).
/// </summary>
public abstract class FakeUploadStore : IDisposable
{
    private readonly List<(Func<SeenRequest, bool> Match, bool Act, int Left)> _faults = [];
    protected readonly Lock Gate = new();

    protected FakeUploadStore() => Server = new FakeHttpServer(Handle);

    public FakeHttpServer Server { get; }

    public string Url => Server.Url;

    /// <summary>Objects by key.</summary>
    public Dictionary<string, byte[]> Objects { get; } = new(StringComparer.Ordinal);

    /// <summary>The requests seen, as "METHOD path?query".</summary>
    public IReadOnlyList<string> Seen => [.. Server.Requests.Select(static r => $"{r.Method} {r.PathAndQuery}")];

    /// <summary>Answers the next <paramref name="times"/> requests that <paramref name="match"/> with 503, without acting.</summary>
    public void Refuse(Func<SeenRequest, bool> match, int times = 1)
    {
        lock (Gate) _faults.Add((match, false, times));
    }

    /// <summary>Acts on the next <paramref name="times"/> requests that <paramref name="match"/>, then answers 503.</summary>
    public void Lose(Func<SeenRequest, bool> match, int times = 1)
    {
        lock (Gate) _faults.Add((match, true, times));
    }

    private int _inFlight;

    /// <summary>How long every request waits before it is answered (outside the gate, so requests overlap).</summary>
    public TimeSpan Delay { get; set; }

    /// <summary>The most requests seen in flight at once.</summary>
    public int MostInFlight { get; private set; }

    private FakeAnswer Handle(SeenRequest request)
    {
        int now = Interlocked.Increment(ref _inFlight);
        lock (Gate) MostInFlight = Math.Max(MostInFlight, now);
        try
        {
            if (Delay > TimeSpan.Zero) Thread.Sleep(Delay);
            return HandleInGate(request);
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }

    private FakeAnswer HandleInGate(SeenRequest request)
    {
        lock (Gate)
        {
            int fault = _faults.FindIndex(f => f.Left > 0 && f.Match(request));
            if (fault >= 0)
            {
                var (match, act, left) = _faults[fault];
                _faults[fault] = (match, act, left - 1);
                if (act) Answer(request);
                return new FakeAnswer(503, "<Error><Code>SlowDown</Code></Error>");
            }
            return Answer(request);
        }
    }

    /// <summary>Acts on a request (under the gate) and answers it.</summary>
    protected abstract FakeAnswer Answer(SeenRequest request);

    protected static (string Path, Dictionary<string, string> Query) Split(string pathAndQuery)
    {
        int q = pathAndQuery.IndexOf('?');
        var query = new Dictionary<string, string>(StringComparer.Ordinal);
        if (q >= 0)
        {
            foreach (string pair in pathAndQuery[(q + 1)..].Split('&', StringSplitOptions.RemoveEmptyEntries))
            {
                int eq = pair.IndexOf('=');
                query[Uri.UnescapeDataString(eq < 0 ? pair : pair[..eq])] = eq < 0 ? "" : Uri.UnescapeDataString(pair[(eq + 1)..]);
            }
        }
        return (Uri.UnescapeDataString(q < 0 ? pathAndQuery : pathAndQuery[..q]), query);
    }

    protected static string Md5Tag(byte[] bytes) => "\"" + Convert.ToHexStringLower(MD5.HashData(bytes)) + "\"";

    public void Dispose() => Server.Dispose();
}

/// <summary>
/// S3 multipart uploads (path style, <c>/bucket/key</c>), conditional completion included. After a completion the
/// upload is remembered and answered again (as AWS does) unless <see cref="ForgetCompleted"/> (as MinIO: the
/// condition fails, or 404 NoSuchUpload).
/// </summary>
public sealed class FakeS3 : FakeUploadStore
{
    private sealed record Upload(string Key, SortedDictionary<int, byte[]> Parts);

    private readonly Dictionary<string, Upload> _uploads = new(StringComparer.Ordinal);
    private readonly HashSet<string> _completed = new(StringComparer.Ordinal);

    public bool ForgetCompleted { get; set; }

    /// <summary>Answers completions 200 with an error document this many times (without acting).</summary>
    public int ErrorBodies { get; set; }

    /// <summary>Answers conditional completions with this status (409, 501) when set.</summary>
    public int? ConditionalAnswer { get; set; }

    /// <summary>Uploads not completed or aborted.</summary>
    public int OpenUploads
    {
        get
        {
            lock (Gate) return _uploads.Count;
        }
    }

    public string BucketUrl => Url + "/bucket";

    protected override FakeAnswer Answer(SeenRequest request)
    {
        var (path, query) = Split(request.PathAndQuery);
        string key = path["/bucket/".Length..];
        // A signed body's hash is checked; an unsigned one is taken (UNSIGNED-PAYLOAD, as S3 takes it).
        if (request.Headers.TryGetValue("x-amz-content-sha256", out string? hash) && hash != "UNSIGNED-PAYLOAD"
            && hash != Convert.ToHexStringLower(SHA256.HashData(request.Content)))
            return new FakeAnswer(400, "<Error><Code>XAmzContentSHA256Mismatch</Code></Error>");

        switch (request.Method)
        {
            case "POST" when query.ContainsKey("uploads"):
            {
                string id = Guid.NewGuid().ToString("N");
                _uploads[id] = new Upload(key, []);
                return new FakeAnswer(200, $"<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>{key}</Key><UploadId>{id}</UploadId></InitiateMultipartUploadResult>");
            }
            case "PUT" when query.TryGetValue("uploadId", out string? id):
            {
                if (!_uploads.TryGetValue(id, out Upload? upload)) return NoSuchUpload();
                upload.Parts[int.Parse(query["partNumber"], CultureInfo.InvariantCulture)] = request.Content;
                return new FakeAnswer(200, "", [$"ETag: {Md5Tag(request.Content)}"]);
            }
            case "POST" when query.TryGetValue("uploadId", out string? id):
            {
                bool conditional = request.Headers.TryGetValue("If-None-Match", out string? match) && match == "*";
                if (conditional && ConditionalAnswer is { } status) return new FakeAnswer(status, "<Error><Code>Conditional</Code></Error>");
                if (ErrorBodies > 0)
                {
                    ErrorBodies--;
                    return new FakeAnswer(200, "<Error><Code>InternalError</Code></Error>");
                }
                if (conditional && Objects.ContainsKey(key)) return new FakeAnswer(412, "<Error><Code>PreconditionFailed</Code></Error>");
                if (!_uploads.TryGetValue(id, out Upload? upload))
                    return _completed.Contains(id) && !ForgetCompleted ? Completed(key) : NoSuchUpload();
                var named = XDocument.Parse(request.Body).Descendants("Part")
                    .Select(p => (Number: int.Parse(p.Element("PartNumber")!.Value, CultureInfo.InvariantCulture), ETag: p.Element("ETag")!.Value))
                    .ToList();
                if (named.Count == 0 || named.Select(p => p.Number).SequenceEqual(Enumerable.Range(1, named.Count)) is false
                    || named.Any(p => !upload.Parts.TryGetValue(p.Number, out byte[]? bytes) || Md5Tag(bytes) != p.ETag))
                    return new FakeAnswer(400, "<Error><Code>InvalidPart</Code></Error>");
                Objects[key] = [.. named.SelectMany(p => upload.Parts[p.Number])];
                _uploads.Remove(id);
                _completed.Add(id);
                return Completed(key);
            }
            case "GET" when query.TryGetValue("uploadId", out string? id):
                return _uploads.ContainsKey(id) ? new FakeAnswer(200, "<ListPartsResult />") : NoSuchUpload();
            case "DELETE" when query.TryGetValue("uploadId", out string? id):
                return _uploads.Remove(id) ? new FakeAnswer(204) : NoSuchUpload();
            case "PUT":
                if (request.Headers.TryGetValue("If-None-Match", out string? none) && none == "*" && Objects.ContainsKey(key))
                    return new FakeAnswer(412, "<Error><Code>PreconditionFailed</Code></Error>");
                Objects[key] = request.Content;
                return new FakeAnswer(200, "", [$"ETag: {Md5Tag(request.Content)}"]);
            case "HEAD" or "GET":
                return Objects.TryGetValue(key, out byte[]? content)
                    ? new FakeAnswer(200, Encoding.UTF8.GetString(content))
                    : new FakeAnswer(404, "<Error><Code>NoSuchKey</Code></Error>");
            default:
                return new FakeAnswer(405);
        }

        static FakeAnswer NoSuchUpload() => new(404, "<Error><Code>NoSuchUpload</Code></Error>");
        static FakeAnswer Completed(string key) => new(200, $"<CompleteMultipartUploadResult><Key>{key}</Key><ETag>\"x-1\"</ETag></CompleteMultipartUploadResult>");
    }
}

/// <summary>
/// Azure block blobs: blocks staged per blob, committed by a block list (Latest: the uncommitted block, else the
/// committed one); a commit or a plain upload discards every uncommitted block of the blob, as the service does.
/// </summary>
public sealed class FakeAzureBlocks : FakeUploadStore
{
    private readonly Dictionary<string, Dictionary<string, byte[]>> _uncommitted = new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<(string Id, byte[] Bytes)>> _committed = new(StringComparer.Ordinal);

    /// <summary>The block ids each blob's commits named, in order.</summary>
    public List<string> BlockIdsSeen { get; } = [];

    /// <summary>Another writer's whole-blob upload: the blob replaced, uncommitted blocks dropped.</summary>
    public void PutByOther(string key, byte[] content)
    {
        lock (Gate)
        {
            Objects[key] = content;
            _uncommitted.Remove(key);
            _committed[key] = [];
        }
    }

    protected override FakeAnswer Answer(SeenRequest request)
    {
        var (path, query) = Split(request.PathAndQuery);
        string key = path[(path.StartsWith("/acct/", StringComparison.Ordinal) ? "/acct/c/" : "/c/").Length..];
        query.TryGetValue("comp", out string? comp);
        bool exists = Objects.ContainsKey(key);
        switch (request.Method)
        {
            case "PUT" when comp == "block":
            {
                string id = query["blockid"];
                BlockIdsSeen.Add(id);
                if (!_uncommitted.TryGetValue(key, out var blocks)) _uncommitted[key] = blocks = new(StringComparer.Ordinal);
                blocks[id] = request.Content;
                return new FakeAnswer(201);
            }
            case "PUT" when comp == "blocklist":
            {
                if (request.Headers.TryGetValue("If-None-Match", out string? match) && match == "*" && exists)
                    return new FakeAnswer(409, "", ["x-ms-error-code: BlobAlreadyExists"]);
                var ids = XDocument.Parse(request.Body).Root!.Elements().Select(e => (Kind: e.Name.LocalName, Id: e.Value)).ToList();
                List<(string Id, byte[] Bytes)> chosen = [];
                foreach (var (kind, id) in ids)
                {
                    byte[]? bytes = kind == "Latest" && _uncommitted.GetValueOrDefault(key)?.GetValueOrDefault(id) is { } staged ? staged
                        : _committed.GetValueOrDefault(key)?.FirstOrDefault(b => b.Id == id).Bytes;
                    if (bytes is null) return new FakeAnswer(400, "", ["x-ms-error-code: InvalidBlockList"]);
                    chosen.Add((id, bytes));
                }
                Objects[key] = [.. chosen.SelectMany(b => b.Bytes)];
                _committed[key] = chosen;
                _uncommitted.Remove(key);
                return new FakeAnswer(201);
            }
            case "GET" when comp == "blocklist":
            {
                if (!exists) return new FakeAnswer(404, "", ["x-ms-error-code: BlobNotFound"]);
                string blocks = string.Concat(_committed.GetValueOrDefault(key, []).Select(b => $"<Block><Name>{b.Id}</Name><Size>{b.Bytes.Length}</Size></Block>"));
                return new FakeAnswer(200, $"""<?xml version="1.0" encoding="utf-8"?><BlockList><CommittedBlocks>{blocks}</CommittedBlocks><UncommittedBlocks /></BlockList>""");
            }
            case "PUT":
                PutByOther(key, request.Content);
                return new FakeAnswer(201);
            case "HEAD" or "GET":
                return exists ? new FakeAnswer(200, Encoding.UTF8.GetString(Objects[key])) : new FakeAnswer(404, "", ["x-ms-error-code: BlobNotFound"]);
            default:
                return new FakeAnswer(405);
        }
    }
}

/// <summary>
/// Google Cloud Storage: XML multipart uploads (<c>/bucket/key</c>), JSON resumable uploads (sessions at
/// <c>/upload/session/id</c>, chunks in order, 256 KiB multiples but the last, <c>ifGenerationMatch=0</c> checked at
/// the end) and JSON media uploads.
/// </summary>
public sealed class FakeGcs : FakeUploadStore
{
    private sealed record Upload(string Key, SortedDictionary<int, byte[]> Parts);

    private sealed class Session(string key, bool ifAbsent)
    {
        public string Key { get; } = key;
        public bool IfAbsent { get; } = ifAbsent;
        public MemoryStream Bytes { get; } = new();
        public bool Finished { get; set; }
        public bool Cancelled { get; set; }
    }

    private readonly Dictionary<string, Upload> _uploads = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Session> _sessions = new(StringComparer.Ordinal);

    /// <summary>Answers finishing requests 308 (kept everything, not finished), as a broken service might.</summary>
    public bool NeverFinish { get; set; }

    /// <summary>Keeps at most this many bytes of the next chunks (one entry per chunk), as a service may.</summary>
    public Queue<int> KeepAtMost { get; } = new();

    public int OpenUploads
    {
        get
        {
            lock (Gate) return _uploads.Count + _sessions.Values.Count(s => !s.Finished && !s.Cancelled);
        }
    }

    protected override FakeAnswer Answer(SeenRequest request)
    {
        var (path, query) = Split(request.PathAndQuery);
        if (path.StartsWith("/upload/session/", StringComparison.Ordinal)) return SessionAnswer(_sessions[path["/upload/session/".Length..]], request);
        if (path == "/upload/storage/v1/b/bucket/o" && request.Method == "POST")
        {
            string name = query["name"];
            bool ifAbsent = query.GetValueOrDefault("ifGenerationMatch") == "0";
            if (query["uploadType"] == "resumable")
            {
                string id = Guid.NewGuid().ToString("N");
                _sessions[id] = new Session(name, ifAbsent);
                return new FakeAnswer(200, "", [$"Location: {Url}/upload/session/{id}"]);
            }
            if (ifAbsent && Objects.ContainsKey(name)) return new FakeAnswer(412, """{"error":{"code":412}}""");
            Objects[name] = request.Content;
            return new FakeAnswer(200, "{}");
        }

        string key = path["/bucket/".Length..];
        switch (request.Method)
        {
            case "POST" when query.ContainsKey("uploads"):
            {
                if (request.Headers.Keys.Any(h => h.StartsWith("x-goog-if-", StringComparison.OrdinalIgnoreCase)))
                    return new FakeAnswer(400, "<Error><Code>NotImplemented</Code></Error>");
                string id = Guid.NewGuid().ToString("N");
                _uploads[id] = new Upload(key, []);
                return new FakeAnswer(200, $"<InitiateMultipartUploadResult><UploadId>{id}</UploadId></InitiateMultipartUploadResult>");
            }
            case "PUT" when query.TryGetValue("uploadId", out string? id):
                if (!_uploads.TryGetValue(id, out Upload? upload)) return NoSuchUpload();
                upload.Parts[int.Parse(query["partNumber"], CultureInfo.InvariantCulture)] = request.Content;
                return new FakeAnswer(200, "", [$"ETag: {Md5Tag(request.Content)}"]);
            case "POST" when query.TryGetValue("uploadId", out string? id):
            {
                if (!_uploads.TryGetValue(id, out Upload? open))
                    return Objects.ContainsKey(key) ? new FakeAnswer(200, "<CompleteMultipartUploadResult />") : NoSuchUpload();
                var named = XDocument.Parse(request.Body).Descendants("Part")
                    .Select(p => (Number: int.Parse(p.Element("PartNumber")!.Value, CultureInfo.InvariantCulture), ETag: p.Element("ETag")!.Value))
                    .ToList();
                if (named.Count == 0) return new FakeAnswer(400, "<Error><Code>MalformedCompleteMultipartUploadRequest</Code></Error>");
                if (named.Any(p => !open.Parts.TryGetValue(p.Number, out byte[]? bytes) || Md5Tag(bytes) != p.ETag))
                    return new FakeAnswer(400, "<Error><Code>InvalidPart</Code></Error>");
                Objects[key] = [.. named.SelectMany(p => open.Parts[p.Number])];
                _uploads.Remove(id); // a completion sent again is still answered 200 (above), as the service does
                return new FakeAnswer(200, "<CompleteMultipartUploadResult />");
            }
            case "DELETE" when query.TryGetValue("uploadId", out string? id):
                return _uploads.Remove(id) ? new FakeAnswer(204) : NoSuchUpload();
            default:
                return new FakeAnswer(405);
        }

        static FakeAnswer NoSuchUpload() => new(404, "<Error><Code>NoSuchUpload</Code></Error>");
    }

    private FakeAnswer SessionAnswer(Session session, SeenRequest request)
    {
        if (request.Method == "DELETE")
        {
            session.Cancelled = true;
            return new FakeAnswer(499, """{"error":{"code":499,"message":"clientClosedRequest"}}""");
        }
        if (session.Cancelled) return new FakeAnswer(404);
        if (session.Finished) return Finished();

        // Content-Range: bytes first-last/total | bytes */total, total "*" when unknown.
        string range = request.Headers["Content-Range"]["bytes ".Length..];
        int slash = range.IndexOf('/');
        long? total = range[(slash + 1)..] == "*" ? null : long.Parse(range[(slash + 1)..], CultureInfo.InvariantCulture);
        byte[] content = request.Content;
        if (range[..slash] != "*")
        {
            long first = long.Parse(range[..range.IndexOf('-')], CultureInfo.InvariantCulture);
            if (first > session.Bytes.Length) return new FakeAnswer(400, "gap");
            if (total is null && content.Length % (256 * 1024) != 0) return new FakeAnswer(400, "not a multiple of 262144");
            int skip = (int)(session.Bytes.Length - first); // bytes kept already, sent again
            int keep = Math.Min(content.Length - skip, KeepAtMost.Count > 0 ? KeepAtMost.Dequeue() : int.MaxValue);
            session.Bytes.Write(content, skip, Math.Max(0, keep));
        }
        if (total is { } size && session.Bytes.Length == size && !NeverFinish)
        {
            if (session.IfAbsent && Objects.ContainsKey(session.Key)) return new FakeAnswer(412, """{"error":{"code":412}}""");
            Objects[session.Key] = session.Bytes.ToArray();
            session.Finished = true;
            return Finished();
        }
        return session.Bytes.Length == 0 ? new FakeAnswer(308) : new FakeAnswer(308, "", [$"Range: bytes=0-{session.Bytes.Length - 1}"]);

        FakeAnswer Finished() => new(200, $$"""{"name":"{{session.Key}}"}""");
    }
}
