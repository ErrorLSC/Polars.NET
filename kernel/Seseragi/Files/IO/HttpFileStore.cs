using System.Buffers;
using System.Net;
using System.Net.Http.Headers;
using Kaminari.Sazanami.Network;

namespace Kaminari.Seseragi.Files.IO;

/// <summary>Options of an <see cref="HttpFileStore"/>.</summary>
public sealed record HttpFileStoreOptions
{
    /// <summary>How failed requests are retried (network failures, 408, 429 and 5xx answers).</summary>
    public RetrySettings Retry { get; init; } = RetrySettings.Default;

    /// <summary>Headers sent with every request (an authorization, for instance).</summary>
    public IReadOnlyDictionary<string, string>? Headers { get; init; }

    /// <summary>
    /// The largest file kept whole in memory when its server ignores ranged requests (answers 200 with the whole
    /// content): such a file is read once; a larger one is an error.
    /// </summary>
    public long WholeFileLimit { get; init; } = 64L << 20;
}

/// <summary>
/// Files served over HTTP(S), named by their URLs and read by ranged GET requests. Nothing is listed (a plain server
/// has no listing: globs and directories are refused). Opening a file of unknown size asks for its last bytes, which
/// gives its size and its footer in one request; no HEAD request is ever sent (presigned URLs are signed for GET only).
/// </summary>
/// <remarks>
/// <para>Answers must not be compressed (<c>Accept-Encoding: identity</c> is sent; a compressed answer is an error).</para>
/// <para>
/// The version of a file seen first (its strong <c>ETag</c>, else its <c>Last-Modified</c>) is required of every later
/// read (<c>If-Match</c>, else <c>If-Unmodified-Since</c>): a file that changes while it is read fails with
/// <see cref="IOException"/> rather than mixing two versions. A server giving neither cannot be checked.
/// </para>
/// <para>
/// Without a client given, a shared one is used: no automatic decompression, connections pooled for five minutes, a
/// connect timeout of 30 seconds and five minutes at most per request (each retried as the retry settings say).
/// </para>
/// </remarks>
public sealed class HttpFileStore(HttpClient? client = null, HttpFileStoreOptions? options = null) : IFileStore
{
    /// <summary>How many of a file's last bytes the first read of a file of unknown size asks for (its footer, usually).</summary>
    internal const int TailRequest = 64 * 1024;

    private static readonly Lazy<HttpClient> SharedClient = new(() => new HttpClient(new SocketsHttpHandler
    {
        AutomaticDecompression = DecompressionMethods.None,
        PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        ConnectTimeout = TimeSpan.FromSeconds(30),
    })
    {
        Timeout = TimeSpan.FromMinutes(5),
    });

    /// <summary>The shared client (stores given none use it).</summary>
    internal static HttpClient Shared => SharedClient.Value;

    private readonly HttpSender _sender = new(client ?? SharedClient.Value, (options ?? new HttpFileStoreOptions()).Retry);
    private readonly HttpFileStoreOptions _options = options ?? new HttpFileStoreOptions();

    /// <inheritdoc/>
    public bool CanList => false;

    /// <inheritdoc/>
    public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException("An HTTP server cannot list its files: name each file by its URL.");

    /// <summary>The file at <paramref name="path"/> (a URL), learned from a request for its last byte; null when there is none.</summary>
    public async ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        return await _sender.SendAsync(() => Request(path, "bytes=-1", null), async (response, _, ct) =>
        {
            if (response.StatusCode == HttpStatusCode.NotFound) return (StoredFile?)null;
            Check(response, path);
            await response.Content.LoadIntoBufferAsync().ConfigureAwait(false);
            long size = response.StatusCode == HttpStatusCode.PartialContent
                ? Total(response, path)
                : response.StatusCode == HttpStatusCode.RequestedRangeNotSatisfiable ? 0
                : response.Content.Headers.ContentLength ?? throw new HttpAnswerException($"{path}: the server tells no size.");
            var (etag, lastModified) = Validators(response);
            return new StoredFile(path, size, lastModified, etag);
        }, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Opens a file whose size is known: no request is sent until it is read.</summary>
    public ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(file.Path);
        return ValueTask.FromResult<IRandomAccess>(new HttpSource(this, file.Path, file.Size, StrongTag(file.ETag), file.LastModified, null));
    }

    /// <summary>
    /// Opens the file at <paramref name="path"/> (a URL): one request for its last bytes; null when there is none. A
    /// server that cannot answer such a suffix range (Azure Blob Storage, for one) is asked for explicit ranges instead
    /// (the first bytes, telling the size; then the last ones), and remembered by host.
    /// </summary>
    public async ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        string host = Host(path);
        if (!_noSuffix.ContainsKey(host))
        {
            Opened opened = await _sender.SendAsync(() => Request(path, $"bytes=-{TailRequest}", null), async (response, _, ct) =>
            {
                switch (response.StatusCode)
                {
                    case HttpStatusCode.NotFound:
                        return new Opened(null, Found: false);
                    case HttpStatusCode.RequestedRangeNotSatisfiable when response.Content.Headers.ContentRange is { Length: 0 }:
                        return new Opened(new MemoryRandomAccess(ReadOnlyMemory<byte>.Empty)); // an empty file
                    case HttpStatusCode.OK when response.Content.Headers.ContentLength is <= TailRequest:
                        return new Opened(new MemoryRandomAccess(await Whole(response, path, ct).ConfigureAwait(false))); // small: whole
                    case HttpStatusCode.PartialContent:
                        return new Opened(await Tail(response, path, ct).ConfigureAwait(false));
                    case HttpStatusCode.PreconditionFailed:
                        Check(response, path);
                        break;
                }
                if (response.Content.Headers.ContentEncoding.Count > 0) Check(response, path);
                return new Opened(null); // no suffix range here: explicit ranges (the answer's body is not read)
            }, cancellationToken: cancellationToken, retryOnly: Throttled).ConfigureAwait(false);
            if (opened.Source is not null || !opened.Found) return opened.Source;
        }
        // The first bytes (a small file whole), with the size; then the last ones.
        Opened head = await _sender.SendAsync(() => Request(path, $"bytes=0-{TailRequest - 1}", null), async (response, _, ct) =>
        {
            switch (response.StatusCode)
            {
                case HttpStatusCode.NotFound:
                    return new Opened(null, Found: false);
                case HttpStatusCode.RequestedRangeNotSatisfiable when response.Content.Headers.ContentRange is { Length: 0 }:
                    return new Opened(new MemoryRandomAccess(ReadOnlyMemory<byte>.Empty));
                case HttpStatusCode.OK:
                    return new Opened(new MemoryRandomAccess(await Whole(response, path, ct).ConfigureAwait(false)));
            }
            Check(response, path);
            _noSuffix[host] = true; // explicit ranges answered where the suffix was not: go there at once from now on
            if (Total(response, path) > TailRequest) return new Opened(null, Size: Total(response, path));
            return new Opened(await Tail(response, path, ct).ConfigureAwait(false));
        }, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (head.Source is not null || !head.Found) return head.Source;
        long size = head.Size;
        return await _sender.SendAsync(() => Request(path, $"bytes={size - TailRequest}-{size - 1}", null), async (response, _, ct) =>
        {
            if (response.StatusCode == HttpStatusCode.NotFound) throw new FileNotFoundException($"'{path}' no longer exists.", path);
            if (response.StatusCode == HttpStatusCode.OK) return (IRandomAccess)new MemoryRandomAccess(await Whole(response, path, ct).ConfigureAwait(false));
            Check(response, path);
            return await Tail(response, path, ct).ConfigureAwait(false);
        }, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The hosts found not to answer suffix ranges.</summary>
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, bool> _noSuffix = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>A file opened (null when it is missing, or when the way asked did not give it, with its size if learned).</summary>
    private readonly record struct Opened(IRandomAccess? Source, bool Found = true, long Size = -1);

    /// <summary>The answers retried for a request with another way out: throttling only.</summary>
    private static bool Throttled(HttpStatusCode status) => status is HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests;

    private static string Host(string url) => Uri.TryCreate(url, UriKind.Absolute, out Uri? uri) ? uri.Authority : url;

    /// <summary>A source of known size holding the range a partial answer gave (the file's end, usually).</summary>
    private async Task<IRandomAccess> Tail(HttpResponseMessage response, string path, CancellationToken ct)
    {
        long size = Total(response, path);
        var (etag, lastModified) = Validators(response);
        ContentRangeHeaderValue range = response.Content.Headers.ContentRange!;
        if (range.From is not { } from || range.To is not { } to || to < from || to >= size)
            throw new HttpAnswerException($"{path}: the server answered an impossible range ({range}).");
        byte[] held = new byte[(int)(to - from + 1)];
        await ReadExactlyAsync(response, held, path, ct).ConfigureAwait(false);
        return new HttpSource(this, path, size, etag, lastModified, new HeldBytes(from, held));
    }

    /// <summary>A GET of <paramref name="url"/>, ranged, with the store's headers and the version required.</summary>
    private HttpRequestMessage Request(string url, string range, HttpSource? source)
    {
        var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.TryAddWithoutValidation("Range", range);
        request.Headers.AcceptEncoding.Add(new StringWithQualityHeaderValue("identity"));
        if (_options.Headers is { } headers)
        {
            foreach (var (name, value) in headers) request.Headers.TryAddWithoutValidation(name, value);
        }
        if (source?.ETag is { } etag) request.Headers.TryAddWithoutValidation("If-Match", etag);
        else if (source?.LastModified is { } date) request.Headers.IfUnmodifiedSince = date;
        return request;
    }

    /// <summary>Fails on an answer that is not the content asked for.</summary>
    private static void Check(HttpResponseMessage response, string path)
    {
        if (response.Content.Headers.ContentEncoding.Any(e => !e.Equals("identity", StringComparison.OrdinalIgnoreCase)))
            throw new HttpAnswerException($"{path}: the server answered compressed ({string.Join(", ", response.Content.Headers.ContentEncoding)}), though asked not to.");
        if (response.StatusCode == HttpStatusCode.PreconditionFailed)
            throw new HttpAnswerException($"{path}: the file changed while it was read (precondition failed).");
        if (response.StatusCode is not (HttpStatusCode.OK or HttpStatusCode.PartialContent or HttpStatusCode.RequestedRangeNotSatisfiable))
            throw new HttpAnswerException($"{path}: HTTP {(int)response.StatusCode} {response.ReasonPhrase}.");
    }

    /// <summary>The whole size from a partial answer's <c>Content-Range</c>.</summary>
    private static long Total(HttpResponseMessage response, string path) =>
        response.Content.Headers.ContentRange is { HasRange: true, Length: { } total }
            ? total
            : throw new HttpAnswerException($"{path}: the server answered a range without its total size.");

    /// <summary>The answer's version: its strong ETag, and its Last-Modified.</summary>
    private static (string? ETag, DateTimeOffset? LastModified) Validators(HttpResponseMessage response) =>
        (StrongTag(response.Headers.ETag?.ToString()), response.Content.Headers.LastModified);

    /// <summary>A strong entity tag (weak ones cannot be required of ranges).</summary>
    private static string? StrongTag(string? etag) => etag is { Length: > 0 } && !etag.StartsWith("W/", StringComparison.Ordinal) ? etag : null;

    /// <summary>The whole content of an answer that ignored the range, when within <see cref="HttpFileStoreOptions.WholeFileLimit"/>.</summary>
    private async Task<byte[]> Whole(HttpResponseMessage response, string path, CancellationToken ct)
    {
        Check(response, path);
        long length = response.Content.Headers.ContentLength
            ?? throw new HttpAnswerException($"{path}: the server ignores ranged requests and tells no size.");
        if (length > _options.WholeFileLimit)
            throw new HttpAnswerException($"{path}: the server ignores ranged requests, and the file ({length} bytes) is larger than HttpFileStoreOptions.WholeFileLimit ({_options.WholeFileLimit}).");
        byte[] whole = new byte[length];
        await ReadExactlyAsync(response, whole, path, ct).ConfigureAwait(false);
        return whole;
    }

    private static async Task ReadExactlyAsync(HttpResponseMessage response, Memory<byte> destination, string path, CancellationToken ct)
    {
        Stream body = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
        await using (body.ConfigureAwait(false))
        {
            // A short body is a network failure (the connection ended early): the request is sent again.
            await body.ReadExactlyAsync(destination, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A file of the store read by ranges: the tail read when it was opened is served from memory; a server ignoring
    /// ranges gives the whole file, kept (within the limit).
    /// </summary>
    private sealed class HttpSource(HttpFileStore store, string url, long length, string? etag, DateTimeOffset? lastModified, HeldBytes? tail)
        : IRandomAccess, IStoredSource
    {
        private HeldBytes? _held = tail;

        public long Length => length;

        /// <summary>The version required of reads: given, or that of the first answer.</summary>
        public string? ETag { get; private set; } = etag;

        public DateTimeOffset? LastModified { get; private set; } = lastModified;

        public StoredFile Stored => new(url, length, LastModified, ETag);

        public int ReadAt(long offset, Span<byte> destination)
        {
            int count = (int)Math.Clamp(length - offset, 0, destination.Length);
            if (count == 0 || Held(offset, destination[..count])) return count;
            byte[] rented = ArrayPool<byte>.Shared.Rent(count);
            try
            {
                int read = Fetch(offset, rented.AsMemory(0, count), CancellationToken.None, sync: true).GetAwaiter().GetResult();
                rented.AsSpan(0, read).CopyTo(destination);
                return read;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        public async ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
        {
            int count = (int)Math.Clamp(length - offset, 0, destination.Length);
            if (count == 0 || Held(offset, destination.Span[..count])) return count;
            return await Fetch(offset, destination[..count], cancellationToken, sync: false).ConfigureAwait(false);
        }

        private bool Held(long offset, Span<byte> destination)
        {
            if (Volatile.Read(ref _held) is not var (start, bytes) || offset < start || offset + destination.Length > start + bytes.Length) return false;
            bytes.AsSpan((int)(offset - start), destination.Length).CopyTo(destination);
            return true;
        }

        private Task<int> Fetch(long offset, Memory<byte> destination, CancellationToken ct, bool sync)
        {
            string range = $"bytes={offset}-{offset + destination.Length - 1}";
            if (sync) return Task.FromResult(store._sender.Send(() => store.Request(url, range, this), (response, _) => Answer(response, offset, destination, default).GetAwaiter().GetResult()));
            return store._sender.SendAsync(() => store.Request(url, range, this), (response, _, token) => Answer(response, offset, destination, token), cancellationToken: ct);
        }

        private async Task<int> Answer(HttpResponseMessage response, long offset, Memory<byte> destination, CancellationToken ct)
        {
            if (response.StatusCode == HttpStatusCode.NotFound) throw new FileNotFoundException($"'{url}' no longer exists.", url);
            if (response.StatusCode == HttpStatusCode.OK)
            {
                // The range was ignored: the whole file, kept for the reads to come.
                byte[] whole = await store.Whole(response, url, ct).ConfigureAwait(false);
                if (whole.Length != length) throw new HttpAnswerException($"{url}: the file changed while it was read ({whole.Length} bytes, {length} before).");
                Volatile.Write(ref _held, new HeldBytes(0, whole));
                whole.AsSpan((int)offset, destination.Length).CopyTo(destination.Span);
                return destination.Length;
            }
            Check(response, url);
            // A server not checking the version asked for is checked here: an answer of another version is a change.
            if (ETag is { } required && StrongTag(response.Headers.ETag?.ToString()) is { } answered && answered != required)
                throw new HttpAnswerException($"{url}: the file changed while it was read (precondition failed).");
            if (response.Content.Headers.ContentRange is not { From: { } from } || from != offset)
                throw new HttpAnswerException($"{url}: the server answered another range than bytes {offset}-.");
            if (ETag is null && LastModified is null) (ETag, LastModified) = Validators(response);
            await ReadExactlyAsync(response, destination, url, ct).ConfigureAwait(false);
            return destination.Length;
        }
    }
}

/// <summary>Bytes of a file held in memory, from <paramref name="Start"/>.</summary>
internal sealed record HeldBytes(long Start, byte[] Bytes);

/// <summary>A source that tells the file it reads, with the version it was opened at (to open it again without asking).</summary>
internal interface IStoredSource
{
    StoredFile Stored { get; }
}
