using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Text;

namespace Seseragi.Files.Tests;

/// <summary>
/// An HTTP server of files for tests, answering ranged GETs as object stores do (<c>bytes=a-b</c>, <c>bytes=-n</c>,
/// 206 with Content-Range, 416 past the end, 412 when <c>If-Match</c> / <c>If-Unmodified-Since</c> fail), with switches
/// for servers that ignore ranges, give no or weak validators, compress, or fail. Usable as a message handler (no
/// sockets) or on a local port (<see cref="Listen"/>). Counts requests per path.
/// </summary>
internal sealed class HttpTestServer : HttpMessageHandler
{
    private readonly ConcurrentDictionary<string, (byte[] Bytes, int Version)> _files = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Func<HttpRequestMessage, HttpResponseMessage>> _routes = new(StringComparer.Ordinal);
    private int _requests, _version;

    public bool IgnoreRanges { get; set; }

    /// <summary>How suffix ranges (<c>bytes=-n</c>) are answered when not supported: "500" (as Azurite), "ignore" (the
    /// whole file, 200), "416"; null when supported.</summary>
    public string? NoSuffix { get; set; }

    /// <summary>What validators answers carry: "strong" ETags (and Last-Modified), "weak" ETags, "date" only, or "none".</summary>
    public string Validators { get; set; } = "strong";

    /// <summary>Answers compressed (a Content-Encoding the client did not ask for).</summary>
    public bool Compress { get; set; }

    /// <summary>The number of answers 503 still to give before answering.</summary>
    public int FailNext;

    public int Requests => Volatile.Read(ref _requests);

    public ConcurrentQueue<HttpRequestMessage> Seen { get; } = new();

    public void Put(string path, byte[] bytes) => _files[path] = (bytes, Interlocked.Increment(ref _version));

    /// <summary>Answers requests to <paramref name="pathAndQuery"/> (a path with its query) with <paramref name="answer"/>.</summary>
    public void Route(string pathAndQuery, Func<HttpRequestMessage, HttpResponseMessage> answer) => _routes[pathAndQuery] = answer;

    private static readonly DateTimeOffset Epoch = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    protected override HttpResponseMessage Send(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _requests);
        Seen.Enqueue(request);
        if (Volatile.Read(ref FailNext) > 0 && Interlocked.Decrement(ref FailNext) >= 0)
            return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable) { Content = new ByteArrayContent([]) };
        if (_routes.TryGetValue(request.RequestUri!.PathAndQuery, out var route)) return route(request);
        if (request.Method != HttpMethod.Get) return new HttpResponseMessage(HttpStatusCode.MethodNotAllowed) { Content = new ByteArrayContent([]) };
        if (!_files.TryGetValue(Uri.UnescapeDataString(request.RequestUri.AbsolutePath), out var file))
            return new HttpResponseMessage(HttpStatusCode.NotFound) { Content = new ByteArrayContent([]) };
        var (bytes, version) = file;
        string etag = $"\"v{version}\"";
        DateTimeOffset modified = Epoch.AddSeconds(version);
        if (request.Headers.TryGetValues("If-Match", out var match) && Validators is "strong" && !match.Contains(etag))
            return Answer(HttpStatusCode.PreconditionFailed, [], null);
        if (request.Headers.IfUnmodifiedSince is { } since && Validators is "strong" or "date" && modified > since)
            return Answer(HttpStatusCode.PreconditionFailed, [], null);
        HttpResponseMessage response;
        if (IgnoreRanges || !request.Headers.TryGetValues("Range", out var ranges)) response = Answer(HttpStatusCode.OK, bytes, null);
        else
        {
            string range = ranges.Single()["bytes=".Length..];
            long start, end;
            if (range.StartsWith('-') && NoSuffix is { } refusal)
            {
                return refusal switch
                {
                    "500" => new HttpResponseMessage(HttpStatusCode.InternalServerError) { Content = new StringContent("suffix ranges are not supported") },
                    "416" => Answer(HttpStatusCode.RequestedRangeNotSatisfiable, [], new ContentRangeHeaderValue(bytes.Length)),
                    _ => Stamp(Answer(HttpStatusCode.OK, bytes, null), etag, modified),
                };
            }
            if (range.StartsWith('-'))
            {
                long n = long.Parse(range[1..]);
                if (bytes.Length == 0)
                {
                    response = Answer(HttpStatusCode.RequestedRangeNotSatisfiable, [], new ContentRangeHeaderValue(0));
                    return Stamp(response, etag, modified);
                }
                start = Math.Max(0, bytes.Length - n);
                end = bytes.Length - 1;
            }
            else
            {
                string[] parts = range.Split('-');
                start = long.Parse(parts[0]);
                end = Math.Min(long.Parse(parts[1]), bytes.Length - 1);
                if (start >= bytes.Length)
                    return Stamp(Answer(HttpStatusCode.RequestedRangeNotSatisfiable, [], new ContentRangeHeaderValue(bytes.Length)), etag, modified);
            }
            response = Answer(HttpStatusCode.PartialContent, bytes[(int)start..(int)(end + 1)], new ContentRangeHeaderValue(start, end, bytes.Length));
        }
        return Stamp(response, etag, modified);
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        Task.FromResult(Send(request, cancellationToken));

    private HttpResponseMessage Answer(HttpStatusCode status, byte[] body, ContentRangeHeaderValue? range)
    {
        var response = new HttpResponseMessage(status) { Content = new ByteArrayContent(body) };
        if (range is not null) response.Content.Headers.ContentRange = range;
        if (Compress) response.Content.Headers.ContentEncoding.Add("gzip");
        return response;
    }

    private HttpResponseMessage Stamp(HttpResponseMessage response, string etag, DateTimeOffset modified)
    {
        if (Validators is "strong") response.Headers.ETag = new EntityTagHeaderValue(etag);
        if (Validators is "weak") response.Headers.ETag = new EntityTagHeaderValue(etag, isWeak: true);
        if (Validators is "strong" or "date") response.Content.Headers.LastModified = modified;
        return response;
    }

    /// <summary>A JSON answer.</summary>
    public static HttpResponseMessage Json(string json, string? link = null)
    {
        var response = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(json, Encoding.UTF8, "application/json") };
        if (link is not null) response.Headers.TryAddWithoutValidation("Link", link);
        return response;
    }

    /// <summary>Serves this server on a local port until disposed: the base URL is <c>http://127.0.0.1:{port}</c>.</summary>
    public Listening Listen() => new(this);

    public sealed class Listening : IDisposable
    {
        private readonly HttpListener _listener = new();
        private readonly HttpMessageInvoker _invoker;
        private readonly Task _loop;

        public Listening(HttpTestServer server)
        {
            _invoker = new HttpMessageInvoker(server, disposeHandler: false);
            for (int attempt = 0; ; attempt++)
            {
                int port = Random.Shared.Next(20000, 60000);
                try
                {
                    _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
                    _listener.Start();
                    BaseUrl = $"http://127.0.0.1:{port}";
                    break;
                }
                catch (HttpListenerException) when (attempt < 20)
                {
                    _listener.Prefixes.Clear();
                }
            }
            _loop = Task.Run(Serve);
        }

        public string BaseUrl { get; }

        private async Task Serve()
        {
            while (_listener.IsListening)
            {
                HttpListenerContext context;
                try
                {
                    context = await _listener.GetContextAsync();
                }
                catch (Exception) when (!_listener.IsListening)
                {
                    return;
                }
                catch (HttpListenerException)
                {
                    return;
                }
                _ = Task.Run(() => Answer(context));
            }
        }

        private async Task Answer(HttpListenerContext context)
        {
            try
            {
                using var request = new HttpRequestMessage(new HttpMethod(context.Request.HttpMethod), $"http://127.0.0.1{context.Request.RawUrl}");
                foreach (string name in context.Request.Headers.AllKeys.OfType<string>())
                    request.Headers.TryAddWithoutValidation(name, context.Request.Headers.GetValues(name) ?? []);
                using HttpResponseMessage response = await _invoker.SendAsync(request, CancellationToken.None);
                context.Response.StatusCode = (int)response.StatusCode;
                foreach (var (name, values) in response.Headers) context.Response.Headers[name] = string.Join(", ", values);
                foreach (var (name, values) in response.Content.Headers)
                {
                    if (name.Equals("Content-Length", StringComparison.OrdinalIgnoreCase)) continue;
                    context.Response.Headers[name] = string.Join(", ", values);
                }
                byte[] body = await response.Content.ReadAsByteArrayAsync();
                context.Response.ContentLength64 = body.Length;
                await context.Response.OutputStream.WriteAsync(body);
                context.Response.Close();
            }
            catch (Exception)
            {
                try
                {
                    context.Response.Abort();
                }
                catch (Exception)
                {
                    // the client went away
                }
            }
        }

        public void Dispose()
        {
            _listener.Stop();
            _listener.Close();
            try
            {
                _loop.Wait(TimeSpan.FromSeconds(5));
            }
            catch (AggregateException)
            {
                // stopped
            }
            _invoker.Dispose();
        }
    }
}
