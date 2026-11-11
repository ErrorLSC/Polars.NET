using System.Net;
using System.Net.Http.Headers;

namespace Kaminari.Seseragi.Lakehouse.Storage.Http;

/// <summary>A store's answer, its body read completely.</summary>
internal sealed class StoreResponse(HttpStatusCode status, HttpResponseHeaders headers, HttpContentHeaders contentHeaders, byte[] body, bool retried)
{
    public HttpStatusCode Status { get; } = status;
    public HttpResponseHeaders Headers { get; } = headers;
    public HttpContentHeaders ContentHeaders { get; } = contentHeaders;
    public byte[] Body { get; } = body;

    /// <summary>Whether an earlier attempt of the request was made: a "precondition failed" may then be our own write.</summary>
    public bool Retried { get; } = retried;

    public bool IsSuccess => (int)Status is >= 200 and < 300;

    public string? Header(string name) =>
        Headers.TryGetValues(name, out IEnumerable<string>? values) ? values.FirstOrDefault() : null;
}

/// <summary>
/// Sends requests to object stores, synchronously, over one connection pool shared by every store, retrying what
/// can be retried. No time limit applies to a single request (uploads of large files take as long as they take);
/// connecting is limited to five minutes.
/// </summary>
internal static class HttpTransport
{
    private static readonly HttpClient Client = new(new SocketsHttpHandler
    {
        PooledConnectionLifetime = TimeSpan.FromMinutes(5),
        ConnectTimeout = TimeSpan.FromMinutes(5),
        AutomaticDecompression = DecompressionMethods.None,
        AllowAutoRedirect = false,
    })
    {
        Timeout = Timeout.InfiniteTimeSpan,
    };

    /// <summary>
    /// Sends the request <paramref name="build"/> makes (afresh for every attempt). Network failures and answers
    /// 408, 429 and 5xx are retried, as are answers <paramref name="alsoRetry"/> names; anything else is returned.
    /// </summary>
    public static StoreResponse Send(Func<HttpRequestMessage> build, RetrySettings retry, Func<HttpStatusCode, bool>? alsoRetry = null)
    {
        var backoff = new Backoff(retry);
        while (true)
        {
            bool retried = backoff.Retries > 0;
            try
            {
                using HttpRequestMessage request = build();
                using HttpResponseMessage response = Client.Send(request, HttpCompletionOption.ResponseHeadersRead);
                byte[] body = request.Method == HttpMethod.Head ? [] : ReadBody(response);
                var answer = new StoreResponse(response.StatusCode, response.Headers, response.Content.Headers, body, retried);
                if (Retryable(answer.Status, alsoRetry) && backoff.CanRetry)
                {
                    backoff.Wait();
                    continue;
                }
                return answer;
            }
            catch (Exception e) when (e is HttpRequestException or IOException && backoff.CanRetry)
            {
                backoff.Wait();
            }
            catch (Exception e) when (e is HttpRequestException or IOException)
            {
                throw new StorageException($"The request failed after {backoff.Retries} retries: {e.Message}", inner: e);
            }
        }
    }

    private static bool Retryable(HttpStatusCode status, Func<HttpStatusCode, bool>? alsoRetry) =>
        status is HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests
        || ((int)status >= 500 && status != HttpStatusCode.NotImplemented)
        || (alsoRetry?.Invoke(status) ?? false);

    private static byte[] ReadBody(HttpResponseMessage response)
    {
        using Stream stream = response.Content.ReadAsStream();
        long? length = response.Content.Headers.ContentLength;
        if (length is { } exact and <= int.MaxValue)
        {
            var buffer = new byte[exact];
            stream.ReadExactly(buffer);
            return buffer;
        }
        using var copy = new MemoryStream();
        stream.CopyTo(copy);
        return copy.ToArray();
    }
}
