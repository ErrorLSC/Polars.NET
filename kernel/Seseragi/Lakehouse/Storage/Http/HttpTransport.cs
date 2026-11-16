using System.Net;
using System.Net.Http.Headers;
using Kaminari.Sazanami.Network;

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
/// can be retried (see <see cref="HttpSender"/>). No time limit applies to a single request (uploads of large files
/// take as long as they take); connecting is limited to five minutes.
/// </summary>
internal static class HttpTransport
{
    // One pair of clients (stores, and instance metadata services) per set of connection options, for the whole
    // process: stores are opened per table operation, connections are pooled per client.
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, (HttpClient Store, HttpClient Metadata)> Clients =
        new(StringComparer.Ordinal);

    private static (HttpClient Store, HttpClient Metadata) ClientsFor(HttpOptions options) =>
        Clients.GetOrAdd(options.Key, _ => (
            new HttpClient(options.Handler(TimeSpan.FromMinutes(5))) { Timeout = Timeout.InfiniteTimeSpan },
            // Instance metadata services answer at once or not at all (off the cloud nothing listens there): a short
            // connect timeout gives quick feedback.
            new HttpClient(options.Handler(TimeSpan.FromSeconds(1))) { Timeout = Timeout.InfiniteTimeSpan }));

    /// <summary>Runs <paramref name="action"/> and counts the requests it sends on this flow (diagnostics and tests).</summary>
    internal static int CountRequests(Action action) => HttpSender.CountRequests(action);

    /// <summary>
    /// Sends the request <paramref name="build"/> makes (afresh for every attempt). Network failures and answers
    /// 408, 429 and 5xx are retried, as are answers <paramref name="alsoRetry"/> names; anything else is returned.
    /// </summary>
    /// <param name="build">Makes the request.</param>
    /// <param name="retry">Retry settings.</param>
    /// <param name="alsoRetry">Further answers to retry.</param>
    /// <param name="metadata">Whether the request goes to an instance metadata service (connecting is limited to one second).</param>
    /// <param name="http">Connection options (proxy, certificate checks); null for the defaults.</param>
    public static StoreResponse Send(
        Func<HttpRequestMessage> build, RetrySettings retry, Func<HttpStatusCode, bool>? alsoRetry = null, bool metadata = false,
        HttpOptions? http = null)
    {
        var (store, metadataClient) = ClientsFor(http ?? HttpOptions.Default);
        try
        {
            HttpAnswer answer = new HttpSender(metadata ? metadataClient : store, retry).Send(build, alsoRetry);
            return new StoreResponse(answer.Status, answer.Headers, answer.ContentHeaders, answer.Body, answer.Retried);
        }
        catch (HttpRetriesExhaustedException e)
        {
            throw new StorageException(e.Message, inner: e.InnerException);
        }
    }

    /// <inheritdoc cref="Send"/>
    public static async Task<StoreResponse> SendAsync(
        Func<HttpRequestMessage> build, RetrySettings retry, Func<HttpStatusCode, bool>? alsoRetry = null, HttpOptions? http = null,
        CancellationToken cancellationToken = default)
    {
        var (store, _) = ClientsFor(http ?? HttpOptions.Default);
        try
        {
            HttpAnswer answer = await new HttpSender(store, retry).SendAsync(build, alsoRetry, cancellationToken).ConfigureAwait(false);
            return new StoreResponse(answer.Status, answer.Headers, answer.ContentHeaders, answer.Body, answer.Retried);
        }
        catch (HttpRetriesExhaustedException e)
        {
            throw new StorageException(e.Message, inner: e.InnerException);
        }
    }

    /// <summary>
    /// Sends the request <paramref name="build"/> makes and hands the answer (its body not read) to
    /// <paramref name="read"/>; a network failure while it reads sends the request again (see <see cref="HttpSender"/>).
    /// </summary>
    public static Task<T> SendAsync<T>(
        Func<HttpRequestMessage> build, Func<HttpResponseMessage, CancellationToken, Task<T>> read, RetrySettings retry,
        HttpOptions? http = null, CancellationToken cancellationToken = default) =>
        SendAsync(build, (response, _, ct) => read(response, ct), retry, alsoRetry: null, http, cancellationToken);

    /// <summary>
    /// Sends the request <paramref name="build"/> makes and hands the answer (its body not read) to
    /// <paramref name="read"/> with whether an earlier attempt was made. Answers <paramref name="alsoRetry"/> names are
    /// retried too; an <see cref="IOException"/> <paramref name="read"/> throws (other than
    /// <see cref="HttpAnswerException"/>) counts as a network failure and sends the request again.
    /// </summary>
    public static async Task<T> SendAsync<T>(
        Func<HttpRequestMessage> build, Func<HttpResponseMessage, bool, CancellationToken, Task<T>> read, RetrySettings retry,
        Func<HttpStatusCode, bool>? alsoRetry, HttpOptions? http = null, CancellationToken cancellationToken = default)
    {
        var (store, _) = ClientsFor(http ?? HttpOptions.Default);
        try
        {
            return await new HttpSender(store, retry).SendAsync(build, read, alsoRetry, cancellationToken).ConfigureAwait(false);
        }
        catch (HttpRetriesExhaustedException e)
        {
            throw new StorageException(e.Message, inner: e.InnerException);
        }
    }

    /// <summary>Reads an answer's whole body (none for a HEAD) into a <see cref="StoreResponse"/>.</summary>
    public static async Task<StoreResponse> Buffer(HttpResponseMessage response, bool retried, CancellationToken cancellationToken)
    {
        byte[] body = response.RequestMessage?.Method == HttpMethod.Head
            ? []
            : await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
        return new StoreResponse(response.StatusCode, response.Headers, response.Content.Headers, body, retried);
    }
}
