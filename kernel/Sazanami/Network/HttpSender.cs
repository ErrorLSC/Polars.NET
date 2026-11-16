using System.Net;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;

namespace Kaminari.Sazanami.Network;

/// <summary>An answer, its body read completely.</summary>
internal sealed class HttpAnswer(HttpStatusCode status, HttpResponseHeaders headers, HttpContentHeaders contentHeaders, byte[] body, bool retried)
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

/// <summary>The retries of a request ran out on network failures: the last failure is the inner exception.</summary>
internal sealed class HttpRetriesExhaustedException(int retries, Exception last)
    : IOException($"The request failed after {retries} retries: {last.Message}", last)
{
    /// <summary>How many retries were made.</summary>
    public int Retries { get; } = retries;
}

/// <summary>
/// A failure an answer decided (a status, a header, a size the caller cannot accept): not a network failure, never
/// retried. Readers of answers throw it (or <see cref="FileNotFoundException"/>) for what sending again would not change.
/// </summary>
internal sealed class HttpAnswerException(string message, Exception? inner = null) : IOException(message, inner);

/// <summary>
/// Sends requests over a client, retrying what can be retried: network failures (a failed read of the answer's body
/// included) and answers 408, 429 and 5xx (but 501), and the answers a caller names. A <c>Retry-After</c> header makes
/// the wait longer, up to <see cref="RetrySettings.MaxBackoff"/>. Synchronous calls block on their waits; asynchronous
/// calls wait with timers. A cancellation by the caller is never retried; other cancellations (a connect timeout
/// surfaces as one) are. When the retries run out on a network failure, <see cref="HttpRetriesExhaustedException"/>.
/// </summary>
/// <remarks>
/// No time limit applies here to a single request (an upload takes as long as it takes): the client's handler sets
/// the connect timeout. Every attempt counts in <see cref="CountRequests"/>.
/// </remarks>
internal sealed class HttpSender(HttpClient client, RetrySettings retry)
{
    private static readonly AsyncLocal<StrongBox<int>?> Counter = new();

    /// <summary>Runs <paramref name="action"/> and counts the requests it sends on this flow (diagnostics and tests).</summary>
    public static int CountRequests(Action action)
    {
        var box = new StrongBox<int>();
        StrongBox<int>? outer = Counter.Value;
        Counter.Value = box;
        try
        {
            action();
        }
        finally
        {
            Counter.Value = outer;
        }
        return box.Value;
    }

    /// <inheritdoc cref="CountRequests"/>
    public static async Task<int> CountRequestsAsync(Func<Task> action)
    {
        var box = new StrongBox<int>();
        StrongBox<int>? outer = Counter.Value;
        Counter.Value = box;
        try
        {
            await action().ConfigureAwait(false);
        }
        finally
        {
            Counter.Value = outer;
        }
        return box.Value;
    }

    /// <summary>
    /// Sends the request <paramref name="build"/> makes (afresh for every attempt) and reads the answer's body (none for
    /// a HEAD); answers not retried, or once the retries ran out, are returned whatever their status.
    /// </summary>
    public HttpAnswer Send(Func<HttpRequestMessage> build, Func<HttpStatusCode, bool>? alsoRetry = null) =>
        Send(build, (response, retried) => Buffer(response, retried), alsoRetry);

    /// <inheritdoc cref="Send(Func{HttpRequestMessage}, Func{HttpStatusCode, bool}?)"/>
    public Task<HttpAnswer> SendAsync(Func<HttpRequestMessage> build, Func<HttpStatusCode, bool>? alsoRetry = null,
        CancellationToken cancellationToken = default) =>
        SendAsync(build, BufferAsync, alsoRetry, cancellationToken);

    /// <summary>
    /// Sends the request <paramref name="build"/> makes and hands the answer (headers read, body not) to
    /// <paramref name="read"/>, with whether it is a retry. A network failure while <paramref name="read"/> reads the
    /// body sends the request again (so <paramref name="read"/> must start afresh each time).
    /// </summary>
    /// <param name="build">Makes the request.</param>
    /// <param name="read">Reads the answer.</param>
    /// <param name="alsoRetry">Further answers to retry.</param>
    /// <param name="retryOnly">When given, the only answers retried (instead of 408, 429, 5xx and <paramref name="alsoRetry"/>):
    /// for a request whose failure has another way out than sending it again.</param>
    public T Send<T>(Func<HttpRequestMessage> build, Func<HttpResponseMessage, bool, T> read, Func<HttpStatusCode, bool>? alsoRetry = null,
        Func<HttpStatusCode, bool>? retryOnly = null)
    {
        var backoff = new Backoff(retry);
        while (true)
        {
            bool retried = backoff.Retries > 0;
            try
            {
                using HttpRequestMessage request = build();
                Count();
                using HttpResponseMessage response = client.Send(request, HttpCompletionOption.ResponseHeadersRead);
                if (Retryable(response.StatusCode, alsoRetry, retryOnly) && backoff.CanRetry)
                {
                    backoff.Wait(RetryAfter(response));
                    continue;
                }
                return read(response, retried);
            }
            catch (Exception e) when (Network(e) && backoff.CanRetry)
            {
                backoff.Wait();
            }
            catch (Exception e) when (Network(e))
            {
                throw new HttpRetriesExhaustedException(backoff.Retries, e);
            }
        }
    }

    /// <inheritdoc cref="Send{T}"/>
    public async Task<T> SendAsync<T>(Func<HttpRequestMessage> build, Func<HttpResponseMessage, bool, CancellationToken, Task<T>> read,
        Func<HttpStatusCode, bool>? alsoRetry = null, CancellationToken cancellationToken = default, Func<HttpStatusCode, bool>? retryOnly = null)
    {
        var backoff = new Backoff(retry);
        while (true)
        {
            bool retried = backoff.Retries > 0;
            TimeSpan? retryAfter = null;
            try
            {
                using HttpRequestMessage request = build();
                Count();
                using HttpResponseMessage response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken)
                    .ConfigureAwait(false);
                if (!Retryable(response.StatusCode, alsoRetry, retryOnly) || !backoff.CanRetry)
                    return await read(response, retried, cancellationToken).ConfigureAwait(false);
                retryAfter = RetryAfter(response);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e) when (Network(e) && backoff.CanRetry)
            {
                // Retried after the wait below.
            }
            catch (Exception e) when (Network(e))
            {
                throw new HttpRetriesExhaustedException(backoff.Retries, e);
            }
            await backoff.WaitAsync(retryAfter, cancellationToken).ConfigureAwait(false);
        }
    }

    private static void Count()
    {
        if (Counter.Value is { } count) Interlocked.Increment(ref count.Value);
    }

    /// <summary>A connect timeout surfaces as a cancellation; failures an answer decided are not the network's.</summary>
    private static bool Network(Exception e) =>
        e is (HttpRequestException or IOException or OperationCanceledException) and not (HttpAnswerException or FileNotFoundException);

    private static bool Retryable(HttpStatusCode status, Func<HttpStatusCode, bool>? alsoRetry, Func<HttpStatusCode, bool>? retryOnly = null) =>
        retryOnly?.Invoke(status) ?? (status is HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests
        || ((int)status >= 500 && status != HttpStatusCode.NotImplemented)
        || (alsoRetry?.Invoke(status) ?? false));

    /// <summary>How long the server asked to wait (<c>Retry-After</c>: seconds or a date), if it did.</summary>
    internal static TimeSpan? RetryAfter(HttpResponseMessage response) => response.Headers.RetryAfter switch
    {
        { Delta: { } delta } => delta,
        { Date: { } date } => date - DateTimeOffset.UtcNow is { Ticks: > 0 } left ? left : TimeSpan.Zero,
        _ => null,
    };

    private static HttpAnswer Buffer(HttpResponseMessage response, bool retried)
    {
        byte[] body = response.RequestMessage?.Method == HttpMethod.Head ? [] : ReadBody(response);
        return new HttpAnswer(response.StatusCode, response.Headers, response.Content.Headers, body, retried);
    }

    private static async Task<HttpAnswer> BufferAsync(HttpResponseMessage response, bool retried, CancellationToken cancellationToken)
    {
        byte[] body = response.RequestMessage?.Method == HttpMethod.Head ? [] : await ReadBodyAsync(response, cancellationToken).ConfigureAwait(false);
        return new HttpAnswer(response.StatusCode, response.Headers, response.Content.Headers, body, retried);
    }

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

    private static async Task<byte[]> ReadBodyAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        Stream stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        await using (stream.ConfigureAwait(false))
        {
            long? length = response.Content.Headers.ContentLength;
            if (length is { } exact and <= int.MaxValue)
            {
                var buffer = new byte[exact];
                await stream.ReadExactlyAsync(buffer, cancellationToken).ConfigureAwait(false);
                return buffer;
            }
            using var copy = new MemoryStream();
            await stream.CopyToAsync(copy, cancellationToken).ConfigureAwait(false);
            return copy.ToArray();
        }
    }
}
