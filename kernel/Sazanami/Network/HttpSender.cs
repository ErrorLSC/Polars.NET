using System.Net;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Sazanami.Network;

/// <summary>An answer, its body read completely into native memory: dispose it to give the memory back.</summary>
internal sealed class HttpAnswer(HttpStatusCode status, HttpResponseHeaders headers, HttpContentHeaders contentHeaders, NativeBufferWriter? body, bool retried)
    : IDisposable
{
    private NativeBufferWriter? _body = body;

    public HttpStatusCode Status { get; } = status;
    public HttpResponseHeaders Headers { get; } = headers;
    public HttpContentHeaders ContentHeaders { get; } = contentHeaders;

    /// <summary>The body (valid until disposed).</summary>
    public ReadOnlySpan<byte> Body => _body is null ? [] : _body.WrittenSpan;

    /// <summary>Whether an earlier attempt of the request was made: a "precondition failed" may then be our own write.</summary>
    public bool Retried { get; } = retried;

    public bool IsSuccess => (int)Status is >= 200 and < 300;

    public string? Header(string name) =>
        Headers.TryGetValues(name, out IEnumerable<string>? values) ? values.FirstOrDefault() : null;

    /// <summary>Hands the body over (the answer no longer frees it).</summary>
    public NativeBufferWriter? TakeBody() => Interlocked.Exchange(ref _body, null);

    public void Dispose() => TakeBody()?.Dispose();
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

    private static HttpAnswer Buffer(HttpResponseMessage response, bool retried) =>
        new(response.StatusCode, response.Headers, response.Content.Headers, ReadBody(response), retried);

    private static async Task<HttpAnswer> BufferAsync(HttpResponseMessage response, bool retried, CancellationToken cancellationToken) =>
        new(response.StatusCode, response.Headers, response.Content.Headers, await ReadBodyAsync(response, cancellationToken).ConfigureAwait(false), retried);

    /// <summary>An answer's whole body in native memory (none for a HEAD, or an empty body); the caller disposes it.</summary>
    internal static NativeBufferWriter? ReadBody(HttpResponseMessage response)
    {
        if (response.RequestMessage?.Method == HttpMethod.Head) return null;
        using Stream stream = response.Content.ReadAsStream();
        var body = new NativeBufferWriter(InitialBody(response));
        try
        {
            body.ReadFrom(stream);
        }
        catch
        {
            body.Dispose();
            throw;
        }
        return Empty(body);
    }

    /// <inheritdoc cref="ReadBody"/>
    internal static async Task<NativeBufferWriter?> ReadBodyAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.RequestMessage?.Method == HttpMethod.Head) return null;
        Stream stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        var body = new NativeBufferWriter(InitialBody(response));
        try
        {
            await using (stream.ConfigureAwait(false)) await body.ReadFromAsync(stream, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            body.Dispose();
            throw;
        }
        return Empty(body);
    }

    /// <summary>The buffer to start with: the announced length (then read without growing), else 4 KiB.</summary>
    private static int InitialBody(HttpResponseMessage response) =>
        response.Content.Headers.ContentLength is { } length and > 0 and <= int.MaxValue ? (int)length : 4096;

    private static NativeBufferWriter? Empty(NativeBufferWriter body)
    {
        if (body.WrittenCount > 0) return body;
        body.Dispose();
        return null;
    }
}
