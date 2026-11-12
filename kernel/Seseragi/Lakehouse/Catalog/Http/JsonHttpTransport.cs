using System.Net.Http.Headers;
using System.Text;

namespace Kaminari.Seseragi.Lakehouse.Catalog.Http;

/// <summary>
/// Sends JSON requests with a bearer token and returns the response bytes. Each call runs either
/// asynchronously or fully synchronously (<c>async: false</c>), so blocking callers never wait on the thread pool.
/// </summary>
internal sealed class JsonHttpTransport(string? bearerToken, HttpClient? httpClient) : IDisposable
{
    internal const string UserAgent = "Kaminari-Seseragi";
    private const int MaxErrorBodyChars = 4096;

    private readonly HttpClient _http = httpClient ?? new HttpClient();
    private readonly bool _ownsClient = httpClient is null;
    private readonly AuthenticationHeaderValue? _authorization = string.IsNullOrEmpty(bearerToken) ? null : new AuthenticationHeaderValue("Bearer", bearerToken);

    public async ValueTask<byte[]> SendAsync(
        HttpMethod method, string url, byte[]? jsonBody, bool async, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(method, url);
        request.Headers.Authorization = _authorization;
        request.Headers.UserAgent.ParseAdd(UserAgent);
        request.Headers.Accept.ParseAdd("application/json");
        if (jsonBody is not null)
        {
            request.Content = new ByteArrayContent(jsonBody);
            request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
        }

        try
        {
            using var response = async
                ? await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false)
                : _http.Send(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

            byte[] body;
            if (async)
            {
                body = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                using var stream = response.Content.ReadAsStream(cancellationToken);
                using var buffer = new MemoryStream();
                stream.CopyTo(buffer);
                body = buffer.ToArray();
            }

            if (!response.IsSuccessStatusCode)
            {
                string text = Encoding.UTF8.GetString(body);
                if (text.Length > MaxErrorBodyChars) text = text[..MaxErrorBodyChars] + "...";
                throw new CatalogException(
                    $"{method} {request.RequestUri?.AbsolutePath} failed with status {(int)response.StatusCode} ({response.ReasonPhrase}), response body: {text}",
                    response.StatusCode,
                    text);
            }

            return body;
        }
        catch (HttpRequestException e)
        {
            throw new CatalogException($"{method} {request.RequestUri?.AbsolutePath} failed: {e.Message}", e);
        }
        catch (OperationCanceledException e) when (!cancellationToken.IsCancellationRequested)
        {
            throw new CatalogException($"{method} {request.RequestUri?.AbsolutePath} timed out.", e);
        }
    }

    public void Dispose()
    {
        if (_ownsClient) _http.Dispose();
    }
}
