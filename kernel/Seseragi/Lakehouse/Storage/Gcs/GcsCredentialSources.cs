using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Storage.Http;

namespace Kaminari.Seseragi.Lakehouse.Storage.Gcs;

/// <summary>
/// OAuth tokens for Google Cloud Storage as the object_store library fetches them: from the metadata server (GCE,
/// GKE, Cloud Run) or with a gcloud user's refresh token. Tokens are fetched when first needed.
/// </summary>
internal static class GcsCredentialSources
{
    /// <summary>The metadata server's host name.</summary>
    internal const string DefaultMetadataHost = "metadata.google.internal";

    /// <summary>The metadata server's address, tried when the host name does not answer.</summary>
    internal const string DefaultMetadataIp = "169.254.169.254";

    /// <summary>Google's token endpoint for refresh tokens.</summary>
    internal const string DefaultTokenUri = "https://accounts.google.com/o/oauth2/token";

    // The engine's object store renews these tokens when less than four minutes are left.
    private static readonly TimeSpan MinTtl = TimeSpan.FromMinutes(4);

    /// <summary>The metadata server: its host name first, then its address.</summary>
    public static CachedCredential<string> MetadataServer(string host, string ip, RetrySettings retry, Func<DateTimeOffset>? clock = null) =>
        new(() =>
        {
            StoreResponse? answer = null;
            StorageException? failure = null;
            foreach (string server in new[] { host, ip })
            {
                try
                {
                    answer = HttpTransport.Send(() =>
                    {
                        var request = new HttpRequestMessage(HttpMethod.Get,
                            $"http://{server}/computeMetadata/v1/instance/service-accounts/default/token?audience=https%3A%2F%2Fwww.googleapis.com%2Foauth2%2Fv4%2Ftoken");
                        request.Headers.TryAddWithoutValidation("Metadata-Flavor", "Google");
                        return request;
                    }, retry, metadata: true);
                    if (answer.IsSuccess) break;
                    failure = new StorageException($"The metadata server {server} answered HTTP {(int)answer.Status}.", answer.Status);
                    answer = null;
                }
                catch (StorageException e)
                {
                    failure = e;
                }
            }
            if (answer is null)
            {
                throw new StorageException(
                    $"No Google Cloud credentials are configured, and the metadata server ({host}, {ip}) gives no token " +
                    $"(not on Google Cloud?): {failure?.Message}", failure?.Status, inner: failure);
            }
            return Token(answer.Body, "the metadata server", clock);
        }, MinTtl, clock)
        {
            Origin = $"metadata server {host} / {ip}",
        };

    /// <summary>A gcloud user (<c>authorized_user</c> credentials): the refresh token is exchanged for access tokens.</summary>
    /// <param name="clientId">The OAuth client.</param>
    /// <param name="clientSecret">The OAuth client's secret.</param>
    /// <param name="refreshToken">The user's refresh token.</param>
    /// <param name="retry">Retry settings.</param>
    /// <param name="tokenUri">The token endpoint (tests use a stand-in).</param>
    /// <param name="clock">The current time.</param>
    public static CachedCredential<string> AuthorizedUser(
        string clientId, string clientSecret, string refreshToken, RetrySettings retry, string? tokenUri = null,
        Func<DateTimeOffset>? clock = null) =>
        new(() =>
        {
            StoreResponse response = HttpTransport.Send(() => new HttpRequestMessage(HttpMethod.Post, tokenUri ?? DefaultTokenUri)
            {
                Content = new FormUrlEncodedContent(
                [
                    new("grant_type", "refresh_token"),
                    new("client_id", clientId),
                    new("client_secret", clientSecret),
                    new("refresh_token", refreshToken),
                ]),
            }, retry);
            if (!response.IsSuccess)
                throw new StorageException($"Failed to refresh the gcloud user's token: HTTP {(int)response.Status}.", response.Status);
            return Token(response.Body, "Google's token endpoint", clock);
        }, MinTtl, clock)
        {
            Origin = $"gcloud user of client {clientId}",
        };

    private static Temporary<string> Token(byte[] body, string source, Func<DateTimeOffset>? clock)
    {
        try
        {
            using var document = JsonDocument.Parse(body);
            JsonElement root = document.RootElement;
            DateTimeOffset now = clock?.Invoke() ?? DateTimeOffset.UtcNow;
            return new Temporary<string>(root.GetProperty("access_token").GetString()!,
                now.AddSeconds(root.GetProperty("expires_in").GetInt64()));
        }
        catch (Exception e) when (e is JsonException or KeyNotFoundException or InvalidOperationException or FormatException)
        {
            throw new StorageException($"The answer of {source} is not a valid token.", inner: e);
        }
    }
}
