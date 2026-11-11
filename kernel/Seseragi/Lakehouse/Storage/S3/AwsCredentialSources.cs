using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Xml;
using Kaminari.Seseragi.Lakehouse.Storage.Http;

namespace Kaminari.Seseragi.Lakehouse.Storage.S3;

/// <summary>
/// Temporary AWS credentials as the object_store library fetches them: from the instance metadata service (EC2),
/// a container credentials endpoint (ECS tasks, EKS Pod Identity) or STS with a web identity token (EKS IRSA).
/// Credentials are fetched when first needed, never when a store is opened.
/// </summary>
internal static class AwsCredentialSources
{
    /// <summary>The instance metadata service's address.</summary>
    internal const string DefaultMetadataEndpoint = "http://169.254.169.254";

    /// <summary>The ECS container credentials host, to which a relative URI is appended.</summary>
    internal const string TaskEndpoint = "http://169.254.170.2";

    private const string CredentialsPath = "latest/meta-data/iam/security-credentials";

    /// <summary>The process-wide source for a configuration (see <see cref="SharedCredentials{T}"/>).</summary>
    public static CachedCredential<AwsCredential> Shared(string[] parts, Func<CachedCredential<AwsCredential>> create) =>
        SharedCredentials<AwsCredential>.Get(parts, create);

    /// <summary>
    /// The instance metadata service: an IMDSv2 session token, then the role and its credentials. With
    /// <paramref name="imdsv1Fallback"/> a refused token request (403) goes on without one (IMDSv1).
    /// </summary>
    public static CachedCredential<AwsCredential> Instance(string endpoint, bool imdsv1Fallback, RetrySettings retry, Func<DateTimeOffset>? clock = null, HttpOptions? http = null)
    {
        endpoint = endpoint.TrimEnd('/');
        return new(() =>
        {
            StoreResponse tokenResponse;
            try
            {
                tokenResponse = HttpTransport.Send(() =>
                {
                    var request = new HttpRequestMessage(HttpMethod.Put, $"{endpoint}/latest/api/token");
                    request.Headers.TryAddWithoutValidation("X-aws-ec2-metadata-token-ttl-seconds", "600");
                    return request;
                }, retry, metadata: true, http: http);
            }
            catch (StorageException e) when (e.Status is null)
            {
                throw new StorageException(
                    $"No AWS credentials are configured, and the instance metadata service at {endpoint} cannot be reached " +
                    $"(not on EC2?): {e.Message}", inner: e);
            }
            string? token = null;
            if (tokenResponse.IsSuccess) token = Encoding.UTF8.GetString(tokenResponse.Body);
            else if (!(imdsv1Fallback && tokenResponse.Status == HttpStatusCode.Forbidden))
                throw Failure(tokenResponse, "get a session token from the instance metadata service");

            HttpRequestMessage Get(string url)
            {
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                if (token is not null) request.Headers.TryAddWithoutValidation("X-aws-ec2-metadata-token", token);
                return request;
            }
            StoreResponse roleResponse = HttpTransport.Send(() => Get($"{endpoint}/{CredentialsPath}/"), retry, metadata: true, http: http);
            if (!roleResponse.IsSuccess) throw Failure(roleResponse, "find the instance's role in the instance metadata service");
            string role = Encoding.UTF8.GetString(roleResponse.Body);
            StoreResponse credentials = HttpTransport.Send(() => Get($"{endpoint}/{CredentialsPath}/{role}"), retry, metadata: true, http: http);
            if (!credentials.IsSuccess) throw Failure(credentials, "get the instance role's credentials");
            return FromJson(credentials.Body, "the instance metadata service");
        }, clock: clock) { Origin = $"instance metadata {endpoint}" + (imdsv1Fallback ? " (IMDSv1 fallback)" : "") };
    }

    /// <summary>An ECS task role: the credentials endpoint plus the task's relative URI.</summary>
    public static CachedCredential<AwsCredential> Task(string url, RetrySettings retry, Func<DateTimeOffset>? clock = null, HttpOptions? http = null) =>
        new(() =>
        {
            StoreResponse response = HttpTransport.Send(() => new HttpRequestMessage(HttpMethod.Get, url), retry, http: http);
            if (!response.IsSuccess) throw Failure(response, "get the task role's credentials");
            return FromJson(response.Body, "the container credentials endpoint");
        }, clock: clock) { Origin = $"task role {url}" };

    /// <summary>EKS Pod Identity: the full URI, authorized with the token the file holds (read for every fetch).</summary>
    public static CachedCredential<AwsCredential> Pod(string url, string tokenFile, RetrySettings retry, Func<DateTimeOffset>? clock = null, HttpOptions? http = null) =>
        new(() =>
        {
            string token = ReadTokenFile(tokenFile, "EKS Pod Identity token").TrimEnd('\r', '\n');
            StoreResponse response = HttpTransport.Send(() =>
            {
                var request = new HttpRequestMessage(HttpMethod.Get, url);
                request.Headers.TryAddWithoutValidation("Authorization", token);
                return request;
            }, retry, http: http);
            if (!response.IsSuccess) throw Failure(response, "get the pod's credentials");
            return FromJson(response.Body, "the container credentials endpoint");
        }, clock: clock) { Origin = $"pod identity {url}" };

    /// <summary>
    /// STS <c>AssumeRoleWithWebIdentity</c> with the token the file holds (read for every fetch), unsigned.
    /// </summary>
    /// <param name="tokenFile">The web identity token file.</param>
    /// <param name="roleArn">The role to assume.</param>
    /// <param name="sessionName">The role session's name.</param>
    /// <param name="endpoint">The STS endpoint.</param>
    /// <param name="retry">Retry settings.</param>
    /// <param name="allowHttp">Whether an <c>http</c> endpoint is accepted (tests only: the token must not travel in clear).</param>
    /// <param name="clock">The current time.</param>
    public static CachedCredential<AwsCredential> WebIdentity(
        string tokenFile, string roleArn, string sessionName, string endpoint, RetrySettings retry, bool allowHttp = false,
        Func<DateTimeOffset>? clock = null, HttpOptions? http = null) =>
        new(() =>
        {
            if (!allowHttp && !endpoint.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
                throw new StorageException("The STS endpoint for web identity credentials must use https.");
            string token = ReadTokenFile(tokenFile, "web identity token");
            var query = new List<KeyValuePair<string, string>>
            {
                new("Action", "AssumeRoleWithWebIdentity"),
                new("DurationSeconds", "3600"),
                new("RoleArn", roleArn),
                new("RoleSessionName", sessionName),
                new("Version", "2011-06-15"),
                new("WebIdentityToken", token),
            };
            string url = endpoint + (endpoint.Contains('?') ? "&" : "?") + string.Join('&', query.Select(p =>
                PercentEncoding.Encode(p.Key, encodeSlash: true) + "=" + PercentEncoding.Encode(p.Value, encodeSlash: true)));
            StoreResponse response = HttpTransport.Send(() => new HttpRequestMessage(HttpMethod.Post, url), retry, http: http);
            if (!response.IsSuccess) throw Failure(response, "assume the role with the web identity token");
            return FromStsXml(response.Body);
        }, clock: clock) { Origin = $"web identity {roleArn} at {endpoint}" };

    private static string ReadTokenFile(string path, string what)
    {
        try
        {
            return File.ReadAllText(path);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            throw new StorageException($"Cannot read the {what} file '{path}': {e.Message}", inner: e);
        }
    }

    private static StorageException Failure(StoreResponse response, string what) =>
        new($"Failed to {what}: HTTP {(int)response.Status}.", response.Status);

    /// <summary>The credentials of an instance metadata or container credentials answer.</summary>
    internal static Temporary<AwsCredential> FromJson(byte[] body, string source)
    {
        try
        {
            using var document = JsonDocument.Parse(body);
            JsonElement root = document.RootElement;
            string Text(string name) => root.GetProperty(name).GetString()
                ?? throw new StorageException($"The answer of {source} has no {name}.");
            return new Temporary<AwsCredential>(
                new AwsCredential(Text("AccessKeyId"), Text("SecretAccessKey"), Text("Token")),
                DateTimeOffset.Parse(Text("Expiration"), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal));
        }
        catch (Exception e) when (e is JsonException or KeyNotFoundException or InvalidOperationException or FormatException)
        {
            throw new StorageException($"The answer of {source} is not valid credentials.", inner: e);
        }
    }

    /// <summary>The credentials of an <c>AssumeRoleWithWebIdentity</c> answer.</summary>
    internal static Temporary<AwsCredential> FromStsXml(byte[] body)
    {
        string? keyId = null, secret = null, token = null, expiration = null;
        try
        {
            using var reader = XmlReader.Create(new MemoryStream(body), new XmlReaderSettings { DtdProcessing = DtdProcessing.Prohibit });
            while (!reader.EOF)
            {
                if (reader.NodeType != XmlNodeType.Element)
                {
                    reader.Read();
                    continue;
                }
                switch (reader.LocalName)
                {
                    case "AccessKeyId": keyId = reader.ReadElementContentAsString(); break;
                    case "SecretAccessKey": secret = reader.ReadElementContentAsString(); break;
                    case "SessionToken": token = reader.ReadElementContentAsString(); break;
                    case "Expiration": expiration = reader.ReadElementContentAsString(); break;
                    default: reader.Read(); break;
                }
            }
        }
        catch (XmlException e)
        {
            throw new StorageException("The STS answer is not XML.", inner: e);
        }
        if (keyId is null || secret is null || token is null || expiration is null)
            throw new StorageException("The STS answer holds no credentials.");
        return new Temporary<AwsCredential>(new AwsCredential(keyId, secret, token),
            DateTimeOffset.Parse(expiration, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal));
    }
}
