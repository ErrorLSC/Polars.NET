using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Kaminari.Seseragi.Lakehouse.Storage.Gcs;

/// <summary>A service account key, as a <c>service-account.json</c> file holds it.</summary>
/// <param name="PrivateKeyPem">The RSA private key (PEM).</param>
/// <param name="PrivateKeyId">The key's id.</param>
/// <param name="ClientEmail">The service account.</param>
/// <param name="BaseUrl">A storage URL the file names (emulators), or null.</param>
/// <param name="DisableOAuth">Whether requests go without a token (emulators).</param>
internal sealed record ServiceAccountKey(string PrivateKeyPem, string PrivateKeyId, string ClientEmail, string? BaseUrl, bool DisableOAuth)
{
    /// <summary>The key in a service account JSON document, or null with the reason.</summary>
    public static ServiceAccountKey? Parse(string json, out string? reason)
    {
        try
        {
            using var document = JsonDocument.Parse(json);
            JsonElement root = document.RootElement;
            string? Text(string name) =>
                root.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

            string? pem = Text("private_key"), id = Text("private_key_id"), email = Text("client_email");
            if (pem is null || id is null || email is null)
            {
                reason = "the service account key lacks private_key, private_key_id or client_email";
                return null;
            }
            bool disable = root.TryGetProperty("disable_oauth", out JsonElement flag) && flag.ValueKind == JsonValueKind.True;
            reason = null;
            return new ServiceAccountKey(pem, id, email, Text("gcs_base_url"), disable);
        }
        catch (JsonException)
        {
            reason = "the service account key is not JSON";
            return null;
        }
    }
}

/// <summary>
/// OAuth tokens of a service account without a token server: self-signed JWTs (RS256) with the cloud-platform
/// scope, as Google accepts them in place of access tokens. A token is reused until five minutes before it expires.
/// </summary>
internal sealed class ServiceAccountToken : CredentialSource<string>, IDisposable
{
    internal const string Scope = "https://www.googleapis.com/auth/cloud-platform";
    private static readonly TimeSpan Lifetime = TimeSpan.FromHours(1);
    private static readonly TimeSpan Renewal = TimeSpan.FromMinutes(5);

    private readonly RSA _rsa;
    private readonly string _keyId;
    private readonly string _email;
    private readonly Func<DateTimeOffset> _clock;
    private readonly object _lock = new();
    private string? _token;
    private DateTimeOffset _expires;

    private ServiceAccountToken(RSA rsa, string keyId, string email, Func<DateTimeOffset> clock)
    {
        _rsa = rsa;
        _keyId = keyId;
        _email = email;
        _clock = clock;
    }

    /// <summary>The token source of a key, or null with the reason when its private key cannot be read.</summary>
    public static ServiceAccountToken? Create(ServiceAccountKey key, out string? reason, Func<DateTimeOffset>? clock = null)
    {
        var rsa = RSA.Create();
        try
        {
            rsa.ImportFromPem(key.PrivateKeyPem);
        }
        catch (Exception e) when (e is ArgumentException or CryptographicException)
        {
            rsa.Dispose();
            reason = "the service account's private key is not an RSA key in PEM";
            return null;
        }
        reason = null;
        return new ServiceAccountToken(rsa, key.PrivateKeyId, key.ClientEmail, clock ?? (() => DateTimeOffset.UtcNow));
    }

    /// <inheritdoc/>
    public override string Get()
    {
        lock (_lock)
        {
            DateTimeOffset now = _clock();
            if (_token is null || now >= _expires - Renewal)
            {
                _token = Jwt(now);
                _expires = now + Lifetime;
            }
            return _token;
        }
    }

    private string Jwt(DateTimeOffset now)
    {
        string header = Segment(writer =>
        {
            writer.WriteString("alg", "RS256");
            writer.WriteString("typ", "JWT");
            writer.WriteString("kid", _keyId);
        });
        string claims = Segment(writer =>
        {
            writer.WriteString("iss", _email);
            writer.WriteString("sub", _email);
            writer.WriteString("scope", Scope);
            writer.WriteNumber("iat", now.ToUnixTimeSeconds());
            writer.WriteNumber("exp", (now + Lifetime).ToUnixTimeSeconds());
        });
        string message = header + "." + claims;
        byte[] signature = _rsa.SignData(Encoding.ASCII.GetBytes(message), HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return message + "." + Base64Url(signature);
    }

    private static string Segment(Action<Utf8JsonWriter> write)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            write(writer);
            writer.WriteEndObject();
        }
        return Base64Url(buffer.ToArray());
    }

    private static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');

    public void Dispose() => _rsa.Dispose();
}
