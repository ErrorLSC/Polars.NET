using Kaminari.Seseragi.Lakehouse.Storage.Azure;
using Kaminari.Seseragi.Lakehouse.Storage.S3;

namespace Kaminari.Seseragi.Lakehouse.Storage;

/// <summary>
/// Storage credentials handed in from outside, as a catalog vends them: object-store configuration keys
/// (<c>aws_access_key_id</c>, <c>aws_secret_access_key</c>, <c>aws_session_token</c>; <c>azure_storage_sas_token</c>;
/// <c>google_oauth_token</c>) and the time they stop working.
/// </summary>
/// <param name="Options">The credentials as configuration keys.</param>
/// <param name="Expires">When they stop working; null when they do not expire.</param>
public sealed record VendedCredentials(IReadOnlyDictionary<string, string> Options, DateTimeOffset? Expires);

/// <summary>
/// Vended credentials that are fetched again before they expire: once less than a margin of their life is left
/// (half their life, at most five minutes). Every store of a table shares one instance, so the credentials are
/// fetched in one place. When fetching fails while the credentials held are still valid, those are used until
/// they expire (and fetching is tried again at most once a second).
/// </summary>
public sealed class RenewableCredentials
{
    private static readonly TimeSpan MaxMargin = TimeSpan.FromMinutes(5);
    private static readonly TimeSpan RetryAfterFailure = TimeSpan.FromSeconds(1);

    private readonly Func<VendedCredentials> _fetch;
    private readonly Func<DateTimeOffset> _clock;
    private readonly object _lock = new();
    private VendedCredentials? _current;
    private DateTimeOffset _renewAt;

    /// <param name="fetch">Fetches fresh credentials; its exceptions are reported to the store that asked.</param>
    /// <param name="initial">Credentials already fetched, used first.</param>
    /// <param name="clock">The current time (tests move it).</param>
    public RenewableCredentials(Func<VendedCredentials> fetch, VendedCredentials? initial = null, Func<DateTimeOffset>? clock = null)
    {
        _fetch = fetch ?? throw new ArgumentNullException(nameof(fetch));
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
        if (initial is not null) Accept(initial, _clock());
    }

    /// <summary>
    /// Takes credentials fetched elsewhere (by a new lookup of the same table) when they last longer than the ones
    /// held.
    /// </summary>
    public void Offer(VendedCredentials credentials)
    {
        ArgumentNullException.ThrowIfNull(credentials);
        lock (_lock)
        {
            if (_current is null || Later(credentials.Expires, _current.Expires)) Accept(credentials, _clock());
        }
    }

    /// <summary>Credentials to use now, fetched again when due.</summary>
    /// <param name="renewAt">When to ask again; null when they never expire.</param>
    public VendedCredentials Current(out DateTimeOffset? renewAt)
    {
        lock (_lock)
        {
            DateTimeOffset now = _clock();
            if (_current is null || now >= _renewAt)
            {
                try
                {
                    Accept(_fetch(), now);
                }
                catch (Exception) when (_current?.Expires is { } expires && expires > now)
                {
                    // Still valid: keep them until they expire, and do not ask the failing source at every request.
                    _renewAt = Min(now + RetryAfterFailure, expires);
                }
            }
            renewAt = _current!.Expires is null ? null : _renewAt;
            return _current;
        }
    }

    private void Accept(VendedCredentials credentials, DateTimeOffset now)
    {
        _current = credentials;
        if (credentials.Expires is not { } expires)
        {
            _renewAt = DateTimeOffset.MaxValue;
            return;
        }
        TimeSpan life = expires - now;
        TimeSpan margin = life <= TimeSpan.Zero ? TimeSpan.Zero : TimeSpan.FromTicks(Math.Min(MaxMargin.Ticks, life.Ticks / 2));
        _renewAt = expires - margin;
    }

    private static bool Later(DateTimeOffset? a, DateTimeOffset? b) => a is null ? b is not null : b is not null && a > b;

    private static DateTimeOffset Min(DateTimeOffset a, DateTimeOffset b) => a < b ? a : b;

    /// <summary>The credentials of one store, made from the vended ones each time those change.</summary>
    internal sealed class Projection<T>(RenewableCredentials source, Func<IReadOnlyDictionary<string, string>, T> make)
        : CredentialSource<T> where T : class
    {
        private readonly object _lock = new();
        private VendedCredentials? _from;
        private T? _value;

        public override T Get()
        {
            VendedCredentials current = source.Current(out _);
            lock (_lock)
            {
                if (!ReferenceEquals(current, _from))
                {
                    _value = make(current.Options);
                    _from = current;
                }
                return _value!;
            }
        }
    }

    internal CredentialSource<AwsCredential> ForS3() => new Projection<AwsCredential>(this, options =>
        Value(options, "aws_access_key_id") is { } id && Value(options, "aws_secret_access_key") is { } secret
            ? new AwsCredential(id, secret, Value(options, "aws_session_token"))
            : throw new StorageException("The renewed credentials are not AWS credentials, which the S3 store needs."));

    internal CredentialSource<IReadOnlyList<KeyValuePair<string, string>>> ForAzureSas() =>
        new Projection<IReadOnlyList<KeyValuePair<string, string>>>(this, options =>
        {
            string sas = Value(options, "azure_storage_sas_token")
                ?? throw new StorageException("The renewed credentials are not an Azure SAS token, which the Azure store needs.");
            return AzureSettings.SasAuth(sas, out string? reason) is AzureAuth.Sas parsed
                ? parsed.Pairs
                : throw new StorageException($"The renewed SAS token cannot be used: {reason}.");
        });

    internal CredentialSource<string> ForGcs() => new Projection<string>(this, options =>
        Value(options, "google_oauth_token")
            ?? throw new StorageException("The renewed credentials are not a Google OAuth token, which the GCS store needs."));

    private static string? Value(IReadOnlyDictionary<string, string> options, string key) =>
        options.FirstOrDefault(o => o.Key.Equals(key, StringComparison.OrdinalIgnoreCase)).Value;
}
