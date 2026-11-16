namespace Kaminari.Seseragi.Lakehouse.Storage;

/// <summary>Where a client takes its credentials from before each request.</summary>
internal abstract class CredentialSource<T> where T : class
{
    /// <summary>Credentials good for the next request (fetched or refreshed as needed).</summary>
    public abstract T Get();

    /// <summary>The credentials, for asynchronous callers (by default <see cref="Get"/>).</summary>
    public virtual ValueTask<T> GetAsync(CancellationToken cancellationToken) => ValueTask.FromResult(Get());
}

/// <summary>Credentials that never change.</summary>
internal sealed class StaticCredential<T>(T value) : CredentialSource<T> where T : class
{
    public override T Get() => value;
}

/// <summary>Credentials with the time they expire (null: never).</summary>
internal readonly record struct Temporary<T>(T Value, DateTimeOffset? Expires);

/// <summary>
/// Temporary credentials, fetched when first needed and again when less than <c>minTtl</c> of their life is left
/// (as the object_store library's token cache: a credential fetched a moment ago is used even when short-lived, a
/// failed fetch is not remembered). Asynchronous callers share one fetch, run on the thread pool, and wait for it
/// without blocking (many requests in flight wait for one token); a synchronous caller arriving meanwhile waits for
/// that fetch too.
/// </summary>
internal sealed class CachedCredential<T> : CredentialSource<T> where T : class
{
    private static readonly TimeSpan FetchBackoff = TimeSpan.FromMilliseconds(100);

    private readonly Func<Temporary<T>> _fetch;
    private readonly TimeSpan _minTtl;
    private readonly Func<DateTimeOffset> _clock;
    private readonly object _lock = new();
    private Temporary<T>? _current;
    private DateTimeOffset _fetchedAt;
    private Task<Temporary<T>>? _fetching; // the asynchronous callers' fetch in flight

    /// <param name="fetch">Fetches fresh credentials; throws <see cref="StorageException"/> when it cannot.</param>
    /// <param name="minTtl">The life a credential must have left to be used (five minutes by default).</param>
    /// <param name="clock">The current time (tests move it).</param>
    public CachedCredential(Func<Temporary<T>> fetch, TimeSpan? minTtl = null, Func<DateTimeOffset>? clock = null)
    {
        _fetch = fetch;
        _minTtl = minTtl ?? TimeSpan.FromMinutes(5);
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
    }

    /// <summary>What the credentials come from (a kind and an address, never a secret), for messages and tests.</summary>
    public string Origin { get; init; } = "";

    /// <inheritdoc/>
    public override T Get()
    {
        Task<Temporary<T>>? fetching;
        lock (_lock)
        {
            DateTimeOffset now = _clock();
            if (_current is { } current && Valid(current, now)) return current.Value;
            fetching = _fetching;
            if (fetching is null)
            {
                Temporary<T> fresh = _fetch();
                _current = fresh;
                _fetchedAt = _clock();
                return fresh.Value;
            }
        }
        return fetching.GetAwaiter().GetResult().Value; // the asynchronous callers' fetch
    }

    /// <inheritdoc/>
    public override async ValueTask<T> GetAsync(CancellationToken cancellationToken)
    {
        Task<Temporary<T>> fetching;
        lock (_lock)
        {
            if (_current is { } current && Valid(current, _clock())) return current.Value;
            fetching = _fetching ??= Task.Run(Fetch, CancellationToken.None);
        }
        return (await fetching.WaitAsync(cancellationToken).ConfigureAwait(false)).Value;

        Temporary<T> Fetch()
        {
            try
            {
                Temporary<T> fresh = _fetch();
                lock (_lock)
                {
                    _current = fresh;
                    _fetchedAt = _clock();
                }
                return fresh;
            }
            finally
            {
                lock (_lock) _fetching = null; // a failed fetch is not remembered: the next caller tries again
            }
        }
    }

    private bool Valid(Temporary<T> current, DateTimeOffset now) =>
        current.Expires is not { } expires
        || expires - now > _minTtl
        || (now - _fetchedAt < FetchBackoff && expires > now);
}

/// <summary>
/// One credential source per configuration for the whole process: stores are opened per table operation (the
/// engine's own object stores are cached the same way), so temporary credentials are fetched once and refreshed in
/// one place.
/// </summary>
internal static class SharedCredentials<T> where T : class
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, CachedCredential<T>> Sources =
        new(StringComparer.Ordinal);

    /// <summary>The source for a configuration, created by <paramref name="create"/> the first time.</summary>
    /// <param name="parts">Everything the source depends on.</param>
    /// <param name="create">Creates the source.</param>
    public static CachedCredential<T> Get(string[] parts, Func<CachedCredential<T>> create) =>
        Sources.GetOrAdd(string.Join('\0', parts), _ => create());
}
