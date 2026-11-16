namespace Kaminari.Sazanami.Network;

/// <summary>
/// How failed requests are retried: up to <paramref name="MaxRetries"/> times while <paramref name="Budget"/> has not
/// run out, waiting a random time between <paramref name="InitialBackoff"/> and twice the previous wait, never more
/// than <paramref name="MaxBackoff"/>. The budget limits the retrying, not a single request: a long upload is not
/// cut off by it.
/// </summary>
public sealed record RetrySettings(int MaxRetries, TimeSpan Budget, TimeSpan InitialBackoff, TimeSpan MaxBackoff)
{
    /// <summary>Two retries within ten seconds, waits from 100 ms up to 15 s.</summary>
    public static RetrySettings Default { get; } =
        new(2, TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(15));
}

/// <summary>The waits of one request's retries.</summary>
internal sealed class Backoff(RetrySettings settings)
{
    private readonly long _start = Environment.TickCount64;
    private double _nextSeconds = settings.InitialBackoff.TotalSeconds;

    /// <summary>How many retries have been taken.</summary>
    public int Retries { get; private set; }

    /// <summary>Whether another retry is allowed.</summary>
    public bool CanRetry =>
        Retries < settings.MaxRetries && Environment.TickCount64 - _start <= settings.Budget.TotalMilliseconds;

    /// <summary>
    /// Counts a retry and tells how long to wait before it: the backoff, or longer when the server asked for it
    /// (<paramref name="retryAfter"/>), never beyond <see cref="RetrySettings.MaxBackoff"/>.
    /// </summary>
    public TimeSpan Next(TimeSpan? retryAfter = null)
    {
        Retries++;
        double low = settings.InitialBackoff.TotalSeconds;
        double high = Math.Max(low, _nextSeconds * 2);
        double drawn = low + Random.Shared.NextDouble() * (high - low);
        double wait = Math.Max(_nextSeconds, retryAfter?.TotalSeconds ?? 0);
        _nextSeconds = Math.Min(settings.MaxBackoff.TotalSeconds, drawn);
        return TimeSpan.FromSeconds(Math.Min(wait, settings.MaxBackoff.TotalSeconds));
    }

    /// <summary>Waits before the next retry.</summary>
    public void Wait(TimeSpan? retryAfter = null) => Thread.Sleep(Next(retryAfter));

    /// <inheritdoc cref="Wait"/>
    public Task WaitAsync(TimeSpan? retryAfter, CancellationToken cancellationToken) => Task.Delay(Next(retryAfter), cancellationToken);
}
