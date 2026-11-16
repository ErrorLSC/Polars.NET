namespace Kaminari.Sazanami.Network;

/// <summary>
/// At most <c>capacity</c> requests at once, those waiting let in by their order (lowest first, then first come): a
/// reader that consumes its sources in order gives each request its source's position, so that the source it waits
/// for is served before those it reads ahead.
/// </summary>
internal sealed class RequestGate
{
    private readonly object _lock = new();
    private readonly PriorityQueue<Waiter, (long Order, long Arrival)> _waiting = new();
    private int _free;
    private long _arrivals;

    public RequestGate(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        Capacity = capacity;
        _free = capacity;
    }

    /// <summary>The most requests let in at once.</summary>
    public int Capacity { get; }

    /// <summary>The requests in at the moment (for tests).</summary>
    public int Inside
    {
        get
        {
            lock (_lock) return Capacity - _free;
        }
    }

    /// <summary>The most requests that were in at once (for tests).</summary>
    public int PeakInside { get; private set; }

    /// <summary>Waits for a place (by <paramref name="order"/>); disposing the lease gives it back.</summary>
    public ValueTask<Lease> EnterAsync(long order, CancellationToken cancellationToken = default)
    {
        // Cancelled already: a cancelled wait, never a throw (callers queue under their own locks).
        if (cancellationToken.IsCancellationRequested) return ValueTask.FromCanceled<Lease>(cancellationToken);
        Waiter waiter;
        lock (_lock)
        {
            if (_free > 0 && _waiting.Count == 0)
            {
                _free--;
                PeakInside = Math.Max(PeakInside, Capacity - _free);
                return ValueTask.FromResult(new Lease(this));
            }
            waiter = new Waiter();
            _waiting.Enqueue(waiter, (order, _arrivals++));
        }
        if (cancellationToken.CanBeCanceled)
            waiter.Registration = cancellationToken.Register(static w => ((Waiter)w!).TrySetCanceled(), waiter);
        return Wait(waiter, cancellationToken);
    }

    private async ValueTask<Lease> Wait(Waiter waiter, CancellationToken cancellationToken)
    {
        try
        {
            await waiter.Task.ConfigureAwait(false);
        }
        catch (TaskCanceledException)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        finally
        {
            await waiter.Registration.DisposeAsync().ConfigureAwait(false);
        }
        return new Lease(this);
    }

    /// <summary>Hands the place to the first waiter still waiting, else frees it.</summary>
    private void Release()
    {
        lock (_lock)
        {
            // A waiter cancelled meanwhile stays queued until reached here; skip it.
            while (_waiting.TryDequeue(out Waiter? next, out _))
            {
                if (next.TrySetResult(true)) return;
            }
            _free++;
        }
    }

    /// <summary>A place taken; disposed once to give it back.</summary>
    public readonly struct Lease : IDisposable
    {
        private readonly RequestGate? _gate;

        internal Lease(RequestGate gate) => _gate = gate;

        public void Dispose() => _gate?.Release();
    }

    private sealed class Waiter() : TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously)
    {
        public CancellationTokenRegistration Registration;
    }
}
