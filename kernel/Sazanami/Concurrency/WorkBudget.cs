namespace Kaminari.Sazanami.Concurrency;

/// <summary>
/// The threads a piece of work may keep busy: the workers running (counted while they run) and the helpers that take
/// work ahead of them, at most <see cref="Max"/> together.
/// </summary>
internal sealed class WorkBudget(int max)
{
    private int _busy;

    public int Max => max;

    public void Enter() => Interlocked.Increment(ref _busy);

    public void Exit() => Interlocked.Decrement(ref _busy);

    /// <summary>Takes a thread for a helper when one is left.</summary>
    public bool TryEnter()
    {
        while (true)
        {
            int busy = Volatile.Read(ref _busy);
            if (busy >= max) return false;
            if (Interlocked.CompareExchange(ref _busy, busy + 1, busy) == busy) return true;
        }
    }
}
