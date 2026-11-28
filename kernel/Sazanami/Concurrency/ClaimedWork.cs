using System.Runtime.ExceptionServices;

namespace Kaminari.Sazanami.Concurrency;

/// <summary>
/// Work that any thread may do, once: a helper runs it when it claims it first (<see cref="TryClaim"/>, then
/// <see cref="Run"/>); the thread that needs the result calls <see cref="Complete"/>, which runs it there when no
/// helper took it, or waits for the helper that did, and rethrows its failure.
/// </summary>
internal abstract class ClaimedWork
{
    private readonly object _lock = new();
    private int _state; // 0 waiting, 1 running, 2 done
    private ManualResetEventSlim? _done;
    private Exception? _error;

    /// <summary>The work itself; its exception is kept for <see cref="Complete"/>.</summary>
    protected abstract void Execute();

    /// <summary>Runs after <see cref="Execute"/>, whatever its outcome, before the work counts as done.</summary>
    protected virtual void Finally() { }

    public bool TryClaim() => Interlocked.CompareExchange(ref _state, 1, 0) == 0;

    /// <summary>Whether the work is done (by whoever ran it); <see cref="Complete"/> then returns at once.</summary>
    public bool IsDone => Volatile.Read(ref _state) == 2;

    /// <summary>Runs the work (once claimed).</summary>
    public void Run()
    {
        try
        {
            Execute();
        }
        catch (Exception e)
        {
            _error = e;
        }
        finally
        {
            Finally();
            lock (_lock)
            {
                Volatile.Write(ref _state, 2);
                _done?.Set();
            }
        }
    }

    /// <summary>The work done: by this thread when no helper took it, else when the helper is done; its failure rethrown.</summary>
    public void Complete()
    {
        Wait();
        if (_error is { } e) ExceptionDispatchInfo.Throw(e);
    }

    /// <summary>The work done (by this thread when no helper took it), its failure kept: before its buffers are let go.</summary>
    public void Wait()
    {
        if (TryClaim()) Run();
        else
        {
            ManualResetEventSlim? wait = null;
            lock (_lock)
            {
                if (Volatile.Read(ref _state) != 2) wait = _done ??= new ManualResetEventSlim(false);
            }
            wait?.Wait();
        }
        _done?.Dispose();
        _done = null;
    }

    /// <summary>The work let go: never run when no helper took it yet, else waited for (before its buffers go).</summary>
    public void Drop()
    {
        if (TryClaim())
        {
            lock (_lock) Volatile.Write(ref _state, 2);
        }
        else Wait();
    }

    /// <summary>
    /// Makes work that is done (or was never queued) ready to run again, for reuse: called once its new state is set (a
    /// helper of its last run may still claim it from then on).
    /// </summary>
    protected void Reset()
    {
        if (Volatile.Read(ref _state) == 1) throw new InvalidOperationException("Work is reset while it runs.");
        _error = null;
        Volatile.Write(ref _state, 0);
    }

    /// <summary>Queues the work for a helper on the thread pool, which holds a thread of <paramref name="budget"/> (taken by the caller) while it runs.</summary>
    public void Help(WorkBudget budget) =>
        ThreadPool.UnsafeQueueUserWorkItem(static state =>
        {
            try
            {
                if (state.Work.TryClaim()) state.Work.Run();
            }
            finally
            {
                state.Budget.Exit();
            }
        }, (Work: this, Budget: budget), preferLocal: false);
}
