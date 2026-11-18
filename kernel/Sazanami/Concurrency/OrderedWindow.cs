namespace Kaminari.Sazanami.Concurrency;

/// <summary>
/// Runs work items <c>0 .. count-1</c> on the thread pool ahead of a consumer that takes their results in order, up to
/// <c>window</c> items at once and while the sizes of the items started and not yet taken stay within a budget (the
/// item taken next always starts). The window opens gradually (1, 2, 4, … items as results are taken), so a consumer
/// that stops after the first results has not paid for a full window. Results own resources: those never taken (the
/// consumer stopped early, or failed) are disposed when the window is.
/// </summary>
/// <remarks>Not thread-safe: one consumer takes the items in order.</remarks>
internal sealed class OrderedWindow<T> : IDisposable where T : class, IDisposable
{
    private readonly Func<int, CancellationToken, Task<T>> _start;
    private readonly Func<int, long> _size;
    private readonly int _window;
    private readonly long _budget;
    private readonly CancellationTokenSource _cancel;
    private readonly Task<T>?[] _tasks;
    private int _started, _open = 1;
    private long _inFlight;
    private bool _disposed;

    /// <param name="count">The number of items.</param>
    /// <param name="start">Starts item <c>i</c> (called on the consumer's thread; the work belongs on the pool).</param>
    /// <param name="window">The most items started and not yet taken.</param>
    /// <param name="size">The size of item <c>i</c> counted against <paramref name="budget"/>.</param>
    /// <param name="budget">The most size started and not yet taken (at least the next item always starts).</param>
    /// <param name="cancellationToken">Cancels the items not yet taken.</param>
    public OrderedWindow(int count, Func<int, CancellationToken, Task<T>> start, int window, Func<int, long> size, long budget,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(window);
        _start = start;
        _size = size;
        _window = window;
        _budget = budget;
        _cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _tasks = new Task<T>?[count];
    }

    /// <summary>
    /// Starts the items up to the window's end and hands over item <paramref name="index"/>'s task (the caller owns its
    /// result from then on). Items are taken in order, each once.
    /// </summary>
    public Task<T> Take(int index)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        for (; _started < _tasks.Length && _started < index + _open
               && (_started == index || _inFlight + _size(_started) <= _budget); _started++)
        {
            _inFlight += _size(_started);
            int item = _started;
            CancellationToken token = _cancel.Token;
            _tasks[item] = Task.Run(() => _start(item, token), token);
        }
        Task<T> task = _tasks[index] ?? throw new InvalidOperationException($"Item {index} was taken already.");
        _tasks[index] = null;
        _inFlight -= _size(index);
        _open = Math.Min(_window, 2 * _open);
        return task;
    }

    /// <summary>Cancels the items not taken, waits for them and disposes their results.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _cancel.Cancel();
        foreach (Task<T>? task in _tasks)
        {
            if (task is null) continue;
            ((Task)task).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
            if (task.IsCompletedSuccessfully) task.Result.Dispose();
        }
        Array.Clear(_tasks);
        _cancel.Dispose();
    }
}
