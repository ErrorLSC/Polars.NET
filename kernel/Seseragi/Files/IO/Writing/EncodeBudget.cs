namespace Kaminari.Seseragi.Files.IO.Writing;

/// <summary>
/// The threads several writers encode on, in all (a dataset's files written side by side): each thread encoding holds
/// one of them — a caller its own while it encodes (waiting for one when none is free), a helper one it found free (none
/// is started otherwise). A batch that feeds one file is then encoded on every thread the others leave free, and the
/// files together never encode on more.
/// </summary>
internal sealed class EncodeBudget(int threads)
{
    private readonly SemaphoreSlim _slots = new(Math.Max(1, threads), Math.Max(1, threads));
    private int _inUse, _peak, _encoding, _peakEncoding;

    /// <summary>The threads in all.</summary>
    public int Threads { get; } = Math.Max(1, threads);

    /// <summary>Whether a caller entering now would find a helper too (two threads free).</summary>
    public bool CanHelp => _slots.CurrentCount >= 2;

    /// <summary>The most threads encoding at once so far (tests: never more than <see cref="Threads"/>).</summary>
    public int Peak => Volatile.Read(ref _peak);

    /// <summary>The most rows encoded at once so far by the writers sharing the budget, counted where they are encoded (tests).</summary>
    public int PeakEncoding => Volatile.Read(ref _peakEncoding);

    /// <summary>Rows start (+1) or stop (−1) being encoded somewhere (<see cref="PeakEncoding"/>).</summary>
    public void Encoding(int change)
    {
        int encoding = Interlocked.Add(ref _encoding, change);
        for (int peak; encoding > (peak = Volatile.Read(ref _peakEncoding)) && Interlocked.CompareExchange(ref _peakEncoding, encoding, peak) != peak;) { }
    }

    /// <summary>A caller's thread, waiting for one to be free.</summary>
    public void Enter()
    {
        _slots.Wait();
        Note();
    }

    /// <inheritdoc cref="Enter"/>
    public async ValueTask EnterAsync()
    {
        await _slots.WaitAsync().ConfigureAwait(false);
        Note();
    }

    /// <summary>A helper's thread, when one is free now.</summary>
    public bool TryEnter()
    {
        if (!_slots.Wait(0)) return false;
        Note();
        return true;
    }

    /// <summary>A thread given back.</summary>
    public void Exit()
    {
        Interlocked.Decrement(ref _inUse);
        _slots.Release();
    }

    private void Note()
    {
        int inUse = Interlocked.Increment(ref _inUse);
        for (int peak; inUse > (peak = Volatile.Read(ref _peak)) && Interlocked.CompareExchange(ref _peak, inUse, peak) != peak;) { }
    }
}
