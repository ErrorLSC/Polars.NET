namespace Kaminari.Sazanami.Concurrency;

/// <summary>
/// A group of units for <see cref="OrderedUnits{TGroup, TResult}"/> (a file of a dataset, say): its units known so far
/// and whether more may come (a group whose units are found as it is read: a stream scanned), what each unit weighs,
/// and what it holds (freed once its units are all taken and nothing keeps it).
/// </summary>
/// <param name="index">The group's place among the groups (its order).</param>
internal abstract class UnitGroup(int index) : IDisposable
{
    private readonly object _lock = new();
    private int _joined, _taken, _holds;
    private bool _complete, _freed;

    /// <summary>The group's place among the groups.</summary>
    public int Index => index;

    /// <summary>The units found so far and whether the group is over (no unit left to find), read at once.</summary>
    public abstract (int Found, bool Over) Progress { get; }

    /// <summary>What ended the group's finding of units, if it failed (once over): delivered after its units found, in order.</summary>
    public virtual Exception? Failure => null;

    /// <summary>
    /// Finds units until <paramref name="units"/> are found or the group is over, returning at once (the work belongs on
    /// the pool; called under the window's lock, so that a request gate can be entered in order). A group whose units
    /// are all known never is asked.
    /// </summary>
    public virtual Task FindAsync(int units, CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>The size of unit <paramref name="unit"/> counted against the window's budget.</summary>
    public abstract long UnitBytes(int unit);

    /// <summary>The rows of unit <paramref name="unit"/> when known (counted against the rows to plan), else -1.</summary>
    public virtual long UnitRows(int unit) => -1;

    /// <summary>Frees what the group holds (once).</summary>
    protected abstract void Free();

    // ---- the window's bookkeeping --------------------------------------------------------------------------------------

    /// <summary>The group's units found that joined the window (placeholders aside).</summary>
    internal int Joined { get; set; }

    /// <summary>The finding of units running, or the last one (a failed one stays).</summary>
    internal Task? Finding { get; set; }

    /// <summary>Whether every unit of the group joined, the last placeholder or failure included.</summary>
    internal bool Complete
    {
        get
        {
            lock (_lock) return _complete;
        }
        set
        {
            lock (_lock) _complete = value;
        }
    }

    /// <summary>Whether every unit joined was taken.</summary>
    internal bool AllTaken
    {
        get
        {
            lock (_lock) return _taken == _joined;
        }
    }

    /// <summary>One more unit joined.</summary>
    internal void UnitJoined()
    {
        lock (_lock) _joined++;
    }

    /// <summary>One more unit taken; true when it was the last (the group is then freed, once nothing keeps it).</summary>
    internal bool Taken()
    {
        bool free;
        lock (_lock)
        {
            _taken++;
            if (!(_complete && _taken >= _joined)) return false;
            free = _holds == 0;
        }
        if (free) Dispose();
        return true;
    }

    /// <summary>Keeps the group for a result beyond its last unit taken (the consumer then calls <see cref="Release"/>).</summary>
    internal void Hold()
    {
        lock (_lock) _holds++;
    }

    /// <summary>A result's hold let go: the group freed if every unit was taken and nothing else keeps it.</summary>
    public void Release()
    {
        lock (_lock)
        {
            if (--_holds != 0 || !(_complete && _taken >= _joined)) return;
        }
        Dispose();
    }

    /// <summary>Frees the group (once), whatever is left of it.</summary>
    public void Dispose()
    {
        lock (_lock)
        {
            if (_freed) return;
            _freed = true;
        }
        Free();
    }
}

/// <summary>What <see cref="OrderedUnits{TGroup, TResult}"/> runs: the groups in order, planned, and their units, started.</summary>
internal interface IUnitSource<TGroup, TResult> where TGroup : UnitGroup where TResult : class, IDisposable
{
    /// <summary>The next group to plan, in order (null when none is left).</summary>
    int? NextGroup();

    /// <summary>Whether a group is left to plan (no <see cref="NextGroup"/> asked).</summary>
    bool GroupsLeft { get; }

    /// <summary>
    /// Plans group <paramref name="group"/>, returning at once (the work belongs on the pool). Called under the window's
    /// lock, in order: a request gate entered here is entered in the groups' order.
    /// </summary>
    Task<TGroup> Plan(int group, CancellationToken cancellationToken);

    /// <summary>
    /// Starts unit <paramref name="unit"/> of <paramref name="group"/>, returning at once (called under the window's
    /// lock). <paramref name="last"/> says that no group is left and fewer units than the window's few are left to start.
    /// </summary>
    Task<TResult> Start(TGroup group, int unit, bool last, CancellationToken cancellationToken);

    /// <summary>The result of a group that has no unit (or whose units were all taken before it was over).</summary>
    TResult Empty(TGroup group);

    /// <summary>Whether the consumer may still need the group of <paramref name="result"/> (it then calls <see cref="UnitGroup.Release"/>).</summary>
    bool Keeps(TResult result);
}

/// <summary>What <see cref="OrderedUnits{TGroup, TResult}"/> allows.</summary>
/// <param name="Ahead">The most units started and not taken (the one taken next always starts).</param>
/// <param name="Bytes">The most unit bytes started and not taken, beyond the one taken next.</param>
/// <param name="Groups">The most groups planning, planned or joined with units not taken.</param>
/// <param name="RowsToPlan">Once the units joined hold this many rows (those that know theirs), no group is planned (null: no bound).</param>
/// <param name="FewUnits">Units started while no group is left and fewer than this many are left to start are told so (0: never).</param>
internal readonly record struct OrderedUnitsLimits(int Ahead, long Bytes, int Groups, long? RowsToPlan = null, int FewUnits = 0);

/// <summary>
/// Runs the units of groups (the record batches or row groups of a dataset's files, say) ahead of a consumer that takes
/// their results in order: the unit taken next always starts; the others start up to <see cref="OrderedUnitsLimits.Ahead"/>
/// beyond it while their bytes stay within <see cref="OrderedUnitsLimits.Bytes"/>. Groups are planned ahead (up to
/// <see cref="OrderedUnitsLimits.Groups"/> at once, while the units known ahead run short), and join in order as their
/// plans complete. A group whose units are found as it is read finds them in the background, a window's worth at a
/// time, once planned; its units join as they are found, the groups after it once it is over. A group is freed when
/// its last unit is taken.
/// </summary>
/// <remarks>
/// Failures come in order: a plan's when the consumer reaches its group (awaited), a unit's when it is taken, a group's
/// failure to find its units after the units it found. Disposing the window stops the work, waits for what runs, frees
/// the results not taken, then the groups. One consumer takes the results.
/// </remarks>
internal sealed class OrderedUnits<TGroup, TResult> : IDisposable, IAsyncDisposable where TGroup : UnitGroup where TResult : class, IDisposable
{
    private readonly IUnitSource<TGroup, TResult> _source;
    private readonly OrderedUnitsLimits _limits;
    private readonly CancellationTokenSource _cancel;
    private readonly object _lock = new();
    private readonly List<Unit?> _units = [];
    private readonly List<Task<TGroup>?> _plans = [];  // the groups planning or planned, in order (null once joined)
    private readonly HashSet<TGroup> _live = [];        // the groups joined whose units are not all taken
    private TGroup? _tail;                              // a group joined that is still finding units (those after it wait)
    private int _integrated, _head, _started;
    private long _inFlight, _plannedRows;
    private bool _disposed;

    public OrderedUnits(IUnitSource<TGroup, TResult> source, OrderedUnitsLimits limits, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limits.Ahead);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limits.Groups);
        _source = source;
        _limits = limits;
        _cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    }

    /// <summary>The most unit bytes and units started and not taken at once, and groups at once (for tests).</summary>
    public (long Bytes, int Units, int Groups) Peak { get; private set; }

    private sealed class Unit(TGroup group, int index, long bytes)
    {
        public TGroup Group => group;

        public int Index => index;

        public long Bytes => bytes;

        public Task<TResult>? Task { get; set; }
    }

    /// <summary>The next unit's result, in order; null when every unit was taken.</summary>
    /// <exception cref="ObjectDisposedException">The window was disposed.</exception>
    public async Task<TResult?> NextAsync()
    {
        while (true)
        {
            Task waiting;
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                Fill();
                if (_head < _units.Count) break;
                if (_tail is { Finding: { } finding })
                {
                    waiting = finding; // the tail finds more units (its failure joins as one)
                }
                else
                {
                    if (_integrated == _plans.Count) return null; // every group planned and taken
                    waiting = _plans[_integrated]!;
                }
            }
            if (waiting is Task<TGroup> plan) await plan.ConfigureAwait(false); // a failure to plan surfaces here, in order
            else await waiting.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
        Unit unit;
        lock (_lock) unit = _units[_head]!;
        TResult result = await unit.Task!.ConfigureAwait(false);
        lock (_lock)
        {
            unit.Task = null;
            _inFlight -= unit.Bytes;
            _units[_head++] = null;
            if (_source.Keeps(result)) unit.Group.Hold();
            if (unit.Group.Taken()) _live.Remove(unit.Group);
        }
        return result;
    }

    /// <summary>
    /// Under the lock: joins the tail's units found and the plans completed in order, starts the units the window allows,
    /// plans more groups while the units known ahead run short.
    /// </summary>
    private void Fill()
    {
        if (_disposed) return;
        if (_tail is { } tail)
        {
            Extend(tail); // the units found (before a failure too)
            if (_tail == tail && tail.Finding is { IsCompleted: true } finding && !finding.IsCompletedSuccessfully)
            {
                // The finding failed (the group's own failure is its Failure): in order, as a unit.
                Join(tail, new Unit(tail, -1, 0) { Task = Failed(finding) });
                tail.Complete = true;
                _tail = null;
            }
            else if (_tail == tail) Find(tail); // the next stretch, unless one runs
        }
        while (_tail is null && _integrated < _plans.Count && _plans[_integrated]!.IsCompletedSuccessfully)
        {
            TGroup group = _plans[_integrated]!.Result;
            _plans[_integrated++] = null;
            _live.Add(group);
            Extend(group);
        }
        while (_started < _units.Count && (_started == _head || _started - _head < _limits.Ahead && _inFlight + _units[_started]!.Bytes <= _limits.Bytes))
        {
            Unit unit = _units[_started++]!;
            if (unit.Task is not null) continue; // a placeholder, or a failure
            _inFlight += unit.Bytes;
            bool last = _limits.FewUnits > 0 && _units.Count - _started < _limits.FewUnits && _tail is null && _integrated == _plans.Count && !_source.GroupsLeft;
            unit.Task = _source.Start(unit.Group, unit.Index, last, _cancel.Token);
            Peak = Peak with { Bytes = Math.Max(Peak.Bytes, _inFlight), Units = Math.Max(Peak.Units, _started - _head) };
        }
        // Groups at once: those planning or planned and not joined, and those joined with units not taken.
        while (_plans.Count - _integrated + _live.Count < _limits.Groups && _units.Count - _head < _limits.Ahead && !(_plannedRows >= _limits.RowsToPlan)
               && _source.NextGroup() is int group)
        {
            _plans.Add(Plan(group));
            Peak = Peak with { Groups = Math.Max(Peak.Groups, _plans.Count - _integrated + _live.Count) };
        }
    }

    /// <summary>
    /// Under the lock: the units of <paramref name="group"/> found join; once it is over, its failure joins as a unit too
    /// (in order), or an empty unit when every unit was taken already (taking it frees the group). A group still finding
    /// units becomes the tail.
    /// </summary>
    private void Extend(TGroup group)
    {
        var (found, over) = group.Progress; // read at once: the group finds units outside the lock
        for (; group.Joined < found; group.Joined++)
        {
            long rows = group.UnitRows(group.Joined);
            if (rows > 0) _plannedRows += rows;
            Join(group, new Unit(group, group.Joined, group.UnitBytes(group.Joined)));
        }
        if (!over)
        {
            _tail = group;
            Find(group); // unless it finds already (the consumer may wait for it next)
            return;
        }
        if (group.Failure is { } failure) Join(group, new Unit(group, -1, 0) { Task = Task.FromException<TResult>(failure) });
        else if (group.AllTaken) Join(group, new Unit(group, -1, 0) { Task = Task.FromResult(_source.Empty(group)) });
        group.Complete = true;
        if (_tail == group) _tail = null;
    }

    private void Join(TGroup group, Unit unit)
    {
        _units.Add(unit);
        group.UnitJoined();
    }

    /// <summary>Under the lock: plans a group; once planned it starts finding its units (if it must), and the window fills on.</summary>
    private Task<TGroup> Plan(int index)
    {
        Task<TGroup> planning = _source.Plan(index, _cancel.Token);
        planning.ContinueWith(done =>
        {
            lock (_lock)
            {
                if (done.IsCompletedSuccessfully) Find(done.Result);
                Fill();
            }
        }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);
        return planning;
    }

    /// <summary>
    /// Under the lock: <paramref name="group"/> finds the units of a window more, unless it is over, a finding runs or
    /// failed, or the window is disposed; once done, the window fills on and the group finds more.
    /// </summary>
    private void Find(TGroup group)
    {
        var (found, over) = group.Progress;
        if (_disposed || over || group.Finding is { IsCompleted: false } or { IsCompletedSuccessfully: false }) return;
        Task finding = group.FindAsync(found + _limits.Ahead, _cancel.Token);
        group.Finding = finding;
        finding.ContinueWith(_ =>
        {
            lock (_lock)
            {
                Find(group);
                Fill();
            }
        }, CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);
    }

    /// <summary>A result that fails as <paramref name="finding"/> did.</summary>
    private static async Task<TResult> Failed(Task finding)
    {
        await finding.ConfigureAwait(false);
        throw new InvalidOperationException("A failed finding of units completed.");
    }

    /// <inheritdoc cref="DisposeAsync"/>
    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    /// <summary>Stops the work, waits for what runs, frees the results not taken, then the groups.</summary>
    public async ValueTask DisposeAsync()
    {
        Task<TGroup>[] plans;
        Unit[] running;
        TGroup[] live;
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            _cancel.Cancel();
            plans = [.. _plans.Skip(_integrated).OfType<Task<TGroup>>()];
            running = [.. _units.Skip(_head).OfType<Unit>().Where(u => u.Task is not null)];
            live = [.. _live];
        }
        // What runs finishes first (a unit reads its group), then everything is freed.
        foreach (Unit unit in running)
        {
            try
            {
                (await unit.Task!.ConfigureAwait(false)).Dispose();
            }
            catch
            {
                // abandoned: its error no longer matters
            }
        }
        foreach (Task<TGroup> plan in plans)
        {
            try
            {
                TGroup group = await plan.ConfigureAwait(false);
                await Found(group).ConfigureAwait(false);
                group.Dispose();
            }
            catch
            {
                // abandoned
            }
        }
        foreach (TGroup group in live)
        {
            await Found(group).ConfigureAwait(false);
            group.Dispose();
        }
        _cancel.Dispose();

        // A finding still running (cancelled: none starts once the window is disposed).
        static async Task Found(TGroup group)
        {
            if (group.Finding is { } finding) await finding.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }
}
