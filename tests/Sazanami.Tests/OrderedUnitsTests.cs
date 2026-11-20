using System.Collections.Concurrent;
using Kaminari.Sazanami.Concurrency;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="OrderedUnits{TGroup, TResult}"/> over a made-up source (no file): results in order, groups after groups
/// (those that find their units as they go included, and those with none); the window's limits; failures in order (a
/// plan's, a unit's, a group's finding, a group's own); early stops and disposal freeing what was not taken; results
/// that keep their group; planning bounded by rows; the last units told so.
/// </summary>
public class OrderedUnitsTests
{
    /// <summary>A group of <c>units</c> units (found <c>step</c> at a time when it grows), failing as asked.</summary>
    private sealed class Group(int index, int units, Probe probe, int step = 0, int failFindingAt = -1, int failAt = -1) : UnitGroup(index)
    {
        private int _found = step > 0 ? 0 : units;

        public int Freed;

        public override (int Found, bool Over) Progress
        {
            get
            {
                int found = Volatile.Read(ref _found);
                bool over = found >= units || failAt >= 0 && found >= failAt;
                return (Math.Min(found, failAt >= 0 ? failAt : units), over);
            }
        }

        public override Exception? Failure => failAt >= 0 && Progress.Over ? new InvalidDataException($"group {Index} damaged") : null;

        public override Task FindAsync(int wanted, CancellationToken cancellationToken) => Task.Run(async () =>
        {
            Assert.Equal(0, Volatile.Read(ref Freed)); // never freed while it finds units
            await Task.Delay(1, cancellationToken);
            int found = Volatile.Read(ref _found);
            if (failFindingAt >= 0 && found >= failFindingAt) throw new IOException($"group {Index} unreadable");
            Volatile.Write(ref _found, Math.Min(units, found + step)); // a step at a time, whatever is wanted
        }, CancellationToken.None);

        public override long UnitBytes(int unit) => probe.Bytes;

        public override long UnitRows(int unit) => probe.Rows;

        protected override void Free() => Interlocked.Increment(ref Freed);
    }

    /// <summary>A result: its group and unit (-1: a group's empty result), disposed once.</summary>
    private sealed class Result(int group, int unit, Probe probe) : IDisposable
    {
        public int Group => group;

        public int Unit => unit;

        public bool Disposed { get; private set; }

        public override string ToString() => $"{group}.{unit}";

        public void Dispose()
        {
            Assert.False(Disposed);
            Disposed = true;
            Interlocked.Increment(ref probe.ResultsDisposed);
        }
    }

    private sealed class Probe(params (int Units, int Step, int FailFindingAt, int FailAt)[] groups) : IUnitSource<Group, Result>
    {
        private int _next, _running;
        public readonly ConcurrentDictionary<int, Group> Planned = [];
        public readonly ConcurrentBag<Result> Made = [];
        public int ResultsDisposed, MostRunning, Plans, Lasts;
        public long Bytes = 1, Rows = -1;
        public int FailPlanAt = -1, FailUnitOf = -1, KeepUnitOf = -1;
        public bool PlannedAtOnce; // plans complete before the window sees them

        public bool GroupsLeft => _next < groups.Length;

        public int? NextGroup() => _next < groups.Length ? _next++ : null;

        public Task<Group> Plan(int group, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Plans);
            if (PlannedAtOnce)
            {
                var (count, every, failFinding, fail) = groups[group];
                return Task.FromResult(Planned[group] = new Group(group, count, this, every, failFinding, fail));
            }
            return Task.Run(async () =>
            {
                await Task.Delay(1, CancellationToken.None);
                if (group == FailPlanAt) throw new InvalidOperationException($"plan {group}");
                var (units, step, failFindingAt, failAt) = groups[group];
                return Planned[group] = new Group(group, units, this, step, failFindingAt, failAt);
            }, CancellationToken.None);
        }

        public Task<Result> Start(Group group, int unit, bool last, CancellationToken cancellationToken)
        {
            if (last) Interlocked.Increment(ref Lasts);
            Assert.Equal(0, Volatile.Read(ref group.Freed)); // never freed while its units are read
            return Task.Run(async () =>
            {
                int now = Interlocked.Increment(ref _running);
                InterlockedMax(ref MostRunning, now);
                try
                {
                    await Task.Delay(unit % 3 == 0 ? 3 : 0, CancellationToken.None); // later units may finish first
                    if (group.Index == FailUnitOf && unit == 1) throw new InvalidOperationException($"unit {group.Index}.1");
                    var result = new Result(group.Index, unit, this);
                    Made.Add(result);
                    return result;
                }
                finally
                {
                    Interlocked.Decrement(ref _running);
                }
            }, cancellationToken);
        }

        public Result Empty(Group group)
        {
            var result = new Result(group.Index, -1, this);
            Made.Add(result);
            return result;
        }

        public bool Keeps(Result result) => result.Group == KeepUnitOf;

        private static void InterlockedMax(ref int target, int value)
        {
            int seen;
            while (value > (seen = Volatile.Read(ref target)) && Interlocked.CompareExchange(ref target, value, seen) != seen) { }
        }
    }

    private static readonly OrderedUnitsLimits Limits = new(Ahead: 4, Bytes: long.MaxValue, Groups: 3);

    /// <summary>Every result in order (failures caught), each disposed after it is taken; the groups' indices as taken.</summary>
    private static async Task<(List<string> Taken, Exception? Error)> Drain(OrderedUnits<Group, Result> window, int stopAfter = int.MaxValue)
    {
        List<string> taken = [];
        try
        {
            while (taken.Count < stopAfter && await window.NextAsync() is { } result)
            {
                taken.Add(result.ToString());
                result.Dispose();
            }
            return (taken, null);
        }
        catch (Exception e)
        {
            return (taken, e);
        }
    }

    private static IEnumerable<string> Units(int group, int count) => Enumerable.Range(0, count).Select(u => $"{group}.{u}");

    [Fact]
    public async Task Results_ComeInOrder_GroupsAfterGroups_AsTheirUnitsAreFound()
    {
        // Fixed groups, a group of none, groups that find their units 2 or 1 at a time (one ending exactly on a step).
        var probe = new Probe((3, 0, -1, -1), (0, 0, -1, -1), (7, 2, -1, -1), (4, 2, -1, -1), (2, 1, -1, -1), (0, 3, -1, -1), (5, 0, -1, -1));
        var window = new OrderedUnits<Group, Result>(probe, Limits);
        var (taken, error) = await Drain(window);
        await window.DisposeAsync();
        Assert.Null(error);
        Assert.Equal([.. Units(0, 3), "1.-1", .. Units(2, 7), .. Units(3, 4), .. Units(4, 2), "5.-1", .. Units(6, 5)], taken);
        Assert.All(probe.Planned.Values, g => Assert.Equal(1, g.Freed));
        Assert.Equal(probe.Made.Count, probe.ResultsDisposed);
        Assert.InRange(window.Peak.Units, 1, Limits.Ahead);
        Assert.InRange(window.Peak.Groups, 1, Limits.Groups);
        Assert.InRange(probe.MostRunning, 1, Limits.Ahead);
    }

    /// <summary>
    /// Groups planned before the window sees their plans (joined in the same fill that planned them): a group finding its
    /// units starts finding when it joins, so the consumer always has something to wait for (many windows: a race).
    /// </summary>
    [Fact]
    public async Task GroupsJoiningAtOnce_FindTheirUnits()
    {
        for (int round = 0; round < 40; round++)
        {
            var probe = new Probe((3, 1, -1, -1), (0, 2, -1, -1), (4, 2, -1, -1)) { PlannedAtOnce = true };
            await using var window = new OrderedUnits<Group, Result>(probe, Limits with { Groups = 1 });
            var (taken, error) = await Drain(window);
            Assert.Null(error);
            Assert.Equal([.. Units(0, 3), "1.-1", .. Units(2, 4)], taken);
        }
    }

    [Fact]
    public async Task TheBytes_LimitTheUnitsAhead_ButTheNextAlwaysStarts()
    {
        var probe = new Probe((12, 0, -1, -1), (12, 0, -1, -1)) { Bytes = 100 };
        var window = new OrderedUnits<Group, Result>(probe, Limits with { Ahead = 8, Bytes = 250 });
        var (taken, error) = await Drain(window);
        await window.DisposeAsync();
        Assert.Null(error);
        Assert.Equal(24, taken.Count);
        Assert.InRange(window.Peak.Bytes, 100, 300); // two ahead within the budget, the next beyond it
        Assert.InRange(probe.MostRunning, 1, 3);
    }

    [Fact]
    public async Task Failures_ComeInOrder_AfterTheResultsBefore()
    {
        // A plan's failure: when its group is reached.
        var plan = new Probe((2, 0, -1, -1), (2, 0, -1, -1), (2, 0, -1, -1)) { FailPlanAt = 1 };
        await using (var window = new OrderedUnits<Group, Result>(plan, Limits))
        {
            var (taken, error) = await Drain(window);
            Assert.Equal(Units(0, 2), taken);
            Assert.Equal("plan 1", Assert.IsType<InvalidOperationException>(error).Message);
        }

        // A unit's failure: when it is taken.
        var unit = new Probe((2, 0, -1, -1), (3, 0, -1, -1)) { FailUnitOf = 1 };
        await using (var window = new OrderedUnits<Group, Result>(unit, Limits))
        {
            var (taken, error) = await Drain(window);
            Assert.Equal([.. Units(0, 2), "1.0"], taken);
            Assert.Equal("unit 1.1", Assert.IsType<InvalidOperationException>(error).Message);
        }

        // A group failing to find its units (the store, say): after the units it found, before the groups after it.
        var finding = new Probe((1, 0, -1, -1), (9, 2, 4, -1), (2, 0, -1, -1));
        await using (var window = new OrderedUnits<Group, Result>(finding, Limits))
        {
            var (taken, error) = await Drain(window);
            Assert.Equal([.. Units(0, 1), .. Units(1, 4)], taken);
            Assert.Equal("group 1 unreadable", Assert.IsType<IOException>(error).Message);
        }

        // A group's own failure (a damaged stream): after the units before it.
        var damaged = new Probe((9, 2, -1, 5), (2, 0, -1, -1));
        await using (var window = new OrderedUnits<Group, Result>(damaged, Limits))
        {
            var (taken, error) = await Drain(window);
            Assert.Equal(Units(0, 5), taken);
            Assert.Equal("group 0 damaged", Assert.IsType<InvalidDataException>(error).Message);
        }
        foreach (Probe probe in (Probe[])[plan, unit, finding, damaged])
        {
            Assert.All(probe.Planned.Values, g => Assert.Equal(1, g.Freed));
            Assert.Equal(probe.Made.Count, probe.ResultsDisposed);
        }
    }

    /// <summary>A consumer that stops early: the results started and not taken are freed, and every group planned.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(13)]
    public async Task Disposing_FreesWhatWasNotTaken(int stopAfter)
    {
        var probe = new Probe((6, 0, -1, -1), (40, 3, -1, -1), (6, 0, -1, -1), (6, 2, -1, -1));
        var window = new OrderedUnits<Group, Result>(probe, Limits);
        var (taken, _) = await Drain(window, stopAfter);
        await window.DisposeAsync();
        Assert.Equal(stopAfter, taken.Count);
        Assert.All(probe.Planned.Values, g => Assert.Equal(1, g.Freed));
        Assert.Equal(probe.Made.Count, probe.ResultsDisposed);
        await Assert.ThrowsAsync<ObjectDisposedException>(window.NextAsync); // nothing more once disposed
    }

    [Fact]
    public async Task AResultThatKeepsItsGroup_HoldsIt_UntilReleased()
    {
        var probe = new Probe((2, 0, -1, -1), (1, 0, -1, -1)) { KeepUnitOf = 0 };
        await using var window = new OrderedUnits<Group, Result>(probe, Limits);
        Result first = (await window.NextAsync())!;
        probe.Planned[0].Release();
        Assert.Equal(0, probe.Planned[0].Freed); // released, but a unit of it is still to be taken
        Result second = (await window.NextAsync())!;
        Assert.Equal(0, probe.Planned[0].Freed); // its last unit taken, but kept
        probe.Planned[0].Release();
        Assert.Equal(1, probe.Planned[0].Freed);
        first.Dispose();
        second.Dispose();
        Assert.Equal("1.0", (await window.NextAsync())!.ToString());
    }

    [Fact]
    public async Task RowsToPlan_StopPlanning_AndTheLastUnitsAreToldSo()
    {
        // Rows count once a group joins: one group at a time, the second passes 500 rows and no third is planned.
        var bounded = new Probe([.. Enumerable.Repeat((4, 0, -1, -1), 10)]) { Rows = 100 };
        await using (var window = new OrderedUnits<Group, Result>(bounded, Limits with { Groups = 1, RowsToPlan = 500 }))
        {
            var (taken, error) = await Drain(window);
            Assert.Null(error);
            Assert.Equal([.. Units(0, 4), .. Units(1, 4)], taken);
        }
        Assert.Equal(2, bounded.Plans);

        var told = new Probe((3, 0, -1, -1), (4, 0, -1, -1));
        await using (var window = new OrderedUnits<Group, Result>(told, Limits with { FewUnits = 2 }))
        {
            var (taken, error) = await Drain(window);
            Assert.Null(error);
            Assert.Equal(7, taken.Count);
        }
        Assert.Equal(2, told.Lasts); // the last two units started once no group was left
    }
}
