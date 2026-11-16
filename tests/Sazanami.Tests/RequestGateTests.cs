using Kaminari.Sazanami.Network;

namespace Sazanami.Tests;

/// <summary><see cref="RequestGate"/>: the bound, the order waiters are let in, cancelled waiters.</summary>
public sealed class RequestGateTests
{
    [Fact]
    public async Task AtMostCapacity_AreIn_TheNextLetInOnRelease()
    {
        var gate = new RequestGate(2);
        RequestGate.Lease a = await gate.EnterAsync(0), b = await gate.EnterAsync(0);
        ValueTask<RequestGate.Lease> c = gate.EnterAsync(0);
        Assert.False(c.IsCompleted);
        Assert.Equal(2, gate.Inside);
        a.Dispose();
        using (await c) Assert.Equal(2, gate.Inside);
        b.Dispose();
        Assert.Equal(0, gate.Inside);
        Assert.Equal(2, gate.PeakInside);
        Assert.Throws<ArgumentOutOfRangeException>(() => new RequestGate(0));
    }

    [Fact]
    public async Task Waiters_AreLetIn_LowestOrderFirst_ThenFirstCome()
    {
        var gate = new RequestGate(1);
        RequestGate.Lease held = await gate.EnterAsync(0);
        List<string> order = [];
        Lock orderLock = new();
        async Task Wait(long at, string name)
        {
            using RequestGate.Lease lease = await gate.EnterAsync(at);
            lock (orderLock) order.Add(name);
        }
        Task[] waiting = [Wait(5, "5"), Wait(1, "1a"), Wait(3, "3"), Wait(1, "1b")];
        held.Dispose();
        await Task.WhenAll(waiting);
        Assert.Equal(["1a", "1b", "3", "5"], order);
        Assert.Equal(0, gate.Inside);
    }

    [Fact]
    public async Task ACancelledWaiter_IsPassedOver()
    {
        var gate = new RequestGate(1);
        RequestGate.Lease held = await gate.EnterAsync(0);
        using var cancel = new CancellationTokenSource();
        ValueTask<RequestGate.Lease> first = gate.EnterAsync(1, cancel.Token);
        ValueTask<RequestGate.Lease> second = gate.EnterAsync(2);
        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await first);
        held.Dispose();
        using (await second) Assert.Equal(1, gate.Inside);
        Assert.Equal(0, gate.Inside);
        ValueTask<RequestGate.Lease> late = gate.EnterAsync(0, cancel.Token); // cancelled already: a cancelled wait, not a throw
        Assert.True(late.IsCanceled);
        Assert.Equal(0, gate.Inside);
    }
}
