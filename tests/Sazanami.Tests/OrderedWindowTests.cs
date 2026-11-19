using Kaminari.Sazanami.Concurrency;

namespace Sazanami.Tests;

/// <summary><see cref="OrderedWindow{T}"/>: items in order, a bounded window, results never taken disposed.</summary>
public class OrderedWindowTests
{
    private static readonly Lock Gate = new();

    private sealed class Result(int item, List<int> disposed) : IDisposable
    {
        public int Item { get; } = item;

        public void Dispose()
        {
            lock (Gate) disposed.Add(Item);
        }
    }

    [Fact]
    public async Task Results_ComeInOrder_AndThoseNotTakenAreDisposed()
    {
        List<int> disposed = [];
        int started = 0;
        using (var window = new OrderedWindow<Result>(20, (i, _) =>
               {
                   Interlocked.Increment(ref started);
                   return Task.FromResult(new Result(i, disposed));
               }, window: 4, size: _ => 1, budget: long.MaxValue))
        {
            for (int i = 0; i < 6; i++)
            {
                using Result result = await window.Take(i);
                Assert.Equal(i, result.Item);
            }
            // The window opens 1, 2, 4 items ahead: items 0 to 8 are on the pool (waited for: one not running yet when
            // the window is disposed would be canceled, never started).
            Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref started) == 9, TimeSpan.FromSeconds(10)));
        }
        // Six taken (disposed by the consumer), the rest started ahead disposed by the window, nothing past the window started.
        Assert.Equal(9, started);
        Assert.Equal(Enumerable.Range(0, started), disposed.Order());
    }

    [Fact]
    public async Task TheBudget_LimitsTheItemsAhead_ButTheNextAlwaysStarts()
    {
        int running = 0, most = 0;
        using var window = new OrderedWindow<Result>(12, async (i, _) =>
        {
            int now = Interlocked.Increment(ref running);
            lock (Gate) most = Math.Max(most, now);
            await Task.Delay(5);
            Interlocked.Decrement(ref running);
            return new Result(i, []);
        }, window: 8, size: _ => 100, budget: 250);
        for (int i = 0; i < 12; i++) (await window.Take(i)).Dispose();
        Assert.InRange(most, 1, 3);
    }

    [Fact]
    public async Task TakingAfterDispose_Throws()
    {
        var window = new OrderedWindow<Result>(3, (i, _) => Task.FromResult(new Result(i, [])), 2, _ => 0, 0);
        (await window.Take(0)).Dispose();
        window.Dispose();
        window.Dispose();
        Assert.Throws<ObjectDisposedException>(() => { _ = window.Take(1); return 0; });
    }
}
