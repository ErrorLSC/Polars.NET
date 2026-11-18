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
    public void Results_ComeInOrder_AndThoseNotTakenAreDisposed()
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
                using Result result = window.Take(i).GetAwaiter().GetResult();
                Assert.Equal(i, result.Item);
            }
        }
        // Six taken (disposed by the consumer), the rest started ahead disposed by the window, nothing past the window started.
        Assert.InRange(started, 7, 10);
        Assert.Equal(Enumerable.Range(0, started), disposed.Order());
    }

    [Fact]
    public void TheBudget_LimitsTheItemsAhead_ButTheNextAlwaysStarts()
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
        for (int i = 0; i < 12; i++) window.Take(i).GetAwaiter().GetResult().Dispose();
        Assert.InRange(most, 1, 3);
    }

    [Fact]
    public void TakingAfterDispose_Throws()
    {
        var window = new OrderedWindow<Result>(3, (i, _) => Task.FromResult(new Result(i, [])), 2, _ => 0, 0);
        window.Take(0).Result.Dispose();
        window.Dispose();
        window.Dispose();
        Assert.Throws<ObjectDisposedException>(() => { _ = window.Take(1); return 0; });
    }
}
