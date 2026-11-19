using Kaminari.Sazanami.Concurrency;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="OrderedParallel"/>, sync and async: results in the source's order with up to a window of maps running
/// (opening gradually),
/// failures where the sequential loop would raise them, and no map left running however the consumer stops.
/// </summary>
public class OrderedParallelTests
{
    /// <summary>The maps running now, the most seen at once, and the items taken from the source.</summary>
    private sealed class Probe
    {
        private readonly Lock _lock = new();
        private readonly ManualResetEventSlim _two = new();
        private int _running;
        public int MostAtOnce { get; private set; }
        public int Taken;
        public int Mapped;
        public int Running { get { lock (_lock) return _running; } }

        public int Map(int item)
        {
            lock (_lock)
            {
                MostAtOnce = Math.Max(MostAtOnce, ++_running);
                if (_running >= 2) _two.Set();
            }
            try
            {
                // Two maps meet here when the window lets them run at once: waited for, not left to the thread pool's
                // timing (a busy pool may run them one after the other; a sequential map times out and fails).
                if (item == MeetAt) _two.Wait(TimeSpan.FromSeconds(10));
                Thread.Sleep(item % 3 == 0 ? 15 : 1); // later items finish first
                if (item == FailAt) throw new InvalidOperationException($"map {item}");
                return item * 10;
            }
            finally
            {
                Interlocked.Increment(ref Mapped);
                lock (_lock) _running--;
            }
        }

        public int FailAt { get; init; } = -1;

        /// <summary>The item whose map waits for a second map to run beside it (none by default).</summary>
        public int MeetAt { get; init; } = -1;

        public IEnumerable<int> Source(int count, int failAfter = -1)
        {
            for (int i = 0; i < count; i++)
            {
                if (i == failAfter) throw new IOException("source");
                Interlocked.Increment(ref Taken);
                yield return i;
            }
        }
    }

    private static IEnumerable<int> Run(bool async, IEnumerable<int> source, Func<int, int> map, int window) => async
        ? OrderedParallel.MapAsync(Async(source), map, window).ToBlockingEnumerable()
        : OrderedParallel.Map(source, map, window);

    private static async IAsyncEnumerable<int> Async(IEnumerable<int> source)
    {
        foreach (int item in source)
        {
            await Task.Yield();
            yield return item;
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Results_ComeInTheSourcesOrder_AWindowAtOnce(bool async)
    {
        var probe = new Probe { MeetAt = 1 }; // items 1 and 2 start together once the window opens to two
        Assert.Equal(Enumerable.Range(0, 40).Select(i => i * 10), Run(async, probe.Source(40), probe.Map, 4));
        Assert.InRange(probe.MostAtOnce, 2, 4);
        Assert.Equal(40, probe.Mapped);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AFailingMap_FailsAtItsItem_AfterTheResultsBefore(bool async)
    {
        var probe = new Probe { FailAt = 7 };
        List<int> results = [];
        var error = Assert.Throws<InvalidOperationException>(() =>
        {
            foreach (int result in Run(async, probe.Source(40), probe.Map, 4)) results.Add(result);
        });
        Assert.Equal("map 7", error.Message);
        Assert.Equal(Enumerable.Range(0, 7).Select(i => i * 10), results);
        Assert.Equal(0, probe.Running);
        Assert.Equal(probe.Taken, probe.Mapped); // every item taken was handed to a map that finished
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AFailingSource_FailsAfterTheResultsOfTheItemsTaken(bool async)
    {
        var probe = new Probe();
        List<int> results = [];
        var error = Assert.Throws<IOException>(() =>
        {
            foreach (int result in Run(async, probe.Source(40, failAfter: 9), probe.Map, 4)) results.Add(result);
        });
        Assert.Equal("source", error.Message);
        Assert.Equal(Enumerable.Range(0, 9).Select(i => i * 10), results);
        Assert.Equal(9, probe.Mapped);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void StoppingEarly_WaitsForTheMapsRunning(bool async)
    {
        var probe = new Probe();
        Assert.Equal([0, 10, 20, 30], Run(async, probe.Source(40), probe.Map, 6).Take(4));
        Assert.Equal(0, probe.Running);
        Assert.Equal(probe.Taken, probe.Mapped);
        Assert.Equal(9, probe.Taken); // 1, 2, 4 then 6 in flight: items 0 | 1 2 | 3 4 5 | 6 7 8
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TheWindowOpensGradually_AFirstResultMapsOneItem(bool async)
    {
        var probe = new Probe();
        Assert.Equal(0, Run(async, probe.Source(40), probe.Map, 16).First());
        Assert.Equal(1, probe.Taken);
        Assert.Equal(1, probe.Mapped);
    }

    [Fact]
    public void AWindow_MustBePositive()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => OrderedParallel.Map([1], static (int i) => i, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => OrderedParallel.MapAsync(Async([1]), static (int i) => i, 0));
    }
}
