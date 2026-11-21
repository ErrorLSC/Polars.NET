using Kaminari.Sazanami.Buffers;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="NativeArray{T}"/>: growth keeps what it is told to keep and nothing more (the rest is undefined: 0xCC
/// here, see <see cref="Poisoned"/>), reserving keeps nothing, the indexer is checked, disposal empties it.
/// </summary>
public class NativeArrayTests
{
    [Fact]
    public void Grow_KeepsTheFirstValues_TheRestIsUndefined()
    {
        using var array = new NativeArray<int>(10);
        Assert.InRange(array.Capacity, 10, 64);
        for (int i = 0; i < array.Capacity; i++) array[i] = i + 1;
        int before = array.Capacity;
        array.Grow(before + 1, keep: 5);
        Assert.True(array.Capacity >= 2 * before);
        Assert.Equal([1, 2, 3, 4, 5], array.AsSpan(0, 5).ToArray());
        Assert.All(array.AsSpan(5).ToArray(), v => Assert.Equal(unchecked((int)0xCCCCCCCC), v));

        // Already large enough: nothing moves.
        array[0] = 42;
        array.Grow(array.Capacity, keep: 0);
        Assert.Equal(42, array[0]);
    }

    [Fact]
    public void Grow_ToPooledSizes_UsesTheWholeBlock()
    {
        using var array = new NativeArray<long>();
        array.Grow(100_000, keep: 0);
        // A whole block of the request's class or, when the pool kept one from earlier use, of the next two.
        int c = NativeMemoryPool.ClassOf(100_000 * sizeof(long));
        long[] whole = [.. Enumerable.Range(c, 3).Select(k => NativeMemoryPool.ClassSize(k) / sizeof(long))];
        Assert.Contains((long)array.Capacity, whole);
        array.Span.Fill(7);
        array.Grow(array.Capacity + 1, keep: array.Capacity);
        Assert.All(array.AsSpan(0, 100_000).ToArray(), v => Assert.Equal(7, v));
    }

    [Fact]
    public void Reserve_KeepsNothing_UnlessLargeEnough()
    {
        using var array = new NativeArray<byte>(16);
        array.Span.Fill(1);
        array.Reserve(array.Capacity);
        Assert.All(array.Span.ToArray(), v => Assert.Equal(1, v));
        array.Reserve(1000);
        Assert.True(array.Capacity >= 1000);
        Assert.All(array.Span.ToArray(), v => Assert.Equal(0xCC, v));
        array.Clear();
        Assert.All(array.Span.ToArray(), v => Assert.Equal(0, v));
    }

    [Fact]
    public void TheIndexer_IsChecked_AndDisposalEmpties()
    {
        var array = new NativeArray<uint>(4);
        Assert.Throws<IndexOutOfRangeException>(() => array[array.Capacity]);
        Assert.Throws<IndexOutOfRangeException>(() => array[-1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => array.Grow(array.Capacity * 4, keep: array.Capacity + 1));
        array.Dispose();
        Assert.Equal(0, array.Capacity);
        Assert.True(array.Span.IsEmpty);
        array.Dispose();
        array.Grow(3, keep: 0); // usable again
        Assert.True(array.Capacity >= 3);
        array.Dispose();
    }
}
