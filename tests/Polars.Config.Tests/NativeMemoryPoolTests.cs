using Kaminari.Sazanami.Helpers;

namespace Polars.Config.Tests;

/// <summary>
/// The process-wide native memory pool: reuse, the retention limit and the release of idle blocks. Global state,
/// hence here, in the serial project; the timer is off and ticks are driven by hand.
/// </summary>
public sealed unsafe class NativeMemoryPoolTests : IDisposable
{
    public NativeMemoryPoolTests()
    {
        // Batches of earlier tests may still be freed by their finalizers.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        NativeMemoryPool.TimerEnabled = false;
        NativeMemoryPool.Trim();
    }

    public void Dispose()
    {
        NativeMemoryPool.RetainLimit = 1L << 30;
        NativeMemoryPool.Trim();
        NativeMemoryPool.TimerEnabled = true;
    }

    [Fact]
    public void FreedBlocks_AreAlignedAndReused_SmallOnesAreNotKept()
    {
        byte* a = NativeMemoryPool.Alloc(100_000);
        long usable = NativeMemoryPool.UsableSize(a);
        Assert.Equal(0, (nint)a % NativeMemoryPool.Alignment);
        Assert.InRange(usable, 100_000, 125_000);
        new Span<byte>(a, (int)usable).Fill(0xAB);
        NativeMemoryPool.Free(a);
        Assert.Equal(usable, NativeMemoryPool.RetainedBytes);
        byte* b = NativeMemoryPool.Alloc(100_000);
        Assert.True(a == b);
        Assert.Equal(0, NativeMemoryPool.RetainedBytes);

        // The block freed last (still warm in the caches) is handed out first.
        byte* c = NativeMemoryPool.Alloc(100_000);
        NativeMemoryPool.Free(b);
        NativeMemoryPool.Free(c);
        Assert.True(NativeMemoryPool.Alloc(100_000) == c);
        NativeMemoryPool.Free(c);

        NativeMemoryPool.Trim();
        byte* small = NativeMemoryPool.Alloc(1_000);
        Assert.Equal(0, (nint)small % NativeMemoryPool.Alignment);
        NativeMemoryPool.Free(small);
        NativeMemoryPool.Free(null);
        Assert.Equal(0, NativeMemoryPool.RetainedBytes);
    }

    [Fact]
    public void RetainLimit_CapsTheKeptBytes()
    {
        NativeMemoryPool.RetainLimit = 200 * 1024;
        byte*[] blocks = [NativeMemoryPool.Alloc(64 * 1024), NativeMemoryPool.Alloc(64 * 1024), NativeMemoryPool.Alloc(64 * 1024), NativeMemoryPool.Alloc(64 * 1024)];
        foreach (byte* block in blocks) NativeMemoryPool.Free(block);
        Assert.Equal(3 * 64 * 1024, NativeMemoryPool.RetainedBytes);
    }

    [Fact]
    public void BlocksUnusedForAWholeTick_AreReleased()
    {
        long size = NativeMemoryPool.ClassSize(NativeMemoryPool.ClassOf(64 * 1024));
        byte* a = NativeMemoryPool.Alloc(64 * 1024), b = NativeMemoryPool.Alloc(64 * 1024);
        NativeMemoryPool.Free(a);
        NativeMemoryPool.Free(b);
        NativeMemoryPool.Tick();                         // both arrived during this tick: kept
        Assert.Equal(2 * size, NativeMemoryPool.RetainedBytes);
        byte* again = NativeMemoryPool.Alloc(64 * 1024); // one is used during the next tick
        NativeMemoryPool.Free(again);
        NativeMemoryPool.Tick();                         // the other stayed unused all along: released
        Assert.Equal(size, NativeMemoryPool.RetainedBytes);
        NativeMemoryPool.Tick();
        Assert.Equal(0, NativeMemoryPool.RetainedBytes);
    }

    [Fact]
    public void PooledByteBuffer_GrowsKeepingItsContents_AndCannotBeDetached()
    {
        var buffer = new NativeByteBuffer(70_000, pooled: true);
        buffer.Span[..70_000].Fill(7);
        buffer.EnsureCapacity(300_000);
        Assert.True(buffer.Span.Length >= 300_000);
        Assert.Equal(-1, buffer.Span[..70_000].IndexOfAnyExcept((byte)7));
        Assert.Throws<InvalidOperationException>(() => buffer.Detach());
        buffer.Release();
        Assert.True(NativeMemoryPool.RetainedBytes >= 300_000);
    }
}
