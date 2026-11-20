using Kaminari.Sazanami.Buffers;

namespace Polars.Config.Tests;

/// <summary>
/// The process-wide native memory pool: reuse (across classes too), the retention cap and limit, and the release of
/// idle blocks. Global state,
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
        NativeMemoryPool.RetainFloor = 64L << 20;
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
    public void PooledSizes_ArePagesOfTheirOwn_GivenBackWhenReleased()
    {
        // Blocks of a pooled size (and larger) are mappings of the system's, not the C library's heap: released, their
        // pages go back at once (glibc's arenas keep what is freed in them).
        if (!OperatingSystem.IsLinux()) return;
        NativeMemoryPool.RetainLimit = 0;
        foreach (long size in (long[])[64 * 1024, 100_000, 3 << 20, (1L << 30) + 1])
        {
            byte* p = NativeMemoryPool.Alloc(size);
            Assert.Equal(0, (long)p % NativeMemoryPool.Alignment);
            long usable = NativeMemoryPool.UsableSize(p);
            Assert.InRange(usable, size, long.MaxValue);
            p[0] = 1;
            p[usable - 1] = 2; // the last usable byte is the block's
            Assert.True(IsMapped((nint)p), $"{size}: mapped");
            NativeMemoryPool.Free(p);
            Assert.False(IsMapped((nint)p), $"{size}: unmapped once released");
        }

        static bool IsMapped(nint address) => File.ReadLines("/proc/self/maps").Any(line =>
        {
            string[] range = line[..line.IndexOf(' ')].Split('-');
            return Convert.ToInt64(range[0], 16) <= address && address < Convert.ToInt64(range[1], 16);
        });
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
    public void AMiss_TakesABlockUpToTwoClassesLarger_WhichGoesBackToItsOwnClass()
    {
        long live = NativeMemoryPool.LiveBytes;
        int c = NativeMemoryPool.ClassOf(64 * 1024);
        long own = NativeMemoryPool.ClassSize(c), next = NativeMemoryPool.ClassSize(c + 1), second = NativeMemoryPool.ClassSize(c + 2);

        byte* larger = NativeMemoryPool.Alloc(second);
        NativeMemoryPool.Free(larger);
        byte* p = NativeMemoryPool.Alloc(64 * 1024);
        Assert.True(p == larger);
        Assert.Equal(second, NativeMemoryPool.UsableSize(p));
        Assert.Equal(0, NativeMemoryPool.RetainedBytes);
        NativeMemoryPool.Free(p);
        Assert.Equal(second, NativeMemoryPool.RetainedBytes);
        Assert.True(NativeMemoryPool.Alloc(second) == larger); // freed, it went back to its own class
        NativeMemoryPool.Free(larger);

        // The request's own class comes first, then the next one up.
        NativeMemoryPool.Trim();
        byte* a = NativeMemoryPool.Alloc(own), b = NativeMemoryPool.Alloc(next), d = NativeMemoryPool.Alloc(second);
        NativeMemoryPool.Free(b);
        NativeMemoryPool.Free(a);
        NativeMemoryPool.Free(d);
        Assert.True(NativeMemoryPool.Alloc(64 * 1024) == a);
        Assert.True(NativeMemoryPool.Alloc(64 * 1024) == b);
        Assert.True(NativeMemoryPool.Alloc(64 * 1024) == d);
        NativeMemoryPool.Free(a);
        NativeMemoryPool.Free(b);
        NativeMemoryPool.Free(d);

        // Three classes larger wastes too much: left alone, the request maps a block of its own class.
        NativeMemoryPool.Trim();
        long third = NativeMemoryPool.ClassSize(c + 3);
        byte* far = NativeMemoryPool.Alloc(third);
        NativeMemoryPool.Free(far);
        byte* fresh = NativeMemoryPool.Alloc(64 * 1024);
        Assert.False(fresh == far);
        Assert.Equal(own, NativeMemoryPool.UsableSize(fresh));
        Assert.Equal(third, NativeMemoryPool.RetainedBytes);
        NativeMemoryPool.Free(fresh);
        Assert.Equal(live, NativeMemoryPool.LiveBytes); // a borrowed block counted in use at its own size, as when freed
    }

    [Fact]
    public void ABlockTakenForASmallerRequest_CountsAsUsedInItsOwnClass()
    {
        long next = NativeMemoryPool.ClassSize(NativeMemoryPool.ClassOf(64 * 1024) + 1);
        NativeMemoryPool.Free(NativeMemoryPool.Alloc(next));
        NativeMemoryPool.Tick();                             // kept: it arrived during this tick
        NativeMemoryPool.Free(NativeMemoryPool.Alloc(64 * 1024)); // used during the next one, by a smaller request
        NativeMemoryPool.Tick();
        Assert.Equal(next, NativeMemoryPool.RetainedBytes);
    }

    [Fact]
    public void TheKeptBytes_FollowTheBytesInUse_AndTheirPeakFades()
    {
        NativeMemoryPool.RetainFloor = 0;
        long baseline = NativeMemoryPool.LiveBytes; // blocks held elsewhere in the process (normally none)
        long small = NativeMemoryPool.ClassSize(NativeMemoryPool.ClassOf(64 * 1024));
        long big = NativeMemoryPool.ClassSize(NativeMemoryPool.ClassOf(1 << 20)); // far above: never taken for a small one

        // Eight small blocks in use at once, then freed: all kept, the peak in use allows that much.
        FreeAll(AllocAll(8, 64 * 1024));
        Assert.Equal(8 * small, NativeMemoryPool.RetainedBytes);
        Assert.Equal(baseline + 8 * small, NativeMemoryPool.RetainCap);

        // Two big ones in use at once: the peak rises to theirs, and the kept small ones count against it.
        FreeAll(AllocAll(2, 1 << 20));
        Assert.Equal(baseline + 2 * big, NativeMemoryPool.RetainCap);
        Assert.Equal(8 * small + (baseline >= 8 * small ? 2 : 1) * big, NativeMemoryPool.RetainedBytes);

        // The tick carries the peak over; the next one halves it (unless that tick's own peak is higher).
        NativeMemoryPool.Tick();
        Assert.Equal(baseline + 2 * big, NativeMemoryPool.RetainCap);
        NativeMemoryPool.Free(NativeMemoryPool.Alloc(64 * 1024)); // one small block stays in use, the others are idle
        NativeMemoryPool.Tick();
        Assert.Equal(small, NativeMemoryPool.RetainedBytes);
        Assert.Equal(Math.Max(baseline + small, (baseline + 2 * big) / 2), NativeMemoryPool.RetainCap);

        // Nothing kept any more: the older peaks are forgotten.
        NativeMemoryPool.Tick();
        Assert.Equal(0, NativeMemoryPool.RetainedBytes);
        Assert.Equal(baseline, NativeMemoryPool.RetainCap);

        static nint[] AllocAll(int count, long bytes)
        {
            nint[] blocks = new nint[count];
            for (int i = 0; i < count; i++) blocks[i] = (nint)NativeMemoryPool.Alloc(bytes);
            return blocks;
        }

        static void FreeAll(nint[] blocks)
        {
            foreach (nint block in blocks) NativeMemoryPool.Free((void*)block);
        }
    }

    [Fact]
    public void TheCap_IsAtLeastTheFloor_AndAtMostTheLimit()
    {
        long baseline = NativeMemoryPool.LiveBytes;
        Assert.Equal(Math.Max(64L << 20, baseline), NativeMemoryPool.RetainCap);
        NativeMemoryPool.RetainLimit = 1 << 20;
        Assert.Equal(1 << 20, NativeMemoryPool.RetainCap);
    }

    [Theory]
    [InlineData("512MiB", 512L << 20)]
    [InlineData(" 2 GiB ", 2L << 30)]
    [InlineData("0", 0L)]
    [InlineData(null, 1L << 30)]
    [InlineData(" ", 1L << 30)]
    [InlineData("lots", 1L << 30)]
    [InlineData("150%", 1L << 30)]
    public void TheRetainSetting_IsASize_ElseTheDefault(string? setting, long limit) =>
        Assert.Equal(limit, NativeMemoryPool.DefaultRetainLimit(setting));

    [Fact]
    public void TheRetainSetting_MayBeAShareOfTheMemoryTheProcessMayUse() =>
        Assert.Equal((long)(GC.GetGCMemoryInfo().TotalAvailableMemoryBytes * 10.0 / 100), NativeMemoryPool.DefaultRetainLimit("10%"));

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
    public void ABlockFreedOnAnotherThread_IsHandedOutHere()
    {
        nint freed = 0;
        var other = new Thread(() =>
        {
            byte* p = NativeMemoryPool.Alloc(200_000);
            freed = (nint)p;
            NativeMemoryPool.Free(p);
        });
        other.Start();
        other.Join();
        Assert.Equal(NativeMemoryPool.ClassSize(NativeMemoryPool.ClassOf(200_000)), NativeMemoryPool.RetainedBytes);
        byte* here = NativeMemoryPool.Alloc(200_000); // this thread's shard is empty: taken from the other's
        Assert.Equal(freed, (nint)here);
        Assert.Equal(0, NativeMemoryPool.RetainedBytes);
        NativeMemoryPool.Free(here);
    }

    [Fact]
    public void ManyThreads_AllocatingAndFreeing_KeepTheCountsRight()
    {
        long live = NativeMemoryPool.LiveBytes;
        long[] sizes = [64 * 1024, 100_000, 256 * 1024, 1 << 20];
        Parallel.For(0, 64, new ParallelOptions { MaxDegreeOfParallelism = 16 }, t =>
        {
            var random = new Random(t);
            var held = new List<nint>();
            for (int i = 0; i < 2000; i++)
            {
                if (held.Count > 0 && random.Next(2) == 0)
                {
                    int k = random.Next(held.Count);
                    NativeMemoryPool.Free((void*)held[k]);
                    held.RemoveAt(k);
                }
                else
                {
                    byte* p = NativeMemoryPool.Alloc(sizes[random.Next(sizes.Length)]);
                    *p = 1;
                    held.Add((nint)p);
                }
            }
            foreach (nint p in held) NativeMemoryPool.Free((void*)p);
        });
        Assert.InRange(NativeMemoryPool.RetainedBytes, 1, NativeMemoryPool.RetainLimit);
        Assert.Equal(live, NativeMemoryPool.LiveBytes);
        NativeMemoryPool.Trim();
        Assert.Equal(0, NativeMemoryPool.RetainedBytes);
        // Every class is empty again: a fresh allocation comes from the system, then is kept once freed.
        byte* fresh = NativeMemoryPool.Alloc(64 * 1024);
        NativeMemoryPool.Free(fresh);
        Assert.Equal(NativeMemoryPool.ClassSize(NativeMemoryPool.ClassOf(64 * 1024)), NativeMemoryPool.RetainedBytes);
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
