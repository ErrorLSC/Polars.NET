using System.Numerics;
using System.Runtime.InteropServices;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// The process's native memory for large buffers (Arrow column buffers, file ranges, decompressed pages): blocks
/// freed here are kept and handed out again instead of going back to the system allocator, which for large sizes
/// maps fresh pages every time (glibc beyond its mmap threshold, musl beyond about 128 KiB) — and every fresh page
/// costs a page fault on first touch. Blocks of 64 KiB to 1 GiB are kept by size class (four per doubling) up to
/// <see cref="RetainLimit"/> bytes in all; a timer, running only while blocks are kept, frees those that stayed
/// unused through a whole tick (one second), so an idle process gives the memory back within about two seconds.
/// Smaller and larger blocks go straight to the system allocator.
/// </summary>
/// <remarks>
/// Every block, pooled or not, starts with a 64-byte header holding its size class and usable size, so any pointer
/// from <see cref="Alloc"/> is freed with <see cref="Free"/> wherever it ends up. Pointers are 64-byte aligned.
/// Safe to use from any thread, finalizers included.
/// </remarks>
internal static unsafe class NativeMemoryPool
{
    public const int Alignment = 64;
    private const int Header = 64;
    private const long Unit = 16 * 1024;               // class sizes are (4..7) × Unit × 2^e: 64, 80, 96, 112, 128, 160 KiB, …
    private const long MinPooled = 4 * Unit;           // 64 KiB
    private const int Classes = 57;                    // the last is 1 GiB
    private const int Unpooled = -1;
    private const int Guarded = -2;                    // a block of its own mapping, ending at a guard page (fuzzing)
    private static readonly TimeSpan TickInterval = TimeSpan.FromSeconds(1);

    private static readonly object _lock = new();
    private static readonly List<nint>[] _free = [.. Enumerable.Range(0, Classes).Select(_ => new List<nint>())];
    private static readonly int[] _lowWater = new int[Classes]; // fewest blocks a class held since the last tick
    private static long _retained;
    private static Timer? _timer;
    private static bool _ticking;

    /// <summary>The most bytes kept in unused blocks (1 GiB); a block freed beyond it goes back to the system.</summary>
    internal static long RetainLimit { get; set; } = 1L << 30;

    /// <summary>Whether the release timer runs (tests turn it off and call <see cref="Tick"/> themselves).</summary>
    internal static bool TimerEnabled { get; set; } = true;

    /// <summary>The bytes now kept in unused blocks.</summary>
    internal static long RetainedBytes
    {
        get
        {
            lock (_lock) return _retained;
        }
    }

    /// <summary>A block of at least <paramref name="bytes"/> bytes, 64-byte aligned, contents undefined.</summary>
    public static byte* Alloc(long bytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(bytes);
        if (GuardedMemory.Enabled) return AllocGuarded(bytes);
        int c = ClassOf(bytes);
        if (c != Unpooled)
        {
            lock (_lock)
            {
                List<nint> free = _free[c];
                if (free.Count > 0)
                {
                    nint block = free[^1];
                    free.RemoveAt(free.Count - 1);
                    _lowWater[c] = Math.Min(_lowWater[c], free.Count);
                    _retained -= ClassSize(c);
                    return (byte*)block + Header;
                }
            }
        }
        long usable = c == Unpooled ? Math.Max(bytes, 8) : ClassSize(c);
        var p = (byte*)NativeMemory.AlignedAlloc((nuint)(usable + Header), Alignment);
        *(int*)p = c;
        *(long*)(p + 8) = usable;
        return p + Header;
    }

    /// <summary>A block that ends (within the alignment) at a guard page: writing or reading past its end faults.</summary>
    private static byte* AllocGuarded(long bytes)
    {
        long usable = (Math.Max(bytes, 8) + Alignment - 1) & ~(long)(Alignment - 1);
        byte* p = GuardedMemory.Alloc(usable + Header, Alignment);
        *(int*)p = Guarded;
        *(long*)(p + 8) = usable;
        return p + Header;
    }

    /// <summary>The bytes the block at <paramref name="pointer"/> can hold (at least what was asked for).</summary>
    public static long UsableSize(void* pointer) => *(long*)((byte*)pointer - Header + 8);

    /// <summary>Frees a block from <see cref="Alloc"/>: kept for reuse, or given back to the system. Null is ignored.</summary>
    public static void Free(void* pointer)
    {
        if (pointer == null) return;
        byte* block = (byte*)pointer - Header;
        int c = *(int*)block;
        if (c == Guarded)
        {
            GuardedMemory.Free(block, *(long*)(block + 8) + Header, Alignment);
            return;
        }
        if (c != Unpooled)
        {
            long size = ClassSize(c);
            lock (_lock)
            {
                if (_retained + size <= RetainLimit)
                {
                    _free[c].Add((nint)block);
                    _retained += size;
                    if (!_ticking) StartTicking();
                    return;
                }
            }
        }
        NativeMemory.AlignedFree(block);
    }

    /// <summary>Gives every kept block back to the system.</summary>
    internal static void Trim()
    {
        List<nint> released = [];
        lock (_lock)
        {
            foreach (List<nint> free in _free)
            {
                released.AddRange(free);
                free.Clear();
            }
            Array.Clear(_lowWater);
            _retained = 0;
        }
        foreach (nint block in released) NativeMemory.AlignedFree((void*)block);
    }

    /// <summary>
    /// One tick of the timer: frees the blocks that stayed unused since the last tick (the oldest of each class, as
    /// many as the class never dropped below), and stops the timer once nothing is kept.
    /// </summary>
    internal static void Tick()
    {
        List<nint> released = [];
        lock (_lock)
        {
            for (int c = 0; c < Classes; c++)
            {
                List<nint> free = _free[c];
                int idle = Math.Min(_lowWater[c], free.Count);
                if (idle > 0)
                {
                    released.AddRange(free.GetRange(0, idle));
                    free.RemoveRange(0, idle);
                    _retained -= idle * ClassSize(c);
                }
                _lowWater[c] = free.Count;
            }
            if (_retained == 0 && _ticking)
            {
                _ticking = false;
                _timer?.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            }
        }
        foreach (nint block in released) NativeMemory.AlignedFree((void*)block);
    }

    // Under the lock.
    private static void StartTicking()
    {
        if (!TimerEnabled) return;
        _ticking = true;
        if (_timer is null)
        {
            // The timer captures no execution context (no async locals of whoever freed first).
            using (ExecutionContext.SuppressFlow()) _timer = new Timer(static _ => Tick(), null, TickInterval, TickInterval);
        }
        else _timer.Change(TickInterval, TickInterval);
    }

    /// <summary>The smallest class holding <paramref name="bytes"/>, or <see cref="Unpooled"/>.</summary>
    internal static int ClassOf(long bytes)
    {
        if (bytes < MinPooled || bytes > ClassSize(Classes - 1)) return Unpooled;
        long units = (bytes + Unit - 1) / Unit;                    // ≥ 4
        int e = BitOperations.Log2((ulong)units) - 2;              // units in [4·2^e, 8·2^e)
        long q = ((units + (1L << e) - 1) >> e) - 4;               // 0..4: the step within the doubling, rounded up
        return q == 4 ? (e + 1) * 4 : e * 4 + (int)q;
    }

    internal static long ClassSize(int c) => (4L + (c & 3)) * Unit << (c >> 2);
}
