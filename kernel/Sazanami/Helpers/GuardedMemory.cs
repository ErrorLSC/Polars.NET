using System.Buffers;
using System.Runtime.InteropServices;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// Memory that ends right before an inaccessible page, so that a read or write past its end faults at once instead
/// of silently touching other data: the fuzzer's way of catching over-reads in unsafe decoders (an over-read that
/// stays inside a larger buffer otherwise goes unnoticed). One mapping per block — for fuzzing only, never on by
/// default. Linux only.
/// </summary>
internal static unsafe partial class GuardedMemory
{
    private const int ProtNone = 0, ProtRead = 1, ProtWrite = 2;
    private const int MapPrivate = 0x02, MapAnonymous = 0x20;

    /// <summary>
    /// Whether <see cref="NativeMemoryPool"/> and the Parquet page reader put a guard page after every block (set by
    /// the fuzzer at start-up, before any allocation; blocks made before are freed the ordinary way).
    /// </summary>
    internal static bool Enabled
    {
        get => s_enabled;
        set
        {
            if (value && !OperatingSystem.IsLinux()) throw new PlatformNotSupportedException("Guard pages are made with Linux mmap.");
            s_enabled = value;
        }
    }

    private static bool s_enabled;

    /// <summary>
    /// <paramref name="length"/> bytes (rounded up to <paramref name="alignment"/>, a power of two) ending at a guard
    /// page; the start is aligned. Free with <see cref="Free"/> and the same length and alignment.
    /// </summary>
    public static byte* Alloc(long length, int alignment = 1)
    {
        long page = Environment.SystemPageSize;
        long size = Size(length, alignment);
        long data = (size + page - 1) / page * page;
        void* map = mmap(null, (nuint)(data + page), ProtRead | ProtWrite, MapPrivate | MapAnonymous, -1, 0);
        if (map == (void*)-1) throw new OutOfMemoryException($"mmap of {data + page} bytes failed (errno {Marshal.GetLastPInvokeError()}).");
        byte* guard = (byte*)map + data;
        if (mprotect(guard, (nuint)page, ProtNone) != 0)
        {
            int errno = Marshal.GetLastPInvokeError();
            munmap(map, (nuint)(data + page));
            throw new OutOfMemoryException($"mprotect of a guard page failed (errno {errno}).");
        }
        return guard - size;
    }

    /// <summary>Unmaps a block from <see cref="Alloc"/> (its guard page included).</summary>
    public static void Free(byte* start, long length, int alignment = 1)
    {
        long page = Environment.SystemPageSize;
        byte* guard = start + Size(length, alignment);
        byte* map = (byte*)((nint)start & ~(nint)(page - 1));
        munmap(map, (nuint)(guard + page - map));
    }

    private static long Size(long length, int alignment) => (Math.Max(length, 0) + alignment - 1) & ~(long)(alignment - 1);

    /// <summary>
    /// A block of exactly its length ending at a guard page, as <see cref="Memory{T}"/>; unmapped when disposed (no
    /// finalizer: spans of it may outlive the last reference to it).
    /// </summary>
    public sealed class Block : MemoryManager<byte>
    {
        private byte* _start;
        private readonly int _length;

        public Block(int length)
        {
            _length = length;
            _start = Alloc(length);
        }

        /// <summary>A guarded copy of <paramref name="bytes"/>.</summary>
        public static Block Copy(ReadOnlySpan<byte> bytes)
        {
            var block = new Block(bytes.Length);
            bytes.CopyTo(block.GetSpan());
            return block;
        }

        public override Span<byte> GetSpan() => _start == null ? throw new ObjectDisposedException(nameof(Block)) : new(_start, _length);

        public override MemoryHandle Pin(int elementIndex = 0) => new(_start + elementIndex);

        public override void Unpin() { }

        protected override void Dispose(bool disposing)
        {
            if (_start == null) return;
            Free(_start, _length);
            _start = null;
        }
    }

    [LibraryImport("libc", SetLastError = true)]
    private static partial void* mmap(void* address, nuint length, int protection, int flags, int fd, nint offset);

    [LibraryImport("libc", SetLastError = true)]
    private static partial int mprotect(void* address, nuint length, int protection);

    [LibraryImport("libc", SetLastError = true)]
    private static partial int munmap(void* address, nuint length);
}
