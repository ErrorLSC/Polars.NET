using System.Runtime.InteropServices;

namespace Kaminari.Sazanami.Buffers;

/// <summary>
/// Whole pages straight from the operating system (Linux and macOS <c>mmap</c>, Windows <c>VirtualAlloc</c>), given
/// back to it when freed. Large blocks are kept out of the C library's heap: glibc puts blocks of any size in its
/// arenas once its mmap threshold has risen, and keeps what is freed there (measured, a thousand-file Delta write:
/// arenas of 3.6 GB, 2.6 GB of it free — resident memory that never drops). Pages come zeroed.
/// </summary>
internal static unsafe partial class OsPages
{
    private const int ProtRead = 1, ProtWrite = 2;
    private const int MapPrivate = 0x02;
    private const uint MemCommit = 0x1000, MemReserve = 0x2000, MemRelease = 0x8000, PageReadWrite = 0x04;

    /// <summary>Whether pages come from here on this system (else callers use the C library's allocator).</summary>
    public static bool Supported { get; } = OperatingSystem.IsLinux() || OperatingSystem.IsMacOS() || OperatingSystem.IsWindows();

    /// <summary>The page size: a mapping's length is a multiple of it.</summary>
    public static long PageSize { get; } = Environment.SystemPageSize;

    /// <summary>
    /// The anonymous-mapping flag of <c>mmap</c>: fixed by the kernel's ABI on Linux (glibc and musl alike, 0x20), and
    /// 0x1000 on macOS.
    /// </summary>
    private static readonly int MapAnonymous = OperatingSystem.IsMacOS() ? 0x1000 : 0x20;

    /// <summary><paramref name="bytes"/> rounded up to whole pages.</summary>
    public static long Round(long bytes) => (bytes + PageSize - 1) / PageSize * PageSize;

    /// <summary>
    /// A mapping of <paramref name="bytes"/> (a multiple of <see cref="PageSize"/>), page-aligned and zeroed. Free it
    /// with <see cref="Free"/> and the same length.
    /// </summary>
    /// <exception cref="OutOfMemoryException">The system refused.</exception>
    public static byte* Alloc(long bytes)
    {
        if (OperatingSystem.IsWindows())
        {
            void* pages = VirtualAlloc(null, (nuint)bytes, MemCommit | MemReserve, PageReadWrite);
            return pages != null ? (byte*)pages
                : throw new OutOfMemoryException($"VirtualAlloc of {bytes} bytes failed (error {Marshal.GetLastPInvokeError()}).");
        }
        void* map = mmap(null, (nuint)bytes, ProtRead | ProtWrite, MapPrivate | MapAnonymous, -1, 0);
        return map != (void*)-1 ? (byte*)map
            : throw new OutOfMemoryException($"mmap of {bytes} bytes failed (errno {Marshal.GetLastPInvokeError()}).");
    }

    /// <summary>Gives a mapping from <see cref="Alloc"/> back to the system.</summary>
    public static void Free(void* pages, long bytes)
    {
        if (OperatingSystem.IsWindows()) VirtualFree(pages, 0, MemRelease);
        else munmap(pages, (nuint)bytes);
    }

    [LibraryImport("libc", SetLastError = true)]
    internal static partial void* mmap(void* address, nuint length, int protection, int flags, int fd, nint offset);

    [LibraryImport("libc", SetLastError = true)]
    internal static partial int mprotect(void* address, nuint length, int protection);

    [LibraryImport("libc", SetLastError = true)]
    internal static partial int munmap(void* address, nuint length);

    [LibraryImport("kernel32", SetLastError = true)]
    private static partial void* VirtualAlloc(void* address, nuint size, uint allocationType, uint protect);

    [LibraryImport("kernel32", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool VirtualFree(void* address, nuint size, uint freeType);
}
