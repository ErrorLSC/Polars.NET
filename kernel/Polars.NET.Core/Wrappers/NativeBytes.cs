using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Polars.NET.Core.Wrappers;

/// <summary>
/// Bytes handed to the engine for an in-memory scan: either copied during the call, or shared with the engine,
/// which reads them in place and releases them when the last plan reading them is dropped.
/// </summary>
public readonly unsafe ref struct NativeBytes
{
    private readonly ReadOnlySpan<byte> _copy;
    private readonly ReadOnlyMemory<byte> _shared;
    private readonly bool _isShared;
    private readonly int _length;

    private NativeBytes(ReadOnlySpan<byte> copy)
    {
        _copy = copy;
        _length = copy.Length;
    }

    private NativeBytes(ReadOnlyMemory<byte> shared)
    {
        _shared = shared;
        _length = shared.Length;
        _isShared = true;
    }

    /// <summary>Bytes the engine copies during the call.</summary>
    public static NativeBytes Copy(ReadOnlySpan<byte> bytes) => new(bytes);

    /// <summary>
    /// Memory the engine reads in place: it is pinned until the last plan reading it is dropped (possibly on an
    /// engine thread, after the frames are released), and must not change or be freed before it is unpinned.
    /// </summary>
    public static NativeBytes Share(ReadOnlyMemory<byte> memory) => new(memory);

    /// <summary>The bytes to fix for the native call (empty when shared).</summary>
    internal ReadOnlySpan<byte> CopySource => _copy;

    /// <summary>The number of bytes.</summary>
    public int Length => _length;

    /// <summary>
    /// The pointer, length and release callback for the native call, given <see cref="CopySource"/> fixed. Shared
    /// memory is pinned here, and the native side owns the pin from the call on (it releases it also when the call
    /// fails): call this right before the native function.
    /// </summary>
    internal Lease Acquire(byte* fixedCopy)
    {
        if (!_isShared)
            return new Lease(fixedCopy, (nuint)_length, null, null);

        var pinned = new PinnedMemory(_shared.Pin());
        var handle = GCHandle.Alloc(pinned);
        return new Lease((byte*)pinned.Handle.Pointer, (nuint)_length, &Release, (void*)GCHandle.ToIntPtr(handle));
    }

    [UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]
    private static void Release(void* userData) => Unpin(userData);

    private static void Unpin(void* userData)
    {
        var handle = GCHandle.FromIntPtr((IntPtr)userData);
        var pinned = (PinnedMemory)handle.Target!;
        handle.Free();
        // Runs on an engine thread: an exception from a MemoryManager's Unpin must not escape into native code.
        try { pinned.Handle.Dispose(); }
        catch { /* the memory stays pinned; nothing else to do */ }
    }

    private sealed class PinnedMemory(MemoryHandle handle)
    {
        public MemoryHandle Handle = handle;
    }

    /// <summary>
    /// Releases the pin of a shared lease whose native call was never reached (a managed exception before the
    /// call); the native side releases it in every other case.
    /// </summary>
    internal static void ReleaseUnused(in Lease lease)
    {
        if (lease.Release != null)
            Unpin(lease.UserData);
    }

    internal readonly struct Lease(byte* pointer, nuint length, delegate* unmanaged[Cdecl]<void*, void> release, void* userData)
    {
        public readonly byte* Pointer = pointer;
        public readonly nuint Length = length;
        public readonly delegate* unmanaged[Cdecl]<void*, void> Release = release;
        public readonly void* UserData = userData;
    }
}
