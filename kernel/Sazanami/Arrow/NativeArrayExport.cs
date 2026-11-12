using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// The Arrow C data interface <c>ArrowArray</c> struct. Apache.Arrow's <see cref="CArrowArray"/> keeps its
/// <c>release</c> member internal, so batches are assembled in this layout-identical struct and imported with
/// <see cref="CArrowArrayImporter"/>.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
internal unsafe struct NativeCArray
{
    public long Length;
    public long NullCount;
    public long Offset;
    public long BufferCount;
    public long ChildCount;
    public byte** Buffers;
    public NativeCArray** Children;
    public NativeCArray* Dictionary;
    public delegate* unmanaged<NativeCArray*, void> Release;
    public void* PrivateData;
}

/// <summary>
/// Collects every native allocation of one record batch (column buffers and the C structs that describe them).
/// The root array's release callback frees them all, which Apache.Arrow calls once every buffer of the imported
/// batch is disposed (or finalized). Builders detach their buffers into it, so nothing is copied.
/// </summary>
internal sealed unsafe class NativeBatchExport
{
    private NativeGrowBuffer _allocations; // void* entries

    /// <summary>Takes ownership of a pointer from <see cref="NativeGrowBuffer.Detach"/> or <see cref="Allocate"/>.</summary>
    public byte* Own(byte* pointer)
    {
        _allocations.Append((nint)pointer);
        return pointer;
    }

    public void* Allocate(long bytes)
    {
        var p = (byte*)NativeMemory.AlignedAlloc((nuint)Math.Max(bytes, 8), NativeGrowBuffer.Alignment);
        NativeMemory.Clear(p, (nuint)Math.Max(bytes, 8));
        return Own(p);
    }

    /// <summary>Creates a child (non-root) array struct with the given buffers and children.</summary>
    public NativeCArray* NewArray(long length, long nullCount, ReadOnlySpan<nint> buffers, ReadOnlySpan<nint> children,
        NativeCArray* dictionary = null)
    {
        var array = (NativeCArray*)Allocate(sizeof(NativeCArray));
        array->Length = length;
        array->NullCount = nullCount;
        array->BufferCount = buffers.Length;
        array->ChildCount = children.Length;
        if (!buffers.IsEmpty)
        {
            array->Buffers = (byte**)Allocate(buffers.Length * sizeof(nint));
            buffers.CopyTo(new Span<nint>(array->Buffers, buffers.Length));
        }
        if (!children.IsEmpty)
        {
            array->Children = (NativeCArray**)Allocate(children.Length * sizeof(nint));
            children.CopyTo(new Span<nint>(array->Children, children.Length));
        }
        array->Dictionary = dictionary;
        array->Release = &ReleaseChild;
        return array;
    }

    /// <summary>
    /// Turns the struct array into the batch root, moves the allocation list to native memory and imports it.
    /// From here on the allocations belong to the returned batch.
    /// </summary>
    public RecordBatch Import(NativeCArray* root, Schema schema)
    {
        long count = _allocations.Length / sizeof(nint);
        var list = (nint*)NativeMemory.Alloc((nuint)((count + 1) * sizeof(nint)));
        list[0] = (nint)count;
        new ReadOnlySpan<nint>(_allocations.Pointer, (int)count).CopyTo(new Span<nint>(list + 1, (int)count));
        _allocations.Free();
        root->PrivateData = list;
        root->Release = &ReleaseRoot;
        return CArrowArrayImporter.ImportRecordBatch((CArrowArray*)root, schema);
    }

    /// <summary>Frees everything collected so far (used when a batch is abandoned before import).</summary>
    public void FreeAll()
    {
        long count = _allocations.Length / sizeof(nint);
        var items = (nint*)_allocations.Pointer;
        for (long i = 0; i < count; i++) NativeMemory.AlignedFree((void*)items[i]);
        _allocations.Free();
    }

    [UnmanagedCallersOnly]
    private static void ReleaseChild(NativeCArray* array) => array->Release = null;

    [UnmanagedCallersOnly]
    private static void ReleaseRoot(NativeCArray* array)
    {
        var list = (nint*)array->PrivateData;
        array->Release = null;
        array->PrivateData = null;
        if (list == null) return;
        long count = list[0];
        // The root struct itself is in the list, so read everything we need before freeing.
        for (long i = 1; i <= count; i++) NativeMemory.AlignedFree((void*)list[i]);
        NativeMemory.Free(list);
    }
}

internal static class NativeExportExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe nint Ptr(this NativeBatchExport export, ref NativeGrowBuffer buffer) =>
        (nint)export.Own(buffer.Detach());
}
