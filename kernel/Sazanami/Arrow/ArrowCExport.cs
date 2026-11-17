using System.Buffers;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// Exports Apache.Arrow arrays through the Arrow C data interface (to the engine, or any consumer) without copying.
/// </summary>
/// <remarks>
/// <para>
/// Apache.Arrow's own exporter (<c>CArrowArrayExporter</c>, <c>CArrowArrayStreamExporter</c>) reports the exported
/// memory to the GC as memory pressure (and fails to release a null array: <c>RemoveMemoryPressure(0)</c>). It is
/// banned in this repository (BannedSymbols.txt); this exporter replaces it.
/// </para>
/// <para>
/// Every buffer is retained (<see cref="ArrowBuffer.Retain"/>: a shared reference, so the caller may dispose its
/// arrays at once) and pinned until the consumer releases the array; each child (and dictionary) has its own release,
/// as the interface asks, and the parent releases those its consumer did not move out. Empty buffers go out as null
/// pointers; view arrays get the variadic buffer sizes the interface wants last.
/// </para>
/// </remarks>
internal static unsafe class ArrowCExport
{
    /// <summary>Exports an array into <paramref name="destination"/> (owned by the caller, released by the consumer).</summary>
    public static void ExportArray(IArrowArray array, CArrowArray* destination)
    {
        ArgumentNullException.ThrowIfNull(array);
        Fill((NativeCArray*)destination, array.Data);
    }

    /// <summary>Exports a record batch as a struct array of its columns into <paramref name="destination"/>.</summary>
    public static void ExportRecordBatch(RecordBatch batch, CArrowArray* destination)
    {
        ArgumentNullException.ThrowIfNull(batch);
        var state = new Exported();
        var root = (NativeCArray*)destination;
        try
        {
            var buffers = (byte**)state.Allocate(sizeof(nint));
            buffers[0] = null; // no validity
            int columns = batch.ColumnCount;
            NativeCArray** children = Children(state, columns);
            for (int c = 0; c < columns; c++) Fill(children[c], batch.Column(c).Data);
            *root = new NativeCArray
            {
                Length = batch.Length,
                BufferCount = 1,
                ChildCount = columns,
                Buffers = buffers,
                Children = children,
                Release = &Release,
                PrivateData = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(state)),
            };
        }
        catch
        {
            state.ReleaseChildren();
            state.Dispose();
            throw;
        }
    }

    private static void Fill(NativeCArray* target, ArrayData data)
    {
        var state = new Exported();
        try
        {
            ArrowBuffer[] buffers = data.Buffers ?? [];
            bool views = data.DataType is StringViewType or BinaryViewType;
            int count = buffers.Length + (views ? 1 : 0);
            byte** pointers = count == 0 ? null : (byte**)state.Allocate(count * sizeof(nint));
            for (int b = 0; b < buffers.Length; b++) pointers[b] = state.Keep(buffers[b]);
            if (views)
            {
                // The sizes of the variadic data buffers (after validity and views), as int64, last.
                int variadic = Math.Max(0, buffers.Length - 2);
                var sizes = (long*)state.Allocate(Math.Max(1, variadic) * sizeof(long));
                for (int k = 0; k < variadic; k++) sizes[k] = buffers[2 + k].Length;
                pointers[count - 1] = (byte*)sizes;
            }

            int childCount = data.Children?.Length ?? 0;
            NativeCArray** children = Children(state, childCount);
            for (int c = 0; c < childCount; c++) Fill(children[c], data.Children![c]);

            NativeCArray* dictionary = null;
            if (data.Dictionary is not null)
            {
                dictionary = state.Child();
                Fill(dictionary, data.Dictionary);
            }

            *target = new NativeCArray
            {
                Length = data.Length,
                NullCount = data.NullCount,
                Offset = data.Offset,
                BufferCount = count,
                ChildCount = childCount,
                Buffers = pointers,
                Children = children,
                Dictionary = dictionary,
                Release = &Release,
                PrivateData = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(state)),
            };
        }
        catch
        {
            state.ReleaseChildren();
            state.Dispose();
            throw;
        }
    }

    private static NativeCArray** Children(Exported state, int count)
    {
        if (count == 0) return null;
        var children = (NativeCArray**)state.Allocate(count * sizeof(nint));
        for (int c = 0; c < count; c++) children[c] = state.Child();
        return children;
    }

    /// <summary>The release callback of every exported array (root, child or dictionary).</summary>
    [UnmanagedCallersOnly]
    private static void Release(NativeCArray* array)
    {
        if (array == null || array->Release == null) return;
        try
        {
            var handle = GCHandle.FromIntPtr((nint)array->PrivateData);
            var state = (Exported)handle.Target!;
            handle.Free();
            state.ReleaseChildren();
            state.Dispose();
        }
        catch
        {
            // A release callback must not throw across the interface.
        }
        array->PrivateData = null;
        array->Release = null;
    }

    /// <summary>
    /// What one exported array holds: its buffers (retained and pinned), the native memory of its pointer arrays and
    /// child structs (the children released first, unless their consumer moved them out).
    /// </summary>
    private sealed class Exported : IDisposable
    {
        private readonly List<(ArrowBuffer Buffer, MemoryHandle Pin)> _kept = [];
        private readonly List<nint> _allocations = [];
        private readonly List<nint> _children = [];

        public void* Allocate(int bytes)
        {
            void* p = NativeMemoryPool.Alloc(bytes);
            _allocations.Add((nint)p);
            return p;
        }

        /// <summary>A zeroed child struct (released with this array when still unreleased).</summary>
        public NativeCArray* Child()
        {
            var child = (NativeCArray*)Allocate(sizeof(NativeCArray));
            *child = default;
            _children.Add((nint)child);
            return child;
        }

        /// <summary>The address of a buffer, retained and pinned until release; null for an empty one.</summary>
        public byte* Keep(ArrowBuffer buffer)
        {
            if (buffer.IsEmpty) return null;
            ArrowBuffer retained = buffer.Retain();
            MemoryHandle pin;
            try
            {
                pin = retained.Memory.Pin();
            }
            catch
            {
                retained.Dispose();
                throw;
            }
            _kept.Add((retained, pin));
            return (byte*)pin.Pointer;
        }

        public void ReleaseChildren()
        {
            foreach (nint p in _children)
            {
                var child = (NativeCArray*)p;
                if (child->Release != null) child->Release(child);
            }
        }

        public void Dispose()
        {
            foreach (var (buffer, pin) in _kept)
            {
                pin.Dispose();
                buffer.Dispose();
            }
            _kept.Clear();
            foreach (nint p in _allocations) NativeMemoryPool.Free((void*)p);
            _allocations.Clear();
            _children.Clear();
        }
    }
}
