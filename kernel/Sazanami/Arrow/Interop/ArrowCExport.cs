using System.Buffers;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Sazanami.Arrow.Interop;

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
/// One export (an array with its children and dictionaries, or a record batch) is one native block — every child
/// struct, buffer pointer list and view size list in it — and one managed holder of what keeps the memory: for buffers
/// over native memory of a <see cref="NativeMemoryOwner"/> (built or imported here), a reference on that owner; any
/// other buffer is retained and pinned. Every array of the export has its own release, as the interface asks (a
/// consumer may move children out and release them later); a release releases its children not moved out, and the
/// last array released lets the whole export go. Empty buffers go out as null pointers; view arrays get the variadic
/// buffer sizes the interface wants last. The source arrays may be disposed as soon as the export returns.
/// </para>
/// </remarks>
internal static unsafe class ArrowCExport
{
    /// <summary>Exports an array into <paramref name="destination"/> (owned by the caller, released by the consumer).</summary>
    public static void ExportArray(IArrowArray array, CArrowArray* destination)
    {
        ArgumentNullException.ThrowIfNull(array);
        ArrayData data = array.Data;
        Measure(data, out int arrays, out int slots, out int sizes);
        var export = new Export(arrays - 1, slots, sizes);
        try
        {
            export.Fill((NativeCArray*)destination, data);
            export.Done();
        }
        catch
        {
            export.Abandon();
            throw;
        }
    }

    /// <summary>Exports a record batch as a struct array of its columns into <paramref name="destination"/>.</summary>
    public static void ExportRecordBatch(RecordBatch batch, CArrowArray* destination)
        => ExportRecordBatch(batch, null, destination);

    /// <summary>
    /// Exports a record batch as a struct array of the columns at <paramref name="columns"/> (all when null), in that
    /// order, into <paramref name="destination"/>.
    /// </summary>
    public static void ExportRecordBatch(RecordBatch batch, int[]? columns, CArrowArray* destination)
    {
        ArgumentNullException.ThrowIfNull(batch);
        int count = columns?.Length ?? batch.ColumnCount;
        int arrays = 0, slots = 1 + count, sizes = 0; // the root: its (null) validity and its children
        for (int c = 0; c < count; c++)
        {
            Measure(batch.Column(columns is null ? c : columns[c]).Data, out int a, out int s, out int z);
            (arrays, slots, sizes) = (arrays + a, slots + s, sizes + z);
        }
        var export = new Export(arrays, slots, sizes);
        try
        {
            var root = (NativeCArray*)destination;
            byte** buffers = (byte**)export.Slots(1);
            buffers[0] = null; // no validity
            NativeCArray** children = export.Children(count);
            for (int c = 0; c < count; c++) export.Fill(children[c], batch.Column(columns is null ? c : columns[c]).Data);
            *root = export.Array(batch.Length, 0, 0, 1, buffers, count, children, null);
            export.Done();
        }
        catch
        {
            export.Abandon();
            throw;
        }
    }

    /// <summary>The arrays (this one included), pointer slots and view sizes an array's export takes.</summary>
    private static void Measure(ArrayData data, out int arrays, out int slots, out int sizes)
    {
        int buffers = data.Buffers?.Length ?? 0;
        bool views = IsView(data.DataType);
        arrays = 1;
        slots = buffers + (views ? 1 : 0) + (data.Children?.Length ?? 0);
        sizes = views ? Math.Max(1, buffers - 2) : 0;
        foreach (ArrayData child in data.Children ?? [])
        {
            Measure(child, out int a, out int s, out int z);
            (arrays, slots, sizes) = (arrays + a, slots + s, sizes + z);
        }
        if (data.Dictionary is not null)
        {
            Measure(data.Dictionary, out int a, out int s, out int z);
            (arrays, slots, sizes) = (arrays + a, slots + s, sizes + z);
        }
    }

    private static bool IsView(IArrowType type) => type is StringViewType or BinaryViewType;

    /// <summary>The head of an export's native block: the arrays not released yet, and the holder's handle.</summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct Block
    {
        public int Live;
        public nint Holder;
    }

    /// <summary>One export being built: its native block (filled front to back) and its holder.</summary>
    private sealed class Export
    {
        private readonly Block* _block;
        private readonly Holder _holder = new();
        private NativeCArray* _nextArray;
        private nint* _nextSlot;
        private long* _nextSize;
        private int _arrays;

        public Export(int children, int slots, int sizes)
        {
            long bytes = sizeof(Block) + (long)children * sizeof(NativeCArray) + (long)slots * sizeof(nint) + (long)sizes * sizeof(long);
            _block = (Block*)NativeMemoryPool.Alloc(bytes);
            _nextArray = (NativeCArray*)(_block + 1);
            _nextSlot = (nint*)(_nextArray + children);
            _nextSize = (long*)(_nextSlot + slots);
        }

        public nint* Slots(int count)
        {
            nint* slots = _nextSlot;
            _nextSlot += count;
            return slots;
        }

        public NativeCArray** Children(int count)
        {
            if (count == 0) return null;
            var children = (NativeCArray**)Slots(count);
            for (int c = 0; c < count; c++)
            {
                children[c] = _nextArray++;
                *children[c] = default;
            }
            return children;
        }

        /// <summary>An array struct of the export (counted: released once by the consumer, or by its parent).</summary>
        public NativeCArray Array(long length, long nullCount, long offset, int bufferCount, byte** buffers, int childCount,
            NativeCArray** children, NativeCArray* dictionary)
        {
            _arrays++;
            return new NativeCArray
            {
                Length = length,
                NullCount = nullCount,
                Offset = offset,
                BufferCount = bufferCount,
                ChildCount = childCount,
                Buffers = bufferCount == 0 ? null : buffers,
                Children = children,
                Dictionary = dictionary,
                Release = &Release,
                PrivateData = _block,
            };
        }

        public void Fill(NativeCArray* target, ArrayData data)
        {
            ArrowBuffer[] buffers = data.Buffers ?? [];
            bool views = IsView(data.DataType);
            int count = buffers.Length + (views ? 1 : 0);
            byte** pointers = (byte**)Slots(count);
            for (int b = 0; b < buffers.Length; b++) pointers[b] = _holder.Keep(buffers[b]);
            if (views)
            {
                // The sizes of the variadic data buffers (after validity and views), as int64, last.
                int variadic = Math.Max(0, buffers.Length - 2);
                long* sizes = _nextSize;
                _nextSize += Math.Max(1, variadic);
                for (int k = 0; k < variadic; k++) sizes[k] = buffers[2 + k].Length;
                pointers[count - 1] = (byte*)sizes;
            }
            int childCount = data.Children?.Length ?? 0;
            NativeCArray** children = Children(childCount);
            for (int c = 0; c < childCount; c++) Fill(children[c], data.Children![c]);
            NativeCArray* dictionary = null;
            if (data.Dictionary is not null)
            {
                dictionary = _nextArray++;
                *dictionary = default;
                Fill(dictionary, data.Dictionary);
            }
            *target = Array(data.Length, data.NullCount, data.Offset, count, pointers, childCount, children, dictionary);
        }

        /// <summary>The export is complete: the consumer's releases let it go from now on.</summary>
        public void Done()
        {
            _block->Live = _arrays;
            _block->Holder = GCHandle.ToIntPtr(GCHandle.Alloc(_holder));
        }

        /// <summary>The export failed on the way: what it holds goes now (nothing reached a consumer).</summary>
        public void Abandon()
        {
            _holder.Let();
            NativeMemoryPool.Free(_block);
        }
    }

    /// <summary>
    /// The release callback of every exported array (root, child or dictionary): releases its children and dictionary
    /// not moved out, then the export once its last array is released.
    /// </summary>
    [UnmanagedCallersOnly]
    private static void Release(NativeCArray* array)
    {
        if (array == null || array->Release == null) return;
        try
        {
            Released(array);
        }
        catch
        {
            // A release callback must not throw across the interface.
        }
    }

    private static void Released(NativeCArray* array)
    {
        var block = (Block*)array->PrivateData;
        for (long c = 0; c < array->ChildCount; c++) ReleaseChild(array->Children[c], block);
        if (array->Dictionary != null) ReleaseChild(array->Dictionary, block);
        array->Release = null;
        array->PrivateData = null;
        if (Interlocked.Decrement(ref block->Live) != 0) return;
        var handle = GCHandle.FromIntPtr(block->Holder);
        var holder = (Holder)handle.Target!;
        handle.Free();
        NativeMemoryPool.Free(block);
        holder.Let();

        static void ReleaseChild(NativeCArray* child, Block* block)
        {
            if (child->Release == null) return; // moved out (or released) by the consumer
            // A child still of this export is released here (no call through the interface); any other by its own release.
            if (child->PrivateData == block) Released(child);
            else child->Release(child);
        }
    }

    /// <summary>
    /// What keeps an export's memory: a reference on each native owner of its buffers (a buffer's owner usually
    /// being that of the buffer before it: one reference per run), and the other buffers retained and pinned.
    /// </summary>
    private sealed class Holder
    {
        private NativeMemoryOwner? _last;
        private List<NativeMemoryOwner>? _owners;
        private List<(ArrowBuffer Buffer, MemoryHandle Pin)>? _kept;

        /// <summary>The address of a buffer, kept until the export is let go; null for an empty one.</summary>
        public byte* Keep(ArrowBuffer buffer)
        {
            if (buffer.IsEmpty) return null;
            if (MemoryMarshal.TryGetMemoryManager(buffer.Memory, out NativeBufferMemory? manager, out int start, out _))
            {
                NativeMemoryOwner owner = manager.Owner;
                if (!ReferenceEquals(owner, _last))
                {
                    owner.AddBuffer();
                    (_owners ??= []).Add(owner);
                    _last = owner;
                }
                return manager.Pointer + start;
            }
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
            (_kept ??= []).Add((retained, pin));
            return (byte*)pin.Pointer;
        }

        /// <summary>Lets go of everything kept.</summary>
        public void Let()
        {
            if (_owners is not null)
                foreach (NativeMemoryOwner owner in _owners) owner.Release();
            if (_kept is not null)
                foreach (var (buffer, pin) in _kept)
                {
                    pin.Dispose();
                    buffer.Dispose();
                }
            _owners = null;
            _kept = null;
            _last = null;
        }
    }
}
