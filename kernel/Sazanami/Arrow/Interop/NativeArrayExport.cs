using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using System.Buffers;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Arrow.Builders;

namespace Kaminari.Sazanami.Arrow.Interop;

/// <summary>
/// The Arrow C data interface <c>ArrowArray</c> struct, in which builders describe a batch's arrays. The batch is
/// then turned into Apache.Arrow arrays by <see cref="NativeBatchExport.Import"/>, over the same native memory.
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
/// Collects every native allocation of one record batch (column buffers and the C structs that describe them) and
/// turns the batch into Apache.Arrow arrays over that memory, without copying. Builders detach their buffers into it.
/// </summary>
/// <remarks>
/// The arrays are made directly, not through Apache.Arrow's C importer: that one reports every imported buffer to
/// the GC as memory pressure, which for large batches forces a full collection per batch. Here the memory is freed
/// when every buffer of the batch is disposed, or, as a safety net, when the batch is collected.
/// </remarks>
internal sealed unsafe class NativeBatchExport
{
    private NativeGrowBuffer _allocations; // NativeAllocation entries, in native memory (no managed lookup per buffer)
    private Dictionary<nint, (NativeMemoryOwner Owner, int Length)>? _shared; // buffers of other batches, made when one is shared
    private List<IDisposable>? _held; // arrays made on the way whose buffers may be shared here: disposed once imported

    /// <summary>
    /// Keeps <paramref name="array"/> (an intermediate array whose buffers this batch may share) alive until the batch
    /// is imported (its shared buffers then hold the memory themselves) or freed; then disposes it.
    /// </summary>
    public void Hold(IDisposable array) => (_held ??= []).Add(array);

    private void Release()
    {
        FreeScratch();
        if (_held is null) return;
        foreach (IDisposable array in _held) array.Dispose();
        _held = null;
    }

    /// <summary>
    /// Puts a buffer of another imported batch into this one without copying (when it is one: a buffer over native
    /// memory of a <see cref="NativeMemoryOwner"/>, built here or imported by <see cref="ArrowCImport"/>): the arrays
    /// made here then hold that batch's memory too, until they are disposed. Returns false for other buffers (managed memory, other importers), which must be copied.
    /// </summary>
    public bool TryShare(ArrowBuffer buffer, out nint pointer)
    {
        pointer = 0;
        if (!MemoryMarshal.TryGetMemoryManager(buffer.Memory, out NativeBufferMemory? manager, out int start, out int length)) return false;
        pointer = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(manager.GetSpan())) + start;
        _shared ??= [];
        if (!_shared.TryGetValue(pointer, out var known) || known.Length < length) _shared[pointer] = (manager.Owner, length);
        return true;
    }

    /// <summary>Takes ownership of a pointer from <see cref="NativeGrowBuffer.Detach(out long)"/> or <see cref="Allocate"/>, with its size in bytes.</summary>
    public byte* Own(byte* pointer, long bytes)
    {
        _allocations.Append(new NativeAllocation((nint)pointer, bytes));
        return pointer;
    }

    /// <summary>Like <see cref="Allocate"/>, without clearing the memory: for buffers the caller writes in full.</summary>
    public void* AllocateUninitialized(long bytes) => Own(NativeMemoryPool.Alloc(Math.Max(bytes, 8)), bytes);

    public void* Allocate(long bytes)
    {
        byte* p = NativeMemoryPool.Alloc(Math.Max(bytes, 8));
        NativeMemory.Clear(p, (nuint)Math.Max(bytes, 8));
        return Own(p, bytes);
    }

    /// <summary>
    /// Creates an array struct with the given buffers and children. The struct and its pointer lists only describe the
    /// batch until it is imported: they live in scratch memory let go of then, not with the batch's buffers.
    /// </summary>
    public NativeCArray* NewArray(long length, long nullCount, ReadOnlySpan<nint> buffers, ReadOnlySpan<nint> children,
        NativeCArray* dictionary = null)
    {
        var array = (NativeCArray*)Scratch(sizeof(NativeCArray) + (buffers.Length + children.Length) * sizeof(nint));
        *array = default;
        array->Length = length;
        array->NullCount = nullCount;
        array->BufferCount = buffers.Length;
        array->ChildCount = children.Length;
        var lists = (nint*)(array + 1);
        if (!buffers.IsEmpty)
        {
            array->Buffers = (byte**)lists;
            buffers.CopyTo(new Span<nint>(lists, buffers.Length));
        }
        if (!children.IsEmpty)
        {
            array->Children = (NativeCArray**)(lists + buffers.Length);
            children.CopyTo(new Span<nint>(lists + buffers.Length, children.Length));
        }
        array->Dictionary = dictionary;
        return array;
    }

    // The scratch memory of the array structs: chunks taken in turn (each starting with the address of the one before),
    // all let go of once the batch is imported or abandoned.
    private byte* _scratch;
    private int _scratchUsed, _scratchSize;

    private const int ScratchChunkBytes = 16 << 10;

    /// <summary><paramref name="bytes"/> (a multiple of 8) of scratch memory, until the batch is imported.</summary>
    private byte* Scratch(int bytes)
    {
        if (_scratch == null || _scratchSize - _scratchUsed < bytes)
        {
            int size = Math.Max(ScratchChunkBytes, bytes + sizeof(nint));
            byte* chunk = NativeMemoryPool.Alloc(size);
            *(nint*)chunk = (nint)_scratch;
            _scratch = chunk;
            _scratchUsed = sizeof(nint);
            _scratchSize = size;
        }
        byte* p = _scratch + _scratchUsed;
        _scratchUsed += bytes;
        return p;
    }

    private void FreeScratch()
    {
        while (_scratch != null)
        {
            byte* before = (byte*)*(nint*)_scratch;
            NativeMemoryPool.Free(_scratch);
            _scratch = before;
        }
        _scratchUsed = _scratchSize = 0;
    }

    /// <summary>
    /// Turns the struct array into a record batch over the collected memory. From here on the allocations belong to
    /// the returned batch.
    /// </summary>
    public RecordBatch Import(NativeCArray* root, Schema schema)
    {
        NativeBatchMemory owner = TakeAllocations();
        try
        {
            if (root->ChildCount != schema.FieldsList.Count)
                throw new InvalidOperationException($"The batch has {root->ChildCount} columns; its schema {schema.FieldsList.Count}.");
            var columns = new IArrowArray[schema.FieldsList.Count];
            for (int i = 0; i < columns.Length; i++)
                columns[i] = ArrowArrayFactory.BuildArray(Data(root->Children[i], schema.FieldsList[i].DataType, owner));
            return new RecordBatch(schema, columns, checked((int)root->Length));
        }
        finally
        {
            owner.Sealed();
            Release();
        }
    }

    /// <summary>
    /// Turns one array into an Apache.Arrow array over the collected memory. From here on the allocations belong to
    /// the returned array.
    /// </summary>
    public IArrowArray ImportArray(NativeCArray* array, IArrowType type)
    {
        NativeBatchMemory owner = TakeAllocations();
        try
        {
            return ArrowArrayFactory.BuildArray(Data(array, type, owner));
        }
        finally
        {
            owner.Sealed();
            Release();
        }
    }

    /// <summary>Hands the allocations collected so far to a batch memory (sorted for lookups); this export is empty after.</summary>
    private NativeBatchMemory TakeAllocations()
    {
        int count = checked((int)(_allocations.Length / sizeof(NativeAllocation)));
        return new NativeBatchMemory((NativeAllocation*)_allocations.Detach(), count);
    }

    private ArrayData Data(NativeCArray* array, IArrowType type, NativeBatchMemory owner)
    {
        var buffers = new ArrowBuffer[array->BufferCount];
        for (int b = 0; b < buffers.Length; b++)
        {
            nint pointer = (nint)array->Buffers[b];
            buffers[b] = pointer == 0 ? ArrowBuffer.Empty
                : _shared is not null && _shared.TryGetValue(pointer, out var shared) ? NewBuffer(new NativeBufferMemory(shared.Owner, (byte*)pointer, shared.Length))
                : NewBuffer(new NativeBufferMemory(owner, (byte*)pointer, checked((int)owner.SizeOf(pointer))));
        }
        var children = new ArrayData[array->ChildCount];
        for (int c = 0; c < children.Length; c++) children[c] = Data(array->Children[c], ChildType(type, c), owner);
        ArrayData? dictionary = array->Dictionary == null ? null : Data(array->Dictionary, ((DictionaryType)type).ValueType, owner);
        return new ArrayData(type, checked((int)array->Length), checked((int)array->NullCount), checked((int)array->Offset), buffers, children, dictionary);
    }

    private static IArrowType ChildType(IArrowType type, int index) => type switch
    {
        StructType s => s.Fields[index].DataType,
        MapType m => m.KeyValueType,
        ListType l => l.ValueDataType,
        LargeListType l => l.ValueDataType,
        ListViewType l => l.ValueDataType,
        LargeListViewType l => l.ValueDataType,
        FixedSizeListType f => f.ValueDataType,
        UnionType u => u.Fields[index].DataType,
        RunEndEncodedType r => index == 0 ? r.RunEndsDataType : r.ValuesDataType,
        _ => throw new InvalidOperationException($"A {type.Name} array has no children."),
    };

    /// <summary>Apache.Arrow's owning buffer constructor (internal there): the buffer disposes its memory owner.</summary>
    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
    internal static extern ArrowBuffer NewBuffer(IMemoryOwner<byte> owner);

    /// <summary>Frees everything collected so far (used when a batch is abandoned before import).</summary>
    public void FreeAll()
    {
        long count = _allocations.Length / sizeof(NativeAllocation);
        var items = (NativeAllocation*)_allocations.Pointer;
        for (long i = 0; i < count; i++) NativeMemoryPool.Free((void*)items[i].Pointer);
        _allocations.Free();
        _shared?.Clear();
        Release();
    }
}

/// <summary>One native allocation of a batch: where it is and the bytes asked for (a buffer's length).</summary>
internal readonly record struct NativeAllocation(nint Pointer, long Bytes) : IComparable<NativeAllocation>
{
    public int CompareTo(NativeAllocation other) => Pointer.CompareTo(other.Pointer);
}

/// <summary>
/// The native memory behind the buffers of one imported batch: counted references (one per buffer over it, one per
/// pin, one for the import until it is sealed), the memory let go of when the last goes. A finalizer lets it go if
/// the batch is dropped undisposed.
/// </summary>
internal abstract class NativeMemoryOwner
{
    private static long _live;

    private int _buffers = 1; // the import itself holds a reference until it is sealed
    private int _freed;

    protected NativeMemoryOwner() => Interlocked.Increment(ref _live);

    /// <summary>How many owners of any kind hold memory not let go of yet, in the whole process (for tests).</summary>
    internal static long Live => Volatile.Read(ref _live);

    /// <summary>Whether the memory was let go of (for tests).</summary>
    internal bool IsFreed => Volatile.Read(ref _freed) != 0;

    public void AddBuffer() => Interlocked.Increment(ref _buffers);

    /// <summary>The import is complete: from now on the buffers alone keep the memory.</summary>
    public void Sealed() => Release();

    public void Release()
    {
        if (Interlocked.Decrement(ref _buffers) == 0) Free();
    }

    private void Free()
    {
        if (Interlocked.Exchange(ref _freed, 1) != 0) return;
        Interlocked.Decrement(ref _live);
        FreeMemory();
        GC.SuppressFinalize(this);
    }

    /// <summary>Lets the memory go (called once).</summary>
    protected abstract void FreeMemory();

    ~NativeMemoryOwner() => Free();
}

/// <summary>
/// The native allocations of one batch built here (<see cref="NativeBatchExport"/>), freed with its last buffer. Their
/// list is native too (sorted by address: a buffer's size is looked up while the batch is imported).
/// </summary>
internal sealed unsafe class NativeBatchMemory : NativeMemoryOwner
{
    private readonly NativeAllocation* _allocations;
    private readonly int _count;

    private static long _live;

    /// <summary>Takes ownership of <paramref name="allocations"/> (a <see cref="NativeMemoryPool"/> block) and of what they point to.</summary>
    public NativeBatchMemory(NativeAllocation* allocations, int count)
    {
        _allocations = allocations;
        _count = count;
        new Span<NativeAllocation>(allocations, count).Sort();
        Interlocked.Increment(ref _live);
    }

    /// <summary>The bytes asked for of the allocation at <paramref name="pointer"/>.</summary>
    public long SizeOf(nint pointer)
    {
        int at = new ReadOnlySpan<NativeAllocation>(_allocations, _count).BinarySearch(new NativeAllocation(pointer, 0));
        return at >= 0 ? _allocations[at].Bytes : throw new InvalidOperationException("A buffer outside the batch's allocations.");
    }

    /// <summary>How many batch memories are allocated and not yet freed, in the whole process (for tests; <see cref="NativeMemoryOwner.Live"/> counts every owner).</summary>
    internal static new long Live => Volatile.Read(ref _live);

    protected override void FreeMemory()
    {
        Interlocked.Decrement(ref _live);
        for (int i = 0; i < _count; i++) NativeMemoryPool.Free((void*)_allocations[i].Pointer);
        NativeMemoryPool.Free(_allocations);
    }
}

/// <summary>
/// One buffer of an imported batch: a view of its native memory, releasing the batch's memory once disposed. A pin
/// (as Apache.Arrow's C exporter takes when it hands the buffer to another library) also keeps the memory until it
/// is released, so a batch may be disposed as soon as it is exported.
/// </summary>
internal sealed unsafe class NativeBufferMemory : MemoryManager<byte>
{
    private readonly NativeMemoryOwner _owner;
    private readonly byte* _pointer;
    private readonly int _length;
    private int _disposed;

    public NativeBufferMemory(NativeMemoryOwner owner, byte* pointer, int length)
    {
        _owner = owner;
        _pointer = pointer;
        _length = length;
        owner.AddBuffer();
    }

    /// <summary>The memory of the batch this buffer belongs to.</summary>
    internal NativeMemoryOwner Owner => _owner;

    /// <summary>Where the buffer's memory starts.</summary>
    internal byte* Pointer => _pointer;

    public override Span<byte> GetSpan() => new(_pointer, _length);

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        _owner.AddBuffer();
        return new(_pointer + elementIndex, default, this);
    }

    public override void Unpin() => _owner.Release();

    protected override void Dispose(bool disposing)
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0) _owner.Release();
    }
}

internal static class NativeExportExtensions
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe nint Ptr(this NativeBatchExport export, ref NativeGrowBuffer buffer) =>
        (nint)export.Own(buffer.Detach(out long bytes), bytes);
}
