using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using System.Buffers;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Helpers;

namespace Kaminari.Sazanami.Arrow;

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
    private NativeGrowBuffer _allocations; // void* entries
    private readonly Dictionary<nint, long> _sizes = [];

    /// <summary>Takes ownership of a pointer from <see cref="NativeGrowBuffer.Detach(out long)"/> or <see cref="Allocate"/>, with its size in bytes.</summary>
    public byte* Own(byte* pointer, long bytes)
    {
        _allocations.Append((nint)pointer);
        _sizes[(nint)pointer] = bytes;
        return pointer;
    }

    public void* Allocate(long bytes)
    {
        byte* p = NativeMemoryPool.Alloc(Math.Max(bytes, 8));
        NativeMemory.Clear(p, (nuint)Math.Max(bytes, 8));
        return Own(p, bytes);
    }

    /// <summary>Creates an array struct with the given buffers and children.</summary>
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
        return array;
    }

    /// <summary>
    /// Turns the struct array into a record batch over the collected memory. From here on the allocations belong to
    /// the returned batch.
    /// </summary>
    public RecordBatch Import(NativeCArray* root, Schema schema)
    {
        long count = _allocations.Length / sizeof(nint);
        var owner = new NativeBatchMemory(new ReadOnlySpan<nint>(_allocations.Pointer, (int)count));
        _allocations.Free();
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
        }
    }

    private ArrayData Data(NativeCArray* array, IArrowType type, NativeBatchMemory owner)
    {
        var buffers = new ArrowBuffer[array->BufferCount];
        for (int b = 0; b < buffers.Length; b++)
        {
            nint pointer = (nint)array->Buffers[b];
            buffers[b] = pointer == 0 ? ArrowBuffer.Empty : NewBuffer(new NativeBufferMemory(owner, (byte*)pointer, checked((int)_sizes[pointer])));
        }
        var children = new ArrayData[array->ChildCount];
        for (int c = 0; c < children.Length; c++) children[c] = Data(array->Children[c], ChildType(type, c), owner);
        ArrayData? dictionary = array->Dictionary == null ? null : Data(array->Dictionary, ((DictionaryType)type).ValueType, owner);
        return new ArrayData(type, checked((int)array->Length), checked((int)array->NullCount), 0, buffers, children, dictionary);
    }

    private static IArrowType ChildType(IArrowType type, int index) => type switch
    {
        StructType s => s.Fields[index].DataType,
        MapType m => m.KeyValueType,
        ListType l => l.ValueDataType,
        FixedSizeListType f => f.ValueDataType,
        _ => throw new InvalidOperationException($"A {type.Name} array has no children."),
    };

    /// <summary>Apache.Arrow's owning buffer constructor (internal there): the buffer disposes its memory owner.</summary>
    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
    private static extern ArrowBuffer NewBuffer(IMemoryOwner<byte> owner);

    /// <summary>Frees everything collected so far (used when a batch is abandoned before import).</summary>
    public void FreeAll()
    {
        long count = _allocations.Length / sizeof(nint);
        var items = (nint*)_allocations.Pointer;
        for (long i = 0; i < count; i++) NativeMemoryPool.Free((void*)items[i]);
        _allocations.Free();
        _sizes.Clear();
    }
}

/// <summary>
/// The native allocations of one imported batch, freed once every buffer over them is disposed. A finalizer frees
/// them if the batch is dropped undisposed.
/// </summary>
internal sealed unsafe class NativeBatchMemory
{
    private nint[]? _allocations;
    private int _buffers = 1; // the import itself holds a reference until it is sealed

    public NativeBatchMemory(ReadOnlySpan<nint> allocations) => _allocations = allocations.ToArray();

    /// <summary>Whether the allocations were freed (for tests).</summary>
    internal bool IsFreed => Volatile.Read(ref _allocations) is null;

    public void AddBuffer() => Interlocked.Increment(ref _buffers);

    /// <summary>The import is complete: from now on the buffers alone keep the memory.</summary>
    public void Sealed() => Release();

    public void Release()
    {
        if (Interlocked.Decrement(ref _buffers) == 0) Free();
    }

    private void Free()
    {
        nint[]? allocations = Interlocked.Exchange(ref _allocations, null);
        if (allocations is null) return;
        foreach (nint p in allocations) NativeMemoryPool.Free((void*)p);
        GC.SuppressFinalize(this);
    }

    ~NativeBatchMemory() => Free();
}

/// <summary>
/// One buffer of an imported batch: a view of its native memory, releasing the batch's memory once disposed. A pin
/// (as Apache.Arrow's C exporter takes when it hands the buffer to another library) also keeps the memory until it
/// is released, so a batch may be disposed as soon as it is exported.
/// </summary>
internal sealed unsafe class NativeBufferMemory : MemoryManager<byte>
{
    private readonly NativeBatchMemory _owner;
    private readonly byte* _pointer;
    private readonly int _length;
    private int _disposed;

    public NativeBufferMemory(NativeBatchMemory owner, byte* pointer, int length)
    {
        _owner = owner;
        _pointer = pointer;
        _length = length;
        owner.AddBuffer();
    }

    /// <summary>The memory of the batch this buffer belongs to.</summary>
    internal NativeBatchMemory Owner => _owner;

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
