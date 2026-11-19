using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>
/// The native memory of one IPC message's arrays: the body (read into a pool block, or a range of a file mapping held
/// while any buffer is alive) and the buffers decompressed or copied out of it. Freed with the last buffer over it.
/// </summary>
internal sealed unsafe class IpcBodyMemory : NativeMemoryOwner
{
    private nint* _allocations;
    private int _count;
    private int _capacity;
    private readonly MappedFile? _map;

    /// <summary>Memory that allocates its blocks itself.</summary>
    public IpcBodyMemory()
    {
    }

    /// <summary>Memory over <paramref name="map"/>, which the caller acquired for it (released when it is freed).</summary>
    public IpcBodyMemory(MappedFile map) => _map = map;

    /// <summary>Whether the body was read from a file's mapping (for tests).</summary>
    internal bool IsMapped => _map is not null;

    /// <summary>A pool block of <paramref name="bytes"/> bytes (64-byte aligned), freed with this memory.</summary>
    public byte* Allocate(long bytes)
    {
        byte* p = NativeMemoryPool.Alloc(Math.Max(bytes, 1));
        Adopt(p);
        return p;
    }

    /// <summary>Takes a block of <see cref="NativeMemoryPool"/> allocated elsewhere, freed with this memory from now on.</summary>
    public void Adopt(byte* p)
    {
        if (_count == _capacity)
        {
            int capacity = Math.Max(8, _capacity * 2);
            var grown = (nint*)NativeMemoryPool.Alloc((long)capacity * sizeof(nint));
            if (_count > 0) new ReadOnlySpan<nint>(_allocations, _count).CopyTo(new Span<nint>(grown, capacity));
            if (_allocations != null) NativeMemoryPool.Free(_allocations);
            _allocations = grown;
            _capacity = capacity;
        }
        _allocations[_count++] = (nint)p;
    }

    protected override void FreeMemory()
    {
        for (int i = 0; i < _count; i++) NativeMemoryPool.Free((void*)_allocations[i]);
        if (_allocations != null) NativeMemoryPool.Free(_allocations);
        _allocations = null;
        _count = 0;
        _map?.Release();
    }
}

/// <summary>A few zero bytes that live for the process: the offsets of an empty array whose writer left them out.</summary>
internal sealed unsafe class IpcZeros : NativeMemoryOwner
{
    public static readonly IpcZeros Instance = new();

    private IpcZeros()
    {
        Pointer = (byte*)NativeMemory.AlignedAlloc(64, 64);
        NativeMemory.Clear(Pointer, 64);
        GC.SuppressFinalize(this); // never released: the import reference is kept
    }

    public byte* Pointer { get; }

    protected override void FreeMemory()
    {
    }
}

/// <summary>A buffer of a decoded array: bytes in native memory kept alive by their owner; empty when the owner is null.</summary>
internal readonly unsafe record struct IpcSlice(NativeMemoryOwner? Owner, nint Pointer, int Length)
{
    public static IpcSlice Empty => default;

    public ReadOnlySpan<byte> Span => Owner is null ? default : new ReadOnlySpan<byte>((void*)Pointer, Length);

    public ArrowBuffer ToBuffer() => Owner is null || Length == 0 && Owner is not IpcZeros
        ? ArrowBuffer.Empty
        : NativeBatchExport.NewBuffer(new NativeBufferMemory(Owner, (byte*)Pointer, Length));
}

/// <summary>
/// An array decoded from an IPC body, kept as native slices: it can be turned into Apache.Arrow arrays any number of
/// times (each with buffers of its own, so that disposing one batch leaves the others and the dictionaries alone).
/// </summary>
internal sealed class IpcArray(IArrowType type, int length, int nullCount, IpcSlice[] buffers, IpcArray[] children, IpcArray? dictionary = null)
{
    public IArrowType Type { get; } = type;

    public int Length { get; } = length;

    public int NullCount { get; } = nullCount;

    public IpcSlice[] Buffers { get; } = buffers;

    public IpcArray[] Children { get; } = children;

    public IpcArray? Dictionary { get; } = dictionary;

    /// <summary>The first <paramref name="length"/> slots (a prefix of a top-level array: children stay whole).</summary>
    public IpcArray Prefix(int length, int nullCount) => new(Type, length, nullCount, Buffers, Children, Dictionary);

    /// <summary>The array as Apache.Arrow array data, with buffers of its own over the same memory.</summary>
    public ArrayData ToArrayData() => new(Type, Length, NullCount, 0, [.. Buffers.Select(b => b.ToBuffer())],
        [.. Children.Select(c => c.ToArrayData())], Dictionary?.ToArrayData());

    /// <summary>Takes a reference on every owner of the array's memory (a dictionary kept between batches).</summary>
    public void Hold() => Visit(static owner => owner.AddBuffer());

    /// <summary>Lets go of the references <see cref="Hold"/> took.</summary>
    public void Unhold() => Visit(static owner => owner.Release());

    private void Visit(Action<NativeMemoryOwner> action)
    {
        foreach (IpcSlice slice in Buffers)
        {
            if (slice.Owner is { } owner) action(owner);
        }
        foreach (IpcArray child in Children) child.Visit(action);
        Dictionary?.Visit(action);
    }
}
