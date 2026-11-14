using System.Buffers;
using System.Runtime.InteropServices;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// A growable byte buffer in native memory, for large I/O scratch (file blocks, decompression output) that
/// should neither land on the managed heap nor stay parked in <see cref="ArrayPool{T}"/> after use.
/// Being a <see cref="MemoryManager{T}"/>, it hands out <see cref="Memory{T}"/> for stream I/O; spans and memory
/// taken from it are invalid after <see cref="EnsureCapacity"/> grows it or after disposal.
/// </summary>
/// <param name="initialCapacity">The bytes allocated up front.</param>
/// <param name="pooled">
/// Whether the memory comes from <see cref="NativeMemoryPool"/> (scratch reused across readers; cannot be
/// <see cref="Detach"/>ed) rather than <see cref="NativeMemory.Alloc(nuint)"/>.
/// </param>
internal sealed unsafe class NativeByteBuffer(int initialCapacity = 0, bool pooled = false) : MemoryManager<byte>
{
    private byte* _ptr = initialCapacity <= 0 ? null : pooled ? NativeMemoryPool.Alloc(initialCapacity) : (byte*)NativeMemory.Alloc((nuint)initialCapacity);
    private int _capacity = initialCapacity;

    public int Capacity => _capacity;

    /// <summary>The whole buffer (its capacity).</summary>
    public Span<byte> Span => new(_ptr, _capacity);

    /// <summary>Grows to at least <paramref name="size"/> bytes, keeping the current contents.</summary>
    public void EnsureCapacity(long size)
    {
        if (size <= _capacity) return;
        if (size > Array.MaxLength) throw new InvalidOperationException("A native buffer cannot exceed 2 GB.");
        long next = Math.Min(Math.Max(size, (long)_capacity * 2), Array.MaxLength);
        if (pooled)
        {
            byte* grown = NativeMemoryPool.Alloc(next);
            if (_ptr != null)
            {
                Buffer.MemoryCopy(_ptr, grown, next, _capacity);
                NativeMemoryPool.Free(_ptr);
            }
            _ptr = grown;
        }
        else _ptr = (byte*)NativeMemory.Realloc(_ptr, (nuint)next);
        _capacity = (int)next;
    }

    public override Span<byte> GetSpan() => Span;

    public override MemoryHandle Pin(int elementIndex = 0) => new(_ptr + elementIndex);

    public override void Unpin() { }

    protected override void Dispose(bool disposing)
    {
        if (pooled) NativeMemoryPool.Free(_ptr);
        else if (_ptr != null) NativeMemory.Free(_ptr);
        _ptr = null;
        _capacity = 0;
    }

    // Safety net for an owner that is never disposed. Owners keep the buffer reachable while any span or memory
    // from it is in use (spans are taken synchronously; Memory<T> handed to stream I/O references this object).
#pragma warning disable CA2015
    ~NativeByteBuffer() => Dispose(false);
#pragma warning restore CA2015

    /// <summary>
    /// Hands the allocation to the caller, who frees it with <see cref="NativeMemory.Free"/>, and leaves this
    /// buffer empty. Returns null when nothing was allocated.
    /// </summary>
    public byte* Detach()
    {
        if (pooled) throw new InvalidOperationException("A pooled buffer cannot be detached.");
        byte* p = _ptr;
        _ptr = null;
        _capacity = 0;
        return p;
    }

    /// <summary>Frees the memory (also done by the finalizer as a fallback).</summary>
    public void Release()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
}
