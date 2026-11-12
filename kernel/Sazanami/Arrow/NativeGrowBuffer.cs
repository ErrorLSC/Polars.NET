using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// A growable, 64-byte aligned buffer in native memory. Column builders append into it and hand the finished
/// allocation to an exported Arrow array without copying (<see cref="Detach"/>).
/// </summary>
internal unsafe struct NativeGrowBuffer
{
    internal const int Alignment = 64;

    private byte* _ptr;
    private long _length;
    private long _capacity;
    private long _hint; // size of the last detached buffer: the next one starts there instead of doubling up again

    public readonly long Length => _length;
    public readonly byte* Pointer => _ptr;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* Reserve(long bytes)
    {
        long needed = _length + bytes;
        if (needed > _capacity) Grow(needed);
        byte* at = _ptr + _length;
        _length = needed;
        return at;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append<T>(T value) where T : unmanaged
    {
        long needed = _length + sizeof(T);
        if (needed > _capacity) Grow(needed);
        Unsafe.WriteUnaligned(_ptr + _length, value);
        _length = needed;
    }

    public void Append(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty) return;
        byte* at = Reserve(bytes.Length);
        bytes.CopyTo(new Span<byte>(at, bytes.Length));
    }

    /// <summary>Appends <paramref name="bytes"/> zero bytes.</summary>
    public void AppendZeros(long bytes)
    {
        if (bytes == 0) return;
        byte* at = Reserve(bytes);
        NativeMemory.Clear(at, (nuint)bytes);
    }

    /// <summary>Shrinks or grows the logical length; new bytes are zeroed.</summary>
    public void SetLength(long length)
    {
        if (length > _length) AppendZeros(length - _length);
        else _length = length;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Grow(long needed)
    {
        long capacity = Math.Max(Math.Max(needed, _capacity * 2), Math.Max(_hint, Alignment));
        capacity = (capacity + Alignment - 1) & ~(long)(Alignment - 1);
        _ptr = (byte*)NativeMemory.AlignedRealloc(_ptr, (nuint)capacity, Alignment);
        _capacity = capacity;
    }

    /// <summary>
    /// Hands over the allocation (never null: an empty buffer still owns a small block, since consumers may
    /// dereference it) and resets this builder to empty. The caller frees the pointer with
    /// <see cref="NativeMemory.AlignedFree"/>.
    /// </summary>
    public byte* Detach()
    {
        if (_ptr == null) Grow(Alignment);
        byte* p = _ptr;
        // Batches of one column tend to have similar sizes; leave a little headroom so the next fill rarely reallocates.
        _hint = _length + (_length >> 3);
        _ptr = null;
        _length = 0;
        _capacity = 0;
        return p;
    }

    public void Clear() => _length = 0;

    public void Free()
    {
        if (_ptr != null) NativeMemory.AlignedFree(_ptr);
        _ptr = null;
        _length = 0;
        _capacity = 0;
    }
}

/// <summary>A validity (or boolean value) bitmap in native memory, LSB bit order as Arrow requires.</summary>
internal unsafe struct NativeBitmap
{
    private NativeGrowBuffer _bytes;
    private long _bitLength;
    private long _unsetCount;

    public readonly long Length => _bitLength;
    public readonly long UnsetCount => _unsetCount;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(bool set)
    {
        long bit = _bitLength;
        if ((bit & 7) == 0) _bytes.Append<byte>(0);
        if (set) _bytes.Pointer[bit >> 3] |= (byte)(1 << (int)(bit & 7));
        else _unsetCount++;
        _bitLength = bit + 1;
    }

    public void AppendMany(bool set, long count)
    {
        for (long i = 0; i < count; i++) Append(set);
    }

    public byte* Detach()
    {
        _bitLength = 0;
        _unsetCount = 0;
        return _bytes.Detach();
    }

    public void Free()
    {
        _bytes.Free();
        _bitLength = 0;
        _unsetCount = 0;
    }
}
