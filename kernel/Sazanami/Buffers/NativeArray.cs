using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Kaminari.Sazanami.Buffers;

/// <summary>
/// A growable array of unmanaged values in native memory (<see cref="NativeMemoryPool"/>): scratch that encoders fill
/// and refill (page values, levels, indices, hash tables) without touching the managed heap. It holds no length of its
/// own: the owner counts what it wrote. Unlike a managed array, memory it hands out is NOT zeroed — neither at first
/// nor when it grows — so whoever relies on zeros clears them (<see cref="Clear"/>).
/// </summary>
/// <remarks>
/// Spans taken from it are invalid once it grows or is disposed. <see cref="Dispose"/> returns the memory to the pool;
/// a finalizer does as a fallback, so the owner keeps the array reachable while a span of it is in use (spans do not).
/// Not thread-safe.
/// </remarks>
internal sealed unsafe class NativeArray<T> : IDisposable where T : unmanaged
{
    private T* _ptr;
    private int _capacity;

    /// <param name="initialCapacity">The values allocated up front (none: the first <see cref="Grow"/> or <see cref="Reserve"/> allocates).</param>
    public NativeArray(int initialCapacity = 0)
    {
        if (initialCapacity > 0) Reserve(initialCapacity);
    }

    /// <summary>The values it holds (at least what was asked for: the pool's block sizes round up).</summary>
    public int Capacity => _capacity;

    /// <summary>The whole array (its capacity).</summary>
    public Span<T> Span => new(_ptr, _capacity);

    public Span<T> AsSpan(int start, int length) => Span.Slice(start, length);

    public Span<T> AsSpan(int start) => Span[start..];

    /// <summary>The value at <paramref name="index"/> (checked against the capacity, as an array's index is).</summary>
    public ref T this[int index]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            if ((uint)index >= (uint)_capacity) ThrowIndex(index);
            return ref _ptr[index];
        }
    }

    /// <summary>
    /// The value at <paramref name="index"/>, checked only in Debug builds (the tests): for per-value hot paths that
    /// made room just before (measured: the check cost the nested Parquet write a few percent through lost inlining).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ref T At(int index)
    {
        Debug.Assert((uint)index < (uint)_capacity, $"Index {index} is outside the native array.");
        return ref _ptr[index];
    }

    /// <summary>
    /// Grows to at least <paramref name="size"/> values (doubling at least), keeping the first <paramref name="keep"/>;
    /// the rest is undefined. Nothing happens when it holds that many already.
    /// </summary>
    public void Grow(int size, int keep)
    {
        if (size <= _capacity) return;
        ArgumentOutOfRangeException.ThrowIfNegative(keep);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(keep, _capacity);
        T* grown = Allocate(Next(size), out int capacity);
        if (keep > 0) Buffer.MemoryCopy(_ptr, grown, (long)capacity * sizeof(T), (long)keep * sizeof(T));
        NativeMemoryPool.Free(_ptr);
        _ptr = grown;
        _capacity = capacity;
    }

    /// <summary>
    /// Makes room for at least <paramref name="size"/> values without keeping any (doubling at least; the old memory
    /// is freed first). Nothing happens, contents kept, when it holds that many already.
    /// </summary>
    public void Reserve(int size)
    {
        if (size <= _capacity) return;
        int next = Next(size);
        NativeMemoryPool.Free(_ptr);
        _ptr = null;
        _capacity = 0;
        _ptr = Allocate(next, out _capacity);
    }

    /// <summary>Sets every value (the whole capacity) to zero.</summary>
    public void Clear() => Span.Clear();

    /// <summary>Returns the memory to the pool; the array is empty after (and may grow again). Safe to repeat.</summary>
    public void Dispose()
    {
        Free();
        GC.SuppressFinalize(this);
    }

    ~NativeArray() => Free();

    private void Free()
    {
        NativeMemoryPool.Free(_ptr);
        _ptr = null;
        _capacity = 0;
    }

    /// <summary>The size to grow to: at least double, at most <see cref="Array.MaxLength"/>.</summary>
    private int Next(int size)
    {
        if ((uint)size > (uint)Array.MaxLength) throw new InvalidOperationException($"A native array cannot exceed {Array.MaxLength} values.");
        return (int)Math.Min(Math.Max(size, 2L * _capacity), Array.MaxLength);
    }

    private static T* Allocate(int size, out int capacity)
    {
        byte* p = NativeMemoryPool.Alloc((long)size * sizeof(T));
        capacity = (int)Math.Min(NativeMemoryPool.UsableSize(p) / sizeof(T), Array.MaxLength);
        return (T*)p;
    }

    [DoesNotReturn]
    private static void ThrowIndex(int index) =>
        throw new IndexOutOfRangeException($"Index {index} is outside the native array.");
}
