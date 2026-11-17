using System.Buffers;

namespace Kaminari.Sazanami.Buffers;

/// <summary>
/// A growable <see cref="IBufferWriter{T}"/> in native memory (<see cref="NativeMemoryPool"/>), shaped like
/// <see cref="ArrayBufferWriter{T}"/>: encoders write pages, levels and scratch into it without touching the managed
/// heap. Spans and memory taken from it are invalid once it grows or is disposed; <see cref="Dispose"/> returns the
/// memory to the pool (a finalizer does as a fallback).
/// </summary>
/// <param name="initialCapacity">The bytes allocated at the first write (at least).</param>
internal sealed class NativeBufferWriter(int initialCapacity = 4096) : IBufferWriter<byte>, IDisposable
{
    private NativeByteBuffer? _buffer;
    private int _length;

    /// <summary>The bytes written.</summary>
    public int WrittenCount => _length;

    public ReadOnlySpan<byte> WrittenSpan => _buffer is null ? [] : _buffer.Span[.._length];

    public ReadOnlyMemory<byte> WrittenMemory => _buffer is null ? default : _buffer.Memory[.._length];

    /// <summary>Forgets what was written, keeping the memory for the next writes.</summary>
    public void ResetWrittenCount() => _length = 0;

    /// <summary>Makes room for <paramref name="bytes"/> more bytes at once (avoiding growth while they are written).</summary>
    public void Reserve(int bytes) => Ensure(bytes);

    public void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (count == 0) return;
        if (_buffer is null || _length + count > _buffer.Capacity) throw new InvalidOperationException("Advanced past the memory handed out.");
        _length += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer!.Memory[_length..];
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer!.Span[_length..];
    }

    private void Ensure(int hint)
    {
        long needed = (long)_length + Math.Max(hint, 1);
        if (_buffer is not null && _buffer.Capacity >= needed) return;
        _buffer ??= new NativeByteBuffer(0, pooled: true);
        _buffer.EnsureCapacity(Math.Max(needed, initialCapacity)); // doubles, keeping what was written
    }

    public void Dispose()
    {
        _buffer?.Release();
        _buffer = null;
        _length = 0;
    }
}
