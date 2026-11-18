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

    /// <summary>The native bytes it holds (none before the first write and after <see cref="Dispose"/>, which it may follow).</summary>
    public long Capacity => _buffer?.Capacity ?? 0;

    public ReadOnlySpan<byte> WrittenSpan => _buffer is null ? [] : _buffer.Span[.._length];

    public ReadOnlyMemory<byte> WrittenMemory => _buffer is null ? default : _buffer.Memory[.._length];

    /// <summary>
    /// A read-only stream over the bytes written, in place (no copy): valid while nothing more is written and the
    /// writer is not disposed (for readers that take a stream, such as XML).
    /// </summary>
    public unsafe Stream OpenRead()
    {
        if (_buffer is null || _length == 0) return Stream.Null;
        using MemoryHandle pin = _buffer.Memory.Pin(); // native memory: the address stays
        return new UnmanagedMemoryStream((byte*)pin.Pointer, _length);
    }

    /// <summary>Appends what <paramref name="stream"/> holds from its position to its end.</summary>
    public void ReadFrom(Stream stream, int sizeHint = 1 << 16)
    {
        while (true)
        {
            int read = stream.Read(GetSpan(Capacity > _length ? 1 : sizeHint)); // the room left first (a body of announced length fills it exactly)
            if (read == 0) return;
            Advance(read);
        }
    }

    /// <inheritdoc cref="ReadFrom"/>
    public async Task ReadFromAsync(Stream stream, CancellationToken cancellationToken, int sizeHint = 1 << 16)
    {
        while (true)
        {
            int read = await stream.ReadAsync(GetMemory(Capacity > _length ? 1 : sizeHint), cancellationToken).ConfigureAwait(false);
            if (read == 0) return;
            Advance(read);
        }
    }

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

    /// <summary>
    /// Returns the memory to the pool between uses, keeping the writer's objects (a writer that lives long would
    /// otherwise point to a new one, young, at its next write). Memory taken before is invalid: only for scratch
    /// whose bytes are never handed out.
    /// </summary>
    public void Trim()
    {
        _buffer?.Empty();
        _length = 0;
    }

    public void Dispose()
    {
        _buffer?.Release();
        _buffer = null;
        _length = 0;
    }
}
