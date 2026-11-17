using System.Buffers;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// An <see cref="IBufferWriter{T}"/> over a native buffer (<see cref="NativeMemoryPool"/>) that hands its bytes to a
/// stream whenever they reach <c>flushAt</c> bytes, and the rest on <see cref="Flush"/>: text writers
/// (<c>Utf8JsonWriter</c>) write large outputs through a small, unmanaged window.
/// </summary>
/// <param name="stream">Where the bytes go.</param>
/// <param name="flushAt">The bytes held before they are written out.</param>
internal sealed class NativeStreamWriter(Stream stream, int flushAt = 64 * 1024) : IBufferWriter<byte>, IDisposable
{
    private readonly NativeByteBuffer _buffer = new(flushAt + 4096, pooled: true);
    private int _written;

    /// <summary>The bytes written so far (out to the stream or held).</summary>
    public long BytesWritten { get; private set; }

    public void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (_written + count > _buffer.Capacity) throw new InvalidOperationException("Advanced past the memory handed out.");
        _written += count;
        BytesWritten += count;
        if (_written >= flushAt) Flush();
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.Memory[_written..];
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _buffer.Span[_written..];
    }

    private void Ensure(int sizeHint)
    {
        int needed = Math.Max(sizeHint, 1);
        if (_buffer.Capacity - _written >= needed) return;
        Flush();
        _buffer.EnsureCapacity(needed);
    }

    /// <summary>Writes the bytes held to the stream.</summary>
    public void Flush()
    {
        if (_written == 0) return;
        stream.Write(_buffer.Span[.._written]);
        _written = 0;
    }

    /// <summary>Frees the buffer (bytes still held are not written: call <see cref="Flush"/> first).</summary>
    public void Dispose() => _buffer.Release();
}
