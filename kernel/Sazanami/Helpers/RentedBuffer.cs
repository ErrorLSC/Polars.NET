using System.Buffers;

namespace Kaminari.Sazanami.Helpers;

/// <summary>A growable buffer of pooled arrays (returned on <see cref="Dispose"/>).</summary>
internal sealed class RentedBuffer : IBufferWriter<byte>, IDisposable
{
    private byte[] _array = [];
    private int _length;

    public int Length => _length;

    public ReadOnlySpan<byte> Written => _array.AsSpan(0, _length);

    /// <summary>Makes room for <paramref name="bytes"/> more bytes at once (avoiding growth while they are written).</summary>
    public void Reserve(int bytes) => Ensure(bytes);

    public void Advance(int count) => _length += count;

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _array.AsMemory(_length);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        Ensure(sizeHint);
        return _array.AsSpan(_length);
    }

    private void Ensure(int hint)
    {
        hint = Math.Max(hint, 1);
        if (_array.Length - _length >= hint) return;
        byte[] next = ArrayPool<byte>.Shared.Rent((int)Math.Min(Array.MaxLength, Math.Max(4096, Math.Max(_array.Length * 2L, (long)_length + hint))));
        Written.CopyTo(next);
        if (_array.Length > 0) ArrayPool<byte>.Shared.Return(_array);
        _array = next;
    }

    public void Dispose()
    {
        if (_array.Length > 0) ArrayPool<byte>.Shared.Return(_array);
        _array = [];
        _length = 0;
    }
}
