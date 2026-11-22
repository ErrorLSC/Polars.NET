namespace Kaminari.Sazanami.Buffers;

/// <summary>
/// Many small byte values kept for as long as the objects holding them live: each value is a slice of a shared chunk
/// (no array, string or object per value), written in place the way an <see cref="System.Buffers.IBufferWriter{T}"/>
/// is (<see cref="GetSpan"/>, then <see cref="Take"/> what was written).
/// </summary>
/// <remarks>
/// The chunks are managed arrays on purpose: a value's lifetime is that of the managed object that holds its slice,
/// which only the collector knows, and arrays of bytes hold no references, so they cost the collector next to nothing
/// to keep. A chunk lives while any slice of it does. Chunks start small and double up to the largest size, so that an
/// arena holding a few values keeps a few bytes alive; a value larger than a quarter of the largest chunk gets an array
/// of its own. Not thread-safe: one arena per writer.
/// </remarks>
internal sealed class ByteArena
{
    /// <summary>The default size of the first chunk.</summary>
    public const int DefaultFirstChunkBytes = 4 << 10;

    /// <summary>The default size of the largest chunks.</summary>
    public const int DefaultMaxChunkBytes = 256 << 10;

    private readonly int _maxChunkBytes;
    private int _nextChunkBytes;
    private byte[] _chunk = [];
    private int _used;
    private byte[]? _own;   // the array of the value being written, when it has one of its own

    /// <param name="firstChunkBytes">The size of the first chunk (when the values are known to need less, that much).</param>
    /// <param name="maxChunkBytes">The size chunks grow to.</param>
    public ByteArena(int firstChunkBytes = DefaultFirstChunkBytes, int maxChunkBytes = DefaultMaxChunkBytes)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxChunkBytes, 4);
        _maxChunkBytes = maxChunkBytes;
        _nextChunkBytes = Math.Clamp(firstChunkBytes, 1, maxChunkBytes);
    }

    /// <summary>Room for a value of at most <paramref name="maxBytes"/> bytes, to be <see cref="Take"/>n.</summary>
    public Span<byte> GetSpan(int maxBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxBytes);
        if (maxBytes > _maxChunkBytes / 4)
        {
            _own = new byte[maxBytes];
            return _own;
        }
        _own = null;
        if (_chunk.Length - _used < maxBytes)
        {
            int size = Math.Max(_nextChunkBytes, maxBytes);
            _chunk = new byte[size];
            _used = 0;
            _nextChunkBytes = Math.Min(_maxChunkBytes, size * 2);
        }
        return _chunk.AsSpan(_used, maxBytes);
    }

    /// <summary>The value written: the first <paramref name="written"/> bytes of the last <see cref="GetSpan"/>.</summary>
    public ReadOnlyMemory<byte> Take(int written)
    {
        if (_own is { } own)
        {
            _own = null;
            return own.AsMemory(0, written);
        }
        ArgumentOutOfRangeException.ThrowIfGreaterThan(written, _chunk.Length - _used);
        ReadOnlyMemory<byte> value = _chunk.AsMemory(_used, written);
        _used += written;
        return value;
    }

    /// <summary>A copy of <paramref name="bytes"/>.</summary>
    public ReadOnlyMemory<byte> Add(ReadOnlySpan<byte> bytes)
    {
        bytes.CopyTo(GetSpan(bytes.Length));
        return Take(bytes.Length);
    }
}
