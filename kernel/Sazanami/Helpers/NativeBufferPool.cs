namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// Native scratch buffers kept between uses (fetched file ranges, decompressed blocks), so that each unit of work does
/// not allocate and first-touch fresh memory. Safe to use from several threads. Holds at most a few buffers; the
/// rest are freed when returned.
/// </summary>
internal sealed class NativeBufferPool(int capacity) : IDisposable
{
    private readonly object _lock = new();
    private readonly List<NativeByteBuffer> _free = [];
    private bool _disposed;

    /// <summary>A buffer of at least <paramref name="bytes"/> bytes (contents undefined).</summary>
    public NativeByteBuffer Rent(long bytes)
    {
        NativeByteBuffer? buffer = null;
        lock (_lock)
        {
            // The smallest buffer that fits; else the largest, grown (least memory moved and wasted).
            int best = -1;
            for (int i = 0; i < _free.Count; i++)
            {
                int capacity = _free[i].Capacity;
                if (best < 0) best = i;
                else if (capacity >= bytes ? _free[best].Capacity < bytes || capacity < _free[best].Capacity : capacity > _free[best].Capacity && _free[best].Capacity < bytes)
                    best = i;
            }
            if (best >= 0)
            {
                buffer = _free[best];
                _free.RemoveAt(best);
            }
        }
        buffer ??= new NativeByteBuffer(0);
        if (buffer.Capacity < bytes)
        {
            // Grow without keeping the old contents: free first, then allocate.
            buffer.Release();
            buffer = new NativeByteBuffer(0);
            buffer.EnsureCapacity(bytes);
        }
        return buffer;
    }

    public void Return(NativeByteBuffer buffer)
    {
        lock (_lock)
        {
            if (!_disposed && _free.Count < capacity)
            {
                _free.Add(buffer);
                return;
            }
        }
        buffer.Release();
    }

    public void Dispose()
    {
        lock (_lock)
        {
            _disposed = true;
            foreach (NativeByteBuffer buffer in _free) buffer.Release();
            _free.Clear();
        }
    }
}
