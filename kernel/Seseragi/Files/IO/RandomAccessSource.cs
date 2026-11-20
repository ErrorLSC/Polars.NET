using Kaminari.Sazanami.Buffers;
using Microsoft.Win32.SafeHandles;

namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// A seekable stream (from its position at open on) read at offsets, so that several readers can share it: a file
/// through its handle without seeking; bytes in memory (a copy in native memory, a memory stream's buffer, an
/// unmanaged memory stream over a pointer) sliced without a gate; any other stream by seeking under a gate.
/// </summary>
internal sealed class RandomAccessSource : IRandomAccess, ILocalSource, IDisposable
{
    private readonly Stream _stream;
    private readonly bool _leaveOpen;
    private readonly NativeByteBuffer? _copy;
    private readonly SafeFileHandle? _file;
    private readonly bool _mappable;
    private readonly long _start;
    private readonly ReadOnlyMemory<byte>? _memory;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly object _mapLock = new();
    private MappedFile? _map;
    private bool _disposed;
    private int _reads;

    /// <summary>Wraps a seekable stream; <paramref name="copy"/> is the native memory the stream reads, when it was
    /// copied there (released on dispose). Only a file this library opened by its path is <paramref name="mappable"/>:
    /// a stream handed in by a caller is never mapped (whoever holds it may truncate the file under the mapping).</summary>
    public RandomAccessSource(Stream stream, bool leaveOpen, NativeByteBuffer? copy, bool mappable = false)
    {
        _stream = stream;
        _leaveOpen = leaveOpen;
        _copy = copy;
        _start = stream.Position;
        Length = stream.Length - _start;
        if (copy is null && stream is FileStream file) _file = file.SafeFileHandle;
        _mappable = mappable && _file is not null;
        _memory = Length <= int.MaxValue ? MemoryOf(stream, copy, _start, (int)Length) : null;
    }

    /// <summary>
    /// The bytes [<paramref name="start"/>, + <paramref name="length"/>) of a stream that lives in memory: a copy in
    /// native memory, a memory stream's buffer (visible, or hidden in a plain <see cref="MemoryStream"/>: see
    /// <see cref="MemoryStreams.TryGetBytes"/>), or the buffer of an unmanaged memory stream made over a pointer
    /// (one made over a <see cref="System.Runtime.InteropServices.SafeBuffer"/> has no pointer to give: read under the gate).
    /// </summary>
    private static unsafe ReadOnlyMemory<byte>? MemoryOf(Stream stream, NativeByteBuffer? copy, long start, int length)
    {
        if (copy is not null) return copy.Memory.Slice((int)start, length);
        if (stream is MemoryStream ms && MemoryStreams.TryGetBytes(ms, out ArraySegment<byte> buffer)) return buffer.AsMemory().Slice((int)start, length);
        if (stream is UnmanagedMemoryStream unmanaged)
        {
            long position = unmanaged.Position;
            try
            {
                byte* at = unmanaged.PositionPointer; // throws over a SafeBuffer
                return PointerMemory.Of(at - position + start, length);
            }
            catch (NotSupportedException)
            {
                return null;
            }
        }
        return null;
    }

    public long Length { get; }

    /// <summary>The bytes of a stream that lives in memory (see <see cref="MemoryOf"/>), up to 2 GiB.</summary>
    public bool TryGetMemory(out ReadOnlyMemory<byte> memory)
    {
        memory = _memory ?? default;
        return _memory is not null;
    }

    /// <summary>A file read through its handle, or bytes in memory (other streams are read under a gate: better in few large reads).</summary>
    public bool SmallReadsAreCheap => _file is not null || _memory is not null;

    public MappedFile? Map(out long start)
    {
        start = _start;
        if (!_mappable) return null;
        lock (_mapLock)
        {
            if (_disposed) return null;
            return _map ??= MappedFile.Open((FileStream)_stream);
        }
    }

    /// <summary>Whether the file was mapped (for tests).</summary>
    internal bool IsMapped => Volatile.Read(ref _map) is not null;

    /// <summary>How many reads were asked of the source (for tests).</summary>
    internal int Reads => Volatile.Read(ref _reads);

    public int ReadAt(long offset, Span<byte> destination)
    {
        Interlocked.Increment(ref _reads);
        int count = (int)Math.Clamp(Length - offset, 0, destination.Length);
        if (count == 0) return 0;
        if (_memory is { } memory)
        {
            memory.Span.Slice((int)offset, count).CopyTo(destination);
            return count;
        }
        if (_file is not null)
        {
            int total = 0;
            while (total < count)
            {
                int n = RandomAccess.Read(_file, destination[total..count], _start + offset + total);
                if (n <= 0) break;
                total += n;
            }
            return total;
        }
        _gate.Wait();
        try
        {
            _stream.Position = _start + offset;
            return _stream.ReadAtLeast(destination[..count], count, throwOnEndOfStream: false);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
    {
        if (_memory is null) Interlocked.Increment(ref _reads);
        int count = (int)Math.Clamp(Length - offset, 0, destination.Length);
        if (count == 0) return 0;
        if (_memory is not null) return ReadAt(offset, destination.Span);
        if (_file is not null)
        {
            int total = 0;
            while (total < count)
            {
                int n = await RandomAccess.ReadAsync(_file, destination[total..count], _start + offset + total, cancellationToken).ConfigureAwait(false);
                if (n <= 0) break;
                total += n;
            }
            return total;
        }
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _stream.Position = _start + offset;
            return await _stream.ReadAtLeastAsync(destination[..count], count, throwOnEndOfStream: false, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose()
    {
        lock (_mapLock)
        {
            _disposed = true;
            _map?.Release();
        }
        if (!_leaveOpen) _stream.Dispose();
        _copy?.Release();
        _gate.Dispose();
    }
}

/// <summary>A read-only stream over a range of an <see cref="IRandomAccess"/> source, with a position of its own.</summary>
internal sealed class RandomAccessStream(IRandomAccess source, long start) : Stream
{
    private long _position;

    public override long Length => source.Length - start;

    public override bool CanRead => true;

    public override bool CanSeek => true;

    public override bool CanWrite => false;

    public override long Position
    {
        get => _position;
        set => _position = value >= 0 ? value : throw new ArgumentOutOfRangeException(nameof(value));
    }

    public override int Read(Span<byte> buffer)
    {
        int n = source.ReadAt(start + _position, buffer);
        _position += n;
        return n;
    }

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        int n = await source.ReadAtAsync(start + _position, buffer, cancellationToken).ConfigureAwait(false);
        _position += n;
        return n;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override long Seek(long offset, SeekOrigin origin) => Position = origin switch
    {
        SeekOrigin.Begin => offset,
        SeekOrigin.Current => _position + offset,
        SeekOrigin.End => Length + offset,
        _ => throw new ArgumentOutOfRangeException(nameof(origin)),
    };

    public override void Flush() { }

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}
