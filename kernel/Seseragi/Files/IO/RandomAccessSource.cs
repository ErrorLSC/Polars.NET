using Kaminari.Sazanami.Helpers;
using Microsoft.Win32.SafeHandles;

namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// A seekable stream (from its position at open on) read at offsets, so that several readers can share it: a file
/// through its handle without seeking, native memory by copying, any other stream by seeking under a gate.
/// </summary>
internal sealed class RandomAccessSource : IRandomAccess, IDisposable
{
    private readonly Stream _stream;
    private readonly bool _leaveOpen;
    private readonly NativeByteBuffer? _copy;
    private readonly SafeFileHandle? _file;
    private readonly long _start;
    private readonly SemaphoreSlim _gate = new(1, 1);

    /// <summary>Wraps a seekable stream; <paramref name="copy"/> is the native memory the stream reads, when it was
    /// copied there (released on dispose).</summary>
    public RandomAccessSource(Stream stream, bool leaveOpen, NativeByteBuffer? copy)
    {
        _stream = stream;
        _leaveOpen = leaveOpen;
        _copy = copy;
        _start = stream.Position;
        Length = stream.Length - _start;
        if (copy is null && stream is FileStream file) _file = file.SafeFileHandle;
    }

    public long Length { get; }

    public int ReadAt(long offset, Span<byte> destination)
    {
        int count = (int)Math.Clamp(Length - offset, 0, destination.Length);
        if (count == 0) return 0;
        if (_copy is not null)
        {
            _copy.Span.Slice((int)(_start + offset), count).CopyTo(destination);
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
        int count = (int)Math.Clamp(Length - offset, 0, destination.Length);
        if (count == 0) return 0;
        if (_copy is not null) return ReadAt(offset, destination.Span);
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
