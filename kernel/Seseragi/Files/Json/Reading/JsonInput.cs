using System.IO.Compression;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Json.Reading;

/// <summary>
/// The bytes of one unit of work: whole lines (each line in exactly one unit), as memory valid until disposed. A
/// borrowed unit (one an input keeps, lent for inference) disposes nothing.
/// </summary>
internal sealed class JsonUnitBytes(ReadOnlyMemory<byte> memory, bool fileStart, IDisposable? owner) : IDisposable
{
    public ReadOnlyMemory<byte> Memory { get; } = memory;

    /// <summary>Whether the unit starts the input (where a byte order mark may be).</summary>
    public bool FileStart { get; } = fileStart;

    public JsonUnitBytes Borrow() => new(Memory, FileStart, null);

    public void Dispose() => owner?.Dispose();
}

/// <summary>The input cut into units of whole lines, read in parallel (random access) or in order (streams).</summary>
internal abstract class JsonInput : IDisposable
{
    /// <summary>The bytes of a unit, about this many (a unit holding a longer line is longer).</summary>
    public const int DefaultUnitBytes = 1 << 20;

    /// <summary>The number of units, when known up front (random access); -1 when found as the input is read.</summary>
    public abstract int Count { get; }

    /// <summary>
    /// Unit <paramref name="index"/>'s bytes (the caller disposes them when done), or null past the end of a stream. A
    /// stream keeps a unit it read until <see cref="Release"/> (inference and a second decode read it again).
    /// </summary>
    public abstract ValueTask<JsonUnitBytes?> GetAsync(int index, bool useAsync, CancellationToken cancellationToken);

    /// <summary>Unit <paramref name="index"/> is done with (a stream frees it).</summary>
    public virtual void Release(int index) { }

    public abstract void Dispose();
}

/// <summary>
/// A random-access input cut into fixed blocks: a unit holds the lines that start in its block, so units are found
/// without reading the ones before. Memory (bytes in memory, a mapped file) is sliced in place; other sources are read
/// into native memory, past the block's end to finish its last line.
/// </summary>
internal sealed class BlockInput : JsonInput
{
    private const int TailStep = 64 << 10;

    private readonly IRandomAccess _source;
    private readonly IDisposable? _owned;
    private readonly ReadOnlyMemory<byte>? _memory;
    private readonly MappedFile? _map;
    private readonly unsafe byte* _mapped;
    private readonly long _length;
    private readonly int _unitBytes;

    /// <param name="source">The input.</param>
    /// <param name="owned">What the input owns (disposed with it).</param>
    /// <param name="map">Whether a local file may be mapped.</param>
    /// <param name="unitBytes">The block size.</param>
    public unsafe BlockInput(IRandomAccess source, IDisposable? owned, bool map, int unitBytes)
    {
        _source = source;
        _owned = owned;
        _length = source.Length;
        _unitBytes = unitBytes;
        if (source is ILocalSource local)
        {
            if (local.TryGetMemory(out ReadOnlyMemory<byte> memory)) _memory = memory;
            else if (map && local.Map(out long start) is { } mapped)
            {
                if (mapped.TryAcquire())
                {
                    _map = mapped;
                    _mapped = mapped.At(start, _length);
                }
            }
        }
    }

    public override int Count => (int)Math.Min(int.MaxValue, (_length + _unitBytes - 1) / _unitBytes);

    /// <summary>Whether the input is read in place (memory or a mapping; for tests).</summary>
    internal bool InPlace => _memory is not null || _map is not null;

    public override async ValueTask<JsonUnitBytes?> GetAsync(int index, bool useAsync, CancellationToken cancellationToken)
    {
        long begin = (long)index * _unitBytes, end = Math.Min(begin + _unitBytes, _length);
        if (begin >= _length) return null;
        if (_memory is { } memory) return InMemory(memory, begin, end);
        if (_map is not null) return InMemory(null, begin, end);
        return await ReadAsync(begin, end, useAsync, cancellationToken).ConfigureAwait(false);
    }

    // The unit's lines in memory: from the first line start at or after begin to the end of the line holding end - 1.
    private unsafe JsonUnitBytes InMemory(ReadOnlyMemory<byte>? memory, long begin, long end)
    {
        long start = begin;
        if (begin > 0)
        {
            long nl = IndexOfNewline(memory, begin - 1, end);
            if (nl < 0) return new JsonUnitBytes(ReadOnlyMemory<byte>.Empty, false, null);
            start = nl + 1;
        }
        long stop = _length;
        if (end < _length)
        {
            long nl = IndexOfNewline(memory, end - 1, _length);
            stop = nl < 0 ? _length : nl + 1;
        }
        if (stop - start > int.MaxValue) throw new JsonFormatException($"A line at byte {start} is longer than 2 GB.");
        ReadOnlyMemory<byte> slice = memory is { } m ? m[(int)start..(int)stop] : PointerMemory.Of(_mapped + start, (int)(stop - start));
        return new JsonUnitBytes(slice, begin == 0, null);
    }

    private unsafe long IndexOfNewline(ReadOnlyMemory<byte>? memory, long from, long to)
    {
        for (long at = from; at < to;)
        {
            int n = (int)Math.Min(to - at, int.MaxValue);
            ReadOnlySpan<byte> span = memory is { } m ? m.Span.Slice((int)at, n) : new ReadOnlySpan<byte>(_mapped + at, n);
            int i = span.IndexOf((byte)'\n');
            if (i >= 0) return at + i;
            at += n;
        }
        return -1;
    }

    // The unit read into native memory: [begin - 1, end), then on in steps until its last line ends.
    private async ValueTask<JsonUnitBytes> ReadAsync(long begin, long end, bool useAsync, CancellationToken ct)
    {
        long from = Math.Max(0, begin - 1);
        var buffer = new NativeByteBuffer((int)(end - from) + TailStep, pooled: true);
        try
        {
            int length = await ReadAtAsync(from, buffer, 0, (int)(end - from), useAsync, ct).ConfigureAwait(false);
            int start = 0;
            if (begin > 0)
            {
                int nl = buffer.Span[..length].IndexOf((byte)'\n');
                if (nl < 0)
                {
                    buffer.Release();
                    return new JsonUnitBytes(ReadOnlyMemory<byte>.Empty, false, null);
                }
                start = nl + 1;
            }
            // Finish the last line: the line holding byte end - 1 ends at the first newline at or after it.
            long at = from + length;
            if (end < _length && buffer.Span[length - 1] != '\n')
            {
                while (at < _length)
                {
                    int step = (int)Math.Min(TailStep, _length - at);
                    buffer.EnsureCapacity((long)length + step);
                    int read = await ReadAtAsync(at, buffer, length, step, useAsync, ct).ConfigureAwait(false);
                    int nl = buffer.Span.Slice(length, read).IndexOf((byte)'\n');
                    length += read;
                    at += read;
                    if (nl >= 0)
                    {
                        length = length - read + nl + 1;
                        break;
                    }
                }
            }
            return new JsonUnitBytes(buffer.Memory[start..length], begin == 0, new Releaser(buffer));
        }
        catch
        {
            buffer.Release();
            throw;
        }
    }

    private async ValueTask<int> ReadAtAsync(long offset, NativeByteBuffer buffer, int at, int count, bool useAsync, CancellationToken ct)
    {
        int done = 0;
        while (done < count)
        {
            int n = useAsync
                ? await _source.ReadAtAsync(offset + done, buffer.Memory.Slice(at + done, count - done), ct).ConfigureAwait(false)
                : _source.ReadAt(offset + done, buffer.Span.Slice(at + done, count - done));
            if (n <= 0) throw new EndOfStreamException("The source ended before its length.");
            done += n;
        }
        return done;
    }

    public override void Dispose()
    {
        _map?.Release();
        _owned?.Dispose();
    }
}

/// <summary>Releases a native buffer.</summary>
internal sealed class Releaser(NativeByteBuffer buffer) : IDisposable
{
    public void Dispose() => buffer.Release();
}

/// <summary>
/// A stream read in order (a stream that cannot seek, or decompressed data): units of about a block each, ending at a
/// line end (the rest of the line carried into the next unit). Units read ahead of their decoding are kept until taken.
/// </summary>
internal sealed class StreamInput(Stream stream, bool leaveOpen, int unitBytes) : JsonInput
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Dictionary<int, JsonUnitBytes?> _read = [];
    private readonly object _lock = new();
    private NativeByteBuffer? _carry;
    private int _carryLength, _next;
    private bool _ended, _disposed;

    public override int Count => -1;

    public override async ValueTask<JsonUnitBytes?> GetAsync(int index, bool useAsync, CancellationToken cancellationToken)
    {
        if (useAsync) await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        else _gate.Wait(cancellationToken);
        try
        {
            while (_next <= index)
            {
                JsonUnitBytes? unit = await ReadUnitAsync(useAsync, cancellationToken).ConfigureAwait(false);
                lock (_lock)
                {
                    if (_disposed)
                    {
                        unit?.Dispose();
                        throw new ObjectDisposedException(nameof(StreamInput));
                    }
                    _read[_next++] = unit;
                }
            }
        }
        finally { _gate.Release(); }
        lock (_lock)
        {
            if (!_read.TryGetValue(index, out JsonUnitBytes? found)) throw new InvalidOperationException($"Unit {index} was released already.");
            return found?.Borrow();
        }
    }

    public override void Release(int index)
    {
        lock (_lock)
        {
            if (_read.Remove(index, out JsonUnitBytes? unit)) unit?.Dispose();
        }
    }

    private async ValueTask<JsonUnitBytes?> ReadUnitAsync(bool useAsync, CancellationToken ct)
    {
        if (_ended && _carryLength == 0) return null;
        bool first = _next == 0;
        var buffer = new NativeByteBuffer(unitBytes + _carryLength, pooled: true);
        try
        {
            int length = 0;
            if (_carryLength > 0)
            {
                _carry!.Span[.._carryLength].CopyTo(buffer.Span);
                length = _carryLength;
                _carryLength = 0;
            }
            int searched = 0;
            while (true)
            {
                // Read until the block is full (or the stream ends), then cut after its last newline.
                while (!_ended && length < buffer.Capacity)
                {
                    int n = useAsync
                        ? await stream.ReadAsync(buffer.Memory[length..], ct).ConfigureAwait(false)
                        : stream.Read(buffer.Span[length..]);
                    if (n == 0) _ended = true;
                    length += n;
                }
                int last = buffer.Span[searched..length].LastIndexOf((byte)'\n');
                if (last >= 0 || _ended)
                {
                    int cut = last >= 0 ? searched + last + 1 : length;
                    if (cut < length)
                    {
                        _carry ??= new NativeByteBuffer(0, pooled: true);
                        _carry.EnsureCapacity(length - cut);
                        buffer.Span[cut..length].CopyTo(_carry.Span);
                        _carryLength = length - cut;
                    }
                    if (cut == 0)
                    {
                        buffer.Release();
                        return null;
                    }
                    return new JsonUnitBytes(buffer.Memory[..cut], first, new Releaser(buffer));
                }
                // A line longer than the block: read on.
                searched = length;
                buffer.EnsureCapacity((long)buffer.Capacity * 2);
            }
        }
        catch
        {
            buffer.Release();
            throw;
        }
    }

    public override void Dispose()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            foreach (JsonUnitBytes? unit in _read.Values) unit?.Dispose();
            _read.Clear();
        }
        _carry?.Release();
        if (!leaveOpen) stream.Dispose();
    }

    /// <summary>
    /// The stream decompressed when it starts with a gzip, zlib or zstd header (dataframe engines read these too), else the
    /// stream as it is; <paramref name="head"/> holds its first bytes, already read from it.
    /// </summary>
    public static Stream Decompressed(Stream stream, ReadOnlySpan<byte> head, out bool compressed)
    {
        compressed = true;
        if (head.Length >= 2 && head[0] == 0x1F && head[1] == 0x8B) return new GZipStream(stream, CompressionMode.Decompress);
        if (head.Length >= 4 && head[0] == 0x28 && head[1] == 0xB5 && head[2] == 0x2F && head[3] == 0xFD) return new ZstdSharp.DecompressionStream(stream);
        if (head.Length >= 2 && (head[0] & 0x0F) == 8 && (head[0] << 8 | head[1]) % 31 == 0) return new ZLibStream(stream, CompressionMode.Decompress);
        compressed = false;
        return stream;
    }
}

/// <summary>A stream whose first bytes were read already: they come first, then the rest of the stream.</summary>
internal sealed class PrefixedStream(byte[] prefix, Stream rest, bool leaveOpen) : Stream
{
    private int _at;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    public override int Read(Span<byte> buffer)
    {
        if (_at < prefix.Length)
        {
            int n = Math.Min(buffer.Length, prefix.Length - _at);
            prefix.AsSpan(_at, n).CopyTo(buffer);
            _at += n;
            return n;
        }
        return rest.Read(buffer);
    }

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        _at < prefix.Length ? ValueTask.FromResult(Read(buffer.Span)) : rest.ReadAsync(buffer, cancellationToken);

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override void Flush() { }
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing && !leaveOpen) rest.Dispose();
        base.Dispose(disposing);
    }
}
