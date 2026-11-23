using System.IO.Compression;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Text;

namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// The bytes of one unit of work: whole records (each in exactly one unit), or a document's whole array elements, as
/// memory valid until disposed. A borrowed unit (one an input keeps, lent for inference) disposes nothing.
/// </summary>
internal sealed class TextUnitBytes(ReadOnlyMemory<byte> memory, bool fileStart, IDisposable? owner, bool fileEnd = false, long elements = 0)
    : IDisposable
{
    public ReadOnlyMemory<byte> Memory { get; } = memory;

    /// <summary>Whether the unit starts the input (where a byte order mark may be).</summary>
    public bool FileStart { get; } = fileStart;

    /// <summary>Whether the unit ends the input (a document's units: the last one holds its end).</summary>
    public bool FileEnd { get; } = fileEnd;

    /// <summary>About how many elements a document's unit holds (to reserve builders).</summary>
    public long Elements { get; } = elements;

    public TextUnitBytes Borrow() => new(Memory, FileStart, null, FileEnd, Elements);

    public void Dispose() => owner?.Dispose();
}

/// <summary>A text input cut into units of whole records, read in parallel (random access) or in order (streams).</summary>
internal abstract class TextUnits : IDisposable
{
    /// <summary>The bytes of a unit, about this many (a unit holding a longer record is longer).</summary>
    public const int DefaultUnitBytes = 1 << 20;

    /// <summary>The number of units, when known up front (random access); -1 when found as the input is read.</summary>
    public abstract int Count { get; }

    /// <summary>Whether the input can be read once only (a stream).</summary>
    public abstract bool ReadOnce { get; }

    /// <summary>
    /// Unit <paramref name="index"/>'s bytes (the caller disposes them when done), or null past the end of a stream. A
    /// stream keeps a unit it read until <see cref="Release"/> (inference and a second decode read it again).
    /// </summary>
    public abstract ValueTask<TextUnitBytes?> GetAsync(int index, bool useAsync, CancellationToken cancellationToken);

    /// <summary>Unit <paramref name="index"/> is done with (a stream frees it).</summary>
    public virtual void Release(int index) { }

    public abstract void Dispose();

    /// <summary>
    /// The units of a record input (CSV) over <paramref name="source"/>, which the caller keeps open while it is read:
    /// gzip, zlib and zstd found by the first bytes and read in order; else blocks that find their own starts (no quotes,
    /// no comments); else, in place (memory, or a local file mapped when <paramref name="map"/> says so), starts planned
    /// side by side (in order with comment lines); else the source read in order.
    /// </summary>
    public static async ValueTask<TextUnits> RecordsAsync(IRandomAccess source, bool map, int unitBytes, RecordCut cut, int threads,
        bool useAsync, CancellationToken ct)
    {
        byte[] head = new byte[(int)Math.Min(4, source.Length)];
        int got = 0;
        while (got < head.Length)
        {
            int n = useAsync ? await source.ReadAtAsync(got, head.AsMemory(got), ct).ConfigureAwait(false) : source.ReadAt(got, head.AsSpan(got));
            if (n <= 0) break;
            got += n;
        }
        Stream data = StreamInput.Decompressed(new RandomAccessStream(source, 0), head.AsSpan(0, got), out bool compressed);
        if (compressed) return new StreamInput(data, leaveOpen: false, unitBytes, cut);
        if (cut.Independent) return new BlockInput(source, null, map, unitBytes, cut);
        return (TextUnits?)Planned(source, map, unitBytes, cut, threads) ?? new StreamInput(data, leaveOpen: false, unitBytes, cut);
    }

    // In place (memory, or a mapped local file): starts planned side by side (in order with comment lines); else null.
    private static unsafe PlannedInput? Planned(IRandomAccess source, bool map, int unitBytes, RecordCut cut, int threads)
    {
        if (source is not ILocalSource local) return null;
        if (local.TryGetMemory(out ReadOnlyMemory<byte> memory))
        {
            using System.Buffers.MemoryHandle pin = memory.Pin();
            return new PlannedInput(memory, null, Plan((byte*)pin.Pointer, memory.Length), cut, null, null);
        }
        if (map && local.Map(out long start) is { } mapped && mapped.TryAcquire())
        {
            byte* at = mapped.At(start, source.Length);
            return new PlannedInput(null, at, Plan(at, source.Length), cut, null, mapped);
        }
        return null;

        long[] Plan(byte* data, long length) => cut.Rules.CommentPrefix is null
            ? QuoteSegments.Cut(data, length, 0, false, unitBytes, cut.Rules, new ParallelOptions { MaxDegreeOfParallelism = threads })
            : QuoteSegments.InOrder(data, length, 0, unitBytes, cut.Rules);
    }
}

/// <summary>Where a format's units may end, and its input errors.</summary>
internal abstract class UnitCut
{
    /// <summary>Whether a block finds its own first record start from the bytes before it (no state carried, e.g. no quotes).</summary>
    public abstract bool Independent { get; }

    /// <summary>
    /// The position after the first record end whose last byte lies before <paramref name="limit"/> (a record start),
    /// -1 when none, -2 when a CR at limit - 1 needs the byte after it (only when <paramref name="data"/> ends there and
    /// the input does not, <paramref name="final"/> false). Independent cuts only.
    /// </summary>
    public abstract int Next(ReadOnlySpan<byte> data, int limit, bool final);

    /// <summary>The last record start in <paramref name="data"/> (which starts at one), 0 when none; a stream cuts there.</summary>
    public abstract int Last(ReadOnlySpan<byte> data, bool final);

    /// <summary>The error for a record longer than a unit can hold (2 GB).</summary>
    public abstract Exception TooLong(long start);

    /// <summary>The error for damaged compressed input.</summary>
    public abstract Exception Damaged(Exception inner);
}

/// <summary>Records that end at a newline byte ('\n'): JSON lines, and the CSV cut's independent case.</summary>
internal abstract class NewlineCut : UnitCut
{
    public override bool Independent => true;

    public override int Next(ReadOnlySpan<byte> data, int limit, bool final)
    {
        int nl = data[..limit].IndexOf((byte)'\n');
        return nl < 0 ? -1 : nl + 1;
    }

    public override int Last(ReadOnlySpan<byte> data, bool final) => data.LastIndexOf((byte)'\n') + 1;
}

/// <summary>
/// CSV records by <see cref="RecordRules"/>: blocks find their own starts only without quoting or comments (a line end
/// is then a record end: LF, CR, CRLF paired across a boundary, or the chosen byte); otherwise the starts are planned
/// (<see cref="PlannedInput"/>) or found in order (<see cref="StreamInput"/>).
/// </summary>
internal sealed class RecordCut(RecordRules rules, Func<long, Exception> tooLong, Func<Exception, Exception> damaged) : UnitCut
{
    public RecordRules Rules { get; } = rules;

    public override bool Independent => Rules.Quote is null && Rules.CommentPrefix is null;

    public override int Next(ReadOnlySpan<byte> data, int limit, bool final)
    {
        if (!Independent) throw new InvalidOperationException("Quoted records have no independent cut.");
        int k = Rules.LineEnd is { } e ? data[..limit].IndexOf(e) : data[..limit].IndexOfAny((byte)'\n', (byte)'\r');
        if (k < 0) return -1;
        if (Rules.LineEnd is not null || data[k] == '\n') return k + 1;
        if (k + 1 < data.Length) return data[k + 1] == '\n' ? k + 2 : k + 1; // CR, maybe CRLF
        return final ? k + 1 : -2;
    }

    public override int Last(ReadOnlySpan<byte> data, bool final) => Rules.CommentPrefix is null
        ? QuoteSegments.LastRecordStart(data, false, final, Rules)
        : QuoteSegments.Scan(data, final, int.MaxValue, Rules, out _);

    public override Exception TooLong(long start) => tooLong(start);

    public override Exception Damaged(Exception inner) => damaged(inner);
}

/// <summary>
/// A random-access input cut into fixed blocks, for cuts whose blocks find their own starts: a unit holds the records
/// that start in its block, so units are found without reading the ones before. Memory (bytes in memory, a mapped
/// file) is sliced in place; other sources are read into native memory, past the block's end to finish its last record.
/// </summary>
internal sealed class BlockInput : TextUnits
{
    private const int TailStep = 64 << 10;

    private readonly IRandomAccess _source;
    private readonly IDisposable? _owned;
    private readonly UnitCut _cut;
    private readonly ReadOnlyMemory<byte>? _memory;
    private readonly MappedFile? _map;
    private readonly unsafe byte* _mapped;
    private readonly long _length;
    private readonly int _unitBytes;

    /// <param name="source">The input.</param>
    /// <param name="owned">What the input owns (disposed with it).</param>
    /// <param name="map">Whether a local file may be mapped.</param>
    /// <param name="unitBytes">The block size.</param>
    /// <param name="cut">Where records end (an independent cut).</param>
    public unsafe BlockInput(IRandomAccess source, IDisposable? owned, bool map, int unitBytes, UnitCut cut)
    {
        if (!cut.Independent) throw new ArgumentException("Blocks need a cut that finds its own starts.", nameof(cut));
        _source = source;
        _owned = owned;
        _cut = cut;
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

    public override bool ReadOnce => false;

    /// <summary>Whether the input is read in place (memory or a mapping; for tests).</summary>
    internal bool InPlace => _memory is not null || _map is not null;

    public override async ValueTask<TextUnitBytes?> GetAsync(int index, bool useAsync, CancellationToken cancellationToken)
    {
        long begin = (long)index * _unitBytes, end = Math.Min(begin + _unitBytes, _length);
        if (begin >= _length) return null;
        if (_memory is { } memory) return InMemory(memory, begin, end);
        if (_map is not null) return InMemory(null, begin, end);
        return await ReadAsync(begin, end, useAsync, cancellationToken).ConfigureAwait(false);
    }

    // The unit's records in memory: from the first record start after begin - 1 to the end of the record holding end - 1.
    private unsafe TextUnitBytes InMemory(ReadOnlyMemory<byte>? memory, long begin, long end)
    {
        long start = begin;
        if (begin > 0)
        {
            start = NextStart(memory, begin - 1, end);
            if (start < 0) return new TextUnitBytes(ReadOnlyMemory<byte>.Empty, false, null);
        }
        long stop = _length;
        if (end < _length)
        {
            long next = NextStart(memory, end - 1, _length);
            stop = next < 0 ? _length : next;
        }
        if (stop - start > int.MaxValue) throw _cut.TooLong(start);
        ReadOnlyMemory<byte> slice = memory is { } m ? m[(int)start..(int)stop] : PointerMemory.Of(_mapped + start, (int)(stop - start));
        return new TextUnitBytes(slice, begin == 0, null);
    }

    // The first record start after a record end whose last byte lies in [from, to), or -1; a window peeks one byte past.
    private unsafe long NextStart(ReadOnlyMemory<byte>? memory, long from, long to)
    {
        for (long at = from; at < to;)
        {
            int n = (int)Math.Min(to - at, int.MaxValue - 1);
            int span = (int)Math.Min(n + 1L, _length - at);
            ReadOnlySpan<byte> window = memory is { } m ? m.Span.Slice((int)at, span) : new ReadOnlySpan<byte>(_mapped + at, span);
            int i = _cut.Next(window, n, final: at + span == _length);
            if (i >= 0) return at + i;
            at += n;
        }
        return -1;
    }

    // The unit read into native memory: [begin - 1, end), then on in steps until its last record ends. A CR at end - 1 that
    // would need the byte after it to place the first start is the block's first record end: the unit before ends there
    // too, so this unit is empty either way.
    private async ValueTask<TextUnitBytes> ReadAsync(long begin, long end, bool useAsync, CancellationToken ct)
    {
        long from = Math.Max(0, begin - 1), to = end;
        var buffer = new NativeByteBuffer((int)(to - from) + TailStep, pooled: true);
        try
        {
            int length = await ReadAtAsync(from, buffer, 0, (int)(to - from), useAsync, ct).ConfigureAwait(false);
            int start = 0;
            if (begin > 0)
            {
                int next = _cut.Next(buffer.Span[..length], (int)(end - from), final: to == _length);
                if (next < 0)
                {
                    buffer.Release();
                    return new TextUnitBytes(ReadOnlyMemory<byte>.Empty, false, null);
                }
                start = next;
            }
            // Finish the last record: it ends at the first record end at or after byte end - 1.
            int stop = length;
            if (end < _length)
            {
                int searched = (int)(end - 1 - from);
                long at = from + length;
                while (true)
                {
                    int next = _cut.Next(buffer.Span[searched..length], length - searched, final: at == _length);
                    if (next >= 0)
                    {
                        stop = searched + next;
                        break;
                    }
                    if (at == _length)
                    {
                        stop = length; // no end: the record runs to the end of the input
                        break;
                    }
                    if (next == -1) searched = length;
                    else searched = length - 1; // a CR last: read on to see whether an LF follows
                    int step = (int)Math.Min(TailStep, _length - at);
                    buffer.EnsureCapacity((long)length + step);
                    int read = await ReadAtAsync(at, buffer, length, step, useAsync, ct).ConfigureAwait(false);
                    length += read;
                    at += read;
                }
            }
            return new TextUnitBytes(buffer.Memory[start..stop], begin == 0, new Releaser(buffer));
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

/// <summary>
/// Bytes in place (memory, or a mapped file) cut at starts planned beforehand (CSV with quotes: QuoteSegments.Cut, or its
/// in-order cut with comment lines): unit i is [starts[i], starts[i + 1]); empty units are allowed.
/// </summary>
internal sealed unsafe class PlannedInput(ReadOnlyMemory<byte>? memory, byte* mapped, long[] starts, UnitCut cut, IDisposable? owned, MappedFile? map)
    : TextUnits
{
    public override int Count => starts.Length - 1;

    public override bool ReadOnce => false;

    public override ValueTask<TextUnitBytes?> GetAsync(int index, bool useAsync, CancellationToken cancellationToken)
    {
        if (index >= Count) return ValueTask.FromResult<TextUnitBytes?>(null);
        long start = starts[index], stop = starts[index + 1];
        if (stop - start > int.MaxValue) throw cut.TooLong(start);
        ReadOnlyMemory<byte> slice = memory is { } m ? m[(int)start..(int)stop] : PointerMemory.Of(mapped + start, (int)(stop - start));
        return ValueTask.FromResult<TextUnitBytes?>(new TextUnitBytes(slice, start == 0, null, stop == starts[^1]));
    }

    public override void Dispose()
    {
        map?.Release();
        owned?.Dispose();
    }
}

/// <summary>Releases a native buffer.</summary>
internal sealed class Releaser(NativeByteBuffer buffer) : IDisposable
{
    public void Dispose() => buffer.Release();
}

/// <summary>
/// A stream read in order (a stream that cannot seek, or decompressed data): units of about a block each, ending at a
/// record start (the rest carried into the next unit). Units read ahead of their decoding are kept until taken.
/// </summary>
internal sealed class StreamInput(Stream stream, bool leaveOpen, int unitBytes, UnitCut cut) : TextUnits
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly Dictionary<int, TextUnitBytes?> _read = [];
    private readonly object _lock = new();
    private NativeByteBuffer? _carry;
    private int _carryLength, _next;
    private bool _ended, _disposed;

    public override int Count => -1;

    public override bool ReadOnce => true;

    public override async ValueTask<TextUnitBytes?> GetAsync(int index, bool useAsync, CancellationToken cancellationToken)
    {
        if (useAsync) await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        else _gate.Wait(cancellationToken);
        try
        {
            while (_next <= index)
            {
                TextUnitBytes? unit = await ReadUnitAsync(useAsync, cancellationToken).ConfigureAwait(false);
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
            if (!_read.TryGetValue(index, out TextUnitBytes? found)) throw new InvalidOperationException($"Unit {index} was released already.");
            return found?.Borrow();
        }
    }

    public override void Release(int index)
    {
        lock (_lock)
        {
            if (_read.Remove(index, out TextUnitBytes? unit)) unit?.Dispose();
        }
    }

    private async ValueTask<TextUnitBytes?> ReadUnitAsync(bool useAsync, CancellationToken ct)
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
            while (true)
            {
                // Read until the block is full (or the stream ends), then cut at its last record start.
                while (!_ended && length < buffer.Capacity)
                {
                    int n = await ReadAsync(stream, buffer, length, useAsync, cut, ct).ConfigureAwait(false);
                    if (n == 0) _ended = true;
                    length += n;
                }
                int last = cut.Last(buffer.Span[..length], _ended);
                if (last > 0 || _ended)
                {
                    int at = last > 0 ? last : length;
                    if (at < length)
                    {
                        _carry ??= new NativeByteBuffer(0, pooled: true);
                        _carry.EnsureCapacity(length - at);
                        buffer.Span[at..length].CopyTo(_carry.Span);
                        _carryLength = length - at;
                    }
                    if (at == 0)
                    {
                        buffer.Release();
                        return null;
                    }
                    return new TextUnitBytes(buffer.Memory[..at], first, new Releaser(buffer));
                }
                // A record longer than the block: read on.
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
            foreach (TextUnitBytes? unit in _read.Values) unit?.Dispose();
            _read.Clear();
        }
        _carry?.Release();
        if (!leaveOpen) stream.Dispose();
    }

    /// <summary>
    /// Reads into <paramref name="buffer"/> from <paramref name="at"/>; damaged compressed input (a header that is not
    /// one, a broken stream) is the cut's format error.
    /// </summary>
    public static async ValueTask<int> ReadAsync(Stream stream, NativeByteBuffer buffer, int at, bool useAsync, UnitCut cut, CancellationToken ct)
    {
        try
        {
            return useAsync
                ? await stream.ReadAsync(buffer.Memory[at..], ct).ConfigureAwait(false)
                : stream.Read(buffer.Span[at..]);
        }
        catch (Exception e) when (e is InvalidDataException or ZstdSharp.ZstdException)
        {
            throw cut.Damaged(e);
        }
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
        // zlib only by the headers compressors write (32 KiB window; fastest, default, best and level 1–5 flags), as the
        // engine's readers sniff it: the general header rule (low nibble 8, divisible by 31) holds for text such as "H,"
        // or "x " (a CSV starting with them would be read as compressed).
        if (head is [0x78, 0x01 or 0x5E or 0x9C or 0xDA, ..]) return new ZLibStream(stream, CompressionMode.Decompress);
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
