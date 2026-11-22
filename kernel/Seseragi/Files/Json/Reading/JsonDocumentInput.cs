using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Json;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Json.Reading;

/// <summary>
/// One JSON document cut into units of whole top-level array elements, found by a <see cref="JsonArrayScanner"/>: a unit
/// ends before a comma between elements (the comma belongs to no unit). The first unit holds the document's start (byte
/// order mark, '['), the last one its end (']' and whatever follows); a document that is not an array is one unit. The
/// scan only picks cut points: the units' decoder validates everything.
/// <para>
/// A source read at offsets is cut in parallel: it is split into segments of a block each, the threads asking for units
/// summarize segments (a bounded number ahead), and the summaries, taken in order, give the scan state at each segment's
/// start, from which its first cut is found (the cuts are those of one scan in order: a unit ends at the first comma
/// between elements in a segment). Memory (bytes in memory, a mapped file) is summarized and sliced in place; another
/// source (a store's object, a file not mapped) has each segment read by the thread that claims it; a unit is lent its
/// first segment, the next segment's first bytes (up to the unit's cut) copied after it, or is copied from the
/// segments it spans when they do not fit (a segment is freed once no unit is lent or built from it; later readers read
/// a unit again by its range).
/// </para>
/// <para>
/// A stream (that cannot seek, or decompressed data) is read in order into native memory as units are asked for, scanned
/// as read, and keeps a unit until <see cref="Release"/>.
/// </para>
/// </summary>
internal sealed class DocumentInput : JsonInput
{
    private const int ReadStep = 64 << 10;
    private const int InPlaceAhead = 64;
    private const int HeadWindow = 64 << 10;

    private readonly int _unitBytes;
    private readonly IDisposable? _owned;
    private readonly object _lock = new();
    private readonly List<UnitSpan> _units = [];
    private bool _all;
    private volatile bool _disposed;

    // Cut in parallel: in place (memory, mapping) or read at offsets (_source).
    private readonly ReadOnlyMemory<byte>? _memory;
    private readonly MappedFile? _map;
    private readonly unsafe byte* _mapped;
    private readonly IRandomAccess? _source;
    private readonly long _length;
    private readonly int _ahead;              // segments summarized ahead of the ones resolved
    private readonly int _slack;              // room after a read segment for the next one's first element
    private readonly SemaphoreSlim _gate = new(1, 1);
    private int _segments = -1;               // -1 until the document's start is seen
    private Summary[] _summaries = [];
    private bool[] _summarized = [];
    private NativeByteBuffer?[] _buffers = []; // read segments (until every unit over them is built)
    private int[] _refs = [];                  // units over each read segment not built yet
    private bool[] _built = [];                // per unit: built from the segments (later reads read it again)
    private int _claimed, _resolved, _busy;
    private long _depth, _unitStart, _backslashes, _open; // at the start of the last segment resolved; where the array opens
    private bool _inString, _escaped, _closed;            // _closed: no more cuts (closed array, or not an array)
    private TaskCompletionSource _progress = Signal();

    // Read in order.
    private readonly Stream? _stream;
    private readonly bool _leaveOpen;
    private readonly Dictionary<int, JsonUnitBytes> _held = [];
    private NativeByteBuffer? _buffer; // the next unit's bytes read so far
    private int _filled, _scanned;
    private bool _ended;
    private JsonArrayScanner _scanner;
    private long _next;

    private readonly record struct UnitSpan(long Start, long End, long Elements, bool Last);

    // A segment summarized: its quotes and depth (JsonArrayScanner.Summarize), and its trailing run of backslashes.
    private readonly record struct Summary(bool Odd, long Outside, long Inside, long Trailing, bool AllBackslashes);

    private DocumentInput(int unitBytes, IDisposable? owned, Stream stream, bool leaveOpen)
    {
        _unitBytes = unitBytes;
        _owned = owned;
        _stream = stream;
        _leaveOpen = leaveOpen;
    }

    private unsafe DocumentInput(int unitBytes, IDisposable? owned, ReadOnlyMemory<byte>? memory, MappedFile? map, byte* mapped,
        IRandomAccess? source, long length, int ahead)
    {
        _unitBytes = unitBytes;
        _owned = owned;
        _memory = memory;
        _map = map;
        _mapped = mapped;
        _source = source;
        _length = length;
        _ahead = ahead;
        // A sixteenth of a segment (64 KiB of 1 MiB): a document left whole (not an array) holds every segment.
        _slack = unitBytes / 16;
    }

    /// <summary>
    /// The document of a source read at offsets: in place (bytes in memory, a local file mapped when
    /// <paramref name="map"/>), else read a segment at a time, <paramref name="threads"/> × 2 segments ahead at most.
    /// </summary>
    public static unsafe DocumentInput Of(IRandomAccess source, IDisposable? owned, bool map, int unitBytes, int threads)
    {
        long length = source.Length;
        if (source is ILocalSource local)
        {
            if (local.TryGetMemory(out ReadOnlyMemory<byte> memory)) return new DocumentInput(unitBytes, owned, memory, null, null, null, length, InPlaceAhead);
            if (map && local.Map(out long start) is { } mapped && mapped.TryAcquire())
                return new DocumentInput(unitBytes, owned, null, mapped, mapped.At(start, length), null, length, InPlaceAhead);
        }
        return new DocumentInput(unitBytes, owned, null, null, null, source, length, Math.Max(2, 2 * threads));
    }

    /// <summary>The document of a stream, read once.</summary>
    public static DocumentInput Of(Stream stream, bool leaveOpen, int unitBytes) => new(unitBytes, null, stream, leaveOpen);

    public override int Count => -1;

    public override bool ReadOnce => _stream is not null;

    /// <summary>Whether the input is read in place (memory or a mapping; for tests).</summary>
    internal bool InPlace => _stream is null && _source is null;

    /// <summary>The segments read and not freed yet (for tests).</summary>
    internal int HeldSegments
    {
        get
        {
            lock (_lock) return _buffers.Count(b => b is not null);
        }
    }

    public override async ValueTask<JsonUnitBytes?> GetAsync(int index, bool useAsync, CancellationToken cancellationToken) =>
        _stream is null
            ? await ParallelAsync(index, useAsync, cancellationToken).ConfigureAwait(false)
            : await InOrderAsync(index, useAsync, cancellationToken).ConfigureAwait(false);

    public override void Release(int index)
    {
        lock (_lock)
        {
            if (_held.Remove(index, out JsonUnitBytes? unit)) unit.Dispose();
        }
    }

    private static TaskCompletionSource Signal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Completes a signal (under the lock), replaced by a fresh one.
    private static void Raise(ref TaskCompletionSource signal)
    {
        TaskCompletionSource raised = signal;
        signal = Signal();
        raised.SetResult();
    }

    private void Add(long start, long end, long elements, bool last, JsonUnitBytes? bytes)
    {
        lock (_lock)
        {
            if (_disposed)
            {
                bytes?.Dispose();
                throw new ObjectDisposedException(nameof(DocumentInput));
            }
            if (bytes is not null) _held[_units.Count] = bytes;
            _units.Add(new UnitSpan(start, end, elements, last));
            _next = end + 1;
            _all = last;
            Raise(ref _progress);
        }
    }

    private static int Length(UnitSpan unit) => unit.End - unit.Start <= Array.MaxLength
        ? (int)(unit.End - unit.Start)
        : throw new JsonFormatException($"An element of the document at byte {unit.Start} is longer than 2 GB.");

    // ------------------------------------------------------------------ cut in parallel

    // A unit: while it is not found, the caller summarizes the next segment (reading it first when not in place), a
    // bounded number ahead of the ones resolved, or waits for the others to.
    private async ValueTask<JsonUnitBytes?> ParallelAsync(int index, bool useAsync, CancellationToken ct)
    {
        if (_segments < 0) await StartAsync(useAsync, ct).ConfigureAwait(false);
        while (true)
        {
            int claim = -1;
            Task progress;
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                if (index < _units.Count) break;
                if (_all) return null;
                if (_claimed < _segments && _claimed < _resolved + _ahead && (!_closed || _source is not null)) claim = _claimed++;
                progress = _progress.Task;
                if (claim >= 0) _busy++;
            }
            if (claim >= 0)
            {
                try { await SummarizeAsync(claim, useAsync, ct).ConfigureAwait(false); }
                finally { Leave(); }
                continue;
            }
            if (useAsync) await progress.WaitAsync(ct).ConfigureAwait(false);
            else progress.Wait(ct);
        }
        return _source is null ? InPlaceBytes(index) : await BuildAsync(index, useAsync, ct).ConfigureAwait(false);
    }

    // A claimed segment: read (unless in place), summarized, then the segments that can be resolved are.
    private async ValueTask SummarizeAsync(int segment, bool useAsync, CancellationToken ct)
    {
        long start = (long)segment * _unitBytes;
        int length = SegmentLength(segment);
        NativeByteBuffer? buffer = null;
        if (_source is not null)
        {
            buffer = new NativeByteBuffer(Math.Max(1, length + _slack), pooled: true);
            try { await ReadAtAsync(start, buffer, length, useAsync, ct).ConfigureAwait(false); }
            catch
            {
                buffer.Release();
                throw;
            }
        }
        ReadOnlySpan<byte> bytes = buffer is null ? Slice(start, length) : buffer.Span[..length];
        Summary summary = Summarize(bytes, escaped: false);
        lock (_lock)
        {
            if (_disposed)
            {
                buffer?.Release();
                return;
            }
            _buffers[segment] = buffer;
            _summaries[segment] = summary;
            _summarized[segment] = true;
            Resolve();
        }
    }

    private static Summary Summarize(ReadOnlySpan<byte> bytes, bool escaped)
    {
        var (odd, outside, inside) = JsonArrayScanner.Summarize(bytes, escaped);
        int other = bytes.LastIndexOfAnyExcept((byte)'\\');
        return new Summary(odd, outside, inside, bytes.Length - 1 - other, other < 0);
    }

    // The segments, once the document's start tells whether it is an array (only an array is cut) and where it opens.
    private async ValueTask StartAsync(bool useAsync, CancellationToken ct)
    {
        if (useAsync) await _gate.WaitAsync(ct).ConfigureAwait(false);
        else _gate.Wait(ct);
        try
        {
            if (_segments >= 0) return;
            long open = await OpenAsync(useAsync, ct).ConfigureAwait(false);
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                int segments = (int)Math.Max(1, (_length + _unitBytes - 1) / _unitBytes);
                _summaries = new Summary[segments];
                _summarized = new bool[segments];
                _buffers = new NativeByteBuffer?[segments];
                _refs = new int[segments];
                _open = Math.Max(0, open);
                _closed = open < 0;
                _segments = segments;
                Resolve();
            }
        }
        finally { _gate.Release(); }
    }

    // Where the top-level array opens: the first byte after a byte order mark and whitespace, when it is '['; else -1.
    private async ValueTask<long> OpenAsync(bool useAsync, CancellationToken ct)
    {
        NativeByteBuffer? window = _source is null ? null : new NativeByteBuffer(HeadWindow, pooled: true);
        try
        {
            for (long at = 0; at < _length;)
            {
                int n = (int)Math.Min(_length - at, HeadWindow);
                if (window is not null) await ReadAtAsync(at, window, n, useAsync, ct).ConfigureAwait(false);
                int i = Next(window is null ? Slice(at, n) : window.Span[..n], at);
                if (i >= 0) return (window is null ? Slice(at + i, 1)[0] : window.Span[i]) == '[' ? at + i : -1;
                at += n;
            }
            return -1;
        }
        finally { window?.Release(); }

        // The first byte that is neither whitespace nor (at the start) a byte order mark.
        static int Next(ReadOnlySpan<byte> bytes, long at)
        {
            int from = at == 0 && bytes.StartsWith("﻿"u8) ? 3 : 0;
            int i = bytes[from..].IndexOfAnyExcept(" \t\r\n"u8);
            return i < 0 ? -1 : from + i;
        }
    }

    private int SegmentLength(int segment) => (int)Math.Min(_unitBytes, _length - (long)segment * _unitBytes);

    // A summarized segment's bytes.
    private ReadOnlySpan<byte> Segment(int segment) =>
        _buffers[segment] is { } buffer ? buffer.Span[..SegmentLength(segment)] : Slice((long)segment * _unitBytes, SegmentLength(segment));

    // Under the lock: the segments whose predecessor is summarized (and which are read, when not in place) get their
    // start state and first cut, in order; the last unit once every segment is.
    private void Resolve()
    {
        int before = _resolved;
        while (_resolved < _segments)
        {
            int k = _resolved;
            // A segment read at offsets is resolved once read (its bytes make units, even past a closed array).
            if (_source is not null && !_summarized[k]) break;
            if (k > 0 && !_closed)
            {
                if (!_summarized[k - 1]) break;
                // The summary assumed an unescaped first byte: summarized again when the segment starts escaped.
                Summary previous = _escaped ? Summarize(Segment(k - 1), escaped: true) : _summaries[k - 1];
                _depth += _inString ? previous.Inside : previous.Outside;
                _inString ^= previous.Odd;
                _backslashes = previous.AllBackslashes ? _backslashes + SegmentLength(k - 1) : previous.Trailing;
                _escaped = (_backslashes & 1) == 1;
                long start = (long)k * _unitBytes;
                if (start > _open)
                {
                    var scanner = JsonArrayScanner.Resume(_depth, _inString, _escaped);
                    int position = 0;
                    long elements = 0;
                    int cut = scanner.Scan(Segment(k), ref position, 0, final: true, fileStart: false, ref elements);
                    if (cut >= 0) Cut(start + cut, last: false);
                    else if (scanner.Done) _closed = true;
                }
            }
            _resolved++;
        }
        if (_resolved == _segments && !_all) Cut(_length, last: true);
        // Segments resolved without a cut (inside a long element) let the look-ahead go on: wake the waiting callers.
        else if (_resolved != before) Raise(ref _progress);
    }

    // A unit ends at end (a comma, or the document's end): the read segments it spans wait for it to be built.
    private void Cut(long end, bool last)
    {
        long start = _unitStart;
        Add(start, end, 0, last, null);
        _unitStart = end + 1;
        if (_source is null) return;
        if (_built.Length < _units.Count) Array.Resize(ref _built, Math.Max(_units.Count, 2 * _built.Length));
        if (end > start)
            for (long s = start / _unitBytes; s <= (end - 1) / _unitBytes; s++) _refs[s]++;
        // Segments wholly before the next unit that no unit holds (a cut comma alone) go.
        for (long s = start / _unitBytes; s < Math.Min(_segments, last ? _segments : _unitStart / _unitBytes); s++) Free((int)s);
    }

    // Under the lock: a read segment is freed once no unit is to be built from it and the next unit starts after it.
    private void Free(int segment)
    {
        if (_buffers[segment] is not { } buffer || _refs[segment] > 0 || _busy > 0 && _disposed) return;
        if (!_all && (long)(segment + 1) * _unitBytes > _unitStart) return;
        // Resolving segment k reads segments k - 1 and k.
        if (!_all && segment >= _resolved - 1) return;
        buffer.Release();
        _buffers[segment] = null;
    }

    private unsafe ReadOnlySpan<byte> Slice(long start, int length) =>
        _memory is { } memory ? memory.Span.Slice((int)start, length) : new ReadOnlySpan<byte>(_mapped + start, length);

    private unsafe JsonUnitBytes InPlaceBytes(int index)
    {
        UnitSpan unit;
        lock (_lock) unit = _units[index];
        int length = Length(unit);
        ReadOnlyMemory<byte> bytes = _memory is { } memory ? memory.Slice((int)unit.Start, length) : PointerMemory.Of(_mapped + unit.Start, length);
        return new JsonUnitBytes(bytes, unit.Start == 0, null, unit.Last, unit.Elements);
    }

    // A unit read at offsets, the first time: lent its first segment (the next one's bytes up to the cut copied after
    // it), or copied from its segments when they do not fit; later, read again by its range.
    private async ValueTask<JsonUnitBytes> BuildAsync(int index, bool useAsync, CancellationToken ct)
    {
        UnitSpan unit;
        bool first;
        lock (_lock)
        {
            unit = _units[index];
            first = !_built[index];
            _built[index] = true;
            if (first) _busy++;
        }
        int length = Length(unit);
        if (first && unit.End > unit.Start)
        {
            int a = (int)(unit.Start / _unitBytes), b = (int)((unit.End - 1) / _unitBytes);
            long head = unit.End - (long)b * _unitBytes;
            if (b == a || b == a + 1 && head <= _slack)
            {
                // The segments over the unit stay while it holds them; segment a until the unit is disposed.
                NativeByteBuffer lent = _buffers[a]!;
                if (b > a) _buffers[b]!.Span[..(int)head].CopyTo(lent.Span[_unitBytes..]);
                lock (_lock)
                {
                    if (b > a)
                    {
                        _refs[b]--;
                        Free(b);
                    }
                }
                int from = (int)(unit.Start - (long)a * _unitBytes);
                return new JsonUnitBytes(lent.Memory.Slice(from, length), unit.Start == 0, new Lease(this, a), unit.Last, unit.Elements);
            }
        }
        var buffer = new NativeByteBuffer(Math.Max(1, length), pooled: true);
        try
        {
            if (!first) await ReadAtAsync(unit.Start, buffer, length, useAsync, ct).ConfigureAwait(false);
            else
            {
                try
                {
                    for (long at = unit.Start; at < unit.End;)
                    {
                        int segment = (int)(at / _unitBytes);
                        int offset = (int)(at - (long)segment * _unitBytes);
                        int n = (int)Math.Min(unit.End - at, SegmentLength(segment) - offset);
                        _buffers[segment]!.Span.Slice(offset, n).CopyTo(buffer.Span[(int)(at - unit.Start)..]);
                        at += n;
                    }
                }
                finally
                {
                    lock (_lock)
                    {
                        if (unit.End > unit.Start)
                            for (long s = unit.Start / _unitBytes; s <= (unit.End - 1) / _unitBytes; s++)
                            {
                                _refs[s]--;
                                Free((int)s);
                            }
                    }
                    Leave();
                }
            }
            return new JsonUnitBytes(buffer.Memory[..length], unit.Start == 0, new Releaser(buffer), unit.Last, unit.Elements);
        }
        catch
        {
            buffer.Release();
            throw;
        }
    }

    // A segment lent to a unit, returned when the unit is disposed.
    private sealed class Lease(DocumentInput input, int segment) : IDisposable
    {
        private int _returned;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _returned, 1) != 0) return;
            lock (input._lock)
            {
                input._refs[segment]--;
                input.Free(segment);
            }
            input.Leave();
        }
    }

    // Work on the segments is over: after disposal, the last one out frees them.
    private void Leave()
    {
        lock (_lock)
        {
            if (--_busy == 0 && _disposed) FreeAll();
        }
    }

    private void FreeAll()
    {
        for (int s = 0; s < _buffers.Length; s++)
        {
            _buffers[s]?.Release();
            _buffers[s] = null;
        }
    }

    private async ValueTask ReadAtAsync(long offset, NativeByteBuffer buffer, int count, bool useAsync, CancellationToken ct)
    {
        int done = 0;
        while (done < count)
        {
            int n = useAsync
                ? await _source!.ReadAtAsync(offset + done, buffer.Memory[done..count], ct).ConfigureAwait(false)
                : _source!.ReadAt(offset + done, buffer.Span[done..count]);
            if (n <= 0) throw new EndOfStreamException("The source ended before its length.");
            done += n;
        }
    }

    // ------------------------------------------------------------------ read in order

    // A unit of the stream: units are read and scanned in order as they are asked for, then kept until released.
    private async ValueTask<JsonUnitBytes?> InOrderAsync(int index, bool useAsync, CancellationToken ct)
    {
        bool found;
        lock (_lock) found = index < _units.Count;
        if (!found)
        {
            if (useAsync) await _gate.WaitAsync(ct).ConfigureAwait(false);
            else _gate.Wait(ct);
            try
            {
                while (!_all && _units.Count <= index)
                {
                    ObjectDisposedException.ThrowIf(_disposed, this);
                    await FindReadingAsync(useAsync, ct).ConfigureAwait(false);
                }
            }
            finally { _gate.Release(); }
        }
        lock (_lock)
        {
            if (index >= _units.Count) return null;
            return _held.TryGetValue(index, out JsonUnitBytes? kept) ? kept.Borrow() : throw new InvalidOperationException($"Unit {index} was released already.");
        }
    }

    // The next unit read from the stream: read on until the scan cuts (or the stream ends); the bytes past the cut stay
    // for the next unit.
    private async ValueTask FindReadingAsync(bool useAsync, CancellationToken ct)
    {
        long start = _next, elements = 0;
        _buffer ??= new NativeByteBuffer(_unitBytes + ReadStep, pooled: true);
        while (true)
        {
            if (!_scanner.Done)
            {
                int position = _scanned;
                int cut = _scanner.Scan(_buffer.Span[.._filled], ref position, _unitBytes, _ended, start == 0, ref elements);
                _scanned = position;
                if (cut >= 0)
                {
                    NativeByteBuffer unit = _buffer;
                    int rest = _filled - cut - 1;
                    _buffer = new NativeByteBuffer(Math.Max(_unitBytes + ReadStep, rest), pooled: true);
                    unit.Span.Slice(cut + 1, rest).CopyTo(_buffer.Span);
                    _filled = rest;
                    _scanned -= cut + 1;
                    Add(start, start + cut, elements, last: false, new JsonUnitBytes(unit.Memory[..cut], start == 0, new Releaser(unit), false, elements));
                    return;
                }
            }
            if (_ended)
            {
                NativeByteBuffer unit = _buffer;
                _buffer = null;
                Add(start, start + _filled, elements + 1, last: true, new JsonUnitBytes(unit.Memory[.._filled], start == 0, new Releaser(unit), true, elements + 1));
                return;
            }
            if (_filled == _buffer.Capacity) _buffer.EnsureCapacity((long)_buffer.Capacity * 2);
            int n = await StreamInput.ReadAsync(_stream!, _buffer, _filled, useAsync, ct).ConfigureAwait(false);
            if (n == 0) _ended = true;
            _filled += n;
        }
    }

    public override void Dispose()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            foreach (JsonUnitBytes unit in _held.Values) unit.Dispose();
            _held.Clear();
            // Segments being read or copied from are freed by the last one out.
            if (_busy == 0) FreeAll();
            Raise(ref _progress);
        }
        // A unit being read in order holds the gate (and the buffer): the buffer's finalizer frees it then.
        if (_stream is not null && _gate.Wait(0))
        {
            _buffer?.Release();
            _buffer = null;
            _gate.Release();
        }
        _map?.Release();
        if (_stream is not null && !_leaveOpen) _stream.Dispose();
        _owned?.Dispose();
    }
}
