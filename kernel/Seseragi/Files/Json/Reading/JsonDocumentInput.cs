using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Json;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Json.Reading;

/// <summary>
/// One JSON document cut into units of whole top-level array elements, found in input order by a
/// <see cref="JsonArrayScanner"/>: a unit ends before the first comma between elements past about a block of bytes (the
/// comma belongs to no unit). The first unit holds the document's start (byte order mark, '['), the last one its end
/// (']' and whatever follows); a document that is not an array is one unit. The scan only picks cut points: the units'
/// decoder validates everything. Memory (bytes in memory, a mapped file) is cut in parallel and sliced in place: it is
/// split into segments of a block each, the threads asking for units summarize segments (a bounded number ahead), and
/// the summaries, taken in order, give the scan state at each segment's start, from which its first cut is found (the
/// cuts are those of one scan in order: a unit ends at the first comma between elements in a segment). Other inputs
/// are read in order into native memory as units are asked for, and scanned as read. A unit read that way is handed to
/// its first reader when the source can be read again by range (later readers read it again); a stream keeps it until
/// <see cref="Release"/>.
/// </summary>
internal sealed class DocumentInput : JsonInput
{
    private const int ReadStep = 64 << 10;
    private const int Ahead = 64;

    private readonly int _unitBytes;
    private readonly IDisposable? _owned;

    // In place.
    private readonly ReadOnlyMemory<byte>? _memory;
    private readonly MappedFile? _map;
    private readonly unsafe byte* _mapped;
    private readonly long _length;
    private int _segments = -1;   // the segments of a block (-1 until the document's start is seen)
    private (bool Odd, long Outside, long Inside)[] _summaries = [];
    private bool[] _summarized = [];
    private int _claimed, _resolved;  // the next segment to summarize; the segments whose start state and cut are known
    private long _depth, _unitStart;  // at the start of segment _resolved
    private long _open;               // where the top-level array opens
    private bool _inString, _closed;  // _closed: no more cuts (the array closed, or the document is not an array)
    private TaskCompletionSource _progress = Signal();

    // Read in order.
    private readonly Stream? _stream;
    private readonly bool _leaveOpen;
    private readonly IRandomAccess? _reread;
    private NativeByteBuffer? _buffer; // the next unit's bytes read so far
    private int _filled, _scanned;
    private bool _ended;

    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly object _lock = new();
    private readonly List<UnitSpan> _units = [];
    private readonly Dictionary<int, JsonUnitBytes> _held = [];
    private JsonArrayScanner _scanner;
    private long _next;
    private bool _all;
    private volatile bool _disposed;

    private readonly record struct UnitSpan(long Start, long End, long Elements, bool Last);

    private DocumentInput(int unitBytes, IDisposable? owned, Stream? stream, bool leaveOpen, IRandomAccess? reread)
    {
        _unitBytes = unitBytes;
        _owned = owned;
        _stream = stream;
        _leaveOpen = leaveOpen;
        _reread = reread;
    }

    private unsafe DocumentInput(int unitBytes, IDisposable? owned, ReadOnlyMemory<byte>? memory, MappedFile? map, byte* mapped, long length)
        : this(unitBytes, owned, null, leaveOpen: false, null)
    {
        _memory = memory;
        _map = map;
        _mapped = mapped;
        _length = length;
    }

    /// <summary>
    /// The document of a source read in place (bytes in memory, a local file mapped when <paramref name="map"/>), else
    /// read in order and again by range.
    /// </summary>
    public static unsafe DocumentInput Of(IRandomAccess source, IDisposable? owned, bool map, int unitBytes)
    {
        long length = source.Length;
        if (source is ILocalSource local)
        {
            if (local.TryGetMemory(out ReadOnlyMemory<byte> memory)) return new DocumentInput(unitBytes, owned, memory, null, null, length);
            if (map && local.Map(out long start) is { } mapped && mapped.TryAcquire())
                return new DocumentInput(unitBytes, owned, null, mapped, mapped.At(start, length), length);
        }
        return new DocumentInput(unitBytes, owned, new RandomAccessStream(source, 0), leaveOpen: false, source);
    }

    /// <summary>The document of a stream, read once.</summary>
    public static DocumentInput Of(Stream stream, bool leaveOpen, int unitBytes) => new(unitBytes, null, stream, leaveOpen, null);

    public override int Count => -1;

    public override bool ReadOnce => _stream is not null && _reread is null;

    /// <summary>Whether the input is read in place (memory or a mapping; for tests).</summary>
    internal bool InPlace => _stream is null;

    public override async ValueTask<JsonUnitBytes?> GetAsync(int index, bool useAsync, CancellationToken cancellationToken)
    {
        if (_stream is null) return await InPlaceAsync(index, useAsync, cancellationToken).ConfigureAwait(false);
        bool found;
        lock (_lock) found = index < _units.Count;
        if (!found)
        {
            if (useAsync) await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            else _gate.Wait(cancellationToken);
            try
            {
                while (!_all && _units.Count <= index)
                {
                    ObjectDisposedException.ThrowIf(_disposed, this);
                    await FindReadingAsync(useAsync, cancellationToken).ConfigureAwait(false);
                }
            }
            finally { _gate.Release(); }
        }
        UnitSpan unit;
        lock (_lock)
        {
            if (index >= _units.Count) return null;
            unit = _units[index];
            if (_reread is null)
                return _held.TryGetValue(index, out JsonUnitBytes? kept) ? kept.Borrow() : throw new InvalidOperationException($"Unit {index} was released already.");
            if (_held.Remove(index, out JsonUnitBytes? first)) return first;
        }
        return await ReadAgainAsync(unit, useAsync, cancellationToken).ConfigureAwait(false);
    }

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

    // A unit of memory: while it is not found, the caller summarizes the next segment (a bounded number ahead of the
    // ones resolved), or waits for the others to.
    private async ValueTask<JsonUnitBytes?> InPlaceAsync(int index, bool useAsync, CancellationToken ct)
    {
        while (true)
        {
            int claim = -1;
            Task progress;
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                if (_segments < 0) Start();
                if (index < _units.Count) return InPlaceBytes(_units[index]);
                if (_all) return null;
                if (!_closed && _claimed < _segments - 1 && _claimed < _resolved + Ahead) claim = _claimed++;
                progress = _progress.Task;
            }
            if (claim >= 0)
            {
                long start = (long)claim * _unitBytes;
                var summary = JsonArrayScanner.Summarize(Slice(start, SegmentLength(claim)), EscapedAt(start));
                lock (_lock)
                {
                    _summaries[claim] = summary;
                    _summarized[claim] = true;
                    Resolve();
                }
                continue;
            }
            if (useAsync) await progress.WaitAsync(ct).ConfigureAwait(false);
            else progress.Wait(ct);
        }
    }

    // The segments, once the document's start tells whether it is an array (only an array is cut).
    private void Start()
    {
        _segments = (int)Math.Max(1, (_length + _unitBytes - 1) / _unitBytes);
        _summaries = new (bool, long, long)[_segments];
        _summarized = new bool[_segments];
        // The first byte after a byte order mark and whitespace: '[' opens the array.
        long at = _length >= 3 && Slice(0, 3).SequenceEqual("\uFEFF"u8) ? 3 : 0;
        while (at < _length)
        {
            int n = (int)Math.Min(_length - at, 1 << 16);
            int i = Slice(at, n).IndexOfAnyExcept(" \t\r\n"u8);
            if (i >= 0)
            {
                at += i;
                break;
            }
            at += n;
        }
        _open = at;
        _closed = at >= _length || Slice(at, 1)[0] != '[';
        Resolve();
    }

    private int SegmentLength(int segment) => (int)Math.Min(_unitBytes, _length - (long)segment * _unitBytes);

    // Whether the byte at position is escaped (the backslashes before it, looked back at a window at a time).
    private bool EscapedAt(long position)
    {
        long run = 0;
        for (long end = position; end > 0;)
        {
            int n = (int)Math.Min(end, 4096);
            ReadOnlySpan<byte> before = Slice(end - n, n);
            int other = before.LastIndexOfAnyExcept((byte)'\\');
            run += n - 1 - other;
            if (other >= 0) break;
            end -= n;
        }
        return (run & 1) == 1;
    }

    // Under the lock: the segments whose predecessors are summarized get their start state and first cut, in order;
    // the last unit once every segment is.
    private void Resolve()
    {
        int before = _resolved;
        while (_resolved < _segments)
        {
            int k = _resolved;
            if (k > 0 && !_closed)
            {
                if (!_summarized[k - 1]) break;
                var (odd, outside, inside) = _summaries[k - 1];
                _depth += _inString ? inside : outside;
                _inString ^= odd;
                long start = (long)k * _unitBytes;
                if (start > _open)
                {
                    var scanner = JsonArrayScanner.Resume(_depth, _inString, EscapedAt(start));
                    int position = 0;
                    long elements = 0;
                    int cut = scanner.Scan(Slice(start, SegmentLength(k)), ref position, 0, final: true, fileStart: false, ref elements);
                    if (cut >= 0)
                    {
                        Add(_unitStart, start + cut, 0, last: false, null);
                        _unitStart = start + cut + 1;
                    }
                    else if (scanner.Done) _closed = true;
                }
            }
            _resolved++;
        }
        if (_resolved == _segments && !_all) Add(_unitStart, _length, 0, last: true, null);
        // Segments resolved without a cut (inside a long element) let the look-ahead go on: wake the waiting callers.
        else if (_resolved != before) Raise(ref _progress);
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

    private unsafe ReadOnlySpan<byte> Slice(long start, int length) =>
        _memory is { } memory ? memory.Span.Slice((int)start, length) : new ReadOnlySpan<byte>(_mapped + start, length);

    private unsafe JsonUnitBytes InPlaceBytes(UnitSpan unit)
    {
        int length = Length(unit);
        ReadOnlyMemory<byte> bytes = _memory is { } memory ? memory.Slice((int)unit.Start, length) : PointerMemory.Of(_mapped + unit.Start, length);
        return new JsonUnitBytes(bytes, unit.Start == 0, null, unit.Last, unit.Elements);
    }

    private static int Length(UnitSpan unit) => unit.End - unit.Start <= Array.MaxLength
        ? (int)(unit.End - unit.Start)
        : throw new JsonFormatException($"An element of the document at byte {unit.Start} is longer than 2 GB.");

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
            int n = useAsync
                ? await _stream!.ReadAsync(_buffer.Memory[_filled..], ct).ConfigureAwait(false)
                : _stream!.Read(_buffer.Span[_filled..]);
            if (n == 0) _ended = true;
            _filled += n;
        }
    }

    // A unit handed out before, read again by its range.
    private async ValueTask<JsonUnitBytes> ReadAgainAsync(UnitSpan unit, bool useAsync, CancellationToken ct)
    {
        int length = Length(unit);
        var buffer = new NativeByteBuffer(length, pooled: true);
        try
        {
            int done = 0;
            while (done < length)
            {
                int n = useAsync
                    ? await _reread!.ReadAtAsync(unit.Start + done, buffer.Memory[done..length], ct).ConfigureAwait(false)
                    : _reread!.ReadAt(unit.Start + done, buffer.Span[done..length]);
                if (n <= 0) throw new EndOfStreamException("The source ended before its length.");
                done += n;
            }
            return new JsonUnitBytes(buffer.Memory[..length], unit.Start == 0, new Releaser(buffer), unit.Last, unit.Elements);
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
            foreach (JsonUnitBytes unit in _held.Values) unit.Dispose();
            _held.Clear();
            Raise(ref _progress);
        }
        // A unit being read holds the gate (and the buffer): the buffer's finalizer frees it then.
        if (_gate.Wait(0))
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
