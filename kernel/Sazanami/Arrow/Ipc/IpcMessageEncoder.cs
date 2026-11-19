using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>A message ready to write: its prefix and metadata (padded) and its body (buffers at their offsets).</summary>
internal sealed class IpcOutgoingMessage : IDisposable
{
    private readonly Func<IpcRecordBatch, long, byte[]>? _describe;
    private readonly long _rows;
    private readonly IpcCompression _compression;
    private readonly TaskCompletionSource? _compressed;
    private byte[]? _flatBuffer;
    private byte[]? _placeholder;

    /// <summary>A message without a body (the schema), framed already.</summary>
    public IpcOutgoingMessage(byte[] framed)
    {
        Metadata = framed;
        Layout = [];
    }

    /// <summary>A batch message whose body is still to be compressed (<see cref="Compress"/>) and framed (<see cref="Frame"/>).</summary>
    public IpcOutgoingMessage(IpcEncodedBody body, long rows, IpcCompression compression, bool isDictionary, Func<IpcRecordBatch, long, byte[]> describe)
    {
        Body = body;
        _rows = rows;
        _compression = compression;
        IsDictionary = isDictionary;
        _describe = describe;
        Layout = [];
        _compressed = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>The continuation marker, the metadata length and the metadata, padded so that the body starts 64-byte aligned (once framed).</summary>
    public byte[] Metadata { get; private set; } = [];

    public IpcEncodedBody? Body { get; }

    public IpcBuffer[] Layout { get; private set; }

    public long BodyLength { get; private set; }

    public bool IsDictionary { get; }

    /// <summary>The bytes of the buffers before compression (what the message holds in memory until written).</summary>
    public long RawBytes => Body?.Buffers.Sum(b => b.Length) ?? 0;

    /// <summary>Ends once <see cref="Compress"/> has (failed with it).</summary>
    public Task Compressed => _compressed?.Task ?? Task.CompletedTask;

    /// <summary>With <see cref="StreamBody"/>, the buffers handed over as they are compressed.</summary>
    public IpcBodyStream? Streamed { get; private set; }

    /// <summary>
    /// Hands the buffers over one by one as <see cref="Compress"/> finishes them (<see cref="Streamed"/>), for a writer
    /// that writes the body before its metadata is known: in its place goes <see cref="Placeholder"/>, as long as the
    /// metadata will be (the FlatBuffer's length depends only on the counts of nodes, buffers and variadic counts).
    /// Before <see cref="Compress"/>, with compression.
    /// </summary>
    public void StreamBody()
    {
        if (Body is null || _compression == IpcCompression.None) throw new InvalidOperationException("Only a compressed body is handed over as it is compressed.");
        var header = new IpcRecordBatch(_rows, [.. Body.Nodes], new IpcBuffer[Body.Buffers.Count], _compression, [.. Body.VariadicCounts]);
        _placeholder = _describe!(header, 0);
        Streamed = new IpcBodyStream(Body.Buffers.Count);
    }

    /// <summary>The framed metadata of a message written at <paramref name="position"/> with every buffer and the body length 0 (<see cref="StreamBody"/>).</summary>
    public byte[] Placeholder(long position) => IpcMessageEncoder.Frame(_placeholder ?? throw new InvalidOperationException("The body is not streamed."), position);

    /// <summary>
    /// Compresses the body (on up to <paramref name="parallelism"/> threads) and lays it out; any thread may run it,
    /// several messages at once. The metadata is then known but for its framing. With <paramref name="detach"/>, the
    /// body then no longer reads the arrays it was made from.
    /// </summary>
    public void Compress(int level, int parallelism, int zstdFrameBytes, bool detach = false, CancellationToken cancellationToken = default)
    {
        if (Body is null || _flatBuffer is not null) return;
        try
        {
            IpcBatchEncoder.Compress(Body, _compression, level, parallelism, zstdFrameBytes, Streamed, detach, cancellationToken);
            if (detach)
            {
                if (Streamed is null) Body.Own();
                else Body.ReleasePins(); // every buffer stored as it is was copied
            }
            (IpcBuffer[] layout, long length) = IpcBatchEncoder.Layout(Body.Buffers);
            var header = new IpcRecordBatch(_rows, [.. Body.Nodes], layout, _compression, [.. Body.VariadicCounts]);
            byte[] flatBuffer = _describe!(header, length);
            if (_placeholder is not null && flatBuffer.Length != _placeholder.Length)
                throw new InvalidOperationException($"The metadata ({flatBuffer.Length} bytes) does not fit its placeholder ({_placeholder.Length} bytes).");
            Layout = layout;
            BodyLength = length;
            _flatBuffer = flatBuffer;
            _compressed!.TrySetResult();
        }
        catch (Exception e)
        {
            Fail(e);
            throw;
        }
    }

    /// <summary>The message will not be compressed (<paramref name="error"/>): whoever waits for it fails with it.</summary>
    public void Fail(Exception error)
    {
        Streamed?.Fail(error);
        if (_compressed?.TrySetException(error) == true) _ = _compressed.Task.Exception; // observed: a writer may have stopped earlier
    }

    /// <summary>Compresses messages one after the other; when one fails, those not compressed fail with it.</summary>
    public static void CompressAll(IReadOnlyList<IpcOutgoingMessage> messages, (int Level, int Parallelism, int ZstdFrameBytes) settings, bool detach,
        CancellationToken cancellationToken)
    {
        int k = 0;
        try
        {
            for (; k < messages.Count; k++) messages[k].Compress(settings.Level, settings.Parallelism, settings.ZstdFrameBytes, detach, cancellationToken);
        }
        catch (Exception e)
        {
            for (; k < messages.Count; k++) messages[k].Fail(e);
            throw;
        }
    }

    /// <summary>Frames the metadata for a message written at <paramref name="position"/> (after <see cref="Compress"/>).</summary>
    public void Frame(long position)
    {
        if (Body is null) return;
        Metadata = IpcMessageEncoder.Frame(_flatBuffer ?? throw new InvalidOperationException("The message is not compressed yet."), position);
    }

    /// <summary>The bytes the message takes in the output (once framed).</summary>
    public long Size => Metadata.Length + BodyLength;

    /// <summary>Frees the body (never while it is compressed).</summary>
    public void Dispose()
    {
        Body?.Dispose();
        Streamed?.Dispose();
    }
}

/// <summary>
/// The writing side of the IPC formats, short of I/O: turns batches into messages (dictionary batches first, then the
/// record batch), keeping each dictionary as last written to tell what a new one is: the same (nothing written),
/// longer with the same first values (a delta), or other (a replacement; refused in the file format, which allows
/// none). Not thread-safe; compression runs in parallel inside one batch.
/// </summary>
internal sealed unsafe class IpcMessageEncoder(Schema schema, IpcCompression compression, int level, int parallelism, bool fileFormat,
    int zstdFrameBytes = IpcBatchEncoder.ZstdFrameBytes) : IDisposable
{
    /// <summary>The schema in its IPC form (extension types as storage, dictionary ids).</summary>
    public IpcSchema Schema { get; } = IpcMetadataWriter.Schema(schema);

    private readonly IpcDictionaries _written = new(); // dictionaries nested in others' values, as last written, whole
    private readonly Dictionary<long, List<Segment>> _kept = []; // the others, in segments (Remember)
    private readonly HashSet<long> _nested = NestedDictionaries(schema);

    /// <summary>The schema message.</summary>
    public IpcOutgoingMessage SchemaMessage(long position, IReadOnlyList<KeyValuePair<string, string>>? metadata) =>
        new(Frame(IpcMetadataWriter.SchemaMessage(Schema, metadata), position));

    /// <summary>
    /// The messages of a batch, written from <paramref name="position"/> on: the dictionary batches it needs, then the
    /// record batch, compressed and framed. The caller disposes them once written.
    /// </summary>
    /// <exception cref="ArgumentException">The batch does not match the schema.</exception>
    /// <exception cref="InvalidOperationException">A dictionary is replaced in the file format.</exception>
    public List<IpcOutgoingMessage> Batch(RecordBatch batch, IReadOnlyList<KeyValuePair<string, string>>? metadata, long position)
    {
        List<IpcOutgoingMessage> messages = Prepare(batch, metadata);
        try
        {
            foreach (IpcOutgoingMessage message in messages)
            {
                message.Compress(level, parallelism, zstdFrameBytes);
                message.Frame(position);
                position += message.Size;
            }
            return messages;
        }
        catch
        {
            foreach (IpcOutgoingMessage m in messages) m.Dispose();
            throw;
        }
    }

    /// <summary>
    /// The messages of a batch before compression and framing (in order: the dictionary batches it needs, then the
    /// record batch): the batch is checked and its dictionaries settled here, in the order batches are written; the
    /// bodies still read the batch's buffers. Then each message is compressed (<see cref="IpcOutgoingMessage.Compress"/>,
    /// on any thread) and framed where it is written (<see cref="IpcOutgoingMessage.Frame"/>, in order).
    /// </summary>
    /// <exception cref="ArgumentException">The batch does not match the schema.</exception>
    /// <exception cref="InvalidOperationException">A dictionary is replaced in the file format.</exception>
    public List<IpcOutgoingMessage> Prepare(RecordBatch batch, IReadOnlyList<KeyValuePair<string, string>>? metadata)
    {
        if (batch.ColumnCount != Schema.Fields.Length)
            throw new ArgumentException($"The batch has {batch.ColumnCount} columns; the schema has {Schema.Fields.Length}.", nameof(batch));
        List<IpcOutgoingMessage> messages = [];
        var body = new IpcEncodedBody();
        try
        {
            IpcBatchEncoder.Encode(body, Schema.Fields, [.. Enumerable.Range(0, batch.ColumnCount).Select(c => batch.Column(c).Data)], 0, batch.Length);
            foreach (var (id, field, dictionary) in body.Dictionaries) Dictionary(id, field, dictionary, messages);
            messages.Add(new IpcOutgoingMessage(body, batch.Length, compression, false,
                (header, length) => IpcMetadataWriter.RecordBatchMessage(header, length, metadata)));
            return messages;
        }
        catch
        {
            body.Dispose();
            foreach (IpcOutgoingMessage m in messages) m.Dispose();
            throw;
        }
    }

    /// <summary>The compression settings of the messages (for writers that compress them elsewhere).</summary>
    public (int Level, int Parallelism, int ZstdFrameBytes) CompressionSettings => (level, parallelism, zstdFrameBytes);

    /// <summary>Writes what dictionary <paramref name="id"/> needs (nothing, a delta or the whole dictionary), after the dictionaries its values use.</summary>
    private void Dictionary(long id, IpcField field, ArrayData dictionary, List<IpcOutgoingMessage> messages)
    {
        IpcField values = field.ValueField();
        int length = dictionary.Length;
        int first = 0;
        bool delta = false;
        if (Kept(id, field) is { } kept)
        {
            int last = kept.Sum(k => k.Values.Length);
            bool extends = length >= last && SamePrefix(values, kept, dictionary);
            if (extends && length == last) return; // the same dictionary
            if (extends)
            {
                first = last;
                delta = true;
            }
            else if (fileFormat)
            {
                throw new InvalidOperationException($"The dictionary of column '{field.Field.Name}' changes, and the IPC file format allows no replacement (only "
                    + "dictionaries that extend the previous one, as deltas): give every batch the same dictionary, or one that extends it, or write the stream format.");
            }
        }

        var body = new IpcEncodedBody();
        try
        {
            IpcBatchEncoder.Encode(body, [values], [dictionary], first, length - first);
            foreach (var (inner, innerField, innerDictionary) in body.Dictionaries) Dictionary(inner, innerField, innerDictionary, messages);
            Remember(id, values, dictionary, first);
            messages.Add(new IpcOutgoingMessage(body, length - first, compression, true,
                (header, bodyLength) => IpcMetadataWriter.DictionaryBatchMessage(new IpcDictionaryBatch(id, header, delta), bodyLength)));
        }
        catch
        {
            body.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Dictionary <paramref name="id"/> as last written, in segments (values and first slot), or null before it is.
    /// One inside another dictionary's values is kept whole in <see cref="_written"/>, where decoding that other one
    /// finds it.
    /// </summary>
    private List<Segment>? Kept(long id, IpcField field)
    {
        if (!_nested.Contains(id)) return _kept.GetValueOrDefault(id);
        return _written.Contains(id) ? [new Segment(_written.Get(id, field, false, field.Field.Name), 0, null)] : null;
    }

    /// <summary>Values of a dictionary as written (from slot <see cref="Start"/> on), with <see cref="Spans"/> for views.</summary>
    private sealed record Segment(IpcArray Values, int Start, ViewSpans? Spans);

    /// <summary>
    /// Where a view segment's valid views reach in each of its data buffers (<see cref="Ends"/>) and how far their
    /// offsets were moved down from those of the dictionary they were taken from (<see cref="Shifts"/>): measured once,
    /// for <see cref="SameViewLayout"/>.
    /// </summary>
    private sealed record ViewSpans(long[] Ends, long[] Shifts);

    /// <summary>
    /// Keeps dictionary <paramref name="id"/> as written, from <paramref name="from"/> on (a delta; 0: all of it), in
    /// memory of its own (the caller's may go). A delta becomes a segment of its own; then the last segments are made
    /// one, taken again from <paramref name="dictionary"/> (whose first values are theirs), until each segment holds
    /// more than twice the values of all after it: O(log n) segments to compare, each value copied O(log n) times
    /// (rather than the whole dictionary on every change). One nested in another dictionary's values is kept whole.
    /// </summary>
    private void Remember(long id, IpcField values, ArrayData dictionary, int from)
    {
        int length = dictionary.Length;
        if (_nested.Contains(id))
        {
            IpcArray whole = Snapshot(values, dictionary, 0, length, out IpcBodyMemory memory);
            _written.Set(id, whole);
            memory.Sealed(); // the kept dictionary holds it now
            return;
        }
        List<Segment> kept = _kept.GetValueOrDefault(id) ?? [];
        int k = from == 0 ? 0 : kept.Count;
        long after = length - from;
        while (k > 0 && kept[k - 1].Values.Length <= 2 * after)
        {
            after += kept[k - 1].Values.Length;
            k--;
        }
        int start = k == kept.Count ? from : kept[k].Start;
        IpcArray segment = Snapshot(values, dictionary, start, length - start, out IpcBodyMemory segmentMemory);
        segment.Hold();
        segmentMemory.Sealed(); // the segment holds it now
        ViewSpans? spans = values.Field.DataType is BinaryViewType or StringViewType ? Spans(segment, dictionary, start) : null;
        for (int i = k; i < kept.Count; i++) kept[i].Values.Unhold();
        kept.RemoveRange(k, kept.Count - k);
        kept.Add(new Segment(segment, start, spans));
        _kept[id] = kept;
    }

    /// <summary>
    /// Values [<paramref name="start"/>, + <paramref name="count"/>) of <paramref name="dictionary"/>: encoded, then
    /// decoded into <paramref name="memory"/> (sealed by the caller once the values hold it).
    /// </summary>
    private IpcArray Snapshot(IpcField values, ArrayData dictionary, int start, int count, out IpcBodyMemory memory)
    {
        using var body = new IpcEncodedBody();
        IpcBatchEncoder.Encode(body, [values], [dictionary], start, count);
        (IpcBuffer[] layout, long length) = IpcBatchEncoder.Layout(body.Buffers);
        memory = new IpcBodyMemory();
        try
        {
            byte* p = memory.Allocate(length);
            for (int i = 0; i < layout.Length; i++)
            {
                if (layout[i].Length > 0) NativeMemory.Copy((void*)body.Buffers[i].Pointer, p + layout[i].Offset, (nuint)layout[i].Length);
            }
            var header = new IpcRecordBatch(count, [.. body.Nodes], layout, IpcCompression.None, [.. body.VariadicCounts]);
            var copy = new IpcBody(memory, length, [new IpcBodySegment(0, (nint)p, length)]);
            return IpcBatchDecoder.Decode([values], null, header, IpcMetadataVersion.V5, copy, _written, false)[0]!;
        }
        catch
        {
            memory.Sealed();
            throw;
        }
    }

    /// <summary>Whether the first values of <paramref name="dictionary"/> are those kept (segment by segment).</summary>
    private static bool SamePrefix(IpcField values, List<Segment> kept, ArrayData dictionary)
    {
        foreach (Segment segment in kept)
        {
            if (!SameValues(values, segment, dictionary)) return false;
        }
        return true;
    }

    /// <summary>Whether the values of <paramref name="dictionary"/> from the segment's start on are the segment's.</summary>
    private static bool SameValues(IpcField values, Segment segment, ArrayData dictionary)
    {
        var (last, start, spans) = segment;
        // Views point anywhere in their data buffers: compare the values themselves.
        if (values.Field.DataType is BinaryViewType or StringViewType) return SameViews(last, spans, dictionary, start);
        using IArrowArray old = ArrowArrayFactory.BuildArray(last.ToArrayData());
        using var x = new IpcEncodedBody();
        using var y = new IpcEncodedBody();
        IpcBatchEncoder.Encode(x, [values], [old.Data], 0, last.Length);
        IpcBatchEncoder.Encode(y, [values], [dictionary], start, last.Length);
        if (!x.Nodes.SequenceEqual(y.Nodes) || x.Buffers.Count != y.Buffers.Count || !x.VariadicCounts.SequenceEqual(y.VariadicCounts)) return false;
        for (int i = 0; i < x.Buffers.Count; i++)
        {
            if (!SameBytes(x.Buffers[i], y.Buffers[i])) return false;
        }
        return true;

        static bool SameBytes(IpcPiece a, IpcPiece b)
        {
            if (a.Length != b.Length) return false;
            if (a.Length == 0) return true;
            ReadOnlySpan<byte> sa = a.Span, sb = b.Span;
            if (a.Bits < 0 || a.Bits % 8 == 0) return sa.SequenceEqual(sb);
            int mask = (1 << (int)(a.Bits % 8)) - 1; // the bits past a bitmap's length do not count
            return sa[..^1].SequenceEqual(sb[..^1]) && (sa[^1] & mask) == (sb[^1] & mask);
        }
    }

    /// <summary>
    /// Whether the values of view array <paramref name="dictionary"/> from <paramref name="start"/> on are those of
    /// <paramref name="last"/> (nulls at the same slots): read from the views and data buffers in place (measured:
    /// through Apache's arrays, 7× slower, most of the time of writing a growing string-view dictionary); first as a
    /// whole when laid out as when kept (<see cref="SameViewLayout"/>), else value by value.
    /// </summary>
    private static bool SameViews(IpcArray last, ViewSpans? spans, ArrayData dictionary, int start)
    {
        int at = dictionary.Offset + start;
        if (dictionary.Buffers[1].Length < (at + (long)last.Length) * 16) return false;
        ReadOnlySpan<byte> aViews = last.Buffers[1].Span, aValid = last.Buffers[0].Span;
        ReadOnlySpan<byte> bViews = dictionary.Buffers[1].Span[(at * 16)..], bValid = dictionary.Buffers[0].Span;
        if (SameViewLayout(last, spans ?? Spans(last, null, 0), dictionary, at)) return true;
        for (int i = 0; i < last.Length; i++)
        {
            bool aValue = aValid.IsEmpty || Kernels.Bits.Get(aValid, i), bValue = bValid.IsEmpty || Kernels.Bits.Get(bValid, at + i);
            if (aValue != bValue) return false;
            if (!aValue) continue;
            ReadOnlySpan<byte> a = aViews.Slice(16 * i, 16), b = bViews.Slice(16 * i, 16);
            int size = BinaryPrimitives.ReadInt32LittleEndian(a);
            if (size != BinaryPrimitives.ReadInt32LittleEndian(b)) return false;
            if (size <= 12)
            {
                if (!a.Slice(4, size).SequenceEqual(b.Slice(4, size))) return false;
                continue;
            }
            if (BinaryPrimitives.ReadInt32LittleEndian(a[4..]) != BinaryPrimitives.ReadInt32LittleEndian(b[4..])) return false; // the prefixes
            ReadOnlySpan<byte> aData = last.Buffers[2 + BinaryPrimitives.ReadInt32LittleEndian(a[8..])].Span.Slice(BinaryPrimitives.ReadInt32LittleEndian(a[12..]), size);
            int buffer = BinaryPrimitives.ReadInt32LittleEndian(b[8..]), offset = BinaryPrimitives.ReadInt32LittleEndian(b[12..]);
            if ((uint)buffer >= (uint)(dictionary.Buffers.Length - 2) || offset < 0 || offset > dictionary.Buffers[2 + buffer].Length - size)
                throw new ArgumentException("A dictionary's view refers to bytes outside its data buffers.");
            if (!aData.SequenceEqual(dictionary.Buffers[2 + buffer].Span.Slice(offset, size))) return false;
        }
        return true;
    }

    /// <summary>
    /// The spans of view array <paramref name="kept"/>: how far its valid views reach in each data buffer and, when it
    /// was taken from <paramref name="source"/> at <paramref name="start"/>, how far each buffer's offsets moved down
    /// (all a buffer's views move alike: the writer moves a slice's views by its first byte used); else 0.
    /// </summary>
    private static ViewSpans Spans(IpcArray kept, ArrayData? source, int start)
    {
        int buffers = kept.Buffers.Length - 2;
        long[] ends = new long[buffers], shifts = new long[buffers];
        bool[] measured = new bool[buffers];
        ReadOnlySpan<byte> views = kept.Buffers[1].Span, valid = kept.Buffers[0].Span;
        ReadOnlySpan<byte> theirs = source is null ? default : source.Buffers[1].Span[((source.Offset + start) * 16)..];
        for (int i = 0; i < kept.Length; i++)
        {
            if (!valid.IsEmpty && !Kernels.Bits.Get(valid, i)) continue;
            int size = BinaryPrimitives.ReadInt32LittleEndian(views[(16 * i)..]);
            if (size <= 12) continue;
            int b = BinaryPrimitives.ReadInt32LittleEndian(views[(16 * i + 8)..]), offset = BinaryPrimitives.ReadInt32LittleEndian(views[(16 * i + 12)..]);
            ends[b] = Math.Max(ends[b], (long)offset + size);
            if (source is null || measured[b]) continue;
            shifts[b] = BinaryPrimitives.ReadInt32LittleEndian(theirs[(16 * i + 12)..]) - (long)offset;
            measured[b] = true;
        }
        return new ViewSpans(ends, shifts);
    }

    /// <summary>
    /// Whether view array <paramref name="dictionary"/> has, from physical slot <paramref name="at"/> on, the views,
    /// validity and data bytes <paramref name="last"/> was kept from: the same views but each offset by its buffer's
    /// shift, the bytes our valid views use found there. Then the same values, whatever the views point at (a
    /// dictionary made again the same way, or the same one), told without looking at each value's bytes.
    /// </summary>
    private static bool SameViewLayout(IpcArray last, ViewSpans spans, ArrayData dictionary, int at)
    {
        int n = last.Length, buffers = last.Buffers.Length - 2;
        if (buffers > dictionary.Buffers.Length - 2) return false;
        ReadOnlySpan<byte> aValid = last.Buffers[0].Span, bValid = dictionary.Buffers[0].Span;
        // No bitmap: every value valid (ours drop an all-valid bitmap; builders often keep one).
        if (aValid.IsEmpty != bValid.IsEmpty)
        {
            if (aValid.IsEmpty ? Kernels.Bits.Count(bValid, at, n) != n : Kernels.Bits.Count(aValid, 0, n) != n) return false;
        }
        else if (!aValid.IsEmpty)
        {
            if (at % 8 != 0 || bValid.Length < (at + n + 7) / 8) return false;
            ReadOnlySpan<byte> b = bValid[(at / 8)..];
            if (!aValid[..(n / 8)].SequenceEqual(b[..(n / 8)])) return false;
            int mask = (1 << (n % 8)) - 1;
            if (n % 8 != 0 && (aValid[n / 8] & mask) != (b[n / 8] & mask)) return false;
        }
        ReadOnlySpan<byte> aViews = last.Buffers[1].Span[..(16 * n)], bViews = dictionary.Buffers[1].Span.Slice(16 * at, 16 * n);
        if (spans.Shifts.AsSpan().ContainsAnyExcept(0))
        {
            // Views moved down: the same but for offsets that differ by their buffer's shift (null views were emptied).
            for (int i = 0; i < n; i++)
            {
                if (!aValid.IsEmpty && !Kernels.Bits.Get(aValid, i)) continue;
                ReadOnlySpan<byte> a = aViews.Slice(16 * i, 16), b = bViews.Slice(16 * i, 16);
                if (BinaryPrimitives.ReadInt64LittleEndian(a) != BinaryPrimitives.ReadInt64LittleEndian(b)) return false; // length and prefix
                if (BinaryPrimitives.ReadInt32LittleEndian(a) <= 12)
                {
                    if (BinaryPrimitives.ReadInt64LittleEndian(a[8..]) != BinaryPrimitives.ReadInt64LittleEndian(b[8..])) return false;
                    continue;
                }
                int buffer = BinaryPrimitives.ReadInt32LittleEndian(a[8..]);
                if (buffer != BinaryPrimitives.ReadInt32LittleEndian(b[8..])
                    || BinaryPrimitives.ReadInt32LittleEndian(b[12..]) != BinaryPrimitives.ReadInt32LittleEndian(a[12..]) + spans.Shifts[buffer]) return false;
            }
        }
        else if (!aViews.SequenceEqual(bViews)) return false;
        for (int k = 0; k < buffers; k++)
        {
            long shift = spans.Shifts[k], end = spans.Ends[k];
            ReadOnlySpan<byte> ours = last.Buffers[2 + k].Span, theirs = dictionary.Buffers[2 + k].Span;
            if (shift < 0 || theirs.Length < shift + end || !ours[..(int)end].SequenceEqual(theirs.Slice((int)shift, (int)end))) return false;
        }
        return true;
    }

    /// <summary>
    /// The continuation marker, the metadata length and the metadata, padded with zeros so that the metadata is a
    /// multiple of 8 bytes and the body starts at a multiple of 64 from where the writing began.
    /// </summary>
    public static byte[] Frame(byte[] flatBuffer, long position)
    {
        long bodyStart = position + 8 + flatBuffer.Length;
        long padded = flatBuffer.Length + (IpcBatchEncoder.Alignment - bodyStart % IpcBatchEncoder.Alignment) % IpcBatchEncoder.Alignment;
        byte[] framed = new byte[8 + padded];
        BinaryPrimitives.WriteUInt32LittleEndian(framed, IpcFraming.Continuation);
        BinaryPrimitives.WriteInt32LittleEndian(framed.AsSpan(4), (int)padded);
        flatBuffer.CopyTo(framed, 8);
        return framed;
    }

    /// <summary>The ids of the dictionaries used inside dictionaries' values (decoding those finds them in <see cref="_written"/>).</summary>
    private static HashSet<long> NestedDictionaries(Schema schema)
    {
        HashSet<long> nested = [];
        foreach (IpcField field in IpcMetadataWriter.Schema(schema).Fields) Visit(field, false);
        return nested;

        void Visit(IpcField field, bool inside)
        {
            if (field.DictionaryId is long id)
            {
                if (inside) nested.Add(id);
                foreach (IpcField child in field.ValueField().Children) Visit(child, true);
                return;
            }
            foreach (IpcField child in field.Children) Visit(child, inside);
        }
    }

    public void Dispose()
    {
        _written.Dispose();
        foreach (List<Segment> kept in _kept.Values)
        {
            foreach (Segment segment in kept) segment.Values.Unhold();
        }
        _kept.Clear();
    }
}
