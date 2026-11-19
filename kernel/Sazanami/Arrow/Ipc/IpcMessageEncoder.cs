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
    private byte[]? _flatBuffer;

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
    }

    /// <summary>The continuation marker, the metadata length and the metadata, padded so that the body starts 64-byte aligned (once framed).</summary>
    public byte[] Metadata { get; private set; } = [];

    public IpcEncodedBody? Body { get; }

    public IpcBuffer[] Layout { get; private set; }

    public long BodyLength { get; private set; }

    public bool IsDictionary { get; }

    /// <summary>The bytes of the buffers before compression (what the message holds in memory until written).</summary>
    public long RawBytes => Body?.Buffers.Sum(b => b.Length) ?? 0;

    /// <summary>
    /// Compresses the body (on up to <paramref name="parallelism"/> threads) and lays it out; any thread may run it,
    /// several messages at once. The metadata is then known but for its framing.
    /// </summary>
    public void Compress(int level, int parallelism, int zstdFrameBytes)
    {
        if (Body is null || _flatBuffer is not null) return;
        IpcBatchEncoder.Compress(Body, _compression, level, parallelism, zstdFrameBytes);
        (IpcBuffer[] layout, long length) = IpcBatchEncoder.Layout(Body.Buffers);
        var header = new IpcRecordBatch(_rows, [.. Body.Nodes], layout, _compression, [.. Body.VariadicCounts]);
        _flatBuffer = _describe!(header, length);
        Layout = layout;
        BodyLength = length;
    }

    /// <summary>Frames the metadata for a message written at <paramref name="position"/> (after <see cref="Compress"/>).</summary>
    public void Frame(long position)
    {
        if (Body is null) return;
        Metadata = IpcMessageEncoder.Frame(_flatBuffer ?? throw new InvalidOperationException("The message is not compressed yet."), position);
    }

    /// <summary>The bytes the message takes in the output (once framed).</summary>
    public long Size => Metadata.Length + BodyLength;

    public void Dispose() => Body?.Dispose();
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

    private readonly IpcDictionaries _written = new(); // each dictionary as last written, decoded into memory of its own

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
        if (_written.Contains(id))
        {
            IpcArray last = _written.Get(id, field, false, field.Field.Name);
            bool extends = length >= last.Length && SamePrefix(values, last, dictionary);
            if (extends && length == last.Length) return; // the same dictionary
            if (extends)
            {
                first = last.Length;
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
            Remember(id, values, dictionary);
            messages.Add(new IpcOutgoingMessage(body, length - first, compression, true,
                (header, bodyLength) => IpcMetadataWriter.DictionaryBatchMessage(new IpcDictionaryBatch(id, header, delta), bodyLength)));
        }
        catch
        {
            body.Dispose();
            throw;
        }
    }

    /// <summary>Keeps dictionary <paramref name="id"/> as written: encoded, then decoded into memory of its own (the caller's may go).</summary>
    private void Remember(long id, IpcField values, ArrayData dictionary)
    {
        using var body = new IpcEncodedBody();
        IpcBatchEncoder.Encode(body, [values], [dictionary], 0, dictionary.Length);
        (IpcBuffer[] layout, long length) = IpcBatchEncoder.Layout(body.Buffers);
        var memory = new IpcBodyMemory();
        try
        {
            byte* p = memory.Allocate(length);
            for (int i = 0; i < layout.Length; i++)
            {
                if (layout[i].Length > 0) NativeMemory.Copy((void*)body.Buffers[i].Pointer, p + layout[i].Offset, (nuint)layout[i].Length);
            }
            var header = new IpcRecordBatch(dictionary.Length, [.. body.Nodes], layout, IpcCompression.None, [.. body.VariadicCounts]);
            var copy = new IpcBody(memory, length, [new IpcBodySegment(0, (nint)p, length)]);
            IpcArray array = IpcBatchDecoder.Decode([values], null, header, IpcMetadataVersion.V5, copy, _written, false)[0]!;
            _written.Set(id, array);
        }
        finally
        {
            memory.Sealed(); // the kept dictionary holds it now
        }
    }

    /// <summary>Whether the first values of <paramref name="dictionary"/> are those of <paramref name="last"/>.</summary>
    private bool SamePrefix(IpcField values, IpcArray last, ArrayData dictionary)
    {
        using IArrowArray old = ArrowArrayFactory.BuildArray(last.ToArrayData());
        if (values.Field.DataType is BinaryViewType or StringViewType)
        {
            // Views point anywhere in their data buffers: compare the values themselves.
            var a = (BinaryViewArray)old;
            // Not disposed: it wraps the caller's buffers, which stay the caller's.
            var b = (BinaryViewArray)ArrowArrayFactory.BuildArray(dictionary);
            for (int i = 0; i < a.Length; i++)
            {
                if (a.IsNull(i) != b.IsNull(i) || !a.IsNull(i) && !a.GetBytes(i).SequenceEqual(b.GetBytes(i))) return false;
            }
            return true;
        }
        using var x = new IpcEncodedBody();
        using var y = new IpcEncodedBody();
        IpcBatchEncoder.Encode(x, [values], [old.Data], 0, last.Length);
        IpcBatchEncoder.Encode(y, [values], [dictionary], 0, last.Length);
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

    public void Dispose() => _written.Dispose();
}
