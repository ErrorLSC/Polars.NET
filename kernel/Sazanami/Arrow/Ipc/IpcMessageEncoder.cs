using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>A message ready to write: its prefix and metadata (padded) and its body (buffers at their offsets).</summary>
internal sealed class IpcOutgoingMessage(byte[] metadata, IpcEncodedBody? body, IpcBuffer[] layout, long bodyLength, bool isDictionary) : IDisposable
{
    /// <summary>The continuation marker, the metadata length and the metadata, padded so that the body starts 64-byte aligned.</summary>
    public byte[] Metadata { get; } = metadata;

    public IpcEncodedBody? Body { get; } = body;

    public IpcBuffer[] Layout { get; } = layout;

    public long BodyLength { get; } = bodyLength;

    public bool IsDictionary { get; } = isDictionary;

    public void Dispose() => Body?.Dispose();
}

/// <summary>
/// The writing side of the IPC formats, short of I/O: turns batches into messages (dictionary batches first, then the
/// record batch), keeping each dictionary as last written to tell what a new one is: the same (nothing written),
/// longer with the same first values (a delta), or other (a replacement; refused in the file format, which allows
/// none). Not thread-safe; compression runs in parallel inside one batch.
/// </summary>
internal sealed unsafe class IpcMessageEncoder(Schema schema, IpcCompression compression, int level, int parallelism, bool fileFormat) : IDisposable
{
    /// <summary>The schema in its IPC form (extension types as storage, dictionary ids).</summary>
    public IpcSchema Schema { get; } = IpcMetadataWriter.Schema(schema);

    private readonly IpcDictionaries _written = new(); // each dictionary as last written, decoded into memory of its own

    /// <summary>The schema message.</summary>
    public IpcOutgoingMessage SchemaMessage(long position, IReadOnlyList<KeyValuePair<string, string>>? metadata) =>
        new(Frame(IpcMetadataWriter.SchemaMessage(Schema, metadata), position), null, [], 0, false);

    /// <summary>
    /// The messages of a batch, written from <paramref name="position"/> on: the dictionary batches it needs, then the
    /// record batch. The caller disposes them once written.
    /// </summary>
    /// <exception cref="ArgumentException">The batch does not match the schema.</exception>
    /// <exception cref="InvalidOperationException">A dictionary is replaced in the file format.</exception>
    public List<IpcOutgoingMessage> Batch(RecordBatch batch, IReadOnlyList<KeyValuePair<string, string>>? metadata, long position)
    {
        if (batch.ColumnCount != Schema.Fields.Length)
            throw new ArgumentException($"The batch has {batch.ColumnCount} columns; the schema has {Schema.Fields.Length}.", nameof(batch));
        List<IpcOutgoingMessage> messages = [];
        var body = new IpcEncodedBody();
        try
        {
            IpcBatchEncoder.Encode(body, Schema.Fields, [.. Enumerable.Range(0, batch.ColumnCount).Select(c => batch.Column(c).Data)], 0, batch.Length);
            foreach (var (id, field, dictionary) in body.Dictionaries) Dictionary(id, field, dictionary, messages, ref position);
            IpcBatchEncoder.Compress(body, compression, level, parallelism);
            (IpcBuffer[] layout, long length) = IpcBatchEncoder.Layout(body.Buffers);
            var header = new IpcRecordBatch(batch.Length, [.. body.Nodes], layout, compression, [.. body.VariadicCounts]);
            messages.Add(new IpcOutgoingMessage(Frame(IpcMetadataWriter.RecordBatchMessage(header, length, metadata), position), body, layout, length, false));
            return messages;
        }
        catch
        {
            body.Dispose();
            foreach (IpcOutgoingMessage m in messages) m.Dispose();
            throw;
        }
    }

    /// <summary>Writes what dictionary <paramref name="id"/> needs (nothing, a delta or the whole dictionary), after the dictionaries its values use.</summary>
    private void Dictionary(long id, IpcField field, ArrayData dictionary, List<IpcOutgoingMessage> messages, ref long position)
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
            foreach (var (inner, innerField, innerDictionary) in body.Dictionaries) Dictionary(inner, innerField, innerDictionary, messages, ref position);
            Remember(id, values, dictionary);
            IpcBatchEncoder.Compress(body, compression, level, parallelism);
            (IpcBuffer[] layout, long bodyLength) = IpcBatchEncoder.Layout(body.Buffers);
            var header = new IpcRecordBatch(length - first, [.. body.Nodes], layout, compression, [.. body.VariadicCounts]);
            byte[] metadata = Frame(IpcMetadataWriter.DictionaryBatchMessage(new IpcDictionaryBatch(id, header, delta), bodyLength), position);
            messages.Add(new IpcOutgoingMessage(metadata, body, layout, bodyLength, true));
            position += metadata.Length + bodyLength;
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
            using var b = (BinaryViewArray)ArrowArrayFactory.BuildArray(dictionary);
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
