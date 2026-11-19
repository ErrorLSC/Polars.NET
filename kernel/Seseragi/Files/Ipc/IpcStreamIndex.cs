using System.Buffers.Binary;
using Kaminari.Sazanami.Arrow.Ipc;
using System.Buffers;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// The messages of an Arrow IPC stream held by a random-access source, found by reading their prefixes and metadata
/// only (bodies passed over by their length): what a file's footer says (the schema, the dictionary batches and record
/// batches as blocks), plus each record batch's rows and how many dictionary batches come before it (its dictionaries
/// are those, in order: streams may replace a dictionary or extend it between record batches).
/// </summary>
/// <param name="Footer">The schema and the blocks, as a file's footer gives them (no file metadata: a stream has none).</param>
/// <param name="Rows">Each record batch's rows.</param>
/// <param name="Epochs">Each record batch's dictionary batches before it (a prefix of <see cref="IpcFooter.Dictionaries"/>).</param>
/// <param name="End">Where the stream's messages end (its end-of-stream marker, or the end of the data).</param>
/// <param name="SchemaMetadata">The schema message's metadata (its FlatBuffer), for keeping the scan as bytes.</param>
internal sealed record IpcStreamIndex(IpcFooter Footer, long[] Rows, int[] Epochs, long End, byte[] SchemaMetadata)
{
    /// <summary>The start of a scan kept as bytes (<see cref="Write"/>): never the start of a FlatBuffer footer (its root offset).</summary>
    private static ReadOnlySpan<byte> Marker => [0xFF, 0xFF, (byte)'S', (byte)'C', (byte)'A', (byte)'N'];

    /// <summary>Whether <paramref name="kept"/> holds a scan (<see cref="Write"/>) rather than a file's footer.</summary>
    public static bool IsKept(ReadOnlySpan<byte> kept) => kept.StartsWith(Marker);

    /// <summary>
    /// The scan as bytes (a dataset keeps them as it keeps footers): the schema message's metadata, then the blocks of
    /// the dictionary batches, those of the record batches with their rows and epochs, and the end.
    /// </summary>
    public byte[] Write()
    {
        IpcBlock[] dictionaries = Footer.Dictionaries, batches = Footer.RecordBatches;
        var bytes = new byte[Marker.Length + 4 + SchemaMetadata.Length + 4 + 4 + 8 + dictionaries.Length * 20 + batches.Length * 32];
        Span<byte> at = bytes;
        Marker.CopyTo(at);
        at = at[Marker.Length..];
        BinaryPrimitives.WriteInt32LittleEndian(at, SchemaMetadata.Length);
        SchemaMetadata.CopyTo(at[4..]);
        at = at[(4 + SchemaMetadata.Length)..];
        BinaryPrimitives.WriteInt32LittleEndian(at, dictionaries.Length);
        BinaryPrimitives.WriteInt32LittleEndian(at[4..], batches.Length);
        BinaryPrimitives.WriteInt64LittleEndian(at[8..], End);
        at = at[16..];
        foreach (IpcBlock block in dictionaries) at = Block(at, block);
        for (int i = 0; i < batches.Length; i++)
        {
            at = Block(at, batches[i]);
            BinaryPrimitives.WriteInt64LittleEndian(at, Rows[i]);
            BinaryPrimitives.WriteInt32LittleEndian(at[8..], Epochs[i]);
            at = at[12..];
        }
        return bytes;

        static Span<byte> Block(Span<byte> at, IpcBlock block)
        {
            BinaryPrimitives.WriteInt64LittleEndian(at, block.Offset);
            BinaryPrimitives.WriteInt32LittleEndian(at[8..], block.MetadataLength);
            BinaryPrimitives.WriteInt64LittleEndian(at[12..], block.BodyLength);
            return at[20..];
        }
    }

    /// <summary>A scan kept as bytes (<see cref="Write"/>; the schema parsed again).</summary>
    /// <exception cref="IpcFormatException">The bytes are not a scan (they were damaged).</exception>
    public static IpcStreamIndex Read(ReadOnlySpan<byte> kept)
    {
        try
        {
            ReadOnlySpan<byte> at = kept[Marker.Length..];
            int schemaLength = BinaryPrimitives.ReadInt32LittleEndian(at);
            byte[] schemaMetadata = at.Slice(4, schemaLength).ToArray();
            at = at[(4 + schemaLength)..];
            int dictionaryCount = BinaryPrimitives.ReadInt32LittleEndian(at), batchCount = BinaryPrimitives.ReadInt32LittleEndian(at[4..]);
            long end = BinaryPrimitives.ReadInt64LittleEndian(at[8..]);
            at = at[16..];
            var dictionaries = new IpcBlock[dictionaryCount];
            for (int i = 0; i < dictionaries.Length; i++) dictionaries[i] = Block(ref at);
            var batches = new IpcBlock[batchCount];
            var rows = new long[batchCount];
            var epochs = new int[batchCount];
            for (int i = 0; i < batches.Length; i++)
            {
                batches[i] = Block(ref at);
                rows[i] = BinaryPrimitives.ReadInt64LittleEndian(at);
                epochs[i] = BinaryPrimitives.ReadInt32LittleEndian(at[8..]);
                at = at[12..];
            }
            IpcMessage schema = IpcMetadata.ParseMessage(schemaMetadata);
            if (schema.Kind != IpcMessageKind.Schema) throw new InvalidDataException("not a schema message");
            return new IpcStreamIndex(new IpcFooter(schema.Version, schema.Schema!, dictionaries, batches, null), rows, epochs, end, schemaMetadata);
        }
        catch (Exception e) when (e is InvalidDataException or ArgumentOutOfRangeException)
        {
            throw new IpcFormatException("A kept Arrow IPC stream scan is damaged.", e);
        }

        static IpcBlock Block(ref ReadOnlySpan<byte> at)
        {
            var block = new IpcBlock(BinaryPrimitives.ReadInt64LittleEndian(at), BinaryPrimitives.ReadInt32LittleEndian(at[8..]), BinaryPrimitives.ReadInt64LittleEndian(at[12..]));
            at = at[20..];
            return block;
        }
    }

    /// <summary>The bytes read at once while scanning a local source, and on a store (fewer requests).</summary>
    private const int LocalChunk = 64 << 10, RemoteChunk = 1 << 20;

    /// <summary>
    /// Scans a stream from the start of <paramref name="source"/>: in place when it lives in memory, through its mapping
    /// when it is a local file opened by path and <paramref name="map"/> allows it (only the pages of the prefixes and
    /// metadata are touched), else by reading it a stretch at a time (bodies smaller than a stretch read with it).
    /// </summary>
    /// <exception cref="IpcFormatException">The data is not a stream, a message is damaged, or the data ends inside a message.</exception>
    public static async ValueTask<IpcStreamIndex> ScanAsync(IRandomAccess source, bool map, bool useAsync, CancellationToken ct)
    {
        long length = source.Length;
        Window window = Window.Over(source, map);
        try
        {
            IpcSchema? schema = null;
            IpcMetadataVersion version = default;
            byte[] schemaMetadata = [];
            List<IpcBlock> dictionaries = [], batches = [];
            List<long> rows = [];
            List<int> epochs = [];
            long at = 0;
            while (true)
            {
                if (at == length)
                {
                    if (schema is null) throw new IpcFormatException("The Arrow IPC stream is empty: it has no schema.");
                    break; // ended by the end of the data (the format allows a writer to end a stream so)
                }
                if (length - at < 4) throw Ended(at, "a message prefix");
                await window.EnsureAsync(at, (int)Math.Min(8, length - at), useAsync, ct).ConfigureAwait(false);
                ReadOnlySpan<byte> head = window.Bytes(at, (int)Math.Min(8, length - at));
                if (at == 0 && head.Length >= 6 && head[..6].SequenceEqual(IpcFraming.FileMagic))
                    throw new IpcFormatException("The data is an Arrow IPC file, not a stream.");
                if (IpcFraming.HasContinuation(head) && head.Length < 8) throw Ended(at, "a message prefix");
                int metadataLength, prefix;
                try
                {
                    metadataLength = IpcFraming.MetadataLength(head, head.Length >= 8 ? head[4..8] : head, out prefix);
                }
                catch (InvalidDataException e)
                {
                    throw IpcIO.Format(e);
                }
                if (metadataLength == 0)
                {
                    if (schema is null) throw new IpcFormatException("The Arrow IPC stream ends before its schema.");
                    break; // the end-of-stream marker
                }
                if (metadataLength > length - at - prefix) throw Ended(at, "a message's metadata");
                await window.EnsureAsync(at + prefix, metadataLength, useAsync, ct).ConfigureAwait(false);
                IpcMessage message;
                try
                {
                    message = IpcMetadata.ParseMessage(window.Bytes(at + prefix, metadataLength));
                }
                catch (InvalidDataException e)
                {
                    throw IpcIO.Format(e);
                }
                long body = message.BodyLength;
                if (body < 0 || body > length - at - prefix - metadataLength) throw Ended(at, $"a message body ({body} bytes declared)");
                var block = new IpcBlock(at, prefix + metadataLength, body);
                switch (message.Kind)
                {
                    case IpcMessageKind.Schema when schema is null:
                        schema = message.Schema;
                        version = message.Version;
                        schemaMetadata = window.Bytes(at + prefix, metadataLength).ToArray();
                        break;
                    case IpcMessageKind.Schema:
                        throw new IpcFormatException("The Arrow IPC stream has a second schema message.");
                    case var _ when schema is null:
                        throw new IpcFormatException($"The Arrow IPC stream starts with a {message.Kind} message, not its schema.");
                    case IpcMessageKind.DictionaryBatch:
                        dictionaries.Add(block);
                        break;
                    case IpcMessageKind.RecordBatch:
                        batches.Add(block);
                        rows.Add(message.RecordBatch!.Length);
                        epochs.Add(dictionaries.Count);
                        break;
                    default:
                        throw new IpcFormatException($"The Arrow IPC stream has a {message.Kind} message, which is not supported.");
                }
                at += prefix + metadataLength + body;
            }
            return new IpcStreamIndex(new IpcFooter(version, schema!, [.. dictionaries], [.. batches], null), [.. rows], [.. epochs], at, schemaMetadata);
        }
        finally
        {
            window.Dispose();
        }

        static IpcFormatException Ended(long at, string what) => new($"The Arrow IPC stream ends inside {what} (the message at {at}).");
    }

    /// <summary>
    /// The bytes of the source the scan looks at: all of them where they already are (memory, a local file's mapping),
    /// else a stretch read at once (at least a chunk, or what a long metadata needs) into native memory.
    /// </summary>
    private sealed class Window : IDisposable
    {
        private readonly IRandomAccess _source;
        private readonly int _chunk;
        private nint _buffer;
        private int _capacity;
        private long _start = -1;
        private long _length;
        private readonly bool _read;          // a stretch read into _buffer (else _buffer is where the whole source is)
        private readonly MappedFile? _map;    // held while scanning
        private MemoryHandle _pin;            // the source's memory, pinned while scanning

        private Window(IRandomAccess source, int chunk)
        {
            _source = source;
            _chunk = chunk;
            _buffer = IpcIO.Alloc(chunk);
            _capacity = chunk;
            _read = true;
        }

        private Window(IRandomAccess source, nint whole, MappedFile? map, MemoryHandle pin)
        {
            _source = source;
            (_buffer, _start, _length) = (whole, 0, source.Length);
            _map = map;
            _pin = pin;
        }

        /// <summary>The window for <paramref name="source"/>: over all of it when it is in memory or (<paramref name="map"/>) mapped.</summary>
        public static unsafe Window Over(IRandomAccess source, bool map)
        {
            long length = source.Length;
            if (source is ILocalSource local)
            {
                if (local.TryGetMemory(out ReadOnlyMemory<byte> memory))
                {
                    MemoryHandle pin = memory.Pin();
                    return new Window(source, (nint)pin.Pointer, null, pin);
                }
                if (map && local.Map(out long start) is { } file && file.TryAcquire())
                {
                    try
                    {
                        return new Window(source, (nint)file.At(start, length), file, default);
                    }
                    catch (ArgumentOutOfRangeException)
                    {
                        file.Release(); // the file changed length since it was mapped: read it instead
                    }
                }
            }
            return new Window(source, (int)Math.Min(source is ILocalSource ? LocalChunk : RemoteChunk, Math.Max(length, 8)));
        }

        public async ValueTask EnsureAsync(long at, int count, bool useAsync, CancellationToken ct)
        {
            if (_start >= 0 && at >= _start && at + count <= _start + _length) return;
            int n = (int)Math.Min(Math.Max(_chunk, count), _source.Length - at);
            if (n > _capacity)
            {
                IpcIO.Free(_buffer);
                _buffer = IpcIO.Alloc(n);
                _capacity = n;
            }
            _start = -1; // invalid until the read completes
            await IpcIO.ReadAsync(_source, at, _buffer, n, useAsync, ct).ConfigureAwait(false);
            (_start, _length) = (at, n);
        }

        public ReadOnlySpan<byte> Bytes(long at, int count) => IpcIO.Bytes(_buffer + (nint)(at - _start), count);

        public void Dispose()
        {
            if (_read) IpcIO.Free(_buffer);
            _map?.Release();
            _pin.Dispose();
            _buffer = 0;
        }
    }
}
