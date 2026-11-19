using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.ExceptionServices;
using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// The messages of an Arrow IPC stream held by a random-access source, found as readers need them by reading their
/// prefixes and metadata only (bodies passed over by their length). Opening reads the schema message; the record
/// batches (with their rows, and how many dictionary batches come before each: its dictionaries are those, in order,
/// as streams may replace a dictionary or extend it between record batches) and the dictionary batches are found as
/// far as readers ask, the whole stream when its batches are counted. A damaged message, or data ending inside one,
/// is an error for whoever asks for a batch at or past it; the batches before it read.
/// </summary>
/// <remarks>
/// One scan runs at a time; what it found is published as it goes (each entry before the count that covers it), so
/// readers look at the batches found while the scan goes on.
/// </remarks>
internal sealed class IpcStreamScan : IDisposable
{
    /// <summary>A record batch found: its block, its rows, the rows of the batches before it, and the dictionary batches before it.</summary>
    internal readonly record struct Batch(IpcBlock Block, long Rows, long RowsBefore, int Epoch);

    /// <summary>The start of a scan kept as bytes (<see cref="Write"/>): never the start of a FlatBuffer footer (its root offset).</summary>
    private static ReadOnlySpan<byte> Marker => [0xFF, 0xFF, (byte)'S', (byte)'C', (byte)'A', (byte)'N'];

    /// <summary>The bytes read at once while scanning a local source, and on a store (fewer requests).</summary>
    private const int LocalChunk = 64 << 10, RemoteChunk = 1 << 20;

    /// <summary>The batches scanned beyond the one asked for when a scan is needed.</summary>
    private const int ReadAhead = 15;

    private readonly IRandomAccess? _source;
    private readonly FooterRead? _completed;
    private readonly SemaphoreSlim _scanning = new(1, 1);
    private Window? _window;   // while the scan goes on
    private long _at;          // where the next message starts
    private long _rows;        // the rows of the batches found
    private Batch[] _batches;
    private IpcBlock[] _dictionaries;
    private int _dictionaryCount;
    private int _progress;     // the record batches found, the top bit set once the scan is over: one read gives both
    private ExceptionDispatchInfo? _failure;
    private bool _disposed;

    private IpcStreamScan(IRandomAccess? source, FooterRead? completed, IpcFooter footer, byte[] schemaMetadata, Batch[] batches, IpcBlock[] dictionaries)
    {
        _source = source;
        _completed = completed;
        Footer = footer;
        SchemaMetadata = schemaMetadata;
        _batches = batches;
        _dictionaries = dictionaries;
    }

    /// <summary>The schema (no blocks: they are found by the scan, see <see cref="BatchAt"/> and <see cref="DictionaryAt"/>).</summary>
    public IpcFooter Footer { get; }

    /// <summary>The schema message's metadata (its FlatBuffer), for keeping the scan as bytes.</summary>
    public byte[] SchemaMetadata { get; }

    /// <summary>The record batches found so far.</summary>
    public int Known => Progress.Known;

    /// <summary>Whether the scan is over: the stream's end found (or a damaged message, which is then its end).</summary>
    public bool Complete => Progress.Complete;

    /// <summary>
    /// The record batches found and whether the scan is over, read at once (read apart while the scan goes on, a batch
    /// found in between would make the scan look over without it).
    /// </summary>
    public (int Known, bool Complete) Progress
    {
        get
        {
            int progress = Volatile.Read(ref _progress);
            return (progress & int.MaxValue, progress < 0);
        }
    }

    /// <summary>The damaged message that ended the scan (<see cref="Complete"/>), if one did: the error for batches past those found.</summary>
    public IpcFormatException? Failure => Complete ? _failure?.SourceException as IpcFormatException : null;

    /// <summary>Where the stream's messages end (its end-of-stream marker, or the end of the data), once <see cref="Complete"/>.</summary>
    public long End { get; private set; }

    /// <summary>Record batch <paramref name="index"/>, found (below <see cref="Known"/>).</summary>
    public Batch BatchAt(int index) => Volatile.Read(ref _batches)[index];

    /// <summary>Dictionary batch <paramref name="index"/>, found (one before a batch found: below its <see cref="Batch.Epoch"/>).</summary>
    public IpcBlock DictionaryAt(int index) => Volatile.Read(ref _dictionaries)[index];

    /// <summary>
    /// Whether record batch <paramref name="index"/> exists: the stream scanned as far as it (or to its end), and a few
    /// batches beyond when a scan is needed (<see cref="ReadAhead"/>: one scan per batch read in order costs more).
    /// </summary>
    /// <exception cref="IpcFormatException">The stream is damaged before that batch.</exception>
    public async ValueTask<bool> HasBatchAsync(int index, bool useAsync, CancellationToken ct)
    {
        if (index < Known) return true;
        try
        {
            await ScanAsync(index + 1 + ReadAhead, useAsync, ct).ConfigureAwait(false);
        }
        catch (Exception e) when (index < Known && e is not OperationCanceledException)
        {
            // A read failing beyond the batch asked for: the batch is there (the read is tried again when it is needed).
        }
        if (index < Known) return true;
        _failure?.Throw();
        return false;
    }

    /// <summary>
    /// Scans as far as record batch <paramref name="batches"/> - 1 (or to the end) when the scan has not got there: for
    /// reading ahead. A damaged message stops it quietly (the error is for whoever asks for a batch past it).
    /// </summary>
    public async ValueTask ScanAheadAsync(int batches, bool useAsync, CancellationToken ct)
    {
        if (Reached(batches)) return;
        await ScanAsync(batches, useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>The number of record batches: the whole stream scanned.</summary>
    /// <exception cref="IpcFormatException">The stream is damaged.</exception>
    public async ValueTask<int> CountAsync(bool useAsync, CancellationToken ct)
    {
        await ScanAsync(int.MaxValue, useAsync, ct).ConfigureAwait(false);
        _failure?.Throw();
        return Known;
    }

    /// <summary>The rows of every record batch: the whole stream scanned.</summary>
    /// <exception cref="IpcFormatException">The stream is damaged.</exception>
    public async ValueTask<long> RowCountAsync(bool useAsync, CancellationToken ct)
    {
        int count = await CountAsync(useAsync, ct).ConfigureAwait(false);
        if (count == 0) return 0;
        Batch last = BatchAt(count - 1);
        return last.RowsBefore + last.Rows;
    }

    /// <summary>Whether <paramref name="batches"/> record batches were found, or the scan is over.</summary>
    private bool Reached(int batches) => Progress is var (known, complete) && (known >= batches || complete);

    /// <summary>Scans until <paramref name="batches"/> record batches are found or the scan is over (one scan at a time).</summary>
    private async ValueTask ScanAsync(int batches, bool useAsync, CancellationToken ct)
    {
        if (Reached(batches)) return;
        if (useAsync) await _scanning.WaitAsync(ct).ConfigureAwait(false);
        else _scanning.Wait(ct);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            while (!Reached(batches))
            {
                ct.ThrowIfCancellationRequested(); // between messages: a scan of a mapped file reads nothing that would see the token
                await StepAsync(useAsync, ct).ConfigureAwait(false);
            }
        }
        finally
        {
            _scanning.Release();
        }
    }

    /// <summary>Finds the next message (under the scan's lock): a batch or a dictionary batch found, or the end.</summary>
    private async ValueTask StepAsync(bool useAsync, CancellationToken ct)
    {
        (IpcMessage Message, IpcBlock Block, int Prefix)? next;
        try
        {
            next = await NextAsync(_window!, _at, useAsync, ct).ConfigureAwait(false);
            if (next is { } found)
            {
                switch (found.Message.Kind)
                {
                    case IpcMessageKind.Schema:
                        throw new IpcFormatException("The Arrow IPC stream has a second schema message.");
                    case IpcMessageKind.DictionaryBatch:
                        Publish(ref _dictionaries, ref _dictionaryCount, found.Block);
                        break;
                    case IpcMessageKind.RecordBatch:
                        long rows = found.Message.RecordBatch!.Length;
                        int count = Known;
                        Publish(ref _batches, ref count, new Batch(found.Block, rows, _rows, _dictionaryCount));
                        Volatile.Write(ref _progress, count); // after the batch (Publish wrote its own copy of the count)
                        _rows += rows;
                        break;
                    default:
                        throw new IpcFormatException($"The Arrow IPC stream has a {found.Message.Kind} message, which is not supported.");
                }
                _at += found.Block.MetadataLength + found.Block.BodyLength;
                return;
            }
        }
        catch (IpcFormatException e)
        {
            _failure = ExceptionDispatchInfo.Capture(e);
            Finish();
            return;
        }
        End = _at;
        Finish();
        _completed?.Invoke(Write());
    }

    /// <summary>The scan is over: its window let go (the mapping's hold, or the buffer).</summary>
    private void Finish()
    {
        Volatile.Write(ref _progress, _progress | int.MinValue); // after every batch found
        _window?.Dispose();
        _window = null;
    }

    /// <summary>Appends an entry for readers: written before the count that covers it (a full array copied into a larger one first).</summary>
    private static void Publish<T>(ref T[] entries, ref int count, T entry)
    {
        T[] array = entries;
        if (count == array.Length)
        {
            var larger = new T[Math.Max(16, array.Length * 2)];
            array.CopyTo(larger, 0);
            array = larger;
        }
        array[count] = entry;
        Volatile.Write(ref entries, array);
        Volatile.Write(ref count, count + 1);
    }

    /// <summary>
    /// Opens the stream at the start of <paramref name="source"/>: its schema message read, the rest scanned later. The
    /// scan reads the bytes in place when they are in memory, through the mapping of a local file opened by path when
    /// <paramref name="map"/> allows it (only the pages of the prefixes and metadata touched), else a stretch at a time.
    /// <paramref name="completed"/> is handed the scan as bytes when it reaches the end.
    /// </summary>
    /// <exception cref="IpcFormatException">The data is not a stream: empty, a file, or not starting with a schema message.</exception>
    public static async ValueTask<IpcStreamScan> OpenAsync(IRandomAccess source, bool map, FooterRead? completed, bool useAsync, CancellationToken ct)
    {
        Window window = Window.Over(source, map);
        try
        {
            if (source.Length == 0) throw new IpcFormatException("The Arrow IPC stream is empty: it has no schema.");
            var (message, block, prefix) = await NextAsync(window, 0, useAsync, ct).ConfigureAwait(false)
                ?? throw new IpcFormatException("The Arrow IPC stream ends before its schema.");
            if (message.Kind != IpcMessageKind.Schema)
                throw new IpcFormatException($"The Arrow IPC stream starts with a {message.Kind} message, not its schema.");
            byte[] schemaMetadata = window.Bytes(prefix, block.MetadataLength - prefix).ToArray();
            var scan = new IpcStreamScan(source, completed, new IpcFooter(message.Version, message.Schema!, [], [], null), schemaMetadata, [], [])
            {
                _window = window,
                _at = block.MetadataLength + block.BodyLength,
            };
            return scan;
        }
        catch
        {
            window.Dispose();
            throw;
        }
    }

    /// <summary>
    /// The message at <paramref name="at"/> with its block (prefix and metadata, body) and the length of its prefix; null
    /// at the end of the stream (its end-of-stream marker, or the end of the data).
    /// </summary>
    private static async ValueTask<(IpcMessage Message, IpcBlock Block, int Prefix)?> NextAsync(Window window, long at, bool useAsync, CancellationToken ct)
    {
        long length = window.Length;
        if (at == length) return null; // ended by the end of the data (the format allows a writer to end a stream so)
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
        if (metadataLength == 0) return null; // the end-of-stream marker
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
        return (message, new IpcBlock(at, prefix + metadataLength, body), prefix);

        static IpcFormatException Ended(long at, string what) => new($"The Arrow IPC stream ends inside {what} (the message at {at}).");
    }

    /// <summary>Waits for a scan in progress, then lets go of what the scan holds.</summary>
    public void Dispose()
    {
        _scanning.Wait();
        try
        {
            if (_disposed) return;
            _disposed = true;
            _window?.Dispose();
            _window = null;
        }
        finally
        {
            _scanning.Release();
        }
    }

    // ---- kept as bytes ----------------------------------------------------------------------------------------------

    /// <summary>Whether <paramref name="kept"/> holds a scan (<see cref="Write"/>) rather than a file's footer.</summary>
    public static bool IsKept(ReadOnlySpan<byte> kept) => kept.StartsWith(Marker);

    /// <summary>
    /// The whole scan as bytes (a dataset keeps them as it keeps footers): the schema message's metadata, then the blocks
    /// of the dictionary batches, those of the record batches with their rows and epochs, and the end.
    /// </summary>
    public byte[] Write()
    {
        var (batchCount, complete) = Progress;
        if (!complete || _failure is not null) throw new InvalidOperationException("Only a whole scan is kept.");
        int dictionaryCount = _dictionaryCount;
        var bytes = new byte[Marker.Length + 4 + SchemaMetadata.Length + 4 + 4 + 8 + dictionaryCount * 20 + batchCount * 32];
        Span<byte> at = bytes;
        Marker.CopyTo(at);
        at = at[Marker.Length..];
        BinaryPrimitives.WriteInt32LittleEndian(at, SchemaMetadata.Length);
        SchemaMetadata.CopyTo(at[4..]);
        at = at[(4 + SchemaMetadata.Length)..];
        BinaryPrimitives.WriteInt32LittleEndian(at, dictionaryCount);
        BinaryPrimitives.WriteInt32LittleEndian(at[4..], batchCount);
        BinaryPrimitives.WriteInt64LittleEndian(at[8..], End);
        at = at[16..];
        for (int i = 0; i < dictionaryCount; i++) at = Block(at, _dictionaries[i]);
        for (int i = 0; i < batchCount; i++)
        {
            Batch batch = _batches[i];
            at = Block(at, batch.Block);
            BinaryPrimitives.WriteInt64LittleEndian(at, batch.Rows);
            BinaryPrimitives.WriteInt32LittleEndian(at[8..], batch.Epoch);
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

    /// <summary>A whole scan kept as bytes (<see cref="Write"/>; the schema parsed again): nothing left to scan.</summary>
    /// <exception cref="IpcFormatException">The bytes are not a scan (they were damaged).</exception>
    public static IpcStreamScan Read(ReadOnlySpan<byte> kept)
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
            var batches = new Batch[batchCount];
            long rows = 0;
            for (int i = 0; i < batches.Length; i++)
            {
                IpcBlock block = Block(ref at);
                long count = BinaryPrimitives.ReadInt64LittleEndian(at);
                batches[i] = new Batch(block, count, rows, BinaryPrimitives.ReadInt32LittleEndian(at[8..]));
                rows += count;
                at = at[12..];
            }
            IpcMessage schema = IpcMetadata.ParseMessage(schemaMetadata);
            if (schema.Kind != IpcMessageKind.Schema) throw new InvalidDataException("not a schema message");
            return new IpcStreamScan(null, null, new IpcFooter(schema.Version, schema.Schema!, [], [], null), schemaMetadata, batches, dictionaries)
            {
                _progress = batchCount | int.MinValue,
                _dictionaryCount = dictionaryCount,
                _rows = rows,
                End = end,
            };
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

    // ---- the bytes looked at ----------------------------------------------------------------------------------------

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
            Length = source.Length;
            _chunk = chunk;
            _buffer = IpcIO.Alloc(chunk);
            _capacity = chunk;
            _read = true;
        }

        private Window(IRandomAccess source, nint whole, MappedFile? map, MemoryHandle pin)
        {
            _source = source;
            Length = source.Length;
            (_buffer, _start, _length) = (whole, 0, Length);
            _map = map;
            _pin = pin;
        }

        /// <summary>The source's length when the window was made.</summary>
        public long Length { get; }

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
            int n = (int)Math.Min(Math.Max(_chunk, count), Length - at);
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
