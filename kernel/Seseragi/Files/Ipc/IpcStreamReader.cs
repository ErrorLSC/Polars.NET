using System.Collections.Frozen;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// Reads an Arrow IPC stream (the stream format: <c>.arrows</c>, or what a socket or pipe carries) forward, message
/// by message: the schema first, then dictionary batches (replacements and deltas) and record batches, until the
/// end-of-stream marker or the end of the data. Only the selected columns' buffers are kept: the rest of each body is
/// skipped. Every array is checked before it is returned. Batches own their memory: dispose them. Not thread-safe.
/// </summary>
/// <remarks>
/// With <see cref="IpcReadOptions.MaxDegreeOfParallelism"/> above 1 the stream is read ahead, from the first
/// <see cref="ReadNext"/> on, by a task of the reader's own (messages in order, never past the end-of-stream marker
/// or the message that reaches <see cref="IpcReadOptions.MaxRows"/>), and the batches it reads are decoded in
/// parallel, up to that many ahead of the consumer. A batch is delivered as soon as it is decoded: the reader never
/// waits for later messages. Until the stream ends, its position is therefore past the batches delivered; with
/// <see cref="IpcReadOptions.FilterRows"/> and a row limit, the reader may read past the batch that reaches the limit.
/// Disposing the reader does not wait for a read in progress (cancelled through its token; a stream the reader owns is
/// closed first): a stream that ignores cancellation finishes that read in the background, the batches read ahead and
/// the dictionaries released after it. Read-ahead holds up to that many batches, while their bodies stay within 512 MiB.
/// </remarks>
public sealed class IpcStreamReader : IDisposable, IAsyncDisposable
{
    private const int SkipChunk = 1 << 16;

    private readonly Stream _stream;
    private readonly bool _leaveOpen;
    private readonly IpcReadOptions _options;
    private readonly IpcSchema _schema;
    private readonly IpcDecoding _decoding;
    private readonly IpcDictionaries _dictionaries = new();
    private long _left;
    private bool _ended;
    private bool _disposed;

    // Reading ahead (more than one thread): the batches being decoded in order, the task reading the stream.
    private Channel<Ahead>? _ahead;
    private Task? _producer;
    private CancellationTokenSource? _cancel;
    private long _aheadBytes; // body bytes of the batches read ahead and not yet taken
    private readonly SemaphoreSlim _taken = new(0); // signalled whenever the consumer takes a batch

    private IpcStreamReader(Stream stream, bool leaveOpen, IpcReadOptions options, IpcSchema schema, IReadOnlyList<KeyValuePair<string, string>>? metadata)
    {
        _stream = stream;
        _leaveOpen = leaveOpen;
        _options = options;
        _schema = schema;
        _decoding = new IpcDecoding(schema, options);
        _left = options.MaxRows ?? long.MaxValue;
        SchemaMessageMetadata = ToDictionary(metadata);
    }

    /// <summary>The schema of the batches (the selected columns), with the schema's custom metadata.</summary>
    public Schema Schema => _decoding.Schema;

    /// <inheritdoc cref="IpcReader.FiltersExactly"/>
    public bool FiltersExactly => _decoding.FiltersExactly;

    /// <summary>The custom metadata of the schema message (empty when it has none).</summary>
    public IReadOnlyDictionary<string, string> SchemaMessageMetadata { get; }

    /// <summary>The custom metadata of the message of the batch read last (empty when it had none).</summary>
    public IReadOnlyDictionary<string, string> BatchMetadata { get; private set; } = FrozenDictionary<string, string>.Empty;

    /// <summary>
    /// Opens a stream, reading its schema message. Unless <paramref name="leaveOpen"/>, the stream is disposed with the
    /// reader, or when opening fails.
    /// </summary>
    /// <exception cref="IpcFormatException">The data is not an Arrow IPC stream, or its schema is damaged.</exception>
    /// <exception cref="ArgumentException">A column named in the options is not in the stream.</exception>
    public static IpcStreamReader Open(Stream stream, IpcReadOptions? options = null, bool leaveOpen = false) =>
        IpcIO.Sync(OpenCoreAsync(stream, options, leaveOpen, false, default));

    /// <inheritdoc cref="Open"/>
    public static ValueTask<IpcStreamReader> OpenAsync(Stream stream, IpcReadOptions? options = null, bool leaveOpen = false,
        CancellationToken cancellationToken = default) => OpenCoreAsync(stream, options, leaveOpen, true, cancellationToken);

    private static async ValueTask<IpcStreamReader> OpenCoreAsync(Stream stream, IpcReadOptions? options, bool leaveOpen, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(stream);
        options ??= new IpcReadOptions();
        try
        {
            options.Validate();
            (IpcMessage? message, _) = await ReadMessageAsync(stream, first: true, useAsync, ct).ConfigureAwait(false);
            if (message is null) throw new IpcFormatException("The Arrow IPC stream is empty: it has no schema.");
            if (message.Kind != IpcMessageKind.Schema) throw new IpcFormatException($"The Arrow IPC stream starts with a {message.Kind} message, not its schema.");
            if (message.BodyLength != 0) await SkipAsync(stream, message.BodyLength, useAsync, ct).ConfigureAwait(false);
            return new IpcStreamReader(stream, leaveOpen, options, message.Schema!, message.Metadata);
        }
        catch when (!leaveOpen)
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Reads the next record batch (applying the dictionary batches before it); null at the end of the stream or after <see cref="IpcReadOptions.MaxRows"/>.</summary>
    /// <exception cref="IpcFormatException">A message is damaged or invalid, or the stream ends inside one.</exception>
    public RecordBatch? ReadNext() => IpcIO.Sync(ReadNextCoreAsync(false, default));

    /// <inheritdoc cref="ReadNext"/>
    public ValueTask<RecordBatch?> ReadNextAsync(CancellationToken cancellationToken = default) => ReadNextCoreAsync(true, cancellationToken);

    /// <summary>Reads the remaining batches in order.</summary>
    public IEnumerable<RecordBatch> ReadBatches()
    {
        while (ReadNext() is { } batch) yield return batch;
    }

    /// <inheritdoc cref="ReadBatches"/>
    public async IAsyncEnumerable<RecordBatch> ReadBatchesAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        while (await ReadNextAsync(cancellationToken).ConfigureAwait(false) is { } batch) yield return batch;
    }

    /// <summary>The remaining batches as an Arrow stream that owns them (each disposed when the next is read).</summary>
    public Apache.Arrow.Ipc.IArrowArrayStream ReadArrowStream() => new IpcReader.OwningBatchStream(Schema, ReadBatches());

    private async ValueTask<RecordBatch?> ReadNextCoreAsync(bool useAsync, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_options.MaxDegreeOfParallelism > 1) return await TakeAheadAsync(useAsync, ct).ConfigureAwait(false);
        while (!_ended && _left > 0)
        {
            (IpcMessage? message, _) = await ReadMessageAsync(_stream, first: false, useAsync, ct).ConfigureAwait(false);
            if (message is null)
            {
                _ended = true;
                break;
            }
            if (message.Kind == IpcMessageKind.DictionaryBatch)
            {
                await ApplyDictionaryAsync(message, useAsync, ct).ConfigureAwait(false);
                continue;
            }
            CheckRecordBatch(message);
            IpcBody body = await ReadBodyAsync(message, _decoding.Plan(message), useAsync, ct).ConfigureAwait(false);
            RecordBatch batch = _decoding.Decode(message, body, _dictionaries, _left);
            if (batch.Length == 0 && _decoding.Filters)
            {
                batch.Dispose();
                continue;
            }
            _left -= batch.Length;
            BatchMetadata = ToDictionary(message.Metadata);
            return batch;
        }
        return null;
    }

    /// <summary>A record batch, or the error for a message that is neither it nor a dictionary batch.</summary>
    private static void CheckRecordBatch(IpcMessage message)
    {
        if (message.Kind == IpcMessageKind.RecordBatch) return;
        throw message.Kind == IpcMessageKind.Schema
            ? new IpcFormatException("The Arrow IPC stream has a second schema message.")
            : new IpcFormatException($"The Arrow IPC stream has a {message.Kind} message, which is not supported.");
    }

    /// <summary>Applies a dictionary batch (one no selected column uses is skipped).</summary>
    private async ValueTask ApplyDictionaryAsync(IpcMessage message, bool useAsync, CancellationToken ct)
    {
        IpcDictionaryBatch dictionary = message.DictionaryBatch!;
        if (!_decoding.DictionaryIds.Contains(dictionary.Id))
        {
            await SkipAsync(_stream, message.BodyLength, useAsync, ct).ConfigureAwait(false);
            return;
        }
        IpcField field = _schema.Dictionaries.TryGetValue(dictionary.Id, out IpcField? f) ? f
            : throw new IpcFormatException($"Arrow IPC dictionary {dictionary.Id} belongs to no field.");
        bool[] needed = IpcIO.Plan([field.ValueField()], null, message, message.Version);
        IpcBody body = await ReadBodyAsync(message, needed, useAsync, ct).ConfigureAwait(false);
        IpcIO.Dictionary(_schema, message, body, _dictionaries, message.Version, _options.ValidateUtf8, replacements: true);
    }

    /// <summary>A decoded batch and the custom metadata of its message.</summary>
    private sealed record Delivered(RecordBatch Batch, IReadOnlyDictionary<string, string> Metadata);

    /// <summary>A batch read ahead: its decoding, and the body bytes it counts against <see cref="IpcReadOptions.InFlightBytes"/>.</summary>
    private readonly record struct Ahead(Task<Delivered> Decoding, long Bytes);

    /// <summary>The next batch the reading-ahead task decoded (starting it on the first call); null at the end.</summary>
    private async ValueTask<RecordBatch?> TakeAheadAsync(bool useAsync, CancellationToken ct)
    {
        if (_ahead is null)
        {
            _ahead = Channel.CreateBounded<Ahead>(new BoundedChannelOptions(_options.MaxDegreeOfParallelism)
            {
                SingleReader = true,
                SingleWriter = true,
            });
            _cancel = new CancellationTokenSource();
            CancellationToken token = _cancel.Token;
            _producer = Task.Run(() => ProduceAsync(token));
        }
        ChannelReader<Ahead> reader = _ahead.Reader;
        while (_left > 0)
        {
            if (!reader.TryRead(out Ahead next))
            {
                bool more = useAsync
                    ? await reader.WaitToReadAsync(ct).ConfigureAwait(false)
                    : reader.WaitToReadAsync(ct).AsTask().GetAwaiter().GetResult();
                if (!more) break;
                continue;
            }
            Interlocked.Add(ref _aheadBytes, -next.Bytes);
            _taken.Release();
            Delivered delivered = useAsync ? await next.Decoding.ConfigureAwait(false) : next.Decoding.GetAwaiter().GetResult();
            RecordBatch batch = delivered.Batch;
            if (batch.Length == 0 && _decoding.Filters)
            {
                batch.Dispose();
                continue;
            }
            BatchMetadata = delivered.Metadata;
            if (batch.Length >= _left) _cancel!.Cancel(); // the last batch wanted: stop reading (a filter cannot tell the producer)
            if (batch.Length <= _left)
            {
                _left -= batch.Length;
                return batch;
            }
            int rows = (int)_left;
            _left = 0;
            return new RecordBatch(batch.Schema, [.. batch.Arrays.Select(a => ArrowArrayFactory.Slice(a, 0, rows))], rows);
        }
        _ended = true;
        return null;
    }

    /// <summary>
    /// Reads the stream ahead (messages in order): dictionary batches applied here, record batches handed to the thread
    /// pool to decode against a snapshot of the dictionaries, their tasks queued in order (up to
    /// <see cref="IpcReadOptions.MaxDegreeOfParallelism"/> of them, and while their bodies stay within
    /// <see cref="IpcReadOptions.InFlightBytes"/>: at least one). A failure is queued after the batches before it, for
    /// the consumer to meet in its place.
    /// </summary>
    private async Task ProduceAsync(CancellationToken ct)
    {
        ChannelWriter<Ahead> writer = _ahead!.Writer;
        long left = _decoding.Filters ? long.MaxValue : _left; // the rows the batches read hold (unknown with a filter)
        try
        {
            while (left > 0)
            {
                (IpcMessage? message, _) = await ReadMessageAsync(_stream, first: false, true, ct).ConfigureAwait(false);
                if (message is null) break;
                if (message.Kind == IpcMessageKind.DictionaryBatch)
                {
                    await ApplyDictionaryAsync(message, true, ct).ConfigureAwait(false);
                    continue;
                }
                CheckRecordBatch(message);
                while (Volatile.Read(ref _aheadBytes) > 0 && Volatile.Read(ref _aheadBytes) + message.BodyLength > _options.InFlightBytes)
                    await _taken.WaitAsync(ct).ConfigureAwait(false);
                IpcBody body = await ReadBodyAsync(message, _decoding.Plan(message), true, ct).ConfigureAwait(false);
                Interlocked.Add(ref _aheadBytes, message.BodyLength);
                long rows = left;
                if (!_decoding.Filters) left -= Math.Min(left, message.RecordBatch!.Length);
                IpcDictionaries snapshot = _dictionaries.Snapshot();
                IReadOnlyDictionary<string, string> metadata = ToDictionary(message.Metadata);
                Task<Delivered> task = Task.Run(() =>
                {
                    using (snapshot) return new Delivered(_decoding.Decode(message, body, snapshot, rows), metadata);
                });
                try
                {
                    await writer.WriteAsync(new Ahead(task, message.BodyLength), ct).ConfigureAwait(false);
                }
                catch
                {
                    await Drop(task).ConfigureAwait(false);
                    throw;
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Disposed, or the consumer has its rows.
        }
        catch (Exception e)
        {
            try
            {
                await writer.WriteAsync(new Ahead(Task.FromException<Delivered>(e), 0), ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Disposed while the queue was full: nobody will take the error.
            }
        }
        finally
        {
            writer.TryComplete();
        }
    }

    /// <summary>Waits for a decode nobody will take and frees its batch.</summary>
    private static async Task Drop(Task<Delivered> task)
    {
        await ((Task)task).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        if (task.IsCompletedSuccessfully) task.Result.Batch.Dispose();
    }

    /// <summary>
    /// Stops reading ahead and releases the dictionaries: cancels the producer (closing the stream first when the
    /// reader owns it) without waiting for it, since a stream may ignore cancellation and block in a read until more
    /// data comes; once it ends, the batches it queued are freed and the dictionaries released.
    /// </summary>
    private void StopAhead()
    {
        _cancel!.Cancel();
        if (!_leaveOpen) _stream.Dispose();
        if (_producer!.IsCompleted) Release();
        else _producer.ContinueWith(_ => Release(), CancellationToken.None, TaskContinuationOptions.None, TaskScheduler.Default);

        void Release()
        {
            while (_ahead!.Reader.TryRead(out Ahead ahead)) Drop(ahead.Decoding).GetAwaiter().GetResult();
            _dictionaries.Dispose();
            _cancel.Dispose();
            _taken.Dispose();
        }
    }

    /// <summary>
    /// Reads the next message's prefix and metadata; null at the end-of-stream marker or at the end of the data (the
    /// format lets a writer end a stream by closing it).
    /// </summary>
    private static async ValueTask<(IpcMessage?, int)> ReadMessageAsync(Stream stream, bool first, bool useAsync, CancellationToken ct)
    {
        nint prefix = IpcIO.Alloc(8);
        try
        {
            long got = await ReadAsync(stream, prefix, 4, useAsync, ct).ConfigureAwait(false);
            if (got == 0)
            {
                if (first) throw new IpcFormatException("The Arrow IPC stream is empty: it has no schema.");
                return (null, 0);
            }
            if (got < 4) throw new IpcFormatException(first ? "The data is too short to be an Arrow IPC stream." : "The Arrow IPC stream ends inside a message prefix.");
            if (first && Starts(prefix, IpcFraming.FileMagic[..4]))
                throw new IpcFormatException("The data is an Arrow IPC file, not a stream: open it with IpcFile.");
            if (first && Starts(prefix, "FEA1"u8))
                throw new IpcFormatException("The data is a Feather v1 file, which is not supported: write it again as Feather v2 (an Arrow IPC file).");
            if (Continued(prefix) && await ReadAsync(stream, prefix + 4, 4, useAsync, ct).ConfigureAwait(false) < 4)
                throw new IpcFormatException("The Arrow IPC stream ends inside a message prefix.");
            int length = MetadataLength(prefix);
            if (length == 0) return (null, 0);
            (nint metadata, long got2) = await ReadGrowingAsync(stream, length, useAsync, ct).ConfigureAwait(false);
            try
            {
                if (got2 < length) throw new IpcFormatException("The Arrow IPC stream ends inside a message's metadata.");
                return (Parse(metadata, length), length);
            }
            finally
            {
                IpcIO.Free(metadata);
            }
        }
        finally
        {
            IpcIO.Free(prefix);
        }

        static bool Starts(nint p, ReadOnlySpan<byte> magic) => IpcIO.Bytes(p, 4).SequenceEqual(magic);
        static bool Continued(nint p) => IpcFraming.HasContinuation(IpcIO.Bytes(p, 4));

        static int MetadataLength(nint p)
        {
            try
            {
                return IpcFraming.MetadataLength(IpcIO.Bytes(p, 4), IpcIO.Bytes(p + 4, 4), out _);
            }
            catch (InvalidDataException e)
            {
                throw IpcIO.Format(e);
            }
        }

        static IpcMessage Parse(nint p, int length)
        {
            try
            {
                return IpcMetadata.ParseMessage(IpcIO.Bytes(p, length));
            }
            catch (InvalidDataException e)
            {
                throw IpcIO.Format(e);
            }
        }
    }

    /// <summary>The body of a message: the ranges holding the needed buffers read into native memory, the rest skipped.</summary>
    private async ValueTask<IpcBody> ReadBodyAsync(IpcMessage message, bool[] needed, bool useAsync, CancellationToken ct)
    {
        IpcRecordBatch header = message.RecordBatch ?? message.DictionaryBatch!.Data;
        long length = message.BodyLength;
        if (_stream.CanSeek && length > _stream.Length - _stream.Position)
            throw new IpcFormatException($"The Arrow IPC stream ends inside a message body ({length} bytes declared).");
        List<(long Start, long Length)> ranges = IpcFraming.Ranges(header.Buffers, needed, IpcIO.Gap);
        var memory = new IpcBodyMemory();
        try
        {
            var segments = new IpcBodySegment[ranges.Count];
            long at = 0;
            for (int i = 0; i < ranges.Count; i++)
            {
                (long start, long n) = ranges[i];
                await SkipAsync(_stream, start - at, useAsync, ct).ConfigureAwait(false);
                (nint p, long got) = await ReadGrowingAsync(_stream, n, useAsync, ct).ConfigureAwait(false);
                Adopt(memory, p);
                if (got < n) throw new IpcFormatException("The Arrow IPC stream ends inside a message body.");
                segments[i] = new IpcBodySegment(start, p, n);
                at = start + n;
            }
            await SkipAsync(_stream, length - at, useAsync, ct).ConfigureAwait(false);
            return new IpcBody(memory, length, segments);
        }
        catch
        {
            memory.Sealed();
            throw;
        }

        static unsafe void Adopt(IpcBodyMemory memory, nint p) => memory.Adopt((byte*)p);
    }

    /// <summary>
    /// Reads up to <paramref name="length"/> bytes into a <see cref="NativeMemoryPool"/> block that grows (from 1 MiB,
    /// doubling) as the bytes arrive: a forged length allocates no more than twice what the stream holds. The caller
    /// owns the block; fewer bytes only at the end of the stream.
    /// </summary>
    private static async ValueTask<(nint Block, long Read)> ReadGrowingAsync(Stream stream, long length, bool useAsync, CancellationToken ct)
    {
        long capacity = Math.Min(length, 1 << 20);
        nint block = IpcIO.Alloc(capacity);
        long done = 0;
        try
        {
            while (true)
            {
                done += await ReadAsync(stream, block + (nint)done, capacity - done, useAsync, ct).ConfigureAwait(false);
                if (done < capacity || capacity == length) return (block, done);
                long grown = Math.Min(length, capacity * 2);
                nint larger = IpcIO.Alloc(grown);
                Copy(block, larger, done);
                IpcIO.Free(block);
                (block, capacity) = (larger, grown);
            }
        }
        catch
        {
            IpcIO.Free(block);
            throw;
        }

        static unsafe void Copy(nint from, nint to, long n) => System.Runtime.InteropServices.NativeMemory.Copy((void*)from, (void*)to, (nuint)n);
    }

    /// <summary>Reads up to <paramref name="length"/> bytes into native memory; fewer only at the end of the stream.</summary>
    private static async ValueTask<long> ReadAsync(Stream stream, nint destination, long length, bool useAsync, CancellationToken ct)
    {
        long done = 0;
        while (done < length)
        {
            int want = (int)Math.Min(1 << 30, length - done);
            int n = useAsync
                ? await stream.ReadAsync(Memory(destination + (nint)done, want), ct).ConfigureAwait(false)
                : Read(stream, destination + (nint)done, want);
            if (n == 0) break;
            done += n;
        }
        return done;

        static unsafe Memory<byte> Memory(nint p, int n) => new PointerMemory((byte*)p, n).Memory;
        static unsafe int Read(Stream stream, nint p, int n) => stream.Read(new Span<byte>((void*)p, n));
    }

    private static async ValueTask SkipAsync(Stream stream, long count, bool useAsync, CancellationToken ct)
    {
        if (count <= 0) return;
        if (stream.CanSeek)
        {
            if (count > stream.Length - stream.Position) throw new IpcFormatException("The Arrow IPC stream ends inside a message body.");
            stream.Seek(count, SeekOrigin.Current);
            return;
        }
        nint scratch = IpcIO.Alloc(SkipChunk);
        try
        {
            while (count > 0)
            {
                long n = await ReadAsync(stream, scratch, Math.Min(count, SkipChunk), useAsync, ct).ConfigureAwait(false);
                if (n == 0) throw new IpcFormatException("The Arrow IPC stream ends inside a message body.");
                count -= n;
            }
        }
        finally
        {
            IpcIO.Free(scratch);
        }
    }

    private static IReadOnlyDictionary<string, string> ToDictionary(IReadOnlyList<KeyValuePair<string, string>>? pairs)
    {
        Dictionary<string, string> map = [];
        if (pairs is not null)
        {
            foreach (var (key, value) in pairs) map[key] = value;
        }
        return map;
    }

    /// <summary>Releases the dictionaries and, unless it was left open, the stream (batches already read keep what they use).</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        if (_ahead is not null) StopAhead();
        else _dictionaries.Dispose();
        if (!_leaveOpen) _stream.Dispose();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        if (_ahead is not null) StopAhead();
        else _dictionaries.Dispose();
        if (!_leaveOpen) await _stream.DisposeAsync().ConfigureAwait(false);
    }
}
