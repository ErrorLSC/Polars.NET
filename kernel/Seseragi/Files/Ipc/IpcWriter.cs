using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// Writes Arrow record batches as an IPC file (<c>.arrow</c>, Feather v2) or stream (<c>.arrows</c>): metadata V5,
/// little-endian, buffers 64-byte aligned, optional LZ4 frame or Zstandard compression of each buffer. Arrays are
/// written from where they are: sliced arrays as their own rows. Dictionary-encoded columns write their dictionary
/// once, then only what changes: a delta when a batch's dictionary extends the last one, else (the stream format only)
/// a replacement. With compression, each batch is compressed before <see cref="Write(RecordBatch)"/> returns, then
/// written to the stream while the next one is compressed (a cancellation token then covers the compression, not that
/// write): a failure of the stream then surfaces from the next call (<see cref="Write(RecordBatch)"/>,
/// <see cref="Complete"/>). Not thread-safe.
/// </summary>
public sealed class IpcWriter : IDisposable, IAsyncDisposable
{
    private static ReadOnlySpan<byte> EndOfStream => [0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0];

    private readonly Stream _stream;
    private readonly IpcWriteOptions _options;
    private readonly IpcMessageEncoder _encoder;
    private readonly List<IpcBlock> _dictionaryBlocks = [];
    private readonly List<IpcBlock> _batchBlocks = [];
    private readonly byte[] _prefix = new byte[8]; // a compressed buffer's length, written before it
    private long _position;  // bytes written to the stream
    private long _planned;   // where the messages handed over so far end (ahead of _position while one is written behind)
    private Task? _pending;  // the messages of the last batch being written behind (with compression)

    // BatchOwnership.Writer: batches compressed side by side, written in order by a chain of tasks.
    private readonly Queue<(Task Written, long Bytes)> _ahead = [];
    private long _aheadBytes;
    private Task _chain = Task.CompletedTask; // the last batch's write (it waits for every earlier one)
    private volatile bool _stopping;           // aborted or disposed: batches still on their way are not written
    private bool _started, _completed, _aborted, _faulted, _disposed;

    private IpcWriter(Stream stream, Schema schema, IpcWriteOptions options)
    {
        options.Validate();
        Schema = schema;
        _stream = stream;
        _options = options;
        _encoder = new IpcMessageEncoder(schema, options.Compression.IpcCodec, options.Compression.IpcLevel, options.MaxDegreeOfParallelism,
            options.Format == IpcFormat.File, options.ZstdFrameBytes);
    }

    /// <summary>The bytes written to the stream so far (with compression, the last batch may still be on its way).</summary>
    public long BytesWritten => Interlocked.Read(ref _position);

    /// <summary>The schema the batches are written with.</summary>
    public Schema Schema { get; }

    /// <summary>Whether batches can still be written.</summary>
    internal bool IsWritable => !_completed && !_aborted && !_faulted && !_disposed;

    /// <summary>Creates a writer over a stream (nothing is written before the first batch, or <see cref="Complete"/>).</summary>
    /// <exception cref="NotSupportedException">The schema has a type Arrow IPC cannot hold.</exception>
    public static IpcWriter Create(Stream stream, Schema schema, IpcWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(schema);
        if (!stream.CanWrite) throw new ArgumentException("The stream is not writable.", nameof(stream));
        return new(stream, schema, options ?? new());
    }

    /// <summary>Creates (or overwrites) a file.</summary>
    /// <exception cref="NotSupportedException">The schema has a type Arrow IPC cannot hold.</exception>
    public static IpcWriter Create(string path, Schema schema, IpcWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 1, FileOptions.Asynchronous);
        try
        {
            return new(stream, schema, (options ?? new()) with { LeaveOpen = false });
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    /// <summary>Writes all batches to a stream and completes it. If writing fails, the stream holds what was written so far.</summary>
    public static void Write(Stream stream, Schema schema, IEnumerable<RecordBatch> batches, IpcWriteOptions? options = null)
    {
        using IpcWriter writer = Create(stream, schema, options);
        WriteAll(writer, batches);
    }

    /// <summary>Writes all batches to a file and completes it.</summary>
    public static void Write(string path, Schema schema, IEnumerable<RecordBatch> batches, IpcWriteOptions? options = null)
    {
        using IpcWriter writer = Create(path, schema, options);
        WriteAll(writer, batches);
    }

    /// <summary>
    /// Writes rows of <typeparamref name="T"/> to a file and completes it (<see cref="IpcWriter{T}"/>). If writing
    /// fails, the partial file is deleted.
    /// </summary>
    public static void WriteRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path, IEnumerable<T> rows,
        IpcWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var writer = IpcWriter<T>.Create(path, options);
        try
        {
            writer.WriteRows(rows);
            writer.Complete();
        }
        catch
        {
            writer.Abort();
            writer.Dispose();
            File.Delete(path);
            throw;
        }
        writer.Dispose();
    }

    /// <summary>
    /// Writes rows of <typeparamref name="T"/> to a stream and completes the output (<see cref="IpcWriter{T}"/>). If
    /// writing fails, the stream holds what was written so far.
    /// </summary>
    public static void WriteRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Stream stream, IEnumerable<T> rows,
        IpcWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(rows);
        using var writer = IpcWriter<T>.Create(stream, options);
        try
        {
            writer.WriteRows(rows);
            writer.Complete();
        }
        catch
        {
            writer.Abort();
            throw;
        }
    }

    /// <inheritdoc cref="WriteRows{T}(string, IEnumerable{T}, IpcWriteOptions?)"/>
    public static async Task WriteRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path, IAsyncEnumerable<T> rows,
        IpcWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var writer = IpcWriter<T>.Create(path, options);
        try
        {
            await writer.WriteRowsAsync(rows, cancellationToken).ConfigureAwait(false);
            await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            writer.Abort();
            await writer.DisposeAsync().ConfigureAwait(false);
            File.Delete(path);
            throw;
        }
        await writer.DisposeAsync().ConfigureAwait(false);
    }

    /// <inheritdoc cref="WriteRows{T}(Stream, IEnumerable{T}, IpcWriteOptions?)"/>
    public static async Task WriteRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Stream stream, IAsyncEnumerable<T> rows,
        IpcWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        await using var writer = IpcWriter<T>.Create(stream, options);
        try
        {
            await writer.WriteRowsAsync(rows, cancellationToken).ConfigureAwait(false);
            await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            writer.Abort();
            throw;
        }
    }

    private static void WriteAll(IpcWriter writer, IEnumerable<RecordBatch> batches)
    {
        ArgumentNullException.ThrowIfNull(batches);
        try
        {
            foreach (RecordBatch batch in batches) writer.Write(batch);
            writer.Complete();
        }
        catch
        {
            writer.Abort();
            throw;
        }
    }

    /// <inheritdoc cref="Write(Stream, Schema, IEnumerable{RecordBatch}, IpcWriteOptions?)"/>
    public static async Task WriteAsync(Stream stream, Schema schema, IAsyncEnumerable<RecordBatch> batches, IpcWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using IpcWriter writer = Create(stream, schema, options);
        await WriteAllAsync(writer, batches, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="Write(string, Schema, IEnumerable{RecordBatch}, IpcWriteOptions?)"/>
    public static async Task WriteAsync(string path, Schema schema, IAsyncEnumerable<RecordBatch> batches, IpcWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using IpcWriter writer = Create(path, schema, options);
        await WriteAllAsync(writer, batches, cancellationToken).ConfigureAwait(false);
    }

    private static async Task WriteAllAsync(IpcWriter writer, IAsyncEnumerable<RecordBatch> batches, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batches);
        try
        {
            await foreach (RecordBatch batch in batches.WithCancellation(ct).ConfigureAwait(false)) await writer.WriteAsync(batch, ct).ConfigureAwait(false);
            await writer.CompleteAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            writer.Abort();
            throw;
        }
    }

    /// <summary>
    /// Writes a batch (and the dictionary batches it needs). By default the batch is not kept: it may be disposed once
    /// this returns. With <see cref="BatchOwnership.Writer"/> the writer takes it (in every case, also when this
    /// fails) and disposes it once written, after this returns.
    /// </summary>
    /// <exception cref="ArgumentException">The batch does not match the schema.</exception>
    /// <exception cref="InvalidOperationException">In the file format, a dictionary changes other than by growing.</exception>
    public void Write(RecordBatch batch) => IpcIO.Sync(WriteCoreAsync(batch, null, false, default));

    /// <summary>Writes a batch with custom metadata on its message.</summary>
    public void Write(RecordBatch batch, IReadOnlyDictionary<string, string> metadata) => IpcIO.Sync(WriteCoreAsync(batch, metadata, false, default));

    /// <inheritdoc cref="Write(RecordBatch)"/>
    public ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellationToken = default) => WriteCoreAsync(batch, null, true, cancellationToken);

    /// <inheritdoc cref="Write(RecordBatch, IReadOnlyDictionary{string, string})"/>
    public ValueTask WriteAsync(RecordBatch batch, IReadOnlyDictionary<string, string> metadata, CancellationToken cancellationToken = default) =>
        WriteCoreAsync(batch, metadata, true, cancellationToken);

    /// <summary>Writes what is still missing (the schema when no batch came, the end-of-stream marker, a file's footer) and flushes the stream.</summary>
    public void Complete() => IpcIO.Sync(CompleteCoreAsync(false, default));

    /// <inheritdoc cref="Complete"/>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => CompleteCoreAsync(true, cancellationToken);

    /// <summary>
    /// Gives the output up: <see cref="Dispose"/> then closes the writer without completing it (the stream keeps what
    /// was written: no end-of-stream marker, no footer). For a failure outside the writer:
    /// <c>try { …; writer.Complete(); } catch { writer.Abort(); throw; }</c>. Writing or completing afterwards throws.
    /// </summary>
    public void Abort()
    {
        if (_completed || _disposed) return;
        _aborted = true;
        _stopping = true; // batches still on their way are freed unwritten
    }

    private void CheckWritable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) throw new InvalidOperationException("The IPC writer has been completed.");
        if (_aborted) throw new InvalidOperationException("The IPC writer was aborted.");
        if (_faulted) throw new InvalidOperationException("The IPC writer failed earlier; its output cannot be completed.");
    }

    private async ValueTask WriteCoreAsync(RecordBatch batch, IReadOnlyDictionary<string, string>? metadata, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batch);
        if (_options.BatchOwnership == BatchOwnership.Writer)
        {
            await WriteOwnedAsync(batch, metadata, useAsync, ct).ConfigureAwait(false);
            return;
        }
        CheckWritable();
        await StartAsync(useAsync, ct).ConfigureAwait(false);
        // Encoding refuses a batch that does not fit before anything is written: the output stays valid.
        List<IpcOutgoingMessage> messages = _encoder.Batch(batch, metadata?.ToList(), _planned);
        bool handed = false;
        try
        {
            bool behind = _options.Compression.IpcCodec != IpcCompression.None;
            if (behind)
            {
                // The messages own their bytes from here: the caller may dispose the batch once this returns.
                foreach (IpcOutgoingMessage message in messages) message.Body?.Own();
            }
            foreach (IpcOutgoingMessage message in messages)
            {
                (message.IsDictionary ? _dictionaryBlocks : _batchBlocks).Add(new IpcBlock(_planned, message.Metadata.Length, message.BodyLength));
                _planned += message.Metadata.Length + message.BodyLength;
            }
            await WaitBehindAsync(useAsync).ConfigureAwait(false);
            if (behind)
            {
                _pending = Task.Run(async () =>
                {
                    try
                    {
                        // Not the caller's token: that call has returned; the batch is the writer's to write now.
                        foreach (IpcOutgoingMessage message in messages) await WriteMessageAsync(message, useAsync, CancellationToken.None).ConfigureAwait(false);
                    }
                    finally
                    {
                        foreach (IpcOutgoingMessage message in messages) message.Dispose();
                    }
                }, CancellationToken.None);
                handed = true;
                return;
            }
            foreach (IpcOutgoingMessage message in messages) await WriteMessageAsync(message, useAsync, ct).ConfigureAwait(false);
        }
        catch
        {
            _faulted = true;
            throw;
        }
        finally
        {
            if (!handed)
            {
                foreach (IpcOutgoingMessage message in messages) message.Dispose();
            }
        }
    }

    /// <summary>
    /// <see cref="BatchOwnership.Writer"/>: takes the batch; once it is checked and its dictionaries settled (in
    /// order, here), its messages are compressed on the thread pool and written after the batches before it, by a task
    /// that then frees them and the batch. Up to <see cref="IpcWriteOptions.MaxDegreeOfParallelism"/> batches (and
    /// <see cref="IpcWriteOptions.InFlightBytes"/> of their buffers, one batch at least) are on their way at once;
    /// beyond, this waits for the oldest to be written.
    /// </summary>
    private async ValueTask WriteOwnedAsync(RecordBatch batch, IReadOnlyDictionary<string, string>? metadata, bool useAsync, CancellationToken ct)
    {
        List<IpcOutgoingMessage> messages;
        try
        {
            CheckWritable();
            await StartAsync(useAsync, ct).ConfigureAwait(false);
            // Encoding refuses a batch that does not fit before anything is written: the output stays valid.
            messages = _encoder.Prepare(batch, metadata?.ToList());
        }
        catch
        {
            batch.Dispose();
            throw;
        }
        long bytes = messages.Sum(m => m.RawBytes);
        try
        {
            while (_ahead.Count >= _options.MaxDegreeOfParallelism || _ahead.Count > 0 && _aheadBytes + bytes > _options.InFlightBytes)
            {
                var (written, oldest) = _ahead.Dequeue();
                _aheadBytes -= oldest;
                await WaitAsync(written, useAsync, ct).ConfigureAwait(false);
            }
        }
        catch
        {
            foreach (IpcOutgoingMessage message in messages) message.Dispose();
            batch.Dispose();
            throw;
        }
        var (level, parallelism, zstdFrames) = _encoder.CompressionSettings;
        Task compressed = Task.Run(() =>
        {
            foreach (IpcOutgoingMessage message in messages) message.Compress(level, parallelism, zstdFrames);
        });
        _chain = WriteInOrderAsync(_chain, compressed, messages, batch, useAsync);
        _ahead.Enqueue((_chain, bytes));
        _aheadBytes += bytes;
    }

    /// <summary>
    /// Writes a batch's messages once it is compressed and every earlier batch written (an earlier failure fails it
    /// too, unwritten), framing them where they land; then frees them and the batch (never under a running compression).
    /// </summary>
    private async Task WriteInOrderAsync(Task previous, Task compressed, List<IpcOutgoingMessage> messages, RecordBatch batch, bool useAsync)
    {
        try
        {
            await ((Task)previous).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            await compressed.ConfigureAwait(false);
            if (previous.IsFaulted) await previous.ConfigureAwait(false); // rethrows the earlier failure
            if (_stopping) return;
            foreach (IpcOutgoingMessage message in messages)
            {
                message.Frame(_planned);
                (message.IsDictionary ? _dictionaryBlocks : _batchBlocks).Add(new IpcBlock(_planned, message.Metadata.Length, message.BodyLength));
                _planned += message.Size;
                await WriteMessageAsync(message, useAsync, CancellationToken.None).ConfigureAwait(false);
            }
        }
        finally
        {
            await ((Task)compressed).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            foreach (IpcOutgoingMessage message in messages) message.Dispose();
            batch.Dispose();
        }
    }

    /// <summary>Waits for a batch's write; its failure faults the writer and is thrown here.</summary>
    private async ValueTask WaitAsync(Task written, bool useAsync, CancellationToken ct)
    {
        try
        {
            if (useAsync) await written.WaitAsync(ct).ConfigureAwait(false);
            else written.GetAwaiter().GetResult();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested && !written.IsCompleted)
        {
            throw; // the caller gave up waiting: the writer is still sound
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    /// <summary>Waits for every batch on its way (<see cref="BatchOwnership.Writer"/>); a failure faults the writer and is thrown.</summary>
    private async ValueTask WaitChainAsync(bool useAsync)
    {
        _ahead.Clear();
        _aheadBytes = 0;
        await WaitAsync(_chain, useAsync, default).ConfigureAwait(false);
    }

    /// <summary>Stops the batches on their way (they are freed unwritten) and waits for them, whatever they end with.</summary>
    private void StopChain()
    {
        _stopping = true;
        _ahead.Clear();
        ((Task)_chain).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
    }

    /// <summary>Waits for the batch being written behind; its failure faults the writer and is thrown here.</summary>
    private async ValueTask WaitBehindAsync(bool useAsync)
    {
        if (_pending is not { } pending) return;
        _pending = null;
        try
        {
            if (useAsync) await pending.ConfigureAwait(false);
            else pending.GetAwaiter().GetResult();
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    /// <summary>Waits for the batch being written behind, whatever it ends with (the writer is closing without completing).</summary>
    private void WaitBehindQuietly()
    {
        if (_pending is not { } pending) return;
        _pending = null;
        ((Task)pending).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
    }

    /// <summary>The file's magic and the schema message, once.</summary>
    private async ValueTask StartAsync(bool useAsync, CancellationToken ct)
    {
        if (_started) return;
        _started = true;
        try
        {
            if (_options.Format == IpcFormat.File) await WriteBytesAsync("ARROW1\0\0"u8.ToArray(), useAsync, ct).ConfigureAwait(false);
            using IpcOutgoingMessage schema = _encoder.SchemaMessage(_position, null);
            await WriteMessageAsync(schema, useAsync, ct).ConfigureAwait(false);
            _planned = _position;
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    private async ValueTask WriteMessageAsync(IpcOutgoingMessage message, bool useAsync, CancellationToken ct)
    {
        await WriteBytesAsync(message.Metadata, useAsync, ct).ConfigureAwait(false);
        if (message.Body is not { } body) return;
        long at = 0;
        for (int i = 0; i < body.Buffers.Count; i++)
        {
            IpcPiece piece = body.Buffers[i];
            IpcBuffer place = message.Layout[i];
            if (place.Length == 0) continue;
            await PadAsync(place.Offset - at, useAsync, ct).ConfigureAwait(false);
            if (piece.Prefixed)
            {
                BinaryPrimitives.WriteInt64LittleEndian(_prefix, piece.Prefix);
                await WriteBytesAsync(_prefix, useAsync, ct).ConfigureAwait(false);
            }
            await WriteNativeAsync(piece.Pointer, piece.Length, useAsync, ct).ConfigureAwait(false);
            at = place.Offset + place.Length;
        }
        await PadAsync(message.BodyLength - at, useAsync, ct).ConfigureAwait(false);
    }

    private static readonly byte[] Zeros = new byte[IpcBatchEncoder.Alignment];

    private async ValueTask PadAsync(long count, bool useAsync, CancellationToken ct)
    {
        while (count > 0)
        {
            int n = (int)Math.Min(count, Zeros.Length);
            await WriteBytesAsync(Zeros.AsMemory(0, n), useAsync, ct).ConfigureAwait(false);
            count -= n;
        }
    }

    private async ValueTask WriteBytesAsync(ReadOnlyMemory<byte> bytes, bool useAsync, CancellationToken ct)
    {
        if (useAsync) await _stream.WriteAsync(bytes, ct).ConfigureAwait(false);
        else _stream.Write(bytes.Span);
        Interlocked.Add(ref _position, bytes.Length);
    }

    private async ValueTask WriteNativeAsync(nint pointer, long length, bool useAsync, CancellationToken ct)
    {
        for (long done = 0; done < length;)
        {
            int n = (int)Math.Min(length - done, 1 << 30);
            await WriteBytesAsync(Memory(pointer + (nint)done, n), useAsync, ct).ConfigureAwait(false);
            done += n;
        }

        static unsafe ReadOnlyMemory<byte> Memory(nint p, int n) => PointerMemory.Of((byte*)p, n);
    }

    private async ValueTask CompleteCoreAsync(bool useAsync, CancellationToken ct)
    {
        CheckWritable();
        try
        {
            await StartAsync(useAsync, ct).ConfigureAwait(false);
            await WaitBehindAsync(useAsync).ConfigureAwait(false);
            await WaitChainAsync(useAsync).ConfigureAwait(false);
            await WriteBytesAsync(EndOfStream.ToArray(), useAsync, ct).ConfigureAwait(false);
            if (_options.Format == IpcFormat.File)
            {
                byte[] footer = IpcMetadataWriter.Footer(_encoder.Schema, _dictionaryBlocks, _batchBlocks, _options.FileMetadata?.ToList());
                await WriteBytesAsync(footer, useAsync, ct).ConfigureAwait(false);
                byte[] tail = new byte[10];
                BinaryPrimitives.WriteInt32LittleEndian(tail, footer.Length);
                "ARROW1"u8.CopyTo(tail.AsSpan(4));
                await WriteBytesAsync(tail, useAsync, ct).ConfigureAwait(false);
            }
            if (useAsync) await _stream.FlushAsync(ct).ConfigureAwait(false);
            else _stream.Flush();
            _completed = true;
        }
        catch
        {
            _faulted = true;
            throw;
        }
    }

    /// <summary>Completes the output unless writing failed or the writer was aborted, and closes the stream unless asked not to.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        try
        {
            if (!_completed && !_faulted && !_aborted) Complete();
        }
        finally
        {
            _disposed = true;
            WaitBehindQuietly(); // aborted or failed: the batch on its way still finishes before the stream closes
            StopChain();
            _encoder.Dispose();
            if (!_options.LeaveOpen) _stream.Dispose();
        }
    }

    /// <inheritdoc cref="Dispose"/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        try
        {
            if (!_completed && !_faulted && !_aborted) await CompleteAsync().ConfigureAwait(false);
        }
        finally
        {
            _disposed = true;
            if (_pending is { } pending)
            {
                _pending = null;
                await ((Task)pending).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
            _stopping = true;
            _ahead.Clear();
            await ((Task)_chain).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            _encoder.Dispose();
            if (!_options.LeaveOpen) await _stream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
