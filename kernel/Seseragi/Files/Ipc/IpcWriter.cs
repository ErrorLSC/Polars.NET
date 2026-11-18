using System.Buffers.Binary;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.Ipc;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// Writes Arrow record batches as an IPC file (<c>.arrow</c>, Feather v2) or stream (<c>.arrows</c>): metadata V5,
/// little-endian, buffers 64-byte aligned, optional LZ4 frame or Zstandard compression of each buffer. Arrays are
/// written from where they are: sliced arrays as their own rows. Dictionary-encoded columns write their dictionary
/// once, then only what changes: a delta when a batch's dictionary extends the last one, else (the stream format only)
/// a replacement. Not thread-safe.
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
    private long _position;
    private bool _started, _completed, _aborted, _faulted, _disposed;

    private IpcWriter(Stream stream, Schema schema, IpcWriteOptions options)
    {
        options.Validate();
        _stream = stream;
        _options = options;
        _encoder = new IpcMessageEncoder(schema, options.Compression.IpcCodec, options.Compression.IpcLevel, options.MaxDegreeOfParallelism,
            options.Format == IpcFormat.File);
    }

    /// <summary>The bytes written to the stream so far.</summary>
    public long BytesWritten => _position;

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

    /// <summary>Writes a batch (and the dictionary batches it needs). The batch is not kept: it may be disposed once this returns.</summary>
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
        if (!_completed && !_disposed) _aborted = true;
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
        CheckWritable();
        await StartAsync(useAsync, ct).ConfigureAwait(false);
        // Encoding refuses a batch that does not fit before anything is written: the output stays valid.
        List<IpcOutgoingMessage> messages = _encoder.Batch(batch, metadata?.ToList(), _position);
        try
        {
            foreach (IpcOutgoingMessage message in messages)
            {
                long at = _position;
                await WriteMessageAsync(message, useAsync, ct).ConfigureAwait(false);
                (message.IsDictionary ? _dictionaryBlocks : _batchBlocks).Add(new IpcBlock(at, message.Metadata.Length, message.BodyLength));
            }
        }
        catch
        {
            _faulted = true;
            throw;
        }
        finally
        {
            foreach (IpcOutgoingMessage message in messages) message.Dispose();
        }
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
        _position += bytes.Length;
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
            _encoder.Dispose();
            if (!_options.LeaveOpen) await _stream.DisposeAsync().ConfigureAwait(false);
        }
    }
}
