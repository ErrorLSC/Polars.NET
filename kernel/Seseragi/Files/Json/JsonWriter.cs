using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Json.Writing;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>
/// Writes Apache Arrow record batches as JSON, the text the reference dataframe engine writes: newline-delimited (one
/// object per row and line) or one document (an array of objects), compact, every field of every row written (nulls
/// included). Rows are cut into pieces encoded on several threads into native memory and written in order; the bytes
/// do not depend on the threads. Values: integers of every width as numbers, floats as their shortest digits (NaN and
/// infinities null), decimals as strings, dates, times and timestamps in ISO 8601 (with the zone's offset), durations
/// as <c>PT…S</c>, binaries as base64 strings, lists as arrays, structs and maps as objects.
/// </summary>
public sealed class JsonWriter : IDisposable, IAsyncDisposable
{
    private readonly JsonSink _sink;
    private readonly JsonWriteOptions _options;
    private readonly byte[][] _prefixes;    // per column: '{' or ',' then its name and a colon
    private readonly NativeBufferWriter?[] _spare;           // buffers of pieces written, used again (no lock: slots taken by exchange)
    private readonly Action<NativeBufferWriter> _giveBack;
    // A batch's pieces are encoded side by side by workers taking them in turn, and written in order by a chain of tasks
    // (each batch's write waits for the one before), behind the encoding: the queue holds each batch's write, for the
    // budget.
    private readonly Queue<(Task Written, long Bytes)> _ahead = [];
    private NativeBufferWriter? _open; // rows encoded by AppendRows, not handed to the writes yet
    private long _aheadBytes, _rows, _written;
    private Task _chain = Task.CompletedTask;
    private Exception? _failure;             // what failed a piece or a write (the chain drops the pieces after it)
    private volatile bool _stopping;         // aborted or disposed: pieces still on their way are dropped
    private double _rowBytes;
    private bool _completed, _aborted, _faulted, _disposed;

    private JsonWriter(JsonSink sink, Schema schema, JsonWriteOptions options)
    {
        options.Validate();
        if (sink.IsText && options.Compression != JsonCompression.None)
            throw new ArgumentException("Text is written uncompressed (write bytes to compress them).", nameof(options));
        foreach (Field field in schema.FieldsList) JsonCell.Check(field, field.Name);
        _sink = sink;
        _options = options;
        Schema = schema;
        _prefixes = [.. schema.FieldsList.Select((f, k) => (byte[])[k == 0 ? (byte)'{' : (byte)',', .. JsonText.Name(f.Name)])];
        _rowBytes = 16 + 24 * schema.FieldsList.Count;
        _giveBack = Spare;
        _spare = new NativeBufferWriter?[4 * options.MaxDegreeOfParallelism];
    }

    /// <summary>The schema the batches are written with.</summary>
    public Schema Schema { get; }

    /// <summary>Whether batches can still be written.</summary>
    internal bool IsWritable => !_completed && !_aborted && !_faulted && !_disposed;

    /// <summary>The bytes handed to the output so far.</summary>
    public long BytesWritten => Interlocked.Read(ref _written);

    /// <summary>Creates a writer over a stream (nothing is written before the first rows, or <see cref="Complete"/>).</summary>
    /// <exception cref="NotSupportedException">The schema has a type JSON cannot hold (or a time zone not known).</exception>
    public static JsonWriter Create(Stream stream, Schema schema, JsonWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(stream);
        ArgumentNullException.ThrowIfNull(schema);
        if (!stream.CanWrite) throw new ArgumentException("The stream is not writable.", nameof(stream));
        options ??= new();
        return new(new StreamSink(stream, options.LeaveOpen), schema, options);
    }

    /// <summary>Creates (or overwrites) a file.</summary>
    /// <exception cref="NotSupportedException">The schema has a type JSON cannot hold (or a time zone not known).</exception>
    public static JsonWriter Create(string path, Schema schema, JsonWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(schema);
        options ??= new();
        options.Validate();
        foreach (Field field in schema.FieldsList) JsonCell.Check(field, field.Name);
        var sink = new FileSink(path);
        try
        {
            return new(sink, schema, options);
        }
        catch
        {
            sink.CloseAsync(false).GetAwaiter().GetResult();
            throw;
        }
    }

    /// <summary>
    /// Creates a writer into a buffer writer (UTF-8): an array or native buffer writer, a pipe. The rows are written
    /// behind the calls: the output is the writer's until <see cref="Flush"/>, <see cref="Complete"/> or
    /// <see cref="Dispose"/> returns. A <see cref="System.IO.Pipelines.PipeWriter"/> is flushed after every piece of
    /// rows (writing waits while its reader is behind: write asynchronously then) and left uncompleted; a reader that
    /// stops fails the writer with an <see cref="IOException"/>.
    /// </summary>
    /// <exception cref="NotSupportedException">The schema has a type JSON cannot hold (or a time zone not known).</exception>
    public static JsonWriter Create(IBufferWriter<byte> output, Schema schema, JsonWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(schema);
        return new(output is System.IO.Pipelines.PipeWriter pipe ? new PipeSink(pipe) : new BufferSink(output), schema, options ?? new());
    }

    /// <summary>Creates a writer of text (UTF-16, uncompressed).</summary>
    /// <exception cref="NotSupportedException">The schema has a type JSON cannot hold (or a time zone not known).</exception>
    public static JsonWriter Create(TextWriter output, Schema schema, JsonWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(output);
        ArgumentNullException.ThrowIfNull(schema);
        options ??= new();
        return new(new TextSink(output, options.LeaveOpen), schema, options);
    }

    /// <summary>Writes all batches to a stream and completes it.</summary>
    public static void Write(Stream stream, Schema schema, IEnumerable<RecordBatch> batches, JsonWriteOptions? options = null)
    {
        using JsonWriter writer = Create(stream, schema, options);
        WriteAll(writer, batches);
    }

    /// <summary>Writes all batches to a file.</summary>
    public static void Write(string path, Schema schema, IEnumerable<RecordBatch> batches, JsonWriteOptions? options = null)
    {
        using JsonWriter writer = Create(path, schema, options);
        WriteAll(writer, batches);
    }

    /// <summary>Writes all batches to a buffer writer.</summary>
    public static void Write(IBufferWriter<byte> output, Schema schema, IEnumerable<RecordBatch> batches, JsonWriteOptions? options = null)
    {
        using JsonWriter writer = Create(output, schema, options);
        WriteAll(writer, batches);
    }

    /// <summary>Writes all batches to a text writer.</summary>
    public static void Write(TextWriter output, Schema schema, IEnumerable<RecordBatch> batches, JsonWriteOptions? options = null)
    {
        using JsonWriter writer = Create(output, schema, options);
        WriteAll(writer, batches);
    }

    /// <summary>The batches as a string of JSON.</summary>
    public static string WriteString(Schema schema, IEnumerable<RecordBatch> batches, JsonWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var sink = new StringSink();
        try
        {
            using (var writer = new JsonWriter(sink, schema, options ?? new())) WriteAll(writer, batches);
            return sink.TakeText();
        }
        catch
        {
            sink.TakeText();
            throw;
        }
    }

    /// <summary>Writes all batches to a stream and completes it.</summary>
    public static async Task WriteAsync(Stream stream, Schema schema, IAsyncEnumerable<RecordBatch> batches, JsonWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using JsonWriter writer = Create(stream, schema, options);
        await WriteAllAsync(writer, batches, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes all batches to a file.</summary>
    public static async Task WriteAsync(string path, Schema schema, IAsyncEnumerable<RecordBatch> batches, JsonWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using JsonWriter writer = Create(path, schema, options);
        await WriteAllAsync(writer, batches, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes all batches to a buffer writer.</summary>
    public static async Task WriteAsync(IBufferWriter<byte> output, Schema schema, IAsyncEnumerable<RecordBatch> batches, JsonWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using JsonWriter writer = Create(output, schema, options);
        await WriteAllAsync(writer, batches, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes all batches to a text writer.</summary>
    public static async Task WriteAsync(TextWriter output, Schema schema, IAsyncEnumerable<RecordBatch> batches, JsonWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        await using JsonWriter writer = Create(output, schema, options);
        await WriteAllAsync(writer, batches, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes rows of <typeparamref name="T"/> to a file and completes it (<see cref="JsonWriter{T}"/>). If writing
    /// fails, the partial file is deleted.
    /// </summary>
    public static void WriteRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path, IEnumerable<T> rows, JsonWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var writer = JsonWriter<T>.Create(path, options);
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

    /// <summary>Writes rows of <typeparamref name="T"/> to a stream and completes the output (<see cref="JsonWriter{T}"/>).</summary>
    public static void WriteRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Stream stream, IEnumerable<T> rows, JsonWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(rows);
        using var writer = JsonWriter<T>.Create(stream, options);
        WriteAllRows(writer, rows);
    }

    /// <summary>Writes rows of <typeparamref name="T"/> to a buffer writer (<see cref="JsonWriter{T}"/>).</summary>
    public static void WriteRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(IBufferWriter<byte> output, IEnumerable<T> rows, JsonWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(rows);
        using var writer = JsonWriter<T>.Create(output, options);
        WriteAllRows(writer, rows);
    }

    /// <summary>Writes rows of <typeparamref name="T"/> to a text writer (<see cref="JsonWriter{T}"/>).</summary>
    public static void WriteRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(TextWriter output, IEnumerable<T> rows, JsonWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(rows);
        using var writer = JsonWriter<T>.Create(output, options);
        WriteAllRows(writer, rows);
    }

    /// <summary>Rows of <typeparamref name="T"/> as a string of JSON (<see cref="JsonWriter{T}"/>).</summary>
    public static string WriteString<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(IEnumerable<T> rows, JsonWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var sink = new StringSink();
        try
        {
            using (var writer = JsonWriter<T>.Over(new JsonWriter(sink, JsonRowSchema<T>.Write, JsonWriter<T>.Owned(options)), options))
                WriteAllRows(writer, rows);
            return sink.TakeText();
        }
        catch
        {
            sink.TakeText();
            throw;
        }
    }

    /// <inheritdoc cref="WriteRows{T}(string, IEnumerable{T}, JsonWriteOptions?)"/>
    public static async Task WriteRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path, IAsyncEnumerable<T> rows,
        JsonWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var writer = JsonWriter<T>.Create(path, options);
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

    /// <inheritdoc cref="WriteRows{T}(Stream, IEnumerable{T}, JsonWriteOptions?)"/>
    public static async Task WriteRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(Stream stream, IAsyncEnumerable<T> rows,
        JsonWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        await using var writer = JsonWriter<T>.Create(stream, options);
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

    /// <inheritdoc cref="WriteRows{T}(IBufferWriter{byte}, IEnumerable{T}, JsonWriteOptions?)"/>
    public static async Task WriteRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(IBufferWriter<byte> output, IAsyncEnumerable<T> rows,
        JsonWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        await using var writer = JsonWriter<T>.Create(output, options);
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

    private static void WriteAllRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(JsonWriter<T> writer, IEnumerable<T> rows)
    {
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

    private static void WriteAll(JsonWriter writer, IEnumerable<RecordBatch> batches)
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

    private static async Task WriteAllAsync(JsonWriter writer, IAsyncEnumerable<RecordBatch> batches, CancellationToken ct)
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

    /// <summary>Writes a batch's rows (taken over with <see cref="BatchOwnership.Writer"/>).</summary>
    /// <exception cref="ArgumentException">The batch's columns are not the schema's.</exception>
    /// <exception cref="JsonValueException">A value JSON cannot hold (a string that is not UTF-8); the writer cannot be completed then.</exception>
    public void Write(RecordBatch batch) => Sync(WriteCoreAsync(batch, false, default));

    /// <inheritdoc cref="Write(RecordBatch)"/>
    public ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellationToken = default) => WriteCoreAsync(batch, true, cancellationToken);

    /// <summary>
    /// Writes the rows still on their way (rows are encoded and written behind <see cref="Write(RecordBatch)"/>): the
    /// output holds every row written so far, and is the caller's until the next write.
    /// </summary>
    public void Flush() => Sync(FlushCoreAsync(false, default));

    /// <inheritdoc cref="Flush"/>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default) => FlushCoreAsync(true, cancellationToken);

    private async ValueTask FlushCoreAsync(bool useAsync, CancellationToken ct)
    {
        CheckWritable();
        await HandOffAsync(useAsync).ConfigureAwait(false);
        await DrainAsync(useAsync).ConfigureAwait(false);
        ThrowIfFailed();
        await _sink.CompleteAsync(useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>Writes the rows still on their way and the document's end; nothing can be written afterwards.</summary>
    public void Complete() => Sync(CompleteCoreAsync(false, default));

    /// <inheritdoc cref="Complete"/>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => CompleteCoreAsync(true, cancellationToken);

    /// <summary>
    /// Gives the output up: rows still on their way are not written and the output is not completed (a document's
    /// array stays open); <see cref="Dispose"/> then closes it. Writing or completing afterwards throws.
    /// </summary>
    public void Abort()
    {
        if (_completed || _disposed) return;
        _aborted = true;
        _stopping = true;
    }

    private static void Sync(ValueTask task)
    {
        if (task.IsCompletedSuccessfully) return;
        task.AsTask().GetAwaiter().GetResult();
    }

    private void CheckWritable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_completed) throw new InvalidOperationException("The JSON writer has been completed.");
        if (_aborted) throw new InvalidOperationException("The JSON writer was aborted.");
        if (_faulted) throw new InvalidOperationException("The JSON writer failed earlier; its output cannot be completed.");
    }

    private void CheckBatch(RecordBatch batch)
    {
        IReadOnlyList<Field> fields = Schema.FieldsList;
        if (batch.ColumnCount != fields.Count)
            throw new ArgumentException($"The batch has {batch.ColumnCount} columns, the schema {fields.Count}.", nameof(batch));
        for (int i = 0; i < fields.Count; i++)
            if (!ArrowTypes.SameType(batch.Column(i).Data.DataType, fields[i].DataType))
                throw new ArgumentException($"Column '{fields[i].Name}' is {batch.Column(i).Data.DataType.Name} in the batch, {fields[i].DataType.Name} in the schema.", nameof(batch));
    }

    private async ValueTask WriteCoreAsync(RecordBatch batch, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batch);
        bool owned = _options.BatchOwnership == BatchOwnership.Writer;
        BatchCells cells;
        try
        {
            CheckWritable();
            CheckBatch(batch);
            if (batch.Length == 0)
            {
                if (owned) batch.Dispose();
                return;
            }
            cells = new BatchCells(batch, Schema, owned);
        }
        catch
        {
            if (owned) batch.Dispose();
            throw;
        }
        try
        {
            await HandOffAsync(useAsync).ConfigureAwait(false); // rows appended before go first
        }
        catch
        {
            cells.Expect(1);
            cells.Release();
            throw;
        }
        // Pieces of about a piece's bytes, smaller when that leaves threads idle on this batch (four per thread at least).
        int threads = _options.MaxDegreeOfParallelism, length = batch.Length;
        double rowBytes = Math.Max(1, Volatile.Read(ref _rowBytes));
        long target = Math.Min(_options.PieceBytes, Math.Max((long)(rowBytes * length) / (4 * threads), Math.Min(_options.PieceBytes, 16L << 10)));
        int pieceRows = (int)Math.Clamp(target / rowBytes, 1, length);
        int pieces = (length + pieceRows - 1) / pieceRows;
        long bytes = (long)(rowBytes * length);
        cells.Expect(pieces);
        try
        {
            // Room for the batch: the writes ahead within the budget (one batch goes beyond alone).
            while (_ahead.Count > 0 && (_ahead.Count >= 4 || _aheadBytes + bytes > _options.InFlightBytes))
            {
                var (written, done) = _ahead.Dequeue();
                _aheadBytes -= done;
                if (useAsync) await written.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                else written.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
                ThrowIfFailed();
            }
        }
        catch
        {
            for (int k = 0; k < pieces; k++) cells.Release();
            throw;
        }
        long first = _rows;
        _rows += length;
        var slots = new EncodedPiece?[pieces];
        int next = 0, workers = Math.Min(threads, pieces);
        var tasks = new Task[workers];
        for (int w = 0; w < workers; w++)
            tasks[w] = Task.Run(() =>
            {
                for (int k; (k = Interlocked.Increment(ref next) - 1) < pieces;)
                {
                    int from = k * pieceRows;
                    slots[k] = Encode(cells, from, Math.Min(length, from + pieceRows), first + from);
                }
            });
        Task encoded = Task.WhenAll(tasks);
        _chain = WriteInOrderAsync(_chain, encoded, slots, useAsync);
        _ahead.Enqueue((_chain, bytes));
        _aheadBytes += bytes;
        // The caller's batch is free once its pieces are encoded (they are written behind); a piece's failure fails this
        // call (the first in order).
        if (!owned)
        {
            if (useAsync) await encoded.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            else encoded.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
            foreach (EncodedPiece? piece in slots)
                if (piece?.Error is { } error)
                {
                    Interlocked.CompareExchange(ref _failure, error, null);
                    break;
                }
        }
        ThrowIfFailed();
    }

    // A failure met behind (a piece's, a write's) fails the writer at the next call.
    private void ThrowIfFailed()
    {
        if (Volatile.Read(ref _failure) is not { } failure) return;
        _faulted = true;
        ExceptionDispatchInfo.Throw(failure);
    }

    // Writes a batch's pieces once they are encoded and the batches before are written (dropped after a failure, or
    // once the writer stops).
    private async Task WriteInOrderAsync(Task previous, Task encoded, EncodedPiece?[] pieces, bool useAsync)
    {
        await previous.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        await encoded.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        try
        {
            // The pieces up to a failure, written as the caller writes (an output may take asynchronous writes only).
            List<ReadOnlyMemory<byte>> ready = new(pieces.Length);
            long bytes = 0;
            foreach (EncodedPiece? piece in pieces)
            {
                if (piece is null || _stopping || Volatile.Read(ref _failure) is not null) break;
                if (piece.Error is { } error)
                {
                    Volatile.Write(ref _failure, error);
                    break;
                }
                ready.Add(piece.Bytes);
                bytes += piece.Bytes.Length;
            }
            if (_sink is FileSink file) file.WriteAll(ready, bytes);
            else
                foreach (ReadOnlyMemory<byte> piece in ready) await _sink.WriteAsync(piece, useAsync, CancellationToken.None).ConfigureAwait(false);
            Interlocked.Add(ref _written, bytes);
        }
        catch (Exception e)
        {
            Volatile.Write(ref _failure, e);
        }
        finally
        {
            foreach (EncodedPiece? piece in pieces) piece?.Dispose();
        }
    }

    // Rows [start, end) of a batch: objects (a document's separated by commas, its first preceded by '['; lines each
    // ending with a newline), compressed as asked. A failure is the piece's, reported in order.
    private EncodedPiece Encode(BatchCells cells, int start, int end, long first)
    {
        NativeBufferWriter buffer = Rent(end - start);
        try
        {
            EncodeRows(cells.Cells, buffer, start, end, first);
            return Piece(buffer);
        }
        catch (Exception e)
        {
            Spare(buffer);
            return new EncodedPiece(null, e);
        }
        finally { cells.Release(); }
    }

    // A buffer for about `rows` rows: a spare one, else a new one.
    private NativeBufferWriter Rent(int rows)
    {
        NativeBufferWriter? buffer = null;
        for (int k = 0; k < _spare.Length && buffer is null; k++) buffer = Interlocked.Exchange(ref _spare[k], null);
        return buffer ?? new NativeBufferWriter((int)Math.Min(1 << 24, (long)(Volatile.Read(ref _rowBytes) * rows * 1.25) + 64));
    }

    // Encoded rows as a piece to write: compressed as asked (one gzip member or Zstandard frame of its own).
    private EncodedPiece Piece(NativeBufferWriter buffer) => _options.Compression == JsonCompression.None
        ? new EncodedPiece(buffer, null, _giveBack)
        : new EncodedPiece(EncodedPiece.Compress(buffer, _options.Compression, _options.CompressionLevel), null);

    // Rows [start, end) of a batch's cells appended to `buffer`: objects (a document's separated by commas, its first
    // preceded by '['; lines each ending with a newline), the first of them row `first` of all written. A value JSON
    // cannot hold fails naming its row among all.
    private void EncodeRows(JsonCell[] columns, NativeBufferWriter buffer, int start, int end, long first)
    {
        bool document = _options.Format == JsonFormat.Document;
        int before = buffer.WrittenCount, row = start;
        try
        {
            var o = new JsonOut(buffer);
            for (; row < end; row++)
            {
                if (document) o.Byte(first + row - start == 0 ? (byte)'[' : (byte)',');
                if (columns.Length == 0) o.Byte((byte)'{');
                for (int k = 0; k < columns.Length; k++)
                {
                    o.Bytes(_prefixes[k]);
                    columns[k].WriteOrNull(ref o, row);
                }
                if (document) o.Byte((byte)'}');
                else o.Bytes("}\n"u8);
            }
            o.Commit();
        }
        catch (JsonValueException v)
        {
            throw new JsonValueException($"Row {first + row - start}: {v.Message}");
        }
        Volatile.Write(ref _rowBytes, (double)(buffer.WrittenCount - before) / (end - start));
    }

    /// <summary>
    /// Rows [<paramref name="start"/>, +<paramref name="count"/>) of the batch <paramref name="cells"/> read, encoded on
    /// this thread into the piece being filled (the caller's batch is free once this returns): the piece is written once
    /// it holds a piece's bytes, or before anything written after it (<see cref="Write(RecordBatch)"/>, <see cref="Flush"/>,
    /// <see cref="Complete"/>). Rows a few at a time (a partition's rows of each batch of a dataset written side by side)
    /// cost no write each, nor a copy before encoding. A value JSON cannot hold fails the writer.
    /// </summary>
    internal void AppendRows(BatchCells cells, int start, int count)
    {
        CheckWritable();
        ThrowIfFailed();
        if (count == 0) return;
        NativeBufferWriter open = _open ??= Rent(count);
        try
        {
            EncodeRows(cells.Cells, open, start, start + count, _rows);
        }
        catch
        {
            _faulted = true; // the piece holds part of a row
            throw;
        }
        _rows += count;
        if (open.WrittenCount >= _options.PieceBytes) Sync(HandOffAsync(false));
    }

    /// <summary>The bytes encoded by <see cref="AppendRows"/>, not handed to the writes yet.</summary>
    internal long OpenBytes => _open?.WrittenCount ?? 0;

    /// <summary>Hands the rows encoded by <see cref="AppendRows"/> to the writes (to hold less memory), without waiting for them.</summary>
    internal void HandOff()
    {
        CheckWritable();
        Sync(HandOffAsync(false));
    }

    // The piece being filled, written after those before (the writes ahead within the budget first).
    private async ValueTask HandOffAsync(bool useAsync)
    {
        if (_open is not { WrittenCount: > 0 } open) return;
        _open = null;
        EncodedPiece piece = Piece(open);
        long bytes = piece.Bytes.Length;
        try
        {
            while (_ahead.Count > 0 && (_ahead.Count >= 4 || _aheadBytes + bytes > _options.InFlightBytes))
            {
                var (written, done) = _ahead.Dequeue();
                _aheadBytes -= done;
                if (useAsync) await written.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                else written.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
                ThrowIfFailed();
            }
        }
        catch
        {
            piece.Dispose();
            throw;
        }
        _chain = WriteInOrderAsync(_chain, Task.CompletedTask, [piece], useAsync);
        _ahead.Enqueue((_chain, bytes));
        _aheadBytes += bytes;
    }

    // A buffer written, kept for the next pieces (as many as may be on their way at once; the others go).
    private void Spare(NativeBufferWriter buffer)
    {
        buffer.ResetWrittenCount();
        if (!_disposed)
            for (int k = 0; k < _spare.Length; k++)
                if (Interlocked.CompareExchange(ref _spare[k], buffer, null) is null) return;
        buffer.Dispose();
    }

    // Every piece's write, waited for.
    private async ValueTask DrainAsync(bool useAsync)
    {
        _ahead.Clear();
        _aheadBytes = 0;
        if (useAsync) await _chain.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        else _chain.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing).GetAwaiter().GetResult();
    }

    private async ValueTask CompleteCoreAsync(bool useAsync, CancellationToken ct)
    {
        CheckWritable();
        await HandOffAsync(useAsync).ConfigureAwait(false);
        await DrainAsync(useAsync).ConfigureAwait(false);
        ThrowIfFailed();
        if (_options.Format == JsonFormat.Document)
        {
            var end = new NativeBufferWriter(8);
            ReadOnlySpan<byte> close = _rows == 0 ? "[]"u8 : "]"u8;
            close.CopyTo(end.GetSpan(close.Length));
            end.Advance(close.Length);
            using var tail = new EncodedPiece(EncodedPiece.Compress(end, _options.Compression, _options.CompressionLevel), null);
            try { await _sink.WriteAsync(tail.Bytes, useAsync, ct).ConfigureAwait(false); }
            catch
            {
                _faulted = true;
                throw;
            }
            _written += tail.Bytes.Length;
        }
        _completed = true;
        await _sink.CompleteAsync(useAsync, ct).ConfigureAwait(false);
    }

    private void FreeSpare()
    {
        for (int k = 0; k < _spare.Length; k++) Interlocked.Exchange(ref _spare[k], null)?.Dispose();
        _open?.Dispose(); // rows appended, not written (an aborted or failed writer)
        _open = null;
    }

    /// <summary>Completes the output unless writing failed or the writer was aborted, and closes it (unless asked not to).</summary>
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
            _stopping = true;
            Sync(DrainAsync(false));
            FreeSpare();
            Sync(_sink.CloseAsync(false));
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
            _stopping = true;
            await DrainAsync(true).ConfigureAwait(false);
            FreeSpare();
            await _sink.CloseAsync(true).ConfigureAwait(false);
        }
    }
}

/// <summary>The cells of a batch's columns, its buffers pinned until its last piece is encoded (an owned batch then disposed).</summary>
internal sealed class BatchCells
{
    private readonly Pins _pins = new();
    private readonly RecordBatch? _owned;
    private int _pending;

    public BatchCells(RecordBatch batch, Schema schema, bool owned)
    {
        try
        {
            Cells = [.. schema.FieldsList.Select((f, i) => JsonCell.For(batch.Column(i).Data, f, _pins, f.Name))];
        }
        catch
        {
            _pins.Dispose();
            throw;
        }
        _owned = owned ? batch : null;
    }

    public JsonCell[] Cells { get; }

    /// <summary>The pieces that will each <see cref="Release"/> the batch.</summary>
    public void Expect(int pieces) => _pending = pieces;

    public void Release()
    {
        if (Interlocked.Decrement(ref _pending) != 0) return;
        _pins.Dispose();
        _owned?.Dispose();
    }

    /// <summary>Unpins the batch's buffers (cells of a caller's batch encoded through <see cref="JsonWriter.AppendRows"/>, not by pieces).</summary>
    public void Dispose() => _pins.Dispose();
}
