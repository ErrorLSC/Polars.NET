using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.IO.Writing;
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
public sealed class JsonWriter : IDisposable, IAsyncDisposable, IRowEncoder<JsonCell>
{
    private readonly JsonWriteOptions _options;
    private readonly byte[][] _prefixes;    // per column: '{' or ',' then its name and a colon
    private readonly RowWriter<JsonCell> _rows;

    private JsonWriter(ByteSink sink, Schema schema, JsonWriteOptions options)
    {
        options.Validate();
        if (sink.IsText && options.Compression != JsonCompression.None)
            throw new ArgumentException("Text is written uncompressed (write bytes to compress them).", nameof(options));
        foreach (Field field in schema.FieldsList) JsonCell.Check(field, field.Name);
        _options = options;
        _prefixes = [.. schema.FieldsList.Select((f, k) => (byte[])[k == 0 ? (byte)'{' : (byte)',', .. JsonText.Name(f.Name)])];
        _rows = new RowWriter<JsonCell>(sink, schema, this, Cell, new RowWriterSettings(
            "JSON", options.MaxDegreeOfParallelism, options.BatchOwnership, Compression(options.Compression), options.CompressionLevel,
            options.PieceBytes, options.InFlightBytes, 16 + 24 * schema.FieldsList.Count,
            options.WindowPieces > 0 ? options.WindowPieces : 8 * options.MaxDegreeOfParallelism));
    }

    private static JsonCell Cell(ArrayData data, Field field, Pins pins) => JsonCell.For(data, field, pins, field.Name);

    private static PieceCompression Compression(JsonCompression compression) => compression switch
    {
        JsonCompression.Gzip => PieceCompression.Gzip,
        JsonCompression.Zstd => PieceCompression.Zstd,
        _ => PieceCompression.None,
    };

    /// <summary>The cells of a batch's columns as a writer of <paramref name="schema"/> writes them (its buffers pinned until disposed).</summary>
    internal static BatchCells<JsonCell> CellsOf(RecordBatch batch, Schema schema) => new(batch, schema, false, Cell);

    /// <summary>The schema the batches are written with.</summary>
    public Schema Schema => _rows.Schema;

    /// <summary>Whether batches can still be written.</summary>
    internal bool IsWritable => _rows.IsWritable;

    /// <summary>The bytes handed to the output so far.</summary>
    public long BytesWritten => _rows.BytesWritten;

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
    public void Write(RecordBatch batch) => _rows.Write(batch);

    /// <inheritdoc cref="Write(RecordBatch)"/>
    public ValueTask WriteAsync(RecordBatch batch, CancellationToken cancellationToken = default) => _rows.WriteAsync(batch, cancellationToken);

    /// <summary>
    /// Writes the rows still on their way (rows are encoded and written behind <see cref="Write(RecordBatch)"/>): the
    /// output holds every row written so far, and is the caller's until the next write.
    /// </summary>
    public void Flush() => _rows.Flush();

    /// <inheritdoc cref="Flush"/>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default) => _rows.FlushAsync(cancellationToken);

    /// <summary>Writes the rows still on their way and the document's end; nothing can be written afterwards.</summary>
    public void Complete() => _rows.Complete();

    /// <inheritdoc cref="Complete"/>
    public ValueTask CompleteAsync(CancellationToken cancellationToken = default) => _rows.CompleteAsync(cancellationToken);

    /// <summary>
    /// Gives the output up: rows still on their way are not written and the output is not completed (a document's
    /// array stays open); <see cref="Dispose"/> then closes it. Writing or completing afterwards throws.
    /// </summary>
    public void Abort() => _rows.Abort();

    /// <summary>
    /// Rows [<paramref name="start"/>, +<paramref name="count"/>) of the batch <paramref name="cells"/> read, encoded on
    /// this thread into the piece being filled (the caller's batch is free once this returns): the piece is written once
    /// it holds a piece's bytes, or before anything written after it (<see cref="Write(RecordBatch)"/>, <see cref="Flush"/>,
    /// <see cref="Complete"/>). Rows a few at a time (a partition's rows of each batch of a dataset written side by side)
    /// cost no write each, nor a copy before encoding. A value JSON cannot hold fails the writer.
    /// </summary>
    internal void AppendRows(BatchCells<JsonCell> cells, int start, int count) => _rows.AppendRows(cells, start, count);

    /// <summary>The bytes encoded by <see cref="AppendRows"/>, not handed to the writes yet.</summary>
    internal long OpenBytes => _rows.OpenBytes;

    /// <summary>Hands the rows encoded by <see cref="AppendRows"/> to the writes (to hold less memory), without waiting for them.</summary>
    internal void HandOff() => _rows.HandOff();

    // Rows [start, end) of a batch's cells appended to `buffer`: objects (a document's separated by commas, its first
    // preceded by '['; lines each ending with a newline), the first of them row `first` of all written. A value JSON
    // cannot hold fails naming its row among all.
    void IRowEncoder<JsonCell>.EncodeRows(JsonCell[] columns, NativeBufferWriter buffer, int start, int end, long first)
    {
        bool document = _options.Format == JsonFormat.Document;
        int row = start;
        try
        {
            var o = new TextOut(buffer);
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
    }

    // A document's end: its array closed (an empty one when no rows).
    ReadOnlySpan<byte> IRowEncoder<JsonCell>.Tail(long rows) => _options.Format != JsonFormat.Document ? default : rows == 0 ? "[]"u8 : "]"u8;

    /// <summary>Completes the output unless writing failed or the writer was aborted, and closes it (unless asked not to).</summary>
    public void Dispose() => _rows.Dispose();

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync() => _rows.DisposeAsync();
}
