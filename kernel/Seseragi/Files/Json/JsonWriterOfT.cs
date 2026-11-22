using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Kaminari.Sazanami.Data.Building;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>
/// Writes rows of <typeparamref name="T"/> (records, classes, structs, F# records) as JSON: one field per public
/// readable property, laid out as <c>DataFrame.From&lt;T&gt;</c> lays them out (<see cref="ArrowRowBuilder{T}"/>:
/// nested records as objects, collections as arrays, enums as their member names, decimals as strings of
/// decimal(38, 18), GUIDs as strings, dictionaries keyed by strings as objects and other dictionaries as arrays of key /
/// value objects, ...; <see cref="JsonReader{T}"/> reads it back); any other <typeparamref name="T"/> as one field
/// named <c>value</c>. The rows are gathered into record batches of <see cref="BatchRows"/> in native memory, each
/// handed to a <see cref="JsonWriter"/> (which owns and frees it, encoding several side by side); the output depends on
/// the rows alone. The rows of an array or a <see cref="List{T}"/> given to <see cref="WriteRows"/> are turned into
/// batches on several threads; rows given one by one or by a lazy sequence are read as they come, on the caller's
/// thread (a producer may reuse one object for every row).
/// </summary>
/// <remarks>
/// A row the layout refuses (a decimal that does not fit decimal(38, 18) exactly, a value outside its enum) aborts the
/// output (<see cref="Abort"/>): the rows before it in the open batch are lost, and later calls throw.
/// </remarks>
public sealed class JsonWriter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    /// <summary>The most rows of a record batch.</summary>
    public const int BatchRows = RowBatcher<T>.BatchRows;

    private readonly JsonWriter _writer;
    private readonly RowBatcher<T> _rows;
    private bool _disposed;

    private JsonWriter(JsonWriter writer, JsonWriteOptions? options)
    {
        _writer = writer;
        _rows = new RowBatcher<T>(new Sink(writer), (options ?? new()).MaxDegreeOfParallelism, "JSON");
    }

    /// <summary>The JSON writer as the batches' sink.</summary>
    private sealed class Sink(JsonWriter writer) : IBatchSink
    {
        public bool IsWritable => writer.IsWritable;

        public bool TakesBatches => true; // the writer owns the batches: several are encoded side by side

        public void Abort() => writer.Abort();

        public async ValueTask WriteAsync(RecordBatch batch, bool useAsync, CancellationToken ct)
        {
            // Not cancelled once handed over (the rows are counted already); cancellation stops the rows at their source.
            if (useAsync) await writer.WriteAsync(batch, CancellationToken.None).ConfigureAwait(false);
            else writer.Write(batch);
        }
    }

    /// <summary>Creates a writer over a stream.</summary>
    public static JsonWriter<T> Create(Stream stream, JsonWriteOptions? options = null) => new(JsonWriter.Create(stream, JsonRowSchema<T>.Write, Owned(options)), options);

    /// <summary>Creates (or overwrites) a file.</summary>
    public static JsonWriter<T> Create(string path, JsonWriteOptions? options = null) => new(JsonWriter.Create(path, JsonRowSchema<T>.Write, Owned(options)), options);

    /// <summary>Creates a writer into a buffer writer (UTF-8).</summary>
    public static JsonWriter<T> Create(IBufferWriter<byte> output, JsonWriteOptions? options = null) =>
        new(JsonWriter.Create(output, JsonRowSchema<T>.Write, Owned(options)), options);

    /// <summary>Creates a writer of text (UTF-16, uncompressed).</summary>
    public static JsonWriter<T> Create(TextWriter output, JsonWriteOptions? options = null) =>
        new(JsonWriter.Create(output, JsonRowSchema<T>.Write, Owned(options)), options);

    /// <summary>A writer over a JSON writer made for <typeparamref name="T"/>'s schema (whose options it took).</summary>
    internal static JsonWriter<T> Over(JsonWriter writer, JsonWriteOptions? options) => new(writer, options);

    /// <summary>The batches are made here and handed over: the writer owns them (whatever the options say).</summary>
    internal static JsonWriteOptions Owned(JsonWriteOptions? options) => (options ?? new()) with { BatchOwnership = BatchOwnership.Writer };

    /// <summary>The Arrow schema the rows are written with (<see cref="ArrowRowBuilder{T}.Schema"/>).</summary>
    public Schema Schema => _writer.Schema;

    /// <summary>The rows written so far (those still gathered into the open batch included).</summary>
    public long RowCount => _rows.RowCount;

    /// <summary>The bytes handed to the output so far (the rows of the open batch not included).</summary>
    public long BytesWritten => _writer.BytesWritten;

    /// <summary>Adds a row.</summary>
    /// <exception cref="ArgumentException"><paramref name="row"/> is null (rows of records cannot be null).</exception>
    public void WriteRow(T row) => _rows.WriteRow(row);

    /// <summary>Adds rows (an array or a <see cref="List{T}"/> is read as a span).</summary>
    /// <inheritdoc cref="WriteRow"/>
    public void WriteRows(IEnumerable<T> rows) => _rows.WriteRowsAsync(rows, false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="WriteRows"/>
    public ValueTask WriteRowsAsync(IEnumerable<T> rows, CancellationToken cancellationToken = default) => _rows.WriteRowsAsync(rows, true, cancellationToken);

    /// <inheritdoc cref="WriteRows"/>
    public ValueTask WriteRowsAsync(IAsyncEnumerable<T> rows, CancellationToken cancellationToken = default) => _rows.WriteRowsAsync(rows, cancellationToken);

    /// <summary>Writes the rows so far (the open batch included) to the output (<see cref="JsonWriter.Flush"/>).</summary>
    public void Flush()
    {
        _rows.FlushAsync(false, default).AsTask().GetAwaiter().GetResult();
        _writer.Flush();
    }

    /// <inheritdoc cref="Flush"/>
    public async ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        await _rows.FlushAsync(true, cancellationToken).ConfigureAwait(false);
        await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes the rows not written yet and the document's end.</summary>
    public void Complete()
    {
        _rows.FlushAsync(false, default).AsTask().GetAwaiter().GetResult();
        _writer.Complete();
    }

    /// <inheritdoc cref="Complete"/>
    public async ValueTask CompleteAsync(CancellationToken cancellationToken = default)
    {
        await _rows.FlushAsync(true, cancellationToken).ConfigureAwait(false);
        await _writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="JsonWriter.Abort"/>
    public void Abort() => _writer.Abort();

    /// <summary>Completes the output (the rows not written yet included) unless writing failed or the writer was aborted, and closes it.</summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        try
        {
            if (_writer.IsWritable) _rows.FlushAsync(false, default).AsTask().GetAwaiter().GetResult();
        }
        finally
        {
            _writer.Dispose();
            _rows.Dispose();
        }
    }

    /// <inheritdoc cref="Dispose"/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;
        try
        {
            if (_writer.IsWritable) await _rows.FlushAsync(true, default).ConfigureAwait(false);
        }
        finally
        {
            await _writer.DisposeAsync().ConfigureAwait(false);
            _rows.Dispose();
        }
    }
}
