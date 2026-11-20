using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Kaminari.Sazanami.Data.Building;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// Writes rows of <typeparamref name="T"/> (records, classes, structs, F# records) as Arrow IPC data: one column per
/// public readable property, laid out as <c>DataFrame.From&lt;T&gt;</c> lays them out (<see cref="ArrowRowBuilder{T}"/>:
/// strings as string views, collections as large lists, nested records as structs, enums as dictionaries of their
/// member names, decimals as decimal(38, 18), dictionaries as lists of key / value structs, ...); any other
/// <typeparamref name="T"/> as one column named <c>value</c>. <see cref="IpcReader{T}"/> reads the rows back. The rows
/// are gathered into record batches of <see cref="BatchRows"/> in native memory, each written by an
/// <see cref="IpcWriter"/> and freed; the output depends on the rows alone, not on how they were passed nor on the
/// threads. The rows of an array or a <see cref="List{T}"/> given to <see cref="WriteRows"/> are turned into batches on
/// up to <see cref="IpcWriteOptions.MaxDegreeOfParallelism"/> threads at once (they are all there and unchanged while
/// the call lasts); rows given one by one or by a lazy sequence are read as they come, on the caller's thread (a
/// producer may reuse one object for every row). The batches are handed to the writer
/// (<see cref="BatchOwnership.Writer"/>), which compresses several of them side by side.
/// </summary>
/// <remarks>
/// A row the layout refuses (a decimal that does not fit decimal(38, 18) exactly, a value outside its enum) aborts the
/// output (<see cref="Abort"/>): the rows before it in the open batch are lost, and later calls throw.
/// </remarks>
public sealed class IpcWriter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    /// <summary>The most rows of a record batch.</summary>
    public const int BatchRows = RowBatcher<T>.BatchRows;

    private readonly IpcWriter _writer;
    private readonly RowBatcher<T> _rows;
    private bool _disposed;

    private IpcWriter(IpcWriter writer, IpcWriteOptions? options)
    {
        _writer = writer;
        _rows = new RowBatcher<T>(new Sink(writer), options?.MaxDegreeOfParallelism ?? Environment.ProcessorCount, "Arrow IPC");
    }

    /// <summary>The IPC writer as the batches' sink.</summary>
    private sealed class Sink(IpcWriter writer) : IBatchSink
    {
        public bool IsWritable => writer.IsWritable;

        public bool TakesBatches => true; // the writer owns the batches: several are compressed side by side

        public void Abort() => writer.Abort();

        public async ValueTask WriteAsync(RecordBatch batch, bool useAsync, CancellationToken ct)
        {
            // Not cancelled once handed over: the batch's rows are counted already (a write that gave up waiting
            // would drop them while the output goes on). Cancellation stops the rows at their source.
            if (useAsync) await writer.WriteAsync(batch, CancellationToken.None).ConfigureAwait(false);
            else writer.Write(batch);
        }
    }

    /// <summary>Creates a writer over a stream (nothing is written before the first batch, or <see cref="Complete"/>).</summary>
    public static IpcWriter<T> Create(Stream stream, IpcWriteOptions? options = null) =>
        new(IpcWriter.Create(stream, ArrowRowBuilder<T>.Schema, Owned(options)), options);

    /// <summary>Creates (or overwrites) a file.</summary>
    public static IpcWriter<T> Create(string path, IpcWriteOptions? options = null) =>
        new(IpcWriter.Create(path, ArrowRowBuilder<T>.Schema, Owned(options)), options);

    /// <summary>The batches are made here and handed over: the writer owns them (whatever the options say).</summary>
    private static IpcWriteOptions Owned(IpcWriteOptions? options) => (options ?? new()) with { BatchOwnership = BatchOwnership.Writer };

    /// <summary>The Arrow schema the rows are written with (<see cref="ArrowRowBuilder{T}.Schema"/>).</summary>
    public Schema Schema => _writer.Schema;

    /// <summary>The rows written so far (those still gathered into the open batch included).</summary>
    public long RowCount => _rows.RowCount;

    /// <summary>The bytes written to the stream so far (the rows of the open batch not included).</summary>
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

    /// <summary>Writes the open batch now, ending a record batch with the rows written so far.</summary>
    public void Flush() => _rows.FlushAsync(false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="Flush"/>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default) => _rows.FlushAsync(true, cancellationToken);

    /// <summary>Writes the rows not written yet and what the format still needs (end-of-stream marker, footer), and flushes the stream.</summary>
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

    /// <inheritdoc cref="IpcWriter.Abort"/>
    public void Abort() => _writer.Abort();

    /// <summary>
    /// Completes the output (the rows not written yet included) unless writing failed or the writer was aborted, and
    /// closes the stream unless asked not to.
    /// </summary>
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
