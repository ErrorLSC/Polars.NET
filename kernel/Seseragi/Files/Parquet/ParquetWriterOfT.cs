using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Sazanami.Data.Mapping;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Writes rows of <typeparamref name="T"/> (records, classes, structs, F# records) as a Parquet file: one column per
/// public readable property, laid out as <c>DataFrame.From&lt;T&gt;</c> lays them out (<see cref="ArrowRowBuilder{T}"/>:
/// strings as string views, collections as large lists, nested records as structs, enums as dictionaries of their
/// member names, decimals as decimal(38, 18), ...; dictionaries as Parquet MAP groups); any other <typeparamref name="T"/> as one column named
/// <c>value</c>. <see cref="ParquetReader{T}"/> reads the rows back. The rows are gathered into batches of
/// <see cref="BatchRows"/> in native memory, each written by a <see cref="ParquetWriter"/> and freed (the caller owning
/// the batches, whatever <see cref="ParquetWriteOptions.BatchOwnership"/> says: measured faster, and lighter, than the
/// writer owning them); the file depends on the rows alone, not on how they were passed nor on the threads. The rows of an array or a <see cref="List{T}"/> given to <see cref="WriteRows"/> are turned into batches
/// on up to <see cref="ParquetWriteOptions.MaxDegreeOfParallelism"/> threads at once (they are all there and unchanged
/// while the call lasts); rows given one by one or by a lazy sequence are read as they come, on the caller's thread
/// (a producer may reuse one object for every row).
/// </summary>
/// <remarks>
/// A row the layout refuses (a decimal that does not fit decimal(38, 18) exactly, a value outside its enum) aborts the
/// file (<see cref="Abort"/>): the rows before it in the open batch are lost, and later calls throw.
/// </remarks>
public sealed class ParquetWriter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    /// <summary>The rows of a batch: the writer's segment, so each batch is encoded as one.</summary>
    public const int BatchRows = RowBatcher<T>.BatchRows;

    private readonly ParquetWriter _writer;
    private readonly RowBatcher<T> _rows;
    private bool _disposed;

    private ParquetWriter(ParquetWriter writer, ParquetWriteOptions? options)
    {
        _writer = writer;
        _rows = new RowBatcher<T>(new Sink(writer), options?.MaxDegreeOfParallelism ?? Environment.ProcessorCount, "Parquet");
    }

    /// <summary>The Parquet writer as the batches' sink.</summary>
    private sealed class Sink(ParquetWriter writer) : IBatchSink
    {
        public bool IsWritable => writer.IsWritable;

        public void Abort() => writer.Abort();

        public async ValueTask WriteAsync(Apache.Arrow.RecordBatch batch, bool useAsync, CancellationToken ct)
        {
            if (useAsync) await writer.WriteAsync(batch, ct).ConfigureAwait(false);
            else writer.Write(batch);
        }
    }

    /// <summary>Creates a writer over a stream. Nothing is written before the first row group closes (or the file completes).</summary>
    public static ParquetWriter<T> Create(Stream stream, ParquetWriteOptions? options = null) =>
        new(ParquetWriter.Create(stream, ArrowRowBuilder<T>.Schema, Typed(options)), options);

    /// <summary>Creates (or overwrites) a file.</summary>
    public static ParquetWriter<T> Create(string path, ParquetWriteOptions? options = null) =>
        new(ParquetWriter.Create(path, ArrowRowBuilder<T>.Schema, Typed(options)), options);

    private static ParquetWriteOptions Typed(ParquetWriteOptions? options)
    {
        options ??= new();
        // Dictionaries as Parquet MAP groups (their entries are lists of Key / Value structs in Arrow).
        IReadOnlySet<string> maps = ArrowRowBuilder<T>.MapColumns;
        return options with
        {
            BatchOwnership = BatchOwnership.Caller,
            MapColumns = maps.Count == 0 ? options.MapColumns : options.MapColumns is { } given ? given.Union(maps).ToFrozenSet() : maps,
        };
    }

    /// <summary>The Arrow schema the rows are written with (<see cref="ArrowRowBuilder{T}.Schema"/>).</summary>
    public Apache.Arrow.Schema Schema => _writer.Schema;

    /// <summary>The rows written so far (those still gathered into the open batch included).</summary>
    public long RowCount => _rows.RowCount;

    /// <inheritdoc cref="ParquetWriter.BytesWritten"/>
    public long BytesWritten => _writer.BytesWritten;

    /// <summary><inheritdoc cref="ParquetWriter.PendingBytes"/> The rows of the open batch (fewer than <see cref="BatchRows"/>) are not counted.</summary>
    public long PendingBytes => _writer.PendingBytes;

    /// <inheritdoc cref="ParquetWriter.Metadata"/>
    public ParquetMetadata Metadata => _writer.Metadata;

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

    private ValueTask FlushAsync(bool useAsync, CancellationToken ct) => _rows.FlushAsync(useAsync, ct);

    /// <summary>Closes the open row group now, with the rows written so far (<see cref="ParquetWriter.EndRowGroup"/>).</summary>
    public void EndRowGroup()
    {
        FlushAsync(false, default).AsTask().GetAwaiter().GetResult();
        _writer.EndRowGroup();
    }

    /// <inheritdoc cref="EndRowGroup"/>
    public async ValueTask EndRowGroupAsync(CancellationToken cancellationToken = default)
    {
        await FlushAsync(true, cancellationToken).ConfigureAwait(false);
        await _writer.EndRowGroupAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes the rows not written yet, the last row group and the footer, and flushes the stream.</summary>
    public void Complete()
    {
        FlushAsync(false, default).AsTask().GetAwaiter().GetResult();
        _writer.Complete();
    }

    /// <inheritdoc cref="Complete"/>
    public async ValueTask CompleteAsync(CancellationToken cancellationToken = default)
    {
        await FlushAsync(true, cancellationToken).ConfigureAwait(false);
        await _writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="ParquetWriter.Abort"/>
    public void Abort() => _writer.Abort();

    /// <summary>
    /// Completes the file (the rows not written yet included) unless writing failed or the writer was aborted, and
    /// closes the stream unless asked not to.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        try
        {
            if (_writer.IsWritable) FlushAsync(false, default).AsTask().GetAwaiter().GetResult();
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
            if (_writer.IsWritable) await FlushAsync(true, default).ConfigureAwait(false);
        }
        finally
        {
            await _writer.DisposeAsync().ConfigureAwait(false);
            _rows.Dispose();
        }
    }
}
