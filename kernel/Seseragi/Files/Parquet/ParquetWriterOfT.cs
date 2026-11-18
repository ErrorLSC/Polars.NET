using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Kaminari.Sazanami.Data;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Writes rows of <typeparamref name="T"/> (records, classes, structs, F# records) as a Parquet file: one column per
/// public readable property, laid out as <c>DataFrame.From&lt;T&gt;</c> lays them out (<see cref="ArrowRowBuilder{T}"/>:
/// strings as string views, collections as large lists, nested records as structs, enums as dictionaries of their
/// member names, decimals as decimal(38, 18), ...); any other <typeparamref name="T"/> as one column named
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
    public const int BatchRows = 16_384;

    private readonly ParquetWriter _writer;
    private readonly ArrowRowBuilder<T> _rows = new();
    private readonly System.Collections.Concurrent.ConcurrentBag<ArrowRowBuilder<T>> _builders = []; // for batches made in parallel
    private readonly int _parallel;
    private long _rowCount;
    private bool _disposed;

    private ParquetWriter(ParquetWriter writer, ParquetWriteOptions? options)
    {
        _writer = writer;
        _parallel = options?.MaxDegreeOfParallelism ?? Environment.ProcessorCount;
    }

    /// <summary>Creates a writer over a stream. Nothing is written before the first row group closes (or the file completes).</summary>
    public static ParquetWriter<T> Create(Stream stream, ParquetWriteOptions? options = null) =>
        new(ParquetWriter.Create(stream, ArrowRowBuilder<T>.Schema, Typed(options)), options);

    /// <summary>Creates (or overwrites) a file.</summary>
    public static ParquetWriter<T> Create(string path, ParquetWriteOptions? options = null) =>
        new(ParquetWriter.Create(path, ArrowRowBuilder<T>.Schema, Typed(options)), options);

    /// <summary>A run of the rows of an array or a list.</summary>
    private delegate ReadOnlySpan<T> RowSlice(int start, int count);

    private static ParquetWriteOptions Typed(ParquetWriteOptions? options) =>
        (options ?? new()) with { BatchOwnership = ParquetBatchOwnership.Caller };

    /// <summary>The Arrow schema the rows are written with (<see cref="ArrowRowBuilder{T}.Schema"/>).</summary>
    public Apache.Arrow.Schema Schema => _writer.Schema;

    /// <summary>The rows written so far (those still gathered into the open batch included).</summary>
    public long RowCount => _rowCount;

    /// <inheritdoc cref="ParquetWriter.BytesWritten"/>
    public long BytesWritten => _writer.BytesWritten;

    /// <summary><inheritdoc cref="ParquetWriter.PendingBytes"/> The rows of the open batch (fewer than <see cref="BatchRows"/>) are not counted.</summary>
    public long PendingBytes => _writer.PendingBytes;

    /// <inheritdoc cref="ParquetWriter.Metadata"/>
    public ParquetMetadata Metadata => _writer.Metadata;

    /// <summary>Adds a row.</summary>
    /// <exception cref="ArgumentException"><paramref name="row"/> is null (rows of records cannot be null).</exception>
    public void WriteRow(T row)
    {
        Append(row);
        if (_rows.Count == BatchRows) FlushAsync(false, default).AsTask().GetAwaiter().GetResult();
    }

    /// <summary>Adds rows (an array or a <see cref="List{T}"/> is read as a span).</summary>
    /// <inheritdoc cref="WriteRow"/>
    public void WriteRows(IEnumerable<T> rows) => WriteRowsCoreAsync(rows, false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="WriteRows"/>
    public ValueTask WriteRowsAsync(IEnumerable<T> rows, CancellationToken cancellationToken = default) => WriteRowsCoreAsync(rows, true, cancellationToken);

    /// <inheritdoc cref="WriteRows"/>
    public async ValueTask WriteRowsAsync(IAsyncEnumerable<T> rows, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        await foreach (T row in rows.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            Append(row);
            if (_rows.Count == BatchRows) await FlushAsync(true, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask WriteRowsCoreAsync(IEnumerable<T> rows, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(rows);
        switch (rows)
        {
            case T[] array:
                await WriteListedAsync(array.Length, (start, count) => array.AsSpan(start, count), useAsync, ct).ConfigureAwait(false);
                return;
            case List<T> list:
                await WriteListedAsync(list.Count, (start, count) => CollectionsMarshal.AsSpan(list).Slice(start, count), useAsync, ct).ConfigureAwait(false);
                return;
        }
        foreach (T row in rows)
        {
            Append(row);
            if (_rows.Count == BatchRows) await FlushAsync(useAsync, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Writes the rows of an array or a list (read as spans, a batch at a time: a span lives between awaits only). With
    /// several threads, the whole batches after the open one are made in parallel, up to the first null row (met by the
    /// rows after them, on this thread, as one thread meets it); the batches are those one thread makes.
    /// </summary>
    private async ValueTask WriteListedAsync(int length, RowSlice slice, bool useAsync, CancellationToken ct)
    {
        int start = 0;
        if (_parallel > 1 && length >= BatchRows)
        {
            if (_rows.Count > 0)
            {
                start += AppendSpan(slice(0, length));
                if (_rows.Count == BatchRows) await FlushAsync(useAsync, ct).ConfigureAwait(false);
            }
            int end = start + (length - start) / BatchRows * BatchRows;
            if (ArrowRowBuilder<T>.RowsAreRecords && FirstNull(slice(start, end - start)) is int firstNull and >= 0)
                end = start + firstNull / BatchRows * BatchRows;
            if (end > start)
            {
                if (!_writer.IsWritable) ThrowNotWritable();
                await WriteInParallelAsync(slice, start, end, useAsync, ct).ConfigureAwait(false);
                start = end;
            }
        }
        while (start < length)
        {
            start += AppendSpan(slice(start, length - start));
            if (_rows.Count == BatchRows) await FlushAsync(useAsync, ct).ConfigureAwait(false);
        }
    }

    private static int FirstNull(ReadOnlySpan<T> rows)
    {
        for (int i = 0; i < rows.Length; i++)
        {
            if (rows[i] is null) return i;
        }
        return -1;
    }

    /// <summary>
    /// Makes the batches of rows <paramref name="start"/> .. <paramref name="end"/> (whole batches) on up to
    /// <see cref="_parallel"/> threads at once and hands them to the writer in order. A refused value aborts the file
    /// (the first in row order surfaces, as on one thread); the batches made beyond a failure are freed.
    /// </summary>
    private async ValueTask WriteInParallelAsync(RowSlice slice, int start, int end, bool useAsync, CancellationToken ct)
    {
        int batches = (end - start) / BatchRows, next = 0;
        Queue<Task<Apache.Arrow.RecordBatch>> making = new(_parallel);
        try
        {
            while (next < batches || making.Count > 0)
            {
                while (next < batches && making.Count < _parallel)
                {
                    int at = start + next++ * BatchRows;
                    making.Enqueue(Task.Run(() => MakeBatch(slice, at)));
                }
                Task<Apache.Arrow.RecordBatch> oldest = making.Dequeue();
                Apache.Arrow.RecordBatch batch;
                try
                {
                    batch = useAsync ? await oldest.ConfigureAwait(false) : oldest.GetAwaiter().GetResult();
                }
                catch
                {
                    _writer.Abort(); // a refused value: the file cannot be completed
                    throw;
                }
                _rowCount += BatchRows;
                using (batch)
                {
                    if (useAsync) await _writer.WriteAsync(batch, ct).ConfigureAwait(false);
                    else _writer.Write(batch);
                }
            }
        }
        catch
        {
            foreach (Task<Apache.Arrow.RecordBatch> task in making)
            {
                try
                {
                    (useAsync ? await task.ConfigureAwait(false) : task.GetAwaiter().GetResult()).Dispose();
                }
                catch
                {
                    // Already failing: a later batch's own failure is not the one to report.
                }
            }
            throw;
        }
    }

    /// <summary>A batch of rows <paramref name="at"/> .. +<see cref="BatchRows"/>, made by a builder of the pool (a faulted one is dropped).</summary>
    private Apache.Arrow.RecordBatch MakeBatch(RowSlice slice, int at)
    {
        ArrowRowBuilder<T> builder = _builders.TryTake(out ArrowRowBuilder<T>? spare) ? spare : new();
        try
        {
            builder.AppendRange(slice(at, BatchRows));
            Apache.Arrow.RecordBatch batch = builder.ExportBatch();
            _builders.Add(builder);
            return batch;
        }
        catch
        {
            builder.Dispose();
            throw;
        }
    }

    /// <summary>Appends rows from <paramref name="rows"/> until the open batch is full; returns how many.</summary>
    private int AppendSpan(ReadOnlySpan<T> rows)
    {
        int take = Math.Min(rows.Length, BatchRows - _rows.Count);
        foreach (T row in rows[..take]) Append(row);
        return take;
    }

    private void Append(T row)
    {
        if (!_writer.IsWritable) ThrowNotWritable();
        if (row is null && ArrowRowBuilder<T>.RowsAreRecords)
            throw new ArgumentException($"Row {_rowCount} is null; a {typeof(T).Name} row cannot be null.", nameof(row));
        try
        {
            _rows.Append(row);
        }
        catch
        {
            _writer.Abort(); // the open batch holds a partial row: the file cannot be completed
            throw;
        }
        _rowCount++;
    }

    private void ThrowNotWritable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        throw new InvalidOperationException("The Parquet writer is completed, aborted or failed: no rows can be written.");
    }

    /// <summary>Hands the open batch to the writer.</summary>
    private async ValueTask FlushAsync(bool useAsync, CancellationToken ct)
    {
        if (_rows.Count == 0) return;
        using Apache.Arrow.RecordBatch batch = _rows.ExportBatch();
        if (useAsync) await _writer.WriteAsync(batch, ct).ConfigureAwait(false);
        else _writer.Write(batch);
    }

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
            FreeBuilders();
        }
    }

    private void FreeBuilders()
    {
        _rows.Dispose();
        while (_builders.TryTake(out ArrowRowBuilder<T>? builder)) builder.Dispose();
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
            FreeBuilders();
        }
    }
}
