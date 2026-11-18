using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Kaminari.Sazanami.Data.Building;

namespace Kaminari.Seseragi.Files;

/// <summary>Where a <see cref="RowBatcher{T}"/> hands its batches: a format's batch writer.</summary>
internal interface IBatchSink
{
    /// <summary>Whether batches can still be written (not completed, aborted, failed or disposed).</summary>
    bool IsWritable { get; }

    /// <summary>Gives the output up: it cannot be completed.</summary>
    void Abort();

    /// <summary>Writes a batch (the caller keeps and disposes it).</summary>
    ValueTask WriteAsync(RecordBatch batch, bool useAsync, CancellationToken ct);
}

/// <summary>
/// Rows of <typeparamref name="T"/> gathered into batches of <see cref="BatchRows"/> in native memory
/// (<see cref="ArrowRowBuilder{T}"/>), each handed to a sink and freed; the batches depend on the rows alone, not on how
/// they were passed nor on the threads. The rows of an array or a <see cref="List{T}"/> are turned into batches on up
/// to <c>parallelism</c> threads at once (they are all there and unchanged while the call lasts); rows given one by one
/// or by a lazy sequence are read as they come, on the caller's thread (a producer may reuse one object for every row).
/// A row the layout refuses aborts the sink: the rows before it in the open batch are lost.
/// </summary>
internal sealed class RowBatcher<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(IBatchSink sink, int parallelism, string format)
    : IDisposable
{
    /// <summary>The rows of a batch.</summary>
    public const int BatchRows = 16_384;

    private readonly ArrowRowBuilder<T> _rows = new();
    private readonly System.Collections.Concurrent.ConcurrentBag<ArrowRowBuilder<T>> _builders = []; // for batches made in parallel
    private bool _disposed;

    /// <summary>A run of the rows of an array or a list.</summary>
    private delegate ReadOnlySpan<T> RowSlice(int start, int count);

    /// <summary>The rows taken so far (those still gathered into the open batch included).</summary>
    public long RowCount { get; private set; }

    /// <summary>Whether the open batch holds rows.</summary>
    public bool HasPending => _rows.Count > 0;

    /// <summary>Adds a row.</summary>
    /// <exception cref="ArgumentException"><paramref name="row"/> is null (rows of records cannot be null).</exception>
    public void WriteRow(T row)
    {
        Append(row);
        if (_rows.Count == BatchRows) FlushAsync(false, default).AsTask().GetAwaiter().GetResult();
    }

    /// <summary>Adds rows (an array or a <see cref="List{T}"/> is read as a span).</summary>
    public async ValueTask WriteRowsAsync(IEnumerable<T> rows, bool useAsync, CancellationToken ct)
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

    /// <summary>Adds rows as they come.</summary>
    public async ValueTask WriteRowsAsync(IAsyncEnumerable<T> rows, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(rows);
        await foreach (T row in rows.WithCancellation(ct).ConfigureAwait(false))
        {
            Append(row);
            if (_rows.Count == BatchRows) await FlushAsync(true, ct).ConfigureAwait(false);
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
        if (parallelism > 1 && length >= BatchRows)
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
                if (!sink.IsWritable) ThrowNotWritable();
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
    /// <c>parallelism</c> threads at once and hands them to the sink in order. A refused value aborts the sink (the
    /// first in row order surfaces, as on one thread); the batches made beyond a failure are freed.
    /// </summary>
    private async ValueTask WriteInParallelAsync(RowSlice slice, int start, int end, bool useAsync, CancellationToken ct)
    {
        int batches = (end - start) / BatchRows, next = 0;
        Queue<Task<RecordBatch>> making = new(parallelism);
        try
        {
            while (next < batches || making.Count > 0)
            {
                while (next < batches && making.Count < parallelism)
                {
                    int at = start + next++ * BatchRows;
                    making.Enqueue(Task.Run(() => MakeBatch(slice, at)));
                }
                Task<RecordBatch> oldest = making.Dequeue();
                RecordBatch batch;
                try
                {
                    batch = useAsync ? await oldest.ConfigureAwait(false) : oldest.GetAwaiter().GetResult();
                }
                catch
                {
                    sink.Abort(); // a refused value: the output cannot be completed
                    throw;
                }
                RowCount += BatchRows;
                using (batch) await sink.WriteAsync(batch, useAsync, ct).ConfigureAwait(false);
            }
        }
        catch
        {
            foreach (Task<RecordBatch> task in making)
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
    private RecordBatch MakeBatch(RowSlice slice, int at)
    {
        ArrowRowBuilder<T> builder = _builders.TryTake(out ArrowRowBuilder<T>? spare) ? spare : new();
        try
        {
            builder.AppendRange(slice(at, BatchRows));
            RecordBatch batch = builder.ExportBatch();
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
        if (!sink.IsWritable) ThrowNotWritable();
        if (row is null && ArrowRowBuilder<T>.RowsAreRecords)
            throw new ArgumentException($"Row {RowCount} is null; a {typeof(T).Name} row cannot be null.", nameof(row));
        try
        {
            _rows.Append(row);
        }
        catch
        {
            sink.Abort(); // the open batch holds a partial row: the output cannot be completed
            throw;
        }
        RowCount++;
    }

    private void ThrowNotWritable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        throw new InvalidOperationException($"The {format} writer is completed, aborted or failed: no rows can be written.");
    }

    /// <summary>Hands the open batch to the sink.</summary>
    public async ValueTask FlushAsync(bool useAsync, CancellationToken ct)
    {
        if (_rows.Count == 0) return;
        using RecordBatch batch = _rows.ExportBatch();
        await sink.WriteAsync(batch, useAsync, ct).ConfigureAwait(false);
    }

    /// <summary>Frees the builders (rows not flushed are dropped).</summary>
    public void Dispose()
    {
        _disposed = true;
        _rows.Dispose();
        while (_builders.TryTake(out ArrowRowBuilder<T>? builder)) builder.Dispose();
    }
}
