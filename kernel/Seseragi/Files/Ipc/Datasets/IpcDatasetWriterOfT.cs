using System.Diagnostics.CodeAnalysis;
using Kaminari.Sazanami.Data.Building;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>
/// Writes rows of <typeparamref name="T"/> (records, classes, structs, F# records) as a dataset of Arrow IPC files (the
/// file or the stream format) under a path, as <see cref="IpcDatasetWriter"/> writes record batches: Hive partitions by the columns
/// <see cref="DatasetWriteOptions.PartitionBy"/> names (the members' names), files cut by rows and size, published
/// whole, files there already refused, kept or replaced. The columns are laid out as <see cref="IpcWriter{T}"/> lays
/// them out (<see cref="ArrowRowBuilder{T}"/>: <c>DataFrame.From&lt;T&gt;</c>'s layout; dictionaries as lists of key /
/// value structs). The rows are gathered into batches in native memory (of 64Ki rows without partitions, each a record
/// batch of a file; else of 32Ki rows, the rows the dataset groups by their keys at once), each written and freed; the
/// files depend on the rows alone, not on how they were passed nor on the threads. The rows of an array or a
/// <see cref="List{T}"/> are turned into batches on up to <see cref="DatasetWriteOptions.MaxDegreeOfParallelism"/>
/// threads at once; rows given one by one or by a lazy sequence are read as they come, on the caller's thread (a
/// producer may reuse one object for every row). <see cref="IpcDatasetReader{T}"/> reads the rows back (with
/// <see cref="PartitionSchema"/> for the partition columns' types). One thread at a time.
/// </summary>
/// <remarks>
/// A row the layout refuses (a decimal that does not fit decimal(38, 18) exactly, a value outside its enum) gives the
/// write up: the files it published are deleted (those that cannot be are listed by a <see cref="DatasetWriteException"/>
/// whose inner exception is the refusal), and later calls throw. A null row is refused alone (the write goes on).
/// Disposing a writer that did not complete gives the write up, as <see cref="IpcDatasetWriter"/> does.
/// </remarks>
public sealed class IpcDatasetWriter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    private readonly DatasetWriting<DatasetWrittenFile> _writing;
    private readonly DatasetRows<T, DatasetWrittenFile> _rows;

    private IpcDatasetWriter(DatasetWriting<DatasetWrittenFile> writing, DatasetWriteOptions options)
    {
        _writing = writing;
        // Without partitions a batch of IpcFileFormat.BatchRows rows is a record batch as it is (shorter ones are gathered
        // by a copy). Measured (10M rows, 2026-11-20; batches of 16Ki / 32Ki / 64Ki rows): unpartitioned 160 / 156 /
        // 133 ms, 30 partitions 267 / 267 / 279.
        _rows = new DatasetRows<T, DatasetWrittenFile>(writing, options, "Arrow IPC dataset",
            options.PartitionBy is null or { Count: 0 } ? IpcFileFormat.BatchRows : DatasetWriting<DatasetWrittenFile>.ChunkRows);
    }

    /// <summary>Starts a write of a dataset under <paramref name="path"/> (a path of <see cref="DatasetWriteOptions.Store"/>).</summary>
    /// <exception cref="IOException">Files are there already (<see cref="ExistingData.Error"/>).</exception>
    /// <exception cref="ArgumentException">A partition column is not a column of <typeparamref name="T"/>, or of a type partition paths cannot hold.</exception>
    public static IpcDatasetWriter<T> Create(string path, IpcDatasetWriteOptions? options = null) =>
        CreateAsync(path, options).GetAwaiter().GetResult();

    /// <inheritdoc cref="Create"/>
    public static async Task<IpcDatasetWriter<T>> CreateAsync(string path, IpcDatasetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new IpcDatasetWriteOptions();
        DatasetWriting<DatasetWrittenFile> writing = await IpcDatasetWriter.StartAsync(path, ArrowRowBuilder<T>.Schema, options, cancellationToken)
            .ConfigureAwait(false);
        return new IpcDatasetWriter<T>(writing, options);
    }

    /// <summary>The Arrow schema the rows are written with (<see cref="ArrowRowBuilder{T}.Schema"/>).</summary>
    public Apache.Arrow.Schema Schema => _writing.Schema;

    /// <summary>
    /// The partition columns' types, for reading the dataset back (<see cref="DatasetOptions.PartitionSchema"/>): the
    /// paths hold text alone. An enum's column is its member names (strings). Null without partitions.
    /// </summary>
    public Apache.Arrow.Schema? PartitionSchema => _rows.PartitionSchema;

    /// <summary>The write's id (in the files' default names).</summary>
    public string WriteId => _writing.WriteId;

    /// <summary>The rows written so far (those still gathered into the open batch included).</summary>
    public long RowCount => _rows.RowCount;

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

    /// <summary>
    /// Completes the write: the rows not written yet written, the files closed and published (then, replacing data, the
    /// old files deleted).
    /// </summary>
    public DatasetWriteResult<DatasetWrittenFile> Complete() => _rows.Complete();

    /// <inheritdoc cref="Complete"/>
    public Task<DatasetWriteResult<DatasetWrittenFile>> CompleteAsync(CancellationToken cancellationToken = default) => _rows.CompleteAsync(cancellationToken);

    /// <summary>Gives the write up: nothing of it stays (files that cannot be deleted are listed by a <see cref="DatasetWriteException"/>).</summary>
    public void Abort() => _rows.Abort();

    /// <summary>Gives the write up unless it completed.</summary>
    public void Dispose() => _rows.Dispose();

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
