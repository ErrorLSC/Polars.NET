using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;

namespace Kaminari.Seseragi.Files.Ipc;

/// <summary>How a dataset of Arrow IPC files is written: the dataset's options, and those of each file.</summary>
public sealed record IpcDatasetWriteOptions : DatasetWriteOptions
{
    /// <summary>
    /// The options of each file (the file or the stream format, compression, metadata). The dataset decides
    /// <see cref="IpcWriteOptions.MaxDegreeOfParallelism"/> (<see cref="DatasetWriteOptions.MaxDegreeOfParallelism"/>)
    /// and <see cref="IpcWriteOptions.LeaveOpen"/>; <see cref="IpcWriteOptions.BatchOwnership"/> stays
    /// <see cref="BatchOwnership.Caller"/> (the files take rows of the caller's batches).
    /// </summary>
    public IpcWriteOptions Writer { get; init; } = new();
}

/// <summary>
/// Writes record batches as a dataset of Arrow IPC files under a path (a directory, or a prefix of a store): the file
/// format (<c>.arrow</c>) or the stream format (<c>.arrows</c>), files cut by rows and size, named apart from any other
/// write's, published whole. Files there already are refused, kept or replaced
/// (<see cref="DatasetWriteOptions.ExistingData"/>). A failed write deletes what it published
/// (<see cref="DatasetWriteException"/> lists what it could not). One thread at a time.
/// </summary>
/// <remarks>
/// Batches are the caller's: each may be disposed once <see cref="Write(RecordBatch)"/> returns. Each batch (or the part of it a file
/// takes) becomes a record batch of a file. The asynchronous methods run the work on the thread pool.
/// </remarks>
public sealed class IpcDatasetWriter : IDisposable
{
    private readonly DatasetWriting<DatasetWrittenFile> _writing;

    private IpcDatasetWriter(DatasetWriting<DatasetWrittenFile> writing) => _writing = writing;

    /// <summary>Starts a write of a dataset under <paramref name="path"/> (a path of <see cref="DatasetWriteOptions.Store"/>).</summary>
    /// <exception cref="IOException">Files are there already (<see cref="ExistingData.Error"/>).</exception>
    public static IpcDatasetWriter Create(string path, Schema schema, IpcDatasetWriteOptions? options = null) =>
        CreateAsync(path, schema, options).GetAwaiter().GetResult();

    /// <inheritdoc cref="Create"/>
    public static async Task<IpcDatasetWriter> CreateAsync(string path, Schema schema, IpcDatasetWriteOptions? options = null,
        CancellationToken cancellationToken = default)
        => new(await StartAsync(path, schema, options ?? new IpcDatasetWriteOptions(), cancellationToken).ConfigureAwait(false));

    /// <summary>Starts the dataset write of Arrow IPC files (the typed writer's too).</summary>
    internal static async Task<DatasetWriting<DatasetWrittenFile>> StartAsync(string path, Schema schema, IpcDatasetWriteOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options.Writer, nameof(options.Writer));
        options.Writer.Validate();
        if (options.Writer.BatchOwnership != BatchOwnership.Caller)
            throw new ArgumentException("The files of a dataset take rows of the caller's batches: leave Writer.BatchOwnership as Caller.", nameof(options));
        return await DatasetWriting<DatasetWrittenFile>.CreateAsync(path, schema, options, new IpcFileFormat(options.Writer),
            static f => new DatasetWrittenFile(f.Path, f.Rows, f.Bytes, f.Partition.Values), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The schema of the batches written.</summary>
    public Schema Schema => _writing.Schema;

    /// <summary>The write's id (in the files' default names).</summary>
    public string WriteId => _writing.WriteId;

    /// <summary>Writes a batch's rows (the caller keeps the batch).</summary>
    /// <exception cref="ArgumentException">The batch's columns are not the schema's (nothing is written; the write goes on).</exception>
    public void Write(RecordBatch batch) => _writing.Write(batch);

    /// <inheritdoc cref="Write(RecordBatch)"/>
    public Task WriteAsync(RecordBatch batch, CancellationToken cancellationToken = default) => Task.Run(() => _writing.Write(batch), cancellationToken);

    /// <summary>Completes the write: the files closed and published (then, replacing data, the old files deleted).</summary>
    public DatasetWriteResult<DatasetWrittenFile> Complete() => _writing.Complete();

    /// <inheritdoc cref="Complete"/>
    public Task<DatasetWriteResult<DatasetWrittenFile>> CompleteAsync(CancellationToken cancellationToken = default) =>
        Task.Run(_writing.Complete, cancellationToken);

    /// <summary>Gives the write up: nothing of it stays (files that cannot be deleted are listed by a <see cref="DatasetWriteException"/>).</summary>
    public void Abort() => _writing.Abort();

    /// <summary>Gives the write up unless it completed.</summary>
    public void Dispose() => _writing.Dispose();

    /// <summary>Writes the batches as a dataset under <paramref name="path"/>, in one call.</summary>
    public static DatasetWriteResult<DatasetWrittenFile> Write(string path, Schema schema, IEnumerable<RecordBatch> batches, IpcDatasetWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(batches);
        using IpcDatasetWriter writer = Create(path, schema, options);
        foreach (RecordBatch batch in batches) writer.Write(batch);
        return writer.Complete();
    }

    /// <inheritdoc cref="Write(string, Schema, IEnumerable{RecordBatch}, IpcDatasetWriteOptions?)"/>
    public static async Task<DatasetWriteResult<DatasetWrittenFile>> WriteAsync(string path, Schema schema, IAsyncEnumerable<RecordBatch> batches,
        IpcDatasetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batches);
        using IpcDatasetWriter writer = await CreateAsync(path, schema, options, cancellationToken).ConfigureAwait(false);
        await foreach (RecordBatch batch in batches.WithCancellation(cancellationToken).ConfigureAwait(false))
            await writer.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
        return await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes rows of <typeparamref name="T"/> as a dataset under <paramref name="path"/>, in one call (<see cref="IpcDatasetWriter{T}"/>).</summary>
    public static DatasetWriteResult<DatasetWrittenFile> WriteRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path,
        IEnumerable<T> rows, IpcDatasetWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(rows);
        using IpcDatasetWriter<T> writer = IpcDatasetWriter<T>.Create(path, options);
        writer.WriteRows(rows);
        return writer.Complete();
    }

    /// <inheritdoc cref="WriteRows{T}(string, IEnumerable{T}, IpcDatasetWriteOptions?)"/>
    public static async Task<DatasetWriteResult<DatasetWrittenFile>> WriteRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(
        string path, IEnumerable<T> rows, IpcDatasetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        await using IpcDatasetWriter<T> writer = await IpcDatasetWriter<T>.CreateAsync(path, options, cancellationToken).ConfigureAwait(false);
        await writer.WriteRowsAsync(rows, cancellationToken).ConfigureAwait(false);
        return await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="WriteRows{T}(string, IEnumerable{T}, IpcDatasetWriteOptions?)"/>
    public static async Task<DatasetWriteResult<DatasetWrittenFile>> WriteRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(
        string path, IAsyncEnumerable<T> rows, IpcDatasetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        await using IpcDatasetWriter<T> writer = await IpcDatasetWriter<T>.CreateAsync(path, options, cancellationToken).ConfigureAwait(false);
        await writer.WriteRowsAsync(rows, cancellationToken).ConfigureAwait(false);
        return await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }
}
