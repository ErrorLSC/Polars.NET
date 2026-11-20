namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>How a dataset of Parquet files is written: the dataset's options, and those of each file.</summary>
public sealed record ParquetDatasetWriteOptions : DatasetWriteOptions
{
    /// <summary>
    /// The options of each file (compression, row groups, pages, encodings, statistics, metadata). The dataset decides
    /// <see cref="ParquetWriteOptions.MaxDegreeOfParallelism"/> (<see cref="DatasetWriteOptions.MaxDegreeOfParallelism"/>)
    /// and <see cref="ParquetWriteOptions.LeaveOpen"/>; <see cref="ParquetWriteOptions.BatchOwnership"/> stays
    /// <see cref="BatchOwnership.Caller"/> (the files take rows of the caller's batches).
    /// </summary>
    public ParquetWriteOptions Writer { get; init; } = new();
}

/// <summary>A Parquet file a dataset write published, with its footer.</summary>
/// <param name="Path">Its path in the store.</param>
/// <param name="Rows">Its rows.</param>
/// <param name="Bytes">Its size.</param>
/// <param name="PartitionValues">Its partition's values by column, as rendered in its path; empty without partitions.</param>
/// <param name="Metadata">Its footer, as written.</param>
public sealed record ParquetWrittenFile(string Path, long Rows, long Bytes, IReadOnlyDictionary<string, string?> PartitionValues, ParquetMetadata Metadata)
    : DatasetWrittenFile(Path, Rows, Bytes, PartitionValues);

/// <summary>
/// Writes record batches as a dataset of Parquet files under a path (a directory, or a prefix of a store): files cut by
/// rows and size, named apart from any other write's, published whole (a file appears with its footer or not at all).
/// Files there already are refused, kept or replaced (<see cref="DatasetWriteOptions.ExistingData"/>). A failed write
/// deletes what it published (<see cref="DatasetWriteException"/> lists what it could not). One thread at a time.
/// </summary>
/// <remarks>
/// Batches are the caller's: each may be disposed once <see cref="Write(Apache.Arrow.RecordBatch)"/> returns. The asynchronous methods run the
/// work (encoding, the store's writes) on the thread pool.
/// </remarks>
public sealed class ParquetDatasetWriter : IDisposable
{
    private readonly DatasetWriting<ParquetWrittenFile> _writing;

    private ParquetDatasetWriter(DatasetWriting<ParquetWrittenFile> writing) => _writing = writing;

    /// <summary>Starts a write of a dataset under <paramref name="path"/> (a path of <see cref="DatasetWriteOptions.Store"/>).</summary>
    /// <exception cref="IOException">Files are there already (<see cref="ExistingData.Error"/>).</exception>
    public static ParquetDatasetWriter Create(string path, Apache.Arrow.Schema schema, ParquetDatasetWriteOptions? options = null) =>
        CreateAsync(path, schema, options).GetAwaiter().GetResult();

    /// <inheritdoc cref="Create"/>
    public static async Task<ParquetDatasetWriter> CreateAsync(string path, Apache.Arrow.Schema schema, ParquetDatasetWriteOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new ParquetDatasetWriteOptions();
        ArgumentNullException.ThrowIfNull(options.Writer, nameof(options.Writer));
        options.Writer.Validate();
        if (options.Writer.BatchOwnership != BatchOwnership.Caller)
            throw new ArgumentException("The files of a dataset take rows of the caller's batches: leave Writer.BatchOwnership as Caller.", nameof(options));
        return new ParquetDatasetWriter(await DatasetWriting<ParquetWrittenFile>.CreateAsync(path, schema, options, new ParquetFileFormat(options.Writer),
            static f => new ParquetWrittenFile(f.Path, f.Rows, f.Bytes, f.Partition.Values, (ParquetMetadata)f.Footer!), cancellationToken).ConfigureAwait(false));
    }

    /// <summary>The schema of the batches written.</summary>
    public Apache.Arrow.Schema Schema => _writing.Schema;

    /// <summary>The write's id (in the files' default names).</summary>
    public string WriteId => _writing.WriteId;

    /// <summary>Writes a batch's rows (the caller keeps the batch).</summary>
    /// <exception cref="ArgumentException">The batch's columns are not the schema's (nothing is written; the write goes on).</exception>
    public void Write(Apache.Arrow.RecordBatch batch) => _writing.Write(batch);

    /// <inheritdoc cref="Write(Apache.Arrow.RecordBatch)"/>
    public Task WriteAsync(Apache.Arrow.RecordBatch batch, CancellationToken cancellationToken = default) =>
        Task.Run(() => _writing.Write(batch), cancellationToken);

    /// <summary>Completes the write: the files closed and published (then, replacing data, the old files deleted).</summary>
    public DatasetWriteResult<ParquetWrittenFile> Complete() => _writing.Complete();

    /// <inheritdoc cref="Complete"/>
    public Task<DatasetWriteResult<ParquetWrittenFile>> CompleteAsync(CancellationToken cancellationToken = default) =>
        Task.Run(_writing.Complete, cancellationToken);

    /// <summary>Gives the write up: nothing of it stays (files that cannot be deleted are listed by a <see cref="DatasetWriteException"/>).</summary>
    public void Abort() => _writing.Abort();

    /// <summary>Gives the write up unless it completed.</summary>
    public void Dispose() => _writing.Dispose();

    /// <summary>Writes the batches as a dataset under <paramref name="path"/>, in one call.</summary>
    public static DatasetWriteResult<ParquetWrittenFile> Write(string path, Apache.Arrow.Schema schema, IEnumerable<Apache.Arrow.RecordBatch> batches,
        ParquetDatasetWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(batches);
        using ParquetDatasetWriter writer = Create(path, schema, options);
        foreach (Apache.Arrow.RecordBatch batch in batches) writer.Write(batch);
        return writer.Complete();
    }

    /// <inheritdoc cref="Write(string, Apache.Arrow.Schema, IEnumerable{Apache.Arrow.RecordBatch}, ParquetDatasetWriteOptions?)"/>
    public static async Task<DatasetWriteResult<ParquetWrittenFile>> WriteAsync(string path, Apache.Arrow.Schema schema,
        IAsyncEnumerable<Apache.Arrow.RecordBatch> batches, ParquetDatasetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batches);
        using ParquetDatasetWriter writer = await CreateAsync(path, schema, options, cancellationToken).ConfigureAwait(false);
        await foreach (Apache.Arrow.RecordBatch batch in batches.WithCancellation(cancellationToken).ConfigureAwait(false))
            await writer.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
        return await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }
}
