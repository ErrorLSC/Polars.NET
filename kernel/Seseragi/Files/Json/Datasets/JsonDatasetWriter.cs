using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>
/// How a dataset of JSON files is written: the dataset's options, and those of each file. The partition columns are
/// written into the files too by default (<see cref="DatasetWriteOptions.IncludePartitionColumns"/>): dataframe engines
/// that read JSON without Hive partitions (Polars) then keep them; every engine reads them either way.
/// </summary>
public sealed record JsonDatasetWriteOptions : DatasetWriteOptions
{
    /// <summary>The options of a dataset write of JSON files, the partition columns written into the files.</summary>
    public JsonDatasetWriteOptions() => IncludePartitionColumns = true;

    /// <summary>
    /// The options of each file (lines or documents, compression). The dataset decides
    /// <see cref="JsonWriteOptions.MaxDegreeOfParallelism"/> (<see cref="DatasetWriteOptions.MaxDegreeOfParallelism"/>)
    /// and <see cref="JsonWriteOptions.LeaveOpen"/>; <see cref="JsonWriteOptions.BatchOwnership"/> stays
    /// <see cref="BatchOwnership.Caller"/> (the files take rows of the caller's batches).
    /// </summary>
    public JsonWriteOptions Writer { get; init; } = new();
}

/// <summary>
/// Writes record batches as a dataset of JSON files under a path (a directory, or a prefix of a store): newline-delimited
/// (<c>.jsonl</c>) or documents (<c>.json</c>), gzip or Zstandard compressed (<c>.gz</c>, <c>.zst</c>) as
/// <see cref="JsonDatasetWriteOptions.Writer"/> says; Hive partitions by the columns
/// <see cref="DatasetWriteOptions.PartitionBy"/> names (<c>key=value</c> directories, the columns kept in the files
/// unless <see cref="DatasetWriteOptions.IncludePartitionColumns"/> is turned off); files cut by rows and size, named apart from any
/// other write's, published whole. Files there already are refused, kept or replaced
/// (<see cref="DatasetWriteOptions.ExistingData"/>). A failed write deletes what it published
/// (<see cref="DatasetWriteException"/> lists what it could not). One thread at a time.
/// </summary>
/// <remarks>
/// Batches are the caller's: each may be disposed once <see cref="Write(RecordBatch)"/> returns. Each file's rows are
/// written as <see cref="JsonWriter"/> writes them. The asynchronous methods run the work on the thread pool.
/// </remarks>
public sealed class JsonDatasetWriter : IDisposable
{
    private readonly DatasetWriting<DatasetWrittenFile> _writing;

    private JsonDatasetWriter(DatasetWriting<DatasetWrittenFile> writing) => _writing = writing;

    /// <summary>Starts a write of a dataset under <paramref name="path"/> (a path of <see cref="DatasetWriteOptions.Store"/>).</summary>
    /// <exception cref="IOException">Files are there already (<see cref="ExistingData.Error"/>).</exception>
    public static JsonDatasetWriter Create(string path, Schema schema, JsonDatasetWriteOptions? options = null) =>
        CreateAsync(path, schema, options).GetAwaiter().GetResult();

    /// <inheritdoc cref="Create"/>
    public static async Task<JsonDatasetWriter> CreateAsync(string path, Schema schema, JsonDatasetWriteOptions? options = null,
        CancellationToken cancellationToken = default)
        => new(await StartAsync(path, schema, options ?? new JsonDatasetWriteOptions(), cancellationToken).ConfigureAwait(false));

    /// <summary>Starts the dataset write of JSON files (the typed writer's too).</summary>
    internal static async Task<DatasetWriting<DatasetWrittenFile>> StartAsync(string path, Schema schema, JsonDatasetWriteOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options.Writer, nameof(options.Writer));
        options.Writer.Validate();
        if (options.Writer.BatchOwnership != BatchOwnership.Caller)
            throw new ArgumentException("The files of a dataset take rows of the caller's batches: leave Writer.BatchOwnership as Caller.", nameof(options));
        return await DatasetWriting<DatasetWrittenFile>.CreateAsync(path, schema, options, new JsonFileFormat(options.Writer, schema),
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
    public static DatasetWriteResult<DatasetWrittenFile> Write(string path, Schema schema, IEnumerable<RecordBatch> batches, JsonDatasetWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(batches);
        using JsonDatasetWriter writer = Create(path, schema, options);
        foreach (RecordBatch batch in batches) writer.Write(batch);
        return writer.Complete();
    }

    /// <inheritdoc cref="Write(string, Schema, IEnumerable{RecordBatch}, JsonDatasetWriteOptions?)"/>
    public static async Task<DatasetWriteResult<DatasetWrittenFile>> WriteAsync(string path, Schema schema, IAsyncEnumerable<RecordBatch> batches,
        JsonDatasetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(batches);
        using JsonDatasetWriter writer = await CreateAsync(path, schema, options, cancellationToken).ConfigureAwait(false);
        await foreach (RecordBatch batch in batches.WithCancellation(cancellationToken).ConfigureAwait(false))
            await writer.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
        return await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Writes rows of <typeparamref name="T"/> as a dataset under <paramref name="path"/>, in one call (<see cref="JsonDatasetWriter{T}"/>).</summary>
    public static DatasetWriteResult<DatasetWrittenFile> WriteRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string path,
        IEnumerable<T> rows, JsonDatasetWriteOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(rows);
        using JsonDatasetWriter<T> writer = JsonDatasetWriter<T>.Create(path, options);
        writer.WriteRows(rows);
        return writer.Complete();
    }

    /// <inheritdoc cref="WriteRows{T}(string, IEnumerable{T}, JsonDatasetWriteOptions?)"/>
    public static async Task<DatasetWriteResult<DatasetWrittenFile>> WriteRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(
        string path, IEnumerable<T> rows, JsonDatasetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        await using JsonDatasetWriter<T> writer = await JsonDatasetWriter<T>.CreateAsync(path, options, cancellationToken).ConfigureAwait(false);
        await writer.WriteRowsAsync(rows, cancellationToken).ConfigureAwait(false);
        return await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc cref="WriteRows{T}(string, IEnumerable{T}, JsonDatasetWriteOptions?)"/>
    public static async Task<DatasetWriteResult<DatasetWrittenFile>> WriteRowsAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(
        string path, IAsyncEnumerable<T> rows, JsonDatasetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        await using JsonDatasetWriter<T> writer = await JsonDatasetWriter<T>.CreateAsync(path, options, cancellationToken).ConfigureAwait(false);
        await writer.WriteRowsAsync(rows, cancellationToken).ConfigureAwait(false);
        return await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }
}
