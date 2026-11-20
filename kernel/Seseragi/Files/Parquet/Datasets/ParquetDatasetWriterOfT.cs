using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using Kaminari.Sazanami.Data.Building;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Writes rows of <typeparamref name="T"/> (records, classes, structs, F# records) as a dataset of Parquet files under a
/// path, as <see cref="ParquetDatasetWriter"/> writes record batches: Hive partitions by the columns
/// <see cref="DatasetWriteOptions.PartitionBy"/> names (the members' names), files cut by rows and size, published
/// whole, files there already refused, kept or replaced. The columns are laid out as <see cref="ParquetWriter{T}"/> lays
/// them out (<see cref="ArrowRowBuilder{T}"/>: <c>DataFrame.From&lt;T&gt;</c>'s layout; dictionaries as Parquet MAP
/// groups). The rows are gathered into batches of 32Ki rows (the rows the dataset groups by their keys at once) in native
/// memory, each written and freed; the files depend on the rows alone, not on how they were passed nor on the threads. The rows of an array or a
/// <see cref="List{T}"/> are turned into batches on up to <see cref="DatasetWriteOptions.MaxDegreeOfParallelism"/>
/// threads at once; rows given one by one or by a lazy sequence are read as they come, on the caller's thread (a
/// producer may reuse one object for every row). <see cref="ParquetDatasetReader{T}"/> reads the rows back (with
/// <see cref="PartitionSchema"/> for the partition columns' types). One thread at a time.
/// </summary>
/// <remarks>
/// A row the layout refuses (a decimal that does not fit decimal(38, 18) exactly, a value outside its enum) gives the
/// write up: the files it published are deleted (those that cannot be are listed by a <see cref="DatasetWriteException"/>
/// whose inner exception is the refusal), and later calls throw. A null row is refused alone (the write goes on).
/// Disposing a writer that did not complete gives the write up, as <see cref="ParquetDatasetWriter"/> does.
/// </remarks>
public sealed class ParquetDatasetWriter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : IDisposable, IAsyncDisposable
{
    private readonly DatasetWriting<ParquetWrittenFile> _writing;
    private readonly DatasetRows<T, ParquetWrittenFile> _rows;

    private ParquetDatasetWriter(DatasetWriting<ParquetWrittenFile> writing, DatasetWriteOptions options)
    {
        _writing = writing;
        // Measured (10M rows, 2026-11-20; batches of 16Ki / 32Ki / 64Ki rows): unpartitioned 173 / 174 / 192 ms, 30
        // partitions 320 / 324 / 340, 1000 partitions 744 / 637 / 601.
        _rows = new DatasetRows<T, ParquetWrittenFile>(writing, options, "Parquet dataset");
    }

    /// <summary>Starts a write of a dataset under <paramref name="path"/> (a path of <see cref="DatasetWriteOptions.Store"/>).</summary>
    /// <exception cref="IOException">Files are there already (<see cref="ExistingData.Error"/>).</exception>
    /// <exception cref="ArgumentException">A partition column is not a column of <typeparamref name="T"/>, or of a type partition paths cannot hold.</exception>
    public static ParquetDatasetWriter<T> Create(string path, ParquetDatasetWriteOptions? options = null) =>
        CreateAsync(path, options).GetAwaiter().GetResult();

    /// <inheritdoc cref="Create"/>
    public static async Task<ParquetDatasetWriter<T>> CreateAsync(string path, ParquetDatasetWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new ParquetDatasetWriteOptions();
        ArgumentNullException.ThrowIfNull(options.Writer, nameof(options.Writer));
        // Dictionaries as Parquet MAP groups (their entries are lists of Key / Value structs in Arrow).
        IReadOnlySet<string> maps = ArrowRowBuilder<T>.MapColumns;
        if (maps.Count > 0)
        {
            options = options with
            {
                Writer = options.Writer with { MapColumns = options.Writer.MapColumns is { } given ? given.Union(maps).ToFrozenSet() : maps },
            };
        }
        DatasetWriting<ParquetWrittenFile> writing = await ParquetDatasetWriter.StartAsync(path, ArrowRowBuilder<T>.Schema, options, cancellationToken)
            .ConfigureAwait(false);
        return new ParquetDatasetWriter<T>(writing, options);
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
    public DatasetWriteResult<ParquetWrittenFile> Complete() => _rows.Complete();

    /// <inheritdoc cref="Complete"/>
    public Task<DatasetWriteResult<ParquetWrittenFile>> CompleteAsync(CancellationToken cancellationToken = default) => _rows.CompleteAsync(cancellationToken);

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
