using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// How a dataset is written, whatever the format of its files (the writers of each format take options deriving from
/// these): where the files go, what happens to files there already, how files are cut and named, the threads and the
/// memory the write takes.
/// </summary>
public abstract record DatasetWriteOptions
{
    /// <summary>
    /// The store the files are written to; null for the local file system (a <see cref="LocalFileStore"/>: relative
    /// paths from the current directory). The dataset's path is a path of this store.
    /// </summary>
    public IWritableFileStore? Store { get; init; }

    /// <summary>
    /// The columns whose values split the rows into Hive partitions: a directory <c>key=value/</c> per column below the
    /// dataset's path, in this order, values rendered as dataframe engines render them (escaped as Hive and Spark escape
    /// them; a null is <c>__HIVE_DEFAULT_PARTITION__</c>, an empty string an empty value). Integers, floats, decimals,
    /// booleans, strings, dates, times, timestamps and dictionaries of them. Null or empty: no partitions.
    /// </summary>
    public IReadOnlyList<string>? PartitionBy { get; init; }

    /// <summary>Whether the partition columns are written into the files too (off by default: their values are in the paths, as Hive, Spark and pyarrow write them).</summary>
    public bool IncludePartitionColumns { get; init; }

    /// <summary>What happens to files under the dataset's path already (<see cref="ExistingData.Error"/> by default).</summary>
    public ExistingData ExistingData { get; init; } = ExistingData.Error;

    /// <summary>The rows a file holds at most (a file full, the next one opens); null for no limit.</summary>
    public long? MaxRowsPerFile { get; init; }

    /// <summary>
    /// The size on disk a file closes at (its rows cut to fit, about: the size of the rows to come is estimated from
    /// those written); null for no limit.
    /// </summary>
    public long? ApproxBytesPerFile { get; init; }

    /// <summary>
    /// Names the files (a file name: no directory); null for <c>part-00000-&lt;write id&gt;.parquet</c> and so on, the
    /// index of the file in its directory with five digits at least (files of one write sort in the order written),
    /// the write's id (<see cref="Guid"/>) apart from any other write's. A name taken already fails the write.
    /// </summary>
    public Func<DatasetFileName, string>? FileName { get; init; }

    /// <summary>The threads the files are encoded on (all cores by default).</summary>
    public int MaxDegreeOfParallelism { get; init; } = Environment.ProcessorCount;

    /// <summary>
    /// The bytes the open files may hold in memory together (pages not written yet, writers' scratch, what the store's
    /// streams buffer: a cloud upload's parts, files being published); beyond, files being published are waited for,
    /// then the largest files' pending pages written out early, then the largest files closed early. Null for an eighth
    /// of the memory the process may use (as the runtime sees it: a container's limit), at least 256 MiB.
    /// </summary>
    public long? MemoryBudget { get; init; }

    /// <summary>Whether a local file is synced to disk before it takes its name (off by default, as dataframe engines and pyarrow write).</summary>
    public bool SyncOnCommit { get; init; }

    internal void Validate()
    {
        if (!Enum.IsDefined(ExistingData)) throw new ArgumentOutOfRangeException(nameof(ExistingData), ExistingData, "Unknown handling of existing data.");
        if (MaxRowsPerFile is <= 0) throw new ArgumentOutOfRangeException(nameof(MaxRowsPerFile), MaxRowsPerFile, "The rows per file must be positive.");
        if (ApproxBytesPerFile is <= 0) throw new ArgumentOutOfRangeException(nameof(ApproxBytesPerFile), ApproxBytesPerFile, "The bytes per file must be positive.");
        if (MaxDegreeOfParallelism <= 0) throw new ArgumentOutOfRangeException(nameof(MaxDegreeOfParallelism), MaxDegreeOfParallelism, "The degree of parallelism must be positive.");
        if (MemoryBudget is <= 0) throw new ArgumentOutOfRangeException(nameof(MemoryBudget), MemoryBudget, "The memory budget must be positive.");
    }
}

/// <summary>What a dataset write does with files under the dataset's path already.</summary>
public enum ExistingData
{
    /// <summary>The write fails when any file is there (a store that cannot list cannot be checked: the write fails too).</summary>
    Error,

    /// <summary>The files are added beside those there (their names apart, by the write's id).</summary>
    Append,

    /// <summary>
    /// The files there in the partitions written are deleted once every file of the write is published (a failed write
    /// leaves them): every file under each partition's directory, at any depth (without partitions, under the
    /// dataset's path); the other partitions are left as they are.
    /// </summary>
    ReplacePartitions,
}

/// <summary>What names a file of a dataset write (<see cref="DatasetWriteOptions.FileName"/>).</summary>
/// <param name="WriteId">The write's id.</param>
/// <param name="Index">The file's index in its directory (0, 1, … in the order the files open).</param>
/// <param name="Extension">The format's extension, with its dot (<c>.parquet</c>, <c>.arrow</c>, <c>.arrows</c>).</param>
public readonly record struct DatasetFileName(string WriteId, long Index, string Extension);

/// <summary>A file a dataset write published.</summary>
/// <param name="Path">Its path in the store.</param>
/// <param name="Rows">Its rows.</param>
/// <param name="Bytes">Its size.</param>
/// <param name="PartitionValues">Its partition's values by column, as rendered in its path (unescaped; null for a null); empty without partitions.</param>
public record DatasetWrittenFile(string Path, long Rows, long Bytes, IReadOnlyDictionary<string, string?> PartitionValues);

/// <summary>A dataset write's files, in the order they were written (partition by partition).</summary>
/// <param name="WriteId">The write's id (in the files' default names).</param>
/// <param name="Files">The files published.</param>
public sealed record DatasetWriteResult<TFile>(string WriteId, IReadOnlyList<TFile> Files) where TFile : DatasetWrittenFile;

/// <summary>
/// A dataset write failed and could not undo all it did: <see cref="Remaining"/> lists the files left behind (those it
/// published, or, when it was replacing data, old files it could not delete). The cause is the inner exception.
/// </summary>
public sealed class DatasetWriteException(string message, IReadOnlyList<string> remaining, Exception? innerException)
    : IOException(message, innerException)
{
    /// <summary>The files left behind.</summary>
    public IReadOnlyList<string> Remaining { get; } = remaining;
}
