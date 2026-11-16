using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>How <see cref="ParquetDataset.Open(string, ParquetDatasetOptions?)"/> finds and describes a dataset's files.</summary>
public sealed record ParquetDatasetOptions
{
    /// <summary>
    /// Where the files are; null for the local file system (a path naming a URL, <c>scheme://…</c>, then needs a store).
    /// </summary>
    public IFileStore? Store { get; init; }

    /// <summary>
    /// File names starting with one of these are left out of listings and globs (Spark's <c>_SUCCESS</c>, temporary
    /// files); null leaves nothing out: hidden files and files starting with '_' are read, as dataframe engines do.
    /// </summary>
    public IReadOnlyList<string>? IgnorePrefixes { get; init; }

    /// <summary>The name of a string column appended to the batches, holding each row's file path (as listed); null adds none.</summary>
    public string? FilePathColumn { get; init; }

    /// <summary>
    /// Whether paths with <c>*</c>, <c>?</c> or <c>[</c> are globs (the default); false reads them as names. HTTP(S)
    /// URLs are never globs.
    /// </summary>
    public bool Glob { get; init; } = true;

    /// <summary>
    /// Whether the <c>key=value</c> directories of the paths give partition columns (<see cref="HivePartitioning"/>;
    /// by default only for a directory given alone).
    /// </summary>
    public HivePartitioning Partitioning { get; init; } = HivePartitioning.Auto;

    /// <summary>
    /// The types of the partition columns (by name; it may name more than the paths hold); null infers them (a
    /// column the files have takes its type; else booleans, integers, floats, dates when
    /// <see cref="ParseHiveDates"/>, strings). Required with <see cref="ParquetDatasetFile.PartitionValues"/>.
    /// Types: booleans, integers, floats, decimal128, dates, timestamps, strings.
    /// </summary>
    public Apache.Arrow.Schema? PartitionSchema { get; init; }

    /// <summary>Whether partition values are read as dates and timestamps when they are written so (on by default).</summary>
    public bool ParseHiveDates { get; init; } = true;

    internal void Validate()
    {
        if (!Enum.IsDefined(Partitioning)) throw new ArgumentOutOfRangeException(nameof(Partitioning), Partitioning, "Unknown partitioning.");
        if (PartitionSchema is { } schema)
        {
            foreach (Apache.Arrow.Field field in schema.FieldsList)
            {
                if (!Partitions.Supported(field.DataType))
                    throw new ArgumentException($"The partition column '{field.Name}' is of type {field.DataType.Name}, which partitions cannot be.", nameof(PartitionSchema));
            }
            if (schema.FieldsList.Select(f => f.Name).Distinct(StringComparer.Ordinal).Count() != schema.FieldsList.Count)
                throw new ArgumentException("The partition schema names a column twice.", nameof(PartitionSchema));
        }
        if (FilePathColumn is "") throw new ArgumentException("The file path column needs a name.", nameof(FilePathColumn));
        if (IgnorePrefixes?.Any(string.IsNullOrEmpty) == true)
            throw new ArgumentException("A prefix of files to leave out is empty (it would leave out every file).", nameof(IgnorePrefixes));
    }
}

/// <summary>
/// A file of a dataset: its path in the store, and what is known of it beforehand (its size, from a listing or a log
/// of the files; its footer, read before), so that opening it asks the store for less.
/// </summary>
/// <param name="Path">The path, as the store spells it.</param>
/// <param name="Size">The size in bytes, when known (else the store is asked).</param>
/// <param name="PartitionValues">
/// The file's partition values by column (as a table's log of files records them): values of the
/// <see cref="ParquetDatasetOptions.PartitionSchema"/> types' CLR kinds (bool, integers, floats, decimal, DateOnly,
/// DateTime, DateTimeOffset, string), or strings read as Hive values; null values allowed. Every file then gives every
/// column of the schema.
/// </param>
/// <param name="Rows">The rows to read, by their numbers in the file (a deletion vector's complement); null reads every row.</param>
/// <param name="Metadata">The footer, read before (<see cref="ParquetFile.Metadata"/> of the same file); null reads it.</param>
public sealed record ParquetDatasetFile(string Path, long? Size = null, IReadOnlyDictionary<string, object?>? PartitionValues = null,
    Sazanami.Rows.RowRanges? Rows = null, ParquetMetadata? Metadata = null);

/// <summary>Whether the <c>key=value</c> directories of a dataset's paths give partition columns.</summary>
public enum HivePartitioning
{
    /// <summary>Only for a directory given alone (the default): its subdirectories' keys.</summary>
    Auto,

    /// <summary>For every input: the keys of a directory's subdirectories, of every directory of a glob's or a file's path.</summary>
    On,

    /// <summary>No partition columns from paths.</summary>
    Off,
}
