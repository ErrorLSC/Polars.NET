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

    /// <summary>
    /// How the dataset's columns are found (<see cref="DatasetSchemaMode"/>; by default the first file's), when no
    /// <see cref="Schema"/> is given.
    /// </summary>
    public DatasetSchemaMode SchemaMode { get; init; } = DatasetSchemaMode.FirstFile;

    /// <summary>
    /// The dataset's columns, given (as a table's schema records them): no file is read to find them. Files'
    /// columns convert to their types (see <see cref="ParquetDatasetReader"/>); columns the files have beyond them
    /// are not read; a column a file lacks is an error unless <see cref="AllowMissingColumns"/>. Strings and binaries
    /// in any layout (plain, large, view) are read in the layout <see cref="ParquetReadOptions.ViewTypes"/> says (a
    /// dictionary's values plain), large lists as lists;
    /// nullability is not enforced (a dataset's columns are nullable). Not with <see cref="DatasetSchemaMode.UnionByName"/>.
    /// </summary>
    public Apache.Arrow.Schema? Schema { get; init; }

    /// <summary>
    /// Whether a column or struct field the dataset has and a file lacks is read as nulls for that file's rows (off
    /// by default: an error naming the file and the column). <see cref="DatasetSchemaMode.UnionByName"/> always does.
    /// </summary>
    public bool AllowMissingColumns { get; init; }

    /// <summary>
    /// Whether files' columns may convert to the dataset's types by conversions that approximate values (off by
    /// default: only lossless widenings): integers and decimals into floats that cannot hold them exactly (the
    /// nearest float), floats into narrower floats, times, timestamps and durations into a coarser unit (rounded
    /// down). Never narrower integers, wall-clock times into instants or back, strings into numbers or back,
    /// timestamps into dates. <see cref="DatasetSchemaMode.UnionByName"/> then also finds types for mixes such as
    /// 64-bit integers and floats (doubles).
    /// </summary>
    public bool AllowLossyCasts { get; init; }

    /// <summary>
    /// How many files are open at once: read, planned (their footers read, their row groups started as threads
    /// allow) or opened ahead; null decides it: from local files and memory enough that small files keep every
    /// thread busy, from other stores 1024, so that the files after one whose request is slow keep being read
    /// meanwhile (their requests bounded by <see cref="MaxRequestsInFlight"/>). Set, it bounds the requests too.
    /// </summary>
    public int? MaxFilesInFlight { get; init; }

    /// <summary>
    /// How many requests to the store run at once (files opened, row groups fetched), those of the files read first
    /// let in first; null: 64 for stores whose requests wait on the network (not local files or memory, which are not
    /// bounded). Fewer suit a store close by, more a distant one; many more at once lose packets on the way. Set for
    /// local files or memory read on one thread, it bounds the files opened ahead (their reads come one at a time).
    /// </summary>
    public int? MaxRequestsInFlight { get; init; }

    internal void Validate()
    {
        if (MaxFilesInFlight is < 1) throw new ArgumentOutOfRangeException(nameof(MaxFilesInFlight), MaxFilesInFlight, "At least one file is opened at a time.");
        if (MaxRequestsInFlight is < 1) throw new ArgumentOutOfRangeException(nameof(MaxRequestsInFlight), MaxRequestsInFlight, "At least one request runs at a time.");
        if (!Enum.IsDefined(Partitioning)) throw new ArgumentOutOfRangeException(nameof(Partitioning), Partitioning, "Unknown partitioning.");
        if (!Enum.IsDefined(SchemaMode)) throw new ArgumentOutOfRangeException(nameof(SchemaMode), SchemaMode, "Unknown schema mode.");
        if (Schema is { } given)
        {
            if (SchemaMode == DatasetSchemaMode.UnionByName)
                throw new ArgumentException("A given schema names the columns itself: it does not go with SchemaMode.UnionByName.", nameof(Schema));
            if (given.FieldsList.Count == 0) throw new ArgumentException("The given schema has no column.", nameof(Schema));
            if (given.FieldsList.Select(f => f.Name).Distinct(StringComparer.Ordinal).Count() != given.FieldsList.Count)
                throw new ArgumentException("The given schema names a column twice.", nameof(Schema));
        }
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

/// <summary>How a dataset's columns are found when no <see cref="ParquetDatasetOptions.Schema"/> is given.</summary>
public enum DatasetSchemaMode
{
    /// <summary>
    /// The first file's columns (the default; read when the dataset opens): the other files' columns must convert
    /// to their types (a wider type in a later file is an error), columns beyond them are not read.
    /// </summary>
    FirstFile,

    /// <summary>
    /// Every file's columns, by name, in the order they first appear (every footer is read when the dataset opens):
    /// each column of the narrowest type every file's converts to; a file lacking a column reads nulls for it. Struct
    /// fields unite the same way.
    /// </summary>
    UnionByName,
}
