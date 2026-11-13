using Apache.Arrow.Types;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>How <see cref="ParquetFile.OpenReader"/> reads a file.</summary>
public sealed record ParquetReadOptions
{
    /// <summary>The top-level columns to read, by name, returned in file order; null reads all of them unless <see cref="ColumnPaths"/> selects some.</summary>
    public IReadOnlyList<string>? Columns { get; init; }

    /// <summary>
    /// Parts of top-level columns to read, each a path of field names from a top-level column down (<c>["add",
    /// "path"]</c>): the column is returned with only the struct fields on the paths. Paths go through lists to
    /// their elements' fields and through maps by <c>key</c> and <c>value</c>. Leaves off every path are not read.
    /// Combines with <see cref="Columns"/> (a column named there is read whole); null selects nothing by path.
    /// </summary>
    public IReadOnlyList<string[]>? ColumnPaths { get; init; }

    /// <summary>The row groups to read, by index, in the order given; null reads all of them.</summary>
    public IReadOnlyList<int>? RowGroups { get; init; }

    /// <summary>The largest number of rows per record batch. Batches never span row groups.</summary>
    public int BatchSize { get; init; } = 65_536;

    /// <summary>
    /// How many threads decode at once (all cores by default; 1 reads on the calling thread, streaming). Row groups are
    /// decoded side by side, each fetching its own column chunks, and the columns of a batch too; batches still come
    /// in order. Up to this many decoded row groups are held in memory ahead of the consumer.
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = Environment.ProcessorCount;

    /// <summary>Whether to check the CRC32 of every page that carries one (a mismatch is an error).</summary>
    public bool VerifyChecksums { get; init; }

    /// <summary>Whether unannotated BYTE_ARRAY columns are read as strings (as old Impala and Hive files need).</summary>
    public bool BinaryAsString { get; init; }

    /// <summary>
    /// Whether strings and binaries are returned in Arrow's view layout (<c>Utf8View</c>, <c>BinaryView</c>; the
    /// default): 16 bytes per value, short values inline, longer ones pointing into data buffers, so that values of a
    /// dictionary-encoded column share the dictionary's bytes instead of being copied per row. Polars and DuckDB
    /// work in this layout. False returns <c>Utf8</c> and <c>Binary</c> (offsets and contiguous bytes). Columns the
    /// file's Arrow schema marks as dictionaries are returned as dictionaries either way.
    /// </summary>
    public bool ViewTypes { get; init; } = true;

    /// <summary>
    /// The unit INT96 timestamps are read in when the file's Arrow schema does not give one: microseconds (the
    /// default; years 1 to 9999 fit) or nanoseconds (years 1677 to 2262; values outside are an error).
    /// </summary>
    public TimeUnit Int96Unit { get; init; } = TimeUnit.Microsecond;

    internal void Validate()
    {
        if (BatchSize <= 0) throw new ArgumentOutOfRangeException(nameof(BatchSize), BatchSize, "The batch size must be positive.");
        if (MaxDegreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxDegreeOfParallelism), MaxDegreeOfParallelism, "The degree of parallelism must be positive.");
        if (Int96Unit is not (TimeUnit.Microsecond or TimeUnit.Nanosecond))
            throw new ArgumentOutOfRangeException(nameof(Int96Unit), Int96Unit, "INT96 timestamps are read in microseconds or nanoseconds.");
    }
}
