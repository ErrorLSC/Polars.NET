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
    /// How many threads decode at once (all cores by default; 1 reads on the calling thread). The columns of a row
    /// group are decoded side by side, and several row groups at once; batches still come in order. Up to this many
    /// decoded row groups (fewer when they are large) are held in memory ahead of the consumer.
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = Environment.ProcessorCount;

    /// <summary>
    /// Whether a local file is read through a memory mapping (<see cref="ParquetMemoryMap"/>; off by default). Other
    /// sources (streams, memory, stores) ignore it.
    /// </summary>
    public ParquetMemoryMap MemoryMap { get; init; } = ParquetMemoryMap.Off;

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

    /// <summary>How column chunks are read (tests and measurements force one; the default follows the source).</summary>
    internal ChunkReading Reading { get; init; }

    /// <summary>Whether the definition levels of flat optional columns are decoded as validity bits (tests turn it off to compare).</summary>
    internal bool LevelsAsBits { get; init; } = true;

    /// <summary>Row groups are started ahead of the consumer while their uncompressed sizes stay within this (at least one is).</summary>
    internal long InFlightBytes { get; init; } = 512L << 20;

    internal void Validate()
    {
        if (BatchSize <= 0) throw new ArgumentOutOfRangeException(nameof(BatchSize), BatchSize, "The batch size must be positive.");
        if (MaxDegreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxDegreeOfParallelism), MaxDegreeOfParallelism, "The degree of parallelism must be positive.");
        if (!Enum.IsDefined(MemoryMap)) throw new ArgumentOutOfRangeException(nameof(MemoryMap), MemoryMap, "Unknown memory map mode.");
        if (Int96Unit is not (TimeUnit.Microsecond or TimeUnit.Nanosecond))
            throw new ArgumentOutOfRangeException(nameof(Int96Unit), Int96Unit, "INT96 timestamps are read in microseconds or nanoseconds.");
    }
}

/// <summary>How a reader gets the bytes of column chunks.</summary>
internal enum ChunkReading
{
    /// <summary>As the source suits: sliced from memory, page by page from a local file, else fetched.</summary>
    Auto,

    /// <summary>A row group's chunks fetched with few ranged reads before decoding.</summary>
    Fetch,

    /// <summary>Sliced from the source's memory (when it has none: as <see cref="Auto"/>).</summary>
    Slice,

    /// <summary>Read page by page from the source.</summary>
    Stream,
}

/// <summary>
/// How the bytes of a local Parquet file are read. Reads through a mapping skip the copy of each page out of the
/// operating system's page cache: in our measurements that pays for uncompressed fixed-width columns and costs on
/// compressed and string columns read on many threads, which <see cref="Auto"/> therefore keeps reading the ordinary
/// way.
/// </summary>
/// <remarks>
/// While a file is mapped, it must not be truncated by another process: reading pages that no longer exist ends the
/// process (SIGBUS on Unix, an access violation on Windows), which no exception handler can catch. Batches never point
/// into the mapping: their values are copied, so they outlive the file.
/// </remarks>
public enum ParquetMemoryMap
{
    /// <summary>Ordinary reads, page by page (the default).</summary>
    Off,

    /// <summary>Every column chunk decoded straight from the mapping.</summary>
    On,

    /// <summary>Uncompressed fixed-width column chunks decoded from the mapping; the others read page by page.</summary>
    Auto,
}
