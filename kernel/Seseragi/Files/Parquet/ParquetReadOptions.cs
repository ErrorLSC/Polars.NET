using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Rows;

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

    /// <summary>
    /// A row filter in the query compiler's IR, used as a hint: row groups whose statistics show that no row can
    /// pass it are not read, nor pages whose column index shows it (the pages of every column read for those rows are
    /// then skipped by the offset index, never read). Rows are not filtered, so the batches may still hold rows the filter drops; the caller
    /// applies it (as a plan keeps its filter above the scan). Columns are the file's top-level names, struct fields
    /// are reached with <c>StructFieldByName</c>; the semantics are the IR's (Kleene logic, NaN greater than every
    /// number). What cannot be decided from statistics keeps the row group. Null reads every row group.
    /// </summary>
    public ScalarExpr? Filter { get; init; }

    /// <summary>
    /// Whether only the rows where <see cref="Filter"/> is true are delivered (off by default: the filter only prunes).
    /// The columns the filter reads are decoded first, row group by row group, and the other columns only for the rows
    /// it keeps (late materialization). Conjuncts of the filter the reader cannot evaluate (comparisons between
    /// columns, functions, …) are left out, so the rows delivered may still include some the filter drops:
    /// <see cref="ParquetReader.FiltersExactly"/> tells. Columns the filter reads are read even when not selected, so
    /// damage in them fails the read. Batches may then hold fewer rows than <see cref="BatchSize"/>.
    /// </summary>
    public bool FilterRows { get; init; }

    /// <summary>
    /// Whether <see cref="Filter"/>'s equality tests (<c>=</c>, null-safe <c>=</c>, <c>IN</c>) also consult the column
    /// chunks' Bloom filters (on by default): a row group whose filter rules the value out is not read. Each filter is
    /// read once per file.
    /// </summary>
    public bool BloomFilters { get; init; } = true;

    /// <summary>
    /// The rows to read, by their numbers in the file (from 0, row groups in file order); null reads every row. A row
    /// group without a selected row is not read; pages wholly between selected rows are passed over unread where their
    /// headers tell their rows. Combines with <see cref="RowGroups"/> and <see cref="Filter"/> (rows of the row groups
    /// they keep). A deletion vector is <c>RowRanges.FromIndices(deleted).Complement(rowCount)</c>.
    /// </summary>
    public RowRanges? Rows { get; init; }

    /// <summary>
    /// The name of an Int64 column appended to the batches, holding each row's number in the file (as
    /// <see cref="Rows"/> numbers them); null adds none. It must differ from the names of the columns read.
    /// </summary>
    public string? RowIndexColumn { get; init; }

    /// <summary>
    /// The most rows to read, in the order they are delivered, counted after <see cref="Filter"/> and <see cref="Rows"/>
    /// left out what they could (with <see cref="FilterRows"/>, the rows delivered after filtering: row groups ahead
    /// of the consumer may then be decoded beyond the last row delivered); null reads them all.
    /// </summary>
    public long? MaxRows { get; init; }

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
    /// dictionary-encoded column share the dictionary's bytes instead of being copied per row. Vectorized engines
    /// (DuckDB and others) work in this layout. False returns <c>Utf8</c> and <c>Binary</c> (offsets and contiguous bytes). Columns the
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

    /// <summary>
    /// Fetched pieces of column chunks closer than this come in one read (the bytes between are read and unused): on
    /// an object store each read is a request. Tests set 0 to see that skipped pages are never requested.
    /// </summary>
    internal long FetchGap { get; init; } = 1 << 20;

    /// <summary>How the rows a filter keeps are read (<see cref="FilterRows"/>; tests force each way).</summary>
    internal LateMaterialization LateMaterialization { get; init; }

    /// <summary>Row groups are started ahead of the consumer while their uncompressed sizes stay within this (at least one is).</summary>
    internal long InFlightBytes { get; init; } = 512L << 20;

    internal void Validate()
    {
        if (BatchSize <= 0) throw new ArgumentOutOfRangeException(nameof(BatchSize), BatchSize, "The batch size must be positive.");
        if (MaxRows < 0) throw new ArgumentOutOfRangeException(nameof(MaxRows), MaxRows, "The most rows to read cannot be negative.");
        if (RowIndexColumn is "") throw new ArgumentException("The row index column needs a name.", nameof(RowIndexColumn));
        if (MaxDegreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxDegreeOfParallelism), MaxDegreeOfParallelism, "The degree of parallelism must be positive.");
        if (!Enum.IsDefined(MemoryMap)) throw new ArgumentOutOfRangeException(nameof(MemoryMap), MemoryMap, "Unknown memory map mode.");
        if (!Enum.IsDefined(LateMaterialization)) throw new ArgumentOutOfRangeException(nameof(LateMaterialization), LateMaterialization, "Unknown mode.");
        if (Int96Unit is not (TimeUnit.Microsecond or TimeUnit.Nanosecond))
            throw new ArgumentOutOfRangeException(nameof(Int96Unit), Int96Unit, "INT96 timestamps are read in microseconds or nanoseconds.");
    }
}

/// <summary>How the columns a filter does not read are read for the rows it keeps (<see cref="ParquetReadOptions.FilterRows"/>).</summary>
internal enum LateMaterialization
{
    /// <summary>By how scattered the rows kept are: <see cref="Dense"/> from <see cref="ParquetReader.DenseFrom"/> runs of them per row read on, else <see cref="Sparse"/>.</summary>
    Auto,

    /// <summary>Decoded for the rows the filter read and filtered batch by batch (batches keep the filter's boundaries).</summary>
    Dense,

    /// <summary>Decoded for the rows kept only, skipping the others (full batches); also whenever a selected column is nested.</summary>
    Sparse,
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
