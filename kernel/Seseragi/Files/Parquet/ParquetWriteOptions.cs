using System.Collections.ObjectModel;
using Apache.Arrow.Types;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>The layout of data pages.</summary>
public enum ParquetPageVersion
{
    /// <summary>Format 1 data pages (levels and values compressed together); what every reader reads.</summary>
    V1,

    /// <summary>Format 2 data pages (levels uncompressed ahead of the values, row counts in the header).</summary>
    V2,
}

/// <summary>Who owns the record batches given to <see cref="ParquetWriter"/>.</summary>
public enum ParquetBatchOwnership
{
    /// <summary>The caller: a batch is encoded before Write returns and may be disposed then.</summary>
    Caller,

    /// <summary>
    /// The writer: it takes every batch given to Write (and those of the static Write methods), reads it after Write
    /// returns, and disposes it once its rows are encoded (also when Write fails); the caller neither uses nor disposes
    /// it afterwards. With more than one thread, several row groups are then encoded at once, straight from the
    /// batches.
    /// </summary>
    Writer,
}

/// <summary>Options for <see cref="ParquetWriter"/>.</summary>
public sealed record ParquetWriteOptions
{
    /// <summary>
    /// The page compression (<see cref="SnappyCompression"/> by default, as Spark writes; also
    /// <see cref="NoCompression"/>, <see cref="ZstandardCompression"/>, <see cref="GzipCompression"/>,
    /// <see cref="BrotliCompression"/> and <see cref="Lz4RawCompression"/>).
    /// </summary>
    public IParquetCompression Compression { get; init; } = SnappyCompression.Instance;

    /// <summary>The layout of data pages (<see cref="ParquetPageVersion.V1"/> by default).</summary>
    public ParquetPageVersion DataPageVersion { get; init; } = ParquetPageVersion.V1;

    /// <summary>A row group is closed once it holds this many rows (or <see cref="RowGroupBytes"/>, whichever comes first).</summary>
    public int RowGroupRows { get; init; } = 1_048_576;

    /// <summary>
    /// A row group is closed once its encoded pages reach about this many bytes (128 MiB by default); with
    /// <see cref="ParquetBatchOwnership.Writer"/> and more than one thread, once the batches' Arrow buffers for its rows
    /// do (row groups are then laid out before they are encoded).
    /// </summary>
    public long RowGroupBytes { get; init; } = 128L << 20;

    /// <summary>A data page is closed once its values reach about this many bytes before compression (1 MiB by default).</summary>
    public int DataPageBytes { get; init; } = 1 << 20;

    /// <summary>A data page holds at most this many rows.</summary>
    public int DataPageRows { get; init; } = 20_000;

    /// <summary>
    /// Whether columns are dictionary-encoded (booleans never are). A column chunk whose first page holding values shows that the
    /// dictionary does not pay is written plainly; one whose dictionary outgrows <see cref="DictionaryPageBytes"/> goes
    /// on plainly from the next page.
    /// </summary>
    public bool Dictionary { get; init; } = true;

    /// <summary>The largest dictionary (its plain-encoded values) a column chunk builds (1 MiB by default).</summary>
    public int DictionaryPageBytes { get; init; } = 1 << 20;

    /// <summary>Whether column chunks carry statistics (bounds, null counts, NaN counts).</summary>
    public bool Statistics { get; init; } = true;

    /// <summary>
    /// String and binary bounds longer than this many bytes are cut to it in the statistics (the upper bound rounded
    /// up so that it still bounds the values; marked inexact); null keeps them whole.
    /// </summary>
    public int? StatisticsTruncateLength { get; init; } = 64;

    /// <summary>
    /// Whether column chunks carry a page index: where each data page lies and the row it starts at (OffsetIndex),
    /// and, with <see cref="Statistics"/>, each page's bounds and null count (ColumnIndex), which readers such as
    /// Spark use to skip pages. A chunk whose values hold a NaN, or whose type has no order (INT96, INTERVAL), gets
    /// no ColumnIndex.
    /// </summary>
    public bool PageIndex { get; init; } = true;

    /// <summary>
    /// Whether column chunks (and their pages, in the page index) carry size statistics: the bytes of string and
    /// binary values, and how many values each repetition and definition level has where that says more than the
    /// null count.
    /// </summary>
    public bool SizeStatistics { get; init; } = true;

    /// <summary>
    /// Settings by column, keyed by the column's path in the Parquet schema, its names joined by dots: <c>"id"</c>,
    /// <c>"s.x"</c> (a struct field), <c>"l.list.element"</c> (list items), <c>"m.key_value.key"</c> and
    /// <c>"m.key_value.value"</c> (map keys and values). A path to a struct, list or map applies to every leaf column
    /// under it, the longest path that names a leaf winning. A path naming no column, or settings a leaf's type cannot
    /// take (an encoding of another type, a Bloom filter on booleans), are refused when the writer is created.
    /// </summary>
    public IReadOnlyDictionary<string, ParquetColumnOptions>? Columns { get; init; }

    /// <summary>Whether every page carries the CRC32 of its bytes (as Spark writes).</summary>
    public bool WriteChecksums { get; init; } = true;

    /// <summary>
    /// The unit timestamps are stored in (null keeps each column's own; seconds, which Parquet lacks, are stored in
    /// milliseconds). Coarser units take only values they hold exactly. Spark reads milliseconds and microseconds,
    /// not nanoseconds.
    /// </summary>
    public TimeUnit? TimestampUnit { get; init; }

    /// <summary>Whether timestamps are stored as INT96 (the legacy layout of old Spark and Hive), without statistics.</summary>
    public bool Int96Timestamps { get; init; }

    /// <summary>
    /// Whether the Arrow schema is stored in the <c>ARROW:schema</c> key, so that Arrow readers restore what Parquet
    /// cannot say: time zone names, durations, seconds, dictionary columns, large and view types, field metadata.
    /// </summary>
    public bool StoreArrowSchema { get; init; } = true;

    /// <summary>Extra key-value metadata of the file (<c>ARROW:schema</c> is reserved while <see cref="StoreArrowSchema"/> is on).</summary>
    public IReadOnlyDictionary<string, string>? Metadata { get; init; }

    /// <summary>
    /// The most threads the writer keeps busy encoding column chunks and compressing pages (the processor count by
    /// default; 1 writes on the caller's thread alone). The file is the same, byte for byte, whatever the number.
    /// </summary>
    public int MaxDegreeOfParallelism { get; init; } = Environment.ProcessorCount;

    /// <summary>
    /// Who owns the batches (<see cref="ParquetBatchOwnership.Caller"/> by default). With
    /// <see cref="ParquetBatchOwnership.Writer"/> and more than one thread, row groups are encoded side by side.
    /// </summary>
    public ParquetBatchOwnership BatchOwnership { get; init; } = ParquetBatchOwnership.Caller;

    /// <summary>
    /// With <see cref="ParquetBatchOwnership.Writer"/>, the batches' Arrow bytes of the row groups encoded at once, at
    /// most (a row group goes beyond alone). Measured (16 threads): row groups of 7 MB (128 Ki rows of a nested table)
    /// wrote fastest eight at a time, of 210 MB (1 Mi rows of eight flat columns) two at a time; more held memory and
    /// slowed the writes. Tests set less.
    /// </summary>
    internal long InFlightBytes { get; init; } = 512L << 20;

    /// <summary>Leave the stream open when the writer is disposed.</summary>
    public bool LeaveOpen { get; init; }

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Compression, nameof(Compression));
        if (RowGroupRows <= 0) throw new ArgumentOutOfRangeException(nameof(RowGroupRows), RowGroupRows, "RowGroupRows must be positive.");
        if (RowGroupBytes <= 0) throw new ArgumentOutOfRangeException(nameof(RowGroupBytes), RowGroupBytes, "RowGroupBytes must be positive.");
        if (DataPageBytes <= 0) throw new ArgumentOutOfRangeException(nameof(DataPageBytes), DataPageBytes, "DataPageBytes must be positive.");
        if (DataPageRows <= 0) throw new ArgumentOutOfRangeException(nameof(DataPageRows), DataPageRows, "DataPageRows must be positive.");
        if (!Enum.IsDefined(BatchOwnership)) throw new ArgumentOutOfRangeException(nameof(BatchOwnership), BatchOwnership, "Unknown batch ownership.");
        if (MaxDegreeOfParallelism <= 0)
            throw new ArgumentOutOfRangeException(nameof(MaxDegreeOfParallelism), MaxDegreeOfParallelism, "The degree of parallelism must be positive.");
        if (DictionaryPageBytes <= 0) throw new ArgumentOutOfRangeException(nameof(DictionaryPageBytes), DictionaryPageBytes, "DictionaryPageBytes must be positive.");
        if (StatisticsTruncateLength is <= 0)
            throw new ArgumentOutOfRangeException(nameof(StatisticsTruncateLength), StatisticsTruncateLength, "StatisticsTruncateLength must be positive (null keeps bounds whole).");
        if (DataPageVersion is not (ParquetPageVersion.V1 or ParquetPageVersion.V2))
            throw new ArgumentOutOfRangeException(nameof(DataPageVersion), DataPageVersion, "Unknown page version.");
        foreach (var (path, column) in Columns ?? ReadOnlyDictionary<string, ParquetColumnOptions>.Empty)
        {
            if (column is null) throw new ArgumentNullException(nameof(Columns), $"The settings of column '{path}' are null.");
            if (!Enum.IsDefined(column.Encoding)) throw new ArgumentOutOfRangeException(nameof(Columns), column.Encoding, $"Unknown encoding for column '{path}'.");
            if (column.BloomFilter is { } bloom)
            {
                if (!(bloom.FalsePositiveRate is > 0 and < 1))
                    throw new ArgumentOutOfRangeException(nameof(Columns), bloom.FalsePositiveRate, $"The Bloom filter false positive rate of column '{path}' must lie between 0 and 1.");
                if (bloom.DistinctValues is <= 0)
                    throw new ArgumentOutOfRangeException(nameof(Columns), bloom.DistinctValues, $"The Bloom filter of column '{path}' must be sized for a positive number of distinct values.");
                if (bloom.MaxBytes < 32)
                    throw new ArgumentOutOfRangeException(nameof(Columns), bloom.MaxBytes, $"The Bloom filter of column '{path}' needs at least 32 bytes (one block).");
            }
        }
        if (StoreArrowSchema && Metadata?.ContainsKey(Schema.ArrowMapping.ArrowSchemaKey) == true)
            throw new ArgumentException($"The metadata key '{Schema.ArrowMapping.ArrowSchemaKey}' is written by the writer while StoreArrowSchema is on.", nameof(Metadata));
    }
}
