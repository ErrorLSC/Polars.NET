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

    /// <summary>A row group is closed once its encoded pages reach about this many bytes (128 MiB by default).</summary>
    public long RowGroupBytes { get; init; } = 128L << 20;

    /// <summary>A data page is closed once its values reach about this many bytes before compression (1 MiB by default).</summary>
    public int DataPageBytes { get; init; } = 1 << 20;

    /// <summary>A data page holds at most this many rows.</summary>
    public int DataPageRows { get; init; } = 20_000;

    /// <summary>
    /// Whether columns are dictionary-encoded (booleans never are). A column chunk whose first page shows that the
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

    /// <summary>Leave the stream open when the writer is disposed.</summary>
    public bool LeaveOpen { get; init; }

    internal void Validate()
    {
        ArgumentNullException.ThrowIfNull(Compression, nameof(Compression));
        if (RowGroupRows <= 0) throw new ArgumentOutOfRangeException(nameof(RowGroupRows), RowGroupRows, "RowGroupRows must be positive.");
        if (RowGroupBytes <= 0) throw new ArgumentOutOfRangeException(nameof(RowGroupBytes), RowGroupBytes, "RowGroupBytes must be positive.");
        if (DataPageBytes <= 0) throw new ArgumentOutOfRangeException(nameof(DataPageBytes), DataPageBytes, "DataPageBytes must be positive.");
        if (DataPageRows <= 0) throw new ArgumentOutOfRangeException(nameof(DataPageRows), DataPageRows, "DataPageRows must be positive.");
        if (DictionaryPageBytes <= 0) throw new ArgumentOutOfRangeException(nameof(DictionaryPageBytes), DictionaryPageBytes, "DictionaryPageBytes must be positive.");
        if (StatisticsTruncateLength is <= 0)
            throw new ArgumentOutOfRangeException(nameof(StatisticsTruncateLength), StatisticsTruncateLength, "StatisticsTruncateLength must be positive (null keeps bounds whole).");
        if (DataPageVersion is not (ParquetPageVersion.V1 or ParquetPageVersion.V2))
            throw new ArgumentOutOfRangeException(nameof(DataPageVersion), DataPageVersion, "Unknown page version.");
        if (StoreArrowSchema && Metadata?.ContainsKey(Schema.ArrowMapping.ArrowSchemaKey) == true)
            throw new ArgumentException($"The metadata key '{Schema.ArrowMapping.ArrowSchemaKey}' is written by the writer while StoreArrowSchema is on.", nameof(Metadata));
    }
}
