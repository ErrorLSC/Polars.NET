namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>How the values of a column's data pages are encoded when they are not dictionary-encoded.</summary>
public enum ParquetValueEncoding
{
    /// <summary>Values as they are (every type).</summary>
    Plain,

    /// <summary>Deltas between consecutive values, bit-packed in blocks (INT32 and INT64: integers, dates, times, timestamps, small decimals).</summary>
    DeltaBinaryPacked,

    /// <summary>The lengths delta-encoded, then the bytes back to back (BYTE_ARRAY: strings and binary).</summary>
    DeltaLengthByteArray,

    /// <summary>The prefix each value shares with the one before it, and the rest (BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY).</summary>
    DeltaByteArray,

    /// <summary>
    /// The values' bytes split into one stream per byte position, which compresses floating-point data well (FLOAT,
    /// DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY). Spark's vectorized reader cannot read it.
    /// </summary>
    ByteStreamSplit,

    /// <summary>Run-length and bit-packed encoding (BOOLEAN).</summary>
    Rle,
}

/// <summary>Settings of one column, or of every leaf column under a struct, list or map (<see cref="ParquetWriteOptions.Columns"/>).</summary>
public sealed record ParquetColumnOptions
{
    /// <summary>
    /// The encoding of the data pages written without the dictionary: when <see cref="Dictionary"/> is off, when the
    /// first page holding values shows that the dictionary does not pay, and once the dictionary is full. Set
    /// <see cref="Dictionary"/> to false to use it from the first page on.
    /// </summary>
    public ParquetValueEncoding Encoding { get; init; } = ParquetValueEncoding.Plain;

    /// <summary>Whether the column is dictionary-encoded; null takes <see cref="ParquetWriteOptions.Dictionary"/>.</summary>
    public bool? Dictionary { get; init; }

    /// <summary>A Bloom filter for each column chunk (null: none), which lets readers skip row groups an equality rules out.</summary>
    public ParquetBloomFilterOptions? BloomFilter { get; init; }
}

/// <summary>How the Bloom filters of a column are sized.</summary>
public sealed record ParquetBloomFilterOptions
{
    /// <summary>
    /// The rate at which a filter lets a value through that the chunk does not hold (0.01 by default): lower rates
    /// make larger filters.
    /// </summary>
    public double FalsePositiveRate { get; init; } = 0.01;

    /// <summary>
    /// The most distinct values a filter is sized for. Each column chunk's filter is sized for the distinct values the
    /// chunk holds (counted while the row group is in memory), at most this many (null: no limit).
    /// </summary>
    public long? DistinctValues { get; init; }

    /// <summary>The largest filter written (1 MiB by default); larger ones are cut to it, letting more values through. At least 32 bytes.</summary>
    public int MaxBytes { get; init; } = 1 << 20;
}
