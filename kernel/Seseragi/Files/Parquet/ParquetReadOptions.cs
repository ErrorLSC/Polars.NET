using Apache.Arrow.Types;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>How <see cref="ParquetFile.OpenReader"/> reads a file.</summary>
public sealed record ParquetReadOptions
{
    /// <summary>The top-level columns to read, by name, returned in file order; null reads all of them.</summary>
    public IReadOnlyList<string>? Columns { get; init; }

    /// <summary>The row groups to read, by index, in the order given; null reads all of them.</summary>
    public IReadOnlyList<int>? RowGroups { get; init; }

    /// <summary>The largest number of rows per record batch. Batches never span row groups.</summary>
    public int BatchSize { get; init; } = 65_536;

    /// <summary>Whether to check the CRC32 of every page that carries one (a mismatch is an error).</summary>
    public bool VerifyChecksums { get; init; }

    /// <summary>Whether unannotated BYTE_ARRAY columns are read as strings (as old Impala and Hive files need).</summary>
    public bool BinaryAsString { get; init; }

    /// <summary>
    /// The unit INT96 timestamps are read in when the file's Arrow schema does not give one: microseconds (the
    /// default; years 1 to 9999 fit) or nanoseconds (years 1677 to 2262; values outside are an error).
    /// </summary>
    public TimeUnit Int96Unit { get; init; } = TimeUnit.Microsecond;

    internal void Validate()
    {
        if (BatchSize <= 0) throw new ArgumentOutOfRangeException(nameof(BatchSize), BatchSize, "The batch size must be positive.");
        if (Int96Unit is not (TimeUnit.Microsecond or TimeUnit.Nanosecond))
            throw new ArgumentOutOfRangeException(nameof(Int96Unit), Int96Unit, "INT96 timestamps are read in microseconds or nanoseconds.");
    }
}
