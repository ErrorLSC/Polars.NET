namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// How <see cref="ParquetDataset.Open(string, ParquetDatasetOptions?)"/> finds and describes a dataset's files (see
/// <see cref="DatasetOptions"/>). With a given <see cref="DatasetOptions.Schema"/>, strings and binaries in any layout
/// (plain, large, view) are read in the layout <see cref="ParquetReadOptions.ViewTypes"/> says (a dictionary's values
/// plain), large lists as lists.
/// </summary>
public sealed record ParquetDatasetOptions : DatasetOptions
{
    /// <summary>
    /// The keys of files written with Parquet modular encryption, the same for every file of the dataset (files that
    /// are not encrypted read as they are). See <see cref="ParquetDecryptionOptions"/>.
    /// </summary>
    public ParquetDecryptionOptions? Decryption { get; init; }
}

/// <summary>
/// A file of a Parquet dataset (see <see cref="DatasetFile"/>), with what else is known of it beforehand.
/// </summary>
/// <param name="Path">The path, as the store spells it.</param>
/// <param name="Size">The size in bytes, when known (else the store is asked).</param>
/// <param name="PartitionValues">The file's partition values by column (see <see cref="DatasetFile.PartitionValues"/>).</param>
/// <param name="Rows">The rows to read, by their numbers in the file (a deletion vector's complement); null reads every row.</param>
/// <param name="Metadata">The footer, read before (<see cref="ParquetFile.Metadata"/> of the same file); null reads it.</param>
public sealed record ParquetDatasetFile(string Path, long? Size = null, IReadOnlyDictionary<string, object?>? PartitionValues = null,
    Sazanami.Rows.RowRanges? Rows = null, ParquetMetadata? Metadata = null) : DatasetFile(Path, Size, PartitionValues);
