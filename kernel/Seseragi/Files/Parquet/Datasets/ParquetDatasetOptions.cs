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

    /// <summary>
    /// How the files' columns are matched to the given <see cref="DatasetOptions.Schema"/>'s: by name (the default) or
    /// by field id, as Iceberg tables match them (see <see cref="ParquetFieldMatching.ByFieldId"/>).
    /// </summary>
    public ParquetFieldMatching FieldMatching { get; init; }

    /// <summary>
    /// With <see cref="ParquetFieldMatching.ByFieldId"/>, the ids of files written without field ids, by their fields'
    /// names (Iceberg's <c>schema.name-mapping.default</c>); null: such a file is an error.
    /// </summary>
    public NameMapping? NameMapping { get; init; }

    internal override void Validate()
    {
        base.Validate();
        if (!Enum.IsDefined(FieldMatching)) throw new ArgumentOutOfRangeException(nameof(FieldMatching), FieldMatching, "Unknown field matching.");
        if (FieldMatching == ParquetFieldMatching.ByFieldId && Schema is null)
            throw new ArgumentException("Matching columns by field id needs the dataset's schema given (Schema), its fields with their ids (PARQUET:field_id).", nameof(FieldMatching));
        if (CaseInsensitiveNames && FieldMatching == ParquetFieldMatching.ByFieldId)
            throw new ArgumentException("Matching by field id leaves names out (a name mapping's names match as spelled): CaseInsensitiveNames does not go with it.", nameof(CaseInsensitiveNames));
        if (NameMapping is not null && FieldMatching != ParquetFieldMatching.ByFieldId)
            throw new ArgumentException("A name mapping gives files field ids: it goes with FieldMatching = ByFieldId.", nameof(NameMapping));
    }
}

/// <summary>How the columns of a Parquet dataset's files are matched to the dataset's given schema.</summary>
public enum ParquetFieldMatching
{
    /// <summary>By name: a top-level column, or a struct field, of the same name.</summary>
    ByName,

    /// <summary>
    /// By field id (<c>PARQUET:field_id</c>), as Iceberg readers match them: every column and struct field of the
    /// schema (Hive partition columns and the file path column aside) has an id, unique in the schema. A file's column
    /// or struct field is the schema's of the same id at the same place (a struct's fields within the struct matched;
    /// lists' elements and maps' keys and values gone through), read under the schema's name (renames) and converted to
    /// its type by the usual widenings; a file field without an id, or of an id the schema lacks, is not read (a
    /// column dropped and added again under its old name never reads the old values). A schema field of an id a file
    /// lacks is missing (see <see cref="DatasetOptions.AllowMissingColumns"/>). A file without any field id takes its
    /// ids from <see cref="ParquetDatasetOptions.NameMapping"/>. Filters, statistics, page indexes and Bloom filters
    /// work through the schema's names.
    /// </summary>
    ByFieldId,
}

/// <summary>
/// A file of a Parquet dataset (see <see cref="DatasetFile"/>), with what else is known of it beforehand.
/// </summary>
/// <param name="Path">The path, as the store spells it.</param>
/// <param name="Size">The size in bytes, when known (else the store is asked).</param>
/// <param name="PartitionValues">The file's partition values by column (see <see cref="DatasetFile.PartitionValues"/>).</param>
/// <param name="Rows">The rows to read, by their numbers in the file (a deletion vector's complement); null reads every row.</param>
/// <param name="Metadata">The footer, read before (<see cref="ParquetFile.Metadata"/> of the same file); null reads it.</param>
/// <param name="Defaults">The values of columns and struct fields this file lacks (see <see cref="DatasetFile.Defaults"/>).</param>
public sealed record ParquetDatasetFile(string Path, long? Size = null, IReadOnlyDictionary<string, object?>? PartitionValues = null,
    Sazanami.Rows.RowRanges? Rows = null, ParquetMetadata? Metadata = null, IReadOnlyList<ColumnDefault>? Defaults = null)
    : DatasetFile(Path, Size, PartitionValues, Defaults);
