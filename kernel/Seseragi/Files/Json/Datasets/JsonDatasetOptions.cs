using Apache.Arrow.Types;

namespace Kaminari.Seseragi.Files.Json;

/// <summary>
/// How <see cref="JsonDataset.Open(string, JsonDatasetOptions?)"/> finds and describes a dataset's files (see
/// <see cref="DatasetOptions"/>), and their layout. The columns are given (<see cref="DatasetOptions.Schema"/>), or
/// inferred from the first records of the first file that holds any (<see cref="DatasetSchemaMode.FirstFile"/>, as
/// dataframe engines infer them) or of every file, united by the rules that unite records' types
/// (<see cref="DatasetSchemaMode.UnionByName"/>). Every file's values are decoded into the dataset's types (no
/// conversion): a field a record lacks is null, a value its column's type cannot hold is an error (or a null with
/// <see cref="JsonReadOptions.IgnoreErrors"/>). <see cref="DatasetOptions.AllowLossyCasts"/> and
/// <see cref="DatasetOptions.UnifyDictionaries"/> do not apply.
/// </summary>
public sealed record JsonDatasetOptions : DatasetOptions
{
    /// <summary>The files' layout: one object per line (the default) or one document each.</summary>
    public JsonFormat Format { get; init; } = JsonFormat.Lines;

    /// <summary>The records of a file the columns are inferred from (100 by default); null takes every record.</summary>
    public int? InferSchemaLength { get; init; } = 100;

    /// <summary>Types of some inferred columns, by name (a name the inference does not find is an error).</summary>
    public IReadOnlyDictionary<string, IArrowType>? SchemaOverrides { get; init; }

    /// <summary>Inferred strings as Utf8View (true, the default) or Utf8.</summary>
    public bool ViewTypes { get; init; } = true;

    internal override void Validate()
    {
        base.Validate();
        if (!Enum.IsDefined(Format)) throw new ArgumentOutOfRangeException(nameof(Format), Format, "Unknown JSON format.");
        if (InferSchemaLength is <= 0) throw new ArgumentOutOfRangeException(nameof(InferSchemaLength), InferSchemaLength, "At least one record is taken in (null takes them all).");
        if (Schema is not null && SchemaOverrides is not null) throw new ArgumentException("Give either a schema or schema overrides, not both.", nameof(SchemaOverrides));
        if (AllowLossyCasts) throw new ArgumentException("JSON values are decoded into the dataset's types: there is no conversion to allow.", nameof(AllowLossyCasts));
        if (UnifyDictionaries) throw new ArgumentException("JSON values are decoded into the dataset's types: there is no file's dictionary to unify.", nameof(UnifyDictionaries));
        if (Defaults is not null) throw new ArgumentException(NoDefaults, nameof(Defaults));
    }

    /// <summary>Why JSON datasets take no defaults.</summary>
    internal const string NoDefaults = "JSON files have no columns of their own to lack (every file reads the dataset's columns, a record without a field "
        + "reading null): defaults do not apply to them.";
}

/// <summary>A file of a JSON dataset (see <see cref="DatasetFile"/>).</summary>
/// <param name="Path">The path, as the store spells it.</param>
/// <param name="Size">The size in bytes, when known (else the store is asked).</param>
/// <param name="PartitionValues">The file's partition values by column (see <see cref="DatasetFile.PartitionValues"/>).</param>
public sealed record JsonDatasetFile(string Path, long? Size = null, IReadOnlyDictionary<string, object?>? PartitionValues = null)
    : DatasetFile(Path, Size, PartitionValues);
