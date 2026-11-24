namespace Kaminari.Seseragi.Files.Csv;

/// <summary>
/// How <see cref="CsvDataset.Open(string, CsvDatasetOptions?)"/> finds and describes a dataset's files (see
/// <see cref="DatasetOptions"/>), and how they are written (<see cref="Format"/>). The columns are given
/// (<see cref="DatasetOptions.Schema"/>), or inferred from the first records of the first <see cref="InferSchemaFiles"/>
/// files (<see cref="DatasetSchemaMode.FirstFile"/>) or of every file (<see cref="DatasetSchemaMode.UnionByName"/>),
/// united as if they were one file: integers with floats are floats, dates with datetimes datetimes, other mixes strings.
/// Each file's columns are matched by name (their order is free; without a header they are numbered
/// <c>column_1</c>, <c>column_2</c> …) and its values decoded into the dataset's types: a dataset's column a file lacks is an
/// error unless <see cref="DatasetOptions.AllowMissingColumns"/> (then nulls; with it the inferred columns are those
/// of every file inferred, in the order first seen), a file's column the dataset lacks is an error unless
/// <see cref="IgnoreExtraColumns"/>. <see cref="DatasetOptions.AllowLossyCasts"/> and
/// <see cref="DatasetOptions.UnifyDictionaries"/> do not apply.
/// </summary>
public sealed record CsvDatasetOptions : DatasetOptions
{
    /// <summary>
    /// How every file is written and its values read: separator, quote, line end, header, comment prefix, skipped lines
    /// and rows, null, true and false values, missing fields, ragged lines, decimal comma, thousands separator, dates,
    /// encoding, invalid text, empty input, and for inference <see cref="CsvReadOptions.InferSchemaLength"/> (records per
    /// file), <see cref="CsvReadOptions.SchemaOverrides"/> and <see cref="CsvReadOptions.ViewTypes"/>. Its
    /// <see cref="CsvReadOptions.Schema"/> must be null (the dataset's is <see cref="DatasetOptions.Schema"/>); what a read
    /// reads (columns, filter, rows, row index, batches, threads) is the reader's (<see cref="CsvDataset.OpenReader"/>).
    /// </summary>
    public CsvReadOptions Format { get; init; } = new();

    /// <summary>
    /// The files the columns are inferred from, first in order (10 by default, as dataframe engines take); values of a
    /// later file the types cannot hold are errors. <see cref="DatasetSchemaMode.UnionByName"/> takes every file.
    /// </summary>
    public int InferSchemaFiles { get; init; } = 10;

    /// <summary>Whether a file's columns the dataset does not have are left out (false, the default: an error naming the file and the column).</summary>
    public bool IgnoreExtraColumns { get; init; }

    internal override void Validate()
    {
        base.Validate();
        ArgumentNullException.ThrowIfNull(Format, nameof(Format));
        CsvReader.Check(Format);
        if (Format.Schema is not null)
            throw new ArgumentException("Give the dataset's columns as its Schema, not as Format.Schema.", nameof(Format));
        if (Schema is not null && Format.SchemaOverrides is not null)
            throw new ArgumentException("Give either a schema or schema overrides, not both.", nameof(Format));
        if (InferSchemaFiles < 1) throw new ArgumentOutOfRangeException(nameof(InferSchemaFiles), InferSchemaFiles, "At least one file's records are taken in.");
        if (AllowLossyCasts) throw new ArgumentException("CSV values are decoded into the dataset's types: there is no conversion to allow.", nameof(AllowLossyCasts));
        if (UnifyDictionaries) throw new ArgumentException("CSV values are decoded into the dataset's types: there is no file's dictionary to unify.", nameof(UnifyDictionaries));
    }
}

/// <summary>A file of a CSV dataset (see <see cref="DatasetFile"/>).</summary>
/// <param name="Path">The path, as the store spells it.</param>
/// <param name="Size">The size in bytes, when known (else the store is asked).</param>
/// <param name="PartitionValues">The file's partition values by column (see <see cref="DatasetFile.PartitionValues"/>).</param>
public sealed record CsvDatasetFile(string Path, long? Size = null, IReadOnlyDictionary<string, object?>? PartitionValues = null)
    : DatasetFile(Path, Size, PartitionValues);
