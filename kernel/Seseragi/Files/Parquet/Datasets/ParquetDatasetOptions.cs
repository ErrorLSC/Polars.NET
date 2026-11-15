using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>How <see cref="ParquetDataset.Open(string, ParquetDatasetOptions?)"/> finds and describes a dataset's files.</summary>
public sealed record ParquetDatasetOptions
{
    /// <summary>
    /// Where the files are; null for the local file system (a path naming a URL, <c>scheme://…</c>, then needs a store).
    /// </summary>
    public IFileStore? Store { get; init; }

    /// <summary>
    /// File names starting with one of these are left out of listings and globs (Spark's <c>_SUCCESS</c>, temporary
    /// files); null leaves nothing out: hidden files and files starting with '_' are read, as Polars and DuckDB do.
    /// </summary>
    public IReadOnlyList<string>? IgnorePrefixes { get; init; }

    /// <summary>The name of a string column appended to the batches, holding each row's file path (as listed); null adds none.</summary>
    public string? FilePathColumn { get; init; }

    internal void Validate()
    {
        if (FilePathColumn is "") throw new ArgumentException("The file path column needs a name.", nameof(FilePathColumn));
        if (IgnorePrefixes?.Any(string.IsNullOrEmpty) == true)
            throw new ArgumentException("A prefix of files to leave out is empty (it would leave out every file).", nameof(IgnorePrefixes));
    }
}

/// <summary>
/// A file of a dataset: its path in the store, and what is known of it beforehand (its size, from a listing or a log
/// of the files; its footer, read before), so that opening it asks the store for less.
/// </summary>
/// <param name="Path">The path, as the store spells it.</param>
/// <param name="Size">The size in bytes, when known (else the store is asked).</param>
/// <param name="Rows">The rows to read, by their numbers in the file (a deletion vector's complement); null reads every row.</param>
/// <param name="Metadata">The footer, read before (<see cref="ParquetFile.Metadata"/> of the same file); null reads it.</param>
public sealed record ParquetDatasetFile(string Path, long? Size = null, Sazanami.Rows.RowRanges? Rows = null, ParquetMetadata? Metadata = null);
