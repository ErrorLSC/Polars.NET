namespace Kaminari.Seseragi.Files.Csv.Reading;

/// <summary>
/// A file's column names (its header's, or numbered) and what its first records hold (the kinds of each column's
/// values: null when the file has no record or inference is off), for a dataset to infer its columns from several files.
/// </summary>
internal sealed class CsvHead(string[] names, CsvInference? inference) : IDisposable
{
    public string[] Names => names;

    public CsvInference? Inference => inference;

    public void Dispose() => inference?.Dispose();
}
