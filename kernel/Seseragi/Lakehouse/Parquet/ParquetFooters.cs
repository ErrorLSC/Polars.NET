using System.Runtime.ExceptionServices;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Parquet;

/// <summary>The sizes and footers of files just written, as a commit needs them for its add actions.</summary>
public static class ParquetFooters
{
    /// <summary>How many footers are read at once from a file system that reads tails.</summary>
    internal const int Parallelism = 16;

    /// <summary>
    /// The size and footer of every file, in the given order. A file system that reads tails gives both in one read
    /// per file, several files at once; any other is listed once per directory for the sizes, then each footer is
    /// read.
    /// </summary>
    /// <exception cref="FileNotFoundException">A file does not exist.</exception>
    /// <exception cref="InvalidDataException">A file is not a Parquet file.</exception>
    public static (long Size, ParquetMetadata Footer)[] Read(IFileSystem fileSystem, IReadOnlyList<string> paths)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(paths);
        var result = new (long Size, ParquetMetadata Footer)[paths.Count];

        if (fileSystem is ITailReadable { CanReadTails: true } tails)
        {
            try
            {
                Parallel.For(0, paths.Count, new ParallelOptions { MaxDegreeOfParallelism = Parallelism }, i =>
                {
                    FileTail end = tails.ReadTail(paths[i], ParquetMetadata.TailGuess) ?? throw Missing(paths[i]);
                    result[i] = (end.Size, ParquetMetadata.FromTail(fileSystem, paths[i], end));
                });
            }
            catch (AggregateException e) when (e.InnerExceptions.Count > 0)
            {
                ExceptionDispatchInfo.Capture(e.InnerExceptions[0]).Throw();
            }
            return result;
        }

        // Sizes from one listing per directory (the store has no per-file size call), then each footer.
        var sizes = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (string directory in paths.Select(DirectoryOf).Distinct(StringComparer.Ordinal))
        {
            foreach (FileEntry entry in fileSystem.List(directory)) sizes[entry.Path] = entry.Size;
        }
        for (int i = 0; i < paths.Count; i++)
        {
            if (!sizes.TryGetValue(paths[i], out long size)) throw Missing(paths[i]);
            result[i] = (size, ParquetMetadata.Read(fileSystem, paths[i], size));
        }
        return result;
    }

    private static FileNotFoundException Missing(string path) =>
        new($"The data file {path} was not found after it was written.", path);

    private static string DirectoryOf(string relative) => relative.LastIndexOf('/') is int i and >= 0 ? relative[..i] : "";
}
