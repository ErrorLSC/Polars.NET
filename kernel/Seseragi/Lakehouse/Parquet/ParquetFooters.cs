using System.Runtime.ExceptionServices;
using Kaminari.Seseragi.Files.Parquet;
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
    /// <exception cref="ParquetFormatException">A file is not a Parquet file.</exception>
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
                    result[i] = (end.Size, FooterFromTail(fileSystem, paths[i], end));
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
            result[i] = (size, ReadFooter(fileSystem, paths[i], size));
        }
        return result;
    }

    /// <summary>Reads the footer of a file of known size with at most two ranged reads.</summary>
    /// <exception cref="ParquetFormatException">The file is not a Parquet file, or its footer is damaged.</exception>
    /// <exception cref="FileNotFoundException">The file does not exist.</exception>
    public static ParquetMetadata ReadFooter(IFileSystem fileSystem, string path, long size)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        if (size < 12) throw new ParquetFormatException($"{path} is too short to be a Parquet file.");
        // Most footers are small: one read of the last 64 KiB usually holds the whole footer.
        int guess = (int)Math.Min(size, ParquetMetadata.TailGuess);
        byte[] tail = fileSystem.ReadRange(path, size - guess, guess) ?? throw new FileNotFoundException($"{path} does not exist.", path);
        if (tail.Length != guess) throw new ParquetFormatException($"{path} is shorter than its listed size.");
        return FooterFromTail(fileSystem, path, new FileTail(tail, size));
    }

    /// <summary>
    /// Reads the footer of a file from its end (as <see cref="ITailReadable.ReadTail"/> gives it), with one more
    /// ranged read when the footer is longer than the tail.
    /// </summary>
    /// <exception cref="ParquetFormatException">The file is not a Parquet file, or its footer is damaged.</exception>
    /// <exception cref="FileNotFoundException">The file does not exist.</exception>
    public static ParquetMetadata FooterFromTail(IFileSystem fileSystem, string path, FileTail end)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        var (offset, length) = ParquetMetadata.FooterRange(end.Bytes, end.Size, path);
        long tailStart = end.Size - end.Bytes.Length;
        if (offset >= tailStart) return ParquetMetadata.Parse(end.Bytes.AsSpan((int)(offset - tailStart), length));
        byte[] footer = fileSystem.ReadRange(path, offset, length) ?? throw new FileNotFoundException($"{path} does not exist.", path);
        return ParquetMetadata.Parse(footer);
    }

    private static FileNotFoundException Missing(string path) =>
        new($"The data file {path} was not found after it was written.", path);

    private static string DirectoryOf(string relative) => relative.LastIndexOf('/') is int i and >= 0 ? relative[..i] : "";
}
