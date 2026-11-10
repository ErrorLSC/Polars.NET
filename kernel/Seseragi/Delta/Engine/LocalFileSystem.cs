namespace Kaminari.Seseragi.Delta.Engine;

/// <summary>A table on the local disk.</summary>
public sealed class LocalFileSystem : IFileSystem
{
    private readonly string _root;

    /// <summary>Creates a file system rooted at the table directory.</summary>
    public LocalFileSystem(string tableDirectory)
    {
        if (string.IsNullOrEmpty(tableDirectory)) throw new ArgumentException("The table directory is required.", nameof(tableDirectory));
        _root = Path.GetFullPath(tableDirectory);
    }

    /// <summary>The absolute table directory.</summary>
    public string Root => _root;

    private string Resolve(string relative) =>
        relative.Length == 0 ? _root : Path.Combine(_root, relative.Replace('/', Path.DirectorySeparatorChar));

    /// <inheritdoc/>
    public IReadOnlyList<FileEntry> List(string directory, string? startFrom = null)
    {
        var dir = new DirectoryInfo(Resolve(directory));
        if (!dir.Exists) return [];

        string prefix = directory.Length == 0 ? "" : directory.TrimEnd('/') + "/";
        var result = new List<FileEntry>();
        foreach (FileInfo file in dir.EnumerateFiles())
        {
            if (startFrom is not null && string.CompareOrdinal(file.Name, startFrom) < 0) continue;
            result.Add(new FileEntry(prefix + file.Name, file.Length, file.LastWriteTimeUtc));
        }
        result.Sort(static (a, b) => string.CompareOrdinal(a.Path, b.Path));
        return result;
    }

    /// <inheritdoc/>
    public byte[]? ReadAll(string path)
    {
        try
        {
            return File.ReadAllBytes(Resolve(path));
        }
        catch (FileNotFoundException)
        {
            return null;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }

    /// <inheritdoc/>
    public byte[]? ReadRange(string path, long offset, int length)
    {
        try
        {
            using FileStream stream = File.OpenRead(Resolve(path));
            stream.Seek(offset, SeekOrigin.Begin);
            var buffer = new byte[length];
            int read = stream.ReadAtLeast(buffer, length, throwOnEndOfStream: false);
            return read == length ? buffer : buffer[..read];
        }
        catch (FileNotFoundException)
        {
            return null;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }
}
