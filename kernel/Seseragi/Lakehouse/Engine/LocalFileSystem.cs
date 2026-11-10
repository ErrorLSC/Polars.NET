namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>A table on the local disk.</summary>
public sealed class LocalFileSystem : IWritableFileSystem
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

    private string Resolve(string relative)
    {
        if (relative.Length == 0) return _root;
        if (relative.Contains("://", StringComparison.Ordinal) || relative.StartsWith("file:", StringComparison.Ordinal))
        {
            if (!Uri.TryCreate(relative, UriKind.Absolute, out Uri? uri) || !uri.IsFile)
                throw new NotSupportedException($"The local file system cannot read '{relative}'.");
            return uri.LocalPath; // decodes %XX escapes
        }
        return Path.IsPathRooted(relative) ? relative : Path.Combine(_root, relative.Replace('/', Path.DirectorySeparatorChar));
    }

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
            try
            {
                result.Add(new FileEntry(prefix + file.Name, file.Length, file.LastWriteTimeUtc));
            }
            catch (FileNotFoundException)
            {
                // Gone since it was listed, such as another writer's temporary file: not part of the listing.
            }
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

    /// <inheritdoc/>
    public bool TryCreate(string path, ReadOnlyMemory<byte> content)
    {
        string target = Resolve(path);
        string temp = WriteTemp(target, content);
        try
        {
            // The whole file is written under a temporary name, then given its name in one step that fails when
            // the name is taken, so the file appears complete and only one of two writers gets it. File.Move
            // without overwrite does not do that on Unix (it checks, then renames), so a hard link is made there.
            if (OperatingSystem.IsWindows())
            {
                try
                {
                    File.Move(temp, target, overwrite: false); // MoveFileEx without replacing: atomic
                    return true;
                }
                catch (IOException) when (File.Exists(target))
                {
                    return false;
                }
            }
            return NoReplace.Link(temp, target);
        }
        finally
        {
            File.Delete(temp);
        }
    }

    /// <inheritdoc/>
    public IReadOnlyList<FileEntry> ListAll(string directory)
    {
        var dir = new DirectoryInfo(Resolve(directory));
        if (!dir.Exists) return [];
        var result = new List<FileEntry>();
        foreach (FileInfo file in dir.EnumerateFiles("*", SearchOption.AllDirectories))
        {
            try
            {
                string relative = Path.GetRelativePath(Root, file.FullName).Replace(Path.DirectorySeparatorChar, '/');
                result.Add(new FileEntry(relative, file.Length, file.LastWriteTimeUtc));
            }
            catch (FileNotFoundException)
            {
                // Gone since it was listed.
            }
        }
        result.Sort(static (a, b) => string.CompareOrdinal(a.Path, b.Path));
        return result;
    }

    /// <inheritdoc/>
    public IReadOnlyList<string> ListDirectories(string directory)
    {
        var dir = new DirectoryInfo(Resolve(directory));
        if (!dir.Exists) return [];
        var result = new List<string>();
        foreach (DirectoryInfo sub in dir.EnumerateDirectories("*", SearchOption.AllDirectories))
            result.Add(Path.GetRelativePath(Root, sub.FullName).Replace(Path.DirectorySeparatorChar, '/'));
        result.Sort(StringComparer.Ordinal);
        return result;
    }

    /// <inheritdoc/>
    public bool DeleteDirectoryIfEmpty(string directory)
    {
        string target = Resolve(directory);
        if (directory.Length == 0 || !Directory.Exists(target) || Directory.EnumerateFileSystemEntries(target).Any()) return false;
        try
        {
            Directory.Delete(target, recursive: false);
            return true;
        }
        catch (IOException)
        {
            return false; // something was written into it meanwhile
        }
    }

    /// <inheritdoc/>
    public bool Delete(string path)
    {
        string target = Resolve(path);
        if (!File.Exists(target)) return false;
        File.Delete(target);
        return true;
    }

    /// <inheritdoc/>
    public void Overwrite(string path, ReadOnlyMemory<byte> content)
    {
        string target = Resolve(path);
        string temp = WriteTemp(target, content);
        try
        {
            File.Move(temp, target, overwrite: true);
        }
        catch
        {
            File.Delete(temp);
            throw;
        }
    }

    /// <summary>Writes the content to a hidden file beside the target, so that the final step is a rename.</summary>
    private static string WriteTemp(string target, ReadOnlyMemory<byte> content)
    {
        string directory = Path.GetDirectoryName(target)!;
        Directory.CreateDirectory(directory);
        string temp = Path.Combine(directory, $".{Path.GetFileName(target)}.{Guid.NewGuid():N}.tmp");
        using (var stream = new FileStream(temp, FileMode.CreateNew, FileAccess.Write))
        {
            stream.Write(content.Span);
            stream.Flush(flushToDisk: true);
        }
        return temp;
    }
}

/// <summary>Creates a name for a file only when the name is free, atomically, through the system call.</summary>
internal static partial class NoReplace
{
    private const int EEXIST = 17; // the same value on Linux and macOS

    [System.Runtime.InteropServices.LibraryImport("libc", EntryPoint = "link", SetLastError = true,
        StringMarshalling = System.Runtime.InteropServices.StringMarshalling.Utf8)]
    private static partial int link(string existing, string created);

    /// <summary>Links <paramref name="existing"/> under <paramref name="created"/>; false when that name exists.</summary>
    public static bool Link(string existing, string created)
    {
        if (link(existing, created) == 0) return true;
        int errno = System.Runtime.InteropServices.Marshal.GetLastPInvokeError();
        if (errno == EEXIST) return false;
        throw new IOException($"Could not create '{created}' (errno {errno}). The file system must support hard links for commits.");
    }
}
