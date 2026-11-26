using Kaminari.Seseragi.Files.IO;
using System.Buffers;
using Microsoft.Win32.SafeHandles;

namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>A table on the local disk.</summary>
public sealed class LocalFileSystem : IWritableFileSystem, ITailReadable
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
            // Spark (Hadoop) writes local URIs with one slash, file:/dir/file; .NET reads file:///dir/file.
            if (relative.StartsWith("file:/", StringComparison.Ordinal) && !relative.StartsWith("file://", StringComparison.Ordinal))
                relative = "file://" + relative["file:".Length..];
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
    public bool TryReadAll(string path, IBufferWriter<byte> into)
    {
        ArgumentNullException.ThrowIfNull(into);
        SafeFileHandle file;
        try
        {
            file = File.OpenHandle(Resolve(path));
        }
        catch (Exception e) when (e is FileNotFoundException or DirectoryNotFoundException)
        {
            return false;
        }
        using (file)
        {
            // Straight into the caller's buffer, as much as the file holds (it may grow while read).
            long offset = 0;
            int hint = (int)Math.Clamp(RandomAccess.GetLength(file), 1, 1 << 20);
            while (true)
            {
                Span<byte> span = into.GetSpan(hint);
                int read = RandomAccess.Read(file, span, offset);
                if (read == 0) return true;
                into.Advance(read);
                offset += read;
                hint = 1 << 16;
            }
        }
    }

    /// <inheritdoc/>
    public bool CanReadTails => true;

    /// <inheritdoc/>
    public FileTail? ReadTail(string path, int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        try
        {
            using FileStream stream = File.OpenRead(Resolve(path));
            long size = stream.Length;
            int count = (int)Math.Min(length, size);
            stream.Seek(size - count, SeekOrigin.Begin);
            var buffer = new byte[count];
            stream.ReadExactly(buffer);
            return new FileTail(buffer, size);
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
    public int ReadRange(string path, long offset, Span<byte> destination)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        SafeFileHandle file;
        try
        {
            file = File.OpenHandle(Resolve(path));
        }
        catch (Exception e) when (e is FileNotFoundException or DirectoryNotFoundException)
        {
            return -1;
        }
        using (file)
        {
            int total = 0;
            while (total < destination.Length)
            {
                int read = RandomAccess.Read(file, destination[total..], offset + total);
                if (read == 0) break;
                total += read;
            }
            return total;
        }
    }

    /// <inheritdoc/>
    public bool TryCreate(string path, ReadOnlyMemory<byte> content)
    {
        string target = Resolve(path);
        return LocalFileWrites.Publish(LocalFileWrites.WriteTemp(target, content), target, createOnly: true);
    }

    /// <inheritdoc/>
    /// <remarks>The bytes go to a hidden temporary file beside the target, flushed to disk and renamed at commit.</remarks>
    public FileWriteStream OpenWrite(string path, bool createOnly) => LocalFileWrites.OpenWrite(Resolve(path), createOnly);

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
    public void CreateDirectory(string directory) => Directory.CreateDirectory(Resolve(directory));

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
        LocalFileWrites.Publish(LocalFileWrites.WriteTemp(target, content), target, createOnly: false);
    }

}

