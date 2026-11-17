using Kaminari.Sazanami.Buffers;
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
        return Publish(WriteTemp(target, content), target, createOnly: true);
    }

    /// <summary>
    /// Gives a complete temporary file its name, and removes the temporary name. Creating, the step fails when the
    /// name is taken, so the file appears complete and only one of two writers gets it: File.Move without overwrite
    /// does not do that on Unix (it checks, then renames), so a hard link is made there. Replacing is a rename.
    /// </summary>
    private static bool Publish(string temp, string target, bool createOnly)
    {
        try
        {
            if (!createOnly)
            {
                File.Move(temp, target, overwrite: true);
                return true;
            }
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
            File.Delete(temp); // gone already after a move
        }
    }

    /// <inheritdoc/>
    /// <remarks>The bytes go to a hidden temporary file beside the target, flushed to disk and renamed at commit.</remarks>
    public FileWriteStream OpenWrite(string path, bool createOnly)
    {
        string target = Resolve(path);
        string temp = TempPath(target);
        // Unbuffered: the stream gathers small writes in native memory itself (a FileStream buffer is a managed array
        // per open file, which hundreds of files written side by side carry into the old generation).
        return new LocalWriteStream(new FileStream(temp, FileMode.CreateNew, FileAccess.Write, FileShare.None, 0), temp, target, createOnly);
    }

    /// <summary>
    /// A file being written to its temporary path: writes of <see cref="Gather"/> bytes and more go straight to the
    /// file, smaller ones are gathered in a native buffer first (written out when it fills, and at commit).
    /// </summary>
    private sealed class LocalWriteStream(FileStream file, string temp, string target, bool createOnly) : FileWriteStream
    {
        private const int Gather = 16 * 1024;
        private NativeBufferWriter? _small;

        protected override void WriteCore(ReadOnlySpan<byte> buffer)
        {
            if (buffer.Length >= Gather)
            {
                WriteGathered();
                file.Write(buffer);
                return;
            }
            _small ??= new NativeBufferWriter(Gather);
            if (_small.WrittenCount + buffer.Length > Gather) WriteGathered();
            _small.Write(buffer);
        }

        protected override async ValueTask WriteCoreAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken)
        {
            if (buffer.Length >= Gather)
            {
                await WriteGatheredAsync(cancellationToken).ConfigureAwait(false);
                await file.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
                return;
            }
            _small ??= new NativeBufferWriter(Gather);
            if (_small.WrittenCount + buffer.Length > Gather) await WriteGatheredAsync(cancellationToken).ConfigureAwait(false);
            _small.Write(buffer.Span);
        }

        private void WriteGathered()
        {
            if (_small is not { WrittenCount: > 0 } small) return;
            file.Write(small.WrittenSpan);
            small.ResetWrittenCount();
        }

        private async ValueTask WriteGatheredAsync(CancellationToken cancellationToken)
        {
            if (_small is not { WrittenCount: > 0 } small) return;
            await file.WriteAsync(small.WrittenMemory, cancellationToken).ConfigureAwait(false);
            small.ResetWrittenCount();
        }

        protected override bool CommitCore()
        {
            WriteGathered();
            FreeGathered();
            file.Flush(flushToDisk: SyncOnCommit);
            file.Dispose();
            return Publish(temp, target, createOnly);
        }

        protected override async ValueTask<bool> CommitCoreAsync(CancellationToken cancellationToken)
        {
            await WriteGatheredAsync(cancellationToken).ConfigureAwait(false);
            FreeGathered();
            await file.FlushAsync(cancellationToken).ConfigureAwait(false);
            if (SyncOnCommit) file.Flush(flushToDisk: true);
            await file.DisposeAsync().ConfigureAwait(false);
            return Publish(temp, target, createOnly);
        }

        protected override void Abort()
        {
            FreeGathered();
            file.Dispose();
            File.Delete(temp);
        }

        protected override void Release() => FreeGathered();

        private void FreeGathered()
        {
            _small?.Dispose();
            _small = null;
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
        Publish(WriteTemp(target, content), target, createOnly: false);
    }

    /// <summary>A hidden name beside the target (its directory created), so that the final step is a rename.</summary>
    private static string TempPath(string target)
    {
        string directory = Path.GetDirectoryName(target)!;
        Directory.CreateDirectory(directory);
        return Path.Combine(directory, $".{Path.GetFileName(target)}.{Guid.NewGuid():N}.tmp");
    }

    /// <summary>Writes the content to a hidden file beside the target, so that the final step is a rename.</summary>
    private static string WriteTemp(string target, ReadOnlyMemory<byte> content)
    {
        string temp = TempPath(target);
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
