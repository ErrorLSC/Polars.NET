using System.Buffers;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// Local files written whole or not at all: the bytes go to a hidden temporary file beside the target (its directory
/// created), which is renamed into place at commit; creating, the name is taken only when free (atomically against
/// other writers). Shared by <see cref="LocalFileStore"/> and the lakehouse's local file system.
/// </summary>
internal static class LocalFileWrites
{
    /// <summary>Opens a file for writing at <paramref name="target"/> (a full path): see <see cref="FileWriteStream"/>.</summary>
    public static FileWriteStream OpenWrite(string target, bool createOnly)
    {
        string temp = TempPath(target);
        // Unbuffered: the stream gathers small writes in native memory itself (a FileStream buffer is a managed array
        // per open file, which hundreds of files written side by side carry into the old generation).
        return new LocalWriteStream(new FileStream(temp, FileMode.CreateNew, FileAccess.Write, FileShare.None, 0), temp, target, createOnly);
    }

    /// <summary>
    /// Gives a complete temporary file its name, and removes the temporary name. Creating, the step fails when the
    /// name is taken, so the file appears complete and only one of two writers gets it: File.Move without overwrite
    /// does not do that on Unix (it checks, then renames), so a hard link is made there. Replacing is a rename.
    /// </summary>
    public static bool Publish(string temp, string target, bool createOnly)
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

    /// <summary>A hidden name beside the target (its directory created), so that the final step is a rename.</summary>
    public static string TempPath(string target)
    {
        string directory = Path.GetDirectoryName(target)!;
        Directory.CreateDirectory(directory);
        return Path.Combine(directory, $".{Path.GetFileName(target)}.{Guid.NewGuid():N}.tmp");
    }

    /// <summary>Writes the content to a hidden file beside the target, so that the final step is a rename.</summary>
    public static string WriteTemp(string target, ReadOnlyMemory<byte> content)
    {
        string temp = TempPath(target);
        using (var stream = new FileStream(temp, FileMode.CreateNew, FileAccess.Write))
        {
            stream.Write(content.Span);
            stream.Flush(flushToDisk: true);
        }
        return temp;
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
