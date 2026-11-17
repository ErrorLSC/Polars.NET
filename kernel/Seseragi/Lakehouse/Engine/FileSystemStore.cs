using System.Runtime.CompilerServices;
using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>
/// A file system as a store of files datasets read (an <see cref="IFileStore"/>): paths are URLs below
/// <see cref="Root"/> (such as <c>s3://bucket/dir/x.parquet</c> below <c>s3://bucket</c>), handed to the file system
/// relative to its root. A file is opened by one read of its last bytes (its size, footer and version); later reads
/// require that version, so a file replaced while it is read fails (<see cref="IOException"/>). Reads are asynchronous
/// where the file system is (<see cref="IAsyncFileSystem"/>). Listing needs a file system that lists at any depth
/// (<see cref="IWritableFileSystem"/> or <see cref="IAsyncFileSystem"/>) for recursive listings. A path not ending with
/// '/' names a file (the convention of object stores: nothing is asked to tell directories apart).
/// </summary>
public sealed class FileSystemStore : IFileStore
{
    /// <summary>How many of a file's last bytes are read when it is opened (its footer, usually).</summary>
    internal const int TailRead = 64 * 1024;

    private readonly IFileSystem _fileSystem;

    /// <param name="fileSystem">The file system.</param>
    /// <param name="root">The URL of the file system's root: the prefix of every path of the store.</param>
    public FileSystemStore(IFileSystem fileSystem, string root)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentException.ThrowIfNullOrEmpty(root);
        _fileSystem = fileSystem;
        Root = root.TrimEnd('/');
    }

    /// <summary>The URL of the file system's root.</summary>
    public string Root { get; }

    /// <inheritdoc/>
    public bool CanList => true;

    /// <summary>A path of the store relative to the file system's root (empty for the root).</summary>
    /// <exception cref="ArgumentException">The path is not below the root.</exception>
    private string Relative(string path)
    {
        if (path.TrimEnd('/') == Root) return "";
        if (!path.StartsWith(Root + "/", StringComparison.Ordinal))
            throw new ArgumentException($"'{path}' is not below the store's root '{Root}'.", nameof(path));
        return path[(Root.Length + 1)..];
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        string directory = Relative(prefix).TrimEnd('/');
        IReadOnlyList<FileEntry> entries;
        try
        {
            entries = recursive
                ? _fileSystem switch
                {
                    IAsyncFileSystem async => await async.ListAllAsync(directory, cancellationToken).ConfigureAwait(false),
                    IWritableFileSystem writable => await Task.Run(() => writable.ListAll(directory), cancellationToken).ConfigureAwait(false),
                    _ => throw new NotSupportedException($"The file system of '{Root}' lists one directory at a time: give a glob without '**', or the files."),
                }
                : await Task.Run(() => _fileSystem.List(directory), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e) when (e is not (IOException or NotSupportedException or OperationCanceledException))
        {
            throw new IOException($"{prefix}: listing failed: {e.Message}", e);
        }
        foreach (FileEntry entry in entries) yield return new StoredFile($"{Root}/{entry.Path}", entry.Size, entry.LastModified);
    }

    /// <summary>The file at a path, from a read of its last byte; null when there is none.</summary>
    public async ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) =>
        await TailAsync(path, 1, cancellationToken).ConfigureAwait(false) is { } tail ? new StoredFile(path, tail.Size, null, tail.Version) : null;

    /// <summary>Opens a file whose size is known: nothing is read until it is.</summary>
    public ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult<IRandomAccess>(new Source(this, file.Path, Relative(file.Path), file.Size, file.ETag, null));

    /// <summary>Opens a file with one read of its last bytes (its size, footer and version); null when there is none.</summary>
    public async ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default)
    {
        if (await TailAsync(path, TailRead, cancellationToken).ConfigureAwait(false) is not { } tail) return null;
        return new Source(this, path, Relative(path), tail.Size, tail.Version, tail.Bytes);
    }

    private async ValueTask<FileTail?> TailAsync(string path, int length, CancellationToken cancellationToken)
    {
        string relative = Relative(path);
        try
        {
            return _fileSystem switch
            {
                IAsyncFileSystem async => await async.ReadTailAsync(relative, length, cancellationToken).ConfigureAwait(false),
                ITailReadable { CanReadTails: true } tails => await Task.Run(() => tails.ReadTail(relative, length), cancellationToken).ConfigureAwait(false),
                _ => throw new NotSupportedException($"The file system of '{Root}' cannot read a file's end without its size: list its files first."),
            };
        }
        catch (Exception e) when (e is not (IOException or NotSupportedException or OperationCanceledException))
        {
            throw new IOException($"{path}: {e.Message}", e);
        }
    }

    /// <summary>A file read by ranges at the version first seen; its last bytes, read when it was opened, held.</summary>
    private sealed class Source(FileSystemStore store, string path, string relative, long length, string? version, byte[]? tail)
        : IRandomAccess, IStoredSource
    {
        private string? _version = version;

        public long Length => length;

        public StoredFile Stored => new(path, length, null, Volatile.Read(ref _version));

        public int ReadAt(long offset, Span<byte> destination)
        {
            int count = (int)Math.Clamp(length - offset, 0, destination.Length);
            if (count == 0 || Held(offset, destination[..count])) return count;
            byte[] rented = System.Buffers.ArrayPool<byte>.Shared.Rent(count);
            try
            {
                int read = ReadAsync(offset, rented.AsMemory(0, count), CancellationToken.None).AsTask().GetAwaiter().GetResult();
                rented.AsSpan(0, read).CopyTo(destination);
                return read;
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(rented);
            }
        }

        public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
        {
            int count = (int)Math.Clamp(length - offset, 0, destination.Length);
            if (count == 0 || Held(offset, destination.Span[..count])) return ValueTask.FromResult(count);
            return ReadAsync(offset, destination[..count], cancellationToken);
        }

        private bool Held(long offset, Span<byte> destination)
        {
            if (tail is null || offset < length - tail.Length) return false;
            tail.AsSpan((int)(offset - (length - tail.Length)), destination.Length).CopyTo(destination);
            return true;
        }

        private async ValueTask<int> ReadAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
        {
            try
            {
                if (store._fileSystem is IAsyncFileSystem async)
                {
                    FileRead read = await async.ReadRangeAsync(relative, offset, destination, Volatile.Read(ref _version), cancellationToken).ConfigureAwait(false)
                        ?? throw new FileNotFoundException($"'{path}' no longer exists.", path);
                    if (read.Version is not null) Interlocked.CompareExchange(ref _version, read.Version, null); // required from now on
                    return read.Count;
                }
                int count = await Task.Run(() => store._fileSystem.ReadRange(relative, offset, destination.Span), cancellationToken).ConfigureAwait(false);
                return count >= 0 ? count : throw new FileNotFoundException($"'{path}' no longer exists.", path);
            }
            catch (Exception e) when (e is not (IOException or OperationCanceledException))
            {
                throw new IOException($"{path}: {e.Message}", e);
            }
        }
    }
}
