namespace Kaminari.Seseragi.Files.IO;

/// <summary>A file in an <see cref="IFileStore"/>: its path as the store spells it, its size, and what the store tells of its version.</summary>
/// <param name="Path">The path, as the store spells it (a local path, an object key, a URL).</param>
/// <param name="Size">The size in bytes.</param>
/// <param name="LastModified">When it was last written, when the store tells.</param>
/// <param name="ETag">The store's version tag of its content, when it has one.</param>
public readonly record struct StoredFile(string Path, long Size, DateTimeOffset? LastModified = null, string? ETag = null);

/// <summary>
/// Where files are read from: a local file system, memory, an object store, an HTTP server. Readers of datasets list
/// directories through it and open files at offsets (<see cref="IRandomAccess"/>). Implementations must allow
/// concurrent calls.
/// </summary>
/// <remarks>
/// Paths are the store's own spelling: local paths for a local file system, keys or URLs for remote stores, with '/'
/// separating directories. A source an open returns that is <see cref="IDisposable"/> or <see cref="IAsyncDisposable"/>
/// belongs to the caller, who disposes it when done.
/// </remarks>
public interface IFileStore
{
    /// <summary>Whether the store can list directories (a plain HTTP server cannot: only files named are read).</summary>
    bool CanList { get; }

    /// <summary>
    /// The files in the directory <paramref name="prefix"/> (empty: the store's root), in no particular order: those
    /// directly in it, or with <paramref name="recursive"/> those at any depth below it. Directories themselves are
    /// not listed; a directory that does not exist lists nothing.
    /// </summary>
    /// <exception cref="NotSupportedException">The store cannot list (<see cref="CanList"/> is false).</exception>
    IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default);

    /// <summary>The file at <paramref name="path"/>; null when there is none (or it is a directory).</summary>
    ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default);

    /// <summary>Opens a file whose size is known (from a listing, or a log of the files), without asking the store for it.</summary>
    /// <exception cref="FileNotFoundException">The file does not exist.</exception>
    ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default);

    /// <summary>Opens the file at <paramref name="path"/>, learning its size; null when there is none.</summary>
    ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default);
}

/// <summary>A store that can tell cheaply whether a path is a directory (a local file system, memory); for others a path ending with '/' is one.</summary>
internal interface IDirectoryAware
{
    /// <summary>Whether <paramref name="path"/> is a directory of the store.</summary>
    bool IsDirectory(string path);
}
