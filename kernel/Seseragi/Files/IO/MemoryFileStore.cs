using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// Files held in memory as an <see cref="IFileStore"/> (tests, data received whole): '/'-separated paths, files read
/// in place. Files can be added and removed while the store is used.
/// </summary>
public sealed class MemoryFileStore : IFileStore, IDirectoryAware
{
    private readonly ConcurrentDictionary<string, (ReadOnlyMemory<byte> Bytes, DateTimeOffset? LastModified)> _files = new(StringComparer.Ordinal);

    /// <summary>Adds a file; its bytes are read in place and must not change while the store holds them.</summary>
    /// <exception cref="ArgumentException">The path is empty, ends with '/', or a file has it already.</exception>
    public void Add(string path, ReadOnlyMemory<byte> bytes, DateTimeOffset? lastModified = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        if (path.EndsWith('/')) throw new ArgumentException($"'{path}' names a directory, not a file.", nameof(path));
        if (!_files.TryAdd(path, (bytes, lastModified))) throw new ArgumentException($"The store has a file '{path}' already.", nameof(path));
    }

    /// <summary>Removes a file; false when there was none.</summary>
    public bool Remove(string path) => _files.TryRemove(path, out _);

    /// <inheritdoc/>
    public bool CanList => true;

    private static string Directory(string prefix) => prefix.Length == 0 || prefix.EndsWith('/') ? prefix : prefix + '/';

    /// <inheritdoc/>
    public bool IsDirectory(string path)
    {
        string directory = Directory(path);
        return _files.Keys.Any(k => k.StartsWith(directory, StringComparison.Ordinal));
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        string directory = Directory(prefix);
        foreach (var (path, file) in _files)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (path.StartsWith(directory, StringComparison.Ordinal) && (recursive || path.IndexOf('/', directory.Length) < 0))
                yield return new StoredFile(path, file.Bytes.Length, file.LastModified);
        }
        await Task.CompletedTask.ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult<StoredFile?>(_files.TryGetValue(path, out var file) ? new StoredFile(path, file.Bytes.Length, file.LastModified) : null);

    /// <inheritdoc/>
    public ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult<IRandomAccess>(_files.TryGetValue(file.Path, out var held)
            ? new MemoryRandomAccess(held.Bytes)
            : throw new FileNotFoundException($"The store has no file '{file.Path}'.", file.Path));

    /// <inheritdoc/>
    public ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) =>
        ValueTask.FromResult<IRandomAccess?>(_files.TryGetValue(path, out var held) ? new MemoryRandomAccess(held.Bytes) : null);
}
