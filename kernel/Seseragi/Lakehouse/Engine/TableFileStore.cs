using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>
/// A table's writable file system as a store files are written to (a dataset sink's): paths are relative to the
/// table's root, as the file system takes them. It only writes and deletes: reading goes through the file system (or a
/// <see cref="FileSystemStore"/>).
/// </summary>
internal sealed class TableFileStore(IWritableFileSystem fileSystem) : IWritableFileStore
{
    /// <inheritdoc/>
    public FileWriteStream OpenWrite(string path, bool createOnly) => fileSystem.OpenWrite(path, createOnly);

    /// <inheritdoc/>
    public async ValueTask<bool> DeleteAsync(string path, CancellationToken cancellationToken = default) =>
        await Task.Run(() => fileSystem.Delete(path), cancellationToken).ConfigureAwait(false);

    /// <summary>False: the store only writes.</summary>
    public bool CanList => false;

    /// <exception cref="NotSupportedException">Always: the store only writes.</exception>
    public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) => throw WriteOnly();

    /// <exception cref="NotSupportedException">Always: the store only writes.</exception>
    public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => throw WriteOnly();

    /// <exception cref="NotSupportedException">Always: the store only writes.</exception>
    public ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) => throw WriteOnly();

    /// <exception cref="NotSupportedException">Always: the store only writes.</exception>
    public ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) => throw WriteOnly();

    private static NotSupportedException WriteOnly() => new("A table's file store only writes files: read them through the table's file system.");
}
