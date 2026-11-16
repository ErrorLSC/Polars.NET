namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>A read of part of a file: the bytes read, and the file's version when the read tells it.</summary>
/// <param name="Count">The bytes read (fewer than asked when the file ends first; none past its end).</param>
/// <param name="Version">The file's version (an object store's ETag or generation), when the read tells it.</param>
public readonly record struct FileRead(int Count, string? Version);

/// <summary>
/// A file system read asynchronously, many files at once (an engine scanning a dataset over a network): ranges read
/// straight into the caller's memory, at a version the caller requires, so that a file replaced while it is read is
/// noticed rather than mixed. Paths are as <see cref="IFileSystem"/> takes them.
/// </summary>
public interface IAsyncFileSystem
{
    /// <summary>
    /// Reads the bytes of a file from <paramref name="offset"/> into <paramref name="destination"/>, requiring the file
    /// to be at <paramref name="version"/> when given; null when there is no file.
    /// </summary>
    /// <exception cref="IOException">The file is not at that version (it changed), or the read failed.</exception>
    ValueTask<FileRead?> ReadRangeAsync(string path, long offset, Memory<byte> destination, string? version, CancellationToken cancellationToken);

    /// <summary>The last <paramref name="length"/> bytes of a file (all of them when it is shorter), its size and version; null when there is no file.</summary>
    ValueTask<FileTail?> ReadTailAsync(string path, int length, CancellationToken cancellationToken);

    /// <summary>The files below a directory, at any depth (as <see cref="IFileSystem"/> lists, recursively).</summary>
    ValueTask<IReadOnlyList<FileEntry>> ListAllAsync(string directory, CancellationToken cancellationToken);
}
