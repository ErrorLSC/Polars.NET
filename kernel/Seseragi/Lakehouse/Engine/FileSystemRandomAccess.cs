using Kaminari.Seseragi.Files.IO;

namespace Kaminari.Seseragi.Lakehouse.Engine;

/// <summary>A file of a table's storage read at offsets: each read is one <see cref="IFileSystem.ReadRange"/>.</summary>
/// <param name="fileSystem">The storage.</param>
/// <param name="path">The file, relative to the table root or an absolute URI.</param>
/// <param name="length">The file's size, as a listing or the log gives it.</param>
internal sealed class FileSystemRandomAccess(IFileSystem fileSystem, string path, long length) : IRandomAccess
{
    /// <inheritdoc/>
    public long Length => length;

    /// <inheritdoc/>
    /// <exception cref="FileNotFoundException">The file is gone.</exception>
    public int ReadAt(long offset, Span<byte> destination)
    {
        if (destination.IsEmpty || offset >= length) return 0;
        byte[] bytes = fileSystem.ReadRange(path, offset, destination.Length)
            ?? throw new FileNotFoundException($"{path} disappeared while it was read.", path);
        int count = Math.Min(bytes.Length, destination.Length);
        bytes.AsSpan(0, count).CopyTo(destination);
        return count;
    }

    /// <inheritdoc/>
    public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(ReadAt(offset, destination.Span));
    }
}
