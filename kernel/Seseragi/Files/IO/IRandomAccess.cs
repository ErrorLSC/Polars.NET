namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// Data read at offsets, whatever the position of other readers of it: a file, a range-readable object in a store,
/// memory. Implementations must allow concurrent calls.
/// </summary>
public interface IRandomAccess
{
    /// <summary>The length of the data in bytes.</summary>
    long Length { get; }

    /// <summary>Reads <paramref name="destination"/>.Length bytes at <paramref name="offset"/>; fewer only at the end.</summary>
    /// <returns>The number of bytes read.</returns>
    int ReadAt(long offset, Span<byte> destination);

    /// <inheritdoc cref="ReadAt"/>
    ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken);
}
