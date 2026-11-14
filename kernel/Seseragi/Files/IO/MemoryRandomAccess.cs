namespace Kaminari.Seseragi.Files.IO;

/// <summary>Bytes in memory read at offsets: a whole small file kept from its first read.</summary>
internal sealed class MemoryRandomAccess(ReadOnlyMemory<byte> bytes) : IRandomAccess, ILocalSource
{
    public long Length => bytes.Length;

    public bool TryGetMemory(out ReadOnlyMemory<byte> memory)
    {
        memory = bytes;
        return true;
    }

    public bool SmallReadsAreCheap => true;

    public Sazanami.Helpers.MappedFile? Map(out long start)
    {
        start = 0;
        return null;
    }

    public int ReadAt(long offset, Span<byte> destination)
    {
        int count = (int)Math.Clamp(bytes.Length - offset, 0, destination.Length);
        if (count > 0) bytes.Span.Slice((int)offset, count).CopyTo(destination);
        return count;
    }

    public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(ReadAt(offset, destination.Span));
    }
}
