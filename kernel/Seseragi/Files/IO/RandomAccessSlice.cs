namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// The data of a source from <paramref name="offset"/> on, read at offsets relative to it (a text input whose first
/// lines are passed over). A local source stays local: its memory sliced, its mapping shifted.
/// </summary>
internal sealed class RandomAccessSlice(IRandomAccess source, long offset) : IRandomAccess, ILocalSource
{
    public long Length => Math.Max(0, source.Length - offset);

    public int ReadAt(long at, Span<byte> destination) => source.ReadAt(offset + at, destination);

    public ValueTask<int> ReadAtAsync(long at, Memory<byte> destination, CancellationToken cancellationToken) =>
        source.ReadAtAsync(offset + at, destination, cancellationToken);

    public bool TryGetMemory(out ReadOnlyMemory<byte> memory)
    {
        if (source is ILocalSource local && local.TryGetMemory(out ReadOnlyMemory<byte> whole))
        {
            memory = whole[(int)Math.Min(offset, whole.Length)..];
            return true;
        }
        memory = default;
        return false;
    }

    public bool SmallReadsAreCheap => source is ILocalSource { SmallReadsAreCheap: true };

    public Sazanami.Buffers.MappedFile? Map(out long start)
    {
        start = 0;
        if (source is not ILocalSource local || local.Map(out long from) is not { } mapped) return null;
        start = from + offset;
        return mapped;
    }
}
