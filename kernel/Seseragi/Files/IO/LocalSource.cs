namespace Kaminari.Seseragi.Files.IO;

/// <summary>
/// What a source of this library tells readers that can choose how to read it: whether its bytes are already in
/// memory (read by slicing, without a copy) or in a local file (cheap enough to read piece by piece). Other sources,
/// such as objects in a store, are best read with few large ranged reads.
/// </summary>
internal interface ILocalSource
{
    /// <summary>The whole source as memory, when it lives in memory.</summary>
    bool TryGetMemory(out ReadOnlyMemory<byte> memory);

    /// <summary>Whether small ranged reads are cheap (a local file).</summary>
    bool SmallReadsAreCheap { get; }

    /// <summary>
    /// The read-only mapping of a local file (made on first use, shared, unmapped once the source and every holder let
    /// go), with where the source starts in it; null for other sources, an empty file, or a disposed source.
    /// </summary>
    Sazanami.Helpers.MappedFile? Map(out long start);
}
