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
}
