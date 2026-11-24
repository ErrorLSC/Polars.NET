namespace Kaminari.Sazanami.Buffers;

/// <summary>
/// The memory of a <see cref="System.Buffers.MemoryManager{T}"/> in native memory that never moves: its address can be
/// taken without pinning while its owners keep it (Arrow writers reading a batch's buffers, the batch held).
/// </summary>
internal unsafe interface INativeMemory
{
    /// <summary>Where the memory starts.</summary>
    byte* Pointer { get; }
}
