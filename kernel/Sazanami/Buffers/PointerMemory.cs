using System.Buffers;

namespace Kaminari.Sazanami.Buffers;

/// <summary>
/// Memory over native bytes another owner holds (a mapping, an <see cref="UnmanagedMemoryStream"/>'s buffer): valid
/// only while that owner keeps them; disposing the manager frees nothing.
/// </summary>
internal sealed unsafe class PointerMemory(byte* pointer, int length) : MemoryManager<byte>, INativeMemory
{
    /// <summary>The bytes [<paramref name="pointer"/>, + <paramref name="length"/>) as memory.</summary>
    public static ReadOnlyMemory<byte> Of(byte* pointer, int length) => new PointerMemory(pointer, length).Memory;

    public override Span<byte> GetSpan() => new(pointer, length);

    public byte* Pointer => pointer;

    public override MemoryHandle Pin(int elementIndex = 0) => new(pointer + elementIndex);

    public override void Unpin() { }

    protected override void Dispose(bool disposing) { }
}
