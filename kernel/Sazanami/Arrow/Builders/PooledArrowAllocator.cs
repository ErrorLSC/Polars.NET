using System.Buffers;
using System.Runtime.InteropServices;
using Apache.Arrow.Memory;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Arrow.Interop;

namespace Kaminari.Sazanami.Arrow.Builders;

/// <summary>
/// The allocator Apache.Arrow builders build with here: zeroed blocks of <see cref="NativeMemoryPool"/>, 64-byte
/// aligned, freed when the buffer over them is disposed (or, as a safety net, collected).
/// </summary>
/// <remarks>
/// Apache.Arrow's default allocator reports every allocation to the GC as memory pressure: with a managed heap of a
/// few MiB, building tens of MiB of columns forces a full (gen2) collection again and again. Pass this one to every
/// builder's <c>Build(allocator)</c> (Apache.Arrow's default cannot be replaced: <c>MemoryAllocator.Default</c> has no
/// setter).
/// </remarks>
internal sealed class PooledArrowAllocator() : MemoryAllocator(NativeMemoryPool.Alignment)
{
    public static PooledArrowAllocator Instance { get; } = new();

    protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
    {
        bytesAllocated = length;
        return new PooledMemory(new Block(length), length);
    }

    /// <summary>
    /// The memory of an Arrow buffer over a block of <see cref="NativeMemoryPool"/> filled already (its
    /// <paramref name="length"/> first bytes): the block is taken over, freed with the buffer.
    /// </summary>
    public static unsafe IMemoryOwner<byte> Adopt(byte* block, int length) => new PooledMemory(new Block(block), length);

    /// <summary>
    /// One block of the pool as the memory of an Arrow buffer. The block itself is a separate object with the
    /// finalizer (a finalizer on a MemoryManager is CA2015's trap; the same split as NativeBufferMemory and its owner):
    /// an undisposed buffer still gives its block back once collected, as Apache.Arrow's allocator does.
    /// </summary>
    private sealed unsafe class PooledMemory(Block block, int length) : MemoryManager<byte>, INativeMemory
    {
        private readonly Block _block = block;

        public override Span<byte> GetSpan() => new(_block.Pointer, length);

        public byte* Pointer => _block.Pointer;

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThan((uint)elementIndex, (uint)length, nameof(elementIndex));
            return new(_block.Pointer + elementIndex); // native: nothing to pin; the buffer's owners keep it (Retain)
        }

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing) => _block.Free();
    }

    /// <summary>A zeroed pool block, freed once (by <see cref="Free"/> or the finalizer).</summary>
    private sealed unsafe class Block
    {
        private nint _pointer;

        public Block(int length)
        {
            byte* p = NativeMemoryPool.Alloc(Math.Max(length, 1));
            NativeMemory.Clear(p, (nuint)length); // builders count on zeroed memory, as Apache.Arrow's allocator gives
            _pointer = (nint)p;
        }

        /// <summary>Takes over a block of the pool.</summary>
        public Block(byte* block) => _pointer = (nint)block;

        public byte* Pointer => _pointer != 0 ? (byte*)_pointer : throw new ObjectDisposedException(nameof(PooledMemory));

        public void Free()
        {
            nint pointer = Interlocked.Exchange(ref _pointer, 0);
            if (pointer == 0) return;
            NativeMemoryPool.Free((void*)pointer);
            GC.SuppressFinalize(this);
        }

        ~Block() => Free();
    }
}
