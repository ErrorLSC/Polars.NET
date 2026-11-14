using System.Buffers;
using System.IO.MemoryMappedFiles;

namespace Kaminari.Sazanami.Helpers;

/// <summary>
/// A file mapped read-only into memory, its ranges handed out as <see cref="ReadOnlyMemory{T}"/> without copying (each
/// range up to 2 GiB; the file may be larger). The mapping lives while its owner and any user that acquired it hold it:
/// <see cref="TryAcquire"/> before reading ranges, <see cref="Release"/> after; the owner releases once when done.
/// </summary>
/// <remarks>
/// Pages are read from the file as they are touched. If another process truncates the file while it is mapped, touching
/// the missing pages ends the process (SIGBUS on Unix, an access violation on Windows): map only files that do not change
/// while being read.
/// </remarks>
internal sealed unsafe class MappedFile
{
    private readonly MemoryMappedFile _map;
    private readonly MemoryMappedViewAccessor _view;
    private readonly byte* _pointer;
    private int _holders = 1; // the owner, then each acquirer

    private MappedFile(MemoryMappedFile map, MemoryMappedViewAccessor view, long length)
    {
        _map = map;
        _view = view;
        Length = length;
        byte* p = null;
        view.SafeMemoryMappedViewHandle.AcquirePointer(ref p);
        _pointer = p + view.PointerOffset;
    }

    /// <summary>Maps <paramref name="file"/> (left open); null when it is empty.</summary>
    public static MappedFile? Open(FileStream file)
    {
        long length = file.Length;
        if (length == 0) return null;
        var map = MemoryMappedFile.CreateFromFile(file, null, 0, MemoryMappedFileAccess.Read, HandleInheritability.None, leaveOpen: true);
        try
        {
            return new MappedFile(map, map.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read), length);
        }
        catch
        {
            map.Dispose();
            throw;
        }
    }

    /// <summary>The length of the file when it was mapped.</summary>
    public long Length { get; }

    /// <summary>Takes a hold on the mapping; false once it was released for good.</summary>
    public bool TryAcquire()
    {
        int holders = Volatile.Read(ref _holders);
        while (holders > 0)
        {
            int seen = Interlocked.CompareExchange(ref _holders, holders + 1, holders);
            if (seen == holders) return true;
            holders = seen;
        }
        return false;
    }

    /// <summary>Lets go of a hold; the last one unmaps the file.</summary>
    public void Release()
    {
        if (Interlocked.Decrement(ref _holders) != 0) return;
        _view.SafeMemoryMappedViewHandle.ReleasePointer();
        _view.Dispose();
        _map.Dispose();
    }

    /// <summary>Bytes [<paramref name="offset"/>, + <paramref name="length"/>) of the file, valid while a hold is kept.</summary>
    public ReadOnlyMemory<byte> Range(long offset, int length)
    {
        if (offset < 0 || length < 0 || offset > Length - length) throw new ArgumentOutOfRangeException(nameof(offset));
        return new Pointer(_pointer + offset, length).Memory;
    }

    private sealed class Pointer(byte* pointer, int length) : MemoryManager<byte>
    {
        public override Span<byte> GetSpan() => new(pointer, length);

        public override MemoryHandle Pin(int elementIndex = 0) => new(pointer + elementIndex);

        public override void Unpin() { }

        protected override void Dispose(bool disposing) { }
    }
}
