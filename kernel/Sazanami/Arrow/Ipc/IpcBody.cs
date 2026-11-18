using System.Buffers.Binary;
using System.Runtime.InteropServices;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Compression;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>A range of a message body that is in memory: where it starts in the body, where it is, how long.</summary>
internal readonly record struct IpcBodySegment(long Start, nint Pointer, long Length);

/// <summary>
/// The body of one IPC message as far as it is in memory (whole, or the ranges a projection needs), owned by
/// <see cref="IpcBodyMemory"/>: hands out each buffer as a native slice, decompressed or copied when needed.
/// </summary>
internal sealed unsafe class IpcBody(IpcBodyMemory memory, long length, IpcBodySegment[] segments)
{
    public IpcBodyMemory Memory { get; } = memory;

    public long Length { get; } = length;

    /// <summary>Compressed buffers decompressed ahead by <see cref="Prepare"/>, by buffer index (unset: not prepared, or it failed).</summary>
    private IpcSlice[]? _ready;

    /// <summary>
    /// Decompresses the <paramref name="needed"/> buffers of a compressed batch on up to <paramref name="parallelism"/>
    /// threads at once, for <see cref="Slice(int, IpcBuffer, IpcCompression, string)"/> to hand out. A buffer that fails
    /// is left for the walk to decompress again, which then reports it with its field.
    /// </summary>
    /// <param name="from">The compressed bytes of the needed buffers from which they are decompressed side by side.</param>
    public void Prepare(IpcRecordBatch header, bool[] needed, int parallelism, long from = ParallelFrom)
    {
        if (header.Compression == IpcCompression.None || parallelism <= 1) return;
        int count = 0;
        long bytes = 0;
        for (int i = 0; i < needed.Length; i++)
        {
            if (!needed[i] || header.Buffers[i].Length <= 8) continue;
            count++;
            bytes += header.Buffers[i].Length;
        }
        if (count < 2 || bytes < from) return;
        var ready = new IpcSlice[needed.Length];
        var blocks = new nint[needed.Length];
        IpcCompression compression = header.Compression;
        Parallel.For(0, needed.Length, new ParallelOptions { MaxDegreeOfParallelism = parallelism }, i =>
        {
            if (!needed[i] || header.Buffers[i].Length <= 8) return;
            try
            {
                ready[i] = Expand(header.Buffers[i], compression, "", out blocks[i]);
            }
            catch (InvalidDataException)
            {
                // Decompressed again by the walk, which names the field.
            }
        });
        foreach (nint block in blocks)
        {
            if (block != 0) Memory.Adopt((byte*)block);
        }
        _ready = ready;
    }

    /// <summary>Compressed bytes of a batch from which <see cref="Prepare"/> decompresses on several threads.</summary>
    internal const long ParallelFrom = 1 << 20;

    /// <summary>Buffer <paramref name="index"/> of a record batch, as <see cref="Prepare"/> left it or else as <see cref="Slice(IpcBuffer, IpcCompression, string)"/> makes it.</summary>
    public IpcSlice Slice(int index, IpcBuffer buffer, IpcCompression compression, string where) =>
        _ready is { } ready && ready[index].Owner is not null ? ready[index] : Slice(buffer, compression, where);

    /// <summary>
    /// Buffer <paramref name="buffer"/> of a record batch: in place when stored uncompressed and 8-byte aligned; copied
    /// when not aligned; decompressed into memory of its own when compressed (the 8-byte uncompressed length first; -1
    /// for a buffer stored as is; an empty buffer may omit it).
    /// </summary>
    public IpcSlice Slice(IpcBuffer buffer, IpcCompression compression, string where)
    {
        IpcSlice slice = Expand(buffer, compression, where, out nint block);
        if (block != 0) Memory.Adopt((byte*)block);
        return slice;
    }

    /// <summary>
    /// A buffer as <see cref="Slice(IpcBuffer, IpcCompression, string)"/> describes; a block it allocated (a copy, or
    /// the decompressed bytes) comes back in <paramref name="block"/> for the caller to adopt into <see cref="Memory"/>
    /// (several threads may expand at once; adopting is not thread-safe).
    /// </summary>
    private IpcSlice Expand(IpcBuffer buffer, IpcCompression compression, string where, out nint block)
    {
        block = 0;
        if (buffer.Length == 0) return IpcSlice.Empty;
        byte* p = At(buffer.Offset, buffer.Length);
        if (compression == IpcCompression.None) return InPlace(p, buffer.Length, where, out block);
        if (buffer.Length < 8) throw IpcValidation.Invalid(where, $"a compressed buffer of {buffer.Length} bytes has no length");
        long expanded = BinaryPrimitives.ReadInt64LittleEndian(new ReadOnlySpan<byte>(p, 8));
        if (expanded == -1) return InPlace(p + 8, buffer.Length - 8, where, out block);
        if (expanded < 0) throw IpcValidation.Invalid(where, $"a compressed buffer declares {expanded} bytes");
        if (expanded > int.MaxValue) throw TooLarge(expanded, where);
        if (expanded == 0) return IpcSlice.Empty;
        if (buffer.Length - 8 > int.MaxValue) throw TooLarge(buffer.Length - 8, where);
        var source = new ReadOnlySpan<byte>(p + 8, (int)(buffer.Length - 8));
        // A forged length must not make the reader allocate: no codec expands data past these ratios (LZ4 about 255:1;
        // Zstandard RLE blocks about 32768:1; its first frame may declare its own size).
        long declared = -1;
        try
        {
            if (compression == IpcCompression.Zstd) declared = ZstdBlock.DeclaredLength(source);
        }
        catch (InvalidDataException e)
        {
            throw IpcValidation.Invalid(where, e.Message.TrimEnd('.'));
        }
        long most = compression == IpcCompression.Lz4Frame ? 256L * source.Length + 1024 : 32_768L * source.Length + 1024;
        if (expanded > most) throw IpcValidation.Invalid(where, $"a buffer of {source.Length} compressed bytes declares {expanded} bytes");
        if (declared > expanded) throw IpcValidation.Invalid(where, $"a Zstandard frame of {declared} bytes in a buffer that declares {expanded}");
        byte* destination = NativeMemoryPool.Alloc(expanded);
        var target = new Span<byte>(destination, (int)expanded);
        int written;
        try
        {
            written = compression == IpcCompression.Lz4Frame ? Lz4Frame.Decompress(source, target) : ZstdBlock.Decompress(source, target);
            if (written != expanded) throw IpcValidation.Invalid(where, $"a buffer decompressed to {written} bytes; it declares {expanded}");
        }
        catch (InvalidDataException e)
        {
            NativeMemoryPool.Free(destination);
            if (e.Message.StartsWith("Arrow IPC", StringComparison.Ordinal)) throw;
            throw IpcValidation.Invalid(where, e.Message.TrimEnd('.'));
        }
        block = (nint)destination;
        return new IpcSlice(Memory, (nint)destination, (int)expanded);
    }

    private IpcSlice InPlace(byte* p, long length, string where, out nint block)
    {
        block = 0;
        if (length > int.MaxValue) throw TooLarge(length, where);
        if (((nint)p & 7) == 0) return new IpcSlice(Memory, (nint)p, (int)length);
        byte* copy = NativeMemoryPool.Alloc(Math.Max(length, 1)); // misaligned (a damaged or unusual writer): values are read in place as wider types
        NativeMemory.Copy(p, copy, (nuint)length);
        block = (nint)copy;
        return new IpcSlice(Memory, (nint)copy, (int)length);
    }

    private byte* At(long offset, long length)
    {
        foreach (IpcBodySegment s in segments)
        {
            if (offset >= s.Start && offset - s.Start <= s.Length - length) return (byte*)s.Pointer + (offset - s.Start);
        }
        throw new InvalidOperationException($"Body bytes [{offset}, +{length}) were not read.");
    }

    private static InvalidDataException TooLarge(long bytes, string where) =>
        new($"Arrow IPC data has a buffer of {bytes} bytes ({where}); buffers over 2 GiB are not supported.");
}
