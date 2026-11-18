using System.Buffers.Binary;
using System.Runtime.InteropServices;
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

    /// <summary>
    /// Buffer <paramref name="buffer"/> of a record batch: in place when stored uncompressed and 8-byte aligned; copied
    /// when not aligned; decompressed into memory of its own when compressed (the 8-byte uncompressed length first; -1
    /// for a buffer stored as is; an empty buffer may omit it).
    /// </summary>
    public IpcSlice Slice(IpcBuffer buffer, IpcCompression compression, string where)
    {
        if (buffer.Length == 0) return IpcSlice.Empty;
        byte* p = At(buffer.Offset, buffer.Length);
        if (compression == IpcCompression.None) return InPlace(p, buffer.Length, where);
        if (buffer.Length < 8) throw IpcValidation.Invalid(where, $"a compressed buffer of {buffer.Length} bytes has no length");
        long expanded = BinaryPrimitives.ReadInt64LittleEndian(new ReadOnlySpan<byte>(p, 8));
        if (expanded == -1) return InPlace(p + 8, buffer.Length - 8, where);
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
        byte* destination = Memory.Allocate(expanded);
        var target = new Span<byte>(destination, (int)expanded);
        int written;
        try
        {
            written = compression == IpcCompression.Lz4Frame ? Lz4Frame.Decompress(source, target) : ZstdBlock.Decompress(source, target);
        }
        catch (InvalidDataException e)
        {
            throw IpcValidation.Invalid(where, e.Message.TrimEnd('.'));
        }
        if (written != expanded) throw IpcValidation.Invalid(where, $"a buffer decompressed to {written} bytes; it declares {expanded}");
        return new IpcSlice(Memory, (nint)destination, (int)expanded);
    }

    private IpcSlice InPlace(byte* p, long length, string where)
    {
        if (length > int.MaxValue) throw TooLarge(length, where);
        if (((nint)p & 7) == 0) return new IpcSlice(Memory, (nint)p, (int)length);
        byte* copy = Memory.Allocate(length); // misaligned (a damaged or unusual writer): values are read in place as wider types
        NativeMemory.Copy(p, copy, (nuint)length);
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
