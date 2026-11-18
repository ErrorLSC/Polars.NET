using System.Buffers.Binary;
using System.IO.Hashing;

namespace Kaminari.Sazanami.Compression;

/// <summary>
/// Decompression of the LZ4 frame format (lz4_Frame_format.md): a magic number, a frame descriptor (flags, block
/// maximum size, optional content size and dictionary id, a header checksum), data blocks (a 4-byte size whose high bit
/// marks a stored block, the data, an optional block checksum), an end mark and an optional content checksum. Blocks
/// are decoded straight into the destination; linked blocks reach back into what is already there. Concatenated
/// frames and skippable frames are read in order. Checksums (XxHash32) are verified when present. Used by Arrow IPC
/// (LZ4_FRAME body compression).
/// </summary>
internal static class Lz4Frame
{
    private const uint Magic = 0x184D2204;
    private const uint SkippableMask = 0xFFFFFFF0;
    private const uint SkippableMagic = 0x184D2A50;

    /// <summary>Decompresses every frame of <paramref name="source"/> into <paramref name="destination"/>; returns the bytes written.</summary>
    /// <exception cref="InvalidDataException">A frame is damaged, uses a dictionary, or does not fit.</exception>
    public static int Decompress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        int ip = 0, op = 0;
        if (source.IsEmpty) throw Damaged("no frame");
        while (ip < source.Length)
        {
            if (source.Length - ip < 4) throw Damaged("a truncated magic number");
            uint magic = BinaryPrimitives.ReadUInt32LittleEndian(source[ip..]);
            if ((magic & SkippableMask) == SkippableMagic)
            {
                if (source.Length - ip < 8) throw Damaged("a truncated skippable frame");
                uint size = BinaryPrimitives.ReadUInt32LittleEndian(source[(ip + 4)..]);
                if (size > (uint)(source.Length - ip - 8)) throw Damaged("a truncated skippable frame");
                ip += 8 + (int)size;
                continue;
            }
            if (magic != Magic) throw Damaged("not an LZ4 frame");
            op = Frame(source, ref ip, destination, op);
        }
        return op;
    }

    private static int Frame(ReadOnlySpan<byte> source, ref int ip, Span<byte> destination, int op)
    {
        int descriptor = ip + 4;
        if (source.Length - descriptor < 3) throw Damaged("a truncated frame descriptor");
        byte flags = source[descriptor], bd = source[descriptor + 1];
        if (flags >> 6 != 1) throw Damaged($"frame version {flags >> 6}");
        if ((flags & 0x02) != 0 || (bd & 0x8F) != 0) throw Damaged("reserved descriptor bits are set");
        bool independent = (flags & 0x20) != 0, blockChecksums = (flags & 0x10) != 0, contentChecksum = (flags & 0x04) != 0;
        bool hasSize = (flags & 0x08) != 0, hasDictionary = (flags & 0x01) != 0;
        int maxBlock = ((bd >> 4) & 7) switch
        {
            4 => 64 << 10,
            5 => 256 << 10,
            6 => 1 << 20,
            7 => 4 << 20,
            var m => throw Damaged($"block maximum size code {m}"),
        };
        int headerEnd = descriptor + 2 + (hasSize ? 8 : 0) + (hasDictionary ? 4 : 0);
        if (headerEnd >= source.Length) throw Damaged("a truncated frame descriptor");
        if (hasDictionary) throw Damaged("frames that need a dictionary are not supported");
        byte check = (byte)(XxHash32.HashToUInt32(source[descriptor..headerEnd]) >> 8);
        if (source[headerEnd] != check) throw Damaged("the frame descriptor checksum does not match");
        long contentSize = hasSize ? BinaryPrimitives.ReadInt64LittleEndian(source[(descriptor + 2)..]) : -1;

        int frameStart = op;
        ip = headerEnd + 1;
        while (true)
        {
            if (source.Length - ip < 4) throw Damaged("a truncated block size");
            uint word = BinaryPrimitives.ReadUInt32LittleEndian(source[ip..]);
            ip += 4;
            if (word == 0) break; // the end mark
            bool stored = (word & 0x80000000) != 0;
            int size = (int)(word & 0x7FFFFFFF);
            if (size > maxBlock || size > source.Length - ip) throw Damaged("a block is larger than allowed or truncated");
            ReadOnlySpan<byte> block = source.Slice(ip, size);
            ip += size;
            if (blockChecksums)
            {
                if (source.Length - ip < 4) throw Damaged("a truncated block checksum");
                if (BinaryPrimitives.ReadUInt32LittleEndian(source[ip..]) != XxHash32.HashToUInt32(block)) throw Damaged("a block checksum does not match");
                ip += 4;
            }
            int written;
            if (stored)
            {
                if (size > destination.Length - op) throw Damaged("the data is longer than declared");
                block.CopyTo(destination[op..]);
                written = size;
            }
            else
            {
                // Independent blocks see only themselves; linked blocks see the frame's output so far.
                int history = independent ? 0 : op - frameStart;
                Span<byte> window = destination.Slice(op - history, Math.Min(destination.Length - (op - history), history + maxBlock));
                written = Lz4Block.Decompress(block, window, history);
            }
            op += written;
        }
        if (contentChecksum)
        {
            if (source.Length - ip < 4) throw Damaged("a truncated content checksum");
            if (BinaryPrimitives.ReadUInt32LittleEndian(source[ip..]) != XxHash32.HashToUInt32(destination[frameStart..op])) throw Damaged("the content checksum does not match");
            ip += 4;
        }
        if (contentSize >= 0 && contentSize != op - frameStart) throw Damaged($"the frame holds {op - frameStart} bytes; its descriptor says {contentSize}");
        return op;
    }

    private static InvalidDataException Damaged(string what) => new($"An LZ4 frame is damaged: {what}.");
}
