using System.Buffers;
using System.Buffers.Binary;
using System.IO.Compression;
using System.IO.Hashing;
using Kaminari.Sazanami.Compression;
using Kaminari.Sazanami.Buffers;
using Snappier;

namespace Kaminari.Seseragi.Files.Avro;

/// <summary>An Avro block codec. Output goes to a native buffer that grows as needed.</summary>
internal abstract class AvroCodec
{
    public abstract string Name { get; }

    /// <summary>Decompresses <paramref name="input"/> into <paramref name="output"/> and returns the length.</summary>
    public abstract int Decompress(ReadOnlySpan<byte> input, NativeByteBuffer output);

    /// <summary>Compresses <paramref name="input"/> into <paramref name="output"/> and returns the length.</summary>
    public abstract int Compress(ReadOnlySpan<byte> input, NativeByteBuffer output);

    /// <summary>The codec a file header names (for reading; levels do not matter there).</summary>
    public static AvroCodec ForName(string? name) => name switch
    {
        null or "" or "null" => NullCodec.Instance,
        "deflate" => new DeflateCodec(CompressionLevel.Optimal),
        "snappy" => SnappyCodec.Instance,
        "zstandard" => new ZstandardCodec(3),
        "bzip2" or "xz" => throw new NotSupportedException($"The Avro codec '{name}' is not supported (supported: null, deflate, snappy, zstandard)."),
        _ => throw new NotSupportedException($"Unknown Avro codec '{name}'."),
    };

    protected static void EnsureCapacity(NativeByteBuffer buffer, long size)
    {
        if (size > Array.MaxLength) throw new AvroFormatException("A decompressed Avro block exceeds 2 GB.");
        buffer.EnsureCapacity(size);
    }

    /// <summary>Doubles the buffer for output of unknown size (contents are kept).</summary>
    protected static void Grow(NativeByteBuffer buffer)
    {
        if (buffer.Capacity >= Array.MaxLength) throw new AvroFormatException("A decompressed Avro block exceeds 2 GB.");
        buffer.EnsureCapacity(Math.Max(4096L, (long)buffer.Capacity * 2));
    }
}

internal sealed class NullCodec : AvroCodec
{
    public static readonly NullCodec Instance = new();
    public override string Name => "null";

    public override int Decompress(ReadOnlySpan<byte> input, NativeByteBuffer output)
    {
        EnsureCapacity(output, input.Length);
        input.CopyTo(output.Span);
        return input.Length;
    }

    public override int Compress(ReadOnlySpan<byte> input, NativeByteBuffer output) => Decompress(input, output);
}

internal sealed class DeflateCodec(CompressionLevel level) : AvroCodec
{
    public override string Name => "deflate";

    public override unsafe int Decompress(ReadOnlySpan<byte> input, NativeByteBuffer output)
    {
        fixed (byte* p = input)
        {
            using var source = new UnmanagedMemoryStream(p, input.Length);
            using var inflater = new DeflateStream(source, CompressionMode.Decompress);
            EnsureCapacity(output, Math.Max(4096L, (long)input.Length * 4));
            int total = 0;
            while (true)
            {
                if (total == output.Capacity) Grow(output);
                int n;
                try
                {
                    n = inflater.Read(output.Span[total..]);
                }
                catch (InvalidDataException e)
                {
                    throw new AvroFormatException("Corrupt deflate block in Avro file.", e);
                }
                if (n == 0) return total;
                total += n;
            }
        }
    }

    public override int Compress(ReadOnlySpan<byte> input, NativeByteBuffer output)
    {
        var sink = new NativeByteBufferStream(output);
        using (var deflater = new DeflateStream(sink, level, leaveOpen: true)) deflater.Write(input);
        return (int)sink.Length;
    }
}

internal sealed class SnappyCodec : AvroCodec
{
    public static readonly SnappyCodec Instance = new();
    public override string Name => "snappy";

    public override int Decompress(ReadOnlySpan<byte> input, NativeByteBuffer output)
    {
        if (input.Length < 4) throw new AvroFormatException("A snappy Avro block is shorter than its CRC.");
        var compressed = input[..^4];
        uint expected = BinaryPrimitives.ReadUInt32BigEndian(input[^4..]);
        int length;
        try
        {
            length = SnappyBlock.UncompressedLength(compressed);
            EnsureCapacity(output, length);
            length = SnappyBlock.Decompress(compressed, output.Span[..length]);
        }
        catch (InvalidDataException e)
        {
            throw new AvroFormatException("Corrupt snappy block in Avro file.", e);
        }
        if (Crc32.HashToUInt32(output.Span[..length]) != expected)
            throw new AvroFormatException("Snappy block CRC mismatch in Avro file.");
        return length;
    }

    public override int Compress(ReadOnlySpan<byte> input, NativeByteBuffer output)
    {
        EnsureCapacity(output, Snappy.GetMaxCompressedLength(input.Length) + 4L);
        int n = Snappy.Compress(input, output.Span);
        BinaryPrimitives.WriteUInt32BigEndian(output.Span[n..], Crc32.HashToUInt32(input));
        return n + 4;
    }
}

internal sealed class ZstandardCodec(int level) : AvroCodec
{
    public override string Name => "zstandard";

    public override int Decompress(ReadOnlySpan<byte> input, NativeByteBuffer output)
    {
        try
        {
            return ZstdBlock.Decompress(input, output, Array.MaxLength);
        }
        catch (InvalidDataException e)
        {
            throw new AvroFormatException($"Corrupt zstandard block in Avro file: {e.Message}", e);
        }
    }

    public override int Compress(ReadOnlySpan<byte> input, NativeByteBuffer output)
    {
        EnsureCapacity(output, ZstdBlock.MaxCompressedLength(input.Length));
        return ZstdBlock.Compress(input, output.Span, level);
    }
}
