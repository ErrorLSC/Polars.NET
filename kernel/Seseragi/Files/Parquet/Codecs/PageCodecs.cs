using System.Buffers.Binary;
using System.IO.Compression;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Snappier;
using ZstdSharp;

namespace Kaminari.Seseragi.Files.Parquet.Codecs;

/// <summary>
/// The page codecs of Parquet. Pages carry their uncompressed size, so decompression writes into a destination of
/// exactly that size and must fill it. Compression is here too, for the writer.
/// </summary>
internal static class PageCodecs
{
    [ThreadStatic] private static Decompressor? t_zstdDecompressor;

    /// <summary>Whether this library reads pages compressed with <paramref name="codec"/>.</summary>
    public static bool CanDecompress(CompressionCodec codec) => codec is CompressionCodec.Uncompressed or CompressionCodec.Snappy
        or CompressionCodec.Gzip or CompressionCodec.Brotli or CompressionCodec.Zstd or CompressionCodec.Lz4Raw or CompressionCodec.Lz4;

    public static string Name(CompressionCodec codec) => codec switch
    {
        CompressionCodec.Uncompressed => "UNCOMPRESSED",
        CompressionCodec.Snappy => "SNAPPY",
        CompressionCodec.Gzip => "GZIP",
        CompressionCodec.Lzo => "LZO",
        CompressionCodec.Brotli => "BROTLI",
        CompressionCodec.Lz4 => "LZ4 (Hadoop)",
        CompressionCodec.Zstd => "ZSTD",
        CompressionCodec.Lz4Raw => "LZ4_RAW",
        _ => $"codec {(int)codec}",
    };

    /// <summary>Decompresses <paramref name="source"/> into all of <paramref name="destination"/>.</summary>
    /// <exception cref="ParquetFormatException">The data is damaged, has another size, or the codec is not supported.</exception>
    public static void Decompress(CompressionCodec codec, ReadOnlySpan<byte> source, Span<byte> destination)
    {
        int written;
        try
        {
            written = codec switch
            {
                CompressionCodec.Uncompressed => Copy(source, destination),
                CompressionCodec.Snappy => Snappy.Decompress(source, destination),
                CompressionCodec.Gzip => Gzip(source, destination),
                CompressionCodec.Brotli => BrotliDecoder.TryDecompress(source, destination, out int n) ? n : throw Damaged(codec),
                CompressionCodec.Zstd => (t_zstdDecompressor ??= new Decompressor()).Unwrap(source, destination),
                CompressionCodec.Lz4Raw => Lz4Block.Decompress(source, destination),
                CompressionCodec.Lz4 => Lz4Hadoop(source, destination),
                _ => throw new ParquetFormatException($"Pages compressed with {Name(codec)} are not supported."),
            };
        }
        catch (Exception e) when (e is InvalidDataException or ZstdException or ArgumentException or IndexOutOfRangeException)
        {
            throw new ParquetFormatException($"A {Name(codec)} page is damaged: {e.Message}", e);
        }
        if (written != destination.Length)
            throw new ParquetFormatException($"A {Name(codec)} page decompressed to {written} bytes; its header says {destination.Length}.");
    }

    private static int Copy(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (source.Length != destination.Length) return source.Length;
        source.CopyTo(destination);
        return source.Length;
    }

    /// <summary>GZIP, reading every member (some writers concatenate several in one page).</summary>
    private static unsafe int Gzip(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (source.IsEmpty) return 0; // .NET writes nothing for empty input; the size check decides
        fixed (byte* p = source)
        {
            using var input = new UnmanagedMemoryStream(p, source.Length);
            using var gzip = new GZipStream(input, CompressionMode.Decompress);
            int total = gzip.ReadAtLeast(destination, destination.Length, throwOnEndOfStream: false);
            if (total == destination.Length && gzip.Read(stackalloc byte[1]) != 0) return total + 1; // more than declared
            return total;
        }
    }

    /// <summary>
    /// The deprecated LZ4 codec: Hadoop's framing (blocks of a big-endian decompressed size, a big-endian compressed
    /// size and an LZ4 block); files whose pages do not parse that way hold a bare LZ4 block, as old parquet-cpp
    /// wrote them.
    /// </summary>
    private static int Lz4Hadoop(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (TryHadoop(source, destination, out int written)) return written;
        return Lz4Block.Decompress(source, destination);

        static bool TryHadoop(ReadOnlySpan<byte> source, Span<byte> destination, out int written)
        {
            written = 0;
            while (source.Length > 0)
            {
                if (source.Length < 8) return false;
                uint expanded = BinaryPrimitives.ReadUInt32BigEndian(source);
                uint compressed = BinaryPrimitives.ReadUInt32BigEndian(source[4..]);
                source = source[8..];
                if (compressed > (uint)source.Length || expanded > (uint)(destination.Length - written)) return false;
                try
                {
                    if (Lz4Block.Decompress(source[..(int)compressed], destination.Slice(written, (int)expanded)) != expanded) return false;
                }
                catch (ParquetFormatException)
                {
                    return false;
                }
                written += (int)expanded;
                source = source[(int)compressed..];
            }
            return written == destination.Length;
        }
    }

    private static ParquetFormatException Damaged(CompressionCodec codec) => new($"A {Name(codec)} page is damaged.");

    // ------------------------------------------------------------------
    // Compression (for the writer)
    // ------------------------------------------------------------------

    /// <summary>An upper bound of the compressed size of <paramref name="length"/> bytes.</summary>
    public static int MaxCompressedLength(CompressionCodec codec, int length) => codec switch
    {
        CompressionCodec.Uncompressed => length,
        CompressionCodec.Snappy => Snappy.GetMaxCompressedLength(length),
        CompressionCodec.Zstd => Compressor.GetCompressBound(length),
        CompressionCodec.Brotli => BrotliEncoder.GetMaxCompressedLength(length),
        CompressionCodec.Lz4Raw => Lz4Block.MaxCompressedLength(length),
        CompressionCodec.Gzip => length + length / 1000 + 64,
        _ => throw new NotSupportedException($"This library does not write {Name(codec)} pages."),
    };

    /// <summary>Compresses <paramref name="source"/> into <paramref name="destination"/> (at least <see cref="MaxCompressedLength"/> long).</summary>
    /// <returns>The compressed length.</returns>
    public static unsafe int Compress(CompressionCodec codec, ReadOnlySpan<byte> source, Span<byte> destination, int level = 0)
    {
        switch (codec)
        {
            case CompressionCodec.Uncompressed:
                source.CopyTo(destination);
                return source.Length;
            case CompressionCodec.Snappy:
                return Snappy.Compress(source, destination);
            case CompressionCodec.Zstd:
            {
                using var compressor = new Compressor(level == 0 ? 3 : level);
                return compressor.Wrap(source, destination);
            }
            case CompressionCodec.Brotli:
                return BrotliEncoder.TryCompress(source, destination, out int n, level == 0 ? 4 : level, 22) ? n : throw new ArgumentException("The destination is too small.");
            case CompressionCodec.Lz4Raw:
                return Lz4Block.Compress(source, destination);
            case CompressionCodec.Gzip:
                fixed (byte* p = destination)
                {
                    using var output = new UnmanagedMemoryStream(p, 0, destination.Length, FileAccess.Write);
                    using (var gzip = new GZipStream(output, level is >= 1 and <= 3 ? CompressionLevel.Fastest : CompressionLevel.Optimal, leaveOpen: true))
                        gzip.Write(source);
                    return (int)output.Position;
                }
            default:
                throw new NotSupportedException($"This library does not write {Name(codec)} pages.");
        }
    }
}
