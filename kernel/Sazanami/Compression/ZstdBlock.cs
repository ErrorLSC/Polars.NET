using ZstdSharp;

namespace Kaminari.Sazanami.Compression;

/// <summary>
/// One-shot Zstandard frames over ZstdSharp (a managed port, no native library), with a decompression context and a
/// compression context (per level) kept per thread. Shared by the formats that compress whole buffers with Zstandard
/// (Parquet pages, Arrow IPC body buffers).
/// </summary>
internal static class ZstdBlock
{
    /// <summary>The level Zstandard itself defaults to.</summary>
    public const int DefaultLevel = 3;

    [ThreadStatic] private static Decompressor? t_decompressor;
    [ThreadStatic] private static Compressor? t_compressor;
    [ThreadStatic] private static int t_level;

    /// <summary>Decompresses <paramref name="source"/> (one or more frames) into <paramref name="destination"/>; returns the bytes written.</summary>
    /// <exception cref="InvalidDataException">The data is damaged or does not fit.</exception>
    public static int Decompress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        try
        {
            return (t_decompressor ??= new Decompressor()).Unwrap(source, destination);
        }
        catch (Exception e) when (e is ZstdException or ArgumentException)
        {
            throw new InvalidDataException($"Zstandard data is damaged: {e.Message}", e);
        }
    }

    /// <summary>The content size the first frame of <paramref name="source"/> declares; -1 when it declares none.</summary>
    /// <exception cref="InvalidDataException">The frame header is damaged.</exception>
    public static long DeclaredLength(ReadOnlySpan<byte> source)
    {
        try
        {
            ulong declared = Decompressor.GetDecompressedSize(source);
            return declared == 0 || declared > long.MaxValue ? -1 : (long)declared;
        }
        catch (ZstdException e)
        {
            throw new InvalidDataException($"Zstandard data is damaged: {e.Message}", e);
        }
    }

    /// <summary>The largest frame <paramref name="length"/> input bytes can compress to.</summary>
    public static int MaxCompressedLength(int length) => Compressor.GetCompressBound(length);

    /// <summary>Compresses <paramref name="source"/> into one frame at <paramref name="level"/>; returns the compressed length.</summary>
    public static int Compress(ReadOnlySpan<byte> source, Span<byte> destination, int level = DefaultLevel)
    {
        if (t_compressor is null || t_level != level)
        {
            t_compressor?.Dispose();
            t_compressor = new Compressor(level);
            t_level = level;
        }
        return t_compressor.Wrap(source, destination);
    }
}
