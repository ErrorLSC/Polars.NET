using System.Buffers;
using Kaminari.Sazanami.Buffers;
using ZstdSharp;
using ZstdSharp.Unsafe;

namespace Kaminari.Sazanami.Compression;

/// <summary>
/// One-shot Zstandard frames over ZstdSharp (a managed port, no native library), with a decompression context and a
/// compression context (per level) kept per thread. Shared by the formats that compress whole buffers with Zstandard
/// (Parquet pages, Arrow IPC body buffers, Avro blocks).
/// </summary>
internal static unsafe class ZstdBlock
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

    private const ulong Unknown = ulong.MaxValue, Error = ulong.MaxValue - 1; // ZSTD_CONTENTSIZE_UNKNOWN, ZSTD_CONTENTSIZE_ERROR

    /// <summary>
    /// Decompresses <paramref name="source"/> (one or more frames) into <paramref name="output"/>, grown as needed: to
    /// the sizes the frames declare, or, when one declares none (streaming writers), as the bytes come. Returns the
    /// bytes written.
    /// </summary>
    /// <exception cref="InvalidDataException">The data is damaged, ends early, or comes to more than <paramref name="maxLength"/> bytes.</exception>
    public static int Decompress(ReadOnlySpan<byte> source, NativeByteBuffer output, long maxLength = int.MaxValue)
    {
        var decompressor = t_decompressor ??= new Decompressor();
        try
        {
            ulong total;
            fixed (byte* p = source) total = Methods.ZSTD_findDecompressedSize(p, (nuint)source.Length);
            if (total == Error) throw new InvalidDataException("Zstandard data is damaged.");
            if (total != Unknown)
            {
                if (total > (ulong)maxLength) throw new InvalidDataException($"Zstandard data declares {total} bytes, more than {maxLength}.");
                output.EnsureCapacity((long)total);
                return decompressor.Unwrap(source, output.Span[..(int)total]);
            }
            // A frame declares no content size: stream it into the growing buffer, frame after frame.
            decompressor.ResetStream();
            try
            {
                output.EnsureCapacity(Math.Min(maxLength, Math.Max(4096L, (long)source.Length * 4)));
                int read = 0, written = 0;
                while (true)
                {
                    if (written == output.Capacity)
                    {
                        if (output.Capacity >= maxLength) throw new InvalidDataException($"Zstandard data comes to more than {maxLength} bytes.");
                        output.EnsureCapacity(Math.Min(maxLength, Math.Max(4096L, (long)output.Capacity * 2)));
                    }
                    OperationStatus status = decompressor.UnwrapStream(source[read..], output.Span[written..], out int consumed, out int produced);
                    read += consumed;
                    written += produced;
                    if (status == OperationStatus.Done && read == source.Length) return written;
                    if (status == OperationStatus.InvalidData) throw new InvalidDataException("Zstandard data is damaged.");
                    if (status == OperationStatus.NeedMoreData && read == source.Length && produced == 0) throw new InvalidDataException("Zstandard data ends early.");
                    if (status == OperationStatus.Done) decompressor.ResetStream(); // the next frame
                }
            }
            finally
            {
                decompressor.ResetStream(); // ready for one-shot use again
            }
        }
        catch (Exception e) when (e is ZstdException or ArgumentException)
        {
            throw new InvalidDataException($"Zstandard data is damaged: {e.Message}", e);
        }
    }

    /// <summary>
    /// The content size the first frame of <paramref name="source"/> declares in its header; -1 when it declares none
    /// (a streaming writer's). Not ZstdSharp's GetDecompressedSize, which gives a bound for such frames.
    /// </summary>
    /// <exception cref="InvalidDataException">The frame header is damaged.</exception>
    public static long DeclaredLength(ReadOnlySpan<byte> source)
    {
        ulong declared;
        fixed (byte* p = source) declared = Methods.ZSTD_getFrameContentSize(p, (nuint)source.Length);
        if (declared == Error) throw new InvalidDataException("Zstandard data is damaged: no frame header.");
        return declared == Unknown || declared > long.MaxValue ? -1 : (long)declared;
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
