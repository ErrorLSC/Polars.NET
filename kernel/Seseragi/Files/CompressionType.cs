using System.IO.Compression;
using Kaminari.Seseragi.Files.Avro;

namespace Kaminari.Seseragi.Files;

/// <summary>
/// A compression codec with its settings. Codecs are shared between the file formats of this package; which
/// format accepts which codec is fixed at compile time by marker interfaces (<see cref="IAvroCompression"/>),
/// so passing a codec a format cannot write does not compile.
/// </summary>
public abstract record CompressionType
{
    private protected CompressionType() { }
}

/// <summary>A codec an Avro object container file can use. Implemented only by this package's codecs.</summary>
public interface IAvroCompression
{
    internal AvroCodec CreateAvroCodec();
}

/// <summary>No compression (Avro codec <c>null</c>).</summary>
public sealed record NoCompression : CompressionType, IAvroCompression
{
    /// <summary>The shared instance.</summary>
    public static NoCompression Instance { get; } = new();

    AvroCodec IAvroCompression.CreateAvroCodec() => NullCodec.Instance;
}

/// <summary>Raw deflate (RFC 1951; Avro codec <c>deflate</c>).</summary>
/// <param name="Level">0–9 as in zlib. The runtime offers coarser steps: 0 stores, 1–3 is fastest,
/// 4–6 optimal, 7–9 smallest.</param>
public sealed record DeflateCompression(int Level = 6) : CompressionType, IAvroCompression
{
    /// <summary>The zlib-style level, 0–9.</summary>
    public int Level { get; } = Level is >= 0 and <= 9
        ? Level
        : throw new ArgumentOutOfRangeException(nameof(Level), Level, "Deflate levels run from 0 to 9.");

    internal CompressionLevel RuntimeLevel => Level switch
    {
        0 => CompressionLevel.NoCompression,
        <= 3 => CompressionLevel.Fastest,
        <= 6 => CompressionLevel.Optimal,
        _ => CompressionLevel.SmallestSize,
    };

    AvroCodec IAvroCompression.CreateAvroCodec() => new DeflateCodec(RuntimeLevel);
}

/// <summary>Snappy (Avro codec <c>snappy</c>: each block carries the CRC32 of its uncompressed bytes).</summary>
public sealed record SnappyCompression : CompressionType, IAvroCompression
{
    /// <summary>The shared instance.</summary>
    public static SnappyCompression Instance { get; } = new();

    AvroCodec IAvroCompression.CreateAvroCodec() => SnappyCodec.Instance;
}

/// <summary>Zstandard (Avro codec <c>zstandard</c>).</summary>
/// <param name="Level">1–22; 3 is the library default, higher is smaller and slower.</param>
public sealed record ZstandardCompression(int Level = 3) : CompressionType, IAvroCompression
{
    /// <summary>The compression level, 1–22.</summary>
    public int Level { get; } = Level is >= 1 and <= 22
        ? Level
        : throw new ArgumentOutOfRangeException(nameof(Level), Level, "Zstandard levels run from 1 to 22.");

    AvroCodec IAvroCompression.CreateAvroCodec() => new ZstandardCodec(Level);
}
