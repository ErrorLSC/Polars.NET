using System.IO.Compression;
using Kaminari.Seseragi.Files.Avro;
using Kaminari.Seseragi.Files.Parquet.Metadata;

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

/// <summary>A codec Parquet pages can use. Implemented only by this package's codecs.</summary>
public interface IParquetCompression
{
    /// <summary>The Parquet codec.</summary>
    internal CompressionCodec ParquetCodec { get; }

    /// <summary>The level passed to the compressor (0: the codec has none).</summary>
    internal int ParquetLevel { get; }
}

/// <summary>No compression (Avro codec <c>null</c>, Parquet <c>UNCOMPRESSED</c>).</summary>
public sealed record NoCompression : CompressionType, IAvroCompression, IParquetCompression
{
    /// <summary>The shared instance.</summary>
    public static NoCompression Instance { get; } = new();

    AvroCodec IAvroCompression.CreateAvroCodec() => NullCodec.Instance;

    CompressionCodec IParquetCompression.ParquetCodec => CompressionCodec.Uncompressed;
    int IParquetCompression.ParquetLevel => 0;
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

/// <summary>Snappy (Avro codec <c>snappy</c>: each block carries the CRC32 of its uncompressed bytes; Parquet <c>SNAPPY</c>).</summary>
public sealed record SnappyCompression : CompressionType, IAvroCompression, IParquetCompression
{
    /// <summary>The shared instance.</summary>
    public static SnappyCompression Instance { get; } = new();

    AvroCodec IAvroCompression.CreateAvroCodec() => SnappyCodec.Instance;

    CompressionCodec IParquetCompression.ParquetCodec => CompressionCodec.Snappy;
    int IParquetCompression.ParquetLevel => 0;
}

/// <summary>Zstandard (Avro codec <c>zstandard</c>, Parquet <c>ZSTD</c>).</summary>
/// <param name="Level">1–22; 3 is the library default, higher is smaller and slower.</param>
public sealed record ZstandardCompression(int Level = 3) : CompressionType, IAvroCompression, IParquetCompression
{
    /// <summary>The compression level, 1–22.</summary>
    public int Level { get; } = Level is >= 1 and <= 22
        ? Level
        : throw new ArgumentOutOfRangeException(nameof(Level), Level, "Zstandard levels run from 1 to 22.");

    AvroCodec IAvroCompression.CreateAvroCodec() => new ZstandardCodec(Level);

    CompressionCodec IParquetCompression.ParquetCodec => CompressionCodec.Zstd;
    int IParquetCompression.ParquetLevel => Level;
}

/// <summary>Gzip (Parquet <c>GZIP</c>: each page a gzip member).</summary>
/// <param name="Level">1–9 as in zlib. The runtime offers coarser steps: 1–3 is fastest, 4–6 optimal, 7–9 smallest.</param>
public sealed record GzipCompression(int Level = 6) : CompressionType, IParquetCompression
{
    /// <summary>The zlib-style level, 1–9.</summary>
    public int Level { get; } = Level is >= 1 and <= 9
        ? Level
        : throw new ArgumentOutOfRangeException(nameof(Level), Level, "Gzip levels run from 1 to 9.");

    CompressionCodec IParquetCompression.ParquetCodec => CompressionCodec.Gzip;
    int IParquetCompression.ParquetLevel => Level;
}

/// <summary>Brotli (Parquet <c>BROTLI</c>). Spark cannot read it (Hadoop has no Brotli codec).</summary>
/// <param name="Level">The quality, 0–11; higher is smaller and slower.</param>
public sealed record BrotliCompression(int Level = 4) : CompressionType, IParquetCompression
{
    /// <summary>The quality, 0–11.</summary>
    public int Level { get; } = Level is >= 0 and <= 11
        ? Level
        : throw new ArgumentOutOfRangeException(nameof(Level), Level, "Brotli qualities run from 0 to 11.");

    CompressionCodec IParquetCompression.ParquetCodec => CompressionCodec.Brotli;
    int IParquetCompression.ParquetLevel => Level;
}

/// <summary>LZ4 blocks without framing (Parquet <c>LZ4_RAW</c>).</summary>
public sealed record Lz4RawCompression : CompressionType, IParquetCompression
{
    /// <summary>The shared instance.</summary>
    public static Lz4RawCompression Instance { get; } = new();

    CompressionCodec IParquetCompression.ParquetCodec => CompressionCodec.Lz4Raw;
    int IParquetCompression.ParquetLevel => 0;
}
