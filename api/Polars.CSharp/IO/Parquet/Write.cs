#pragma warning disable CS1573
using System.ComponentModel;
using System.IO.Pipelines;
using Polars.NET.Core;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable, IEnumerable<Series>
{
    /// <summary>
    /// Write DataFrame to a Parquet file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming, statistics, and cloud storage.
    /// </para>
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method. Defaults to Snappy.</param>
    /// <param name="compressionLevel">Compression level. -1 means default.</param>
    /// <param name="statistics">Compute and write column statistics (min, max, null count). Defaults to true.</param>
    /// <param name="rowGroupSize">Number of rows per row group. 0 means use default.</param>
    /// <param name="dataPageSize">Size of data page in bytes. 0 means use default.</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    /// <param name="metadata">Key-value metadata written into the file footer.</param>
    /// <param name="arrowSchema">The Arrow schema stored in the file (field metadata, extension types); its names and types must match the data's (unstable in the engine).</param>
    /// <param name="encryption">Encrypt the file with Parquet modular encryption (experimental).</param>
    public void WriteParquet(
        string path,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = true,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        Apache.Arrow.Schema? arrowSchema = null,
        ParquetEncryption? encryption = null)
    {
        var lf = Lazy();

        lf.SinkParquet(
            path,
            compression,
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder,
            syncOnClose,
            mkdir,
            cloudOptions,
            metadata,
            arrowSchema,
            encryption
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkParquetPartitioned"/>
    public void WriteParquetPartitioned(
        string path,
        IntoSelector partitionBy,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = true,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null,
        IReadOnlyDictionary<string, string>? metadata = null,
        Apache.Arrow.Schema? arrowSchema = null,
        ParquetEncryption? encryption = null)
    {
        var lf = Lazy();
        lf.SinkParquetPartitioned(
            path,

            // --- Partition Params ---
            partitionBy,
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile,
            approxBytesPerFile,

            // --- Parquet Options ---
            compression,
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,

            // --- Unified Options ---
            maintainOrder,
            syncOnClose,
            mkdir,
            cloudOptions,
            metadata,
            arrowSchema,
            encryption
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkParquetMemory"/>
    public byte[] WriteParquetMemory(
        ParquetCompression compression = ParquetCompression.ZSTD,
        int compressionLevel = 3,
        bool statistics = true,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        IReadOnlyDictionary<string, string>? metadata = null,
        Apache.Arrow.Schema? arrowSchema = null,
        ParquetEncryption? encryption = null)
    {
        var lf = Lazy();
        return lf.SinkParquetMemory(
            compression,
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder,
            metadata,
            arrowSchema,
            encryption
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkParquetNativeMemory"/>
    public NativeBuffer WriteParquetNativeMemory(
        ParquetCompression compression = ParquetCompression.ZSTD,
        int compressionLevel = 3,
        bool statistics = true,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        IReadOnlyDictionary<string, string>? metadata = null,
        Apache.Arrow.Schema? arrowSchema = null,
        ParquetEncryption? encryption = null)
    {
        var lf = Lazy();
        return lf.SinkParquetNativeMemory(
            compression,
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder,
            metadata,
            arrowSchema,
            encryption
        );
    }
    /// <summary>
    /// Write DataFrame to a Parquet format written into a <see cref="Stream"/>.
    /// <para>
    /// Output is streamed in chunks as it is produced. The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <param name="stream">Writable destination stream.</param>
    /// <inheritdoc cref="LazyFrame.SinkParquet(Stream, ParquetCompression, int, bool, int, int, int, bool, IReadOnlyDictionary{string, string}, Apache.Arrow.Schema, ParquetEncryption)"/>
    public void WriteParquet(
        Stream stream,
        ParquetCompression compression = ParquetCompression.ZSTD,
        int compressionLevel = 3,
        bool statistics = true,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        IReadOnlyDictionary<string, string>? metadata = null,
        Apache.Arrow.Schema? arrowSchema = null,
        ParquetEncryption? encryption = null)
    {
        var lf = Lazy();
        lf.SinkParquet(stream, compression, compressionLevel, statistics, rowGroupSize, dataPageSize, compatLevel, maintainOrder, metadata, arrowSchema, encryption);
    }
    /// <inheritdoc cref="LazyFrame.SinkParquetAsync(Stream, ParquetCompression, int, bool, int, int, int, bool, IReadOnlyDictionary{string, string}, Apache.Arrow.Schema, ParquetEncryption, CancellationToken)"/>
    public Task WriteParquetAsync(
        Stream stream,
        ParquetCompression compression = ParquetCompression.ZSTD,
        int compressionLevel = 3,
        bool statistics = true,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        IReadOnlyDictionary<string, string>? metadata = null,
        Apache.Arrow.Schema? arrowSchema = null,
        ParquetEncryption? encryption = null,
        CancellationToken cancellationToken = default)
    {
        var lf = Lazy();
        return lf.SinkParquetAsync(stream, compression, compressionLevel, statistics, rowGroupSize, dataPageSize, compatLevel, maintainOrder, metadata, arrowSchema, encryption, cancellationToken);
    }
    /// <inheritdoc cref="LazyFrame.SinkParquetAsync(PipeWriter, ParquetCompression, int, bool, int, int, int, bool, IReadOnlyDictionary{string, string}, Apache.Arrow.Schema, ParquetEncryption, CancellationToken)"/>
    public Task WriteParquetAsync(
        PipeWriter writer,
        ParquetCompression compression = ParquetCompression.ZSTD,
        int compressionLevel = 3,
        bool statistics = true,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        IReadOnlyDictionary<string, string>? metadata = null,
        Apache.Arrow.Schema? arrowSchema = null,
        ParquetEncryption? encryption = null,
        CancellationToken cancellationToken = default)
    {
        var lf = Lazy();
        return lf.SinkParquetAsync(writer, compression, compressionLevel, statistics, rowGroupSize, dataPageSize, compatLevel, maintainOrder, metadata, arrowSchema, encryption, cancellationToken);
    }

    /// <summary>The signature of version 1.1 (kept for callers compiled against it, and for a positional token).</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task WriteParquetAsync(Stream stream, ParquetCompression compression, int compressionLevel, bool statistics, int rowGroupSize,
        int dataPageSize, int compatLevel, bool maintainOrder, CancellationToken cancellationToken)
        => WriteParquetAsync(stream, compression, compressionLevel, statistics, rowGroupSize, dataPageSize, compatLevel, maintainOrder,
            null, null, null, cancellationToken);

    /// <summary>The signature of version 1.1 (kept for callers compiled against it, and for a positional token).</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task WriteParquetAsync(PipeWriter writer, ParquetCompression compression, int compressionLevel, bool statistics, int rowGroupSize,
        int dataPageSize, int compatLevel, bool maintainOrder, CancellationToken cancellationToken)
        => WriteParquetAsync(writer, compression, compressionLevel, statistics, rowGroupSize, dataPageSize, compatLevel, maintainOrder,
            null, null, null, cancellationToken);
}
