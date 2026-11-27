#pragma warning disable CS1573
using System.ComponentModel;
using System.IO.Pipelines;
using Polars.NET.Core;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable
{
    /// <summary>
    /// Sink the LazyFrame to a Parquet file.
    /// <para>
    /// This allows for streaming execution, processing the data in chunks and writing it to the file
    /// without loading the entire dataset into memory.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the output file.</param>
    /// <param name="compression">Compression codec to use.</param>
    /// <param name="compressionLevel">Compression level (depends on the codec).</param>
    /// <param name="statistics">Write column statistics (min, max, null count) to the file (default: true).</param>
    /// <param name="rowGroupSize">Target row group size (in rows).</param>
    /// <param name="dataPageSize">Target data page size (in bytes).</param>
    /// <param name="compatLevel">IPC format compatibility, -1: oldest, 0: default, 1: newest.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    /// <param name="metadata">Key-value metadata written into the file footer.</param>
    /// <param name="arrowSchema">The Arrow schema stored in the file (field metadata, extension types); its names and types must match the data's (unstable in the engine).</param>
    /// <param name="encryption">Encrypt the files with Parquet modular encryption (experimental).</param>
    public void SinkParquet(
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
        using var extras = ParquetNativeOptions.Write(metadata, arrowSchema, encryption);
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkParquet(
            CloneHandle(),
            path,
            compression.ToNative(),
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values,
            extras
        );
    }
    /// <summary>
    /// Sink the LazyFrame to a set of Parquet files, partitioned by the specified selector.
    /// <para>
    /// This writes the dataset to a directory, splitting the data into multiple files based on the
    /// partition key(s) defined in <paramref name="partitionBy"/>.
    /// </para>
    /// </summary>
    /// <param name="path">Base path to the output directory.</param>
    /// <param name="partitionBy">The selector(s) to partition the data by.</param>
    /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// Use with caution: if the data is not grouped, the output may be incorrect.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    /// <param name="compression">Compression codec to use.</param>
    /// <param name="compressionLevel">Compression level (depends on the codec).</param>
    /// <param name="statistics">Write column statistics (min, max, null count) to the file (default: true).</param>
    /// <param name="rowGroupSize">Target row group size (in rows).</param>
    /// <param name="dataPageSize">Target data page size (in bytes).</param>
    /// <param name="compatLevel">IPC format compatibility, -1: oldest, 0: default, 1: newest.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    /// <param name="metadata">Key-value metadata written into the file footer.</param>
    /// <param name="arrowSchema">The Arrow schema stored in the file (field metadata, extension types); its names and types must match the data's (unstable in the engine).</param>
    /// <param name="encryption">Encrypt the files with Parquet modular encryption (experimental).</param>
    public void SinkParquetPartitioned(
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
        using var extras = ParquetNativeOptions.Write(metadata, arrowSchema, encryption);
        var pby = partitionBy.Consume();
        // Parse cloud options using the helper
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkParquetPartitioned(
            CloneHandle(),
            path,

            // --- Partition Params ---
            pby.CloneHandle(),
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile > 0 ? (nuint)maxRowsPerFile : 0,
            approxBytesPerFile > 0 ? (ulong)approxBytesPerFile : 0,

            // --- Parquet Options ---
            compression.ToNative(),
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,

            // --- Unified Options ---
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,

            // --- Cloud Params ---
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values,
            extras
        );
    }
    /// <summary>
    /// Sink the LazyFrame to a Parquet format in memory.
    /// <para>
    /// This allows for streaming execution directly into a byte array without writing to disk.
    /// </para>
    /// </summary>
    /// <inheritdoc cref="SinkParquet(Stream, ParquetCompression, int, bool, int, int, int, bool, IReadOnlyDictionary{string, string}, Apache.Arrow.Schema, ParquetEncryption)"/>
    public byte[] SinkParquetMemory(
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
        using var extras = ParquetNativeOptions.Write(metadata, arrowSchema, encryption);
        return PolarsWrapper.SinkParquetMemory(
            CloneHandle(),
            compression.ToNative(),
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder,
            extras
        );
    }
    /// <summary>
    /// Sink the LazyFrame to a Parquet format in native memory, without copying into the managed heap.
    /// </summary>
    /// <returns>
    /// A <see cref="NativeBuffer"/> owning the serialized Parquet data. Dispose it to release the native memory.
    /// </returns>
    /// <inheritdoc cref="SinkParquet(Stream, ParquetCompression, int, bool, int, int, int, bool, IReadOnlyDictionary{string, string}, Apache.Arrow.Schema, ParquetEncryption)"/>
    public NativeBuffer SinkParquetNativeMemory(
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
        using var extras = ParquetNativeOptions.Write(metadata, arrowSchema, encryption);
        return PolarsWrapper.SinkParquetNativeMemory(
            CloneHandle(),
            compression.ToNative(),
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder,
            extras
        );
    }
    /// <summary>
    /// Sink the LazyFrame to a Parquet format written into a <see cref="Stream"/>.
    /// <para>
    /// Output is streamed in chunks as it is produced; the whole file is never materialized in memory.
    /// The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <param name="stream">Writable destination stream.</param>
    /// <param name="compression">Compression codec to use.</param>
    /// <param name="compressionLevel">Compression level (depends on the codec).</param>
    /// <param name="statistics">Write column statistics (min, max, null count) to the file (default: true).</param>
    /// <param name="rowGroupSize">Target row group size (in rows).</param>
    /// <param name="dataPageSize">Target data page size (in bytes).</param>
    /// <param name="compatLevel">IPC format compatibility, -1: oldest, 0: default, 1: newest.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="metadata">Key-value metadata written into the file footer.</param>
    /// <param name="arrowSchema">The Arrow schema stored in the file (field metadata, extension types); its names and types must match the data's (unstable in the engine).</param>
    /// <param name="encryption">Encrypt the files with Parquet modular encryption (experimental).</param>
    public void SinkParquet(
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
        ArgumentNullException.ThrowIfNull(stream);
        using var extras = ParquetNativeOptions.Write(metadata, arrowSchema, encryption);
        var handle = CloneHandle();
        NativeStreamWriter.Write(stream, target => PolarsWrapper.SinkParquetCallback(
            handle, target, compression.ToNative(), compressionLevel, statistics, rowGroupSize, dataPageSize, compatLevel, maintainOrder, extras));
    }

    /// <summary>
    /// Asynchronously sink the LazyFrame to a Parquet format written into a <see cref="Stream"/>.
    /// <para>
    /// The query runs on a background thread; chunks are written to the stream with async I/O,
    /// so it is safe for destinations that disallow synchronous writes (e.g. ASP.NET Core response bodies).
    /// The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <inheritdoc cref="SinkParquet(Stream, ParquetCompression, int, bool, int, int, int, bool, IReadOnlyDictionary{string, string}, Apache.Arrow.Schema, ParquetEncryption)"/>
    /// <param name="cancellationToken">Cancels writing; the native query is aborted at its next write.</param>
    public Task SinkParquetAsync(
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
        ArgumentNullException.ThrowIfNull(stream);
        var extras = ParquetNativeOptions.Write(metadata, arrowSchema, encryption);
        var handle = CloneHandle();
        return DisposeWhenDone(NativeStreamWriter.WriteAsync(stream, target => PolarsWrapper.SinkParquetCallback(
            handle, target, compression.ToNative(), compressionLevel, statistics, rowGroupSize, dataPageSize, compatLevel, maintainOrder, extras),
            cancellationToken), extras);
    }

    /// <summary>
    /// Asynchronously sink the LazyFrame to a Parquet format written into a <see cref="PipeWriter"/>
    /// (e.g. ASP.NET Core <c>HttpResponse.BodyWriter</c>).
    /// <para>The writer is flushed but not completed.</para>
    /// </summary>
    /// <param name="writer">Destination pipe writer.</param>
    /// <inheritdoc cref="SinkParquetAsync(Stream, ParquetCompression, int, bool, int, int, int, bool, IReadOnlyDictionary{string, string}, Apache.Arrow.Schema, ParquetEncryption, CancellationToken)"/>
    public Task SinkParquetAsync(
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
        ArgumentNullException.ThrowIfNull(writer);
        var extras = ParquetNativeOptions.Write(metadata, arrowSchema, encryption);
        var handle = CloneHandle();
        return DisposeWhenDone(NativeStreamWriter.WriteAsync(writer, target => PolarsWrapper.SinkParquetCallback(
            handle, target, compression.ToNative(), compressionLevel, statistics, rowGroupSize, dataPageSize, compatLevel, maintainOrder, extras),
            cancellationToken), extras);
    }

    /// <summary>Releases the write options once the background write is over.</summary>
    private static async Task DisposeWhenDone(Task write, IDisposable? options)
    {
        try { await write.ConfigureAwait(false); }
        finally { options?.Dispose(); }
    }

    /// <summary>The signature of version 1.1 (kept for callers compiled against it, and for a positional token).</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task SinkParquetAsync(Stream stream, ParquetCompression compression, int compressionLevel, bool statistics, int rowGroupSize,
        int dataPageSize, int compatLevel, bool maintainOrder, CancellationToken cancellationToken)
        => SinkParquetAsync(stream, compression, compressionLevel, statistics, rowGroupSize, dataPageSize, compatLevel, maintainOrder,
            null, null, null, cancellationToken);

    /// <summary>The signature of version 1.1 (kept for callers compiled against it, and for a positional token).</summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public Task SinkParquetAsync(PipeWriter writer, ParquetCompression compression, int compressionLevel, bool statistics, int rowGroupSize,
        int dataPageSize, int compatLevel, bool maintainOrder, CancellationToken cancellationToken)
        => SinkParquetAsync(writer, compression, compressionLevel, statistics, rowGroupSize, dataPageSize, compatLevel, maintainOrder,
            null, null, null, cancellationToken);
}
