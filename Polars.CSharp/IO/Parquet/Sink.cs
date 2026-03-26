using Polars.NET.Core;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable,IPolarsLazyFrame
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
    /// <param name="statistics">Write statistics to the parquet file.</param>
    /// <param name="rowGroupSize">Target row group size (in rows).</param>
    /// <param name="dataPageSize">Target data page size (in bytes).</param>
    /// <param name="compatLevel">IPC format compatibility, -1: oldest, 0: default, 1: newest.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public void SinkParquet(
        string path,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = false,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkParquet(
            Handle,
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
            values
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
    /// <param name="statistics">Write statistics to the parquet file.</param>
    /// <param name="rowGroupSize">Target row group size (in rows).</param>
    /// <param name="dataPageSize">Target data page size (in bytes).</param>
    /// <param name="compatLevel">IPC format compatibility, -1: oldest, 0: default, 1: newest.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public void SinkParquetPartitioned(
        string path,
        Selector partitionBy,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = false,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        // Parse cloud options using the helper
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkParquetPartitioned(
            Handle,
            path,
            
            // --- Partition Params ---
            partitionBy.Handle, 
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
            values
        );
    }
    /// <summary>
    /// Sink the LazyFrame to a Parquet format in memory.
    /// <para>
    /// This allows for streaming execution directly into a byte array without writing to disk.
    /// </para>
    /// </summary>
    public byte[] SinkParquetMemory(
        ParquetCompression compression = ParquetCompression.ZSTD,
        int compressionLevel = 3, 
        bool statistics = true,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true)
    {
        return PolarsWrapper.SinkParquetMemory(
            Handle,
            compression.ToNative(),
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder
        );
    }
}