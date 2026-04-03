#pragma warning disable CS1573
using Polars.NET.Core;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
    /// <summary>
    /// Sink the LazyFrame to an IPC (Arrow) file.
    /// <para>
    /// This allows for streaming execution.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the output file.</param>
    /// <param name="compression">Compression method to use.</param>
    /// <param name="compatLevel">Compatibility level (default -1 = newest).</param>
    /// <param name="recordBatchSize">Number of rows per record batch (0 = default).</param>
    /// <param name="recordBatchStatistics">Write statistics to the record batch header (default = true).</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
    /// <param name="cloudOptions">Options for cloud storage.</param>
    public void SinkIpc(
        string path,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkIpc(
            Handle,
            path,
            compression.ToNative(),
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,
            // Cloud
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
    /// <inheritdoc cref="SinkIpc"/>
    /// <param name="partitionBy">The selector(s) to partition the data by.</param>
    /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// Use with caution: if the data is not grouped, the output may be incorrect.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    public void SinkIpcPartitioned(
        string path,
        IntoSelector partitionBy,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkIpcPartitioned(
            Handle,
            path,
            // --- Partition Params ---
            partitionBy.Consume().CloneHandle(), 
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile > 0 ? (nuint)maxRowsPerFile : 0,
            approxBytesPerFile > 0 ? (ulong)approxBytesPerFile : 0,
            compression.ToNative(),
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,
            // Cloud
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
    /// Sink the LazyFrame to an IPC (Arrow) format in memory.
    /// <para>
    /// This allows for streaming execution directly into a byte array without writing to disk.
    /// </para>
    /// </summary>
    /// <param name="compression">Compression method to use.</param>
    /// <param name="compatLevel">Compatibility level (default -1 = newest).</param>
    /// <param name="recordBatchSize">Number of rows per record batch (0 = default).</param>
    /// <param name="recordBatchStatistics">Write statistics to the record batch header (default = true).</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <returns>A byte array containing the serialized IPC data.</returns>
    public byte[] SinkIpcMemory(
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true)
    {
        return PolarsWrapper.SinkIpcMemory(
            Handle,
            compression.ToNative(),
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder
        );
    }
}