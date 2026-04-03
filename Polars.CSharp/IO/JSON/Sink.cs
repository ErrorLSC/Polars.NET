#pragma warning disable CS1573
using Polars.NET.Core;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
    /// <summary>
    /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) file.
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method (Gzip/Zstd).</param>
    /// <param name="compressionLevel">Compression level.</param>
    /// <param name="checkExtension">Whether to check if the file extension matches '.json' or '.ndjson'.</param>
    /// <param name="maintainOrder">Maintain the order of data.</param>
    /// <param name="syncOnClose">Sync to disk on close.</param>
    /// <param name="mkdir">Create parent directories.</param>
    /// <param name="cloudOptions">Cloud storage options.</param>
    public void SinkJson(
        string path,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkJson(
            Handle,
            path,
            compression.ToNative(),
            compressionLevel,
            checkExtension,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,
            // Cloud
            provider.ToNative(),
            retries,
            timeout,
            initBackoff,
            maxBackoff,
            cacheTtl,
            keys,
            values
        );
    }
    /// <inheritdoc cref="SinkJson"/>
    /// <param name="partitionBy">The selector(s) to partition the data by.</param>
    /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// Use with caution: if the data is not grouped, the output may be incorrect.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    public void SinkJsonPartitioned(
        string path,
        IntoSelector partitionBy,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkJsonPartitioned(
            Handle,
            path,
            // --- Partition Params ---
            partitionBy.Consume().CloneHandle(), 
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile > 0 ? (nuint)maxRowsPerFile : 0,
            approxBytesPerFile > 0 ? (ulong)approxBytesPerFile : 0,
            compression.ToNative(),
            compressionLevel,
            checkExtension,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,
            // Cloud
            provider.ToNative(),
            retries,
            timeout,
            initBackoff,
            maxBackoff,
            cacheTtl,
            keys,
            values
        );
    }
    /// <summary>
    /// Alias for SinkJson with format=JsonLines.
    /// </summary>
    public void SinkNdJson(
        string path,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        SinkJson(path,compression,compressionLevel,checkExtension, maintainOrder, syncOnClose, mkdir,cloudOptions);
    }
    /// <summary>
    /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) format in memory.
    /// </summary>
    public byte[] SinkJsonMemory(
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true)
    {
        return PolarsWrapper.SinkJsonMemory(
            Handle,
            compression.ToNative(),
            compressionLevel,
            checkExtension,
            maintainOrder
        );
    }
}