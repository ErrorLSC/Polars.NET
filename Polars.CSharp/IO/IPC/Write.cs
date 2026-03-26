#pragma warning disable CS1573
using Polars.NET.Core;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Write DataFrame to an IPC (Arrow/Feather) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming and cloud storage.
    /// </para>
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="compression">Compression method (None, LZ4, ZSTD). Defaults to None.</param>
    /// <param name="compatLevel">Compatibility level (default -1 = newest).</param>
    /// <param name="recordBatchSize">Number of rows per record batch (0 = default).</param>
    /// <param name="recordBatchStatistics">Write statistics to the record batch header (default = true).</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
    /// <param name="cloudOptions">Options for cloud storage.</param>
    public void WriteIpc(
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
        var lf = Lazy();

        lf.SinkIpc(
            path,
            compression,
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder,
            syncOnClose,
            mkdir,
            cloudOptions
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkIpcPartitioned"/>
    public void WriteIpcPartitioned(
        string path,
        Selector partitionBy,
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
        var lf = Lazy();
        lf.SinkIpcPartitioned(
            path,partitionBy,includeKeys,keysPreGrouped,maxRowsPerFile,approxBytesPerFile,
            compression,compatLevel,recordBatchSize,recordBatchStatistics,maintainOrder,syncOnClose,mkdir,cloudOptions
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkIpcMemory(IpcCompression, int, int, bool, bool)"/>
    public byte[] WriteIpcMemory(
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true)
    {
        var lf = Lazy();
        return lf.SinkIpcMemory(
            compression,
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder
        );
    }
}