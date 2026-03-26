#pragma warning disable CS1573
using Polars.NET.Core;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
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
    /// <param name="statistics">Compute and write column statistics. Defaults to false.</param>
    /// <param name="rowGroupSize">Number of rows per row group. 0 means use default.</param>
    /// <param name="dataPageSize">Size of data page in bytes. 0 means use default.</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public void WriteParquet(
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
            cloudOptions
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkParquetPartitioned"/>
    public void WriteParquetPartitioned(
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
            cloudOptions

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
        bool maintainOrder = true)
    {
        var lf = Lazy();
        return lf.SinkParquetMemory(
            compression,
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder
        );
    }
}