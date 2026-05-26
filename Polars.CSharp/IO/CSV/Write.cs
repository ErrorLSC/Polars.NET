#pragma warning disable CS1573
using Polars.NET.Core;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Write DataFrame to a comma-separated values (CSV) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming and cloud storage.
    /// </para>
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="includeHeader">Whether to include the header row. Defaults to true.</param>
    /// <param name="includeBom">Whether to include the UTF-8 Byte Order Mark (BOM). Defaults to false.</param>
    /// <param name="separator">The character used as a field separator. Defaults to ','.</param>
    /// <param name="quoteChar">The character used for quoting fields. Defaults to '"'.</param>
    /// <param name="quoteStyle">The quoting style to use. Defaults to <see cref="QuoteStyle.Necessary"/>.</param>
    /// <param name="nullValue">The string representation for null values. Defaults to empty string.</param>
    /// <param name="lineTerminator">The character sequence used to terminate lines. Defaults to "\n".</param>
    /// <param name="floatScientific">
    /// Whether to always use scientific notation for floats. 
    /// If null (default), formatting is automatic.
    /// </param>
    /// <param name="floatPrecision">
    /// The number of decimal places to write for floats. 
    /// If null (default), uses full precision.
    /// </param>
    /// <param name="decimalComma">Whether to use a comma ',' as the decimal separator. Defaults to false.</param>
    /// <param name="dateFormat">Format string for Date columns. If null, uses ISO 8601.</param>
    /// <param name="timeFormat">Format string for Time columns. If null, uses ISO 8601.</param>
    /// <param name="datetimeFormat">Format string for Datetime columns. If null, uses ISO 8601.</param>
    /// <param name="checkExtension">Whether to check if the file extension matches '.csv'. Defaults to true.</param>
    /// <param name="compression">Compression method (Gzip/Zstd). Defaults to None.</param>
    /// <param name="compressionLevel">Compression level (depends on the codec). -1 for default.</param>
    /// <param name="maintainOrder">
    /// Whether to maintain the order of the data. 
    /// Setting this to false can improve performance in streaming mode. Defaults to true.
    /// </param>
    /// <param name="syncOnClose">File synchronization behavior on close (e.g., flush to disk). Defaults to None.</param>
    /// <param name="mkdir">Recursively create the output directory if it does not exist. Defaults to false.</param>
    /// <param name="batchSize">
    /// The batch size for writing rows. 
    /// 0 means use the Polars default.
    /// </param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public void WriteCsv(
        string path,
        bool includeHeader = true,
        bool includeBom = false,
        char separator = ',',
        char quoteChar = '"',
        QuoteStyle quoteStyle = QuoteStyle.Necessary,
        string? nullValue = null,
        string? lineTerminator = "\n",
        bool? floatScientific = null,
        int? floatPrecision = null,
        bool decimalComma = false,
        string? dateFormat = null,
        string? timeFormat = null,
        string? datetimeFormat = null,
        bool checkExtension = true,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        int batchSize = 0,
        CloudOptions? cloudOptions = null)
    {

        var lf = Lazy();

        lf.SinkCsv(
            path,
            includeHeader,
            includeBom,
            separator,
            quoteChar,
            quoteStyle,
            nullValue,
            lineTerminator,
            floatScientific,
            floatPrecision,
            decimalComma,
            dateFormat,
            timeFormat,
            datetimeFormat,
            checkExtension,
            compression,
            compressionLevel,
            maintainOrder,
            syncOnClose,
            mkdir,
            batchSize,
            cloudOptions
        );
    }
    /// <summary>
    /// Write DataFrame to a partitioned comma-separated values (CSV) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming and cloud storage.
    /// </para>
    /// </summary>
    /// <param name="path">The output base directory path.</param>
    /// <param name="partitionBy">The selector(s) to partition the data by.</param>
    /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// Use with caution: if the data is not grouped, the output may be incorrect.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    /// <inheritdoc cref="WriteCsv"/>
    public void WriteCsvPartitioned(
        string path,
        IntoSelector partitionBy,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        bool includeHeader = true,
        bool includeBom = false,
        char separator = ',',
        char quoteChar = '"',
        QuoteStyle quoteStyle = QuoteStyle.Necessary,
        string? nullValue = null,
        string? lineTerminator = "\n",
        bool? floatScientific = null,
        int? floatPrecision = null,
        bool decimalComma = false,
        string? dateFormat = null,
        string? timeFormat = null,
        string? datetimeFormat = null,
        bool checkExtension = true,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        int batchSize = 0,
        CloudOptions? cloudOptions = null)
    {

        var lf = Lazy();

        lf.SinkCsvPartitioned(
            path,
            // --- Partition Params ---
            partitionBy, 
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile,
            approxBytesPerFile,
            includeHeader,
            includeBom,
            separator,
            quoteChar,
            quoteStyle,
            nullValue,
            lineTerminator,
            floatScientific,
            floatPrecision,
            decimalComma,
            dateFormat,
            timeFormat,
            datetimeFormat,
            checkExtension,
            compression,
            compressionLevel,
            maintainOrder,
            syncOnClose,
            mkdir,
            batchSize,
            cloudOptions
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkCsvMemory"/>
    public byte[] WriteCsvMemory(
        bool includeBom = false,
        bool includeHeader = true,
        int batchSize = 1024,
        bool checkExtension = false, 
        ExternalCompression compressionCode = 0,    
        int compressionLevel = 0,
        string? dateFormat = null,
        string? timeFormat = null,
        string? datetimeFormat = null,
        int floatScientific = -1,
        int floatPrecision = -1,
        bool decimalComma = false,
        byte separator = (byte)',',
        byte quoteChar = (byte)'"',
        string? nullValue = null,
        string? lineTerminator = "\n",
        QuoteStyle quoteStyle = QuoteStyle.Necessary,         
        bool maintainOrder = true)
    {
        var lf = Lazy();
        return lf.SinkCsvMemory(
            includeBom,
            includeHeader,
            batchSize,
            checkExtension,
            compressionCode,
            compressionLevel,
            dateFormat,
            timeFormat,
            datetimeFormat,
            floatScientific,
            floatPrecision,
            decimalComma,
            separator,
            quoteChar,
            nullValue,
            lineTerminator,
            quoteStyle,
            maintainOrder
        );
    }
}