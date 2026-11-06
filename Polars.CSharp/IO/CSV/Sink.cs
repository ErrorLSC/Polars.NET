#pragma warning disable CS1573
using System.IO.Pipelines;
using Polars.NET.Core;
using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable
{
    /// <summary>
    /// Execute the LazyFrame and sink the result to a CSV file.
    /// <para>
    /// This operation allows processing datasets larger than memory by streaming results 
    /// directly to the file system.
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
    public void SinkCsv(
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
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkCsv(
            CloneHandle(),
            path,

            // CSV Writer Options
            includeHeader,
            includeBom,
            batchSize,
            checkExtension,

            // Compression
            compression.ToNative(),
            compressionLevel,

            // Serialize Options 
            separator,
            quoteChar,
            quoteStyle.ToNative(),
            nullValue,
            lineTerminator,
            dateFormat,
            timeFormat,
            datetimeFormat,
            floatScientific,
            floatPrecision,
            decimalComma,

            // Unified Sink Options
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,

            // Cloud Options
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
    /// Execute the LazyFrame and sink the result to a CSV file.
    /// <para>
    /// This operation allows processing datasets larger than memory by streaming results 
    /// directly to the file system.
    /// </para>
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="partitionBy">The selector(s) to partition the data by.</param>
    /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// Use with caution: if the data is not grouped, the output may be incorrect.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
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
    public void SinkCsvPartitioned(
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
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkCsvPartitioned(
            CloneHandle(),
            path,
            // --- Partition Params ---
            partitionBy.Consume().CloneHandle(),
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile > 0 ? (nuint)maxRowsPerFile : 0,
            approxBytesPerFile > 0 ? (ulong)approxBytesPerFile : 0,
            // CSV Writer Options
            includeHeader,
            includeBom,
            batchSize,
            checkExtension,

            // Compression
            compression.ToNative(),
            compressionLevel,

            // Serialize Options 
            separator,
            quoteChar,
            quoteStyle.ToNative(),
            nullValue,
            lineTerminator,
            dateFormat,
            timeFormat,
            datetimeFormat,
            floatScientific,
            floatPrecision,
            decimalComma,

            // Unified Sink Options
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,

            // Cloud Options
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
    /// Sink the LazyFrame to a CSV format in memory.
    /// <para>
    /// This allows for streaming execution directly into a byte array without writing to disk.
    /// </para>
    /// </summary>
    public byte[] SinkCsvMemory(
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
        return PolarsWrapper.SinkCsvMemory(
            CloneHandle(),
            includeBom,
            includeHeader,
            batchSize,
            checkExtension,
            compressionCode.ToNative(),
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
            quoteStyle.ToNative(),
            maintainOrder
        );
    }

    /// <summary>
    /// Sink the LazyFrame to a CSV format in native memory, without copying into the managed heap.
    /// <para>
    /// This allows for streaming execution directly into a native buffer without writing to disk.
    /// </para>
    /// </summary>
    /// <returns>
    /// A <see cref="NativeBuffer"/> owning the serialized CSV data. Dispose it to release the native memory.
    /// </returns>
    public NativeBuffer SinkCsvNativeMemory(
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
        return PolarsWrapper.SinkCsvNativeMemory(
            CloneHandle(),
            includeBom,
            includeHeader,
            batchSize,
            checkExtension,
            compressionCode.ToNative(),
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
            quoteStyle.ToNative(),
            maintainOrder
        );
    }
    /// <summary>
    /// Sink the LazyFrame to a CSV format written into a <see cref="Stream"/>.
    /// <para>
    /// Output is streamed in chunks as it is produced; the whole file is never materialized in memory.
    /// The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <param name="stream">Writable destination stream.</param>
    /// <param name="includeBom">Whether to include the UTF-8 Byte Order Mark (BOM).</param>
    /// <param name="includeHeader">Whether to include the header row.</param>
    /// <param name="batchSize">The batch size for writing rows. 0 means use the Polars default.</param>
    /// <param name="checkExtension">Whether to check if the file extension matches '.csv'.</param>
    /// <param name="compressionCode">Compression method (Gzip/Zstd).</param>
    /// <param name="compressionLevel">Compression level (depends on the codec).</param>
    /// <param name="dateFormat">Format string for Date columns. If null, uses ISO 8601.</param>
    /// <param name="timeFormat">Format string for Time columns. If null, uses ISO 8601.</param>
    /// <param name="datetimeFormat">Format string for Datetime columns. If null, uses ISO 8601.</param>
    /// <param name="floatScientific">Whether to always use scientific notation for floats (-1 = automatic).</param>
    /// <param name="floatPrecision">The number of decimal places to write for floats (-1 = full precision).</param>
    /// <param name="decimalComma">Whether to use a comma ',' as the decimal separator.</param>
    /// <param name="separator">The character used as a field separator.</param>
    /// <param name="quoteChar">The character used for quoting fields.</param>
    /// <param name="nullValue">The string representation for null values.</param>
    /// <param name="lineTerminator">The character sequence used to terminate lines.</param>
    /// <param name="quoteStyle">The quoting style to use.</param>
    /// <param name="maintainOrder">Whether to maintain the order of the data.</param>
    public void SinkCsv(
        Stream stream,
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
        ArgumentNullException.ThrowIfNull(stream);
        var handle = CloneHandle();
        NativeStreamWriter.Write(stream, target => PolarsWrapper.SinkCsvCallback(
            handle, target, includeBom, includeHeader, batchSize, checkExtension, compressionCode.ToNative(),
            compressionLevel, dateFormat, timeFormat, datetimeFormat, floatScientific, floatPrecision, decimalComma,
            separator, quoteChar, nullValue, lineTerminator, quoteStyle.ToNative(), maintainOrder));
    }

    /// <summary>
    /// Asynchronously sink the LazyFrame to a CSV format written into a <see cref="Stream"/>.
    /// <para>
    /// The query runs on a background thread; chunks are written to the stream with async I/O,
    /// so it is safe for destinations that disallow synchronous writes (e.g. ASP.NET Core response bodies).
    /// The stream is flushed but not disposed.
    /// </para>
    /// </summary>
    /// <inheritdoc cref="SinkCsv(Stream, bool, bool, int, bool, ExternalCompression, int, string?, string?, string?, int, int, bool, byte, byte, string?, string?, QuoteStyle, bool)"/>
    /// <param name="cancellationToken">Cancels writing; the native query is aborted at its next write.</param>
    public Task SinkCsvAsync(
        Stream stream,
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
        bool maintainOrder = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);
        var handle = CloneHandle();
        return NativeStreamWriter.WriteAsync(stream, target => PolarsWrapper.SinkCsvCallback(
            handle, target, includeBom, includeHeader, batchSize, checkExtension, compressionCode.ToNative(),
            compressionLevel, dateFormat, timeFormat, datetimeFormat, floatScientific, floatPrecision, decimalComma,
            separator, quoteChar, nullValue, lineTerminator, quoteStyle.ToNative(), maintainOrder),
            cancellationToken);
    }

    /// <summary>
    /// Asynchronously sink the LazyFrame to a CSV format written into a <see cref="PipeWriter"/>
    /// (e.g. ASP.NET Core <c>HttpResponse.BodyWriter</c>).
    /// <para>The writer is flushed but not completed.</para>
    /// </summary>
    /// <param name="writer">Destination pipe writer.</param>
    /// <inheritdoc cref="SinkCsvAsync(Stream, bool, bool, int, bool, ExternalCompression, int, string?, string?, string?, int, int, bool, byte, byte, string?, string?, QuoteStyle, bool, CancellationToken)"/>
    public Task SinkCsvAsync(
        PipeWriter writer,
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
        bool maintainOrder = true,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(writer);
        var handle = CloneHandle();
        return NativeStreamWriter.WriteAsync(writer, target => PolarsWrapper.SinkCsvCallback(
            handle, target, includeBom, includeHeader, batchSize, checkExtension, compressionCode.ToNative(),
            compressionLevel, dateFormat, timeFormat, datetimeFormat, floatScientific, floatPrecision, decimalComma,
            separator, quoteChar, nullValue, lineTerminator, quoteStyle.ToNative(), maintainOrder),
            cancellationToken);
    }
}