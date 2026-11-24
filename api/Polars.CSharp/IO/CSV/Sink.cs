#pragma warning disable CS1573 // overloads inherit the shared parameters' docs and document their own
using System.IO.Pipelines;
using Polars.NET.Core;
using Polars.NET.Core.Csv;
using Polars.NET.Core.IO;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable
{
    /// <summary>
    /// Run the query and write its rows as CSV to a file, local or on a cloud store, batch by batch as the streaming
    /// engine produces them (larger than memory works): encoded on every core, published whole once written (a failed
    /// sink leaves no file).
    /// </summary>
    /// <param name="path">The output file path (<c>s3://…</c>, <c>az://…</c>, <c>gs://…</c> on a store).</param>
    /// <param name="maintainOrder">Whether the rows keep the query's order. Setting this to false can improve performance. Defaults to true.</param>
    /// <param name="batchSize">Accepted for compatibility; the writer sizes its units itself.</param>
    /// <inheritdoc cref="DataFrame.WriteCsv(string, bool, bool, char, char, QuoteStyle, string?, string?, bool?, int?, bool, string?, string?, string?, bool, ExternalCompression, int, bool, SyncOnClose, bool, int, CloudOptions?, CsvEncoding, string?)"/>
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
        CloudOptions? cloudOptions = null,
        CsvEncoding encoding = default,
        string? thousandsSeparator = null)
        => ManagedCsv.SinkCsv(CloneHandle(), path,
            DataFrame.CsvWriteArgs(includeHeader, includeBom, separator, quoteChar, quoteStyle, nullValue, lineTerminator, floatScientific, floatPrecision,
                decimalComma, dateFormat, timeFormat, datetimeFormat, compression, compressionLevel, encoding, thousandsSeparator),
            checkExtension, maintainOrder, syncOnClose.ToNative(), mkdir, CloudOptions.ToCloudArgs(cloudOptions));

    /// <summary>
    /// Run the query and write its rows as a dataset of CSV files under <paramref name="path"/>, in Hive partitions of
    /// <paramref name="partitionBy"/> (<c>key=value</c> directories), batch by batch as the streaming engine produces them.
    /// </summary>
    /// <inheritdoc cref="DataFrame.WriteCsvPartitioned(string, IntoSelector, bool, bool, int, long, bool, bool, char, char, QuoteStyle, string?, string?, bool?, int?, bool, string?, string?, string?, bool, ExternalCompression, int, bool, SyncOnClose, bool, int, CloudOptions?, CsvEncoding, string?, ExistingData)"/>
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
        CloudOptions? cloudOptions = null,
        CsvEncoding encoding = default,
        string? thousandsSeparator = null,
        ExistingData existingData = ExistingData.Error)
    {
        string[] keys;
        using (Selector selector = partitionBy.Consume()) keys = ManagedFrames.SelectedColumns(Handle, selector.Handle);
        ManagedCsv.SinkCsvPartitioned(CloneHandle(), path, keys, includeKeys, keysPreGrouped,
            maxRowsPerFile > 0 ? (ulong)maxRowsPerFile : 0, approxBytesPerFile > 0 ? (ulong)approxBytesPerFile : 0,
            DataFrame.CsvWriteArgs(includeHeader, includeBom, separator, quoteChar, quoteStyle, nullValue, lineTerminator, floatScientific, floatPrecision,
                decimalComma, dateFormat, timeFormat, datetimeFormat, compression, compressionLevel, encoding, thousandsSeparator),
            maintainOrder, syncOnClose.ToNative(), existingData.ToNative(), CloudOptions.ToCloudArgs(cloudOptions));
    }

    /// <summary>Run the query and return its rows as CSV bytes.</summary>
    /// <param name="includeBom">Whether to start with the encoding's byte order mark. Defaults to false.</param>
    /// <param name="includeHeader">Whether to write the header row. Defaults to true.</param>
    /// <param name="batchSize">Accepted for compatibility; the writer sizes its units itself.</param>
    /// <param name="checkExtension">Unused: there is no path.</param>
    /// <param name="compressionCode">Compression of the whole output (Gzip/Zstd). Defaults to none.</param>
    /// <param name="compressionLevel">The compression level; -1 for the codec's default.</param>
    /// <param name="dateFormat">The format of Date columns (strftime). ISO 8601 when null.</param>
    /// <param name="timeFormat">The format of Time columns (strftime). ISO 8601 when null.</param>
    /// <param name="datetimeFormat">The format of Datetime columns (strftime). ISO 8601 when null.</param>
    /// <param name="floatScientific">Floats always (1) or never (0) in scientific notation; -1 (default) as is shortest.</param>
    /// <param name="floatPrecision">Floats with this many decimals; -1 (default) with the digits that read back the same value.</param>
    /// <param name="decimalComma">',' as the decimal separator. Defaults to false.</param>
    /// <param name="separator">The field separator. Defaults to ','.</param>
    /// <param name="quoteChar">The quote. Defaults to '"'.</param>
    /// <param name="nullValue">The text of nulls. Defaults to an empty field.</param>
    /// <param name="lineTerminator">The line end. Defaults to "\n".</param>
    /// <param name="quoteStyle">When fields are quoted. Defaults to <see cref="QuoteStyle.Necessary"/>.</param>
    /// <param name="maintainOrder">Whether the rows keep the query's order. Defaults to true.</param>
    /// <param name="encoding">The output's encoding (UTF-8 by default; see <see cref="CsvEncoding"/>). Strict: a character it cannot hold is an error naming the row, unless lossy.</param>
    /// <param name="thousandsSeparator">A digit group separator written in integers and floats. Off by default.</param>
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
        bool maintainOrder = true,
        CsvEncoding encoding = default,
        string? thousandsSeparator = null)
        => ManagedCsv.SinkCsvToMemory(CloneHandle(), DataFrame.CsvWriteArgs(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat,
            datetimeFormat, floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding, thousandsSeparator),
            maintainOrder);

    /// <summary>Run the query and return its rows as CSV in native memory, handed over without a copy.</summary>
    /// <inheritdoc cref="SinkCsvMemory"/>
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
        bool maintainOrder = true,
        CsvEncoding encoding = default,
        string? thousandsSeparator = null)
        => ManagedCsv.SinkCsvNativeMemory(CloneHandle(), DataFrame.CsvWriteArgs(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat,
            datetimeFormat, floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding, thousandsSeparator),
            maintainOrder);

    /// <summary>Run the query and write its rows as CSV into a <see cref="Stream"/>, batch by batch (flushed, not disposed).</summary>
    /// <param name="stream">Writable destination stream.</param>
    /// <inheritdoc cref="SinkCsvMemory"/>
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
        bool maintainOrder = true,
        CsvEncoding encoding = default,
        string? thousandsSeparator = null)
        => ManagedCsv.SinkCsv(CloneHandle(), stream, DataFrame.CsvWriteArgs(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat,
            datetimeFormat, floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding, thousandsSeparator),
            maintainOrder);

    /// <summary>
    /// Run the query and write its rows as CSV into a <see cref="Stream"/> with asynchronous writes only, the engine read
    /// off the caller's thread (flushed, not disposed).
    /// </summary>
    /// <param name="stream">Writable destination stream.</param>
    /// <param name="cancellationToken">Stops the sink.</param>
    /// <inheritdoc cref="SinkCsvMemory"/>
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
        CancellationToken cancellationToken = default,
        CsvEncoding encoding = default,
        string? thousandsSeparator = null)
        => ManagedCsv.SinkCsvAsync(CloneHandle(), stream, DataFrame.CsvWriteArgs(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat,
            datetimeFormat, floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding, thousandsSeparator),
            maintainOrder, cancellationToken);

    /// <summary>
    /// Run the query and write its rows as CSV into a pipe with asynchronous flushes, the engine read off the caller's
    /// thread (flushed, not completed).
    /// </summary>
    /// <param name="writer">The pipe.</param>
    /// <param name="cancellationToken">Stops the sink.</param>
    /// <inheritdoc cref="SinkCsvMemory"/>
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
        CancellationToken cancellationToken = default,
        CsvEncoding encoding = default,
        string? thousandsSeparator = null)
        => ManagedCsv.SinkCsvAsync(CloneHandle(), writer, DataFrame.CsvWriteArgs(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat,
            datetimeFormat, floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding, thousandsSeparator),
            maintainOrder, cancellationToken);
}
