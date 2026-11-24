#pragma warning disable CS1573 // overloads inherit the shared parameters' docs and document their own
using System.IO.Pipelines;
using Polars.NET.Core;
using Polars.NET.Core.Csv;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable, IEnumerable<Series>, IPolarsDataFrame
{
    /// <summary>
    /// Write the DataFrame as CSV to a file, local or on a cloud store: encoded on every core, published whole once
    /// written (a failed write leaves no file).
    /// </summary>
    /// <param name="path">The output file path (<c>s3://…</c>, <c>az://…</c>, <c>gs://…</c> on a store).</param>
    /// <param name="includeHeader">Whether to write the header row. Defaults to true.</param>
    /// <param name="includeBom">Whether to start with the encoding's byte order mark. Defaults to false.</param>
    /// <param name="separator">The field separator. Defaults to ','.</param>
    /// <param name="quoteChar">The quote. Defaults to '"'.</param>
    /// <param name="quoteStyle">When fields are quoted. Defaults to <see cref="QuoteStyle.Necessary"/>.</param>
    /// <param name="nullValue">The text of nulls. Defaults to an empty field.</param>
    /// <param name="lineTerminator">The line end. Defaults to "\n".</param>
    /// <param name="floatScientific">Floats always (true) or never (false) in scientific notation; null (default) as is shortest.</param>
    /// <param name="floatPrecision">Floats with this many decimals; null (default) with the digits that read back the same value.</param>
    /// <param name="decimalComma">',' as the decimal separator. Defaults to false.</param>
    /// <param name="dateFormat">The format of Date columns (strftime). ISO 8601 when null.</param>
    /// <param name="timeFormat">The format of Time columns (strftime). ISO 8601 when null.</param>
    /// <param name="datetimeFormat">The format of Datetime columns (strftime). ISO 8601 when null.</param>
    /// <param name="checkExtension">Whether the path's extension must fit the compression (.gz, .zst). Defaults to true.</param>
    /// <param name="compression">Compression of the whole file (Gzip/Zstd). Defaults to none.</param>
    /// <param name="compressionLevel">The compression level; -1 for the codec's default.</param>
    /// <param name="maintainOrder">Accepted for compatibility: rows are written in order.</param>
    /// <param name="syncOnClose">Whether the file is flushed to the disk before it is published. Defaults to None.</param>
    /// <param name="mkdir">Create the file's directory when it is missing. Defaults to false.</param>
    /// <param name="batchSize">Accepted for compatibility; the writer sizes its units itself.</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    /// <param name="encoding">The output's encoding (UTF-8 by default; Shift_JIS, GBK, Windows-1252, UTF-16 … see <see cref="CsvEncoding"/>). Strict: a character it cannot hold is an error naming the row, unless lossy.</param>
    /// <param name="thousandsSeparator">A digit group separator written in integers and floats ("." with <paramref name="decimalComma"/>, " ", "'"). Off by default.</param>
    /// <remarks>Durations are written as ISO 8601 (PT1.5S). Errors are <see cref="Kaminari.Seseragi.Files.Csv.CsvWriteException"/>, naming the row and the column.</remarks>
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
        CloudOptions? cloudOptions = null,
        CsvEncoding encoding = default,
        string? thousandsSeparator = null)
        => ManagedCsv.WriteCsv(Handle, path,
            CsvWriteArgs(includeHeader, includeBom, separator, quoteChar, quoteStyle, nullValue, lineTerminator, floatScientific, floatPrecision, decimalComma,
                dateFormat, timeFormat, datetimeFormat, compression, compressionLevel, encoding, thousandsSeparator),
            checkExtension, syncOnClose.ToNative(), mkdir, CloudOptions.ToCloudArgs(cloudOptions));

    /// <summary>
    /// Write the DataFrame as a dataset of CSV files under <paramref name="path"/>, in Hive partitions of
    /// <paramref name="partitionBy"/> (<c>key=value</c> directories).
    /// </summary>
    /// <param name="path">The dataset's root directory.</param>
    /// <param name="partitionBy">The columns the rows are partitioned by.</param>
    /// <param name="includeKeys">Whether the files also hold the key columns. Defaults to true.</param>
    /// <param name="keysPreGrouped">The rows come grouped by their keys: each partition's files are finished as its keys end. Rows not grouped write more files, never wrong ones.</param>
    /// <param name="maxRowsPerFile">The most rows a file holds; 0 means no limit.</param>
    /// <param name="approxBytesPerFile">About how many bytes a file holds; 0 means no limit.</param>
    /// <param name="checkExtension">Unused: the files are named by the writer.</param>
    /// <param name="mkdir">Unused: the dataset's directories are created.</param>
    /// <param name="existingData">What happens to files under <paramref name="path"/> already (an error by default).</param>
    /// <remarks>
    /// Files are named <c>part-{index:D5}-{write id}.csv</c> (<c>.gz</c>, <c>.zst</c> when compressed), each with its header,
    /// each published whole.
    /// </remarks>
    /// <inheritdoc cref="WriteCsv(string, bool, bool, char, char, QuoteStyle, string?, string?, bool?, int?, bool, string?, string?, string?, bool, ExternalCompression, int, bool, SyncOnClose, bool, int, CloudOptions?, CsvEncoding, string?)"/>
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
        CloudOptions? cloudOptions = null,
        CsvEncoding encoding = default,
        string? thousandsSeparator = null,
        ExistingData existingData = ExistingData.Error)
    {
        using LazyFrame lf = Lazy();
        lf.SinkCsvPartitioned(path, partitionBy, includeKeys, keysPreGrouped, maxRowsPerFile, approxBytesPerFile, includeHeader, includeBom, separator, quoteChar,
            quoteStyle, nullValue, lineTerminator, floatScientific, floatPrecision, decimalComma, dateFormat, timeFormat, datetimeFormat, checkExtension, compression,
            compressionLevel, maintainOrder, syncOnClose, mkdir, batchSize, cloudOptions, encoding, thousandsSeparator, existingData);
    }

    /// <summary>The DataFrame as CSV bytes.</summary>
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
        bool maintainOrder = true,
        CsvEncoding encoding = default,
        string? thousandsSeparator = null)
        => ManagedCsv.WriteCsvToMemory(Handle, CsvWriteArgs(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
            floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding, thousandsSeparator));

    /// <summary>The DataFrame as CSV in native memory, handed over without a copy.</summary>
    /// <inheritdoc cref="LazyFrame.SinkCsvNativeMemory"/>
    public NativeBuffer WriteCsvNativeMemory(
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
        => ManagedCsv.WriteCsvNativeMemory(Handle, CsvWriteArgs(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
            floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding, thousandsSeparator));

    /// <summary>Write the DataFrame as CSV into a <see cref="Stream"/> (flushed, not disposed).</summary>
    /// <param name="stream">Writable destination stream.</param>
    /// <inheritdoc cref="LazyFrame.SinkCsv(Stream, bool, bool, int, bool, ExternalCompression, int, string?, string?, string?, int, int, bool, byte, byte, string?, string?, QuoteStyle, bool, CsvEncoding, string?)"/>
    public void WriteCsv(
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
        => ManagedCsv.WriteCsv(Handle, stream, CsvWriteArgs(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
            floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding, thousandsSeparator));

    /// <summary>Write the DataFrame as CSV into a <see cref="Stream"/> with asynchronous writes only (flushed, not disposed).</summary>
    /// <inheritdoc cref="LazyFrame.SinkCsvAsync(Stream, bool, bool, int, bool, ExternalCompression, int, string?, string?, string?, int, int, bool, byte, byte, string?, string?, QuoteStyle, bool, CancellationToken, CsvEncoding, string?)"/>
    public Task WriteCsvAsync(
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
        => ManagedCsv.WriteCsvAsync(Handle, stream, CsvWriteArgs(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
            floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding, thousandsSeparator), cancellationToken);

    /// <summary>Write the DataFrame as CSV into a pipe with asynchronous flushes (flushed, not completed).</summary>
    /// <inheritdoc cref="LazyFrame.SinkCsvAsync(PipeWriter, bool, bool, int, bool, ExternalCompression, int, string?, string?, string?, int, int, bool, byte, byte, string?, string?, QuoteStyle, bool, CancellationToken, CsvEncoding, string?)"/>
    public Task WriteCsvAsync(
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
        => ManagedCsv.WriteCsvAsync(Handle, writer, CsvWriteArgs(includeBom, includeHeader, compressionCode, compressionLevel, dateFormat, timeFormat, datetimeFormat,
            floatScientific, floatPrecision, decimalComma, separator, quoteChar, nullValue, lineTerminator, quoteStyle, encoding, thousandsSeparator), cancellationToken);

    // How CSV is written, from the path overloads' parameters.
    internal static CsvWriteArgs CsvWriteArgs(bool includeHeader, bool includeBom, char separator, char quoteChar, QuoteStyle quoteStyle, string? nullValue,
        string? lineTerminator, bool? floatScientific, int? floatPrecision, bool decimalComma, string? dateFormat, string? timeFormat, string? datetimeFormat,
        ExternalCompression compression, int compressionLevel, CsvEncoding encoding, string? thousandsSeparator) => new()
    {
        IncludeHeader = includeHeader,
        IncludeBom = includeBom,
        Separator = separator,
        QuoteChar = quoteChar,
        QuoteStyle = quoteStyle.ToNative(),
        NullValue = nullValue,
        LineTerminator = lineTerminator,
        FloatScientific = floatScientific,
        FloatPrecision = floatPrecision,
        DecimalComma = decimalComma,
        DateFormat = dateFormat,
        TimeFormat = timeFormat,
        DatetimeFormat = datetimeFormat,
        ThousandsSeparator = thousandsSeparator,
        Encoding = encoding.Given,
        LossyEncoding = encoding.IsLossy,
        Compression = compression.ToNative(),
        CompressionLevel = compressionLevel,
    };

    // How CSV is written, from the memory and stream overloads' parameters (floats: -1 unset, 0 false, 1 true; precision -1 unset).
    internal static CsvWriteArgs CsvWriteArgs(bool includeBom, bool includeHeader, ExternalCompression compression, int compressionLevel, string? dateFormat,
        string? timeFormat, string? datetimeFormat, int floatScientific, int floatPrecision, bool decimalComma, byte separator, byte quoteChar, string? nullValue,
        string? lineTerminator, QuoteStyle quoteStyle, CsvEncoding encoding, string? thousandsSeparator) =>
        CsvWriteArgs(includeHeader, includeBom, (char)separator, (char)quoteChar, quoteStyle, nullValue, lineTerminator,
            floatScientific switch { 0 => false, 1 => true, _ => null }, floatPrecision >= 0 ? floatPrecision : null, decimalComma, dateFormat, timeFormat,
            datetimeFormat, compression, compressionLevel, encoding, thousandsSeparator);
}
