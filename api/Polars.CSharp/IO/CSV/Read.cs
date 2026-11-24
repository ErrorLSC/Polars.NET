#pragma warning disable CS1573 // overloads inherit the shared parameters' docs and document their own
using Polars.NET.Core;
using Polars.NET.Core.Csv;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable, IEnumerable<Series>, IPolarsDataFrame
{
    /// <summary>
    /// Read CSV into a DataFrame: a file, a directory of files, a glob, or any of them on a cloud store (read as one
    /// dataset: the files' columns united by name, the types inferred from the first files). The input is decoded on
    /// every core.
    /// </summary>
    /// <param name="path">The file, directory or glob (<c>s3://…</c>, <c>az://…</c>, <c>gs://…</c> on a store).</param>
    /// <param name="columns">The columns read, in this order (all when null or empty); the row index only when named among them.</param>
    /// <param name="hasHeader">Whether the first row names the columns. Defaults to true.</param>
    /// <param name="separator">The field separator. Defaults to ','.</param>
    /// <param name="quoteChar">The quote. Defaults to '"'; null or '\0' reads without quoting.</param>
    /// <param name="eolChar">The line end: '\n' (the default) ends records at LF, CRLF or a CR alone; another character ends them at it.</param>
    /// <param name="ignoreErrors">A value its column cannot hold is a null instead of an error (malformed CSV still is an error).</param>
    /// <param name="tryParseDates">Infer dates, times and datetimes from text. Defaults to true.</param>
    /// <param name="lowMemory">Accepted for compatibility; reading streams its input whatever it says.</param>
    /// <param name="rechunk">Contiguous columns (else in chunks as read). Defaults to false.</param>
    /// <param name="skipRows">Records skipped before the header (quote-aware). Defaults to 0.</param>
    /// <param name="skipRowsAfterHeader">Records skipped after the header. Defaults to 0.</param>
    /// <param name="nRows">Stop after this many rows (all when null).</param>
    /// <param name="inferSchemaLength">Rows to infer the column types from (100 by default). Null infers them from the whole file; values above 16,777,216 do the same. 0 reads every column as String.</param>
    /// <param name="inferSchemaFiles">The files the types are inferred from (10 by default, as dataframe engines take; <see cref="ulong.MaxValue"/> also means 10).</param>
    /// <param name="schema">Every column and its type: nothing is inferred. With a header the columns are matched by name (the frame in the schema's order); without one, by position.</param>
    /// <param name="dtypeOverride">The types of some inferred columns, by name (names the files lack are passed over).</param>
    /// <param name="encoding">The files' encoding (UTF-8 by default; Shift_JIS, GBK, Windows-1252, UTF-16 … see <see cref="CsvEncoding"/>).</param>
    /// <param name="nullValues">Values read as null.</param>
    /// <param name="missingIsNull">Empty unquoted fields are null (else empty strings). Defaults to true.</param>
    /// <param name="commentPrefix">Lines starting with this text are passed over.</param>
    /// <param name="decimalComma">',' is the decimal separator. Defaults to false.</param>
    /// <param name="truncateRaggedLines">Fields past the last column are dropped (else an error). Defaults to false.</param>
    /// <param name="rowIndexName">If provided, adds a first column (UInt32) numbering the rows.</param>
    /// <param name="rowIndexOffset">The row index's first number. Defaults to 0.</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    /// <param name="trueValues">Values read as true (localized booleans such as "VERDADEIRO"); they also steer inference.</param>
    /// <param name="falseValues">Values read as false.</param>
    /// <param name="thousandsSeparator">The digit group separator of numbers ("." with <paramref name="decimalComma"/>, " ", "'", ","; groups of three are checked). Off by default.</param>
    /// <param name="allowMissingColumns">A file lacking some of the dataset's columns reads them as nulls (else an error).</param>
    /// <param name="ignoreExtraColumns">Columns of later files the first files lack are passed over (else an error).</param>
    /// <param name="hivePartitioning">Whether <c>key=value</c> directories of the paths are columns (off by default).</param>
    /// <remarks>
    /// Errors are <see cref="Kaminari.Seseragi.Files.Csv.CsvFormatException"/> (malformed CSV) and
    /// <see cref="Kaminari.Seseragi.Files.Csv.CsvValueException"/> (a value its column cannot hold), naming the file, the
    /// row and the column.
    /// </remarks>
    public static DataFrame ReadCsv(
        string path,
        string[]? columns = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool rechunk = false,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        ulong? inferSchemaFiles = 18446744073709551615,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        CsvEncoding encoding = default,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        CloudOptions? cloudOptions = null,
        string[]? trueValues = null,
        string[]? falseValues = null,
        string? thousandsSeparator = null,
        bool allowMissingColumns = false,
        bool ignoreExtraColumns = false,
        bool hivePartitioning = false)
    {
        CsvScanArgs args = CsvReadArgs(hasHeader, separator, quoteChar, eolChar, ignoreErrors, tryParseDates, rechunk, skipRows, skipRowsAfterHeader, nRows,
            inferSchemaLength, inferSchemaFiles, encoding, nullValues, missingIsNull, commentPrefix, decimalComma, truncateRaggedLines, rowIndexName, rowIndexOffset,
            trueValues, falseValues, thousandsSeparator) with
        {
            AllowMissingColumns = allowMissingColumns,
            IgnoreExtraColumns = ignoreExtraColumns,
            HivePartitioning = hivePartitioning,
        };
        return ReadCsvWith(schema, dtypeOverride, args, a => ManagedCsv.ReadCsv(path, Asked(columns), a, CloudOptions.ToCloudArgs(cloudOptions)));
    }

    /// <summary>Read CSV from an in-memory byte array.</summary>
    /// <param name="buffer">The bytes, read in place during the call.</param>
    /// <inheritdoc cref="ReadCsv(string, string[], bool, char, char?, char, bool, bool, bool, bool, ulong, ulong, ulong?, ulong?, ulong?, IntoSchema?, IntoSchema?, CsvEncoding, string[], bool, string?, bool, bool, string?, ulong, CloudOptions?, string[], string[], string?, bool, bool, bool)"/>
    public static DataFrame ReadCsv(
        byte[] buffer,
        string[]? columns = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool rechunk = false,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        ulong? inferSchemaFiles = 18446744073709551615,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        CsvEncoding encoding = default,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        string[]? trueValues = null,
        string[]? falseValues = null,
        string? thousandsSeparator = null)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        return ReadCsv(buffer.AsMemory(), columns, hasHeader, separator, quoteChar, eolChar, ignoreErrors, tryParseDates, lowMemory, rechunk, skipRows,
            skipRowsAfterHeader, nRows, inferSchemaLength, inferSchemaFiles, schema, dtypeOverride, encoding, nullValues, missingIsNull, commentPrefix, decimalComma,
            truncateRaggedLines, rowIndexName, rowIndexOffset, trueValues, falseValues, thousandsSeparator);
    }

    /// <summary>Read CSV from in-memory bytes.</summary>
    /// <param name="buffer">The bytes, read in place during the call.</param>
    /// <inheritdoc cref="ReadCsv(string, string[], bool, char, char?, char, bool, bool, bool, bool, ulong, ulong, ulong?, ulong?, ulong?, IntoSchema?, IntoSchema?, CsvEncoding, string[], bool, string?, bool, bool, string?, ulong, CloudOptions?, string[], string[], string?, bool, bool, bool)"/>
    public static DataFrame ReadCsv(
        ReadOnlySpan<byte> buffer,
        string[]? columns = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool rechunk = false,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        ulong? inferSchemaFiles = 18446744073709551615,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        CsvEncoding encoding = default,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        string[]? trueValues = null,
        string[]? falseValues = null,
        string? thousandsSeparator = null)
    {
        CsvScanArgs args = CsvReadArgs(hasHeader, separator, quoteChar, eolChar, ignoreErrors, tryParseDates, rechunk, skipRows, skipRowsAfterHeader, nRows,
            inferSchemaLength, inferSchemaFiles, encoding, nullValues, missingIsNull, commentPrefix, decimalComma, truncateRaggedLines, rowIndexName, rowIndexOffset,
            trueValues, falseValues, thousandsSeparator);
        // A span cannot be captured: the schemas are taken here.
        try
        {
            return new DataFrame(ManagedCsv.ReadCsv(buffer, Asked(columns), args with { Schema = schema?.Consume().Handle, DtypeOverride = dtypeOverride?.Consume().Handle }));
        }
        finally
        {
            schema?.DisposeTempSchema();
            dtypeOverride?.DisposeTempSchema();
        }
    }

    /// <summary>Read CSV from in-memory bytes.</summary>
    /// <param name="buffer">The bytes, read in place during the call (unchanged meanwhile).</param>
    /// <inheritdoc cref="ReadCsv(string, string[], bool, char, char?, char, bool, bool, bool, bool, ulong, ulong, ulong?, ulong?, ulong?, IntoSchema?, IntoSchema?, CsvEncoding, string[], bool, string?, bool, bool, string?, ulong, CloudOptions?, string[], string[], string?, bool, bool, bool)"/>
    public static DataFrame ReadCsv(
        ReadOnlyMemory<byte> buffer,
        string[]? columns = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool rechunk = false,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        ulong? inferSchemaFiles = 18446744073709551615,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        CsvEncoding encoding = default,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        string[]? trueValues = null,
        string[]? falseValues = null,
        string? thousandsSeparator = null)
    {
        CsvScanArgs args = CsvReadArgs(hasHeader, separator, quoteChar, eolChar, ignoreErrors, tryParseDates, rechunk, skipRows, skipRowsAfterHeader, nRows,
            inferSchemaLength, inferSchemaFiles, encoding, nullValues, missingIsNull, commentPrefix, decimalComma, truncateRaggedLines, rowIndexName, rowIndexOffset,
            trueValues, falseValues, thousandsSeparator);
        return ReadCsvWith(schema, dtypeOverride, args, a => ManagedCsv.ReadCsv(buffer, Asked(columns), a));
    }

    /// <summary>Read CSV from a stream, decoded as it arrives (from its position; the stream is left open).</summary>
    /// <param name="stream">The stream.</param>
    /// <inheritdoc cref="ReadCsv(string, string[], bool, char, char?, char, bool, bool, bool, bool, ulong, ulong, ulong?, ulong?, ulong?, IntoSchema?, IntoSchema?, CsvEncoding, string[], bool, string?, bool, bool, string?, ulong, CloudOptions?, string[], string[], string?, bool, bool, bool)"/>
    public static DataFrame ReadCsv(
        Stream stream,
        string[]? columns = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool rechunk = false,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        ulong? inferSchemaFiles = null,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        CsvEncoding encoding = default,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        string[]? trueValues = null,
        string[]? falseValues = null,
        string? thousandsSeparator = null)
    {
        ArgumentNullException.ThrowIfNull(stream);
        CsvScanArgs args = CsvReadArgs(hasHeader, separator, quoteChar, eolChar, ignoreErrors, tryParseDates, rechunk, skipRows, skipRowsAfterHeader, nRows,
            inferSchemaLength, inferSchemaFiles, encoding, nullValues, missingIsNull, commentPrefix, decimalComma, truncateRaggedLines, rowIndexName, rowIndexOffset,
            trueValues, falseValues, thousandsSeparator);
        return ReadCsvWith(schema, dtypeOverride, args, a => ManagedCsv.ReadCsv(stream, Asked(columns), a));
    }

    /// <summary>Asynchronously read CSV into a DataFrame (read off the caller's thread).</summary>
    /// <param name="columns">The columns read, in this order (all when null or empty); the row index is added when not named.</param>
    /// <inheritdoc cref="ReadCsv(string, string[], bool, char, char?, char, bool, bool, bool, bool, ulong, ulong, ulong?, ulong?, ulong?, IntoSchema?, IntoSchema?, CsvEncoding, string[], bool, string?, bool, bool, string?, ulong, CloudOptions?, string[], string[], string?, bool, bool, bool)"/>
    public static Task<DataFrame> ReadCsvAsync(
        string path,
        string[]? columns = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool rechunk = false,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        ulong? inferSchemaFiles = 18446744073709551615,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        CsvEncoding encoding = default,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        CloudOptions? cloudOptions = null,
        string[]? trueValues = null,
        string[]? falseValues = null,
        string? thousandsSeparator = null,
        bool allowMissingColumns = false,
        bool ignoreExtraColumns = false,
        bool hivePartitioning = false)
    {
        // As released: with columns named, the row index is added after them.
        string[]? read = Asked(columns) is { } asked && !string.IsNullOrEmpty(rowIndexName) && !asked.Contains(rowIndexName) ? [.. asked, rowIndexName] : columns;
        return Task.Run(() => ReadCsv(path, read, hasHeader, separator, quoteChar, eolChar, ignoreErrors, tryParseDates, lowMemory, rechunk, skipRows,
            skipRowsAfterHeader, nRows, inferSchemaLength, inferSchemaFiles, schema, dtypeOverride, encoding, nullValues, missingIsNull, commentPrefix, decimalComma,
            truncateRaggedLines, rowIndexName, rowIndexOffset, cloudOptions, trueValues, falseValues, thousandsSeparator, allowMissingColumns, ignoreExtraColumns,
            hivePartitioning));
    }

    // Empty column lists read every column (as released).
    private static string[]? Asked(string[]? columns) => columns is { Length: > 0 } ? columns : null;

    // The options every CSV read takes (the schemas are put in by ReadCsvWith).
    internal static CsvScanArgs CsvReadArgs(bool hasHeader, char separator, char? quoteChar, char eolChar, bool ignoreErrors, bool tryParseDates, bool rechunk,
        ulong skipRows, ulong skipRowsAfterHeader, ulong? nRows, ulong? inferSchemaLength, ulong? inferSchemaFiles, CsvEncoding encoding, string[]? nullValues,
        bool missingIsNull, string? commentPrefix, bool decimalComma, bool truncateRaggedLines, string? rowIndexName, ulong rowIndexOffset, string[]? trueValues,
        string[]? falseValues, string? thousandsSeparator) => new()
    {
        HasHeader = hasHeader,
        Separator = separator,
        QuoteChar = quoteChar,
        EolChar = eolChar,
        IgnoreErrors = ignoreErrors,
        TryParseDates = tryParseDates,
        Rechunk = rechunk,
        SkipRows = skipRows,
        SkipRowsAfterHeader = skipRowsAfterHeader,
        NRows = nRows,
        InferSchemaLength = inferSchemaLength,
        InferSchemaFiles = inferSchemaFiles,
        Encoding = encoding.Given,
        LossyEncoding = encoding.IsLossy,
        NullValues = nullValues,
        MissingIsNull = missingIsNull,
        CommentPrefix = commentPrefix,
        DecimalComma = decimalComma,
        TruncateRaggedLines = truncateRaggedLines,
        RowIndexName = rowIndexName,
        RowIndexOffset = rowIndexOffset,
        TrueValues = trueValues,
        FalseValues = falseValues,
        ThousandsSeparator = thousandsSeparator,
    };

    // The schemas given (borrowed by the read; temporary ones disposed after it).
    private static DataFrame ReadCsvWith(IntoSchema? schema, IntoSchema? overrides, CsvScanArgs args, Func<CsvScanArgs, DataFrameHandle> read)
    {
        try
        {
            return new DataFrame(read(args with { Schema = schema?.Consume().Handle, DtypeOverride = overrides?.Consume().Handle }));
        }
        finally
        {
            schema?.DisposeTempSchema();
            overrides?.DisposeTempSchema();
        }
    }
}
