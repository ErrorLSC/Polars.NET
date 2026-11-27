#pragma warning disable CS1573 // overloads inherit the shared parameters' docs and document their own
using Polars.NET.Core;
using Polars.NET.Core.Csv;

namespace Polars.CSharp;

/// <summary>
/// Represents a lazily evaluated DataFrame.
/// Until the query is executed, operations are just recorded in a query plan.
/// Once executed, the data is materialized in memory.
/// </summary>
public partial class LazyFrame : IDisposable
{
    /// <summary>
    /// Lazily read CSV: a file, a directory of files, a glob, or any of them on a cloud store (one dataset: the files'
    /// columns united by name). The files are listed and the columns inferred now (from the first files); the data is
    /// read each time the query runs: only the columns it reads and the leading rows it keeps, streamed batch by batch.
    /// </summary>
    /// <param name="path">The file, directory or glob (<c>s3://…</c>, <c>az://…</c>, <c>gs://…</c> on a store).</param>
    /// <param name="schema">Every column and its type: nothing is inferred. With a header the columns are matched by name (the frame in the schema's order); without one, by position.</param>
    /// <param name="dtypeOverride">The types of some inferred columns, by name (names the files lack are passed over).</param>
    /// <param name="hasHeader">Whether the first row names the columns. Defaults to true.</param>
    /// <param name="separator">The field separator. Defaults to ','.</param>
    /// <param name="quoteChar">The quote. Defaults to '"'; null or '\0' reads without quoting.</param>
    /// <param name="eolChar">The line end: '\n' (the default) ends records at LF, CRLF or a CR alone; another character ends them at it.</param>
    /// <param name="ignoreErrors">A value its column cannot hold is a null instead of an error (malformed CSV still is an error).</param>
    /// <param name="tryParseDates">Infer dates, times and datetimes from text. Defaults to true.</param>
    /// <param name="lowMemory">Accepted for compatibility; reading streams its input whatever it says.</param>
    /// <param name="cache">Accepted for compatibility; it changes nothing.</param>
    /// <param name="glob">Expand <c>*</c>, <c>?</c> and <c>[…]</c> in the path. Defaults to true.</param>
    /// <param name="rechunk">Accepted for compatibility; a scan streams batches whatever it says.</param>
    /// <param name="raiseIfEmpty">Empty input is an error (else an empty frame). Defaults to true.</param>
    /// <param name="skipRows">Records skipped before the header (quote-aware). Defaults to 0.</param>
    /// <param name="skipRowsAfterHeader">Records skipped after the header. Defaults to 0.</param>
    /// <param name="skipLines">Raw lines skipped first, quotes not considered. Defaults to 0.</param>
    /// <param name="nRows">Stop after this many rows (all when null).</param>
    /// <param name="inferSchemaLength">Rows to infer the column types from (100 by default). Null infers them from the whole file; values above 16,777,216 do the same. 0 reads every column as String.</param>
    /// <param name="inferSchemaFiles">The files the types are inferred from (10 by default, as dataframe engines take; <see cref="ulong.MaxValue"/> also means 10).</param>
    /// <param name="nThreads">The threads decoding (all cores when null).</param>
    /// <param name="chunkSize">Accepted for compatibility; the reader sizes its units itself.</param>
    /// <param name="rowIndexName">If provided, adds a first column (UInt32) numbering the rows across the files.</param>
    /// <param name="rowIndexOffset">The row index's first number. Defaults to 0.</param>
    /// <param name="includeFilePaths">If provided, adds a last column with each row's file path.</param>
    /// <param name="encoding">The files' encoding (UTF-8 by default; Shift_JIS, GBK, Windows-1252, UTF-16 … see <see cref="CsvEncoding"/>).</param>
    /// <param name="nullValues">Values read as null.</param>
    /// <param name="missingIsNull">Empty unquoted fields are null (else empty strings). Defaults to true.</param>
    /// <param name="commentPrefix">Lines starting with this text are passed over.</param>
    /// <param name="decimalComma">',' is the decimal separator. Defaults to false.</param>
    /// <param name="truncateRaggedLines">Fields past the last column are dropped (else an error). Defaults to false.</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    /// <param name="trueValues">Values read as true (localized booleans such as "VERDADEIRO"); they also steer inference.</param>
    /// <param name="falseValues">Values read as false.</param>
    /// <param name="thousandsSeparator">The digit group separator of numbers ("." with <paramref name="decimalComma"/>, " ", "'", ","; groups of three are checked). Off by default.</param>
    /// <param name="allowMissingColumns">A file lacking some of the dataset's columns reads them as nulls (else an error).</param>
    /// <param name="ignoreExtraColumns">Columns of later files the first files lack are passed over (else an error).</param>
    /// <param name="hivePartitioning">Whether <c>key=value</c> directories of the paths are columns (off by default).</param>
    /// <returns>A new LazyFrame.</returns>
    /// <remarks>
    /// The query's filters are pushed into the reader: it skips the rows they reject (decoding only the columns they
    /// read), leaves out the partitions they rule out, and applies exactly the ones it can (the engine applies the
    /// rest). Errors are <see cref="Kaminari.Seseragi.Files.Csv.CsvFormatException"/>
    /// (malformed CSV) and <see cref="Kaminari.Seseragi.Files.Csv.CsvValueException"/> (a value its column cannot hold),
    /// naming the file, the row and the column.
    /// </remarks>
    public static LazyFrame ScanCsv(
        string path,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool cache = true,
        bool glob = true,
        bool rechunk = false,
        bool raiseIfEmpty = true,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong skipLines = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        ulong? inferSchemaFiles = 18446744073709551615,
        ulong? nThreads = null,
        ulong? chunkSize = null,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        string? includeFilePaths = null,
        CsvEncoding encoding = default,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        CloudOptions? cloudOptions = null,
        string[]? trueValues = null,
        string[]? falseValues = null,
        string? thousandsSeparator = null,
        bool allowMissingColumns = false,
        bool ignoreExtraColumns = false,
        bool hivePartitioning = false)
    {
        CsvScanArgs args = CsvScanOptions(hasHeader, separator, quoteChar, eolChar, ignoreErrors, tryParseDates, rechunk, raiseIfEmpty, skipRows, skipRowsAfterHeader,
            skipLines, nRows, inferSchemaLength, inferSchemaFiles, nThreads, rowIndexName, rowIndexOffset, encoding, nullValues, missingIsNull, commentPrefix,
            decimalComma, truncateRaggedLines, trueValues, falseValues, thousandsSeparator) with
        {
            Glob = glob,
            IncludeFilePaths = includeFilePaths,
            AllowMissingColumns = allowMissingColumns,
            IgnoreExtraColumns = ignoreExtraColumns,
            HivePartitioning = hivePartitioning,
        };
        return ScanCsvWith(schema, dtypeOverride, args, a => ManagedCsv.ScanCsv(path, a, CloudOptions.ToCloudArgs(cloudOptions)));
    }

    /// <summary>Lazily read CSV from an in-memory byte array.</summary>
    /// <param name="buffer">The bytes, copied (into native memory the scan keeps).</param>
    /// <param name="glob">Unused for memory.</param>
    /// <param name="includeFilePaths">Unused for memory.</param>
    /// <inheritdoc cref="ScanCsv(string, IntoSchema?, IntoSchema?, bool, char, char?, char, bool, bool, bool, bool, bool, bool, bool, ulong, ulong, ulong, ulong?, ulong?, ulong?, ulong?, ulong?, string?, ulong, string?, CsvEncoding, string[], bool, string?, bool, bool, CloudOptions?, string[], string[], string?, bool, bool, bool)"/>
    public static LazyFrame ScanCsv(
        byte[] buffer,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool cache = true,
        bool glob = true,
        bool rechunk = false,
        bool raiseIfEmpty = true,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong skipLines = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        ulong? inferSchemaFiles = 18446744073709551615,
        ulong? nThreads = null,
        ulong? chunkSize = null,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        string? includeFilePaths = null,
        CsvEncoding encoding = default,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string[]? trueValues = null,
        string[]? falseValues = null,
        string? thousandsSeparator = null)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        return ScanCsv(buffer.AsSpan(), schema, dtypeOverride, hasHeader, separator, quoteChar, eolChar, ignoreErrors, tryParseDates, lowMemory, cache, glob,
            rechunk, raiseIfEmpty, skipRows, skipRowsAfterHeader, skipLines, nRows, inferSchemaLength, inferSchemaFiles, nThreads, chunkSize, rowIndexName,
            rowIndexOffset, includeFilePaths, encoding, nullValues, missingIsNull, commentPrefix, decimalComma, truncateRaggedLines, trueValues, falseValues,
            thousandsSeparator);
    }

    /// <summary>Lazily read CSV from in-memory bytes.</summary>
    /// <param name="buffer">The bytes, copied (into native memory the scan keeps).</param>
    /// <param name="glob">Unused for memory.</param>
    /// <param name="includeFilePaths">Unused for memory.</param>
    /// <inheritdoc cref="ScanCsv(string, IntoSchema?, IntoSchema?, bool, char, char?, char, bool, bool, bool, bool, bool, bool, bool, ulong, ulong, ulong, ulong?, ulong?, ulong?, ulong?, ulong?, string?, ulong, string?, CsvEncoding, string[], bool, string?, bool, bool, CloudOptions?, string[], string[], string?, bool, bool, bool)"/>
    public static LazyFrame ScanCsv(
        ReadOnlySpan<byte> buffer,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool cache = true,
        bool glob = true,
        bool rechunk = false,
        bool raiseIfEmpty = true,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong skipLines = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        ulong? inferSchemaFiles = 18446744073709551615,
        ulong? nThreads = null,
        ulong? chunkSize = null,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        string? includeFilePaths = null,
        CsvEncoding encoding = default,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string[]? trueValues = null,
        string[]? falseValues = null,
        string? thousandsSeparator = null)
    {
        CsvScanArgs args = CsvScanOptions(hasHeader, separator, quoteChar, eolChar, ignoreErrors, tryParseDates, rechunk, raiseIfEmpty, skipRows, skipRowsAfterHeader,
            skipLines, nRows, inferSchemaLength, inferSchemaFiles, nThreads, rowIndexName, rowIndexOffset, encoding, nullValues, missingIsNull, commentPrefix,
            decimalComma, truncateRaggedLines, trueValues, falseValues, thousandsSeparator);
        // A span cannot be captured: the schemas are taken here.
        try
        {
            return new LazyFrame(ManagedCsv.ScanCsv(buffer, args with { Schema = schema?.Consume().Handle, DtypeOverride = dtypeOverride?.Consume().Handle }));
        }
        finally
        {
            schema?.DisposeTempSchema();
            dtypeOverride?.DisposeTempSchema();
        }
    }

    /// <summary>Lazily read CSV from in-memory bytes, read in place each time the query runs.</summary>
    /// <param name="buffer">The bytes, read in place without a copy: the memory stays pinned until the last frame reading it is released, and must not change meanwhile. Wrap unmanaged memory in a <see cref="System.Buffers.MemoryManager{T}"/> and free it only once the engine unpins it (<see cref="System.Buffers.MemoryManager{T}.Unpin"/>): a query can still be finishing on a background thread right after its frames are released.</param>
    /// <param name="glob">Unused for memory.</param>
    /// <param name="includeFilePaths">Unused for memory.</param>
    /// <inheritdoc cref="ScanCsv(string, IntoSchema?, IntoSchema?, bool, char, char?, char, bool, bool, bool, bool, bool, bool, bool, ulong, ulong, ulong, ulong?, ulong?, ulong?, ulong?, ulong?, string?, ulong, string?, CsvEncoding, string[], bool, string?, bool, bool, CloudOptions?, string[], string[], string?, bool, bool, bool)"/>
    public static LazyFrame ScanCsv(
        ReadOnlyMemory<byte> buffer,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool cache = true,
        bool glob = true,
        bool rechunk = false,
        bool raiseIfEmpty = true,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong skipLines = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        ulong? inferSchemaFiles = 18446744073709551615,
        ulong? nThreads = null,
        ulong? chunkSize = null,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        string? includeFilePaths = null,
        CsvEncoding encoding = default,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string[]? trueValues = null,
        string[]? falseValues = null,
        string? thousandsSeparator = null)
    {
        CsvScanArgs args = CsvScanOptions(hasHeader, separator, quoteChar, eolChar, ignoreErrors, tryParseDates, rechunk, raiseIfEmpty, skipRows, skipRowsAfterHeader,
            skipLines, nRows, inferSchemaLength, inferSchemaFiles, nThreads, rowIndexName, rowIndexOffset, encoding, nullValues, missingIsNull, commentPrefix,
            decimalComma, truncateRaggedLines, trueValues, falseValues, thousandsSeparator);
        return ScanCsvWith(schema, dtypeOverride, args, a => ManagedCsv.ScanCsv(buffer, a));
    }

    // The options every CSV scan takes (the schemas are put in by ScanCsvWith).
    private static CsvScanArgs CsvScanOptions(bool hasHeader, char separator, char? quoteChar, char eolChar, bool ignoreErrors, bool tryParseDates, bool rechunk,
        bool raiseIfEmpty, ulong skipRows, ulong skipRowsAfterHeader, ulong skipLines, ulong? nRows, ulong? inferSchemaLength, ulong? inferSchemaFiles,
        ulong? nThreads, string? rowIndexName, ulong rowIndexOffset, CsvEncoding encoding, string[]? nullValues, bool missingIsNull, string? commentPrefix,
        bool decimalComma, bool truncateRaggedLines, string[]? trueValues, string[]? falseValues, string? thousandsSeparator) =>
        DataFrame.CsvReadArgs(hasHeader, separator, quoteChar, eolChar, ignoreErrors, tryParseDates, rechunk, skipRows, skipRowsAfterHeader, nRows,
            inferSchemaLength, inferSchemaFiles, encoding, nullValues, missingIsNull, commentPrefix, decimalComma, truncateRaggedLines, rowIndexName,
            rowIndexOffset, trueValues, falseValues, thousandsSeparator) with
        {
            RaiseIfEmpty = raiseIfEmpty,
            SkipLines = skipLines,
            NThreads = nThreads,
        };

    // The schemas given (borrowed while the scan is made; temporary ones disposed after it).
    private static LazyFrame ScanCsvWith(IntoSchema? schema, IntoSchema? overrides, CsvScanArgs args, Func<CsvScanArgs, LazyFrameHandle> scan)
    {
        try
        {
            return new LazyFrame(scan(args with { Schema = schema?.Consume().Handle, DtypeOverride = overrides?.Consume().Handle }));
        }
        finally
        {
            schema?.DisposeTempSchema();
            overrides?.DisposeTempSchema();
        }
    }
}
