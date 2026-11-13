using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Excel.Reading;

namespace Kaminari.Seseragi.Files.Excel;

/// <summary>Options for <see cref="ExcelReader"/>.</summary>
public sealed record ExcelReadOptions
{
    /// <summary>Default number of rows per record batch.</summary>
    public const int DefaultBatchSize = 65_536;

    /// <summary>The sheet to read, by name (case-insensitive). Takes precedence over <see cref="SheetIndex"/>.</summary>
    public string? SheetName { get; init; }

    /// <summary>The sheet to read, by zero-based position among the sheet tabs (hidden sheets and chart sheets count; a
    /// chart sheet cannot be read).</summary>
    public int SheetIndex { get; init; }

    /// <summary>
    /// Read a table (list object) by name (case-insensitive): its range only, its own header row, its totals row
    /// left out. Overrides <see cref="SheetName"/>, <see cref="SheetIndex"/> and <see cref="HasHeader"/>.
    /// </summary>
    public string? TableName { get; init; }

    /// <summary>
    /// The first row with a value holds the column names (default). A blank name becomes <c>__UNNAMED__i</c> (i the
    /// column's position); a repeated one gets <c>_1</c>, <c>_2</c>… Without a header, columns are named
    /// <c>column_1</c>, <c>column_2</c>…
    /// </summary>
    public bool HasHeader { get; init; } = true;

    /// <summary>Columns to read, by name. Columns come back in sheet order. Cannot be combined with <see cref="ColumnIndices"/>.</summary>
    public IReadOnlyList<string>? Columns { get; init; }

    /// <summary>Columns to read, by zero-based position among the columns read without a projection.</summary>
    public IReadOnlyList<int>? ColumnIndices { get; init; }

    /// <summary>
    /// Column types by name instead of inferred ones. Numbers convert to integer (integral values in range only),
    /// floating-point, decimal (exact at the scale only), string and temporal types (as serial numbers); text is
    /// parsed (invariant culture, ISO 8601 for temporal types); booleans read as booleans or strings. A time zone on a
    /// timestamp type reads the workbook's clock times as that zone's local time.
    /// </summary>
    public IReadOnlyDictionary<string, IArrowType>? SchemaOverrides { get; init; }

    /// <summary>
    /// Rows the column types are inferred from (held back until the schema is known); null reads the whole sheet
    /// first. Whole numbers give Int64, other numbers Float64, booleans Boolean, text Utf8; by number format, dates
    /// give Date32, dates with times Timestamp(µs), times of day Time64(µs) (a time format over a whole serial number is a
    /// date-time), elapsed times Duration(µs); any other mix,
    /// or no value at all, gives Utf8. A later value that does not fit raises <see cref="ExcelValueException"/>.
    /// 0 reads every column as Utf8.
    /// </summary>
    public int? InferSchemaLength { get; init; } = 100;

    /// <summary>Skip rows without values (default). Otherwise empty rows between rows with values read as nulls.</summary>
    public bool DropEmptyRows { get; init; } = true;

    /// <summary>Leave out columns without a header name and without values in the inference window (default).</summary>
    public bool DropEmptyColumns { get; init; } = true;

    /// <summary>Raise <see cref="ExcelFormatException"/> for a sheet without data rows (default).</summary>
    public bool RaiseIfEmpty { get; init; } = true;

    /// <summary>Rows per record batch (the last batch may be shorter).</summary>
    public int BatchSize { get; init; } = DefaultBatchSize;

    /// <summary>Leave the stream open when the reader is disposed.</summary>
    public bool LeaveOpen { get; init; }

    internal void Validate()
    {
        if (BatchSize <= 0) throw new ArgumentOutOfRangeException(nameof(BatchSize), "BatchSize must be positive.");
        if (InferSchemaLength < 0) throw new ArgumentOutOfRangeException(nameof(InferSchemaLength), "InferSchemaLength must not be negative.");
        if (SheetIndex < 0) throw new ArgumentOutOfRangeException(nameof(SheetIndex), "SheetIndex must not be negative.");
        if (Columns is not null && ColumnIndices is not null)
            throw new ArgumentException("Specify Columns or ColumnIndices, not both.");
    }
}

/// <summary>
/// Streams one sheet of a workbook as Arrow record batches in native memory. Opening reads the header and the
/// inference window (<see cref="ExcelReadOptions.InferSchemaLength"/>), so <see cref="Schema"/> is known at once;
/// the rest of the sheet is decompressed and parsed as the batches are read. Error values (<c>#N/A</c>,
/// <c>#DIV/0!</c> …) read as nulls.
/// </summary>
public sealed class ExcelReader : IDisposable, IAsyncDisposable
{
    private readonly ExcelWorkbook? _ownedWorkbook;
    private readonly SheetReader _sheet;

    private ExcelReader(ExcelWorkbook? ownedWorkbook, SheetReader sheet, ExcelSheetInfo info)
    {
        _ownedWorkbook = ownedWorkbook;
        _sheet = sheet;
        Sheet = info;
    }

    /// <summary>The Arrow schema of the batches.</summary>
    public Schema Schema => _sheet.Schema;

    /// <summary>The sheet being read.</summary>
    public ExcelSheetInfo Sheet { get; }

    /// <summary>Opens a sheet of a workbook file.</summary>
    public static ExcelReader Open(string path, ExcelReadOptions? options = null) =>
        OpenStreamCoreAsync(ExcelWorkbook.OpenFile(path), (options ?? new()) with { LeaveOpen = false }, false, default).AsTask().GetAwaiter().GetResult();

    /// <summary>Opens a sheet of a workbook in a stream (disposed with the reader unless <see cref="ExcelReadOptions.LeaveOpen"/>).</summary>
    public static ExcelReader Open(Stream stream, ExcelReadOptions? options = null) =>
        OpenStreamCoreAsync(stream, options ?? new(), false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="Open(string, ExcelReadOptions?)"/>
    public static async Task<ExcelReader> OpenAsync(string path, ExcelReadOptions? options = null, CancellationToken cancellationToken = default) =>
        await OpenStreamCoreAsync(ExcelWorkbook.OpenFile(path), (options ?? new()) with { LeaveOpen = false }, true, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc cref="Open(Stream, ExcelReadOptions?)"/>
    public static async Task<ExcelReader> OpenAsync(Stream stream, ExcelReadOptions? options = null, CancellationToken cancellationToken = default) =>
        await OpenStreamCoreAsync(stream, options ?? new(), true, cancellationToken).ConfigureAwait(false);

    private static async ValueTask<ExcelReader> OpenStreamCoreAsync(Stream stream, ExcelReadOptions options, bool useAsync, CancellationToken ct)
    {
        options.Validate();
        var workbook = await ExcelWorkbook.OpenCoreAsync(stream, options.LeaveOpen, useAsync, ct).ConfigureAwait(false);
        return await OpenCoreAsync(workbook, options, ownsWorkbook: true, useAsync, ct).ConfigureAwait(false);
    }

    internal static async ValueTask<ExcelReader> OpenCoreAsync(ExcelWorkbook workbook, ExcelReadOptions options, bool ownsWorkbook,
        bool useAsync, CancellationToken ct)
    {
        try
        {
            var (sheet, info) = await workbook.OpenSheetReaderAsync(options, useAsync, ct).ConfigureAwait(false);
            return new(ownsWorkbook ? workbook : null, sheet, info);
        }
        catch
        {
            if (ownsWorkbook) workbook.Dispose();
            throw;
        }
    }

    /// <summary>The batches, read as they are enumerated (dispose each batch to free its native memory promptly).</summary>
    public IEnumerable<RecordBatch> ReadBatches() => _sheet.ReadBatches();

    /// <inheritdoc cref="ReadBatches"/>
    public IAsyncEnumerable<RecordBatch> ReadBatchesAsync(CancellationToken cancellationToken = default) =>
        _sheet.ReadBatchesAsync(cancellationToken);

    /// <inheritdoc />
    public void Dispose()
    {
        _sheet.Dispose();
        _ownedWorkbook?.Dispose();
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
