using Kaminari.Seseragi.Files.Excel.Packaging;
using Kaminari.Seseragi.Files.Excel.Reading;
using Kaminari.Seseragi.Files.Excel.Xlsb;
using Kaminari.Seseragi.Files.Excel.Xlsx;

namespace Kaminari.Seseragi.Files.Excel;

/// <summary>
/// An open workbook: its format, sheets, tables and date system, and readers for its sheets. The format is detected
/// from the content, not the file name. Opening reads the workbook's directory parts (not the sheets); a stream that
/// cannot seek is first copied into native memory, since a zip archive's directory sits at its end.
/// </summary>
public sealed class ExcelWorkbook : IDisposable, IAsyncDisposable
{
    private readonly WorkbookSource _source;
    private bool _disposed;

    private ExcelWorkbook(WorkbookSource source) => _source = source;

    /// <summary>The container format.</summary>
    public ExcelFormat Format => _source.Format;

    /// <summary>Whether dates count from 1904-01-01 (the Mac date system) instead of 1900-01-01.</summary>
    public bool Date1904 => _source.Date1904;

    /// <summary>The sheets in tab order, hidden ones and chart sheets included (see <see cref="ExcelSheetInfo.Kind"/>).</summary>
    public IReadOnlyList<ExcelSheetInfo> Sheets => _source.Sheets;

    /// <summary>The tables defined on the worksheets (read on first access).</summary>
    public IReadOnlyList<ExcelTableInfo> Tables => _source.Tables;

    /// <summary>Opens a workbook file.</summary>
    public static ExcelWorkbook Open(string path) =>
        OpenCoreAsync(OpenFile(path), false, false, default).AsTask().GetAwaiter().GetResult();

    /// <summary>Opens a workbook from a stream (left open with <paramref name="leaveOpen"/>).</summary>
    public static ExcelWorkbook Open(Stream stream, bool leaveOpen = false) =>
        OpenCoreAsync(stream, leaveOpen, false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="Open(string)"/>
    public static async Task<ExcelWorkbook> OpenAsync(string path, CancellationToken cancellationToken = default) =>
        await OpenCoreAsync(OpenFile(path), false, true, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc cref="Open(Stream, bool)"/>
    public static async Task<ExcelWorkbook> OpenAsync(Stream stream, bool leaveOpen = false, CancellationToken cancellationToken = default) =>
        await OpenCoreAsync(stream, leaveOpen, true, cancellationToken).ConfigureAwait(false);

    internal static FileStream OpenFile(string path) =>
        new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.RandomAccess);

    internal static async ValueTask<ExcelWorkbook> OpenCoreAsync(Stream stream, bool leaveOpen, bool useAsync, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(stream);
        try
        {
            return new(await DetectAsync(stream, leaveOpen, useAsync, ct).ConfigureAwait(false));
        }
        catch when (!leaveOpen)
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    // Zip containers are told apart by their parts; OLE2 compound files (xls, encrypted workbooks) are not read yet.
    private static async ValueTask<WorkbookSource> DetectAsync(Stream stream, bool leaveOpen, bool useAsync, CancellationToken ct)
    {
        if (stream.CanSeek)
        {
            Span<byte> magic = stackalloc byte[4];
            long start = stream.Position;
            int n = stream.ReadAtLeast(magic, 4, throwOnEndOfStream: false);
            stream.Position = start;
            if (n == 4 && magic.SequenceEqual((ReadOnlySpan<byte>)[0xD0, 0xCF, 0x11, 0xE0]))
                throw new ExcelFormatException("The workbook is an OLE2 compound file (an .xls workbook or an encrypted one), which is not supported yet.");
            if (n < 4 || !magic.SequenceEqual("PK\u0003\u0004"u8))
                throw new ExcelFormatException("The data is not an Excel workbook (no zip or OLE2 signature).");
        }
        var package = useAsync
            ? await OpcPackage.OpenAsync(stream, leaveOpen, ct).ConfigureAwait(false)
            : OpcPackage.Open(stream, leaveOpen);
        try
        {
            if (XlsxWorkbook.FindWorkbookPart(package) is { } part && package.HasPart(part))
            {
                return part.EndsWith(".bin", StringComparison.OrdinalIgnoreCase)
                    ? XlsbWorkbook.Open(package, part)
                    : XlsxWorkbook.Open(package, part);
            }
            if (package.HasPart("content.xml") && package.HasPart("mimetype"))
                throw new ExcelFormatException("The workbook is an OpenDocument spreadsheet (.ods), which is not supported yet.");
            throw new ExcelFormatException("The zip archive holds no Excel workbook.");
        }
        catch
        {
            package.Dispose();
            throw;
        }
    }

    /// <summary>
    /// A reader over one sheet or table of this workbook (chosen by <see cref="ExcelReadOptions.SheetName"/>,
    /// <see cref="ExcelReadOptions.SheetIndex"/> or <see cref="ExcelReadOptions.TableName"/>). The workbook must stay
    /// open while the reader is in use; <see cref="ExcelReadOptions.LeaveOpen"/> does not apply.
    /// </summary>
    public ExcelReader OpenSheet(ExcelReadOptions? options = null) =>
        ExcelReader.OpenCoreAsync(this, options ?? new(), ownsWorkbook: false, useAsync: false, default).AsTask().GetAwaiter().GetResult();

    /// <inheritdoc cref="OpenSheet"/>
    public async Task<ExcelReader> OpenSheetAsync(ExcelReadOptions? options = null, CancellationToken cancellationToken = default) =>
        await ExcelReader.OpenCoreAsync(this, options ?? new(), ownsWorkbook: false, useAsync: true, cancellationToken).ConfigureAwait(false);

    internal async ValueTask<SheetReader> OpenSheetReaderAsync(ExcelReadOptions options, bool useAsync, CancellationToken ct)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        options.Validate();
        int index;
        ReadRange? range = null;
        if (options.TableName is { } tableName)
        {
            var table = Tables.FirstOrDefault(t => string.Equals(t.Name, tableName, StringComparison.OrdinalIgnoreCase))
                ?? throw new ArgumentException($"The workbook has no table named '{tableName}'.", nameof(options));
            if (!ExcelCellReference.TryParseRange(table.Range, out var first, out var last))
                throw new ExcelFormatException($"Table '{table.Name}' has an invalid range '{table.Range}'.");
            index = Sheets.First(s => s.Name == table.SheetName).Index;
            range = new(first.Row, first.Column, last.Row, last.Column, table.HeaderRowCount, table.TotalsRowCount);
        }
        else if (options.SheetName is { } name)
        {
            index = Sheets.FirstOrDefault(s => string.Equals(s.Name, name, StringComparison.OrdinalIgnoreCase))?.Index
                ?? throw new ArgumentException($"The workbook has no sheet named '{name}' (sheets: {string.Join(", ", Sheets.Select(s => s.Name))}).", nameof(options));
        }
        else
        {
            index = options.SheetIndex;
            if ((uint)index >= (uint)Sheets.Count)
                throw new ArgumentOutOfRangeException(nameof(options), $"Sheet index {index} is out of range; the workbook has {Sheets.Count} sheets.");
        }
        if (Sheets[index].Kind != ExcelSheetKind.Worksheet)
            throw new ArgumentException(Sheets[index].Kind == ExcelSheetKind.Chartsheet
                ? $"Sheet '{Sheets[index].Name}' (index {index}) is a chart sheet: it holds a chart, no cells to read."
                : $"Sheet '{Sheets[index].Name}' (index {index}) is not a worksheet (a dialog or macro sheet): it holds no cells to read.", nameof(options));
        var source = await _source.OpenSheetAsync(index, useAsync, ct).ConfigureAwait(false);
        return await SheetReader.OpenAsync(source, options, Sheets[index].Name, Date1904, range, useAsync, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _source.Dispose();
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
