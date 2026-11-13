namespace Kaminari.Seseragi.Files.Excel.Reading;

/// <summary>A workbook in one of the container formats: its sheets and tables, and a row source per sheet.</summary>
internal abstract class WorkbookSource : IDisposable
{
    public abstract ExcelFormat Format { get; }

    public abstract bool Date1904 { get; }

    public abstract IReadOnlyList<ExcelSheetInfo> Sheets { get; }

    public abstract IReadOnlyList<ExcelTableInfo> Tables { get; }

    /// <summary>
    /// Opens a sheet by name (case-insensitive) or, without a name, by index, for formats whose sheet list costs a
    /// pass over the whole workbook: only as much is read as finding that sheet takes. Null when the format does not
    /// do this, or has no such sheet (the caller then resolves through <see cref="Sheets"/>, which reports what is
    /// wrong). The sheet must be a worksheet.
    /// </summary>
    public virtual ValueTask<(ExcelSheetInfo Sheet, SheetSource Source)?> TryOpenSheetDirectAsync(string? name, int index, bool useAsync, CancellationToken cancellationToken) =>
        ValueTask.FromResult<(ExcelSheetInfo, SheetSource)?>(null);

    /// <summary>Opens the rows of a sheet (by its index in <see cref="Sheets"/>).</summary>
    public abstract ValueTask<SheetSource> OpenSheetAsync(int index, bool useAsync, CancellationToken cancellationToken);

    public abstract void Dispose();
}
