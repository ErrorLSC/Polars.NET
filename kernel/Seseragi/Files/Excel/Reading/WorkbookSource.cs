namespace Kaminari.Seseragi.Files.Excel.Reading;

/// <summary>A workbook in one of the container formats: its sheets and tables, and a row source per sheet.</summary>
internal abstract class WorkbookSource : IDisposable
{
    public abstract ExcelFormat Format { get; }

    public abstract bool Date1904 { get; }

    public abstract IReadOnlyList<ExcelSheetInfo> Sheets { get; }

    public abstract IReadOnlyList<ExcelTableInfo> Tables { get; }

    /// <summary>Opens the rows of a sheet (by its index in <see cref="Sheets"/>).</summary>
    public abstract ValueTask<SheetSource> OpenSheetAsync(int index, bool useAsync, CancellationToken cancellationToken);

    public abstract void Dispose();
}
