namespace Kaminari.Seseragi.Files.Excel;

/// <summary>The container format of a workbook, detected from its content (never from the file extension).</summary>
public enum ExcelFormat
{
    /// <summary>Office Open XML workbook (.xlsx, .xlsm, .xltx, .xltm), transitional or strict.</summary>
    Xlsx,

    /// <summary>Excel binary workbook (.xlsb): the same package with BIFF12 record parts.</summary>
    Xlsb,
}

/// <summary>Whether a sheet shows in Excel's sheet tabs.</summary>
public enum ExcelSheetVisibility
{
    /// <summary>Shown.</summary>
    Visible,

    /// <summary>Hidden; the user can unhide it.</summary>
    Hidden,

    /// <summary>Hidden; only code can unhide it.</summary>
    VeryHidden,
}

/// <summary>What a sheet holds.</summary>
public enum ExcelSheetKind
{
    /// <summary>A worksheet: cells.</summary>
    Worksheet,

    /// <summary>A chart sheet: one chart, no cells.</summary>
    Chartsheet,

    /// <summary>A dialog sheet or an Excel 4 macro sheet: nothing this package reads.</summary>
    Other,
}

/// <summary>A sheet of a workbook.</summary>
/// <param name="Name">The sheet name.</param>
/// <param name="Index">Zero-based position among the workbook's sheet tabs, hidden sheets and chart sheets included.</param>
/// <param name="Visibility">Whether the sheet is shown.</param>
/// <param name="Kind">Whether it is a worksheet (only worksheets hold cells to read).</param>
public sealed record ExcelSheetInfo(string Name, int Index, ExcelSheetVisibility Visibility, ExcelSheetKind Kind = ExcelSheetKind.Worksheet);

/// <summary>A table (list object) defined on a sheet.</summary>
/// <param name="Name">The table's display name (what Excel shows and formulas use).</param>
/// <param name="SheetName">The sheet holding the table.</param>
/// <param name="Range">The table's cell range in A1 notation, header and totals rows included (for example <c>B2:F100</c>).</param>
/// <param name="HeaderRowCount">Header rows at the top of the range (0 or 1).</param>
/// <param name="TotalsRowCount">Totals rows at the bottom of the range (0 or 1).</param>
public sealed record ExcelTableInfo(string Name, string SheetName, string Range, int HeaderRowCount, int TotalsRowCount);

/// <summary>A workbook that is malformed, encrypted, or in a format this package cannot read.</summary>
public sealed class ExcelFormatException(string message, Exception? inner = null) : FormatException(message, inner);

/// <summary>
/// A cell value that does not fit its column: the type inferred from the first rows, a schema override, or the
/// columns known once the first rows were read. Read with a larger inference window or a schema override.
/// </summary>
public sealed class ExcelValueException(string message, string sheet, string cell) : Exception(message)
{
    /// <summary>The sheet holding the cell.</summary>
    public string Sheet { get; } = sheet;

    /// <summary>The cell address in A1 notation.</summary>
    public string Cell { get; } = cell;
}
