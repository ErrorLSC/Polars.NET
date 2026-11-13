using System.Text;

namespace Kaminari.Seseragi.Files.Excel.Writing;

/// <summary>
/// Where the cell encoders write: one cell per call at the current row and column, in the cell form of one workbook
/// format (<see cref="XlsxCellSink"/> for xlsx). The encoders know Arrow values and Excel's rules for them; a sink
/// knows only how a format stores a number, a boolean, an error or a string.
/// </summary>
internal abstract class CellSink
{
    /// <summary>The longest string a cell holds, in UTF-16 code units.</summary>
    public const int MaxStringLength = 32_767;

    /// <summary>The current sheet row (1-based).</summary>
    public int RowNumber { get; private set; }

    /// <summary>The current column (0-based).</summary>
    public int ColumnIndex { get; private set; }

    /// <summary>The current column's name, for error messages.</summary>
    public string ColumnName { get; private set; } = "";

    /// <summary>Starts sheet row <paramref name="rowNumber"/> (1-based).</summary>
    public void BeginRow(int rowNumber)
    {
        RowNumber = rowNumber;
        StartRow();
    }

    /// <summary>Moves to a column of the current row.</summary>
    public void SetColumn(int index, string name)
    {
        ColumnIndex = index;
        ColumnName = name;
    }

    protected abstract void StartRow();

    /// <summary>Ends the current row.</summary>
    public abstract void EndRow();

    /// <summary>A finite number, with a cell format (0: the default).</summary>
    public abstract void Number(double value, byte style = 0);

    /// <summary>A whole number exact as a double (|value| ≤ 2^53), with a cell format.</summary>
    public abstract void Integer(long value, byte style = 0);

    /// <summary>A whole number exact as a double (≤ 2^53).</summary>
    public abstract void UnsignedInteger(ulong value);

    /// <summary>A finite single-precision number, kept in its shortest single-precision form.</summary>
    public abstract void Single(float value);

    public abstract void Boolean(bool value);

    /// <summary>The #NUM! error (NaN and infinities).</summary>
    public abstract void NumError();

    /// <summary>A string (UTF-8).</summary>
    public abstract void String(ReadOnlySpan<byte> utf8);

    /// <summary>Refuses strings longer than a cell holds.</summary>
    protected void CheckLength(ReadOnlySpan<byte> utf8)
    {
        if (utf8.Length > MaxStringLength && Encoding.UTF8.GetCharCount(utf8) is var chars && chars > MaxStringLength)
            throw Refuse($"a string of {chars} characters exceeds Excel's limit of {MaxStringLength}");
    }

    public NotSupportedException Refuse(string what) => new($"Column '{ColumnName}', row {RowNumber}: {what}.");
}
