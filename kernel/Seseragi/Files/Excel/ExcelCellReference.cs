namespace Kaminari.Seseragi.Files.Excel;

/// <summary>A1 cell references: zero-based row and column numbers to and from text.</summary>
internal static class ExcelCellReference
{
    /// <summary>Excel's sheet size.</summary>
    public const int MaxRows = 1_048_576;

    /// <summary>Excel's sheet width (XFD).</summary>
    public const int MaxColumns = 16_384;

    /// <summary>
    /// Parses a cell reference such as <c>B12</c> (or <c>$B$12</c>) into zero-based row and column. A reference
    /// without a row (<c>B</c>) gives row -1.
    /// </summary>
    public static bool TryParse(ReadOnlySpan<byte> text, out int row, out int column)
    {
        row = -1;
        column = -1;
        int i = 0;
        if (i < text.Length && text[i] == (byte)'$') i++;
        int col = 0, letters = 0;
        for (; i < text.Length; i++)
        {
            int c = text[i] | 0x20; // lower case
            if (c is < 'a' or > 'z') break;
            col = col * 26 + (c - 'a' + 1);
            if (++letters > 3) return false;
        }
        if (letters == 0) return false;
        if (i < text.Length && text[i] == (byte)'$') i++;
        long r = 0;
        int digits = 0;
        for (; i < text.Length; i++)
        {
            int d = text[i] - '0';
            if ((uint)d > 9) return false;
            r = r * 10 + d;
            if (++digits > 7) return false;
        }
        column = col - 1;
        row = digits == 0 ? -1 : (int)r - 1;
        return digits == 0 || r >= 1;
    }

    /// <inheritdoc cref="TryParse(ReadOnlySpan{byte}, out int, out int)"/>
    public static bool TryParse(ReadOnlySpan<char> text, out int row, out int column)
    {
        Span<byte> bytes = stackalloc byte[16];
        if (text.Length > bytes.Length)
        {
            row = column = -1;
            return false;
        }
        for (int i = 0; i < text.Length; i++)
        {
            if (text[i] > 0x7F)
            {
                row = column = -1;
                return false;
            }
            bytes[i] = (byte)text[i];
        }
        return TryParse(bytes[..text.Length], out row, out column);
    }

    /// <summary>Parses a range such as <c>A1:C10</c> (a single cell is a one-cell range).</summary>
    public static bool TryParseRange(ReadOnlySpan<char> text, out (int Row, int Column) first, out (int Row, int Column) last)
    {
        first = last = default;
        int colon = text.IndexOf(':');
        var a = colon < 0 ? text : text[..colon];
        var b = colon < 0 ? text : text[(colon + 1)..];
        if (!TryParse(a, out int r1, out int c1) || !TryParse(b, out int r2, out int c2) || r1 < 0 || r2 < 0) return false;
        first = (Math.Min(r1, r2), Math.Min(c1, c2));
        last = (Math.Max(r1, r2), Math.Max(c1, c2));
        return true;
    }

    /// <summary>Writes the column letters of a zero-based column; returns the number of bytes written (1 to 3).</summary>
    public static int WriteColumn(int column, Span<byte> destination)
    {
        Span<byte> tmp = stackalloc byte[3];
        int n = 0;
        int c = column + 1;
        while (c > 0)
        {
            int rem = (c - 1) % 26;
            tmp[n++] = (byte)('A' + rem);
            c = (c - 1) / 26;
        }
        for (int i = 0; i < n; i++) destination[i] = tmp[n - 1 - i];
        return n;
    }

    /// <summary>The column letters of a zero-based column.</summary>
    public static string ColumnName(int column)
    {
        Span<byte> b = stackalloc byte[3];
        int n = WriteColumn(column, b);
        return System.Text.Encoding.ASCII.GetString(b[..n]);
    }

    /// <summary>The A1 reference of a zero-based row and column.</summary>
    public static string Format(int row, int column) => $"{ColumnName(column)}{row + 1}";
}
