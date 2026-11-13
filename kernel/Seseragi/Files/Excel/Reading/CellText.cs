using System.Buffers.Text;
using System.Globalization;

namespace Kaminari.Seseragi.Files.Excel.Reading;

/// <summary>
/// Cells as text, for string columns and header names: integral numbers without a fraction, other numbers in their
/// shortest round-trip form, booleans as <c>true</c>/<c>false</c>, temporal values in ISO 8601 with a space between
/// date and time (<c>2024-05-06 07:08:09.123456</c>, fraction only when not zero), durations as <c>[-]h:mm:ss</c>.
/// </summary>
internal static class CellText
{
    /// <summary>The longest text <see cref="TryFormat"/> writes for a non-string cell.</summary>
    public const int MaxLength = 64;

    /// <summary>Writes a non-string cell as UTF-8; false for empty and error cells (no text).</summary>
    public static bool TryFormat(in Cell cell, bool date1904, Span<byte> destination, out int written)
    {
        written = 0;
        switch (cell.Type)
        {
            case CellType.Boolean:
                return (cell.Number != 0 ? "true"u8 : "false"u8).TryCopyTo(destination) && Set(cell.Number != 0 ? 4 : 5, out written);
            case CellType.Temporal:
                return FormatTemporal(cell.Kind, cell.Micros, destination, out written);
            case CellType.Number:
                if (cell.Kind is ExcelValueKind.Date or ExcelValueKind.DateTime || (cell.Kind == ExcelValueKind.Time && cell.Number >= 1))
                {
                    if (ExcelDates.TrySplit(cell.Number, date1904, out long days, out long ofDay))
                        return FormatTemporal(ofDay == 0 && cell.Kind == ExcelValueKind.Date ? ExcelValueKind.Date : ExcelValueKind.DateTime,
                            days * ExcelDates.MicrosPerDay + ofDay, destination, out written);
                }
                else if (cell.Kind == ExcelValueKind.Time && cell.Number is >= 0 and < 1)
                {
                    return FormatTemporal(ExcelValueKind.Time, (long)Math.Round(cell.Number * ExcelDates.MicrosPerDay), destination, out written);
                }
                else if (cell.Kind == ExcelValueKind.Duration && ExcelDates.TryToDurationMicros(cell.Number, out long d))
                {
                    return FormatDuration(d, destination, out written);
                }
                return FormatNumber(cell.Number, destination, out written);
            default:
                return false;
        }
    }

    /// <summary>A number: integral values below 10^15 as integers, others in shortest round-trip form.</summary>
    public static bool FormatNumber(double value, Span<byte> destination, out int written)
    {
        if (value == Math.Floor(value) && Math.Abs(value) < 1e15)
            return Utf8Formatter.TryFormat((long)value, destination, out written);
        return value.TryFormat(destination, out written, default, CultureInfo.InvariantCulture);
    }

    private static bool FormatTemporal(ExcelValueKind kind, long micros, Span<byte> destination, out int written)
    {
        if (kind == ExcelValueKind.Time)
        {
            written = WriteTime(micros, destination);
            return true;
        }
        var (days, ofDay) = ExcelDates.SplitMicros(micros);
        var date = DateOnly.FromDayNumber((int)(days + 719_162)); // 0001-01-01 to 1970-01-01
        if (!date.TryFormat(destination, out written, "yyyy-MM-dd", CultureInfo.InvariantCulture)) return false;
        if (kind == ExcelValueKind.Date && ofDay == 0) return true;
        destination[written++] = (byte)' ';
        written += WriteTime(ofDay, destination[written..]);
        return true;
    }

    private static bool FormatDuration(long micros, Span<byte> destination, out int written)
    {
        written = 0;
        if (micros < 0)
        {
            destination[written++] = (byte)'-';
            micros = -micros;
        }
        long hours = micros / 3_600_000_000;
        Utf8Formatter.TryFormat(hours, destination[written..], out int n);
        written += n;
        long rest = micros - hours * 3_600_000_000;
        // ":mm:ss[.ffffff]" from the time-of-day writer (skip its hour digits).
        Span<byte> tmp = stackalloc byte[24];
        int t = WriteTime(rest, tmp);
        tmp[2..t].CopyTo(destination[written..]);
        written += t - 2;
        return true;
    }

    // hh:mm:ss[.ffffff] (trailing zeros of the fraction trimmed).
    private static int WriteTime(long micros, Span<byte> d)
    {
        long seconds = micros / 1_000_000;
        int fraction = (int)(micros % 1_000_000);
        int h = (int)(seconds / 3600), m = (int)(seconds / 60 % 60), s = (int)(seconds % 60);
        d[0] = (byte)('0' + h / 10);
        d[1] = (byte)('0' + h % 10);
        d[2] = (byte)':';
        d[3] = (byte)('0' + m / 10);
        d[4] = (byte)('0' + m % 10);
        d[5] = (byte)':';
        d[6] = (byte)('0' + s / 10);
        d[7] = (byte)('0' + s % 10);
        if (fraction == 0) return 8;
        d[8] = (byte)'.';
        int len = 6;
        while (fraction % 10 == 0)
        {
            fraction /= 10;
            len--;
        }
        for (int i = len; i > 0; i--)
        {
            d[8 + i] = (byte)('0' + fraction % 10);
            fraction /= 10;
        }
        return 9 + len;
    }

    private static bool Set(int n, out int written)
    {
        written = n;
        return true;
    }
}
