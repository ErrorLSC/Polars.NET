namespace Kaminari.Sazanami.Text;

/// <summary>
/// Temporal values written as ISO 8601 text, as the reference dataframe engine writes them to JSON (chrono's formats):
/// dates as <c>yyyy-MM-dd</c> (years outside 0000–9999 signed, at least four digits), times of day as
/// <c>HH:mm:ss</c>, fractions of a second as the shortest of 3, 6 or 9 digits that holds them (none when zero), UTC
/// offsets as <c>±HH:MM</c> (rounded to the minute).
/// </summary>
internal static partial class IsoTemporal
{
    /// <summary>The longest text of a date (a sign and 18 digits of year).</summary>
    public const int MaxDateBytes = 26;

    /// <summary>The year, month and day of <paramref name="days"/> since 1970-01-01 (proleptic Gregorian, any year).</summary>
    public static (long Year, int Month, int Day) CivilFromDays(long days)
    {
        // Howard Hinnant's civil_from_days.
        long z = days + 719_468;
        long era = (z >= 0 ? z : z - 146_096) / 146_097;
        long doe = z - era * 146_097;
        long yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        long doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        long mp = (5 * doy + 2) / 153;
        int day = (int)(doy - (153 * mp + 2) / 5 + 1);
        int month = (int)(mp < 10 ? mp + 3 : mp - 9);
        return (yoe + era * 400 + (month <= 2 ? 1 : 0), month, day);
    }

    /// <summary>Writes <paramref name="days"/> since 1970-01-01 as <c>yyyy-MM-dd</c>; returns the bytes written.</summary>
    public static int FormatDate(long days, Span<byte> destination)
    {
        var (year, month, day) = CivilFromDays(days);
        int at = 0;
        if (year is >= 0 and <= 9999) at = Digits(year, 4, destination);
        else
        {
            destination[0] = year < 0 ? (byte)'-' : (byte)'+';
            ulong magnitude = year < 0 ? (ulong)-(year + 1) + 1 : (ulong)year;
            at = 1 + Digits(magnitude, Math.Max(4, CountDigits(magnitude)), destination[1..]);
        }
        destination[at] = (byte)'-';
        Two(month, destination[(at + 1)..]);
        destination[at + 3] = (byte)'-';
        Two(day, destination[(at + 4)..]);
        return at + 6;
    }

    /// <summary>Writes a time of day (<c>HH:mm:ss</c> and a fraction); returns the bytes written.</summary>
    public static int FormatTime(int secondOfDay, int nanos, Span<byte> destination)
    {
        Two(secondOfDay / 3600, destination);
        destination[2] = (byte)':';
        Two(secondOfDay / 60 % 60, destination[3..]);
        destination[5] = (byte)':';
        Two(secondOfDay % 60, destination[6..]);
        return 8 + FormatFraction(nanos, destination[8..]);
    }

    /// <summary>Writes <c>.fff</c>, <c>.ffffff</c> or <c>.fffffffff</c>, the shortest that holds the nanoseconds (none when zero).</summary>
    public static int FormatFraction(int nanos, Span<byte> destination)
    {
        if (nanos == 0) return 0;
        destination[0] = (byte)'.';
        if (nanos % 1_000_000 == 0) return 1 + Digits((ulong)(nanos / 1_000_000), 3, destination[1..]);
        if (nanos % 1_000 == 0) return 1 + Digits((ulong)(nanos / 1_000), 6, destination[1..]);
        return 1 + Digits((ulong)nanos, 9, destination[1..]);
    }

    /// <summary>Writes a UTC offset as <c>±HH:MM</c>, rounded to the nearest minute; returns the bytes written (6).</summary>
    public static int FormatOffset(int seconds, Span<byte> destination)
    {
        destination[0] = seconds < 0 ? (byte)'-' : (byte)'+';
        int minutes = (Math.Abs(seconds) + 30) / 60;
        Two(minutes / 60, destination[1..]);
        destination[3] = (byte)':';
        Two(minutes % 60, destination[4..]);
        return 6;
    }

    private static void Two(int value, Span<byte> destination)
    {
        destination[0] = (byte)('0' + value / 10);
        destination[1] = (byte)('0' + value % 10);
    }

    private static int Digits(long value, int width, Span<byte> destination) => Digits((ulong)value, width, destination);

    // value in exactly width digits (zero-padded).
    private static int Digits(ulong value, int width, Span<byte> destination)
    {
        for (int i = width - 1; i >= 0; i--)
        {
            destination[i] = (byte)('0' + value % 10);
            value /= 10;
        }
        return width;
    }

    private static int CountDigits(ulong value)
    {
        int n = 1;
        while (value >= 10)
        {
            value /= 10;
            n++;
        }
        return n;
    }
}
