namespace Kaminari.Sazanami.Text;

/// <summary>
/// Temporal values written as ISO 8601 text, as JSON writers write them (dataframe engines, Spark, .NET), read from UTF-8 exactly
/// to the nanosecond: the caller decides what a unit coarser than the text means (a fraction it cannot hold is not
/// silently cut here). Years have four digits.
/// </summary>
internal static partial class IsoTemporal
{
    private const long NanosPerSecond = 1_000_000_000;
    private const long NanosPerDay = 86_400 * NanosPerSecond;

    /// <summary><c>yyyy-MM-dd</c> as days since 1970-01-01.</summary>
    public static bool TryParseDate(ReadOnlySpan<byte> text, out int days)
    {
        days = 0;
        if (text.Length != 10 || !Date(text, out long d)) return false;
        days = (int)d;
        return true;
    }

    /// <summary>
    /// A date and a time: <c>yyyy-MM-dd</c>, then <c>T</c> or a space and <c>HH:mm[:ss[.f]]</c> (up to nine fraction
    /// digits), then optionally <c>Z</c> or an offset (<c>±hh</c>, <c>±hhmm</c>, <c>±hh:mm</c>); a date alone is
    /// midnight. The value is nanoseconds since the epoch, of the instant (UTC) when an offset is given
    /// (<paramref name="zoned"/>), else of the wall-clock time read as UTC.
    /// </summary>
    public static bool TryParseDatetime(ReadOnlySpan<byte> text, out Int128 nanos, out bool zoned)
    {
        nanos = 0;
        zoned = false;
        if (text.Length < 10 || !Date(text[..10], out long days)) return false;
        if (text.Length == 10)
        {
            nanos = (Int128)days * NanosPerDay;
            return true;
        }
        if (text[10] is not ((byte)'T' or (byte)'t' or (byte)' ')) return false;
        ReadOnlySpan<byte> rest = text[11..];
        long offsetSeconds = 0;
        if (rest.Length > 0 && rest[^1] is (byte)'Z' or (byte)'z')
        {
            zoned = true;
            rest = rest[..^1];
        }
        else
        {
            int sign = rest.IndexOfAny((byte)'+', (byte)'-');
            if (sign >= 0)
            {
                if (!Offset(rest[sign..], out offsetSeconds)) return false;
                zoned = true;
                rest = rest[..sign];
            }
        }
        if (!TryParseTime(rest, out long ofDay)) return false;
        nanos = (Int128)days * NanosPerDay + ofDay - (Int128)offsetSeconds * NanosPerSecond;
        return true;
    }

    /// <summary><c>HH:mm</c> or <c>HH:mm:ss[.f]</c> (up to nine fraction digits) as nanoseconds since midnight.</summary>
    public static bool TryParseTime(ReadOnlySpan<byte> text, out long nanos)
    {
        nanos = 0;
        if (text.Length < 5 || text[2] != ':' || !Digits(text[..2], out int hour) || !Digits(text.Slice(3, 2), out int minute)) return false;
        int second = 0;
        long fraction = 0;
        if (text.Length > 5)
        {
            if (text.Length < 8 || text[5] != ':' || !Digits(text.Slice(6, 2), out second)) return false;
            if (text.Length > 8 && !Fraction(text[8..], out fraction)) return false;
        }
        if (hour > 23 || minute > 59 || second > 59) return false;
        nanos = ((hour * 60L + minute) * 60 + second) * NanosPerSecond + fraction;
        return true;
    }

    /// <summary>
    /// An ISO 8601 duration, <c>[-]P[nW][nD][T[nH][nM][n[.f]S]]</c> with at least one part (the fraction on the seconds
    /// only, up to nine digits), as nanoseconds; weeks are 7 days, days 24 hours (as dataframe engines write durations).
    /// </summary>
    public static bool TryParseDuration(ReadOnlySpan<byte> text, out Int128 nanos)
    {
        nanos = 0;
        bool negative = text.Length > 0 && text[0] == '-';
        if (negative) text = text[1..];
        if (text.Length < 2 || text[0] is not ((byte)'P' or (byte)'p')) return false;
        text = text[1..];
        Int128 total = 0;
        bool inTime = false, any = false;
        int order = 0; // the parts come in order: W D | H M S
        while (!text.IsEmpty)
        {
            if (text[0] is (byte)'T' or (byte)'t')
            {
                if (inTime) return false;
                inTime = true;
                text = text[1..];
                if (text.IsEmpty) return false;
                continue;
            }
            int digits = 0;
            while (digits < text.Length && (uint)(text[digits] - '0') <= 9) digits++;
            if (digits == 0 || digits > 18) return false;
            long value = long.Parse(text[..digits], provider: System.Globalization.CultureInfo.InvariantCulture);
            text = text[digits..];
            long fraction = 0;
            if (!text.IsEmpty && text[0] == '.')
            {
                int end = 1;
                while (end < text.Length && (uint)(text[end] - '0') <= 9) end++;
                if (!Fraction(text[..end], out fraction)) return false;
                text = text[end..];
                if (text.IsEmpty || text[0] is not ((byte)'S' or (byte)'s') || !inTime) return false;
            }
            if (text.IsEmpty) return false;
            (int rank, long unit) = (inTime, text[0] | 0x20) switch
            {
                (false, 'w') => (1, 7 * NanosPerDay),
                (false, 'd') => (2, NanosPerDay),
                (true, 'h') => (3, 3600 * NanosPerSecond),
                (true, 'm') => (4, 60 * NanosPerSecond),
                (true, 's') => (5, NanosPerSecond),
                _ => (0, 0L),
            };
            if (rank <= order) return false;
            order = rank;
            total += (Int128)value * unit + fraction;
            any = true;
            text = text[1..];
        }
        if (!any) return false;
        nanos = negative ? -total : total;
        return true;
    }

    // yyyy-MM-dd (exactly ten bytes) as days since the epoch.
    private static bool Date(ReadOnlySpan<byte> text, out long days)
    {
        days = 0;
        if (text.Length != 10 || text[4] != '-' || text[7] != '-'
            || !Digits(text[..4], out int year) || !Digits(text.Slice(5, 2), out int month) || !Digits(text.Slice(8, 2), out int day)) return false;
        if (month is < 1 or > 12 || day < 1 || day > DateTime.DaysInMonth(Math.Max(year, 1), month) || year == 0) return false;
        // Howard Hinnant's days_from_civil.
        long y = month <= 2 ? year - 1 : year;
        long era = (y >= 0 ? y : y - 399) / 400;
        long yoe = y - era * 400;
        long doy = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
        long doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        days = era * 146_097 + doe - 719_468;
        return true;
    }

    // ±hh, ±hhmm or ±hh:mm as seconds east of UTC.
    private static bool Offset(ReadOnlySpan<byte> text, out long seconds)
    {
        seconds = 0;
        int sign = text[0] == '-' ? -1 : 1;
        text = text[1..];
        int hours, minutes = 0;
        switch (text.Length)
        {
            case 2 when Digits(text, out hours): break;
            case 4 when Digits(text[..2], out hours) && Digits(text[2..], out minutes): break;
            case 5 when text[2] == ':' && Digits(text[..2], out hours) && Digits(text[3..], out minutes): break;
            default: return false;
        }
        if (hours > 23 || minutes > 59) return false;
        seconds = sign * (hours * 3600L + minutes * 60L);
        return true;
    }

    // ".f" with one to nine digits as nanoseconds.
    private static bool Fraction(ReadOnlySpan<byte> text, out long nanos)
    {
        nanos = 0;
        if (text.Length < 2 || text.Length > 10 || text[0] != '.') return false;
        for (int i = 1; i < 10; i++)
        {
            int d = 0;
            if (i < text.Length)
            {
                d = text[i] - '0';
                if ((uint)d > 9) return false;
            }
            nanos = nanos * 10 + d;
        }
        return true;
    }

    private static bool Digits(ReadOnlySpan<byte> text, out int value)
    {
        value = 0;
        foreach (byte b in text)
        {
            int d = b - '0';
            if ((uint)d > 9) return false;
            value = value * 10 + d;
        }
        return !text.IsEmpty;
    }
}
