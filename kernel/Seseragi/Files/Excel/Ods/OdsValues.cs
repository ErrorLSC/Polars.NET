namespace Kaminari.Seseragi.Files.Excel.Ods;

/// <summary>
/// The typed values of OpenDocument cells ([ODF 1.3] part 3, 19.389): <c>office:date-value</c> as an xsd:date or
/// xsd:dateTime and <c>office:time-value</c> as an xsd:duration, to microseconds (finer digits rounded to the
/// nearest). A time zone suffix on a date is ignored: the clock time is kept, as Excel keeps no zones.
/// </summary>
internal static class OdsValues
{
    private const long MicrosPerSecond = 1_000_000;
    private const long MicrosPerDay = 86_400 * MicrosPerSecond;

    /// <summary>
    /// <c>YYYY-MM-DD</c> with an optional <c>Thh:mm:ss[.f…]</c> and zone (<c>Z</c>, <c>±hh:mm</c>); years 1 to 9999.
    /// Gives Unix microseconds and whether a time of day was written.
    /// </summary>
    public static bool TryParseDate(ReadOnlySpan<byte> s, out long micros, out bool hasTime)
    {
        micros = 0;
        hasTime = false;
        s = Xml.XmlText.Trim(s);
        if (s.Length < 10 || s[4] != '-' || s[7] != '-') return false;
        if (!Digits(s[..4], out int year) || !Digits(s.Slice(5, 2), out int month) || !Digits(s.Slice(8, 2), out int day)) return false;
        if (year < 1 || month is < 1 or > 12 || day < 1 || day > DateTime.DaysInMonth(year, month)) return false;
        long days = new DateOnly(year, month, day).DayNumber - 719_162L;
        var rest = s[10..];
        long ofDay = 0;
        if (!rest.IsEmpty && rest[0] == 'T')
        {
            hasTime = true;
            rest = rest[1..];
            if (rest.Length < 8 || rest[2] != ':' || rest[5] != ':') return false;
            if (!Digits(rest[..2], out int h) || !Digits(rest.Slice(3, 2), out int m) || !Digits(rest.Slice(6, 2), out int sec)) return false;
            if (h > 24 || m > 59 || sec > 59 || h == 24 && (m != 0 || sec != 0)) return false;
            rest = rest[8..];
            long fraction = 0;
            if (!rest.IsEmpty && rest[0] == '.')
            {
                int n = 1;
                while (n < rest.Length && (uint)(rest[n] - '0') <= 9) n++;
                if (n == 1) return false;
                fraction = FractionMicros(rest[1..n]);
                rest = rest[n..];
            }
            ofDay = (h * 3600L + m * 60 + sec) * MicrosPerSecond + fraction;
        }
        if (!rest.IsEmpty && !IsZone(rest)) return false;
        micros = days * MicrosPerDay + ofDay;
        return true;

        static bool IsZone(ReadOnlySpan<byte> z) =>
            z.SequenceEqual("Z"u8) || z.Length == 6 && z[0] is (byte)'+' or (byte)'-' && z[3] == ':'
                && Digits(z.Slice(1, 2), out _) && Digits(z.Slice(4, 2), out _);
    }

    /// <summary>
    /// An xsd:duration <c>[-]P[nD][T[nH][nM][n[.f…]S]]</c> in microseconds. Years and months (of no fixed length) are
    /// refused unless zero.
    /// </summary>
    public static bool TryParseDuration(ReadOnlySpan<byte> s, out long micros)
    {
        micros = 0;
        s = Xml.XmlText.Trim(s);
        bool negative = !s.IsEmpty && s[0] == '-';
        if (negative) s = s[1..];
        if (s.IsEmpty || s[0] != 'P') return false;
        s = s[1..];
        bool inTime = false, any = false;
        decimal total = 0;
        while (!s.IsEmpty)
        {
            if (s[0] == 'T')
            {
                if (inTime) return false;
                inTime = true;
                s = s[1..];
                continue;
            }
            int n = 0;
            while (n < s.Length && ((uint)(s[n] - '0') <= 9 || s[n] == '.')) n++;
            if (n == 0 || n >= s.Length || n > 28) return false;
            if (!decimal.TryParse(System.Text.Encoding.ASCII.GetString(s[..n]), System.Globalization.NumberStyles.AllowDecimalPoint,
                    System.Globalization.CultureInfo.InvariantCulture, out decimal value))
                return false;
            byte unit = s[n];
            bool fractional = s[..n].Contains((byte)'.');
            decimal perUnit = (inTime, unit) switch
            {
                (false, (byte)'Y') or (false, (byte)'M') when value == 0 => 0,
                (false, (byte)'D') => MicrosPerDay,
                (true, (byte)'H') => 3600 * MicrosPerSecond,
                (true, (byte)'M') => 60 * MicrosPerSecond,
                (true, (byte)'S') => MicrosPerSecond,
                _ => -1,
            };
            if (perUnit < 0 || fractional && unit != 'S') return false;
            total += value * perUnit;
            if (total > long.MaxValue / 2) return false;
            any = true;
            s = s[(n + 1)..];
        }
        if (!any) return false;
        long result = (long)decimal.Round(total, MidpointRounding.AwayFromZero);
        micros = negative ? -result : result;
        return true;
    }

    private static long FractionMicros(ReadOnlySpan<byte> digits)
    {
        long value = 0;
        for (int i = 0; i < 6; i++) value = value * 10 + (i < digits.Length ? digits[i] - '0' : 0);
        if (digits.Length > 6 && digits[6] >= '5') value++; // nearest microsecond
        return value;
    }

    private static bool Digits(ReadOnlySpan<byte> s, out int value)
    {
        value = 0;
        foreach (byte b in s)
        {
            uint d = (uint)(b - '0');
            if (d > 9) return false;
            value = value * 10 + (int)d;
        }
        return !s.IsEmpty;
    }
}
