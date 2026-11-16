using System.Globalization;

namespace Kaminari.Sazanami.Text;

/// <summary>The kind of value a text holds, as CSV readers infer it.</summary>
internal enum TextKind
{
    Boolean,
    Int64,
    /// <summary>An integer beyond Int64 (an Int128).</summary>
    Int128,
    Float64,
    Date,
    /// <summary>A date and time without an offset.</summary>
    Datetime,
    /// <summary>A date and time with an offset or 'Z' (read as UTC).</summary>
    DatetimeUtc,
    String,
}

/// <summary>
/// Values written as text (CSV fields, Hive partition values) inferred and parsed as dataframe engines' CSV readers do:
/// booleans (<c>true</c>/<c>false</c>, any case), then floats, then integers (<c>-?\d+</c>, leading zeros allowed,
/// no '+'), then, when dates are parsed, dates (day-month-year or year-month-day, '-', '/' or '.' between) and
/// date-times (a 'T' or a space, <c>H:M[:S[.f]]</c> or <c>HM[S[.f]]</c>, an optional offset or 'Z'); everything else is
/// a string. Times of day alone are strings.
/// </summary>
internal static class TextValues
{
    /// <summary>The kind <paramref name="text"/> holds.</summary>
    public static TextKind Infer(ReadOnlySpan<char> text, bool parseDates)
    {
        if (text.Length >= 2 && text[0] == '"' && text[^1] == '"')
            return parseDates ? TemporalKind(text[1..^1]) ?? TextKind.String : TextKind.String;
        if (IsBoolean(text)) return TextKind.Boolean;
        if (IsFloat(text)) return TextKind.Float64;
        if (IsInteger(text)) return TryParseInt64(text, out _) ? TextKind.Int64 : TryParseInt128(text, out _) ? TextKind.Int128 : TextKind.String;
        return parseDates ? TemporalKind(text) ?? TextKind.String : TextKind.String;
    }

    /// <summary>The kind of a column whose values were inferred as <paramref name="kinds"/> (conflicts are strings).</summary>
    public static TextKind? Finish(IReadOnlyCollection<TextKind> kinds) => kinds.Count switch
    {
        0 => null,
        1 => kinds.First(),
        2 when kinds.Contains(TextKind.Int64) && kinds.Contains(TextKind.Float64) => TextKind.Float64,
        2 when kinds.Contains(TextKind.Int64) && kinds.Contains(TextKind.Int128) => TextKind.Int128,
        2 when kinds.Contains(TextKind.Int128) && kinds.Contains(TextKind.Float64) => TextKind.Float64,
        _ => TextKind.String,
    };

    private static TextKind? TemporalKind(ReadOnlySpan<char> text) =>
        TryParseDate(text, out _) ? TextKind.Date
        : TryParseDatetime(text, out _, out bool zoned) ? zoned ? TextKind.DatetimeUtc : TextKind.Datetime
        : null;

    public static bool IsBoolean(ReadOnlySpan<char> text) =>
        text.Equals("true", StringComparison.OrdinalIgnoreCase) || text.Equals("false", StringComparison.OrdinalIgnoreCase);

    public static bool TryParseBoolean(ReadOnlySpan<char> text, out bool value)
    {
        value = text.Equals("true", StringComparison.OrdinalIgnoreCase);
        return value || text.Equals("false", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary><c>-?\d+</c>.</summary>
    public static bool IsInteger(ReadOnlySpan<char> text)
    {
        if (text.Length > 0 && text[0] == '-') text = text[1..];
        return text.Length > 0 && !text.ContainsAnyExceptInRange('0', '9');
    }

    /// <summary><c>[-+]?((\d*\.\d+)([eE][-+]?\d+)?|inf|NaN|(\d+)[eE][-+]?\d+|\d+\.)</c>.</summary>
    public static bool IsFloat(ReadOnlySpan<char> text)
    {
        if (text.Length > 0 && text[0] is '-' or '+') text = text[1..];
        if (text is "inf" or "NaN") return true;
        int whole = Digits(text);
        ReadOnlySpan<char> rest = text[whole..];
        if (rest.Length > 0 && rest[0] == '.')
        {
            int fraction = Digits(rest[1..]);
            if (fraction == 0) return whole > 0 && rest.Length == 1; // "1."
            rest = rest[(1 + fraction)..];
            return rest.IsEmpty || Exponent(rest);
        }
        return whole > 0 && Exponent(rest);

        static int Digits(ReadOnlySpan<char> s)
        {
            int n = s.IndexOfAnyExceptInRange('0', '9');
            return n < 0 ? s.Length : n;
        }

        static bool Exponent(ReadOnlySpan<char> s)
        {
            if (s.Length < 2 || s[0] is not ('e' or 'E')) return false;
            s = s[1..];
            if (s[0] is '-' or '+') s = s[1..];
            return s.Length > 0 && Digits(s) == s.Length;
        }
    }

    public static bool TryParseInt64(ReadOnlySpan<char> text, out long value) =>
        long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value) && IsInteger(text);

    public static bool TryParseInt128(ReadOnlySpan<char> text, out Int128 value) =>
        Int128.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value) && IsInteger(text);

    /// <summary>A float as the float pattern writes it, or an integer.</summary>
    public static bool TryParseFloat64(ReadOnlySpan<char> text, out double value)
    {
        value = 0;
        if (!IsFloat(text) && !IsInteger(text)) return false;
        bool negative = text.Length > 0 && text[0] == '-';
        ReadOnlySpan<char> unsigned = text.Length > 0 && text[0] is '-' or '+' ? text[1..] : text;
        if (unsigned is "inf")
        {
            value = negative ? double.NegativeInfinity : double.PositiveInfinity;
            return true;
        }
        if (unsigned is "NaN")
        {
            value = double.NaN;
            return true;
        }
        return double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out value);
    }

    /// <summary>A date alone: day-month-year or year-month-day, one separator ('-', '/' or '.') used twice.</summary>
    public static bool TryParseDate(ReadOnlySpan<char> text, out DateOnly value)
    {
        value = default;
        int first = text.IndexOfAny('-', '/', '.');
        if (first < 0) return false;
        char separator = text[first];
        int second = text[(first + 1)..].IndexOf(separator);
        if (second < 0) return false;
        second += first + 1;
        if (text[(second + 1)..].IndexOfAny('-', '/', '.') >= 0) return false;
        ReadOnlySpan<char> a = text[..first], b = text[(first + 1)..second], c = text[(second + 1)..];
        // Day-month-year first.
        if (Number(a, 1, 2, out int day) && Number(b, 1, 2, out int month) && Number(c, 4, 4, out int year) && Valid(year, month, day, out value)) return true;
        return Number(a, 4, 4, out year) && Number(b, 1, 2, out month) && Number(c, 1, 2, out day) && Valid(year, month, day, out value);
    }

    /// <summary>
    /// A date and time (the date as <see cref="TryParseDate"/> reads it, or compact <c>yyyyMMdd</c> before a 'T'), then
    /// a 'T' or a space and a time: <c>H:M</c>, <c>H:M:S[.f]</c>, <c>HHMM</c>, <c>HHMMSS[.f]</c>; an offset after it
    /// (<c>Z</c>, <c>±hh</c>, <c>±hhmm</c>, <c>±hh:mm</c>) marks it <paramref name="zoned"/>, the value then in UTC. A
    /// date alone is midnight.
    /// </summary>
    public static bool TryParseDatetime(ReadOnlySpan<char> text, out DateTime value, out bool zoned)
    {
        value = default;
        zoned = false;
        int split = text.IndexOfAny('T', ' ');
        if (split < 0)
        {
            if (!TryParseDate(text, out DateOnly day)) return false;
            value = day.ToDateTime(TimeOnly.MinValue, DateTimeKind.Unspecified);
            return true;
        }
        ReadOnlySpan<char> datePart = text[..split], rest = text[(split + 1)..];
        DateOnly date;
        if (datePart.Length == 8 && text[split] == 'T' && !datePart.ContainsAnyExceptInRange('0', '9'))
        {
            if (!Valid(int.Parse(datePart[..4], CultureInfo.InvariantCulture), int.Parse(datePart[4..6], CultureInfo.InvariantCulture),
                    int.Parse(datePart[6..], CultureInfo.InvariantCulture), out date)) return false;
        }
        else if (!TryParseDate(datePart, out date)) return false;

        // The offset, when there is one: 'Z' or a sign after the time.
        TimeSpan offset = default;
        int sign = rest.IndexOfAny('+', '-');
        if (rest.Length > 0 && rest[^1] == 'Z')
        {
            zoned = true;
            rest = rest[..^1];
        }
        else if (sign >= 0)
        {
            if (!Offset(rest[sign..], out offset)) return false;
            zoned = true;
            rest = rest[..sign];
        }
        if (!Time(rest, out TimeSpan time)) return false;
        value = date.ToDateTime(TimeOnly.MinValue, DateTimeKind.Unspecified) + time - offset;
        if (zoned) value = DateTime.SpecifyKind(value, DateTimeKind.Utc);
        return true;
    }

    private static bool Time(ReadOnlySpan<char> text, out TimeSpan time)
    {
        time = default;
        int dot = text.IndexOf('.');
        ReadOnlySpan<char> fraction = dot < 0 ? [] : text[(dot + 1)..];
        if (dot >= 0 && (fraction.IsEmpty || fraction.ContainsAnyExceptInRange('0', '9'))) return false;
        ReadOnlySpan<char> clock = dot < 0 ? text : text[..dot];
        int hour, minute, second = 0;
        if (clock.Contains(':'))
        {
            int c1 = clock.IndexOf(':');
            int c2 = clock[(c1 + 1)..].IndexOf(':');
            if (c2 < 0)
            {
                if (dot >= 0 || !Number(clock[..c1], 1, 2, out hour) || !Number(clock[(c1 + 1)..], 2, 2, out minute)) return false;
            }
            else
            {
                c2 += c1 + 1;
                if (!Number(clock[..c1], 1, 2, out hour) || !Number(clock[(c1 + 1)..c2], 2, 2, out minute) || !Number(clock[(c2 + 1)..], 2, 2, out second)) return false;
            }
        }
        else if (clock.Length == 4 && dot < 0)
        {
            if (!Number(clock[..2], 2, 2, out hour) || !Number(clock[2..], 2, 2, out minute)) return false;
        }
        else if (clock.Length == 6)
        {
            if (!Number(clock[..2], 2, 2, out hour) || !Number(clock[2..4], 2, 2, out minute) || !Number(clock[4..], 2, 2, out second)) return false;
        }
        else return false;
        if (hour > 23 || minute > 59 || second > 59) return false;
        long ticks = 0;
        for (int i = 0, scale = (int)(TimeSpan.TicksPerSecond / 10); i < fraction.Length && scale > 0; i++, scale /= 10) ticks += (fraction[i] - '0') * scale;
        time = new TimeSpan(hour, minute, second) + TimeSpan.FromTicks(ticks);
        return true;
    }

    private static bool Offset(ReadOnlySpan<char> text, out TimeSpan offset)
    {
        offset = default;
        int sign = text[0] == '-' ? -1 : 1;
        text = text[1..];
        int hours, minutes = 0;
        if (text.Length == 2) { if (!Number(text, 2, 2, out hours)) return false; }
        else if (text.Length == 4) { if (!Number(text[..2], 2, 2, out hours) || !Number(text[2..], 2, 2, out minutes)) return false; }
        else if (text.Length == 5 && text[2] == ':') { if (!Number(text[..2], 2, 2, out hours) || !Number(text[3..], 2, 2, out minutes)) return false; }
        else return false;
        if (hours > 23 || minutes > 59) return false;
        offset = sign * new TimeSpan(hours, minutes, 0);
        return true;
    }

    private static bool Number(ReadOnlySpan<char> text, int min, int max, out int value)
    {
        value = 0;
        if (text.Length < min || text.Length > max || text.ContainsAnyExceptInRange('0', '9')) return false;
        value = int.Parse(text, NumberStyles.None, CultureInfo.InvariantCulture);
        return true;
    }

    private static bool Valid(int year, int month, int day, out DateOnly date)
    {
        date = default;
        if (year < 1 || month is < 1 or > 12 || day < 1 || day > DateTime.DaysInMonth(year, month)) return false;
        date = new DateOnly(year, month, day);
        return true;
    }
}
