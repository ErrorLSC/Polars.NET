using System.Globalization;
using System.Text;

namespace Kaminari.Sazanami.Text;

/// <summary>What a temporal format is for: dates, times of day, timestamps without a zone, timestamps with one.</summary>
internal enum TemporalTarget
{
    Date,
    Time,
    Naive,
    Zoned,
}

/// <summary>One value to format: its local calendar date and clock, and for a zoned timestamp its offset and instant.</summary>
internal struct TemporalValue
{
    /// <summary>The local date, in days since 1970-01-01.</summary>
    public long Days;

    /// <summary>The local time of day, in seconds.</summary>
    public int SecondOfDay;

    public int Nanosecond;

    /// <summary>The offset east of UTC, in seconds (zoned).</summary>
    public int Offset;

    /// <summary>The instant in seconds since 1970-01-01T00:00:00Z (a naive timestamp's own seconds).</summary>
    public long UnixSeconds;

    /// <summary>The zone's abbreviation (zoned, when the format writes it).</summary>
    public string? Zone;
}

/// <summary>
/// A date, time or timestamp format compiled once and applied to many values: a .NET format (custom, as
/// <c>yyyy-MM-dd HH:mm:ss.fff</c>, or standard, as <c>o</c>), written as <see cref="DateTime.ToString(string, IFormatProvider)"/>
/// writes it with the invariant culture, or — a format holding '%' — a chrono (strftime) format, written as the
/// reference dataframe engine writes it. A specifier that does not fit what is formatted (an hour in a date, an offset in
/// a timestamp without a zone) is refused when compiling.
/// </summary>
internal sealed class TemporalFormat
{
    private static readonly string[] MonthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
    private static readonly string[] DayNames = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];

    private readonly Op[] _ops;

    private TemporalFormat(Op[] ops, bool utc)
    {
        _ops = ops;
        Utc = utc;
        WritesZone = ops.Any(o => o.Kind == Kind.ZoneName);
        MaxBytes = ops.Sum(o => o.Kind == Kind.Literal ? o.Text!.Length : 24);
    }

    /// <summary>The most bytes a value's text takes.</summary>
    public int MaxBytes { get; }

    /// <summary>Whether the format writes the zone's abbreviation (<see cref="TemporalValue.Zone"/> is needed).</summary>
    public bool WritesZone { get; }

    /// <summary>Whether a zoned value is written as its UTC time (the .NET standard formats <c>u</c>, <c>r</c>).</summary>
    public bool Utc { get; }

    private enum Kind
    {
        Literal, Year, Century, YearOfCentury, Month, MonthAbbreviation, MonthName, Day, DayAbbreviation, DayName, WeekdayFromSunday,
        WeekdayFromMonday, WeekFromSunday, WeekFromMonday, IsoYearOfCentury, IsoWeek, DayOfYear, Hour, Hour12, Minute, Second,
        AmPm, AmPmLower, AmPmFirst, Fraction, FractionTrimmed, FractionAuto, OffsetHours, OffsetCompact, OffsetColon, OffsetSeconds,
        ZoneName, UnixSeconds, Era, SignedYear, SignedIsoYear,
    }

    private enum Pad : byte
    {
        Zero,
        Space,
        None,
    }

    // A piece of the format: a literal, or a field (with its width and padding; for fractions, its digits and a leading point).
    private readonly record struct Op(Kind Kind, int Width = 0, Pad Pad = Pad.Zero, byte[]? Text = null, bool Point = false);

    /// <summary>Compiles <paramref name="format"/> for <paramref name="target"/>.</summary>
    /// <exception cref="FormatException">The format has a specifier that is not known or does not fit the target.</exception>
    public static TemporalFormat Compile(string format, TemporalTarget target)
    {
        ArgumentNullException.ThrowIfNull(format);
        return format.Contains('%', StringComparison.Ordinal) ? Chrono(format, target) : Net(format, target);
    }

    // ---------------------------------------------------------------- chrono (strftime)

    private static TemporalFormat Chrono(string format, TemporalTarget target)
    {
        List<Op> ops = [];
        var literal = new StringBuilder();
        for (int i = 0; i < format.Length; i++)
        {
            char c = format[i];
            if (c != '%')
            {
                literal.Append(c);
                continue;
            }
            int start = i;
            if (++i >= format.Length) throw Refuse(format, "%", "a '%' ends it (write %% for a percent sign)");
            Pad? pad = format[i] switch { '-' => Pad.None, '_' => Pad.Space, '0' => Pad.Zero, _ => null };
            if (pad is not null && ++i >= format.Length) throw Refuse(format, format[start..], "a padding flag ends it");
            string rest = format[i..];
            string spec = rest switch
            {
                _ when rest.StartsWith(".f", StringComparison.Ordinal) || rest.StartsWith(":z", StringComparison.Ordinal)
                    || rest.StartsWith("#z", StringComparison.Ordinal) => rest[..2],
                _ when rest.StartsWith("::z", StringComparison.Ordinal) => rest[..3],
                _ when rest.Length >= 3 && rest[0] == '.' && rest[1] is '3' or '6' or '9' && rest[2] == 'f' => rest[..3],
                _ when rest.Length >= 2 && rest[0] is '3' or '6' or '9' && rest[1] == 'f' => rest[..2],
                _ => rest[..1],
            };
            i += spec.Length - 1;
            string token = format[start..(i + 1)];
            if (spec is "t" or "n" or "%")
            {
                literal.Append(spec switch { "t" => '\t', "n" => '\n', _ => '%' });
                continue;
            }
            Flush(ops, literal);
            if (!ChronoSpec(ops, spec, pad, target)) throw Refuse(format, token, Unknown(spec) ? "it is not a chrono specifier" : $"it does not fit a {What(target)}");
        }
        Flush(ops, literal);
        return new TemporalFormat([.. ops], utc: false);
    }

    private static bool Unknown(string spec) => spec is not ("Y" or "C" or "y" or "m" or "b" or "B" or "h" or "d" or "e" or "a" or "A" or "w" or "u"
        or "U" or "W" or "G" or "g" or "V" or "j" or "D" or "x" or "F" or "v" or "H" or "k" or "I" or "l" or "P" or "p" or "M" or "S" or "f"
        or ".f" or ".3f" or ".6f" or ".9f" or "3f" or "6f" or "9f" or "R" or "T" or "X" or "r" or "Z" or "z" or ":z" or "::z" or "c" or "+" or "s");

    // One chrono specifier's pieces, or false when it does not fit the target (or is not known).
    private static bool ChronoSpec(List<Op> ops, string spec, Pad? pad, TemporalTarget target)
    {
        bool date = target != TemporalTarget.Time, time = target != TemporalTarget.Date, zoned = target == TemporalTarget.Zoned;
        Op Number(Kind kind, int width, Pad standard = Pad.Zero) => new(kind, width, pad ?? standard);
        Op? op = spec switch
        {
            "Y" when date => Number(Kind.SignedYear, 4),
            "C" when date => Number(Kind.Century, 2),
            "y" when date => Number(Kind.YearOfCentury, 2),
            "m" when date => Number(Kind.Month, 2),
            "b" or "h" when date => new Op(Kind.MonthAbbreviation),
            "B" when date => new Op(Kind.MonthName),
            "d" when date => Number(Kind.Day, 2),
            "e" when date => Number(Kind.Day, 2, Pad.Space),
            "a" when date => new Op(Kind.DayAbbreviation),
            "A" when date => new Op(Kind.DayName),
            "w" when date => Number(Kind.WeekdayFromSunday, 1),
            "u" when date => Number(Kind.WeekdayFromMonday, 1),
            "U" when date => Number(Kind.WeekFromSunday, 2),
            "W" when date => Number(Kind.WeekFromMonday, 2),
            "G" when date => Number(Kind.SignedIsoYear, 4),
            "g" when date => Number(Kind.IsoYearOfCentury, 2),
            "V" when date => Number(Kind.IsoWeek, 2),
            "j" when date => Number(Kind.DayOfYear, 3),
            "H" when time => Number(Kind.Hour, 2),
            "k" when time => Number(Kind.Hour, 2, Pad.Space),
            "I" when time => Number(Kind.Hour12, 2),
            "l" when time => Number(Kind.Hour12, 2, Pad.Space),
            "P" when time => new Op(Kind.AmPmLower),
            "p" when time => new Op(Kind.AmPm),
            "M" when time => Number(Kind.Minute, 2),
            "S" when time => Number(Kind.Second, 2),
            "f" when time => new Op(Kind.Fraction, 9),
            "3f" or "6f" or "9f" when time => new Op(Kind.Fraction, spec[0] - '0'),
            ".3f" or ".6f" or ".9f" when time => new Op(Kind.Fraction, spec[1] - '0', Point: true),
            ".f" when time => new Op(Kind.FractionAuto),
            "z" when zoned => new Op(Kind.OffsetCompact),
            ":z" when zoned => new Op(Kind.OffsetColon),
            "::z" when zoned => new Op(Kind.OffsetSeconds),
            "Z" when zoned => new Op(Kind.ZoneName),
            "s" when date && time => new Op(Kind.UnixSeconds),
            _ => null,
        };
        if (op is { } single)
        {
            ops.Add(single);
            return true;
        }
        // Composites: their pieces.
        string[]? parts = spec switch
        {
            "D" or "x" when date => ["m", "/", "d", "/", "y"],
            "F" when date => ["Y", "-", "m", "-", "d"],
            "v" when date => ["e", "-", "b", "-", "Y"],
            "R" when time => ["H", ":", "M"],
            "T" or "X" when time => ["H", ":", "M", ":", "S"],
            "r" when time => ["I", ":", "M", ":", "S", " ", "p"],
            "c" when date && time => ["a", " ", "b", " ", "e", " ", "H", ":", "M", ":", "S", " ", "Y"],
            "+" when zoned => ["Y", "-", "m", "-", "d", "T", "H", ":", "M", ":", "S", ".f", ":z"],
            _ => null,
        };
        if (parts is null) return false;
        foreach (string part in parts)
            if (part.Length == 1 && !char.IsAsciiLetter(part[0]) || part == "T" && spec == "+") ops.Add(new Op(Kind.Literal, Text: Encoding.UTF8.GetBytes(part)));
            else ChronoSpec(ops, part, null, target);
        return true;
    }

    // ---------------------------------------------------------------- .NET

    private static TemporalFormat Net(string format, TemporalTarget target)
    {
        if (format.Length == 0) throw Refuse(format, "", "it is empty");
        bool utc = false;
        string pattern = format;
        if (format.Length == 1)
        {
            DateTimeFormatInfo info = CultureInfo.InvariantCulture.DateTimeFormat;
            const string RoundTrip = "yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fffffffK";
            (string? expanded, bool allowed) = (format[0], target) switch
            {
                ('o' or 'O', TemporalTarget.Date) => ("yyyy'-'MM'-'dd", true),
                ('o' or 'O', TemporalTarget.Time) => ("HH':'mm':'ss'.'fffffff", true),
                ('o' or 'O', _) => (RoundTrip, true),
                ('r' or 'R', TemporalTarget.Date) => ("ddd, dd MMM yyyy", true),
                ('r' or 'R', TemporalTarget.Time) => ("HH':'mm':'ss", true),
                ('r' or 'R', _) => (info.RFC1123Pattern, true),
                ('d', not TemporalTarget.Time) => (info.ShortDatePattern, true),
                ('D', not TemporalTarget.Time) => (info.LongDatePattern, true),
                ('m' or 'M', not TemporalTarget.Time) => (info.MonthDayPattern, true),
                ('y' or 'Y', not TemporalTarget.Time) => (info.YearMonthPattern, true),
                ('t', not TemporalTarget.Date) => (info.ShortTimePattern, true),
                ('T', not TemporalTarget.Date) => (info.LongTimePattern, true),
                ('f', TemporalTarget.Naive or TemporalTarget.Zoned) => (info.LongDatePattern + " " + info.ShortTimePattern, true),
                ('F', TemporalTarget.Naive or TemporalTarget.Zoned) => (info.FullDateTimePattern, true),
                ('g', TemporalTarget.Naive or TemporalTarget.Zoned) => (info.ShortDatePattern + " " + info.ShortTimePattern, true),
                ('G', TemporalTarget.Naive or TemporalTarget.Zoned) => (info.ShortDatePattern + " " + info.LongTimePattern, true),
                ('s', TemporalTarget.Naive or TemporalTarget.Zoned) => (info.SortableDateTimePattern, true),
                ('u', TemporalTarget.Naive or TemporalTarget.Zoned) => (info.UniversalSortableDateTimePattern, true),
                _ => (null, false),
            };
            if (!allowed) throw Refuse(format, format, $"it is not a .NET standard format of a {What(target)}");
            // u and r write a zoned value's UTC time.
            utc = target == TemporalTarget.Zoned && format[0] is 'u' or 'r' or 'R';
            pattern = expanded!;
        }
        List<Op> ops = [];
        var literal = new StringBuilder();
        for (int i = 0; i < pattern.Length;)
        {
            char c = pattern[i];
            int count = 1;
            while (i + count < pattern.Length && pattern[i + count] == c) count++;
            switch (c)
            {
                case '\'' or '"':
                {
                    int end = i + 1;
                    for (; end < pattern.Length && pattern[end] != c; end++)
                    {
                        if (pattern[end] == '\\' && end + 1 < pattern.Length) end++;
                        literal.Append(pattern[end]);
                    }
                    if (end >= pattern.Length) throw Refuse(format, pattern[i..], "a quoted text is not closed");
                    i = end + 1;
                    continue;
                }
                case '\\':
                    if (i + 1 >= pattern.Length) throw Refuse(format, "\\", "a '\\' ends it");
                    literal.Append(pattern[i + 1]);
                    i += 2;
                    continue;
                // The time and date separators: .NET refuses the one that does not fit (a date's ':', a time's '/').
                case ':' when target == TemporalTarget.Date:
                case '/' when target == TemporalTarget.Time:
                    throw Refuse(format, c.ToString(), $"it does not fit a {What(target)}");
                case ':' or '/':
                    literal.Append(c, count);
                    i += count;
                    continue;
            }
            Op? op = NetSpec(c, count, target, out bool known);
            if (!known)
            {
                literal.Append(c, count);
                i += count;
                continue;
            }
            if (op is null) throw Refuse(format, new string(c, count), $"it does not fit a {What(target)}");
            Flush(ops, literal);
            ops.Add(op.Value);
            i += count;
        }
        Flush(ops, literal);
        return new TemporalFormat([.. ops], utc);
    }

    // A .NET custom specifier repeated `count` times: its piece, null when it does not fit the target; not `known` for a literal.
    private static Op? NetSpec(char c, int count, TemporalTarget target, out bool known)
    {
        bool date = target != TemporalTarget.Time, time = target != TemporalTarget.Date;
        known = c is 'y' or 'M' or 'd' or 'g' or 'h' or 'H' or 'm' or 's' or 'f' or 'F' or 't' or 'z' or 'K';
        return (c, count) switch
        {
            ('y', <= 2) when date => new Op(Kind.YearOfCentury, count),
            ('y', _) when date => new Op(Kind.Year, count),
            ('M', <= 2) when date => new Op(Kind.Month, count),
            ('M', 3) when date => new Op(Kind.MonthAbbreviation),
            ('M', _) when date => new Op(Kind.MonthName),
            ('d', <= 2) when date => new Op(Kind.Day, count),
            ('d', 3) when date => new Op(Kind.DayAbbreviation),
            ('d', _) when date => new Op(Kind.DayName),
            ('g', _) when date => new Op(Kind.Era),
            ('h', _) when time => new Op(Kind.Hour12, Math.Min(count, 2)),
            ('H', _) when time => new Op(Kind.Hour, Math.Min(count, 2)),
            ('m', _) when time => new Op(Kind.Minute, Math.Min(count, 2)),
            ('s', _) when time => new Op(Kind.Second, Math.Min(count, 2)),
            ('f', <= 7) when time => new Op(Kind.Fraction, count),
            ('F', <= 7) when time => new Op(Kind.FractionTrimmed, count),
            ('t', 1) when time => new Op(Kind.AmPmFirst),
            ('t', _) when time => new Op(Kind.AmPm),
            ('z', _) when target == TemporalTarget.Zoned => new Op(Kind.OffsetHours, Math.Min(count, 3)),
            ('K', _) when target == TemporalTarget.Zoned => new Op(Kind.OffsetColon),
            ('K', _) when target == TemporalTarget.Naive => new Op(Kind.Literal, Text: []),
            _ => null,
        };
    }

    private static void Flush(List<Op> ops, StringBuilder literal)
    {
        if (literal.Length == 0) return;
        ops.Add(new Op(Kind.Literal, Text: Encoding.UTF8.GetBytes(literal.ToString())));
        literal.Clear();
    }

    private static string What(TemporalTarget target) => target switch
    {
        TemporalTarget.Date => "date",
        TemporalTarget.Time => "time of day",
        TemporalTarget.Naive => "timestamp without a time zone",
        _ => "timestamp with a time zone",
    };

    private static FormatException Refuse(string format, string token, string why) =>
        new($"The format '{format}' cannot be used: '{token}': {why}. Write a .NET format (yyyy-MM-dd HH:mm:ss.fff) or a chrono one (%Y-%m-%d %H:%M:%S%.3f).");

    // ---------------------------------------------------------------- writing

    /// <summary>A value's text into <paramref name="destination"/> (at least <see cref="MaxBytes"/> long): its length.</summary>
    public int Format(in TemporalValue value, Span<byte> destination)
    {
        // The calendar fields, worked out once.
        var (year, month, day) = IsoTemporal.CivilFromDays(value.Days);
        int weekday = (int)(((value.Days + 4) % 7 + 7) % 7); // 1970-01-01 was a Thursday; Sunday = 0
        int hour = value.SecondOfDay / 3600, minute = value.SecondOfDay / 60 % 60, second = value.SecondOfDay % 60;
        int at = 0;
        foreach (Op op in _ops)
        {
            if (op.Kind == Kind.FractionTrimmed)
            {
                at = Trimmed(value.Nanosecond, op.Width, destination, at);
                continue;
            }
            Span<byte> d = destination[at..];
            at += op.Kind switch
            {
                Kind.Literal => Copy(op.Text!, d),
                Kind.Year => Number(year, op.Width, op.Pad, d),
                Kind.SignedYear => Year(year, op.Width, op.Pad, d),
                Kind.SignedIsoYear => Year(IsoWeek(year, month, day, weekday).Year, op.Width, op.Pad, d),
                Kind.Century => Number(Math.DivRem(year, 100, out long r) - (r < 0 ? 1 : 0), op.Width, op.Pad, d),
                Kind.YearOfCentury => Number((year % 100 + 100) % 100, op.Width, op.Pad, d),
                Kind.Month => Number(month, op.Width, op.Pad, d),
                Kind.MonthAbbreviation => Copy(MonthNames[month - 1].AsSpan(0, 3), d),
                Kind.MonthName => Copy(MonthNames[month - 1], d),
                Kind.Day => Number(day, op.Width, op.Pad, d),
                Kind.DayAbbreviation => Copy(DayNames[weekday].AsSpan(0, 3), d),
                Kind.DayName => Copy(DayNames[weekday], d),
                Kind.WeekdayFromSunday => Number(weekday, op.Width, op.Pad, d),
                Kind.WeekdayFromMonday => Number(weekday == 0 ? 7 : weekday, op.Width, op.Pad, d),
                Kind.WeekFromSunday => Number((DayOfYear(year, month, day) - 1 + 7 - weekday) / 7, op.Width, op.Pad, d),
                Kind.WeekFromMonday => Number((DayOfYear(year, month, day) - 1 + 7 - (weekday + 6) % 7) / 7, op.Width, op.Pad, d),
                Kind.IsoYearOfCentury => Number((IsoWeek(year, month, day, weekday).Year % 100 + 100) % 100, op.Width, op.Pad, d),
                Kind.IsoWeek => Number(IsoWeek(year, month, day, weekday).Week, op.Width, op.Pad, d),
                Kind.DayOfYear => Number(DayOfYear(year, month, day), op.Width, op.Pad, d),
                Kind.Hour => Number(hour, op.Width, op.Pad, d),
                Kind.Hour12 => Number(hour % 12 == 0 ? 12 : hour % 12, op.Width, op.Pad, d),
                Kind.Minute => Number(minute, op.Width, op.Pad, d),
                Kind.Second => Number(second, op.Width, op.Pad, d),
                Kind.AmPm => Copy(hour < 12 ? "AM"u8 : "PM"u8, d),
                Kind.AmPmLower => Copy(hour < 12 ? "am"u8 : "pm"u8, d),
                Kind.AmPmFirst => Copy(hour < 12 ? "A"u8 : "P"u8, d),
                Kind.Fraction => Fraction(value.Nanosecond, op.Width, op.Point, d),
                Kind.FractionAuto => value.Nanosecond == 0 ? 0 : Fraction(value.Nanosecond, value.Nanosecond % 1_000_000 == 0 ? 3 : value.Nanosecond % 1000 == 0 ? 6 : 9, true, d),
                Kind.OffsetHours => Offset(value.Offset, op.Width, d),
                Kind.OffsetCompact => Offset(value.Offset, 4, d),
                Kind.OffsetColon => Offset(value.Offset, 5, d),
                Kind.OffsetSeconds => Offset(value.Offset, 6, d),
                Kind.ZoneName => Copy(value.Zone ?? "", d),
                Kind.UnixSeconds => Number(value.UnixSeconds, 1, Pad.None, d),
                Kind.Era => Copy("A.D."u8, d),
                _ => 0,
            };
        }
        return at;
    }

    private static int Copy(ReadOnlySpan<byte> text, Span<byte> destination)
    {
        text.CopyTo(destination);
        return text.Length;
    }

    private static int Copy(ReadOnlySpan<char> text, Span<byte> destination)
    {
        for (int i = 0; i < text.Length; i++) destination[i] = (byte)text[i];
        return text.Length;
    }

    // A number at least `width` digits wide (zeros or spaces before it, or none), a '-' before a negative one.
    private static int Number(long value, int width, Pad pad, Span<byte> destination)
    {
        ulong magnitude = value < 0 ? (ulong)-value : (ulong)value;
        int count = 1;
        for (ulong m = magnitude; m >= 10; m /= 10) count++;
        int at = 0;
        if (value < 0) destination[at++] = (byte)'-';
        if (pad != Pad.None)
            for (int i = count; i < width; i++) destination[at++] = pad == Pad.Zero ? (byte)'0' : (byte)' ';
        // The digits written in place, from the last.
        for (int i = at + count - 1; i >= at; i--)
        {
            destination[i] = (byte)('0' + magnitude % 10);
            magnitude /= 10;
        }
        return at + count;
    }

    // A chrono year: four digits at least, past 9999 with a '+' (as the engine writes them).
    private static int Year(long year, int width, Pad pad, Span<byte> destination)
    {
        if (year > 9999)
        {
            destination[0] = (byte)'+';
            return 1 + Number(year, width, pad, destination[1..]);
        }
        return Number(year, width, pad, destination);
    }

    // The first `digits` digits of the nanoseconds (cut, not rounded), a '.' before them if asked.
    private static int Fraction(int nanoseconds, int digits, bool point, Span<byte> destination)
    {
        int at = 0;
        if (point) destination[at++] = (byte)'.';
        int value = nanoseconds;
        for (int i = digits; i < 9; i++) value /= 10;
        for (int i = digits - 1; i >= 0; i--)
        {
            destination[at + i] = (byte)('0' + value % 10);
            value /= 10;
        }
        return at + digits;
    }

    // .NET's F: the fraction's first `digits` digits without trailing zeros; none at all (and the '.' written before it
    // taken back) when they are all zero. Where the text ends then.
    private static int Trimmed(int nanoseconds, int digits, Span<byte> destination, int at)
    {
        Span<byte> d = destination[at..];
        int length = Fraction(nanoseconds, digits, false, d);
        while (length > 0 && d[length - 1] == '0') length--;
        if (length == 0 && at > 0 && destination[at - 1] == '.') return at - 1;
        return at + length;
    }

    // An offset: width 1–3 as .NET's z, zz, zzz (+9, +09, +09:00); 4 as +0900; 5 as +09:00; 6 as +09:00:00. Minutes are
    // the nearest (a local mean time's seconds).
    private static int Offset(int seconds, int width, Span<byte> destination)
    {
        destination[0] = seconds < 0 ? (byte)'-' : (byte)'+';
        int abs = Math.Abs(seconds);
        if (width == 6)
        {
            int at6 = 1 + Number(abs / 3600, 2, Pad.Zero, destination[1..]);
            destination[at6++] = (byte)':';
            at6 += Number(abs / 60 % 60, 2, Pad.Zero, destination[at6..]);
            destination[at6++] = (byte)':';
            return at6 + Number(abs % 60, 2, Pad.Zero, destination[at6..]);
        }
        int minutes = width <= 2 ? abs / 60 : (abs + 30) / 60;
        int at = 1 + Number(minutes / 60, width == 1 ? 1 : 2, Pad.Zero, destination[1..]);
        if (width <= 2) return at;
        if (width != 4) destination[at++] = (byte)':';
        return at + Number(minutes % 60, 2, Pad.Zero, destination[at..]);
    }

    private static int DayOfYear(long year, int month, int day)
    {
        ReadOnlySpan<int> before = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];
        bool leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        return before[month - 1] + day + (leap && month > 2 ? 1 : 0);
    }

    // The ISO 8601 week-numbering year and week (weeks start on Monday; week 1 holds the year's first Thursday).
    private static (long Year, int Week) IsoWeek(long year, int month, int day, int weekday)
    {
        int isoWeekday = weekday == 0 ? 7 : weekday;
        int week = (DayOfYear(year, month, day) - isoWeekday + 10) / 7;
        if (week < 1) return (year - 1, WeeksIn(year - 1));
        if (week > WeeksIn(year)) return (year + 1, 1);
        return (year, week);

        static int WeeksIn(long y)
        {
            // A year has 53 weeks when it starts on a Thursday, or on a Wednesday in a leap year.
            long days = IsoTemporal.DaysFromCivil(y, 1, 1);
            int first = (int)(((days + 4) % 7 + 7) % 7);
            bool leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
            return first == 4 || leap && first == 3 ? 53 : 52;
        }
    }
}
