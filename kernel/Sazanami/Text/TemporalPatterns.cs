using System.Text;
using Apache.Arrow.Types;

namespace Kaminari.Sazanami.Text;

/// <summary>The groups of temporal patterns a dataframe engine's CSV reader infers; a column keeps one group.</summary>
internal enum TemporalPattern : byte
{
    /// <summary>Day-month-year dates (<c>31-12-2021</c>, '/' or '.' between).</summary>
    DateDmy,
    /// <summary>Year-month-day dates (<c>2021-12-31</c>, '/' or '.' between).</summary>
    DateYmd,
    /// <summary>Times of day (<c>H:M</c>, <c>H:M:S[.f]</c>).</summary>
    Time,
    /// <summary>Day-month-year dates with a time.</summary>
    DatetimeDmy,
    /// <summary>Year-month-day dates with a time (or alone), and the compact <c>yyyyMMddTHHmmss</c>.</summary>
    DatetimeYmd,
    /// <summary>Year-month-day dates with a time and an offset or 'Z'; the value is the instant (UTC).</summary>
    DatetimeYmdZ,
}

/// <summary>What reading a temporal value gave.</summary>
internal enum TemporalOutcome : byte
{
    Parsed,
    /// <summary>The text is no value of the pattern group.</summary>
    Invalid,
    /// <summary>A valid value with fraction digits the unit cannot hold (the value returned drops them).</summary>
    TooFine,
    /// <summary>A valid value the unit cannot hold (datetime[ns] outside 1677–2262).</summary>
    OutOfRange,
}

/// <summary>
/// A CSV column's date or datetime values read in file order: the column's pattern group is the inferred group of the
/// first value that reads (the engine compiles its parser on the first success, a date column refusing datetime
/// groups); every later value follows that group. A caller that knows the group (from a sample read first) sets it.
/// </summary>
internal struct TemporalColumn
{
    public TemporalPattern? Group { get; set; }

    public TemporalOutcome ReadDate(ReadOnlySpan<byte> text, out int days)
    {
        days = 0;
        if (Group is { } group) return TemporalPatterns.ParseDate(text, group, out days);
        TemporalPattern? inferred = TemporalPatterns.Infer(text);
        if (inferred is not (TemporalPattern.DateDmy or TemporalPattern.DateYmd)) return TemporalOutcome.Invalid;
        TemporalOutcome outcome = TemporalPatterns.ParseDate(text, inferred.Value, out days);
        if (outcome != TemporalOutcome.Invalid) Group = inferred;
        return outcome;
    }

    public TemporalOutcome ReadDatetime(ReadOnlySpan<byte> text, TimeUnit unit, out long value)
    {
        value = 0;
        if (Group is { } group) return TemporalPatterns.ParseDatetime(text, group, unit, out value);
        TemporalPattern? inferred = TemporalPatterns.Infer(text);
        if (inferred is null or TemporalPattern.Time) return TemporalOutcome.Invalid;
        TemporalOutcome outcome = TemporalPatterns.ParseDatetime(text, inferred.Value, unit, out value);
        if (outcome != TemporalOutcome.Invalid) Group = inferred;
        return outcome;
    }
}

/// <summary>
/// Temporal text read as the reference CSV reader (0.55) reads it: its pattern lists (its time crate's patterns.rs) run through an
/// interpreter of the chrono 0.4 rules they rely on — numbers skip leading (Unicode) whitespace and take 1 to 2 digits
/// (years 1 to 4, or any count after a sign), a space in a pattern matches any whitespace or none, <c>%.f</c> takes '.'
/// and 1 to 9 digits (more are skipped), offsets are Z or ±hh[[:]mm], second 60 is a leap second (+1 s), dates are
/// proleptic Gregorian within years −262143…262142. <see cref="Infer"/> is <c>infer_pattern_single</c>; the parsers
/// read a value of a column whose group is known: the group's first pattern, else, when the engine's regex gate for the
/// group admits the text, its first pattern that reads it.
/// </summary>
internal static class TemporalPatterns
{
    private static readonly string[] DateDmyPatterns = ["%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y"];
    private static readonly string[] DateYmdPatterns = ["%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"];
    private static readonly string[] TimePatterns = ["%T%.f", "%R"];
    private static readonly string[] DatetimeDmyPatterns =
    [
        "%d-%m-%YT%H:%M:%S%.f", "%d-%m-%YT%H%M%S%.f", "%d-%m-%YT%H:%M", "%d-%m-%YT%H%M",
        "%d-%m-%Y %H:%M:%S%.f", "%d-%m-%Y %H%M%S%.f", "%d-%m-%Y %H:%M", "%d-%m-%Y %H%M", "%d-%m-%Y",
        "%d/%m/%YT%H:%M:%S%.f", "%d/%m/%YT%H%M%S%.f", "%d/%m/%YT%H:%M", "%d/%m/%YT%H%M",
        "%d/%m/%Y %H:%M:%S%.f", "%d/%m/%Y %H%M%S%.f", "%d/%m/%Y %H:%M", "%d/%m/%Y %H%M", "%d/%m/%Y",
        "%d.%m.%YT%H:%M:%S%.f", "%d.%m.%YT%H%M%S%.f", "%d.%m.%YT%H:%M", "%d.%m.%YT%H%M",
        "%d.%m.%Y %H:%M:%S%.f", "%d.%m.%Y %H%M%S%.f", "%d.%m.%Y %H:%M", "%d.%m.%Y %H%M", "%d.%m.%Y",
    ];
    private static readonly string[] DatetimeYmdPatterns =
    [
        "%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H%M%S%.f", "%Y-%m-%dT%H:%M", "%Y-%m-%dT%H%M",
        "%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H%M%S%.f", "%Y-%m-%d %H:%M", "%Y-%m-%d %H%M", "%Y-%m-%d",
        "%Y/%m/%dT%H:%M:%S%.f", "%Y/%m/%dT%H%M%S%.f", "%Y/%m/%dT%H:%M", "%Y/%m/%dT%H%M",
        "%Y/%m/%d %H:%M:%S%.f", "%Y/%m/%d %H%M%S%.f", "%Y/%m/%d %H:%M", "%Y/%m/%d %H%M", "%Y/%m/%d",
        "%Y.%m.%dT%H:%M:%S%.f", "%Y.%m.%dT%H%M%S%.f", "%Y.%m.%dT%H:%M", "%Y.%m.%dT%H%M",
        "%Y.%m.%d %H:%M:%S%.f", "%Y.%m.%d %H%M%S%.f", "%Y.%m.%d %H:%M", "%Y.%m.%d %H%M", "%Y.%m.%d",
        "%Y%m%dT%H%M%S%.f", "%Y-%m-%dT%H:%M:%S%.f",
    ];
    private static readonly string[] DatetimeYmdZPatterns =
    [
        "%Y-%m-%dT%H:%M:%S%.f%#z", "%Y-%m-%dT%H%M%S%.f%#z", "%Y-%m-%dT%H:%M%#z", "%Y-%m-%dT%H%M%#z",
        "%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%d %H%M%S%.f%#z", "%Y-%m-%d %H:%M%#z", "%Y-%m-%d %H%M%#z",
        "%Y/%m/%dT%H:%M:%S%.f%#z", "%Y/%m/%dT%H%M%S%.f%#z", "%Y/%m/%dT%H:%M%#z", "%Y/%m/%dT%H%M%#z",
        "%Y/%m/%d %H:%M:%S%.f%#z", "%Y/%m/%d %H%M%S%.f%#z", "%Y/%m/%d %H:%M%#z", "%Y/%m/%d %H%M%#z",
        "%Y.%m.%dT%H:%M:%S%.f%#z", "%Y.%m.%dT%H%M%S%.f%#z", "%Y.%m.%dT%H:%M%#z", "%Y.%m.%dT%H%M%#z",
        "%Y.%m.%d %H:%M:%S%.f%#z", "%Y.%m.%d %H%M%S%.f%#z", "%Y.%m.%d %H:%M%#z", "%Y.%m.%d %H%M%#z",
        "%Y%m%dT%H%M%S%.f%#z", "%Y%m%dT%H%M%S%.fZ", "%+",
    ];

    private const int MinYear = -262_143, MaxYear = 262_142;
    private const long NanosPerSecond = 1_000_000_000;

    /// <summary>The pattern group of <paramref name="text"/>, as a value alone infers it (dates, times, then datetimes).</summary>
    public static TemporalPattern? Infer(ReadOnlySpan<byte> text) => Infer(text, out _);

    /// <summary>
    /// The pattern group of <paramref name="text"/>; <paramref name="fraction"/> tells what its fraction of a second
    /// needs: 0 none or at most microseconds, 1 nanoseconds, 2 more than nanoseconds hold.
    /// </summary>
    public static TemporalPattern? Infer(ReadOnlySpan<byte> text, out int fraction)
    {
        fraction = 0;
        // Every pattern starts with a number: whitespace, then a digit (or a year's sign).
        int start = 0;
        SkipWhitespace(text, ref start);
        if (start == text.Length || text[start] is not ((>= (byte)'0' and <= (byte)'9') or (byte)'+' or (byte)'-')) return null;

        Fields f = default;
        TemporalPattern? found = Any(text, DateDmyPatterns, Target.Date, ref f) ? TemporalPattern.DateDmy
            : Any(text, DateYmdPatterns, Target.Date, ref f) ? TemporalPattern.DateYmd
            : Any(text, TimePatterns, Target.Time, ref f) ? TemporalPattern.Time
            : Any(text, DatetimeDmyPatterns, Target.Naive, ref f) ? TemporalPattern.DatetimeDmy
            : Any(text, DatetimeYmdPatterns, Target.Naive, ref f) ? TemporalPattern.DatetimeYmd
            : Any(text, DatetimeYmdZPatterns, Target.Naive, ref f) ? TemporalPattern.DatetimeYmdZ
            : null;
        if (found is not null) fraction = f.ExtraNonzero ? 2 : f.Nanosecond % 1000 != 0 ? 1 : 0;
        return found;

        static bool Any(ReadOnlySpan<byte> text, string[] patterns, Target target, ref Fields f)
        {
            foreach (string pattern in patterns)
                if (Read(text, pattern, target, out f)) return true;
            return false;
        }
    }

    /// <summary>A date (days since 1970-01-01) of a column of group <see cref="TemporalPattern.DateDmy"/> or <see cref="TemporalPattern.DateYmd"/>.</summary>
    public static TemporalOutcome ParseDate(ReadOnlySpan<byte> text, TemporalPattern group, out int days)
    {
        days = 0;
        string[] patterns = group switch
        {
            TemporalPattern.DateDmy => DateDmyPatterns,
            TemporalPattern.DateYmd => DateYmdPatterns,
            _ => throw new ArgumentOutOfRangeException(nameof(group)),
        };
        if (Fast(text, group == TemporalPattern.DateDmy, zoned: false, out Fields fast) && !fast.HasTime)
        {
            days = (int)DaysFromCivil(fast.Year, fast.Month, fast.Day);
            return TemporalOutcome.Parsed;
        }
        foreach (string pattern in patterns)
            if (Read(text, pattern, Target.Date, out Fields f))
            {
                days = (int)DaysFromCivil(f.Year, f.Month, f.Day);
                return TemporalOutcome.Parsed;
            }
        return TemporalOutcome.Invalid;
    }

    /// <summary>
    /// A date and time in <paramref name="unit"/> since the epoch, of a column of a date or datetime group (date groups
    /// read as their datetime lists); <see cref="TemporalPattern.DatetimeYmdZ"/> values are instants (UTC).
    /// </summary>
    public static TemporalOutcome ParseDatetime(ReadOnlySpan<byte> text, TemporalPattern group, TimeUnit unit, out long value)
    {
        value = 0;
        (string[] patterns, Target target) = group switch
        {
            TemporalPattern.DateDmy or TemporalPattern.DatetimeDmy => (DatetimeDmyPatterns, Target.Naive),
            TemporalPattern.DateYmd or TemporalPattern.DatetimeYmd => (DatetimeYmdPatterns, Target.Naive),
            TemporalPattern.DatetimeYmdZ => (DatetimeYmdZPatterns, Target.Zoned),
            _ => throw new ArgumentOutOfRangeException(nameof(group)),
        };
        bool dayFirst = group is TemporalPattern.DateDmy or TemporalPattern.DatetimeDmy;
        if (Fast(text, dayFirst, target == Target.Zoned, out Fields f)) return Instant(f, unit, out value);
        if (!Read(text, patterns[0], target, out f))
        {
            if (!Gate(text, group)) return TemporalOutcome.Invalid;
            int i = 1;
            while (i < patterns.Length && !Read(text, patterns[i], target, out f)) i++;
            if (i == patterns.Length) return TemporalOutcome.Invalid;
        }
        return Instant(f, unit, out value);
    }

    /// <summary>A time of day in nanoseconds since midnight: <c>H:M:S[.f]</c> or <c>H:M</c>.</summary>
    public static TemporalOutcome ParseTime(ReadOnlySpan<byte> text, out long nanoseconds)
    {
        nanoseconds = 0;
        Fields f = default;
        int p = 0;
        bool fast = Clock(text, ref p, ref f) && p == text.Length;
        if (!fast && !Read(text, TimePatterns[0], Target.Time, out f) && !Read(text, TimePatterns[1], Target.Time, out f)) return TemporalOutcome.Invalid;
        // A leap second is one more second (10:00:60 is 10:01:00); 23:59:60 would be no time of day.
        long nanos = ((f.Hour * 60L + f.Minute) * 60 + f.Second) * NanosPerSecond + f.Nanosecond;
        if (nanos >= 86_400 * NanosPerSecond) return TemporalOutcome.Invalid;
        nanoseconds = nanos;
        return f.ExtraNonzero ? TemporalOutcome.TooFine : TemporalOutcome.Parsed;
    }

    /// <summary>
    /// The shapes nearly every file writes, read without the pattern lists: <c>yyyy-MM-dd</c> (or <c>dd-MM-yyyy</c>;
    /// '-', '/' or '.' twice), then optionally 'T' or a space and <c>HH:mm[:ss[.f]]</c> (1 to 9 fraction digits), then,
    /// for zoned groups, <c>Z</c> or <c>±hh[[:]mm]</c>; every field in range. For these the pattern lists, regex gates
    /// included, read the same value; false sends anything else the general way.
    /// </summary>
    private static bool Fast(ReadOnlySpan<byte> s, bool dayFirst, bool zoned, out Fields f)
    {
        f = default;
        if (s.Length < 10) return false;
        byte separator = s[dayFirst ? 2 : 4];
        if (separator is not ((byte)'-' or (byte)'/' or (byte)'.') || s[dayFirst ? 5 : 7] != separator) return false;
        int century = Two(s, dayFirst ? 6 : 0), years = Two(s, dayFirst ? 8 : 2);
        f.Month = Two(s, dayFirst ? 3 : 5);
        f.Day = Two(s, dayFirst ? 0 : 8);
        int year = century * 100 + years;
        if (century < 0 || years < 0 || f.Month is < 1 or > 12 || f.Day < 1 || f.Day > DaysInMonth(year, f.Month)) return false;
        f.Year = year;
        int p = 10;
        if (p == s.Length) return !zoned;
        if (s[p] is not ((byte)'T' or (byte)' ')) return false;
        p++;
        if (!Clock(s, ref p, ref f)) return false;
        if (!zoned) return p == s.Length;
        if (p == s.Length) return false;
        f.HasOffset = true;
        if (s[p] == 'Z') return p + 1 == s.Length;
        if (s[p] is not ((byte)'+' or (byte)'-')) return false;
        int sign = s[p] == '-' ? -1 : 1, hours = Two(s, p + 1), minutes = 0;
        if (hours < 0) return false; // also an offset cut short at the end: no two digits, p must not pass the end
        p += 3;
        if (p < s.Length && s[p] == ':') p++; // then minutes must follow (the regex gate wants them)
        if (p < s.Length || s[p - 1] == ':')
        {
            minutes = Two(s, p);
            p += 2;
        }
        if (p != s.Length || hours is < 0 or > 23 || minutes is < 0 or > 59) return false;
        f.Offset = sign * (hours * 3600 + minutes * 60);
        return true;
    }

    /// <summary><c>HH:mm[:ss[.f]]</c> (1 to 9 fraction digits), every field in range (second 60 a leap second).</summary>
    private static bool Clock(ReadOnlySpan<byte> s, ref int p, ref Fields f)
    {
        if (s.Length - p < 5 || s[p + 2] != ':') return false;
        f.Hour = Two(s, p);
        f.Minute = Two(s, p + 3);
        f.HasTime = true;
        p += 5;
        if (f.Hour is < 0 or > 23 || f.Minute is < 0 or > 59) return false;
        if (s.Length - p < 3 || s[p] != ':') return true;
        f.Second = Two(s, p + 1);
        p += 3;
        if (f.Second is < 0 or > 60) return false;
        if (p == s.Length || s[p] != '.') return true;
        int start = ++p, nanos = 0;
        while (p < s.Length && p - start < 9 && s[p] - (uint)'0' <= 9) nanos = nanos * 10 + s[p++] - '0';
        if (p == start || (p < s.Length && s[p] - (uint)'0' <= 9)) return false;
        for (int digits = p - start; digits < 9; digits++) nanos *= 10;
        f.Nanosecond = nanos;
        return true;
    }

    /// <summary>Two ASCII digits at <paramref name="at"/>, or a negative number.</summary>
    private static int Two(ReadOnlySpan<byte> s, int at)
    {
        if (at + 1 >= s.Length) return -1;
        uint high = s[at] - (uint)'0', low = s[at + 1] - (uint)'0';
        return high <= 9 && low <= 9 ? (int)(high * 10 + low) : -1;
    }

    private static TemporalOutcome Instant(in Fields f, TimeUnit unit, out long value)
    {
        long seconds = DaysFromCivil(f.Year, f.Month, f.Day) * 86_400 + (f.Hour * 60L + f.Minute) * 60 + Math.Min(f.Second, 59) - f.Offset;
        long nanos = f.Nanosecond + (f.Second == 60 ? NanosPerSecond : 0);
        // Constant divisors per unit; within years −262143…262142 only nanoseconds can overflow.
        (value, bool lost) = unit switch
        {
            TimeUnit.Second => (seconds + nanos / NanosPerSecond, nanos % NanosPerSecond != 0),
            TimeUnit.Millisecond => (seconds * 1_000 + nanos / 1_000_000, nanos % 1_000_000 != 0),
            TimeUnit.Microsecond => (seconds * 1_000_000 + nanos / 1_000, nanos % 1_000 != 0),
            _ => (0, false),
        };
        if (unit == TimeUnit.Nanosecond)
        {
            if (seconds is <= -9_223_372_037 or >= 9_223_372_035)
            {
                Int128 total = (Int128)seconds * NanosPerSecond + nanos;
                if (total < long.MinValue || total > long.MaxValue) return TemporalOutcome.OutOfRange;
                value = (long)total;
            }
            else value = seconds * NanosPerSecond + nanos;
        }
        return lost || f.ExtraNonzero ? TemporalOutcome.TooFine : TemporalOutcome.Parsed;
    }

    private enum Target : byte { Date, Time, Naive, Zoned }

    private struct Fields
    {
        public int Year, Month, Day, Hour, Minute, Second, Nanosecond, Offset;
        public bool HasTime, HasOffset, ExtraNonzero;
    }

    /// <summary>Reads all of <paramref name="text"/> with one chrono format, then checks what <paramref name="target"/> needs.</summary>
    private static bool Read(ReadOnlySpan<byte> text, string format, Target target, out Fields f)
    {
        f = default;
        int p = 0;
        for (int i = 0; i < format.Length; i++)
        {
            char c = format[i];
            if (c == ' ')
            {
                SkipWhitespace(text, ref p);
                continue;
            }
            if (c != '%')
            {
                if (p == text.Length || text[p] != c) return false;
                p++;
                continue;
            }
            switch (format[++i])
            {
                case 'Y':
                    if (!Year(text, ref p, out f.Year)) return false;
                    break;
                case 'm':
                    if (!Number(text, ref p, 2, 1, 12, out f.Month)) return false;
                    break;
                case 'd':
                    if (!Number(text, ref p, 2, 1, 31, out f.Day)) return false;
                    break;
                case 'H':
                    if (!Number(text, ref p, 2, 0, 23, out f.Hour)) return false;
                    f.HasTime = true;
                    break;
                case 'M':
                    if (!Number(text, ref p, 2, 0, 59, out f.Minute)) return false;
                    break;
                case 'S':
                    if (!Number(text, ref p, 2, 0, 60, out f.Second)) return false;
                    break;
                case 'T':
                    if (!Number(text, ref p, 2, 0, 23, out f.Hour) || !Literal(text, ref p, ':') || !Number(text, ref p, 2, 0, 59, out f.Minute)
                        || !Literal(text, ref p, ':') || !Number(text, ref p, 2, 0, 60, out f.Second)) return false;
                    f.HasTime = true;
                    break;
                case 'R':
                    if (!Number(text, ref p, 2, 0, 23, out f.Hour) || !Literal(text, ref p, ':') || !Number(text, ref p, 2, 0, 59, out f.Minute)) return false;
                    f.HasTime = true;
                    break;
                case '.': // %.f
                    i++;
                    if (!Fraction(text, ref p, ref f)) return false;
                    break;
                case '#': // %#z
                    i++;
                    SkipWhitespace(text, ref p);
                    if (!Offset(text, ref p, missingMinutes: true, out f.Offset)) return false;
                    f.HasOffset = true;
                    break;
                case '+':
                    if (!Rfc3339(text, ref p, ref f)) return false;
                    break;
                default:
                    throw new InvalidOperationException(format);
            }
        }
        if (p != text.Length) return false;
        return target switch
        {
            Target.Date => ValidDate(f),
            Target.Time => f.HasTime,
            Target.Naive => ValidDate(f),
            _ => ValidDate(f) && f.HasOffset && Math.Abs(f.Offset) < 86_400 && ValidUtc(f),
        };

        static bool ValidDate(in Fields f) =>
            f.Year is >= MinYear and <= MaxYear && f.Day <= DaysInMonth(f.Year, f.Month);

        // The instant must be a date chrono holds too.
        static bool ValidUtc(in Fields f)
        {
            long days = DaysFromCivil(f.Year, f.Month, f.Day);
            long seconds = ((f.Hour * 60L + f.Minute) * 60 + Math.Min(f.Second, 59)) - f.Offset;
            days += Math.DivRem(seconds, 86_400L, out long rest) - (rest < 0 ? 1 : 0);
            return days >= DaysFromCivil(MinYear, 1, 1) && days <= DaysFromCivil(MaxYear, 12, 31);
        }
    }

    /// <summary>chrono's <c>%+</c> when parsing: RFC 3339, relaxed.</summary>
    private static bool Rfc3339(ReadOnlySpan<byte> text, ref int p, ref Fields f)
    {
        if (!Year(text, ref p, out f.Year)) return false;
        SkipWhitespace(text, ref p);
        if (!Literal(text, ref p, '-') || !Number(text, ref p, 2, 1, 12, out f.Month)) return false;
        SkipWhitespace(text, ref p);
        if (!Literal(text, ref p, '-') || !Number(text, ref p, 2, 1, 31, out f.Day)) return false;
        if (p == text.Length || text[p] is not ((byte)'T' or (byte)'t' or (byte)' ')) return false;
        p++;
        if (!Number(text, ref p, 2, 0, 23, out f.Hour)) return false;
        SkipWhitespace(text, ref p);
        if (!Literal(text, ref p, ':') || !Number(text, ref p, 2, 0, 59, out f.Minute)) return false;
        SkipWhitespace(text, ref p);
        if (!Literal(text, ref p, ':') || !Number(text, ref p, 2, 0, 60, out f.Second) || !Fraction(text, ref p, ref f)) return false;
        f.HasTime = true;
        SkipWhitespace(text, ref p);
        if (text.Length - p >= 3 && Ascii.EqualsIgnoreCase(text.Slice(p, 3), "UTC"u8))
        {
            p += 3;
            f.Offset = 0;
        }
        else if (!Offset(text, ref p, missingMinutes: false, out f.Offset)) return false;
        f.HasOffset = true;
        return true;
    }

    /// <summary>A number item: whitespace, then 1 to <paramref name="width"/> ASCII digits, within the field's range.</summary>
    private static bool Number(ReadOnlySpan<byte> text, ref int p, int width, int min, int max, out int value)
    {
        SkipWhitespace(text, ref p);
        value = 0;
        int start = p;
        while (p < text.Length && p - start < width && text[p] - (uint)'0' <= 9) value = value * 10 + text[p++] - '0';
        return p > start && value >= min && value <= max;
    }

    /// <summary>A year item: whitespace, then 1 to 4 digits, or a sign and any count of digits (fitting an i32).</summary>
    private static bool Year(ReadOnlySpan<byte> text, ref int p, out int year)
    {
        SkipWhitespace(text, ref p);
        year = 0;
        bool signed = p < text.Length && text[p] is (byte)'+' or (byte)'-';
        bool negative = signed && text[p] == '-';
        if (signed) p++;
        int start = p;
        long value = 0;
        while (p < text.Length && (signed || p - start < 4) && text[p] - (uint)'0' <= 9)
        {
            value = value * 10 + text[p++] - '0';
            if (value > (long)int.MaxValue + 1) return false; // past i64 or i32: OUT_OF_RANGE either way
        }
        if (p == start) return false;
        value = negative ? -value : value;
        if (value is < int.MinValue or > int.MaxValue) return false;
        year = (int)value;
        return true;
    }

    /// <summary>chrono's <c>%.f</c>: nothing, or '.' then 1 to 9 digits scaled to nanoseconds (further digits skipped).</summary>
    private static bool Fraction(ReadOnlySpan<byte> text, ref int p, ref Fields f)
    {
        if (p == text.Length || text[p] != '.') return true;
        p++;
        int start = p, nanos = 0;
        while (p < text.Length && p - start < 9 && text[p] - (uint)'0' <= 9) nanos = nanos * 10 + text[p++] - '0';
        if (p == start) return false;
        for (int digits = p - start; digits < 9; digits++) nanos *= 10;
        f.Nanosecond = nanos;
        while (p < text.Length && text[p] - (uint)'0' <= 9) f.ExtraNonzero |= text[p++] != '0';
        return true;
    }

    /// <summary>chrono's offset scanner: Z or z, or a sign ('+', '-', U+2212), two hour digits, any run of ':' and whitespace, two minute digits.</summary>
    private static bool Offset(ReadOnlySpan<byte> text, ref int p, bool missingMinutes, out int seconds)
    {
        seconds = 0;
        if (p == text.Length) return false;
        if (text[p] is (byte)'Z' or (byte)'z')
        {
            p++;
            return true;
        }
        bool negative;
        if (text[p] == '+') { negative = false; p++; }
        else if (text[p] == '-') { negative = true; p++; }
        else if (text[p..].StartsWith("−"u8)) { negative = true; p += 3; }
        else return false;
        if (text.Length - p < 2 || text[p] - (uint)'0' > 9 || text[p + 1] - (uint)'0' > 9) return false;
        int hours = (text[p] - '0') * 10 + text[p + 1] - '0';
        p += 2;
        while (p < text.Length)
        {
            int skip = text[p] == ':' ? 1 : WhitespaceLength(text[p..]);
            if (skip == 0) break;
            p += skip;
        }
        int minutes = 0;
        if (text.Length - p >= 2)
        {
            if (text[p] - (uint)'0' > 5 || text[p + 1] - (uint)'0' > 9) return false;
            minutes = (text[p] - '0') * 10 + text[p + 1] - '0';
            p += 2;
        }
        else if (!missingMinutes || text.Length - p == 1) return false;
        seconds = (hours * 3600 + minutes * 60) * (negative ? -1 : 1);
        return true;
    }

    private static bool Literal(ReadOnlySpan<byte> text, ref int p, char c)
    {
        if (p == text.Length || text[p] != c) return false;
        p++;
        return true;
    }

    /// <summary>Skips Unicode whitespace (Rust's <c>char::is_whitespace</c>, the same set as .NET's).</summary>
    private static void SkipWhitespace(ReadOnlySpan<byte> text, ref int p)
    {
        int n;
        while (p < text.Length && (n = WhitespaceLength(text[p..])) > 0) p += n;
    }

    private static int WhitespaceLength(ReadOnlySpan<byte> text)
    {
        byte b = text[0];
        if (b < 0x80) return b is (byte)' ' or (>= 0x09 and <= 0x0D) ? 1 : 0;
        return Rune.DecodeFromUtf8(text, out Rune rune, out int length) == System.Buffers.OperationStatus.Done && Rune.IsWhiteSpace(rune) ? length : 0;
    }

    private static int DaysInMonth(int year, int month) => month switch
    {
        2 => year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) ? 29 : 28,
        4 or 6 or 9 or 11 => 30,
        _ => 31,
    };

    /// <summary>Days since 1970-01-01 of a proleptic Gregorian date (any year).</summary>
    private static long DaysFromCivil(long year, int month, int day)
    {
        year -= month <= 2 ? 1 : 0;
        long era = (year >= 0 ? year : year - 399) / 400;
        long yearOfEra = year - era * 400;
        long dayOfYear = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
        return era * 146_097 + yearOfEra * 365 + yearOfEra / 4 - yearOfEra / 100 + dayOfYear - 719_468;
    }

    // The regex gate (the time crate's infer.rs DATETIME_DMY_RE / _YMD_RE / _YMDZ_RE, quotes left out: chrono refuses them
    // anyway), run as a backtracking program in the regex engine's priority order, since the month the first match
    // captures must be 1 to 12.
    private static readonly Inst[] DmyGate = Assemble(static a =>
    {
        a.Digits(1, 2); a.Set("-/."); a.Month(); a.Set("-/."); a.Digits(4, int.MaxValue);
        a.Optional(static a => { a.Set("T "); a.Digits(1, 2); a.Optional(":"); a.Digits(1, 2); a.Optional(static a => { a.Optional(":"); a.Digits(1, 2); a.Optional(static a => { a.Set("."); a.Digits(1, 9); }); }); });
    });
    private static readonly Inst[] YmdGate = Assemble(static a =>
    {
        a.Digits(4, int.MaxValue); a.Optional("-/."); a.Month(); a.Optional("-/."); a.Digits(1, 2);
        a.Optional(static a => { a.Set("T "); a.Digits(1, 2); a.Optional(":"); a.Digits(1, 2); a.Optional(static a => { a.Optional(":"); a.Digits(1, 2); a.Optional(static a => { a.Set("."); a.Digits(1, 9); }); }); });
    });
    private static readonly Inst[] YmdZGate = Assemble(static a =>
    {
        a.Digits(4, int.MaxValue); a.Optional("-/."); a.Month(); a.Optional("-/."); a.Digits(1, 2);
        a.Set("T "); a.Digits(2, 2); a.Optional(":"); a.Digits(2, 2);
        a.Optional(static a => { a.Optional(":"); a.Digits(2, 2); a.Optional(static a => { a.Set("."); a.Digits(1, 9); }); });
        a.Either(static a => { a.Set("+-"); a.Digits(2, 2); a.Optional(static a => { a.Optional(":"); a.Digits(2, 2); }); }, static a => a.Set("Z"));
    });

    private static bool Gate(ReadOnlySpan<byte> text, TemporalPattern group)
    {
        Inst[] program = group switch
        {
            TemporalPattern.DateDmy or TemporalPattern.DatetimeDmy => DmyGate,
            TemporalPattern.DateYmd or TemporalPattern.DatetimeYmd => YmdGate,
            _ => YmdZGate,
        };
        // No text past 35 bytes (yyyy-MM-ddTHH:mm:ss.fffffffff+hh:mm) passes both the gate and a pattern, and the
        // program recurses once per repeated digit: a long field must not reach it.
        if (text.Length > 64) return false;
        int monthStart = -1, monthEnd = -1;
        if (!Run(program, 0, text, 0, ref monthStart, ref monthEnd)) return false;
        int month = 0;
        for (int i = monthStart; i < monthEnd; i++) month = month * 10 + text[i] - '0';
        return month is >= 1 and <= 12;
    }

    private enum Op : byte { Digit, Set, Split, Jump, MonthStart, MonthEnd, End }

    private readonly record struct Inst(Op Op, int A = 0, int B = 0, string? Chars = null);

    /// <summary>Runs a program from <paramref name="pc"/>; a split tries its first branch first (greedy, as the regex engine).</summary>
    private static bool Run(Inst[] program, int pc, ReadOnlySpan<byte> text, int p, ref int monthStart, ref int monthEnd)
    {
        while (true)
        {
            Inst inst = program[pc];
            switch (inst.Op)
            {
                case Op.Digit:
                    if (p == text.Length || text[p] - (uint)'0' > 9) return false;
                    p++;
                    pc++;
                    break;
                case Op.Set:
                    if (p == text.Length || !inst.Chars!.Contains((char)text[p])) return false;
                    p++;
                    pc++;
                    break;
                case Op.Split:
                    int start = monthStart, end = monthEnd;
                    if (Run(program, inst.A, text, p, ref monthStart, ref monthEnd)) return true;
                    (monthStart, monthEnd) = (start, end);
                    pc = inst.B;
                    break;
                case Op.Jump:
                    pc = inst.A;
                    break;
                case Op.MonthStart:
                    monthStart = p;
                    pc++;
                    break;
                case Op.MonthEnd:
                    monthEnd = p;
                    pc++;
                    break;
                default:
                    return p == text.Length;
            }
        }
    }

    private static Inst[] Assemble(Action<Assembler> build)
    {
        var a = new Assembler();
        build(a);
        a.Code.Add(new Inst(Op.End));
        return [.. a.Code];
    }

    private sealed class Assembler
    {
        public List<Inst> Code { get; } = [];

        public void Set(string chars) => Code.Add(new Inst(Op.Set, Chars: chars));

        /// <summary><c>\d{min,max}</c>, greedy.</summary>
        public void Digits(int min, int max)
        {
            for (int i = 0; i < min; i++) Code.Add(new Inst(Op.Digit));
            if (max == int.MaxValue)
            {
                int loop = Code.Count;
                Code.Add(default); // split: one more digit, or out
                Code.Add(new Inst(Op.Digit));
                Code.Add(new Inst(Op.Jump, loop));
                Code[loop] = new Inst(Op.Split, loop + 1, Code.Count);
                return;
            }
            Nested(max - min);
        }

        // (?:\d(?:\d(?:…)?)?)?: the regex engine's order for a bounded repeat, longest first.
        private void Nested(int count)
        {
            if (count > 0) Optional(a => { a.Code.Add(new Inst(Op.Digit)); a.Nested(count - 1); });
        }

        /// <summary><c>(?P&lt;month&gt;[01]?\d)</c>.</summary>
        public void Month()
        {
            Code.Add(new Inst(Op.MonthStart));
            Optional("01");
            Code.Add(new Inst(Op.Digit));
            Code.Add(new Inst(Op.MonthEnd));
        }

        public void Optional(string chars) => Optional(a => a.Set(chars));

        /// <summary><c>(?:…)?</c>, greedy.</summary>
        public void Optional(Action<Assembler> body)
        {
            int split = Code.Count;
            Code.Add(default);
            body(this);
            Code[split] = new Inst(Op.Split, split + 1, Code.Count);
        }

        /// <summary><c>(?:first|second)</c>.</summary>
        public void Either(Action<Assembler> first, Action<Assembler> second)
        {
            int split = Code.Count;
            Code.Add(default);
            first(this);
            int jump = Code.Count;
            Code.Add(default);
            int other = Code.Count;
            second(this);
            Code[split] = new Inst(Op.Split, split + 1, other);
            Code[jump] = new Inst(Op.Jump, Code.Count);
        }
    }
}
