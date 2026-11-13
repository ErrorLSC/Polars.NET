namespace Kaminari.Seseragi.Files.Excel;

/// <summary>What a number format makes of a number: a plain number or one of the temporal kinds.</summary>
internal enum ExcelValueKind : byte
{
    /// <summary>A plain number (also text, percentages, currencies, fractions).</summary>
    Number,

    /// <summary>A calendar date without a time of day (<c>yyyy-mm-dd</c>).</summary>
    Date,

    /// <summary>A date with a time of day (<c>m/d/yy h:mm</c>), or a locale-dependent built-in format.</summary>
    DateTime,

    /// <summary>A time of day (<c>h:mm:ss</c>).</summary>
    Time,

    /// <summary>Elapsed time (<c>[h]:mm:ss</c>).</summary>
    Duration,
}

/// <summary>
/// Classifies number formats. Only the first section (before an unquoted <c>;</c>) counts. Quoted literals,
/// <c>\</c> escapes, the character after <c>_</c> (width) and <c>*</c> (fill), and bracketed parts (colours,
/// conditions, locales) are skipped, except the elapsed-time brackets <c>[h]</c>, <c>[m]</c>, <c>[s]</c>, which make
/// a duration. <c>m</c> means minutes right after an hour token or right before a seconds token, months otherwise.
/// </summary>
internal static class ExcelNumberFormat
{
    /// <summary>The kind of a built-in format id (one that styles.xml need not define).</summary>
    public static ExcelValueKind BuiltIn(int id) => id switch
    {
        14 or 15 or 16 or 17 => ExcelValueKind.Date,              // mm-dd-yy, d-mmm-yy, d-mmm, mmm-yy
        18 or 19 or 20 or 21 or 45 or 47 => ExcelValueKind.Time,  // h:mm AM/PM … h:mm:ss, mm:ss, mmss.0
        22 => ExcelValueKind.DateTime,                            // m/d/yy h:mm
        46 => ExcelValueKind.Duration,                            // [h]:mm:ss
        // East Asian built-in ids depend on the workbook's locale and hold dates in some, times in others.
        >= 27 and <= 36 or >= 50 and <= 58 => ExcelValueKind.DateTime,
        _ => ExcelValueKind.Number,
    };

    /// <summary>The kind of a format code.</summary>
    public static ExcelValueKind Classify(ReadOnlySpan<char> format)
    {
        bool date = false, time = false;
        char last = '\0'; // the last date/time token: y, d, M (month), h, n (minute), s
        bool pendingM = false; // an 'm' whose meaning depends on the next token
        for (int i = 0; i < format.Length; i++)
        {
            char c = format[i];
            switch (c)
            {
                case ';':
                    i = format.Length; // first section only
                    continue;
                case '"':
                    int close = format[(i + 1)..].IndexOf('"');
                    i = close < 0 ? format.Length : i + 1 + close;
                    continue;
                case '\\' or '_' or '*':
                    i++;
                    continue;
                case '[':
                    int end = format[(i + 1)..].IndexOf(']');
                    var inside = end < 0 ? format[(i + 1)..] : format.Slice(i + 1, end);
                    if (IsElapsed(inside)) return ExcelValueKind.Duration;
                    i = end < 0 ? format.Length : i + 1 + end;
                    continue;
            }
            char t = char.ToLowerInvariant(c);
            switch (t)
            {
                case 'y' or 'd':
                    ResolvePending(false);
                    date = true;
                    last = t;
                    break;
                case 'h':
                    ResolvePending(false);
                    time = true;
                    last = 'h';
                    break;
                case 's':
                    ResolvePending(true);
                    time = true;
                    last = 's';
                    break;
                case 'm':
                    if (last == 'h')
                    {
                        time = true;
                        last = 'n';
                    }
                    else if (!pendingM)
                    {
                        pendingM = true;
                    }
                    while (i + 1 < format.Length && char.ToLowerInvariant(format[i + 1]) == 'm') i++;
                    break;
                case 'a' when Matches(format, i, "am/pm") || Matches(format, i, "a/p"):
                    ResolvePending(false);
                    time = true;
                    i += Matches(format, i, "am/pm") ? 4 : 2;
                    break;
            }
        }
        ResolvePending(false);
        return (date, time) switch
        {
            (true, true) => ExcelValueKind.DateTime,
            (true, false) => ExcelValueKind.Date,
            (false, true) => ExcelValueKind.Time,
            _ => ExcelValueKind.Number,
        };

        void ResolvePending(bool beforeSeconds)
        {
            if (!pendingM) return;
            pendingM = false;
            if (beforeSeconds) time = true;
            else date = true;
            last = beforeSeconds ? 'n' : 'M';
        }
    }

    // [h], [hh], [m], [mm], [s], [ss]: one letter repeated.
    private static bool IsElapsed(ReadOnlySpan<char> inside)
    {
        if (inside.IsEmpty) return false;
        char c = char.ToLowerInvariant(inside[0]);
        if (c is not ('h' or 'm' or 's')) return false;
        foreach (char x in inside)
            if (char.ToLowerInvariant(x) != c) return false;
        return true;
    }

    private static bool Matches(ReadOnlySpan<char> format, int i, string token) =>
        format.Length - i >= token.Length && format.Slice(i, token.Length).Equals(token, StringComparison.OrdinalIgnoreCase);
}
