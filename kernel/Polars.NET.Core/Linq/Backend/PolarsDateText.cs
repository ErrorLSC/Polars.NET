using System.Globalization;
using System.Text;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>The engine's text of dates and times (strftime, chrono) for .NET custom formats (ScalarFunction.DtToText).</summary>
internal static class PolarsDateText
{
    /// <summary>
    /// A .NET custom date and time format of the culture named <paramref name="cultureName"/> as a strftime pattern,
    /// or null where the engine cannot write it as .NET does: a calendar other than the Gregorian one (th-TH's Buddhist
    /// era, fa-IR's Persian calendar), names (months, days, AM/PM) other than the invariant culture's English ones,
    /// fractions of a second, eras, time zones, one-letter years.
    /// </summary>
    public static string? TryStrftime(string format, string cultureName)
    {
        var dtfi = CultureInfo.GetCultureInfo(cultureName).DateTimeFormat;
        if (dtfi.Calendar is not GregorianCalendar) return null;
        var english = CultureInfo.InvariantCulture.DateTimeFormat;
        bool englishMonths = dtfi.MonthNames.SequenceEqual(english.MonthNames) && dtfi.AbbreviatedMonthNames.SequenceEqual(english.AbbreviatedMonthNames);
        bool englishDays = dtfi.DayNames.SequenceEqual(english.DayNames) && dtfi.AbbreviatedDayNames.SequenceEqual(english.AbbreviatedDayNames);
        bool englishAmPm = dtfi.AMDesignator == "AM" && dtfi.PMDesignator == "PM";
        var output = new StringBuilder();
        void Literal(string text) => output.Append(text.Replace("%", "%%"));
        int i = 0;
        while (i < format.Length)
        {
            char c = format[i];
            int run = 1;
            while (i + run < format.Length && format[i + run] == c) run++;
            string? specifier = (c, run) switch
            {
                ('y', 2) => "%y",
                ('y', 4) => "%Y",
                ('M', 1) => "%-m",
                ('M', 2) => "%m",
                ('M', 3) when englishMonths => "%b",
                ('M', 4) when englishMonths => "%B",
                ('d', 1) => "%-d",
                ('d', 2) => "%d",
                ('d', 3) when englishDays => "%a",
                ('d', 4) when englishDays => "%A",
                ('H', 1) => "%-H",
                ('H', 2) => "%H",
                ('h', 1) => "%-I",
                ('h', 2) => "%I",
                ('m', 1) => "%-M",
                ('m', 2) => "%M",
                ('s', 1) => "%-S",
                ('s', 2) => "%S",
                ('t', 2) when englishAmPm => "%p",
                _ => null,
            };
            if (specifier is not null)
            {
                output.Append(specifier);
                i += run;
                continue;
            }
            switch (c)
            {
                case 'y' or 'M' or 'd' or 'H' or 'h' or 'm' or 's' or 't' or 'f' or 'F' or 'g' or 'z' or 'K':
                    return null;
                case '/':
                    Literal(string.Concat(Enumerable.Repeat(dtfi.DateSeparator, run)));
                    i += run;
                    break;
                case ':':
                    Literal(string.Concat(Enumerable.Repeat(dtfi.TimeSeparator, run)));
                    i += run;
                    break;
                case '\'' or '"':
                    int close = format.IndexOf(c, i + 1);
                    if (close == -1) return null;
                    Literal(format.Substring(i + 1, close - i - 1));
                    i = close + 1;
                    break;
                case '\\' when i + 1 < format.Length:
                    Literal(format[i + 1].ToString());
                    i += 2;
                    break;
                default:
                    Literal(new string(c, run));
                    i += run;
                    break;
            }
        }
        return output.ToString();
    }
}
