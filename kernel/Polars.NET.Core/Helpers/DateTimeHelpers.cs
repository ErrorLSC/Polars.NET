using System.Text.RegularExpressions;

namespace Polars.NET.Core.Helpers;

public static partial class DateTimeFormatHelper
{
    // Regex matching .NET date/time format tokens
    // Ordered from longest to shortest to avoid premature partial replacements
    private static readonly Regex FormatTokenRegex = ChronoFormatRegex();

    /// <summary>
    /// Translates .NET DateTime format specifiers to Rust Chrono strftime specifiers.
    /// Supports standard .NET tokens as well as common uppercase SQL/ISO aliases (e.g., YYYY-MM-DD).
    /// </summary>
    public static string? ToChronoFormat(string? format)
    {
        if (format is null)
            return null;

        return format switch
        {
            "o" or "O" or "s" => "%Y-%m-%dT%H:%M:%S",
            "d" => "%m/%d/%Y",
            "D" => "%A, %B %d, %Y",
            "t" => "%H:%M",
            "T" => "%H:%M:%S",
            _ => FormatTokenRegex.Replace(format, match => match.Value switch
            {
                // Year
                "yyyy" or "YYYY" => "%Y",
                "yy" or "YY" => "%y",

                // Month
                "MMMM" => "%B",
                "MMM" => "%b",
                "MM" => "%m",

                // Day
                "dddd" => "%A",
                "ddd" => "%a",
                "dd" or "DD" => "%d",

                // Hour
                "HH" => "%H",
                "hh" => "%I",

                // Minute & Second
                "mm" => "%M",
                "ss" => "%S",

                // Fractional seconds
                "ffffff" => "%6f",
                "fff" => "%3f",

                _ => match.Value
            })
        };
    }

    [GeneratedRegex(@"(yyyy|YYYY|yy|YY|MMMM|MMM|MM|dddd|ddd|dd|DD|HH|hh|mm|ss|ffffff|fff)", RegexOptions.Compiled)]
    private static partial Regex ChronoFormatRegex();
}
