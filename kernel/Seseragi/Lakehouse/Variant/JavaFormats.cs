using System.Buffers;
using System.Globalization;
using System.Text;

namespace Kaminari.Seseragi.Lakehouse.Variant;

/// <summary>
/// Doubles and floats as Java's <c>Double.toString</c> / <c>Float.toString</c> print them (JDK 19 and later): the
/// shortest decimal of at least two digits that rounds to the value, the closest when several do.
/// </summary>
internal static class JavaNumbers
{
    private static readonly SearchValues<char> Exponent = SearchValues.Create("Ee");

    public static string Double(double d) => double.IsFinite(d)
        ? Format(TwoDigitsAtLeast(d.ToString("R", CultureInfo.InvariantCulture), d.ToString("E1", CultureInfo.InvariantCulture)), d == 0 && double.IsNegative(d))
        : NonFinite(double.IsNaN(d), double.IsPositiveInfinity(d));

    public static string Float(float f) => float.IsFinite(f)
        ? Format(TwoDigitsAtLeast(f.ToString("R", CultureInfo.InvariantCulture), f.ToString("E1", CultureInfo.InvariantCulture)), f == 0 && float.IsNegative(f))
        : NonFinite(float.IsNaN(f), float.IsPositiveInfinity(f));

    /// <summary>
    /// The shortest digits .NET found, or, when that is a single digit, the value rounded to two digits (Java takes the
    /// closest two-digit decimal then: 4.9E-324, not 5.0E-324; it rounds to the value as the one digit did).
    /// </summary>
    private static string TwoDigitsAtLeast(string shortest, string twoDigits) => SignificantDigits(shortest) <= 1 ? twoDigits : shortest;

    /// <summary>The significant digits of a number's text: those of the mantissa, zeros around them left out.</summary>
    private static int SignificantDigits(string text)
    {
        int e = text.AsSpan().IndexOfAny(Exponent);
        ReadOnlySpan<char> mantissa = e >= 0 ? text.AsSpan(0, e) : text;
        Span<char> digits = stackalloc char[mantissa.Length];
        int count = 0;
        foreach (char c in mantissa)
        {
            if (char.IsAsciiDigit(c)) digits[count++] = c;
        }
        return digits[..count].Trim('0').Length;
    }

    private static string NonFinite(bool nan, bool positive) => nan ? "NaN" : positive ? "Infinity" : "-Infinity";

    /// <summary>
    /// Java's layout of the shortest digits .NET found ("R"): plain with at least one fractional digit when
    /// 10^-3 &lt;= |x| &lt; 10^7, else one digit, a point, the rest (at least one digit) and <c>E</c> with the exponent.
    /// </summary>
    private static string Format(string shortest, bool negativeZero)
    {
        bool negative = shortest.StartsWith('-') || negativeZero;
        string text = shortest.TrimStart('-');
        int exponent = 0;
        int e = text.AsSpan().IndexOfAny(Exponent);
        if (e >= 0)
        {
            exponent = int.Parse(text[(e + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
            text = text[..e];
        }
        int point = text.IndexOf('.');
        string integer = point < 0 ? text : text[..point], fraction = point < 0 ? "" : text[(point + 1)..];
        // All digits, and where the point goes in them.
        string digits = (integer + fraction).TrimStart('0');
        if (digits.Length == 0) return negative ? "-0.0" : "0.0";
        int leadingZeros = (integer + fraction).Length - digits.Length;
        int scientific = integer.Length - leadingZeros - 1 + exponent; // the value is d.ddd × 10^scientific
        digits = digits.TrimEnd('0');

        var result = new StringBuilder(digits.Length + 8);
        if (negative) result.Append('-');
        if (scientific >= -3 && scientific < 7)
        {
            if (scientific >= 0)
            {
                string whole = digits.Length > scientific + 1 ? digits[..(scientific + 1)] : digits.PadRight(scientific + 1, '0');
                string rest = digits.Length > scientific + 1 ? digits[(scientific + 1)..] : "0";
                result.Append(whole).Append('.').Append(rest);
            }
            else
            {
                result.Append("0.").Append('0', -scientific - 1).Append(digits);
            }
        }
        else
        {
            result.Append(digits[0]).Append('.').Append(digits.Length > 1 ? digits[1..] : "0").Append('E').Append(scientific);
        }
        return result.ToString();
    }
}

/// <summary>Dates and times as Java's <c>LocalDate.toString</c> and ISO local date-time formats print them, any year.</summary>
internal static class JavaTime
{
    private const long MicrosPerDay = 86_400_000_000;

    /// <summary>A date (days since 1970-01-01) as <c>yyyy-MM-dd</c>; years beyond 9999 with a plus sign.</summary>
    public static string Date(long days)
    {
        var (year, month, day) = Civil(days);
        string y = year switch
        {
            > 9999 => "+" + year.ToString(CultureInfo.InvariantCulture),
            < 0 => "-" + (-year).ToString("D4", CultureInfo.InvariantCulture),
            _ => year.ToString("D4", CultureInfo.InvariantCulture),
        };
        return $"{y}-{month:D2}-{day:D2}";
    }

    /// <summary>
    /// Microseconds since the epoch as <c>yyyy-MM-dd HH:mm:ss</c> and, when not zero, the fraction of the second
    /// without trailing zeros.
    /// </summary>
    public static string DateTime(long micros)
    {
        long days = Math.DivRem(micros, MicrosPerDay, out long ofDay);
        if (ofDay < 0)
        {
            days--;
            ofDay += MicrosPerDay;
        }
        long seconds = ofDay / 1_000_000, fraction = ofDay % 1_000_000;
        string time = string.Create(CultureInfo.InvariantCulture, $"{seconds / 3600:D2}:{seconds / 60 % 60:D2}:{seconds % 60:D2}");
        if (fraction != 0) time += "." + fraction.ToString("D6", CultureInfo.InvariantCulture).TrimEnd('0');
        return Date(days) + " " + time;
    }

    /// <summary>The proleptic Gregorian date of a day number (days since 1970-01-01).</summary>
    private static (long Year, int Month, int Day) Civil(long days)
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
        long year = yoe + era * 400 + (month <= 2 ? 1 : 0);
        return (year, month, day);
    }
}
