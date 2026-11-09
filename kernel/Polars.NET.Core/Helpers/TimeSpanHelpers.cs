using System.Text;

namespace Polars.NET.Core.Helpers;

public static class DurationFormatter
{
    /// <summary>
    /// Format C# Duration to Polars duration string
    /// </summary>
    public static string ToPolarsString(TimeSpan ts)
    {
        if (ts == TimeSpan.Zero) return "0s";

        var sb = new StringBuilder();

        // A negative duration is one leading minus sign over the whole string. Every component of a negative
        // TimeSpan is zero or negative.
        if (ts < TimeSpan.Zero) sb.Append('-');

        // Polars suffixs: ns, us, ms, s, m, h, d, w

        // 1. Days (d)
        if (ts.Days != 0) sb.Append($"{Math.Abs(ts.Days)}d");

        // 2. Hours (h)
        if (ts.Hours != 0) sb.Append($"{Math.Abs(ts.Hours)}h");

        // 3. Minutes (m)
        if (ts.Minutes != 0) sb.Append($"{Math.Abs(ts.Minutes)}m");

        // 4. Seconds (s)
        if (ts.Seconds != 0) sb.Append($"{Math.Abs(ts.Seconds)}s");

        // 5. Milliseconds (ms)
        if (ts.Milliseconds != 0) sb.Append($"{Math.Abs(ts.Milliseconds)}ms");

        // 6. Microseconds (us)
        if (ts.Microseconds != 0) sb.Append($"{Math.Abs(ts.Microseconds)}us");

        // 7. Nanoseconds (ns)
        if (ts.Nanoseconds != 0) sb.Append($"{Math.Abs(ts.Nanoseconds)}ns");

        return sb.ToString();
    }

    [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull(nameof(ts))]
    public static string? ToPolarsString(TimeSpan? ts)
    {
        return ts.HasValue ? ToPolarsString(ts.Value) : null;
    }
}

public static class PolarsExtensions
{
    // ts.ToPolarsDuration()
    public static string ToPolarsDuration(this TimeSpan ts)
        => DurationFormatter.ToPolarsString(ts);

    // tsNullable.ToPolarsDuration()
    public static string? ToPolarsDuration(this TimeSpan? ts)
        => DurationFormatter.ToPolarsString(ts);
}