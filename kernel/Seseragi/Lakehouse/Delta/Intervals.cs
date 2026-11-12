using System.Collections.Frozen;
using System.Globalization;

namespace Kaminari.Seseragi.Lakehouse.Delta;

/// <summary>
/// The interval table properties (retentions), as Delta Spark parses and checks them: <c>[interval] N unit [N unit …]</c>
/// with units from microseconds to weeks (months and years parse but are refused), any case; a value that cannot be
/// read, is negative, or uses months or years is refused when the property is set or the table created.
/// </summary>
public static class Intervals
{
    /// <summary>The table property giving the log retention (<see cref="LogCleanup.RetentionKey"/>).</summary>
    private const string LogRetentionKey = LogCleanup.RetentionKey;

    /// <summary>The interval properties Delta defines.</summary>
    public static readonly FrozenSet<string> Keys = ((string[])
    [
        LogCleanup.RetentionKey, Checkpoints.TombstoneRetentionKey, LogCleanup.TruncateHistoryRetentionKey,
        "delta.checkpointRetentionDuration", "delta.setTransactionRetentionDuration", "delta.sampleRetentionDuration",
    ]).ToFrozenSet(StringComparer.Ordinal);

    private static readonly TimeSpan DefaultLogRetention = TimeSpan.FromDays(30);
    private static readonly TimeSpan DefaultTombstoneRetention = TimeSpan.FromDays(7);

    /// <summary>
    /// Reads an interval as Spark's <c>IntervalUtils</c> reads one: an optional <c>interval</c>, then number–unit
    /// pairs. Whole numbers only, except fractions of seconds. False when the text is not an interval.
    /// </summary>
    /// <param name="text">The text.</param>
    /// <param name="value">The interval without its months.</param>
    /// <param name="months">The months and years in it (12 a year).</param>
    public static bool TryParse(string? text, out TimeSpan value, out long months)
    {
        value = TimeSpan.Zero;
        months = 0;
        if (string.IsNullOrWhiteSpace(text)) return false;
        string[] parts = text.Trim().ToLowerInvariant().Split(' ', StringSplitOptions.RemoveEmptyEntries);
        int at = parts[0] == "interval" ? 1 : 0;
        if (at == parts.Length || (parts.Length - at) % 2 != 0) return false;
        for (; at < parts.Length; at += 2)
        {
            string number = parts[at], unit = parts[at + 1].EndsWith('s') ? parts[at + 1][..^1] : parts[at + 1];
            if (unit == "second")
            {
                if (!decimal.TryParse(number, NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out decimal seconds))
                    return false;
                value += TimeSpan.FromTicks((long)(seconds * TimeSpan.TicksPerSecond));
                continue;
            }
            if (!long.TryParse(number, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long n)) return false;
            switch (unit)
            {
                case "year": months += 12 * n; break;
                case "month": months += n; break;
                case "week": value += TimeSpan.FromDays(7 * n); break;
                case "day": value += TimeSpan.FromDays(n); break;
                case "hour": value += TimeSpan.FromHours(n); break;
                case "minute": value += TimeSpan.FromMinutes(n); break;
                case "millisecond": value += TimeSpan.FromMilliseconds(n); break;
                case "microsecond": value += TimeSpan.FromMicroseconds(n); break;
                default: return false;
            }
        }
        return true;
    }

    /// <summary>Reads a stored interval for use: false when it cannot be read or uses months (the default applies then).</summary>
    public static bool TryRead(string? text, out TimeSpan value) => TryParse(text, out value, out long months) && months == 0 && value >= TimeSpan.Zero;

    /// <summary>Refuses a value of an interval property that Spark refuses.</summary>
    /// <exception cref="DeltaException">The value is blank, not an interval, negative, or uses months or years.</exception>
    public static void Validate(string key, string value)
    {
        if (string.IsNullOrWhiteSpace(value)) throw new DeltaException($"{key}: Interval cannot be null or blank.");
        if (!TryParse(value, out TimeSpan interval, out long months)) throw new DeltaException($"{key}: {value} is not a valid INTERVAL.");
        if (months != 0 || interval < TimeSpan.Zero)
            throw new DeltaException(
                $"{key} needs to be provided as a calendar interval such as '2 weeks'. Months and years are not accepted. You may specify '365 days' for a year instead.");
    }

    /// <summary>
    /// Spark's check of the properties set together (as one <c>SET TBLPROPERTIES</c> or a table's creation): each
    /// interval readable, and the log retention not shorter than the deleted file retention, either as given or by
    /// default (30 days, 1 week). Like Spark, the table's other properties are not looked at.
    /// </summary>
    /// <param name="properties">The properties being set, by their canonical keys.</param>
    /// <exception cref="DeltaException">An interval is refused, or the two retentions do not fit.</exception>
    public static void ValidateSet(IReadOnlyDictionary<string, string> properties)
    {
        ArgumentNullException.ThrowIfNull(properties);
        foreach (var (key, value) in properties)
            if (Keys.Contains(key)) Validate(key, value);
        bool logGiven = properties.TryGetValue(LogRetentionKey, out string? log);
        TimeSpan logRetention = logGiven && TryRead(log, out TimeSpan l) ? l : DefaultLogRetention;
        TimeSpan tombstoneRetention = properties.TryGetValue(Checkpoints.TombstoneRetentionKey, out string? tombstone) && TryRead(tombstone, out TimeSpan t)
            ? t : DefaultTombstoneRetention;
        if (logRetention >= tombstoneRetention) return;
        throw new DeltaException(logGiven
            ? $"The table property {LogRetentionKey}({log}) needs to be greater than or equal to {Checkpoints.TombstoneRetentionKey}({tombstone ?? "1 week"})."
            : $"The table property {Checkpoints.TombstoneRetentionKey}({tombstone}) needs to be less than or equal to {LogRetentionKey}(30 days).");
    }
}
