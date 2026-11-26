namespace Polars.NET.Core.Delta;

/// <summary>
/// Process-wide settings of the Delta kernel, as a Spark session holds them. The public Delta API exposes them.
/// </summary>
internal static class DeltaSession
{
    private static volatile string _timeZone = "UTC";

    /// <summary>
    /// The session time zone (Spark's <c>spark.sql.session.timeZone</c>): the zone the SQL expressions of CHECK
    /// constraints, generated columns and column defaults read instants in (<c>CAST(ts AS DATE)</c>, <c>hour(ts)</c>,
    /// <c>current_date()</c>). An IANA name; UTC by default. Each write reads it once, when it binds the table's
    /// expressions.
    /// </summary>
    /// <exception cref="ArgumentException">The name is not an IANA time zone this machine knows.</exception>
    public static string TimeZone
    {
        get => _timeZone;
        set
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(value);
            if (!TimeZoneInfo.TryFindSystemTimeZoneById(value, out TimeZoneInfo? zone) || !(zone.HasIanaId || value == "UTC"))
                throw new ArgumentException($"Unknown time zone '{value}': give an IANA name such as UTC or America/Los_Angeles.", nameof(value));
            _timeZone = value;
        }
    }
}
