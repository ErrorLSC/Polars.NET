namespace Polars.CSharp;

/// <summary>
/// Date Ops Namespace
/// </summary>
public readonly struct SeriesDtOps
{
    private readonly Series _series;
    internal SeriesDtOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
    {
        var expr = op(Polars.Col(_series.Name));
        
        return _series.ApplyExpr(expr);
    }
    /// <summary>Get the year from the underlying date/datetime.</summary>
    public Series Year() => Apply(e => e.Dt.Year());
    /// <summary>Get the quarter from the underlying date/datetime.</summary>
    public Series Quarter() => Apply(e => e.Dt.Quarter());
    /// <summary>Get the month from the underlying date/datetime.</summary>
    public Series Month() => Apply(e => e.Dt.Month());
    /// <summary>Get the day from the underlying date/datetime.</summary>
    public Series Day() => Apply(e => e.Dt.Day());
    /// <summary>Get the ordinal day (day of year) from the underlying date/datetime.</summary>
    public Series OrdinalDay() => Apply(e => e.Dt.OrdinalDay());
    /// <summary>Get the weekday from the underlying date/datetime.</summary>
    public Series WeekDay() => Apply(e => e.Dt.Weekday());
    /// <summary>Get the hour from the underlying datetime.</summary>
    public Series Hour() => Apply(e => e.Dt.Hour());
    /// <summary>Get the minute from the underlying datetime.</summary>
    public Series Minute() => Apply(e => e.Dt.Minute());
    /// <summary>Get the second from the underlying datetime.</summary>
    public Series Second() => Apply(e => e.Dt.Second());
    /// <summary>Get the millisecond from the underlying datetime.</summary>
    public Series Millisecond() => Apply(e => e.Dt.Millisecond());
    /// <summary>Get the microsecond from the underlying datetime.</summary>
    public Series Microsecond() => Apply(e => e.Dt.Microsecond());
    /// <summary>Get the nanosecond from the underlying datetime.</summary>
    public Series Nanosecond() => Apply(e => e.Dt.Nanosecond());

    /// <summary>
    /// Cast to Date (remove time component).
    /// </summary>
    /// <returns></returns>
    public Series Date() => Apply(e => e.Dt.Date());
    /// <summary>
    /// Cast to Time (remove Date component).
    /// </summary>
    /// <returns></returns>
    public Series Time() => Apply(e => e.Dt.Time());
    // ==========================================
    // Truncate & Round
    // ==========================================

    /// <summary>
    /// Truncate the datetimes to the given interval (e.g. "1d", "1h", "15m").
    /// </summary>
    public Series Truncate(string every) => Apply(e => e.Dt.Truncate(every));
    /// <summary>
    /// Truncate the datetimes to the given timespan
    /// </summary>
    /// <param name="every"></param>
    /// <returns></returns>
    public Series Truncate(TimeSpan every) => Apply(e => e.Dt.Truncate(every));
    /// <summary>
    /// Round the datetimes to the given interval.
    /// </summary>
    public Series Round(string every) => Apply(e => e.Dt.Round(every));
    /// <summary>
    /// Round the datetimes to the given timespan interval.
    /// </summary>
    /// <param name="every"></param>
    /// <returns></returns>
    public Series Round(TimeSpan every) => Apply(e => e.Dt.Round(every));
    // ==========================================
    // Offset
    // ==========================================

    /// <summary>
    /// Offset the datetimes by a given duration expression.
    /// </summary>
    public Series OffsetBy(Expr by) => Apply(e => e.Dt.OffsetBy(by));
    /// <summary>
    /// Offset the datetimes by a constant duration string (e.g., "1d", "-2h").
    /// </summary>
    public Series OffsetBy(string duration) => Apply(e => e.Dt.OffsetBy(duration));
    /// <summary>
    /// Offset the datetimes by TimeSpan
    /// </summary>
    /// <param name="duration"></param>
    /// <returns></returns>
    public Series OffsetBy(TimeSpan duration) => Apply(e => e.Dt.OffsetBy(duration));

    // ==========================================
    // Timestamp
    // ==========================================

    /// <summary>
    /// Convert the datetime to an integer timestamp (Unix epoch).
    /// </summary>
    /// <param name="timeUnit">
    /// The desired TimeUnit for the resulting Datetime.
    /// <para><b>Note:</b> Only sub-second units (<see cref="TimeUnit.Nanoseconds"/>, <see cref="TimeUnit.Microseconds"/>, <see cref="TimeUnit.Milliseconds"/>) are supported.</para>
    /// </param>
    public Series Timestamp(TimeUnit timeUnit = TimeUnit.Microseconds) => Apply(e => e.Dt.Timestamp(timeUnit));
    /// <summary>
    /// Combine the date from the underlying date/datetime with the time from another expression.
    /// <para>The resulting Series will have the specified TimeUnit.</para>
    /// </summary>
    /// <param name="time">An expression yielding the Time component.</param>
    /// <param name="timeUnit">
    /// The desired TimeUnit for the resulting Datetime.
    /// <para><b>Note:</b> Only sub-second units (<see cref="TimeUnit.Nanoseconds"/>, <see cref="TimeUnit.Microseconds"/>, <see cref="TimeUnit.Milliseconds"/>) are supported.</para>
    /// </param>
    public Series Combine(Expr time,TimeUnit timeUnit) => Apply(e => e.Dt.Combine(time,timeUnit));

    // ==========================================
    // TimeZone
    // ==========================================
    /// <summary>
    /// Convert from one timezone to another.
    /// Resulting Series will have the given time zone.
    /// </summary>
    /// <param name="tz">Target time zone (e.g. "Asia/Shanghai")</param>    
    public Series ConvertTimeZone(string tz) => Apply(e => e.Dt.ConvertTimeZone(tz));

    /// <summary>
    /// Replace the time zone of a Series.
    /// This does not change the underlying timestamp, only the metadata.
    /// </summary>
    public Series ReplaceTimeZone(string? timeZone, string? ambiguous = null, string? nonExistent = "raise")
         =>Apply(e => e.Dt.ReplaceTimeZone(timeZone,ambiguous,nonExistent));
    // ==========================================
    // BusinessDays
    // ==========================================
    /// <summary>
    /// Add business days to the date column.
    /// </summary>
    /// <param name="n">Number of business days to add (can be negative).</param>
    /// <param name="holidays">List of holidays (dates to skip).</param>
    /// <param name="weekMask">
    /// Array of 7 bools indicating business days, starting from Monday. 
    /// Default is Mon-Fri.
    /// </param>
    /// <param name="roll">Strategy for handling non-business days.</param>
    public Series AddBusinessDays(
        int n, 
        IEnumerable<DateOnly>? holidays = null, 
        bool[]? weekMask = null, 
        Roll roll = Roll.Raise)
        =>Apply(e => e.Dt.AddBusinessDays(n,holidays,weekMask,roll));
    /// <summary>
    /// Add business days to the date column.
    /// </summary>
    public Series AddBusinessDays(
        Expr n, 
        IEnumerable<DateOnly>? holidays = null, 
        bool[]? weekMask = null, 
        Roll roll = Roll.Raise)
        =>Apply(e => e.Dt.AddBusinessDays(n,holidays,weekMask,roll));
    /// <summary>
    /// Check if the date is a business day.
    /// </summary>
    public Series IsBusinessDay(IEnumerable<DateOnly>? holidays = null, bool[]? weekMask = null)
        =>Apply(e => e.Dt.IsBusinessDay(holidays,weekMask));
}