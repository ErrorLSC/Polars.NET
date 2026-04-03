using Polars.NET.Core;
using Polars.NET.Core.Helpers;

namespace Polars.CSharp;

// ==========================================
// DtOps Helper Class
// ==========================================

/// <summary>
/// Contains methods for temporal (Date/Time) operations.
/// Access this via <see cref="Expr.Dt"/>.
/// </summary>
public readonly struct DtOps
{
    private readonly Expr _expr;

    internal DtOps(Expr expr)
    {
        _expr = expr;
    }

    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
        => new(op(_expr.CloneHandle()));

    /// <summary>Get the year from the underlying date/datetime.</summary>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     dt = new[] { new DateTime(2024, 1, 1, 10, 15, 30) } 
    /// });
    /// 
    /// df.Select(
    ///     Col("dt").Dt.Year(),    // 2024
    ///     Col("dt").Dt.Month(),   // 1
    ///     Col("dt").Dt.Weekday(), // 1 (Monday)
    ///     Col("dt").Dt.ToString("%Y-%m-%d") // "2024-01-01"
    /// ).Show();
    /// </code>
    /// </example>
    public Expr Year() => Wrap(PolarsWrapper.DtYear);

    /// <summary>Get the quarter from the underlying date/datetime.</summary>
    public Expr Quarter() => Wrap(PolarsWrapper.DtQuarter);

    /// <summary>Get the month from the underlying date/datetime.</summary>
    public Expr Month() => Wrap(PolarsWrapper.DtMonth);

    /// <summary>Get the day from the underlying date/datetime.</summary>
    public Expr Day() => Wrap(PolarsWrapper.DtDay);

    /// <summary>Get the ordinal day (day of year) from the underlying date/datetime.</summary>
    public Expr OrdinalDay() => Wrap(PolarsWrapper.DtOrdinalDay);

    /// <summary>Get the weekday from the underlying date/datetime.</summary>
    public Expr Weekday() => Wrap(PolarsWrapper.DtWeekday);

    /// <summary>Get the hour from the underlying datetime.</summary>
    public Expr Hour() => Wrap(PolarsWrapper.DtHour);

    /// <summary>Get the minute from the underlying datetime.</summary>
    public Expr Minute() => Wrap(PolarsWrapper.DtMinute);

    /// <summary>Get the second from the underlying datetime.</summary>
    public Expr Second() => Wrap(PolarsWrapper.DtSecond);

    /// <summary>Get the millisecond from the underlying datetime.</summary>
    public Expr Millisecond() => Wrap(PolarsWrapper.DtMillisecond);

    /// <summary>Get the microsecond from the underlying datetime.</summary>
    public Expr Microsecond() => Wrap(PolarsWrapper.DtMicrosecond);

    /// <summary>Get the nanosecond from the underlying datetime.</summary>
    public Expr Nanosecond() => Wrap(PolarsWrapper.DtNanosecond);

    /// <summary>
    /// Format the date/datetime as a string using the given format string.
    /// <para>Format codes follow the Rust `chrono` crate syntax (similar to strftime).</para>
    /// </summary>
    public Expr ToString(string format)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtToString(h, format));
    }
    /// <summary>
    /// Alias for <see cref="ToString(string)"/>.
    /// </summary>
    public Expr Strftime(string format)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtToString(h, format));
    }

    /// <summary>
    /// Format the date/datetime as a string using the default format "%Y-%m-%dT%H:%M:%S%.f".
    /// </summary>
    public override string ToString() => "DtOps";
    /// <summary>
    /// Cast to Date (remove time component).
    /// </summary>
    /// <returns></returns>
    public Expr Date() => Wrap(PolarsWrapper.DtDate);
    /// <summary>
    /// Cast to Time (remove Date component).
    /// </summary>
    /// <returns></returns>
    public Expr Time() => Wrap(PolarsWrapper.DtTime);

    // ==========================================
    // Truncate & Round
    // ==========================================

    /// <summary>       
    /// Truncate the datetimes to the given interval (e.g. "1d", "1h", "15m").
    /// <para>This behaves like a "floor" operation for time.</para>
    /// </summary>
    /// <example>
    /// <code>
    /// // Input: 2024-02-29 23:59:59
    /// df.Select(
    ///     Col("dt").Dt.Truncate("1h"), // Result: 23:00:00
    ///     Col("dt").Dt.Round("1h")     // Result: 2024-03-01 00:00:00
    /// );
    /// </code>
    /// </example>
    public Expr Truncate(IntoDuration every)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtTruncate(h, every.Value));
    }
    /// <summary>
    /// Round the datetimes to the given interval.
    /// </summary>
    public Expr Round(IntoDuration every)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtRound(h, every.Value));
    }
    // ==========================================
    // Offset
    // ==========================================

    /// <summary>
    /// Offset the datetimes by a given duration expression.
    /// </summary>
    public Expr OffsetBy(Expr by) => new(PolarsWrapper.DtOffsetBy(_expr.Handle, by.Handle));
    /// <summary>
    /// Offset the datetimes by a constant duration string (e.g., "1d", "-2h").
    /// </summary>
    /// <example>
    /// <code>
    /// df.Select(
    ///     // Add 2 days
    ///     Col("dt").Dt.OffsetBy("2d"),
    ///     // Subtract 1 hour
    ///     Col("dt").Dt.OffsetBy("-1h")
    /// );
    /// </code>
    /// </example>
    public Expr OffsetBy(string duration) => OffsetBy(Polars.Lit(duration));
    /// <summary>
    /// Offset the datetimes by TimeSpan
    /// </summary>
    /// <param name="duration"></param>
    /// <returns></returns>
    public Expr OffsetBy(TimeSpan duration)
    {
        string durationStr = DurationFormatter.ToPolarsString(duration);
        return OffsetBy(Polars.Lit(durationStr));
    }

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
    public Expr Timestamp(TimeUnit timeUnit = TimeUnit.Microseconds)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtTimestamp(h, timeUnit.ToNative()));
    }
    /// <summary>
    /// Combine the date from the underlying date/datetime with the time from another expression.
    /// <para>The resulting Series will have the specified TimeUnit.</para>
    /// </summary>
    /// <param name="time">An expression yielding the Time component.</param>
    /// <param name="timeUnit">
    /// The desired TimeUnit for the resulting Datetime.
    /// <para><b>Note:</b> Only sub-second units (<see cref="TimeUnit.Nanoseconds"/>, <see cref="TimeUnit.Microseconds"/>, <see cref="TimeUnit.Milliseconds"/>) are supported.</para>
    /// </param>
    public Expr Combine(Expr time, TimeUnit timeUnit = TimeUnit.Microseconds)
    {

        var hExpr = PolarsWrapper.CloneExpr(_expr.Handle);
        var hTime = PolarsWrapper.CloneExpr(time.Handle);
        
        return new Expr(PolarsWrapper.DtCombine(hExpr, hTime, timeUnit.ToNative()));
    }
    // ==========================================
    // TimeZone
    // ==========================================
    /// <summary>
    /// Convert from one timezone to another.
    /// <para>
    /// This changes the physical time (wall clock time) to match the target timezone.
    /// The input Series must already have a timezone assigned (e.g. via <see cref="ReplaceTimeZone"/>).
    /// </para>
    /// </summary>
    /// <param name="timeZone">Target time zone string (IANA database, e.g. "Asia/Shanghai", "America/New_York").</param>
    /// <returns>A new expression with the converted time.</returns>
    /// <example>
    /// <code>
    /// // 1. Start with a naive datetime (noon)
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     dt = new[] { new DateTime(2024, 1, 1, 12, 0, 0) } 
    /// });
    /// 
    /// // 2. Operations:
    /// // - ReplaceTimeZone("UTC"): Define metadata (it is now UTC noon)
    /// // - ConvertTimeZone("Asia/Shanghai"): Shift to +8 hours (20:00)
    /// // - ConvertTimeZone("America/New_York"): Shift to -5 hours (07:00)
    /// df.Select(
    ///     Col("dt").Alias("naive"),
    ///     
    ///     Col("dt").Dt.ReplaceTimeZone("UTC").Alias("utc_tagged"),
    ///     
    ///     Col("dt").Dt.ReplaceTimeZone("UTC")
    ///               .Dt.ConvertTimeZone("Asia/Shanghai")
    ///               .Alias("shanghai_time"),
    /// 
    ///     Col("dt").Dt.ReplaceTimeZone("UTC")
    ///               .Dt.ConvertTimeZone("America/New_York")
    ///               .Alias("ny_time")
    /// ).Show();
    /// /* Output:
    /// shape: (1, 4)
    /// ┌─────────────────────┬─────────────────────────┬─────────────────────────────┬────────────────────────────────┐
    /// │ naive               ┆ utc_tagged              ┆ shanghai_time               ┆ ny_time                        │
    /// │ ---                 ┆ ---                     ┆ ---                         ┆ ---                            │
    /// │ datetime[μs]        ┆ datetime[μs, UTC]       ┆ datetime[μs, Asia/Shanghai] ┆ datetime[μs, America/New_York] │
    /// ╞═════════════════════╪═════════════════════════╪═════════════════════════════╪════════════════════════════════╡
    /// │ 2024-01-01 12:00:00 ┆ 2024-01-01 12:00:00 UTC ┆ 2024-01-01 20:00:00 CST     ┆ 2024-01-01 07:00:00 EST        │
    /// └─────────────────────┴─────────────────────────┴─────────────────────────────┴────────────────────────────────┘
    /// */
    /// </code>
    /// </example>
    public Expr ConvertTimeZone(string timeZone)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtConvertTimeZone(h, timeZone));
    }
    /// <summary>
    /// Replace the time zone of a Series.
    /// <para>
    /// This sets the time zone metadata without changing the underlying physical time (wall clock).
    /// Use this to assign a timezone to a naive datetime.
    /// </para>
    /// </summary>
    /// <param name="timeZone">The time zone to assign (e.g. "UTC", "Asia/Shanghai"). If null, removes timezone info.</param>
    /// <param name="ambiguous">How to handle ambiguous times (e.g. DST transitions). Default "raise".</param>
    /// <param name="nonExistent">How to handle non-existent times. Default "raise".</param>
    /// <seealso cref="ConvertTimeZone(string)"/>
    public Expr ReplaceTimeZone(string? timeZone, string? ambiguous = null, string? nonExistent = "raise")
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtReplaceTimeZone(h, timeZone, ambiguous, nonExistent));
    }
    // ==========================================
    // BusinessDays
    // ==========================================
    private static readonly bool[] DefaultWeekMask = [true, true, true, true, true, false, false];

    /// <summary>
    /// Add business days to the date column.
    /// <para>
    /// Automatically skips weekends (by default) and specified holidays.
    /// </para>
    /// </summary>
    /// <param name="n">Number of business days to add (can be negative).</param>
    /// <param name="holidays">List of holidays to skip.</param>
    /// <param name="weekMask">
    /// Array of 7 bools indicating business days, starting from Monday. 
    /// Default is Mon-Fri [true, true, true, true, true, false, false].
    /// </param>
    /// <param name="roll">Strategy for handling non-business days.</param>
    /// <example>
    /// <code>
    /// // Add 1 business day (skipping weekends)
    /// // Fri 2024-03-01 -> Mon 2024-03-04
    /// df.Select(
    ///     Col("dt").Dt.AddBusinessDays(1)
    /// );
    /// </code>
    /// </example>
    public Expr AddBusinessDays(
        Expr n,
        IEnumerable<DateOnly>? holidays = null,
        bool[]? weekMask = null,
        Roll roll = Roll.Raise)
    {
        var mask = weekMask ?? DefaultWeekMask;

        int[] holidayInts;
        if (holidays == null)
        {
            holidayInts = [];
        }
        else
        {
            // Polars using 1970-01-01 as 0
            // DateOnly.DayNumber is days from 0001-01-01
            // 1970-01-01 DayNumber is 719162
            const int EpochDayNumber = 719162;
            holidayInts = [.. holidays.Select(d => d.DayNumber - EpochDayNumber)];
        }

        var nHandle = PolarsWrapper.CloneExpr(n.Handle);
        var handle = PolarsWrapper.CloneExpr(_expr.Handle);

        return new Expr(PolarsWrapper.DtAddBusinessDays(
            handle,
            nHandle,
            mask,
            holidayInts,
            roll.ToNative()
        ));
    }

    /// <summary>
    /// Check if the date is a business day.
    /// </summary>
    public Expr IsBusinessDay(IEnumerable<DateOnly>? holidays = null, bool[]? weekMask = null)
    {
        var mask = weekMask ?? DefaultWeekMask;

        int[] holidayInts;
        if (holidays == null)
        {
            holidayInts = [];
        }
        else
        {
            const int EpochDayNumber = 719162;
            holidayInts = [.. holidays.Select(d => d.DayNumber - EpochDayNumber)];
        }
        var handle = PolarsWrapper.CloneExpr(_expr.Handle);
        return new Expr(PolarsWrapper.DtIsBusinessDay(handle, mask, holidayInts));
    }
}