using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;
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
    /// <summary>
    /// Extract the millennium from underlying representation.
    /// Applies to Date and Datetime columns.
    /// Returns the millennium number in the calendar date.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    public Expr Millennium() => Wrap(PolarsWrapper.DtMillennium);
    /// <summary>
    /// Extract the century from underlying representation.Returns the century number in the calendar date.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    public Expr Century() => Wrap(PolarsWrapper.DtCentury);
    /// <summary>Get the year from the underlying date/datetime.Returns the year number in the calendar date.</summary>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     dt = new[] { new DateTime(2024, 1, 1, 10, 15, 30) } 
    /// });
    /// 
    /// df.Select(
    ///     Pl.Col("dt").Dt.Year(),    // 2024
    ///     Pl.Col("dt").Dt.Month(),   // 1
    ///     Pl.Col("dt").Dt.Weekday(), // 1 (Monday)
    ///     Pl.Col("dt").Dt.ToString("%Y-%m-%d") // "2024-01-01"
    /// ).Show();
    /// </code>
    /// </example>
    /// <returns>Expression/Series of data type Int32.</returns>
    public Expr Year() => Wrap(PolarsWrapper.DtYear);
    /// <summary>
    /// Extract ISO year from underlying Date representation.
    /// Applies to Date and Datetime columns.
    /// Returns the year number in the ISO standard. This may not correspond with the calendar year.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    public Expr IsoYear() => Wrap(PolarsWrapper.DtIsoYear);
    /// <summary>
    /// Extract quarter from underlying Date representation. Returns the quarter ranging from 1 to 4.
    /// </summary>
    /// <returns>Expression/Series of data type Int8.</returns>
    public Expr Quarter() => Wrap(PolarsWrapper.DtQuarter);
    /// <summary>
    /// Extract month from underlying Date representation.Returns the month number starting from 1. The return value ranges from 1 to 12.
    /// </summary>
    /// <returns>Expression/Series of data type Int8.</returns>
    public Expr Month() => Wrap(PolarsWrapper.DtMonth);
    /// <summary>
    /// Extract day from underlying Date representation.Returns the day of month starting from 1. The return value ranges from 1 to 31. (The last day of month differs by months.)
    /// </summary>
    /// <returns>Expression/Series of data type Int8.</returns>
    public Expr Day() => Wrap(PolarsWrapper.DtDay);
    /// <summary>
    /// Extract the number of days in the month from the underlying Date representation.Returns the number of days in the month. The return value ranges from 28 to 31.
    /// </summary>
    /// <returns>Expression/Series of data type Int8.</returns>
    public Expr DaysInMonth() => Wrap(PolarsWrapper.DtDaysInMonth);
    /// <summary>
    /// Extract ordinal day from underlying Date representation.Returns the day of year starting from 1. The return value ranges from 1 to 366. (The last day of year differs by years.)
    /// </summary>
    /// <returns>Expression/Series of data type Int16.</returns>
    public Expr OrdinalDay() => Wrap(PolarsWrapper.DtOrdinalDay);
    /// <summary>
    /// Extract the week day from the underlying Date representation.Returns the ISO weekday number where monday = 1 and sunday = 7
    /// </summary>
    /// <returns>Expression/Series of data type Int8.</returns>
    public Expr Weekday() => Wrap(PolarsWrapper.DtWeekday);
    /// <summary>
    /// Extract hour from underlying DateTime representation.Returns the hour number from 0 to 23.
    /// </summary>
    /// <returns>Expression/Series of data type Int8.</returns>
    public Expr Hour() => Wrap(PolarsWrapper.DtHour);
    /// <summary>
    /// Extract minutes from underlying DateTime representation.Returns the minute number from 0 to 59.
    /// </summary>
    /// <returns>Expression/Series of data type Int8.</returns>
    public Expr Minute() => Wrap(PolarsWrapper.DtMinute);
    /// <summary>
    /// Extract seconds from underlying DateTime representation.Returns the integer second number from 0 to 59, or a floating point number from 0 ~ 60 if fractional=True that includes any milli/micro/nanosecond component.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the second.</param> 
    /// <returns>Expression/Series of data type Int8 or Float64.</returns>
    public Expr Second(bool fractional = false)
    {
        var sec = Wrap(PolarsWrapper.DtSecond);

        if (!fractional)
        {
            return sec;
        }
        
        var nano = Nanosecond();
        
        return sec + (nano / 1_000_000_000.0);
    }
    /// <summary>
    /// Extract milliseconds from underlying DateTime representation.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    public Expr Millisecond() => Wrap(PolarsWrapper.DtMillisecond);
    /// <summary>
    /// Extract microseconds from underlying DateTime representation.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    public Expr Microsecond() => Wrap(PolarsWrapper.DtMicrosecond);
    /// <summary>
    /// Extract nanoseconds from underlying DateTime representation.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    public Expr Nanosecond() => Wrap(PolarsWrapper.DtNanosecond);
    /// <summary>
    /// Determine whether the year of the underlying date is a leap year.
    /// </summary>
    /// <returns>Expression/Series of data type Boolean.</returns>
    public Expr IsLeapYear() => Wrap(PolarsWrapper.DtIsLeapYear);
    /// <summary>
    /// Roll backward to the first day of the month.
    /// For datetimes, the time-of-day is preserved.
    /// </summary>
    /// <returns>Expression/Series of data type Date or Datetime.</returns>
    public Expr MonthStart() => Wrap(PolarsWrapper.DtMonthStart);
    /// <summary>
    /// Roll forward to the last day of the month.
    /// For datetimes, the time-of-day is preserved.
    /// </summary>
    /// <returns>Expression/Series of data type Date or Datetime.</returns>
    public Expr MonthEnd() => Wrap(PolarsWrapper.DtMonthEnd);
    /// <summary>
    /// Base offset from UTC.
    /// This is usually constant for all datetimes in a given time zone, but may vary in the rare case that a country switches time zone, like Samoa (Apia) did at the end of 2011.
    /// </summary>
    /// <returns>Expression/Series of data type Duration.</returns>
    public Expr BaseUtcOffset() => Wrap(PolarsWrapper.DtBaseUtcOffset);
    /// <summary>
    /// Additional offset currently in effect (typically due to daylight saving time).
    /// </summary>
    /// <returns>Expression/Series of data type Duration.</returns>
    public Expr DstOffset() => Wrap(PolarsWrapper.DtDstOffset);
    /// <summary>
    /// Extract date from date(time).
    /// </summary>
    /// <returns>Expression/Series of data type Date.</returns>
    public Expr Date() => Wrap(PolarsWrapper.DtDate);
    /// <summary>
    /// Extract time.Applies to Datetime columns only; fails on Date.
    /// </summary>
    /// <returns>Expression/Series of data type Time.</returns>
    public Expr Time() => Wrap(PolarsWrapper.DtTime);
    /// <summary>
    /// Extract the total days from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the day.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>
    public Expr TotalDays(bool fractional=false) => new(PolarsWrapper.DtTotalDays(_expr.CloneHandle(),fractional));
    /// <summary>
    /// Extract the total hours from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the hour.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>    
    public Expr TotalHours(bool fractional=false) => new(PolarsWrapper.DtTotalHours(_expr.CloneHandle(),fractional));
    /// <summary>
    /// Extract the total minutes from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the minute.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>    
    public Expr TotalMinutes(bool fractional=false) => new(PolarsWrapper.DtTotalMinutes(_expr.CloneHandle(),fractional));
    /// <summary>
    /// Extract the total seconds from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the second.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>    
    public Expr TotalSeconds(bool fractional=false) => new(PolarsWrapper.DtTotalSeconds(_expr.CloneHandle(),fractional));
    /// <summary>
    /// Extract the total milliseconds from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the millisecond.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>    
    public Expr TotalMilliseconds(bool fractional=false) => new(PolarsWrapper.DtTotalMilliseconds(_expr.CloneHandle(),fractional));
    /// <summary>
    /// Extract the total microseconds from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the microsecond.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>    
    public Expr TotalMicroseconds(bool fractional=false) => new(PolarsWrapper.DtTotalMicroseconds(_expr.CloneHandle(),fractional));
    /// <summary>
    /// Extract the total nanoseconds from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include return the result as a Float64. Because the smallest TimeUnit is 'ns', the fractional component will always be zero.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>    
    public Expr TotalNanoseconds(bool fractional=false) => new(PolarsWrapper.DtTotalNanoseconds(_expr.CloneHandle(),fractional));
    /// <summary>
    /// Convert a Date/Time/Datetime column into a String column with the given format.
    /// </summary>
    /// <param name="format">Format codes follow the Rust `chrono` crate syntax (similar to strftime).
    ///  <para>If no format is provided, the appropriate ISO format for the underlying data type is used. This can be made explicit by passing "iso" or "iso:strict" as the format string.</para>
    /// </param>
    public Expr ToString(string format="iso") => new(PolarsWrapper.DtToString(_expr.CloneHandle(), format));
    /// <summary>
    /// Alias for <see cref="ToString(string)"/>.
    /// </summary>
    public Expr Strftime(string format) => ToString(format);

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
    ///     Pl.Col("dt").Dt.Truncate("1h"), // Result: 23:00:00
    ///     Pl.Col("dt").Dt.Round("1h")     // Result: 2024-03-01 00:00:00
    /// );
    /// </code>
    /// </example>
    public Expr Truncate(DurationOrExpr every)
        => new(PolarsWrapper.DtTruncate(_expr.CloneHandle(),every.Expression.CloneHandle()));

    /// <summary>
    /// Round the datetimes to the given interval.
    /// </summary>
    public Expr Round(DurationOrExpr every)
        => new(PolarsWrapper.DtRound(_expr.CloneHandle(),every.Expression.CloneHandle()));
    // ==========================================
    // Offset
    // ==========================================

    /// <summary>
    /// Offset the datetimes by a constant duration string (e.g., "1d", "-2h").
    /// </summary>
    /// <example>
    /// <code>
    /// df.Select(
    ///     // Add 2 days
    ///     Pl.Col("dt").Dt.OffsetBy("2d"),
    ///     // Subtract 1 hour
    ///     Pl.Col("dt").Dt.OffsetBy("-1h")
    /// );
    /// </code>
    /// </example>
    public Expr OffsetBy(DurationOrExpr by) => new(PolarsWrapper.DtOffsetBy(_expr.Handle, by.Expression.CloneHandle()));

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
        => new(PolarsWrapper.DtTimestamp(_expr.CloneHandle(), timeUnit.ToNative()));
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
        => new(PolarsWrapper.DtCombine(_expr.CloneHandle(),time.CloneHandle(), timeUnit.ToNative()));
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
    /// <returns>A new expression/Series with the converted time.</returns>
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
    ///     Pl.Col("dt").Alias("naive"),
    ///     
    ///     Pl.Col("dt").Dt.ReplaceTimeZone("UTC").Alias("utc_tagged"),
    ///     
    ///     Pl.Col("dt").Dt.ReplaceTimeZone("UTC")
    ///               .Dt.ConvertTimeZone("Asia/Shanghai")
    ///               .Alias("shanghai_time"),
    /// 
    ///     Pl.Col("dt").Dt.ReplaceTimeZone("UTC")
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
    public Expr ConvertTimeZone(string timeZone) => new(PolarsWrapper.DtConvertTimeZone(_expr.CloneHandle(), timeZone));
    /// <summary>
    /// Replace the time zone of a column.
    /// <para>
    /// This sets the time zone metadata without changing the underlying physical time (wall clock).
    /// Use this to assign a timezone to a naive datetime.
    /// </para>
    /// </summary>
    /// <param name="timeZone">The time zone to assign (e.g. "UTC", "Asia/Shanghai"). If null, removes timezone info.</param>
    /// <param name="ambiguous">How to handle ambiguous times (e.g. DST transitions). Default "raise".</param>
    /// <param name="nonExistent">How to handle non-existent times. Default "raise".</param>
    /// <seealso cref="ConvertTimeZone(string)"/>
    public Expr ReplaceTimeZone(string? timeZone, AmbiguousArg? ambiguous = null, NonExistent nonExistent = NonExistent.Raise)
    {
        Expr amExpr = ambiguous.HasValue ? ambiguous.Value.Expression : Pl.Lit("raise"); 
        return new Expr(PolarsWrapper.DtReplaceTimeZone(_expr.CloneHandle(), timeZone, amExpr.CloneHandle(), nonExistent.ToNative()));
    }
    // ==========================================
    // BusinessDays
    // ==========================================
    /// <summary>
    /// Default Weekday Mask
    /// </summary>
    public static readonly bool[] DefaultWeekMask = [true, true, true, true, true, false, false];

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
    ///     Pl.Col("dt").Dt.AddBusinessDays(1)
    /// );
    /// </code>
    /// </example>
    public Expr AddBusinessDays(
        IntOrExpr n,
        bool[]? weekMask = null,
        IntoDateSeries? holidays = null,
        Roll roll = Roll.Raise)
    {
        var mask = weekMask ?? DefaultWeekMask;

        return new Expr(PolarsWrapper.DtAddBusinessDays(
            _expr.CloneHandle(),
            n.Expression.CloneHandle(),
            mask,
            holidays?.ToPhysicalArray() ?? [],
            roll.ToNative()
        ));
    }
    /// <summary>
    /// Determine whether each day lands on a business day.
    /// </summary>
    /// <param name="holidays">Holidays to exclude from the count.</param>
    /// <param name="weekMask">Which days of the week to count. The default is Monday to Friday. If you wanted to count only Monday to Thursday, you would pass (True, True, True, True, False, False, False).</param>
    /// <returns>Expression/Series of data type Boolean.</returns>
    public Expr IsBusinessDay(bool[]? weekMask = null,IntoDateSeries? holidays= null)
    {
        var mask = weekMask ?? DefaultWeekMask;
        int[] holidaysMask = holidays?.ToPhysicalArray() ?? [];
        return new Expr(PolarsWrapper.DtIsBusinessDay(_expr.CloneHandle(), mask, holidaysMask));
    }
    /// <summary>
    /// Replace the datetime components of the underlying Datetime/Date.
    /// </summary>
    /// <param name="year">Year to replace</param>
    /// <param name="month">Month to replace</param>
    /// <param name="day">Day to replace</param>
    /// <param name="hour">Hour to replace</param>
    /// <param name="minute">Minute to replace</param>
    /// <param name="second">Second to replace</param>
    /// <param name="microsecond">Microsecond to replace</param>
    /// <param name="ambiguous">Determine how to deal with ambiguous datetimes.</param>
    /// <returns>A new expression.</returns>
    public Expr Replace(
        IntoExprColumn? year = null,
        IntoExprColumn? month = null,
        IntoExprColumn? day = null,
        IntoExprColumn? hour = null,
        IntoExprColumn? minute = null,
        IntoExprColumn? second = null,
        IntoExprColumn? microsecond = null,
        AmbiguousArg? ambiguous = null)
    {
        Expr yearExpr = year?.Consume() ?? Pl.LitNull();
        Expr monthExpr = month?.Consume() ?? Pl.LitNull();
        Expr dayExpr = day?.Consume() ?? Pl.LitNull();
        Expr hourExpr = hour?.Consume() ?? Pl.LitNull();
        Expr minuteExpr = minute?.Consume() ?? Pl.LitNull();
        Expr secondExpr = second?.Consume() ?? Pl.LitNull();
        Expr microsecondExpr = microsecond?.Consume() ?? Pl.LitNull();

        Expr amExpr = ambiguous?.Expression ?? Pl.Lit("raise");

        return new Expr(PolarsWrapper.DtReplace(
            _expr.CloneHandle(),
            yearExpr.Handle,
            monthExpr.Handle,
            dayExpr.Handle,
            hourExpr.Handle,
            minuteExpr.Handle,
            secondExpr.Handle,
            microsecondExpr.Handle,
            amExpr.Handle
        ));
    }
    /// <summary>
    /// Get the time passed since the Unix epoch (1970-01-01 00:00:00).
    /// </summary>
    /// <param name="timeUnit">
    /// The time unit to compute the epoch for. 
    /// Supported units: Nanoseconds, Microseconds, Milliseconds, Second, Day.
    /// (default: Microseconds)
    /// </param>
    /// <returns>A new expression/Series representing the epoch time.</returns>
    /// <exception cref="ArgumentException">Thrown when an unsupported TimeUnit is provided.</exception>
    public Expr Epoch(EpochTimeUnit timeUnit = EpochTimeUnit.Microseconds)
    {
        return timeUnit switch
        {
            EpochTimeUnit.Nanoseconds  => Timestamp(TimeUnit.Nanoseconds),
            EpochTimeUnit.Microseconds => Timestamp(TimeUnit.Microseconds),
            EpochTimeUnit.Milliseconds => Timestamp(TimeUnit.Milliseconds),
            
            EpochTimeUnit.Second => Timestamp(TimeUnit.Milliseconds).FloorDiv(1000L),
            
            EpochTimeUnit.Day => _expr.Cast<DateOnly>().Cast<int>(),
            
            _ => throw new ArgumentException($"`timeUnit` must be one of {{Nanoseconds, Microseconds, Milliseconds, Second, Day}}, got {timeUnit}")
        };
    }
}