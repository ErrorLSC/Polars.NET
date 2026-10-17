namespace Polars.FSharp


type SeriesDtNameSpace(parent: Series) =

    // Helper: col("Name").Dt.Op(...)
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary>
    /// Extract the millennium from underlying representation.
    /// Applies to Date and Datetime columns.
    /// Returns the millennium number in the calendar date.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Millennium() = apply (fun e -> e.Dt.Millennium())
    /// <summary>
    /// Extract the century from underlying representation. Returns the century number in the calendar date.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Century() = apply (fun e -> e.Dt.Century())
    /// <summary>
    /// Get the year from the underlying date/datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Year() = apply (fun e -> e.Dt.Year())
    /// <summary>
    /// Get the ISO year from the underlying date/datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.IsoYear() = apply (fun e -> e.Dt.IsoYear())
    /// <summary>
    /// Get the quarter from the underlying date/datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Quarter() = apply (fun e -> e.Dt.Quarter())
    /// <summary>
    /// Get the month from the underlying date/datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Month() = apply (fun e -> e.Dt.Month())
    /// <summary>
    /// Get the day from the underlying date/datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Day() = apply (fun e -> e.Dt.Day())
    /// <summary>
    /// Get the number of days in the month from the underlying Date representation.
    /// </summary>
    /// <returns>Expression/Series of data type Int8.</returns>
    member _.DayInMonth() = apply (fun e -> e.Dt.DayInMonth())
    /// <summary>
    /// Get the hour from the underlying datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Hour() = apply (fun e -> e.Dt.Hour())
    /// <summary>
    /// Get the minute from the underlying datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Minute() = apply (fun e -> e.Dt.Minute())

    /// <summary>
    /// Get the second from the underlying datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Second(?fractional: bool) =
        apply (fun e -> e.Dt.Second(?fractional = fractional))

    /// <summary>
    /// Get the millisecond from the underlying datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Millisecond() = apply (fun e -> e.Dt.Millisecond())
    /// <summary>
    /// Get the microsecond from the underlying datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Microsecond() = apply (fun e -> e.Dt.Microsecond())
    /// <summary>
    /// Get the nanosecond from the underlying datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Nanosecond() = apply (fun e -> e.Dt.Nanosecond())
    /// <summary>
    /// Get the ordinal day (day of year) from the underlying date/datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.OrdinalDay() = apply (fun e -> e.Dt.OrdinalDay())
    /// <summary>
    /// Get the weekday from the underlying date/datetime.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Weekday() = apply (fun e -> e.Dt.Weekday())
    /// <summary>
    /// Determine whether the year of the underlying date is a leap year.
    /// </summary>
    /// <returns>Expression/Series of data type Boolean.</returns>
    member _.IsLeapYear() = apply (fun e -> e.Dt.IsLeapYear())
    /// <summary>
    /// Roll backward to the first day of the month. For datetimes, the time-of-day is preserved.
    /// </summary>
    /// <returns>Expression/Series of data type Date or Datetime.</returns>
    member _.MonthStart() = apply (fun e -> e.Dt.MonthStart())
    /// <summary>
    /// Roll forward to the last day of the month. For datetimes, the time-of-day is preserved.
    /// </summary>
    /// <returns>Expression/Series of data type Date or Datetime.</returns>
    member _.MonthEnd() = apply (fun e -> e.Dt.MonthEnd())
    /// <summary>
    /// Base offset from UTC. This is usually constant for all datetimes in a given time zone, but may vary in the rare case that a country switches time zone, like Samoa (Apia) did at the end of 2011.
    /// </summary>
    /// <returns>Expression/Series of data type Duration.</returns>
    member _.BaseUtcOffset() = apply (fun e -> e.Dt.BaseUtcOffset())
    /// <summary>
    /// Additional offset currently in effect (typically due to daylight saving time).
    /// </summary>
    /// <returns>Expression/Series of data type Duration.</returns>
    member _.DstOffset() = apply (fun e -> e.Dt.DstOffset())

    /// <summary>
    /// Extract the total days from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the day.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>
    member _.TotalDays(?fractional) =
        apply (fun e -> e.Dt.TotalDays(?fractional = fractional))

    /// <summary>
    /// Extract the total hours from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the hour.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>
    member _.TotalHours(?fractional) =
        apply (fun e -> e.Dt.TotalHours(?fractional = fractional))

    /// <summary>
    /// Extract the total minutes from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the minute.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>
    member _.TotalMinutes(?fractional) =
        apply (fun e -> e.Dt.TotalMinutes(?fractional = fractional))

    /// <summary>
    /// Extract the total seconds from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the second.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>
    member _.TotalSeconds(?fractional) =
        apply (fun e -> e.Dt.TotalSeconds(?fractional = fractional))

    /// <summary>
    /// Extract the total milliseconds from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the millisecond.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>
    member _.TotalMilliseconds(?fractional) =
        apply (fun e -> e.Dt.TotalMilliseconds(?fractional = fractional))

    /// <summary>
    /// Extract the total microseconds from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the microsecond.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>
    member _.TotalMicroseconds(?fractional) =
        apply (fun e -> e.Dt.TotalMicroseconds(?fractional = fractional))

    /// <summary>
    /// Extract the total nanoseconds from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the nanosecond.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>
    member _.TotalNanoseconds(?fractional) =
        apply (fun e -> e.Dt.TotalNanoseconds(?fractional = fractional))

    /// <summary>
    /// Cast to Date (remove time component).
    /// </summary>
    /// <returns>Expression/Series of data type Date.</returns>
    member _.Date() = apply (fun e -> e.Dt.Date())

    /// <summary>
    /// Cast to Time (remove Date component).
    /// </summary>
    /// <returns>Expression/Series of data type Time.</returns>
    member _.Time() = apply (fun e -> e.Dt.Time())

    /// <summary> Format datetime to string using the given format string (strftime). </summary>
    member _.ToString(?format: string) =
        apply (fun e -> e.Dt.ToString(?format = format))

    /// <summary> Default ISO format. </summary>
    member this.ToString() = this.ToString "%Y-%m-%dT%H:%M:%S%.f"

    /// <summary>
    /// Format datetime to string using the given format string (strftime).
    /// </summary>
    /// <param name="format">The format string to use.</param>
    /// <returns>Expression/Series of data type String.</returns>
    member this.Strftime(?format: string) =
        apply (fun e -> e.Dt.Strftime(?format = format))

    /// <summary>
    /// Truncate a datetime to the nearest multiple of a given duration.
    /// </summary>
    /// <param name="every">The duration to truncate to.</param>
    /// <returns>Expression/Series of data type Date or Datetime.</returns>
    member _.Truncate(every: Dur) = apply (fun e -> e.Dt.Truncate every)
    /// <summary>
    /// Round a datetime to the nearest multiple of a given duration.
    /// </summary>
    /// <param name="every">The duration to round to.</param>
    /// <returns>Expression/Series of data type Date or Datetime.</returns>
    member _.Round(every: Dur) = apply (fun e -> e.Dt.Round every)
    /// <summary>
    /// Offset a datetime by a given duration.
    /// </summary>
    /// <param name="duration">The duration to offset by.</param>
    /// <returns>Expression/Series of data type Date or Datetime.</returns>
    member _.OffsetBy(duration: Dur) = apply (fun e -> e.Dt.OffsetBy duration)

    /// <summary>
    /// Convert to timestamp.
    /// </summary>
    /// <param name="timeUnit">The time unit to convert to.</param>
    /// <returns>Expression/Series of data type Int64.</returns>
    member _.Timestamp(?timeUnit) =
        apply (fun e -> e.Dt.Timestamp(?timeUnit = timeUnit))

    /// <summary>
    /// Combine a date and time into a datetime.
    /// </summary>
    /// <param name="time">The time to combine with.</param>
    /// <param name="timeUnit">The time unit of the time.</param>
    /// <returns>Expression/Series of data type Datetime.</returns>
    member _.Combine(time: Expr, ?timeUnit: TimeUnit) =
        apply (fun e -> e.Dt.Combine(time, ?timeUnit = timeUnit))

    /// <summary>
    /// Convert to a different time zone.
    /// </summary>
    /// <param name="timeZone">The time zone to convert to.</param>
    /// <returns>Expression/Series of data type Datetime.</returns>
    member _.ConvertTimeZone(timeZone: string) =
        apply (fun e -> e.Dt.ConvertTimeZone timeZone)

    /// <summary>
    /// Replace the time zone of a datetime.
    /// </summary>
    /// <param name="timeZone">The new time zone.</param>
    /// <param name="ambiguous">How to handle ambiguous times.</param>
    /// <param name="nonExistent">How to handle non-existent times.</param>
    /// <returns>Expression/Series of data type Datetime.</returns>
    member _.ReplaceTimeZone(timeZone: string option, ?ambiguous: Expr, ?nonExistent: NonExistent) =
        apply (fun e -> e.Dt.ReplaceTimeZone(timeZone, ?ambiguous = ambiguous, ?nonExistent = nonExistent))

    /// <summary>
    /// Replace the time zone of a datetime.
    /// </summary>
    /// <param name="timeZone">The new time zone.</param>
    /// <param name="ambiguous">How to handle ambiguous times.</param>
    /// <param name="nonExistent">How to handle non-existent times.</param>
    /// <returns>Expression/Series of data type Datetime.</returns>
    member this.ReplaceTimeZone(timeZone: string, ?ambiguous, ?nonExistent) =
        this.ReplaceTimeZone(Some timeZone, ?ambiguous = ambiguous, ?nonExistent = nonExistent)

    /// <summary>
    /// Add business days (using integer).
    /// </summary>
    /// <param name="n">The number of business days to add.</param>
    /// <param name="weekMask">The week mask.</param>
    /// <param name="holidays">The holidays.</param>
    /// <param name="roll">How to handle rolled dates.</param>
    /// <returns>Expression/Series of data type Date or Datetime.</returns>
    member _.AddBusinessDays(n: int, ?weekMask, ?holidays, ?roll) =
        apply (fun e -> e.Dt.AddBusinessDays(n, ?weekMask = weekMask, ?holidays = holidays, ?roll = roll))

    /// <summary>
    /// Check whether the date is a business day.
    /// </summary>
    /// <param name="weekMask">The week mask.</param>
    /// <param name="holidays">The holidays.</param>
    /// <returns>Expression/Series of data type Boolean.</returns>
    member _.IsBusinessDay(?weekMask, ?holidays) =
        apply (fun e -> e.Dt.IsBusinessDay(?weekMask = weekMask, ?holidays = holidays))

    /// <summary>
    /// Convert to timestamp.
    /// </summary>
    /// <param name="timeUnit">The time unit to convert to.</param>
    /// <returns>Expression/Series of data type Int64.</returns>
    member _.Epoch(?timeUnit) =
        apply (fun e -> e.Dt.Epoch(?timeUnit = timeUnit))

    /// <summary>
    /// Replace the date or time components of a datetime.
    /// </summary>
    /// <param name="year">The new year.</param>
    /// <param name="month">The new month.</param>
    /// <param name="day">The new day.</param>
    /// <param name="hour">The new hour.</param>
    /// <param name="minute">The new minute.</param>
    /// <param name="second">The new second.</param>
    /// <param name="microsecond">The new microsecond.</param>
    /// <param name="ambiguous">How to handle ambiguous times.</param>
    /// <returns>Expression/Series of data type Datetime.</returns>
    member _.Replace
        (?year: int, ?month: int, ?day: int, ?hour: int, ?minute: int, ?second: int, ?microsecond: int, ?ambiguous: Expr) =
        apply (fun e ->
            e.Dt.Replace(
                ?year = year,
                ?month = month,
                ?day = day,
                ?hour = hour,
                ?minute = minute,
                ?second = second,
                ?microsecond = microsecond,
                ?ambiguous = ambiguous
            ))
