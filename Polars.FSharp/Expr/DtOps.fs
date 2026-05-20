namespace Polars.FSharp

open System
open Polars.NET.Core

[<RequireQualifiedAccess>]
type Dur =
    | String of string
    | TimeSpan of TimeSpan

[<RequireQualifiedAccess>]
module Dur =
    open Polars.NET.Core.Helpers
    let consume (src: Dur) =
        match src with
        | Dur.String s ->
            if String.IsNullOrWhiteSpace s then
                invalidArg "src" "Duration string cannot be null or empty."
            s
        | Dur.TimeSpan ts ->
            ts.ToPolarsDuration()

type [<Struct>] DtOps(handle: ExprHandle) =
    /// <summary>
    /// Extract the millennium from underlying representation.
    /// Applies to Date and Datetime columns.
    /// Returns the millennium number in the calendar date.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Millennium() = new Expr(PolarsWrapper.DtMillennium handle)
    /// <summary>
    /// Extract the century from underlying representation.Returns the century number in the calendar date.
    /// </summary>
    /// <returns>Expression/Series of data type Int32.</returns>
    member _.Century() = new Expr(PolarsWrapper.DtCentury handle)
    /// <summary>Get the year from the underlying date/datetime.</summary>
    member _.Year() = new Expr(PolarsWrapper.DtYear handle)
    member _.IsoYear() = new Expr(PolarsWrapper.DtIsoYear handle)
    /// <summary>Get the quarter from the underlying date/datetime.</summary>
    member _.Quarter() = new Expr(PolarsWrapper.DtQuarter handle)
    /// <summary>Get the month from the underlying date/datetime.</summary>
    member _.Month() = new Expr(PolarsWrapper.DtMonth handle)
    /// <summary>Get the day from the underlying date/datetime.</summary>
    member _.Day() = new Expr(PolarsWrapper.DtDay handle)
    /// <summary>
    /// Extract the number of days in the month from the underlying Date representation.Returns the number of days in the month. The return value ranges from 28 to 31.
    /// </summary>
    /// <returns>Expression/Series of data type Int8.</returns>
    member _.DayInMonth() = new Expr(PolarsWrapper.DtDaysInMonth handle)
    /// <summary>Get the ordinal day(day of year) from the underlying date/datetime.</summary>
    member _.OrdinalDay() = new Expr(PolarsWrapper.DtOrdinalDay handle)
    /// <summary>Get the weekday from the underlying date/datetime.</summary>
    member _.Weekday() = new Expr(PolarsWrapper.DtWeekday handle)
    /// <summary>Get the hour from the underlying datetime.</summary>
    member _.Hour() = new Expr(PolarsWrapper.DtHour handle)
    /// <summary>Get the minute from the underlying datetime.</summary>
    member _.Minute() = new Expr(PolarsWrapper.DtMinute handle)
    /// <summary>Get the second from the underlying datetime.</summary>
    /// <param name="fractional">Whether to include the fractional component of the second.</param> 
    member this.Second(?fractional:bool) = 
        let sec = new Expr(PolarsWrapper.DtSecond handle)
        let frac = defaultArg fractional false
        match frac with
        | true -> sec
        | false -> 
            let nano = this.Nanosecond()
            sec + nano / new Expr(PolarsWrapper.Lit 1_000_000_000.0)
    /// <summary>Get the millisecond from the underlying datetime.</summary>
    member _.Millisecond() = new Expr(PolarsWrapper.DtMillisecond handle)
    /// <summary>Get the microsecond from the underlying datetime.</summary>
    member _.Microsecond() = new Expr(PolarsWrapper.DtMicrosecond handle)
    /// <summary>Get the nanosecond from the underlying datetime.</summary>
    member _.Nanosecond() = new Expr(PolarsWrapper.DtNanosecond handle)
    /// <summary>
    /// Determine whether the year of the underlying date is a leap year.
    /// </summary>
    /// <returns>Expression/Series of data type Boolean.</returns>
    member _.IsLeapYear() = new Expr(PolarsWrapper.DtIsLeapYear handle)
    /// <summary>
    /// Roll backward to the first day of the month.
    /// For datetimes, the time-of-day is preserved.
    /// </summary>
    /// <returns>Expression/Series of data type Date or Datetime.</returns>
    member _.MonthStart() = new Expr(PolarsWrapper.DtMonthStart handle)
    /// <summary>
    /// Roll forward to the last day of the month.
    /// For datetimes, the time-of-day is preserved.
    /// </summary>
    /// <returns>Expression/Series of data type Date or Datetime.</returns>
    member _.MonthEnd() = new Expr(PolarsWrapper.DtMonthEnd handle)
    /// <summary>
    /// Base offset from UTC.
    /// This is usually constant for all datetimes in a given time zone, but may vary in the rare case that a country switches time zone, like Samoa (Apia) did at the end of 2011.
    /// </summary>
    /// <returns>Expression/Series of data type Duration.</returns>
    member _.BaseUtcOffset() = new Expr(PolarsWrapper.DtBaseUtcOffset handle)
    /// <summary>
    /// Additional offset currently in effect (typically due to daylight saving time).
    /// </summary>
    /// <returns>Expression/Series of data type Duration.</returns>
    member _.DstOffset() = new Expr(PolarsWrapper.DtDstOffset handle)
    /// <summary>
    /// Cast to Date (remove time component).
    /// </summary>
    member _.Date() = new Expr(PolarsWrapper.DtDate handle)
    /// <summary>
    /// Cast to Time (remove Date component).
    /// </summary>
    member _.Time() = new Expr(PolarsWrapper.DtTime handle)
    /// <summary>
    /// Extract the total days from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the day.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>
    member _.TotalDays(?fractional) = 
        let frac = defaultArg fractional false
        new Expr(PolarsWrapper.DtTotalDays(handle,frac))
    /// <summary>
    /// Extract the total hours from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the hour.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>  
    member _.TotalHours(?fractional) = 
        let frac = defaultArg fractional false
        new Expr(PolarsWrapper.DtTotalHours(handle,frac))
    /// <summary>
    /// Extract the total minutes from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the minute.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>    
    member _.TotalMinutes(?fractional) = 
        let frac = defaultArg fractional false
        new Expr(PolarsWrapper.DtTotalMinutes(handle,frac))
    /// <summary>
    /// Extract the total seconds from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the second.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns> 
    member _.TotalSeconds(?fractional) = 
        let frac = defaultArg fractional false
        new Expr(PolarsWrapper.DtTotalSeconds(handle,frac))
    /// <summary>
    /// Extract the total milliseconds from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the millisecond.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>   
    member _.TotalMilliseconds(?fractional) = 
        let frac = defaultArg fractional false
        new Expr(PolarsWrapper.DtTotalMilliseconds(handle,frac))
    /// <summary>
    /// Extract the total microseconds from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include the fractional component of the microsecond.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>  
    member _.TotalMicroseconds(?fractional) = 
        let frac = defaultArg fractional false
        new Expr(PolarsWrapper.DtTotalMicroseconds(handle,frac))
    /// <summary>
    /// Extract the total nanoseconds from a Duration type.
    /// </summary>
    /// <param name="fractional">Whether to include return the result as a Float64. Because the smallest TimeUnit is 'ns', the fractional component will always be zero.</param>
    /// <returns>Expression/Series of data type Int64 or Float64 if fractional is set.</returns>   
    member _.TotalNanoseconds(?fractional) = 
        let frac = defaultArg fractional false
        new Expr(PolarsWrapper.DtTotalNanoseconds(handle,frac))
    /// <summary> Format datetime to string using the given format string (strftime). </summary>
    member _.ToString(format: string) = 
        new Expr(PolarsWrapper.DtToString(handle, format))
    member this.ToString() = 
        this.ToString "%Y-%m-%dT%H:%M:%S%.f"
    /// <summary>
    /// Alias for <see cref="ToString(string)"/>.
    /// </summary>
    member this.Strftime(format:string) = this.ToString format
    // --- Manipulation ---

    /// <summary>
    /// Truncate dates to the specified interval (e.g., "1d", "1h", "15m").
    /// </summary>
    member _.Truncate(every: Dur) = 
        let everyStr = Dur.consume every
        new Expr(PolarsWrapper.DtTruncate(handle, PolarsWrapper.Lit everyStr))
    /// <summary>
    /// Round dates to the nearest interval.
    /// </summary>
    member _.Round(every: Dur) = 
        let everyStr = Dur.consume every
        new Expr(PolarsWrapper.DtRound(handle, PolarsWrapper.Lit everyStr))
    /// <summary>
    /// Offset the date by a given duration string (e.g., "1d", "-2h").
    /// </summary>
    member _.OffsetBy(duration: Dur) =
        let durationStr = Dur.consume duration
        new Expr(PolarsWrapper.DtOffsetBy(handle, PolarsWrapper.Lit durationStr))

    // --- Conversion ---

    /// <summary>
    /// Convert to integer timestamp.
    /// </summary>
    member _.Timestamp(?timeUnit) =
        let unit = defaultArg timeUnit TimeUnit.Microseconds 
        new Expr(PolarsWrapper.DtTimestamp(handle, unit.ToNative()))
    /// <summary>
    /// Combine the date from the underlying date/datetime with the time from another expression.
    /// <para>The resulting Series will have the specified TimeUnit.</para>
    /// </summary>
    /// <param name="time">An expression yielding the Time component.</param>
    /// <param name="timeUnit">
    /// The desired TimeUnit for the resulting Datetime.
    /// <para><b>Note:</b> Only sub-second units (<see cref="TimeUnit.Nanoseconds"/>, <see cref="TimeUnit.Microseconds"/>, <see cref="TimeUnit.Milliseconds"/>) are supported.</para>
    /// </param>
    member _.Combine(time:Expr, ?timeUnit:TimeUnit ) = 
        let unit = defaultArg timeUnit TimeUnit.Microseconds
        let hTime = PolarsWrapper.CloneExpr time.Handle;
        new Expr(PolarsWrapper.DtCombine(handle, hTime, unit.ToNative()));

    /// <summary>
    /// Convert the datetime to a different time zone.
    /// The underlying physical value (UTC timestamp) remains the same, but the display time changes.
    /// </summary>
    member _.ConvertTimeZone(timeZone: string) =
        new Expr(PolarsWrapper.DtConvertTimeZone(handle, timeZone))

    /// <summary>
    /// Replace the time zone of a datetime.
    /// Use None (null) to make it TimeZone-Naive.
    /// ambiguous: Strategy for DST overlaps ("raise", "earliest", "latest", "null").
    /// nonExistent: Strategy for missing DST times ("raise", "null").
    /// </summary>
    member _.ReplaceTimeZone(timeZone: string option, ?ambiguous: Expr, ?nonExistent: NonExistent) =
        let tz = Option.toObj timeZone
        let amb = 
            match ambiguous with
            | Some a -> a.CloneHandle()
            | None -> PolarsWrapper.Lit "raise"
        let ne = defaultArg nonExistent NonExistent.Raise
        new Expr(PolarsWrapper.DtReplaceTimeZone(handle, tz, amb, ne.ToNative()))

    /// <summary>
    /// Helper: Replace time zone with a specific string.
    /// </summary>
    member this.ReplaceTimeZone(timeZone: string, ?ambiguous: Expr, ?nonExistent: NonExistent) =
        this.ReplaceTimeZone(Some timeZone, ?ambiguous=ambiguous, ?nonExistent=nonExistent)
    /// <summary>
    /// Add business days to a date column.
    /// </summary>
    /// <param name="n">Number of business days to add.</param>
    /// <param name="weekMask">Array of 7 bools (Mon-Sun) indicating business days. Default: [true, true, true, true, true, false, false].</param>
    /// <param name="holidays">List of holidays to skip.</param>
    /// <param name="roll">Strategy for handling non-business start dates. Default: Raise.</param>
    member this.AddBusinessDays(
        n: Expr, 
        ?weekMask: bool[], 
        ?holidays: seq<DateOnly>, 
        ?roll: Roll
    ) =
        let mask = defaultArg weekMask [| true; true; true; true; true; false; false |]
        let r = defaultArg roll Roll.Raise
        
        let epoch = DateOnly(1970, 1, 1).DayNumber
        let holidayInts = 
            match holidays with
            | Some hols -> hols |> Seq.map (fun d -> d.DayNumber - epoch) |> Seq.toArray
            | None -> [||]

        new Expr(PolarsWrapper.DtAddBusinessDays(
            handle, 
            n.CloneHandle(), 
            mask, 
            holidayInts, 
            r.ToNative()
        ))

    /// <summary>
    /// Overload: Add business days using an integer literal.
    /// </summary>
    member this.AddBusinessDays(n: int, ?weekMask, ?holidays, ?roll) =
        let expr = new Expr(PolarsWrapper.Lit n)
        this.AddBusinessDays(
            expr, 
            ?weekMask = weekMask, 
            ?holidays = holidays, 
            ?roll = roll
        )

    /// <summary>
    /// Check if the date is a business day.
    /// </summary>
    /// <param name="weekMask">Array of 7 bools (Mon-Sun). Default: Mon-Fri are business days.</param>
    /// <param name="holidays">List of holidays.</param>
    member this.IsBusinessDay(?weekMask: bool[], ?holidays: seq<DateOnly>) =
        let mask = defaultArg weekMask [| true; true; true; true; true; false; false |]
        
        let epoch = DateOnly(1970, 1, 1).DayNumber
        let holidayInts = 
            match holidays with
            | Some hols -> hols |> Seq.map (fun d -> d.DayNumber - epoch) |> Seq.toArray
            | None -> [||]

        new Expr(PolarsWrapper.DtIsBusinessDay(
            handle,
            mask,
            holidayInts
        ))
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
    member this.Epoch(?timeUnit) =
        let tu = defaultArg timeUnit EpochTimeUnit.Microseconds
        match tu with
        | EpochTimeUnit.Nanoseconds -> this.Timestamp(TimeUnit.Nanoseconds)
        | EpochTimeUnit.Microseconds -> this.Timestamp(TimeUnit.Microseconds)
        | EpochTimeUnit.Milliseconds -> this.Timestamp(TimeUnit.Milliseconds)
        | EpochTimeUnit.Second -> this.Timestamp(TimeUnit.Milliseconds).FloorDiv(new Expr(PolarsWrapper.Lit 1000L))
        | EpochTimeUnit.Day -> 
            let h1 = PolarsWrapper.ExprCast(handle,DataType.Date.ToDataTypeExpr().handle,true,false)
            new Expr(PolarsWrapper.ExprCast(h1,DataType.Int32.ToDataTypeExpr().handle,true,false))
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
    member _.Replace(
        ?year:int,?month:int,?day:int,?hour:int,
        ?minute:int,?second:int,?microsecond:int,
        ?ambiguous:Expr) =
        let litOrNull (value: int option) =
            match value with
            | Some v -> PolarsWrapper.Lit v   
            | None   -> PolarsWrapper.LitNull()
        let y   = litOrNull year
        let mo  = litOrNull month
        let d   = litOrNull day
        let h   = litOrNull hour
        let mi  = litOrNull minute
        let s   = litOrNull second
        let ms  = litOrNull microsecond
        let amb =
            match ambiguous with
            | Some a -> a.CloneHandle()
            | None   -> PolarsWrapper.Lit "raise"
        new Expr(PolarsWrapper.DtReplace(handle, y, mo, d, h, mi, s, ms, amb))
        
