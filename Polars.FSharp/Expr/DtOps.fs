namespace Polars.FSharp

open System
open Polars.NET.Core

type [<Struct>] DtOps(handle: ExprHandle) =
    /// <summary>Get the year from the underlying date/datetime.</summary>
    member _.Year() = new Expr(PolarsWrapper.DtYear handle)
    /// <summary>Get the quarter from the underlying date/datetime.</summary>
    member _.Quarter() = new Expr(PolarsWrapper.DtQuarter handle)
    /// <summary>Get the month from the underlying date/datetime.</summary>
    member _.Month() = new Expr(PolarsWrapper.DtMonth handle)
    /// <summary>Get the day from the underlying date/datetime.</summary>
    member _.Day() = new Expr(PolarsWrapper.DtDay handle)
    /// <summary>Get the ordinal day(day of year) from the underlying date/datetime.</summary>
    member _.OrdinalDay() = new Expr(PolarsWrapper.DtOrdinalDay handle)
    /// <summary>Get the weekday from the underlying date/datetime.</summary>
    member _.Weekday() = new Expr(PolarsWrapper.DtWeekday handle)
    /// <summary>Get the hour from the underlying datetime.</summary>
    member _.Hour() = new Expr(PolarsWrapper.DtHour handle)
    /// <summary>Get the minute from the underlying datetime.</summary>
    member _.Minute() = new Expr(PolarsWrapper.DtMinute handle)
    /// <summary>Get the second from the underlying datetime.</summary>
    member _.Second() = new Expr(PolarsWrapper.DtSecond handle)
    /// <summary>Get the millisecond from the underlying datetime.</summary>
    member _.Millisecond() = new Expr(PolarsWrapper.DtMillisecond handle)
    /// <summary>Get the microsecond from the underlying datetime.</summary>
    member _.Microsecond() = new Expr(PolarsWrapper.DtMicrosecond handle)
    /// <summary>Get the nanosecond from the underlying datetime.</summary>
    member _.Nanosecond() = new Expr(PolarsWrapper.DtNanosecond handle)
    /// <summary>
    /// Cast to Date (remove time component).
    /// </summary>
    member _.Date() = new Expr(PolarsWrapper.DtDate handle)
    /// <summary>
    /// Cast to Time (remove Date component).
    /// </summary>
    member _.Time() = new Expr(PolarsWrapper.DtTime handle)

    /// <summary> Format datetime to string using the given format string (strftime). </summary>
    member _.ToString(format: string) = 
        new Expr(PolarsWrapper.DtToString(handle, format))

    member this.ToString() = 
        this.ToString "%Y-%m-%dT%H:%M:%S%.f"
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
    /// Convert to integer timestamp (Microseconds).
    /// </summary>
    member _.TimestampMicros() = 
        new Expr(PolarsWrapper.DtTimestamp(handle, TimeUnit.Microseconds.ToNative()))

    /// <summary>
    /// Convert to integer timestamp (Milliseconds).
    /// </summary>
    member _.TimestampMillis() = 
        new Expr(PolarsWrapper.DtTimestamp(handle, TimeUnit.Milliseconds.ToNative()))
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
    member _.ReplaceTimeZone(timeZone: string option, ?ambiguous: string, ?nonExistent: string) =
        let tz = Option.toObj timeZone
        let amb = Option.toObj ambiguous
        let ne = Option.toObj nonExistent
        new Expr(PolarsWrapper.DtReplaceTimeZone(handle, tz, amb, ne))

    /// <summary>
    /// Helper: Replace time zone with a specific string.
    /// </summary>
    member this.ReplaceTimeZone(timeZone: string, ?ambiguous: string, ?nonExistent: string) =
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