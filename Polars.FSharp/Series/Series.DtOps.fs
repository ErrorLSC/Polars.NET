namespace Polars.FSharp


type SeriesDtNameSpace(parent: Series) =
    
    // Helper: col("Name").Dt.Op(...)
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    member _.Millennium() = apply (fun e -> e.Dt.Millennium())
    member _.Century() = apply (fun e -> e.Dt.Century())
    member _.Year() = apply (fun e -> e.Dt.Year())
    member _.IsoYear() = apply (fun e -> e.Dt.IsoYear())
    member _.Quarter() = apply (fun e -> e.Dt.Quarter())
    member _.Month() = apply (fun e -> e.Dt.Month())
    member _.Day() = apply (fun e -> e.Dt.Day())
    member _.DayInMonth() = apply (fun e -> e.Dt.DayInMonth())
    member _.Hour() = apply (fun e -> e.Dt.Hour())
    member _.Minute() = apply (fun e -> e.Dt.Minute())
    member _.Second(?fractional:bool) = apply (fun e -> e.Dt.Second(?fractional=fractional))
    member _.Millisecond() = apply (fun e -> e.Dt.Millisecond())
    member _.Microsecond() = apply (fun e -> e.Dt.Microsecond())
    member _.Nanosecond() = apply (fun e -> e.Dt.Nanosecond())
    member _.OrdinalDay() = apply (fun e -> e.Dt.OrdinalDay())
    member _.Weekday() = apply (fun e -> e.Dt.Weekday())
    member _.IsLeapYear() = apply (fun e -> e.Dt.IsLeapYear())
    member _.MonthStart() = apply (fun e -> e.Dt.MonthStart())
    member _.MonthEnd() = apply (fun e -> e.Dt.MonthEnd())
    member _.BaseUtcOffset() = apply (fun e -> e.Dt.BaseUtcOffset())
    member _.DstOffset() = apply (fun e -> e.Dt.DstOffset())
    member _.TotalDays(?fractional) = apply (fun e -> e.Dt.TotalDays(?fractional=fractional))
    member _.TotalHours(?fractional) = apply (fun e -> e.Dt.TotalHours(?fractional=fractional))
    member _.TotalMinutes(?fractional) = apply (fun e -> e.Dt.TotalMinutes(?fractional=fractional))
    member _.TotalSeconds(?fractional) = apply (fun e -> e.Dt.TotalSeconds(?fractional=fractional))
    member _.TotalMilliseconds(?fractional) = apply (fun e -> e.Dt.TotalMilliseconds(?fractional=fractional))
    member _.TotalMicroseconds(?fractional) = apply (fun e -> e.Dt.TotalMicroseconds(?fractional=fractional))
    member _.TotalNanoseconds(?fractional) = apply (fun e -> e.Dt.TotalNanoseconds(?fractional=fractional))
    member _.Date() = apply (fun e -> e.Dt.Date())
    member _.Time() = apply (fun e -> e.Dt.Time())

    /// <summary> Format datetime to string using the given format string (strftime). </summary>
    member _.ToString(format: string) = 
        apply (fun e -> e.Dt.ToString format)

    /// <summary> Default ISO format. </summary>
    member this.ToString() = 
        this.ToString "%Y-%m-%dT%H:%M:%S%.f"
    member this.Strftime(format:string) = apply (fun e -> e.Dt.Strftime format)

    // --- Manipulation ---

    member _.Truncate(every: Dur) = 
        apply (fun e -> e.Dt.Truncate every)

    member _.Round(every: Dur) = 
        apply (fun e -> e.Dt.Round every)

    member _.OffsetBy(duration: Dur) =
        apply (fun e -> e.Dt.OffsetBy duration)

    // --- Conversion ---

    member _.Timestamp(?timeUnit) = apply (fun e -> e.Dt.Timestamp(?timeUnit=timeUnit))
    member _.Combine(time:Expr,?timeUnit:TimeUnit) = apply (fun e -> e.Dt.Combine(time,?timeUnit=timeUnit))

    // --- TimeZone ---

    member _.ConvertTimeZone(timeZone: string) =
        apply (fun e -> e.Dt.ConvertTimeZone timeZone)

    member _.ReplaceTimeZone(timeZone: string option, ?ambiguous: Expr, ?nonExistent: NonExistent) =
        apply (fun e -> e.Dt.ReplaceTimeZone(timeZone, ?ambiguous=ambiguous, ?nonExistent=nonExistent))

    member this.ReplaceTimeZone(timeZone: string, ?ambiguous, ?nonExistent) =
        this.ReplaceTimeZone(Some timeZone, ?ambiguous=ambiguous, ?nonExistent=nonExistent)

    // --- Business Days ---

    /// <summary>
    /// Add business days (using integer).
    /// </summary>
    member _.AddBusinessDays(n: int, ?weekMask, ?holidays, ?roll) =
        apply (fun e -> 
            e.Dt.AddBusinessDays(
                n, 
                ?weekMask=weekMask, 
                ?holidays=holidays, 
                ?roll=roll
            )
        )

    /// <summary>
    /// Is Business Day check.
    /// </summary>
    member _.IsBusinessDay(?weekMask, ?holidays) =
        apply (fun e -> 
            e.Dt.IsBusinessDay(?weekMask=weekMask, ?holidays=holidays)
        )
    member _.Epoch(?timeUnit) = apply (fun e -> e.Dt.Epoch(?timeUnit=timeUnit))
    member _.Replace(
        ?year:int,?month:int,?day:int,?hour:int,
        ?minute:int,?second:int,?microsecond:int,
        ?ambiguous:Expr) = 
        apply(fun e -> e.Dt.Replace(?year=year,?month=month,?day=day,
            ?hour=hour,?minute=minute,?second=second,?microsecond=microsecond,?ambiguous=ambiguous))