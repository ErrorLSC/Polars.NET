namespace Polars.FSharp


type SeriesDtNameSpace(parent: Series) =
    
    // Helper: col("Name").Dt.Op(...)
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    // --- Extraction ---
    
    member _.Year() = apply (fun e -> e.Dt.Year())
    member _.Quarter() = apply (fun e -> e.Dt.Quarter())
    member _.Month() = apply (fun e -> e.Dt.Month())
    member _.Day() = apply (fun e -> e.Dt.Day())
    member _.Hour() = apply (fun e -> e.Dt.Hour())
    member _.Minute() = apply (fun e -> e.Dt.Minute())
    member _.Second() = apply (fun e -> e.Dt.Second())
    member _.Millisecond() = apply (fun e -> e.Dt.Millisecond())
    member _.Microsecond() = apply (fun e -> e.Dt.Microsecond())
    member _.Nanosecond() = apply (fun e -> e.Dt.Nanosecond())
    member _.OrdinalDay() = apply (fun e -> e.Dt.OrdinalDay())
    member _.Weekday() = apply (fun e -> e.Dt.Weekday())
    member _.Date() = apply (fun e -> e.Dt.Date())
    member _.Time() = apply (fun e -> e.Dt.Time())

    // --- Formatting ---

    /// <summary> Format datetime to string using the given format string (strftime). </summary>
    member _.ToString(format: string) = 
        apply (fun e -> e.Dt.ToString format)

    /// <summary> Default ISO format. </summary>
    member this.ToString() = 
        this.ToString "%Y-%m-%dT%H:%M:%S%.f"

    // --- Manipulation ---

    member _.Truncate(every: string) = 
        apply (fun e -> e.Dt.Truncate every)

    member _.Round(every: string) = 
        apply (fun e -> e.Dt.Round every)

    member _.OffsetBy(duration: string) =
        apply (fun e -> e.Dt.OffsetBy duration)
    member _.OffsetBy(duration: Expr) =
        apply (fun e -> e.Dt.OffsetBy duration)

    // --- Conversion ---

    member _.TimestampMicros() = apply (fun e -> e.Dt.TimestampMicros())
    member _.TimestampMillis() = apply (fun e -> e.Dt.TimestampMillis())
    member _.Combine(time:Expr,timeUnit:TimeUnit) = apply (fun e -> e.Dt.Combine(time,timeUnit))

    // --- TimeZone ---

    member _.ConvertTimeZone(timeZone: string) =
        apply (fun e -> e.Dt.ConvertTimeZone timeZone)

    member _.ReplaceTimeZone(timeZone: string option, ?ambiguous: string, ?nonExistent: string) =
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