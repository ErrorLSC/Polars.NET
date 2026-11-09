namespace Kaminari.Samidare

open Kaminari.Samidare.IR
open System
open System.Linq.Expressions
open Kaminari.Sazanami.Helpers

[<AutoOpen>]
module internal MemberAccessTranslator =
    let internal translateMemberAccess
        (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (memberExpr: Expression)
        : ScalarExpr option =
        match tryResolveColumnNameWithContext ctx memberExpr with
        | Some actualCol -> Some(ScalarExpr.Column actualCol)
        | None ->
            match memberExpr with
            | MemberAccess(inner, m) when isNullableValue m ->
                eval ctx inner

            | MemberAccess(null, m) when m.DeclaringType = typeof<Math> || m.DeclaringType = typeof<MathF> ->
                match m.Name with
                | "PI" -> Some(ScalarExpr.Literal(LiteralValue.Float64 Math.PI))
                | "E" -> Some(ScalarExpr.Literal(LiteralValue.Float64 Math.E))
                | "Tau" -> Some(ScalarExpr.Literal(LiteralValue.Float64(Math.PI * 2.0)))
                | _ -> None

            | MemberAccess(target, m) when not (isNull target) && target.Type = typeof<string> && m.Name = "Length" ->
                // C# Length is an int (the engine counts in u32, which cannot be negated).
                eval ctx target |> Option.map (fun t -> ScalarExpr.Cast(ScalarExpr.Function(ScalarFunction.LenChars, [ t ], LogicalTypes.UInt32), LogicalTypes.Int32, false))

            | MemberAccess(target, m) when not (isNull target) && ClrTypeHelper.IsTemporalType target.Type && target.Type <> typeof<TimeSpan> ->
                let t () = eval ctx target

                match m.Name with
                | "Year" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtYear, [ x ], LogicalTypes.Int32))
                | "Month" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtMonth, [ x ], LogicalTypes.Int32))
                | "Day" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtDay, [ x ], LogicalTypes.Int32))
                | "DayOfYear" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtOrdinalDay, [ x ], LogicalTypes.Int32))
                | "DayOfWeek" ->
                    t ()
                    |> Option.bind (fun dtH ->
                        let weekday = ScalarExpr.Function(ScalarFunction.DtWeekday, [ dtH ], LogicalTypes.Int8)
                        let weekdayInt = ScalarExpr.Cast(weekday, LogicalTypes.Int32, isStrict = false)
                        Some(ScalarExpr.Binary(BinaryOp.Modulo, weekdayInt, ScalarExpr.Literal(LiteralValue.Int32 7))))
                | "Hour" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtHour, [ x ], LogicalTypes.Int32))
                | "Minute" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtMinute, [ x ], LogicalTypes.Int32))
                | "Second" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtSecond, [ x ], LogicalTypes.Int32))
                | "Millisecond" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtMillisecond, [ x ], LogicalTypes.Int32))
                | "Microsecond" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtMicrosecond, [ x ], LogicalTypes.Int32))
                | "Nanosecond" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtNanosecond, [ x ], LogicalTypes.Int32))
                | "Date" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtDate, [ x ], LogicalTypes.Date))
                | "TimeOfDay" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTime, [ x ], LogicalTypes.Time))
                | _ -> None

            | MemberAccess(target, m) when not (isNull target) && target.Type = typeof<TimeSpan> ->
                let t () = eval ctx target

                // A TimeSpan component is its whole count of the unit, truncated, with the carry into the next unit removed.
                let part (total: ScalarFunction) (unitsPerNext: int64 option) =
                    t ()
                    |> Option.map (fun x ->
                        let whole = ScalarExpr.Cast(ScalarExpr.Function(total, [ x ], LogicalTypes.Float64), LogicalTypes.Int64, false)
                        let value =
                            match unitsPerNext with
                            | Some n -> ScalarExpr.Binary(BinaryOp.Modulo, whole, ScalarExpr.Literal(LiteralValue.Int64 n))
                            | None -> whole
                        ScalarExpr.Cast(value, LogicalTypes.Int32, false))

                match m.Name with
                | "TotalDays" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalDays, [ x ], LogicalTypes.Float64))
                | "TotalHours" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalHours, [ x ], LogicalTypes.Float64))
                | "TotalMinutes" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalMinutes, [ x ], LogicalTypes.Float64))
                | "TotalSeconds" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalSeconds, [ x ], LogicalTypes.Float64))
                | "TotalMilliseconds" ->
                    t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalMilliseconds, [ x ], LogicalTypes.Float64))
                | "TotalMicroseconds" ->
                    t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalMicroseconds, [ x ], LogicalTypes.Float64))
                | "TotalNanoseconds" ->
                    t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalNanoseconds, [ x ], LogicalTypes.Float64))
                | "Days" -> part ScalarFunction.DtTotalDays None
                | "Hours" -> part ScalarFunction.DtTotalHours (Some 24L)
                | "Minutes" -> part ScalarFunction.DtTotalMinutes (Some 60L)
                | "Seconds" -> part ScalarFunction.DtTotalSeconds (Some 60L)
                | "Milliseconds" -> part ScalarFunction.DtTotalMilliseconds (Some 1000L)
                | "Microseconds" -> part ScalarFunction.DtTotalMicroseconds (Some 1000L)
                | "Nanoseconds" -> part ScalarFunction.DtTotalNanoseconds (Some 1000L)
                | _ -> None

            // Translate Nullable<T>.HasValue directly to is_not_null()
            | MemberAccess(target, m) when isNullableHasValue m ->
                match eval ctx target with
                | Some colH -> Some(ScalarExpr.Unary(UnaryOp.IsNotNull, colH))
                | None ->
                    match ctx with
                    | TranslationContext.TupleJoin(pairParam, leftCols, _) ->
                        match isTupleAccess pairParam target with
                        | Some true ->
                            leftCols
                            |> Seq.tryHead
                            |> Option.map (fun c -> ScalarExpr.Unary(UnaryOp.IsNotNull, ScalarExpr.Column c))
                        | _ -> None
                    | _ ->
                        // A Nullable<struct> entity (an outer join's side, `(T?)x`): present when its row is.
                        tryResolveEntityNullTarget ctx target
                        |> Option.map (fun x -> ScalarExpr.Unary(UnaryOp.IsNotNull, x))

            | _ ->
                PartialEvaluator.tryEvaluate memberExpr
                |> Option.map (fun value -> ScalarExprOps.toLiteral value memberExpr.Type)