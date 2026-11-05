namespace Polars.NET.Linq

open Polars.NET.Linq.IR
open System
open System.Linq.Expressions
open Polars.NET.Core.Helpers

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
                eval ctx target |> Option.map (fun t -> ScalarExpr.Function(ScalarFunction.LenChars, [ t ], PlDataType.UInt32))

            | MemberAccess(target, m) when not (isNull target) && PolarsTypeHelper.IsTemporalType target.Type ->
                let t () = eval ctx target

                match m.Name with
                | "Year" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtYear, [ x ], PlDataType.Int32))
                | "Month" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtMonth, [ x ], PlDataType.Int32))
                | "Day" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtDay, [ x ], PlDataType.Int32))
                | "DayOfYear" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtOrdinalDay, [ x ], PlDataType.Int32))
                | "DayOfWeek" ->
                    t ()
                    |> Option.bind (fun dtH ->
                        let weekday = ScalarExpr.Function(ScalarFunction.DtWeekday, [ dtH ], PlDataType.Int8)
                        let weekdayInt = ScalarExpr.Cast(weekday, PlDataType.Int32, isStrict = false)
                        Some(ScalarExpr.Binary(BinaryOp.Modulo, weekdayInt, ScalarExpr.Literal(LiteralValue.Int32 7))))
                | "Hour" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtHour, [ x ], PlDataType.Int32))
                | "Minute" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtMinute, [ x ], PlDataType.Int32))
                | "Second" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtSecond, [ x ], PlDataType.Int32))
                | "Millisecond" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtMillisecond, [ x ], PlDataType.Int32))
                | "Microsecond" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtMicrosecond, [ x ], PlDataType.Int32))
                | "Nanosecond" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtNanosecond, [ x ], PlDataType.Int32))
                | "Date" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtDate, [ x ], PlDataType.Date))
                | "TimeOfDay" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTime, [ x ], PlDataType.Time))
                | _ -> None

            | MemberAccess(target, m) when not (isNull target) && target.Type = typeof<TimeSpan> ->
                let t () = eval ctx target

                match m.Name with
                | "TotalDays" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalDays, [ x ], PlDataType.Float64))
                | "TotalHours" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalHours, [ x ], PlDataType.Float64))
                | "TotalMinutes" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalMinutes, [ x ], PlDataType.Float64))
                | "TotalSeconds" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalSeconds, [ x ], PlDataType.Float64))
                | "TotalMilliseconds" ->
                    t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalMilliseconds, [ x ], PlDataType.Float64))
                | "TotalMicroseconds" ->
                    t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalMicroseconds, [ x ], PlDataType.Float64))
                | "TotalNanoseconds" ->
                    t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtTotalNanoseconds, [ x ], PlDataType.Float64))
                | "Days" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtDay, [ x ], PlDataType.Int32))
                | "Hours" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtHour, [ x ], PlDataType.Int32))
                | "Minutes" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtMinute, [ x ], PlDataType.Int32))
                | "Seconds" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtSecond, [ x ], PlDataType.Int32))
                | "Milliseconds" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtMillisecond, [ x ], PlDataType.Int32))
                | "Microseconds" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtMicrosecond, [ x ], PlDataType.Int32))
                | "Nanoseconds" -> t () |> Option.map (fun x -> ScalarExpr.Function(ScalarFunction.DtNanosecond, [ x ], PlDataType.Int32))
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
                    | _ -> None

            | _ ->
                tryEvaluate memberExpr
                |> Option.map (fun value -> ScalarExprOps.toLiteral value memberExpr.Type)