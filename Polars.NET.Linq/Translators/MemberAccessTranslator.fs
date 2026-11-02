namespace Polars.NET.Linq

open Polars.NET.Core
open System
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core.Helpers

[<AutoOpen>]
module internal MemberAccessTranslator =


    let private isTemporalType (t: Type) =
        t = typeof<DateTime>
        || t = typeof<DateTimeOffset>
        || t = typeof<DateOnly>
        || t = typeof<TimeOnly>


    let internal translateMemberAccess
        (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (memberExpr: Expression)
        : ExprHandle option =
        match tryResolveColumnNameWithContext ctx memberExpr with
        | Some actualCol -> Some(PolarsWrapper.Col actualCol)
        | None ->
            match memberExpr with
            | MemberAccess(inner, m) when isNullableValue m ->
                eval ctx inner

            | MemberAccess(null, m) when m.DeclaringType = typeof<Math> || m.DeclaringType = typeof<MathF> ->
                match m.Name with
                | "PI" -> Some(PolarsWrapper.Lit Math.PI)
                | "E" -> Some(PolarsWrapper.Lit Math.E)
                | "Tau" -> Some(PolarsWrapper.Lit(Math.PI * 2.0))
                | _ -> None

            | MemberAccess(target, m) when not (isNull target) && target.Type = typeof<string> && m.Name = "Length" ->
                eval ctx target |> Option.map PolarsWrapper.StrLenChars

            | MemberAccess(target, m) when not (isNull target) && isTemporalType target.Type ->
                let t () = eval ctx target

                match m.Name with
                | "Year" -> t () |> Option.map PolarsWrapper.DtYear
                | "Month" -> t () |> Option.map PolarsWrapper.DtMonth
                | "Day" -> t () |> Option.map PolarsWrapper.DtDay
                | "DayOfYear" -> t () |> Option.map PolarsWrapper.DtOrdinalDay
                | "DayOfWeek" ->
                    // Polars dt.weekday() returns ISO weekday (1..7, Monday=1, Sunday=7) as Int8.
                    // .NET System.DayOfWeek defines Sunday=0, Monday=1..Saturday=6, with underlying type Int32.
                    // We cast to Int32 and compute (weekday % 7) to:
                    // 1. Map Sunday (7 % 7 = 0) to DayOfWeek.Sunday and preserve Monday..Saturday (1..6).
                    // 2. Ensure the Arrow column type is Int32 so CLR materializer can cleanly unbox it to DayOfWeek.
                    t ()
                    |> Option.bind (fun dtH ->
                        let weekdayH = PolarsWrapper.DtWeekday dtH

                        match tryTranslateCast typeof<int> weekdayH with
                        | Some weekdayInt -> Some(PolarsWrapper.Rem(weekdayInt, PolarsWrapper.Lit 7))
                        | None ->
                            let rem = PolarsWrapper.Rem(weekdayH, PolarsWrapper.Lit 7y)
                            tryTranslateCast typeof<int> rem)
                | "Hour" -> t () |> Option.map PolarsWrapper.DtHour
                | "Minute" -> t () |> Option.map PolarsWrapper.DtMinute
                | "Second" -> t () |> Option.map PolarsWrapper.DtSecond
                | "Millisecond" -> t () |> Option.map PolarsWrapper.DtMillisecond
                | "Microsecond" -> t () |> Option.map PolarsWrapper.DtMicrosecond
                | "Nanosecond" -> t () |> Option.map PolarsWrapper.DtNanosecond
                | "Date" -> t () |> Option.map PolarsWrapper.DtDate
                | "TimeOfDay" -> t () |> Option.map PolarsWrapper.DtTime
                | _ -> None

            | MemberAccess(target, m) when not (isNull target) && target.Type = typeof<TimeSpan> ->
                let t () = eval ctx target

                match m.Name with
                | "TotalDays" -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalDays(x, fractional = true))
                | "TotalHours" -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalHours(x, fractional = true))
                | "TotalMinutes" -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalMinutes(x, fractional = true))
                | "TotalSeconds" -> t () |> Option.map (fun x -> PolarsWrapper.DtTotalSeconds(x, fractional = true))
                | "TotalMilliseconds" ->
                    t ()
                    |> Option.map (fun x -> PolarsWrapper.DtTotalMilliseconds(x, fractional = true))
                | "TotalMicroseconds" ->
                    t ()
                    |> Option.map (fun x -> PolarsWrapper.DtTotalMicroseconds(x, fractional = true))
                | "TotalNanoseconds" ->
                    t ()
                    |> Option.map (fun x -> PolarsWrapper.DtTotalNanoseconds(x, fractional = true))
                | "Days" -> t () |> Option.map PolarsWrapper.DtDay
                | "Hours" -> t () |> Option.map PolarsWrapper.DtHour
                | "Minutes" -> t () |> Option.map PolarsWrapper.DtMinute
                | "Seconds" -> t () |> Option.map PolarsWrapper.DtSecond
                | "Milliseconds" -> t () |> Option.map PolarsWrapper.DtMillisecond
                | "Microseconds" -> t () |> Option.map PolarsWrapper.DtMicrosecond
                | "Nanoseconds" -> t () |> Option.map PolarsWrapper.DtNanosecond
                | _ -> None

            // Translate Nullable<T>.HasValue directly to Polars is_not_null()
            | MemberAccess(target, m) when isNullableHasValue m ->
                match eval ctx target with
                | Some colH -> Some(PolarsWrapper.IsNotNull colH)
                | None ->
                    match ctx with
                    | TranslationContext.TupleJoin(pairParam, leftCols, _) ->
                        match isTupleAccess pairParam target with
                        | Some true ->
                            // Left side of join: picking any valid left column detects whether Outer row exists
                            leftCols
                            |> Seq.tryHead
                            |> Option.map (fun c -> PolarsWrapper.IsNotNull(PolarsWrapper.Col c))
                        | _ -> None
                    | _ -> None

            | _ ->
                tryEvaluate memberExpr
                |> Option.map (fun value -> toLiteralHandle value memberExpr.Type)

    // =========================================================================
