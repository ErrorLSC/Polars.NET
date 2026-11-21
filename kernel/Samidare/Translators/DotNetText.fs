namespace Kaminari.Samidare

open System
open System.Globalization
open System.Linq.Expressions
open Kaminari.Samidare.IR

/// .NET's text of a value (ToString(), the text a string concatenation, string.Format or Convert.ToString writes)
/// under a culture, for the types an engine can write as .NET does. The culture is the thread's current culture when
/// the query is compiled (part of the plan cache key), or the provider a call names.
[<AutoOpen>]
module internal DotNetText =

    /// The type whose ToString a string concatenation or string.Format calls: a boxed operand's own type.
    let operandTextType (e: Expression) : Type =
        match e with
        | :? UnaryExpression as u when u.NodeType = ExpressionType.Convert && u.Type = typeof<obj> -> u.Operand.Type
        | _ -> e.Type

    let private core (t: Type) = match Nullable.GetUnderlyingType t with null -> t | u -> u

    let private isInteger (t: Type) =
        match Type.GetTypeCode t with
        | TypeCode.SByte | TypeCode.Byte | TypeCode.Int16 | TypeCode.UInt16 | TypeCode.Int32 | TypeCode.UInt32
        | TypeCode.Int64 | TypeCode.UInt64 -> not t.IsEnum
        | _ -> false

    /// Whether ToString() of a value of type `t` (or its Nullable) translates natively.
    let isTextType (t: Type) =
        let t = core t
        t = typeof<double> || t = typeof<float32> || t = typeof<decimal> || t = typeof<bool> || isInteger t
        || t = typeof<DateTime> || t = typeof<DateOnly> || t = typeof<TimeOnly>

    /// The culture an IFormatProvider argument names, when it is a constant culture.
    let tryCultureOf (provider: Expression) : CultureInfo option =
        match PartialEvaluator.tryEvaluate provider with
        | Some(:? CultureInfo as culture) -> Some culture
        | _ -> None

    let private symbolsOf (culture: CultureInfo) =
        let nf = culture.NumberFormat
        {
            NegativeSign = nf.NegativeSign
            PositiveSign = nf.PositiveSign
            DecimalSeparator = nf.NumberDecimalSeparator
            NaN = nf.NaNSymbol
            PositiveInfinity = nf.PositiveInfinitySymbol
            NegativeInfinity = nf.NegativeInfinitySymbol
        }

    /// The translated value `s`, of type `t`, as .NET's ToString() writes it under `culture`, for a string
    /// concatenation, string.Format or ToString: floating values as .NET lays out their shortest round-trip digits;
    /// integers and decimals with the culture's signs and separator (a decimal keeps its column's scale, where .NET keeps
    /// each value's own); booleans as "True" / "False"; dates and times by the culture's general patterns (G, d, t).
    /// Values of other types are left as they are. None where the culture's text cannot be written natively.
    let asDotNetText (culture: CultureInfo) (t: Type) (s: ScalarExpr) : ScalarExpr option =
        let t = core t
        let symbols = symbolsOf culture
        let replace (from: string) (into: string) (e: ScalarExpr) =
            if from = into then e
            else
                ScalarExpr.Function(
                    ScalarFunction.ReplaceAll,
                    [ e; ScalarExpr.Literal(LiteralValue.String from); ScalarExpr.Literal(LiteralValue.String into); ScalarExpr.Literal(LiteralValue.Bool true) ],
                    LogicalTypes.String)
        let asText (e: ScalarExpr) = ScalarExpr.Cast(e, LogicalTypes.String, false)
        // A date's text where the backend writes it as .NET does (calendars, names, specifiers it may not have).
        let dated (format: string) =
            match CompilationScope.TryCurrent with
            | Some scope when scope.Backend.IsDateTextSupported(format, culture.Name) ->
                Some(ScalarExpr.Function(ScalarFunction.DtToText(format, culture.Name), [ s ], LogicalTypes.String))
            | _ -> None
        if t = typeof<double> || t = typeof<float32> then
            Some(ScalarExpr.Function(ScalarFunction.FloatToText symbols, [ s ], LogicalTypes.String))
        elif t = typeof<bool> then
            Some(ScalarExpr.Conditional(s, ScalarExpr.Literal(LiteralValue.String "True"), ScalarExpr.Literal(LiteralValue.String "False")))
        elif isInteger t then
            Some(asText s |> replace "-" symbols.NegativeSign)
        elif t = typeof<decimal> then
            // The separator first: a negative sign may hold a dot.
            Some(asText s |> replace "." symbols.DecimalSeparator |> replace "-" symbols.NegativeSign)
        elif t = typeof<DateTime> then
            dated (culture.DateTimeFormat.ShortDatePattern + " " + culture.DateTimeFormat.LongTimePattern)
        elif t = typeof<DateOnly> then
            dated culture.DateTimeFormat.ShortDatePattern
        elif t = typeof<TimeOnly> then
            dated culture.DateTimeFormat.ShortTimePattern
        else
            Some s
