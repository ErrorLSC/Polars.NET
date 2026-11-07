namespace Polars.NET.Linq

open Polars.NET.Core.Enums
open Polars.NET.Linq.IR
open System
open System.Linq.Expressions
open System.Reflection

[<AutoOpen>]
module internal NumericTranslator =

    let private tryMapRoundMode (mode: MidpointRounding) : RoundMode option =
        match mode with
        | MidpointRounding.AwayFromZero -> Some RoundMode.HalfAwayFromZero
        | MidpointRounding.ToEven -> Some RoundMode.HalfToEven
        | MidpointRounding.ToZero -> Some RoundMode.ToZero
        | _ -> None

    /// Maps the user-facing Polars round mode to the IR vocabulary.
    let private ofPlRoundMode (mode: PlRoundMode) : RoundMode option =
        match mode with
        | PlRoundMode.HalfAwayFromZero -> Some RoundMode.HalfAwayFromZero
        | PlRoundMode.HalfToEven -> Some RoundMode.HalfToEven
        | PlRoundMode.ToZero -> Some RoundMode.ToZero
        | _ -> None

    let private tryExtractRoundMode (expr: Expression) : RoundMode option =
        match PartialEvaluator.tryEvaluate expr with
        | Some (:? MidpointRounding as m) -> tryMapRoundMode m
        | Some (:? PlRoundMode as m) -> ofPlRoundMode m
        | Some other when expr.Type = typeof<MidpointRounding> ->
            try
                let intVal = Convert.ToInt32 other
                let m = LanguagePrimitives.EnumOfValue<int, MidpointRounding> intVal
                tryMapRoundMode m
            with ConversionFailure -> None
        | Some other when expr.Type = typeof<PlRoundMode> ->
            try
                let byteVal = Convert.ToByte other
                let m = LanguagePrimitives.EnumOfValue<byte, PlRoundMode> byteVal
                ofPlRoundMode m
            with ConversionFailure -> None
        | _ -> None

    let private tryExtractDecimals (expr: Expression) : int option =
        match PartialEvaluator.tryEvaluate expr with
        | Some d ->
            try
                Some(Convert.ToInt32 d)
            with ConversionFailure -> None
        | None -> None

    let internal isNumericDeclaringType (t: Type) =
        not (isNull t)
        && (t = typeof<Math>
            || t = typeof<MathF>
            || t = typeof<decimal>
            || t = typeof<System.Numerics.BitOperations>)

    let internal translateNumericMethod
        (translateExpr: Expression -> ScalarExpr option)
        (m: MethodInfo)
        (args: Expression list)
        : ScalarExpr option =
        let t1 expr = translateExpr expr

        let t2 e1 e2 =
            match translateExpr e1, translateExpr e2 with
            | Some l, Some r -> Some(l, r)
            | _ -> None

        match m.Name, args with
        | "Abs", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Unary(UnaryOp.Abs, a))
        | "Ceiling", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Ceil, [ a ], LogicalType.Unknown))
        | "Floor", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Floor, [ a ], LogicalType.Unknown))
        | "Truncate", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Truncate, [ a ], LogicalType.Unknown))
        | "Sign", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Sign, [ a ], LogicalType.Unknown))
        | "Round", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Round RoundMode.HalfToEven, [ a ], LogicalType.Unknown))
        | "Round", [ x; arg2 ] ->
            match t1 x with
            | None -> None
            | Some a ->
                match tryExtractRoundMode arg2 with
                | Some plMode ->
                    Some(ScalarExpr.Function(ScalarFunction.Round plMode, [ a; ScalarExpr.Literal(LiteralValue.Int32 0) ], LogicalType.Unknown))
                | None ->
                    match tryExtractDecimals arg2 with
                    | Some decimals ->
                        Some(ScalarExpr.Function(ScalarFunction.Round RoundMode.HalfToEven, [ a; ScalarExpr.Literal(LiteralValue.Int32 decimals) ], LogicalType.Unknown))
                    | None -> None
        | "Round", [ x; digitsExpr; modeExpr ] ->
            match t1 x, tryExtractDecimals digitsExpr, tryExtractRoundMode modeExpr with
            | Some a, Some decimals, Some plMode ->
                Some(ScalarExpr.Function(ScalarFunction.Round plMode, [ a; ScalarExpr.Literal(LiteralValue.Int32 decimals) ], LogicalType.Unknown))
            | _ -> None
        | "Min", [ x; y ] ->
            t2 x y
            |> Option.map (fun (l, r) ->
                let cond = ScalarExpr.Binary(BinaryOp.LtEq, l, r)
                let minimum = ScalarExpr.Conditional(cond, l, r)
                // Math.Min is NaN when either operand is (the engine orders NaN above every number, so `<=` alone
                // would pick the number); NaN + x is NaN.
                if NaNSemantics.isFloatingClrType x.Type then
                    let eitherNaN = ScalarExpr.Binary(BinaryOp.Or, ScalarExpr.Unary(UnaryOp.IsNan, l), ScalarExpr.Unary(UnaryOp.IsNan, r))
                    ScalarExpr.Conditional(eitherNaN, ScalarExpr.Binary(BinaryOp.Add, l, r), minimum)
                else
                    minimum)
        | "Max", [ x; y ] ->
            t2 x y
            |> Option.map (fun (l, r) ->
                let cond = ScalarExpr.Binary(BinaryOp.GtEq, l, r)
                ScalarExpr.Conditional(cond, l, r))
        | "Clamp", [ valExpr; minExpr; maxExpr ] ->
            match t1 valExpr, t1 minExpr, t1 maxExpr with
            | Some v, Some mn, Some mx -> Some(ScalarExpr.Function(ScalarFunction.Clamp, [ v; mn; mx ], LogicalType.Unknown))
            | _ -> None
        | "Sqrt", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Sqrt, [ a ], LogicalTypes.Float64))
        | "Cbrt", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Cbrt, [ a ], LogicalTypes.Float64))
        | "Exp", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Exp, [ a ], LogicalTypes.Float64))
        | "Log", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Log, [ a ], LogicalTypes.Float64))
        | "Log10", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Log, [ a; ScalarExpr.Literal(LiteralValue.Float64 10.0) ], LogicalTypes.Float64))
        | "Log2", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Log, [ a; ScalarExpr.Literal(LiteralValue.Float64 2.0) ], LogicalTypes.Float64))
        | "Log", [ x; newBase ] -> t2 x newBase |> Option.map (fun (a, b) -> ScalarExpr.Function(ScalarFunction.Log, [ a; b ], LogicalTypes.Float64))
        | "Sin", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Sin, [ a ], LogicalTypes.Float64))
        | "Cos", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Cos, [ a ], LogicalTypes.Float64))
        | "Tan", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Tan, [ a ], LogicalTypes.Float64))
        | "Asin", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Asin, [ a ], LogicalTypes.Float64))
        | "Acos", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Acos, [ a ], LogicalTypes.Float64))
        | "Atan", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Atan, [ a ], LogicalTypes.Float64))
        | "Atan2", [ y; x ] -> t2 y x |> Option.map (fun (a, b) -> ScalarExpr.Function(ScalarFunction.Atan2, [ a; b ], LogicalTypes.Float64))
        | "Sinh", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Sinh, [ a ], LogicalTypes.Float64))
        | "Cosh", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Cosh, [ a ], LogicalTypes.Float64))
        | "Tanh", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.Tanh, [ a ], LogicalTypes.Float64))
        | "Pow", [ x; y ] -> t2 x y |> Option.map (fun (a, b) -> ScalarExpr.Binary(BinaryOp.Power, a, b))
        | "PopCount", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.BitwiseCountOnes, [ a ], LogicalTypes.UInt32))
        | "LeadingZeroCount", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.BitwiseLeadingZeros, [ a ], LogicalTypes.UInt32))
        | "TrailingZeroCount", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function(ScalarFunction.BitwiseTrailingZeros, [ a ], LogicalTypes.UInt32))
        | "ToInt32", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<int>)
        | "ToUInt32", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<uint32>)
        | "ToInt64", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<int64>)
        | "ToUInt64", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<uint64>)
        | "ToSingle", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<float32>)
        | "ToDouble", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<double>)
        | "ToByte", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<byte>)
        | "ToSByte", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<sbyte>)
        | "ToInt16", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<int16>)
        | "ToUInt16", [ x ] -> t1 x |> Option.bind (tryTranslateCast typeof<uint16>)
        | _ -> None
