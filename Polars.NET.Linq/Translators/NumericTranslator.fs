namespace Polars.NET.Linq

open Polars.NET.Core
open Polars.NET.Linq.IR
open System
open System.Linq.Expressions
open System.Reflection

[<AutoOpen>]
module internal NumericTranslator =

    let private tryMapRoundMode (mode: MidpointRounding) : PlRoundMode option =
        match mode with
        | MidpointRounding.AwayFromZero -> Some PlRoundMode.HalfAwayFromZero
        | MidpointRounding.ToEven -> Some PlRoundMode.HalfToEven
        | MidpointRounding.ToZero -> Some PlRoundMode.ToZero
        | _ -> None

    let private tryExtractRoundMode (expr: Expression) : PlRoundMode option =
        match tryEvaluate expr with
        | Some (:? MidpointRounding as m) -> tryMapRoundMode m
        | Some (:? PlRoundMode as m) -> Some m
        | Some other when expr.Type = typeof<MidpointRounding> ->
            try
                let intVal = Convert.ToInt32 other
                let m = LanguagePrimitives.EnumOfValue<int, MidpointRounding> intVal
                tryMapRoundMode m
            with _ -> None
        | Some other when expr.Type = typeof<PlRoundMode> ->
            try
                let byteVal = Convert.ToByte other
                let m = LanguagePrimitives.EnumOfValue<byte, PlRoundMode> byteVal
                Some m
            with _ -> None
        | _ -> None

    let private tryExtractDecimals (expr: Expression) : int option =
        match tryEvaluate expr with
        | Some d ->
            try
                Some(Convert.ToInt32 d)
            with _ -> None
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
        | "Ceiling", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("ceil", [ a ], PlDataType.Unknown))
        | "Floor", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("floor", [ a ], PlDataType.Unknown))
        | "Truncate", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("truncate", [ a ], PlDataType.Unknown))
        | "Sign", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("sign", [ a ], PlDataType.Unknown))
        | "Round", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("round", [ a ], PlDataType.Unknown))
        | "Round", [ x; arg2 ] ->
            match t1 x with
            | None -> None
            | Some a ->
                match tryExtractRoundMode arg2 with
                | Some plMode ->
                    Some(ScalarExpr.Function("round", [ a; ScalarExpr.Literal(LiteralValue.Int32 0); ScalarExpr.Literal(LiteralValue.Int32 (int plMode)) ], PlDataType.Unknown))
                | None ->
                    match tryExtractDecimals arg2 with
                    | Some decimals ->
                        Some(ScalarExpr.Function("round", [ a; ScalarExpr.Literal(LiteralValue.Int32 decimals) ], PlDataType.Unknown))
                    | None -> None
        | "Round", [ x; digitsExpr; modeExpr ] ->
            match t1 x, tryExtractDecimals digitsExpr, tryExtractRoundMode modeExpr with
            | Some a, Some decimals, Some plMode ->
                Some(ScalarExpr.Function("round", [ a; ScalarExpr.Literal(LiteralValue.Int32 decimals); ScalarExpr.Literal(LiteralValue.Int32 (int plMode)) ], PlDataType.Unknown))
            | _ -> None
        | "Min", [ x; y ] ->
            t2 x y
            |> Option.map (fun (l, r) ->
                let cond = ScalarExpr.Binary(BinaryOp.LtEq, l, r)
                ScalarExpr.Conditional(cond, l, r))
        | "Max", [ x; y ] ->
            t2 x y
            |> Option.map (fun (l, r) ->
                let cond = ScalarExpr.Binary(BinaryOp.GtEq, l, r)
                ScalarExpr.Conditional(cond, l, r))
        | "Clamp", [ valExpr; minExpr; maxExpr ] ->
            match t1 valExpr, t1 minExpr, t1 maxExpr with
            | Some v, Some mn, Some mx -> Some(ScalarExpr.Function("clamp", [ v; mn; mx ], PlDataType.Unknown))
            | _ -> None
        | "Sqrt", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("sqrt", [ a ], PlDataType.Float64))
        | "Cbrt", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("cbrt", [ a ], PlDataType.Float64))
        | "Exp", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("exp", [ a ], PlDataType.Float64))
        | "Log", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("log", [ a ], PlDataType.Float64))
        | "Log10", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("log", [ a; ScalarExpr.Literal(LiteralValue.Float64 10.0) ], PlDataType.Float64))
        | "Log2", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("log", [ a; ScalarExpr.Literal(LiteralValue.Float64 2.0) ], PlDataType.Float64))
        | "Log", [ x; newBase ] -> t2 x newBase |> Option.map (fun (a, b) -> ScalarExpr.Function("log", [ a; b ], PlDataType.Float64))
        | "Sin", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("sin", [ a ], PlDataType.Float64))
        | "Cos", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("cos", [ a ], PlDataType.Float64))
        | "Tan", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("tan", [ a ], PlDataType.Float64))
        | "Asin", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("asin", [ a ], PlDataType.Float64))
        | "Acos", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("acos", [ a ], PlDataType.Float64))
        | "Atan", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("atan", [ a ], PlDataType.Float64))
        | "Sinh", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("sinh", [ a ], PlDataType.Float64))
        | "Cosh", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("cosh", [ a ], PlDataType.Float64))
        | "Tanh", [ x ] -> t1 x |> Option.map (fun a -> ScalarExpr.Function("tanh", [ a ], PlDataType.Float64))
        | "Pow", [ x; y ] -> t2 x y |> Option.map (fun (a, b) -> ScalarExpr.Binary(BinaryOp.Power, a, b))
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
