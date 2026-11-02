namespace Polars.NET.Linq

open Polars.NET.Core
open System
open System.Linq.Expressions
open System.Reflection

[<AutoOpen>]
module internal NumericTranslator =

    let private tryMapRoundMode (midpoint: MidpointRounding) : PlRoundMode option =
        match midpoint with
        | MidpointRounding.ToEven -> Some PlRoundMode.HalfToEven
        | MidpointRounding.AwayFromZero -> Some PlRoundMode.HalfAwayFromZero
        | MidpointRounding.ToZero -> Some PlRoundMode.ToZero
        | _ -> None

    let internal isNumericDeclaringType (t: Type) =
        not (isNull t)
        && (t = typeof<Math>
            || t = typeof<MathF>
            || t = typeof<decimal>
            || t = typeof<System.Numerics.BitOperations>)

    let internal translateNumericMethodWithTranslator
        (translateExpr: Expression -> ExprHandle option)
        (m: MethodInfo)
        (args: Expression list)
        : ExprHandle option =
        let t1 expr = translateExpr expr

        let t2 e1 e2 =
            match translateExpr e1, translateExpr e2 with
            | Some l, Some r -> Some(l, r)
            | _ -> None

        match m.Name, args with
        | "Abs", [ x ] -> t1 x |> Option.map PolarsWrapper.Abs
        | "Ceiling", [ x ] -> t1 x |> Option.map PolarsWrapper.Ceil
        | "Floor", [ x ] -> t1 x |> Option.map PolarsWrapper.Floor
        | "Truncate", [ x ] -> t1 x |> Option.map (fun h -> PolarsWrapper.Truncate(h, 0u))
        | "Sign", [ x ] -> t1 x |> Option.map PolarsWrapper.Sign

        | "Round", [ x ] -> t1 x |> Option.map (fun h -> PolarsWrapper.Round(h, 0u, PlRoundMode.HalfToEven))

        | "Round", [ x; digitsExpr ] when digitsExpr.Type = typeof<int> || digitsExpr.Type = typeof<uint32> ->
            match t1 x, tryEvaluate digitsExpr with
            | Some h, Some d ->
                let decimals = uint32 (Convert.ToInt32 d)
                Some(PolarsWrapper.Round(h, decimals, PlRoundMode.HalfToEven))
            | _ -> None

        | "Round", [ x; modeExpr ] when modeExpr.Type = typeof<MidpointRounding> ->
            match t1 x, tryEvaluate modeExpr with
            | Some h, Some(:? MidpointRounding as mode) ->
                tryMapRoundMode mode
                |> Option.map (fun plMode -> PolarsWrapper.Round(h, 0u, plMode))
            | _ -> None

        | "Round", [ x; digitsExpr; modeExpr ] ->
            match t1 x, tryEvaluate digitsExpr, tryEvaluate modeExpr with
            | Some h, Some d, Some(:? MidpointRounding as mode) ->
                let decimals = uint32 (Convert.ToInt32 d)

                tryMapRoundMode mode
                |> Option.map (fun plMode -> PolarsWrapper.Round(h, decimals, plMode))
            | _ -> None

        | "Min", [ x; y ] ->
            t2 x y
            |> Option.map (fun (l, r) ->
                let cond = PolarsWrapper.LtEq(PolarsWrapper.CloneExpr l, PolarsWrapper.CloneExpr r)
                PolarsWrapper.IfElse(cond, l, r))

        | "Max", [ x; y ] ->
            t2 x y
            |> Option.map (fun (l, r) ->
                let cond = PolarsWrapper.GtEq(PolarsWrapper.CloneExpr l, PolarsWrapper.CloneExpr r)
                PolarsWrapper.IfElse(cond, l, r))

        | "Clamp", [ valExpr; minExpr; maxExpr ] ->
            match t1 valExpr, t1 minExpr, t1 maxExpr with
            | Some v, Some minH, Some maxH -> Some(PolarsWrapper.Clip(v, minH, maxH))
            | _ -> None

        | "Sqrt", [ x ] -> t1 x |> Option.map PolarsWrapper.Sqrt
        | "Cbrt", [ x ] -> t1 x |> Option.map PolarsWrapper.Cbrt
        | "Exp", [ x ] -> t1 x |> Option.map PolarsWrapper.Exp
        | "Log", [ x ] -> t1 x |> Option.map (fun h -> PolarsWrapper.Log(h, PolarsWrapper.Lit Math.E))
        | "Log10", [ x ] -> t1 x |> Option.map (fun h -> PolarsWrapper.Log(h, PolarsWrapper.Lit 10.0))
        | "Log2", [ x ] -> t1 x |> Option.map (fun h -> PolarsWrapper.Log(h, PolarsWrapper.Lit 2.0))
        | "Log", [ x; newBase ] -> t2 x newBase |> Option.map PolarsWrapper.Log

        | "Sin", [ x ] -> t1 x |> Option.map PolarsWrapper.Sin
        | "Cos", [ x ] -> t1 x |> Option.map PolarsWrapper.Cos
        | "Tan", [ x ] -> t1 x |> Option.map PolarsWrapper.Tan
        | "Asin", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcSin
        | "Acos", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcCos
        | "Atan", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcTan
        | "Sinh", [ x ] -> t1 x |> Option.map PolarsWrapper.Sinh
        | "Cosh", [ x ] -> t1 x |> Option.map PolarsWrapper.Cosh
        | "Tanh", [ x ] -> t1 x |> Option.map PolarsWrapper.Tanh
        | "Asinh", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcSinh
        | "Acosh", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcCosh
        | "Atanh", [ x ] -> t1 x |> Option.map PolarsWrapper.ArcTanh

        | "Pow", [ x; y ] -> t2 x y |> Option.map PolarsWrapper.Pow
        | "Atan2", [ y; x ] -> t2 y x |> Option.map PolarsWrapper.ArcTan2

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

        | "PopCount", [ x ] -> t1 x |> Option.map PolarsWrapper.BitwiseCountOnes

        | "LeadingZeroCount", [ x ] -> t1 x |> Option.map PolarsWrapper.BitwiseLeadingZeros

        | "TrailingZeroCount", [ x ] -> t1 x |> Option.map PolarsWrapper.BitwiseTrailingZeros

        | _ -> None

    let internal translateNumericMethod
        (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (args: Expression list)
        : ExprHandle option =
        translateNumericMethodWithTranslator (eval ctx) m args
