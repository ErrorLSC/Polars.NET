namespace Kaminari.Samidare

open Kaminari.Samidare.IR
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

    let private tryExtractRoundMode (expr: Expression) : RoundMode option =
        match PartialEvaluator.tryEvaluate expr with
        | Some (:? MidpointRounding as m) -> tryMapRoundMode m
        | Some other when expr.Type = typeof<MidpointRounding> ->
            try
                let intVal = Convert.ToInt32 other
                let m = LanguagePrimitives.EnumOfValue<int, MidpointRounding> intVal
                tryMapRoundMode m
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

    /// The static classification methods of double, float and Half translated natively.
    let internal isFloatingClassification (m: MethodInfo) =
        m.IsStatic
        && (m.DeclaringType = typeof<double> || m.DeclaringType = typeof<float32> || m.DeclaringType = typeof<Half>)
        && m.GetParameters().Length = 1
        && (match m.Name with
            | "IsNaN" | "IsRealNumber" | "IsInfinity" | "IsPositiveInfinity" | "IsNegativeInfinity" | "IsFinite" -> true
            | _ -> false)

    /// double.IsNaN and its kin (also of float and Half) over the translated value, as comparisons with the infinity
    /// of the value's own type: the engine orders NaN above every number, so NaN is neither infinite nor finite.
    let internal translateFloatingClassification
        (translateExpr: Expression -> ScalarExpr option)
        (m: MethodInfo)
        (args: Expression list)
        : ScalarExpr option =
        match args with
        | [ x ] ->
            let infinity positive =
                ScalarExpr.Literal(
                    if x.Type = typeof<float32> then LiteralValue.Float32(if positive then Single.PositiveInfinity else Single.NegativeInfinity)
                    elif x.Type = typeof<Half> then LiteralValue.Float16(if positive then Half.PositiveInfinity else Half.NegativeInfinity)
                    else LiteralValue.Float64(if positive then Double.PositiveInfinity else Double.NegativeInfinity))
            translateExpr x
            |> Option.map (fun v ->
                match m.Name with
                | "IsNaN" -> ScalarExpr.Unary(UnaryOp.IsNan, v)
                | "IsRealNumber" -> ScalarExpr.Unary(UnaryOp.Not, ScalarExpr.Unary(UnaryOp.IsNan, v))
                | "IsPositiveInfinity" -> ScalarExpr.Binary(BinaryOp.Eq, v, infinity true)
                | "IsNegativeInfinity" -> ScalarExpr.Binary(BinaryOp.Eq, v, infinity false)
                | "IsInfinity" -> ScalarExpr.Binary(BinaryOp.Eq, ScalarExpr.Unary(UnaryOp.Abs, v), infinity true)
                | _ -> ScalarExpr.Binary(BinaryOp.Lt, ScalarExpr.Unary(UnaryOp.Abs, v), infinity true))
        | _ -> None

    /// Number and boolean types System.Convert converts between natively.
    let private isConvertible (t: Type) =
        match Type.GetTypeCode t with
        | TypeCode.Boolean | TypeCode.SByte | TypeCode.Byte | TypeCode.Int16 | TypeCode.UInt16 | TypeCode.Int32
        | TypeCode.UInt32 | TypeCode.Int64 | TypeCode.UInt64 | TypeCode.Single | TypeCode.Double | TypeCode.Decimal ->
            not t.IsEnum
        | _ -> false

    let private isIntegerType (t: Type) =
        not (t = typeof<bool> || t = typeof<float32> || t = typeof<double> || t = typeof<decimal>)

    /// <summary>
    /// A System.Convert conversion of a number or bool to a number or bool (an IFormatProvider argument is ignored, as
    /// Convert does for numbers), as the C# expression of equal value (checked against Convert on .NET 11):
    /// <c>ToInt32(x)</c> of a floating or decimal x is <c>checked((int)Math.Round(x))</c> (rounded half to even,
    /// OverflowException out of range or for NaN; a float is rounded as a double), of an integer
    /// <c>checked((int)x)</c>; <c>ToDouble</c>, <c>ToSingle</c> and <c>ToDecimal</c> are casts; <c>ToBoolean(x)</c> is
    /// <c>x != 0</c> (true for NaN); a bool converts to 1 or 0. None for other conversions (strings, chars, objects,
    /// dates), which stay calls.
    /// </summary>
    let internal tryConvertEquivalent (m: MethodInfo) (args: Expression list) : Expression option =
        let parameters = m.GetParameters()
        let target = m.ReturnType
        // The object overloads (`ToDouble(object, provider)`, chosen with a provider) convert a boxed value as its own
        // overload does; a boxed non-nullable value is never null (Convert takes null as 0).
        let args =
            match args with
            | (:? UnaryExpression as boxed) :: rest when
                boxed.NodeType = ExpressionType.Convert
                && boxed.Type = typeof<obj>
                && boxed.Operand.Type.IsValueType
                && isNull (Nullable.GetUnderlyingType boxed.Operand.Type)
                ->
                boxed.Operand :: rest
            | _ -> args
        match args with
        | value :: rest when
            m.IsStatic
            && m.DeclaringType = typeof<Convert>
            && m.Name.StartsWith("To", StringComparison.Ordinal)
            && (parameters.[0].ParameterType = value.Type || parameters.[0].ParameterType = typeof<obj>)
            && (rest.IsEmpty || rest.Length = 1 && parameters.[1].ParameterType = typeof<IFormatProvider>)
            && isConvertible value.Type
            && isConvertible target
            ->
            let source = value.Type
            let constant (n: int) (t: Type) = Expression.Constant(Convert.ChangeType(n, t), t) :> Expression
            let rounded (t: Type) (e: Expression) = Expression.Call(typeof<Math>.GetMethod("Round", [| t |]), e) :> Expression
            Some(
                if target = source then value
                elif target = typeof<bool> then Expression.NotEqual(value, constant 0 source)
                elif source = typeof<bool> then Expression.Condition(value, constant 1 target, constant 0 target)
                elif not (isIntegerType target) then Expression.Convert(value, target)
                elif source = typeof<double> || source = typeof<decimal> then Expression.ConvertChecked(rounded source value, target)
                elif source = typeof<float32> then
                    Expression.ConvertChecked(rounded typeof<double> (Expression.Convert(value, typeof<double>)), target)
                else Expression.ConvertChecked(value, target))
        | _ -> None

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
        // decimal.ToInt32(d) and its kin are the explicit conversions (truncated, OverflowException out of range).
        | ("ToInt32" | "ToUInt32" | "ToInt64" | "ToUInt64" | "ToSingle" | "ToDouble" | "ToByte" | "ToSByte" | "ToInt16" | "ToUInt16"), [ x ]
            when x.Type = typeof<decimal> ->
            t1 (Expression.Convert(x, m.ReturnType))
        | _ -> None
