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
            | "IsNaN" | "IsRealNumber" | "IsInfinity" | "IsPositiveInfinity" | "IsNegativeInfinity" | "IsFinite"
            | "IsNormal" | "IsSubnormal" | "IsInteger" | "IsEvenInteger" | "IsOddInteger" -> true
            | _ -> false)

    /// double.IsNaN and its kin (also of float and Half) over the translated value, as comparisons with the infinity
    /// of the value's own type: the engine orders NaN above every number, so NaN is neither infinite nor finite (every
    /// test below starts from one that NaN fails). A Half is tested as the double it converts to exactly.
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
            // The least normal magnitude of the value's type, and a literal of that type.
            let leastNormal, literal =
                if x.Type = typeof<float32> then Math.ScaleB(1.0, -126), (fun (d: float) -> ScalarExpr.Literal(LiteralValue.Float32(float32 d)))
                elif x.Type = typeof<Half> then Math.ScaleB(1.0, -14), (fun (d: float) -> ScalarExpr.Literal(LiteralValue.Float64 d))
                else Math.ScaleB(1.0, -1022), (fun (d: float) -> ScalarExpr.Literal(LiteralValue.Float64 d))
            let both a b = ScalarExpr.Binary(BinaryOp.And, a, b)
            translateExpr x
            |> Option.map (fun v ->
                let abs = ScalarExpr.Unary(UnaryOp.Abs, v)
                let finite = ScalarExpr.Binary(BinaryOp.Lt, abs, infinity true)
                // Floor and remainder of a Half as its double (exact).
                let wide = if x.Type = typeof<Half> then ScalarExpr.Cast(v, LogicalTypes.Float64, false) else v
                let integer = both finite (ScalarExpr.Binary(BinaryOp.Eq, ScalarExpr.Function(ScalarFunction.Floor, [ wide ], LogicalType.Unknown), wide))
                let halved = ScalarExpr.Binary(BinaryOp.Modulo, wide, literal 2.0)
                match m.Name with
                | "IsNaN" -> ScalarExpr.Unary(UnaryOp.IsNan, v)
                | "IsRealNumber" -> ScalarExpr.Unary(UnaryOp.Not, ScalarExpr.Unary(UnaryOp.IsNan, v))
                | "IsPositiveInfinity" -> ScalarExpr.Binary(BinaryOp.Eq, v, infinity true)
                | "IsNegativeInfinity" -> ScalarExpr.Binary(BinaryOp.Eq, v, infinity false)
                | "IsInfinity" -> ScalarExpr.Binary(BinaryOp.Eq, abs, infinity true)
                | "IsNormal" -> both finite (ScalarExpr.Binary(BinaryOp.GtEq, ScalarExpr.Unary(UnaryOp.Abs, wide), literal leastNormal))
                | "IsSubnormal" ->
                    let absWide = ScalarExpr.Unary(UnaryOp.Abs, wide)
                    both (ScalarExpr.Binary(BinaryOp.Gt, absWide, literal 0.0)) (ScalarExpr.Binary(BinaryOp.Lt, absWide, literal leastNormal))
                | "IsInteger" -> integer
                | "IsEvenInteger" -> both integer (ScalarExpr.Binary(BinaryOp.Eq, halved, literal 0.0))
                | "IsOddInteger" -> both integer (ScalarExpr.Binary(BinaryOp.Eq, ScalarExpr.Unary(UnaryOp.Abs, halved), literal 1.0))
                | _ -> finite)
        | _ -> None

    /// <summary>
    /// The pattern of a char classification (char.IsDigit and its kin), matched against the one-character string a char
    /// is in a column, or None. Unicode general categories as .NET's char methods use them (checked over every char
    /// outside the surrogates, which a string column cannot hold alone).
    /// </summary>
    let internal charClassPattern (name: string) : string option =
        match name with
        | "IsDigit" -> Some @"\p{Nd}"
        | "IsNumber" -> Some @"\p{N}"
        | "IsLetter" -> Some @"\p{L}"
        | "IsLetterOrDigit" -> Some @"[\p{L}\p{Nd}]"
        | "IsUpper" -> Some @"\p{Lu}"
        | "IsLower" -> Some @"\p{Ll}"
        | "IsPunctuation" -> Some @"\p{P}"
        | "IsSymbol" -> Some @"\p{S}"
        | "IsSeparator" -> Some @"\p{Z}"
        | "IsControl" -> Some @"\p{Cc}"
        | "IsWhiteSpace" -> Some @"[\t\n\v\f\r\x{85}\p{Z}]"
        | "IsAscii" -> Some @"[\x00-\x7F]"
        | "IsAsciiDigit" -> Some "[0-9]"
        | "IsAsciiLetter" -> Some "[A-Za-z]"
        | "IsAsciiLetterLower" -> Some "[a-z]"
        | "IsAsciiLetterUpper" -> Some "[A-Z]"
        | "IsAsciiLetterOrDigit" -> Some "[A-Za-z0-9]"
        | "IsAsciiHexDigit" -> Some "[0-9A-Fa-f]"
        | "IsAsciiHexDigitLower" -> Some "[0-9a-f]"
        | "IsAsciiHexDigitUpper" -> Some "[0-9A-F]"
        | _ -> None

    /// char.IsDigit(c) and its kin, also of `s[i]` (char.IsDigit(s, i)), as an anchored match of the char's string.
    let internal translateCharClassification
        (translateExpr: Expression -> ScalarExpr option)
        (m: MethodInfo)
        (args: Expression list)
        : ScalarExpr option =
        let charOf (args: Expression list) : Expression option =
            match args with
            | [ c ] when c.Type = typeof<char> -> Some c
            | [ s; i ] when s.Type = typeof<string> && i.Type = typeof<int> ->
                Some(Expression.Call(s, typeof<string>.GetProperty("Chars").GetMethod, i))
            | _ -> None
        match charClassPattern m.Name, charOf args with
        | Some pattern, Some c ->
            translateExpr c
            |> Option.map (fun s ->
                ScalarExpr.Function(
                    ScalarFunction.StrContains,
                    [ s; ScalarExpr.Literal(LiteralValue.String("^" + pattern + "$")); ScalarExpr.Literal(LiteralValue.Bool false) ],
                    LogicalTypes.Boolean))
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
    /// <c>x != 0</c> (true for NaN); a bool converts to 1 or 0. Also: a boxed value (the object overloads) as its own
    /// overload, a boxed null as 0; <c>ToString(bool)</c> as "True" / "False"; <c>ToDateTime(string)</c> as
    /// <c>DateTime.Parse</c>. None for other conversions (chars, other strings), which stay calls.
    /// </summary>
    let internal tryConvertEquivalent (m: MethodInfo) (args: Expression list) : Expression option =
        let parameters = m.GetParameters()
        let target = m.ReturnType
        let isProvider (rest: Expression list) = rest.IsEmpty || rest.Length = 1 && parameters.[1].ParameterType = typeof<IFormatProvider>
        // Convert's own overload of the same name for these argument types, if any.
        let overload (types: Type list) =
            typeof<Convert>.GetMethod(m.Name, [| yield! types |])
            |> Option.ofObj
            |> Option.filter (fun own -> own.GetParameters() |> Array.map (fun p -> p.ParameterType) |> List.ofArray = types)
        match args with
        | _ when not (m.IsStatic && m.DeclaringType = typeof<Convert>) -> None
        // The object overloads (chosen with a provider, or for a boxed nullable) convert a boxed value as its own
        // overload does, and null as 0 (false). The provider is dropped for a number or bool, which Convert formats
        // without it; a string with a provider is parsed by its culture and stays a call.
        | (:? UnaryExpression as boxed) :: rest when
            boxed.NodeType = ExpressionType.Convert && boxed.Type = typeof<obj> && isProvider rest
            ->
            let operand = boxed.Operand
            let valueType = match Nullable.GetUnderlyingType operand.Type with null -> operand.Type | u -> u
            let own =
                if valueType = typeof<string> && rest.IsEmpty then overload [ valueType ]
                elif isConvertible valueType then overload [ valueType ]
                else None
            match own, Nullable.GetUnderlyingType operand.Type with
            | None, _ -> None
            | Some own, null -> Some(Expression.Call(own, operand) :> Expression)
            | Some own, _ ->
                Some(
                    Expression.Condition(
                        Expression.Property(operand, "HasValue"),
                        Expression.Call(own, Expression.Property(operand, "Value")),
                        Expression.Constant(Activator.CreateInstance target, target)))
        | value :: rest when m.Name = "ToString" && value.Type = typeof<bool> && isProvider rest ->
            Some(Expression.Condition(value, Expression.Constant "True", Expression.Constant "False"))
        | [ value ] when m.Name = "ToDateTime" && value.Type = typeof<string> ->
            Some(Expression.Call(typeof<DateTime>.GetMethod("Parse", [| typeof<string> |]), value))
        | value :: rest when
            m.Name.StartsWith("To", StringComparison.Ordinal)
            && parameters.[0].ParameterType = value.Type
            && isProvider rest
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
