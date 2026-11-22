namespace Kaminari.Samidare

open Kaminari.Samidare.IR
open System
open System.Globalization
open System.Text
open System.Linq
open System.Text.RegularExpressions
open System.Linq.Expressions
open System.Reflection
open Kaminari.Sazanami.Marshalling

[<AutoOpen>]
module internal WindowAndMethodTranslator =

    let private invalidEnumArgument (what: string) (value: obj) =
        InvalidQueryArgumentException(what, sprintf "%O is not a valid %s." value what) |> raise

    /// The integer value of a method-enum argument. Each public enum of a method argument numbers its members as
    /// the decoders below do. A value none of them defines is rejected: silently substituting a default method
    /// would compute a different result than the one the caller asked for.
    let private enumValueOf (what: string) (value: obj) : int64 =
        match value with
        | :? Enum
        | :? byte
        | :? sbyte
        | :? int16
        | :? uint16
        | :? int
        | :? uint32
        | :? int64 -> Convert.ToInt64 value
        | _ -> invalidEnumArgument what value

    let internal rankMethodOf (value: obj) : RankMethod =
        match enumValueOf "rank method" value with
        | 0L -> RankMethod.Average
        | 1L -> RankMethod.Min
        | 2L -> RankMethod.Max
        | 3L -> RankMethod.Dense
        | 4L -> RankMethod.Ordinal
        | 5L -> RankMethod.Random
        | _ -> invalidEnumArgument "rank method" value

    let internal interpolationMethodOf (value: obj) : InterpolationMethod =
        match enumValueOf "interpolation method" value with
        | 0L -> InterpolationMethod.Linear
        | 1L -> InterpolationMethod.Nearest
        | _ -> invalidEnumArgument "interpolation method" value

    let internal nullBehaviorOf (value: obj) : NullBehavior =
        match enumValueOf "null behavior" value with
        | 0L -> NullBehavior.Ignore
        | 1L -> NullBehavior.Drop
        | _ -> invalidEnumArgument "null behavior" value

    let internal quantileMethodOf (value: obj) : QuantileMethod =
        match enumValueOf "quantile method" value with
        | 0L -> QuantileMethod.Nearest
        | 1L -> QuantileMethod.Higher
        | 2L -> QuantileMethod.Lower
        | 3L -> QuantileMethod.Midpoint
        | 4L -> QuantileMethod.Linear
        | 5L -> QuantileMethod.Equiprobable
        | _ -> invalidEnumArgument "quantile method" value

    let private isIntegerType (t: Type) =
        t = typeof<int>
        || t = typeof<int64>
        || t = typeof<int16>
        || t = typeof<sbyte>
        || t = typeof<byte>
        || t = typeof<uint32>
        || t = typeof<uint64>
        || t = typeof<uint16>

    let private translateShiftMethod (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let targetS () = eval ctx target

        let resolveOffset (expr: Expression) : ScalarExpr option =
            match PartialEvaluator.tryEvaluate expr with
            | Some nVal ->
                try
                    let intVal = Convert.ToInt64 nVal
                    Some(ScalarExpr.Literal(LiteralValue.Int64 intVal))
                with ConversionFailure ->
                    eval ctx expr
            | None ->
                eval ctx expr

        let offsetHandleOpt =
            match args with
            | [] -> Some(ScalarExpr.Literal(LiteralValue.Int64 1L))
            | [ nExpr ] -> resolveOffset nExpr
            | _ -> None

        match targetS (), offsetHandleOpt with
        | Some eS, Some nS -> Some(ScalarExpr.Function(ScalarFunction.Shift, [ eS; nS ], LogicalType.Unknown))
        | _ -> None

    let private translateDiffMethod (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let targetS () = eval ctx target

        let resolveN nExpr =
            match PartialEvaluator.tryEvaluate nExpr with
            | Some nVal ->
                try
                    let intVal = Convert.ToInt64 nVal
                    Some(ScalarExpr.Literal(LiteralValue.Int64 intVal))
                with ConversionFailure ->
                    eval ctx nExpr
            | None -> eval ctx nExpr

        match args with
        | [] ->
            targetS ()
            |> Option.map (fun eS ->
                ScalarExpr.Function(ScalarFunction.Diff NullBehavior.Ignore, [ eS; ScalarExpr.Literal(LiteralValue.Int64 1L) ], LogicalType.Unknown))
        | [ nExpr ] ->
            match targetS (), resolveN nExpr with
            | Some eS, Some nS ->
                Some(ScalarExpr.Function(ScalarFunction.Diff NullBehavior.Ignore, [ eS; nS ], LogicalType.Unknown))
            | _ -> None
        | [ nExpr; nbExpr ] ->
            match targetS (), resolveN nExpr, PartialEvaluator.tryEvaluate nbExpr with
            | Some eS, Some nS, Some nb ->
                Some(ScalarExpr.Function(ScalarFunction.Diff(nullBehaviorOf nb), [ eS; nS ], LogicalType.Unknown))
            | _ -> None
        | _ -> None

    let private translateRankMethod (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let targetS () = eval ctx target

        match args with
        | [] ->
            targetS ()
            |> Option.map (fun eS ->
                ScalarExpr.Function(ScalarFunction.Rank(RankMethod.Average, None), [ eS; ScalarExpr.Literal(LiteralValue.Bool false) ], LogicalType.Unknown))

        | [ descExpr ] when descExpr.Type = typeof<bool> ->
            match targetS (), PartialEvaluator.tryEvaluate descExpr with
            | Some eS, Some(:? bool as desc) ->
                Some(ScalarExpr.Function(ScalarFunction.Rank(RankMethod.Average, None), [ eS; ScalarExpr.Literal(LiteralValue.Bool desc) ], LogicalType.Unknown))
            | _ -> None

        | [ methodExpr ] ->
            match targetS (), PartialEvaluator.tryEvaluate methodExpr with
            | Some eS, Some mObj ->
                Some(ScalarExpr.Function(ScalarFunction.Rank(rankMethodOf mObj, None), [ eS; ScalarExpr.Literal(LiteralValue.Bool false) ], LogicalType.Unknown))
            | _ -> None

        | [ methodExpr; descExpr ] ->
            match targetS (), PartialEvaluator.tryEvaluate methodExpr, PartialEvaluator.tryEvaluate descExpr with
            | Some eS, Some mObj, Some(:? bool as desc) ->
                Some(ScalarExpr.Function(ScalarFunction.Rank(rankMethodOf mObj, None), [ eS; ScalarExpr.Literal(LiteralValue.Bool desc) ], LogicalType.Unknown))
            | _ -> None

        | [ methodExpr; descExpr; seedExpr ] ->
            // Some(Some seed) for a value, Some None for a null ulong?; None when the seed cannot be evaluated.
            let seedOpt =
                match PartialEvaluator.tryEvaluate seedExpr with
                | Some(:? uint64 as seed) -> Some(Some seed)
                | Some null -> Some None
                | _ -> None

            match targetS (), PartialEvaluator.tryEvaluate methodExpr, PartialEvaluator.tryEvaluate descExpr, seedOpt with
            | Some eS, Some mObj, Some(:? bool as desc), Some seed ->
                Some(ScalarExpr.Function(ScalarFunction.Rank(rankMethodOf mObj, seed), [ eS; ScalarExpr.Literal(LiteralValue.Bool desc) ], LogicalType.Unknown))
            | _ -> None

        | _ -> None

    let private translatePctChangeMethod (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let targetS () = eval ctx target

        match args with
        | [] -> targetS () |> Option.map (fun eS -> ScalarExpr.Function(ScalarFunction.PctChange, [ eS; ScalarExpr.Literal(LiteralValue.Int64 1L) ], LogicalType.Unknown))
        | [ nExpr ] ->
            match targetS (), PartialEvaluator.tryEvaluate nExpr with
            | Some eS, Some nVal -> Some(ScalarExpr.Function(ScalarFunction.PctChange, [ eS; ScalarExpr.Literal(LiteralValue.Int64(Convert.ToInt64 nVal)) ], LogicalType.Unknown))
            | _ -> None
        | _ -> None

    let private translateOverMethod (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        match eval ctx target with
        | None -> None
        | Some targetS ->
            let extractList (arg: Expression) : Expression list =
                match unwrap arg with
                | null -> []
                | :? NewExpression as ne -> ne.Arguments |> Seq.toList
                | :? NewArrayExpression as nae -> nae.Expressions |> Seq.toList
                | :? MemberInitExpression as mie ->
                    mie.Bindings
                    |> Seq.choose (function :? MemberAssignment as ma -> Some ma.Expression | _ -> None)
                    |> Seq.toList
                | other -> [ other ]

            let translateList (exprs: Expression list) : ScalarExpr list option =
                if exprs.IsEmpty then
                    Some []
                else
                    let scalars =
                        exprs
                        |> List.choose (fun arg ->
                            match arg with
                            | :? LambdaExpression as lam ->
                                eval (TranslationContext.Entity lam.Parameters.[0].Name) lam.Body
                            | other -> eval ctx other)
                    if scalars.Length = exprs.Length then
                        Some scalars
                    else
                        None

            let rec isOrderSensitive (e: Expression) : bool =
                match unwrap e with
                | null -> false
                | :? MethodCallExpression as mc ->
                    match mc.Method.Name with
                    | "Shift" | "Diff" | "PctChange"
                    | "CumSum" | "CumMax" | "CumMin" | "CumProd" | "CumCount"
                    | "Interpolate" | "InterpolateBy" -> true
                    | _ -> false
                | :? UnaryExpression as ue -> isOrderSensitive ue.Operand
                | _ -> false

            let partitionExprs, orderExprs, isDescending =
                match args with
                | [ pArg ] ->
                    extractList pArg, [], false
                | [ pArg; oArg ] when oArg.Type = typeof<bool> ->
                    let desc = match PartialEvaluator.tryEvaluate oArg with Some (:? bool as b) -> b | _ -> false
                    extractList pArg, [], desc
                | [ pArg; oArg ] ->
                    if isOrderSensitive target then
                        extractList pArg, extractList oArg, false
                    else
                        extractList pArg @ extractList oArg, [], false
                | pArg :: oArg :: descArg :: _ when descArg.Type = typeof<bool> ->
                    let desc = match PartialEvaluator.tryEvaluate descArg with Some (:? bool as b) -> b | _ -> false
                    if desc || isOrderSensitive target then
                        extractList pArg, extractList oArg, desc
                    else
                        extractList pArg @ extractList oArg, [], false
                | other ->
                    other |> List.collect extractList, [], false

            match translateList partitionExprs, translateList orderExprs with
            | Some pExprs, Some oExprs when pExprs.Length > 0 || oExprs.Length > 0 ->
                // A floating key orders NaN as .NET does (NaNSemantics.sortOrders).
                let orderings =
                    List.zip orderExprs oExprs
                    |> List.collect (fun (clrExpr, e) ->
                        let keyType =
                            match clrExpr with
                            | :? LambdaExpression as lam -> lam.Body.Type
                            | other -> other.Type
                        NaNSemantics.sortOrders keyType { SortOrder.Expr = e; Descending = isDescending; NullsLast = false })

                Some(ScalarExpr.Window(targetS, pExprs, orderings))
            | _ -> None

    let private translateReductionMethod (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then
                args.Head, args.Tail
            else
                target, args

        match m.Name, effectiveArgs with
        | ("Sum" | "Mean" | "Average" | "Min" | "Max" | "Median" | "First" | "Last"), [] ->
            eval ctx effectiveTarget
            |> Option.bind (translateReduction m)

        | "Count", [] ->
            eval ctx effectiveTarget
            |> Option.map (fun s -> ScalarExpr.Aggregation(AggKind.Count, Some s, false))

        | ("Std" | "StdDev"), [] ->
            eval ctx effectiveTarget
            |> Option.bind (translateDispersionOp m.Name 1uy)

        | ("Var" | "Variance"), [] ->
            eval ctx effectiveTarget
            |> Option.bind (translateDispersionOp m.Name 1uy)

        | ("Std" | "StdDev" | "Var" | "Variance"), [ ddofExpr ] ->
            match eval ctx effectiveTarget, PartialEvaluator.tryEvaluate ddofExpr with
            | Some colS, Some ddofVal ->
                let ddof = Convert.ToByte ddofVal
                translateDispersionOp m.Name ddof colS
            | _ -> None

        | _ -> None

    let private translateCumulativeMethod (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then
                args.Head, args.Tail
            else
                target, args

        let reverse =
            match effectiveArgs with
            | [ revExpr ] ->
                match PartialEvaluator.tryEvaluate revExpr with
                | Some(:? bool as r) -> r
                | _ -> false
            | _ -> false

        eval ctx effectiveTarget
        |> Option.bind (fun colS ->
            match m.Name with
            | "CumSum" -> Some(ScalarExpr.Function(ScalarFunction.CumSum, [ colS; ScalarExpr.Literal(LiteralValue.Bool reverse) ], LogicalType.Unknown))
            | "CumMax" -> Some(ScalarExpr.Function(ScalarFunction.CumMax, [ colS; ScalarExpr.Literal(LiteralValue.Bool reverse) ], LogicalType.Unknown))
            | "CumMin" -> Some(ScalarExpr.Function(ScalarFunction.CumMin, [ colS; ScalarExpr.Literal(LiteralValue.Bool reverse) ], LogicalType.Unknown))
            | "CumProd" -> Some(ScalarExpr.Function(ScalarFunction.CumProd, [ colS; ScalarExpr.Literal(LiteralValue.Bool reverse) ], LogicalType.Unknown))
            | "CumCount" -> Some(ScalarExpr.Function(ScalarFunction.CumCount, [ colS; ScalarExpr.Literal(LiteralValue.Bool reverse) ], LogicalTypes.UInt32))
            | _ -> None)

    let private translateInterpolateMethod (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then
                args.Head, args.Tail
            else
                target, args

        match m.Name, effectiveArgs with
        | "Interpolate", [] ->
            eval ctx effectiveTarget
            |> Option.map (fun s -> ScalarExpr.Function(ScalarFunction.Interpolate InterpolationMethod.Linear, [ s ], LogicalType.Unknown))

        | "Interpolate", [ methodExpr ] ->
            match eval ctx effectiveTarget, PartialEvaluator.tryEvaluate methodExpr with
            | Some targetS, Some methodVal ->
                Some(ScalarExpr.Function(ScalarFunction.Interpolate(interpolationMethodOf methodVal), [ targetS ], LogicalType.Unknown))
            | _ -> None

        | "InterpolateBy", [ byExpr ] ->
            match eval ctx effectiveTarget, eval ctx byExpr with
            | Some targetS, Some byS -> Some(ScalarExpr.Function(ScalarFunction.InterpolateBy, [ targetS; byS ], LogicalType.Unknown))
            | _ -> None

        | _ -> None

    let internal translateQuantileMethod (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then
                args.Head, args.Tail
            else
                target, args

        match effectiveArgs with
        | [ qExpr ] ->
            match eval ctx effectiveTarget, PartialEvaluator.tryEvaluate qExpr with
            | Some targetS, Some qVal ->
                try
                    let q = Convert.ToDouble qVal
                    Some(ScalarExpr.Aggregation(AggKind.Quantile q, Some targetS, false))
                with ConversionFailure ->
                    None
            | _ -> None

        | [ qExpr; _methodExpr ] ->
            match eval ctx effectiveTarget, PartialEvaluator.tryEvaluate qExpr with
            | Some targetS, Some qVal ->
                try
                    let q = Convert.ToDouble qVal
                    Some(ScalarExpr.Aggregation(AggKind.Quantile q, Some targetS, false))
                with ConversionFailure ->
                    None
            | _ -> None

        | _ -> None

    /// bool.Parse: "true" or "false" ignoring case and surrounding white space; null for other text (where .NET throws).
    /// The engine casts no string to a boolean.
    let private parseBoolean (strS: ScalarExpr) : ScalarExpr =
        let trimmed = ScalarExpr.Function(ScalarFunction.Strip, [ strS; ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String) ], LogicalTypes.String)
        let word = ScalarExpr.Function(ScalarFunction.ToLower, [ trimmed ], LogicalTypes.String)
        let is (text: string) = ScalarExpr.Binary(BinaryOp.Eq, word, ScalarExpr.Literal(LiteralValue.String text))
        ScalarExpr.Conditional(
            is "true",
            ScalarExpr.Literal(LiteralValue.Bool true),
            ScalarExpr.Conditional(is "false", ScalarExpr.Literal(LiteralValue.Bool false), ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Boolean)))

    /// Convert takes a null string as 0 (false, DateTime.MinValue), where parsing it gives null.
    let private convertNullDefault (eval: ScalarExprEvaluator) (ctx: TranslationContext) (m: MethodInfo) (args: Expression list) (converted: ScalarExpr) =
        match args with
        | s :: _ when m.DeclaringType = typeof<Convert> && s.Type = typeof<string> && m.ReturnType.IsValueType ->
            match eval ctx s with
            | Some str ->
                ScalarExpr.Conditional(
                    ScalarExpr.Unary(UnaryOp.IsNull, str),
                    ScalarExprOps.toLiteral (Activator.CreateInstance m.ReturnType) m.ReturnType,
                    converted)
            | None -> converted
        | _ -> converted

    let private translateConversionOrParseMethod (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let targetType = m.DeclaringType

        if isNull target && (m.Name = "Parse" || m.Name = "TryParse") then
            match args with
            | strExpr :: _ when strExpr.Type = typeof<string> ->
                match eval ctx strExpr with
                | Some strS ->
                    if isIntegerType targetType then
                        match (LogicalTypes.tryOfClrType targetType) with
                        | Some dtype ->
                            Some(ScalarExpr.Function(ScalarFunction.StrToInteger dtype, [ strS; ScalarExpr.Literal(LiteralValue.Int32 10) ], dtype))
                        | None -> None
                    elif targetType = typeof<decimal> then
                        Some(ScalarExpr.Function(ScalarFunction.StrToDecimal, [ strS; ScalarExpr.Literal(LiteralValue.Int32 4) ], LogicalTypes.Decimal))
                    elif targetType = typeof<bool> then
                        Some(parseBoolean strS)
                    elif targetType = typeof<double> || targetType = typeof<float> then
                        tryTranslateCast targetType strS
                    else
                        None
                | None -> None
            | _ -> None

        elif isNull target && targetType = typeof<Convert> then
            match m.Name, args with
            | "FromHexString", [ strExpr ] ->
                eval ctx strExpr
                |> Option.map (fun strS -> ScalarExpr.Function(ScalarFunction.HexDecode, [ strS ], LogicalTypes.Binary))

            | "FromBase64String", [ strExpr ] ->
                eval ctx strExpr
                |> Option.map (fun strS -> ScalarExpr.Function(ScalarFunction.Base64Decode, [ strS ], LogicalTypes.Binary))

            | "ToHexString", [ strExpr ] ->
                eval ctx strExpr
                |> Option.map (fun strS -> ScalarExpr.Function(ScalarFunction.HexEncode, [ strS ], LogicalTypes.String))

            | "ToBase64String", [ strExpr ] ->
                eval ctx strExpr
                |> Option.map (fun strS -> ScalarExpr.Function(ScalarFunction.Base64Encode, [ strS ], LogicalTypes.String))

            | "ToInt32" | "ToInt64" | "ToInt16" | "ToByte" | "ToSByte" | "ToUInt32" | "ToUInt64" | "ToUInt16" as convName, [ strExpr ] when strExpr.Type = typeof<string> ->
                let intType =
                    match convName with
                    | "ToInt32" -> typeof<int>
                    | "ToInt64" -> typeof<int64>
                    | "ToInt16" -> typeof<int16>
                    | "ToByte" -> typeof<byte>
                    | "ToSByte" -> typeof<sbyte>
                    | "ToUInt32" -> typeof<uint32>
                    | "ToUInt64" -> typeof<uint64>
                    | "ToUInt16" -> typeof<uint16>
                    | _ -> typeof<int>

                match eval ctx strExpr, (LogicalTypes.tryOfClrType intType) with
                | Some strS, Some dtype ->
                    Some(ScalarExpr.Function(ScalarFunction.StrToInteger dtype, [ strS; ScalarExpr.Literal(LiteralValue.Int32 10) ], dtype))
                | _ -> None

            | "ToInt32" | "ToInt64" as convName, [ strExpr; baseExpr ] when strExpr.Type = typeof<string> ->
                let intType = if convName = "ToInt32" then typeof<int> else typeof<int64>
                match eval ctx strExpr, eval ctx baseExpr, (LogicalTypes.tryOfClrType intType) with
                | Some strS, Some baseS, Some dtype ->
                    Some(ScalarExpr.Function(ScalarFunction.StrToInteger dtype, [ strS; baseS ], dtype))
                | _ -> None

            | "ToDecimal", [ strExpr ] when strExpr.Type = typeof<string> ->
                eval ctx strExpr
                |> Option.map (fun strS -> ScalarExpr.Function(ScalarFunction.StrToDecimal, [ strS; ScalarExpr.Literal(LiteralValue.Int32 4) ], LogicalTypes.Decimal))

            | "ToDouble", [ strExpr ] when strExpr.Type = typeof<string> ->
                eval ctx strExpr
                |> Option.bind (tryTranslateCast typeof<double>)

            | "ToSingle", [ strExpr ] when strExpr.Type = typeof<string> ->
                eval ctx strExpr
                |> Option.bind (tryTranslateCast typeof<float>)

            | "ToBoolean", [ strExpr ] when strExpr.Type = typeof<string> ->
                eval ctx strExpr |> Option.map parseBoolean

            // Between numbers and booleans: the C# expression of equal value.
            | _ -> tryConvertEquivalent m args |> Option.bind (eval ctx)

        else
            None

    /// The enum value a method is called on, seen through the boxing conversion to System.Enum or object.
    let private enumReceiver (target: Expression) : Expression option =
        match target with
        | :? UnaryExpression as u when u.NodeType = ExpressionType.Convert && u.Operand.Type.IsEnum -> Some u.Operand
        | t when t.Type.IsEnum -> Some t
        | _ -> None

    /// Instance methods of an enum value: ToString (the member name; "G" too, "D" the underlying value as text),
    /// Equals, HasFlag and CompareTo (on underlying values).
    let private translateEnumMethod
        (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (receiver: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let underlying = Enum.GetUnderlyingType receiver.Type
        let valueOf (e: Expression) = Expression.Convert(e, underlying) :> Expression

        /// The argument as a value of the receiver's enum type, seen through its boxing conversion.
        let sameEnum (arg: Expression) =
            match arg with
            | :? UnaryExpression as u when u.NodeType = ExpressionType.Convert && u.Operand.Type = receiver.Type -> Some u.Operand
            | a when a.Type = receiver.Type -> Some a
            | :? ConstantExpression as c when not (isNull c.Value) && c.Value.GetType() = receiver.Type ->
                Some(Expression.Constant(c.Value, receiver.Type) :> Expression)
            | _ -> None

        match m.Name, args with
        | "ToString", [] ->
            match ScalarExprOps.tryEnumMapping receiver.Type with
            | Some mapping ->
                eval ctx receiver |> Option.map (fun r -> ScalarExpr.EnumConvert(EnumConversion.ToName, r, mapping))
            | None -> None
        | "Equals", [ other ] ->
            sameEnum other
            |> Option.bind (fun o -> eval ctx (Expression.Equal(valueOf receiver, valueOf o)))
        | "HasFlag", [ flag ] ->
            sameEnum flag
            |> Option.bind (fun f -> eval ctx (Expression.Equal(Expression.And(valueOf receiver, valueOf f), valueOf f)))
        // The sign of the comparison of the underlying values (compared as 64-bit integers, which expression trees
        // compare for every underlying type).
        | "CompareTo", [ other ] ->
            let wide = if underlying = typeof<uint64> then typeof<uint64> else typeof<int64>
            sameEnum other
            |> Option.bind (fun o ->
                let a, b = Expression.Convert(receiver, wide), Expression.Convert(o, wide)
                eval ctx (
                    Expression.Condition(
                        Expression.LessThan(a, b),
                        Expression.Constant(-1),
                        Expression.Condition(Expression.GreaterThan(a, b), Expression.Constant(1), Expression.Constant(0)))))
        | "ToString", [ format ] ->
            match PartialEvaluator.tryEvaluate format with
            | Some null
            | Some(:? string as ("" | "G" | "g")) ->
                eval ctx (Expression.Call(receiver, receiver.Type.GetMethod("ToString", Type.EmptyTypes)))
            // The underlying value as the current culture writes the integer (as .NET's "D").
            | Some(:? string as ("D" | "d")) ->
                eval ctx (Expression.Call(valueOf receiver, underlying.GetMethod("ToString", Type.EmptyTypes)))
            | _ -> None
        | _ -> None

    /// The enum type an Enum method's call names: its type argument, else its Type argument.
    let private enumTypeOf (m: MethodInfo) (args: Expression list) : Type option =
        if m.IsGenericMethod then
            Some(m.GetGenericArguments()[0])
        else
            match args with
            | first :: _ when first.Type = typeof<Type> ->
                match PartialEvaluator.tryEvaluate first with
                | Some(:? Type as t) when t.IsEnum -> Some t
                | _ -> None
            | _ -> None

    /// A value boxed to object (Enum.GetName(Type, object), Convert.ToInt32(object)), seen through its boxing.
    let private unboxed (arg: Expression) : Expression =
        match arg with
        | :? UnaryExpression as u when u.NodeType = ExpressionType.Convert && arg.Type = typeof<obj> -> u.Operand
        | _ -> arg

    /// Static methods of Enum over a non-[Flags] enum: GetName (null for a value no member has), IsDefined (of a
    /// value of the enum, of its underlying type, or of a name), Parse (a name, in any case with ignoreCase, or a
    /// number, around white space; null where .NET throws, as the provider's other conversions of row values).
    let private translateEnumStatic
        (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (args: Expression list)
        : ScalarExpr option =
        match enumTypeOf m args with
        | None -> None
        | Some enumType ->
            match ScalarExprOps.tryEnumMapping enumType with
            | None -> None
            | Some mapping ->
                let underlying = Enum.GetUnderlyingType enumType
                let valueArgs = if m.IsGenericMethod then args else List.tail args
                let names = EnumCategories.GetMembers enumType |> Array.map (fun struct (name, _) -> name)
                let isDefined (value: Expression) : ScalarExpr option =
                    match unboxed value with
                    | v when ClrTypeHelper.UnwrapCoreType v.Type = enumType ->
                        eval ctx v |> Option.map (fun s -> ScalarExpr.EnumConvert(EnumConversion.IsDefined, s, mapping))
                    | v when v.Type = underlying ->
                        let values = Array.CreateInstance(underlying, mapping.Values.Length)
                        mapping.Values |> List.iteri (fun i literal -> values.SetValue(literal.BoxedValue, i))
                        eval ctx v |> Option.map (fun s -> ScalarExpr.InList(s, InListSource.Collection(values, underlying), false, false))
                    | v when v.Type = typeof<string> ->
                        eval ctx v |> Option.map (fun s -> ScalarExpr.InList(s, InListSource.Collection(names, typeof<string>), false, false))
                    | _ -> None
                match m.Name, valueArgs with
                | "IsDefined", [ value ] -> isDefined value
                | "GetName", [ value ] ->
                    match unboxed value with
                    | v when ClrTypeHelper.UnwrapCoreType v.Type = enumType ->
                        eval ctx v
                        |> Option.map (fun s ->
                            ScalarExpr.Conditional(
                                ScalarExpr.EnumConvert(EnumConversion.IsDefined, s, mapping),
                                ScalarExpr.EnumConvert(EnumConversion.ToName, s, mapping),
                                ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String)))
                    | v when v.Type = underlying ->
                        eval ctx v
                        |> Option.map (fun s ->
                            ScalarExpr.Function(
                                ScalarFunction.ReplaceStrict(mapping.Values, mapping.Categories |> List.map LiteralValue.String),
                                [ s; ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String) ],
                                LogicalTypes.String))
                    | _ -> None
                | "Parse", (text :: rest) when text.Type = typeof<string> ->
                    let ignoreCase =
                        match rest with
                        | [] -> Some false
                        | [ flag ] ->
                            match PartialEvaluator.tryEvaluate flag with
                            | Some(:? bool as b) -> Some b
                            | _ -> None
                        | _ -> None
                    match ignoreCase, eval ctx (Expression.Call(text, typeof<string>.GetMethod("Trim", Type.EmptyTypes))) with
                    | Some ignoreCase, Some trimmed ->
                        // A member's name (exactly, or lower-cased on both sides), else the number the text is.
                        let members =
                            EnumCategories.GetMembers enumType
                            |> Array.map (fun struct (name, value) -> (if ignoreCase then name.ToLowerInvariant() else name), LiteralValue.OfObject value)
                            |> Array.distinctBy fst
                        let key = if ignoreCase then ScalarExpr.Function(ScalarFunction.ToLower, [ trimmed ], LogicalTypes.String) else trimmed
                        let byName =
                            ScalarExpr.Function(
                                ScalarFunction.ReplaceStrict(members |> Array.map (fst >> LiteralValue.String) |> List.ofArray, members |> Array.map snd |> List.ofArray),
                                [ key; ScalarExpr.Literal(LiteralValue.Null mapping.Underlying) ],
                                mapping.Underlying)
                        let byNumber =
                            ScalarExpr.Function(ScalarFunction.StrToInteger mapping.Underlying, [ trimmed; ScalarExpr.Literal(LiteralValue.Int32 10) ], mapping.Underlying)
                        Some(ScalarExpr.EnumConvert(EnumConversion.FromValue, ScalarExpr.Coalesce [ byName; byNumber ], mapping))
                    | _ -> None
                | _ -> None

    /// Convert.ToString / Convert.ToInt32 & co of a (boxed) enum: its name, its underlying value converted as .NET
    /// converts it (checked); a null (a Nullable enum's) gives the conversion's null default ("", 0).
    let private translateEnumConvertCall (eval: ScalarExprEvaluator) (ctx: TranslationContext) (m: MethodInfo) (args: Expression list) : ScalarExpr option =
        match args with
        | value :: rest when (unboxed value).Type <> value.Type && (ClrTypeHelper.UnwrapCoreType (unboxed value).Type).IsEnum ->
            let v = unboxed value
            let enumType = ClrTypeHelper.UnwrapCoreType v.Type
            let core = if v.Type = enumType then v else Expression.Convert(v, enumType) :> Expression
            let converted =
                match m.Name, rest with
                | "ToString", ([] | [ _ ]) -> Some(Expression.Call(core, enumType.GetMethod("ToString", Type.EmptyTypes)) :> Expression)
                | ("ToInt32" | "ToInt64" | "ToInt16" | "ToByte" | "ToSByte" | "ToUInt32" | "ToUInt64" | "ToUInt16"), ([] | [ _ ]) ->
                    Some(Expression.ConvertChecked(Expression.Convert(core, Enum.GetUnderlyingType enumType), m.ReturnType) :> Expression)
                | _ -> None
            converted
            |> Option.bind (fun c ->
                if v.Type = enumType then
                    eval ctx c
                else
                    let fallback = if m.ReturnType = typeof<string> then Expression.Constant("") :> Expression else Expression.Constant(Activator.CreateInstance m.ReturnType, m.ReturnType)
                    eval ctx (Expression.Condition(Expression.Equal(v, Expression.Constant(null, v.Type)), fallback, c)))
        | _ -> None

    let internal translateMethodCall
        (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let effectiveTarget, effectiveArgs =
            if isNull target && args.Length > 0 then
                args.Head, args.Tail
            else
                target, args

        // A chain over a MatchCollection that has no native form runs whole as a UDF.
        if RegexTranslator.readsMatchCollection effectiveTarget then
            None
        elif (m.Name = "IsNull" || m.Name = "isNull") && args.Length = 1 then
            eval ctx args.Head
            |> Option.orElseWith (fun () -> tryResolveEntityNullTarget ctx args.Head)
            |> Option.map (fun s -> ScalarExpr.Unary(UnaryOp.IsNull, s))
        // x.ToString() and x.ToString(culture) of a number, bool, date or time: .NET's text under the current culture
        // (or the one given). A Nullable's null is "".
        elif
            m.Name = "ToString"
            && not (isNull target)
            && isTextType target.Type
            && (match args with
                | [] -> true
                | [ provider ] -> provider.Type = typeof<IFormatProvider> || provider.Type = typeof<CultureInfo>
                | _ -> false)
        then
            let culture =
                match args with
                | [ provider ] -> tryCultureOf provider
                | _ -> Some CultureInfo.CurrentCulture
            match culture, eval ctx target with
            | Some culture, Some value ->
                asDotNetText culture target.Type value
                |> Option.map (fun text ->
                    if isNull (Nullable.GetUnderlyingType target.Type) then text
                    else ScalarExpr.Coalesce [ text; ScalarExpr.Literal(LiteralValue.String "") ])
            | _ -> None
        elif not (isNull effectiveTarget) && m.Name = "Shift" then
            translateShiftMethod eval ctx effectiveTarget effectiveArgs
        elif
            (m.Name = "ToArray" || m.Name = "ToList" || m.Name = "AsEnumerable")
            && effectiveArgs.IsEmpty
            && not (isNull effectiveTarget)
        then
            eval ctx effectiveTarget

        elif not (isNull effectiveTarget) && m.Name = "Diff" then
            translateDiffMethod eval ctx effectiveTarget effectiveArgs

        elif not (isNull effectiveTarget) && m.Name = "Over" then
            translateOverMethod eval ctx effectiveTarget effectiveArgs

        elif not (isNull effectiveTarget) && m.Name = "Rank" then
            translateRankMethod eval ctx effectiveTarget effectiveArgs

        elif not (isNull effectiveTarget) && m.Name = "PctChange" then
            translatePctChangeMethod eval ctx effectiveTarget effectiveArgs

        elif not (isNull effectiveTarget) && m.Name = "Quantile" then
            translateQuantileMethod eval ctx effectiveTarget effectiveArgs

        elif
            not (isNull effectiveTarget)
            && (m.Name = "Interpolate" || m.Name = "InterpolateBy")
        then
            translateInterpolateMethod eval ctx m effectiveTarget effectiveArgs

        elif
            not (isNull effectiveTarget)
            && (m.Name = "CumSum"
                || m.Name = "CumMax"
                || m.Name = "CumMin"
                || m.Name = "CumProd"
                || m.Name = "CumCount")
        then
            translateCumulativeMethod eval ctx m effectiveTarget effectiveArgs

        elif m.DeclaringType = typeof<Regex> && isNull target && m.Name = "Escape" then
            match args with
            | [ strExpr ] ->
                eval ctx strExpr |> Option.map (fun s -> ScalarExpr.Function(ScalarFunction.RegexEscape, [ s ], LogicalTypes.String))
            | _ -> None

        elif isNull target && m.DeclaringType = typeof<Enum> then
            translateEnumStatic eval ctx m args

        elif
            m.DeclaringType = typeof<Convert>
            && (match args with
                | value :: _ -> (unboxed value).Type <> value.Type && (ClrTypeHelper.UnwrapCoreType (unboxed value).Type).IsEnum
                | [] -> false)
        then
            translateEnumConvertCall eval ctx m args

        elif
            m.DeclaringType = typeof<Convert>
            || (isNull target && (m.Name = "Parse" || m.Name = "TryParse"))
        then
            match translateConversionOrParseMethod eval ctx m target args with
            | Some s -> Some(convertNullDefault eval ctx m args s)
            | None ->
                if m.DeclaringType = typeof<DateTime> || m.DeclaringType = typeof<DateOnly> || m.DeclaringType = typeof<TimeOnly> then
                    translateTemporalMethod eval ctx m target args
                else
                    None

        elif m.Name = "ToTitleCase" && args.Length >= 1 then
            match args |> List.tryFind (fun a -> a.Type = typeof<string>) with
            | Some strExpr ->
                eval ctx strExpr |> Option.map (fun s -> ScalarExpr.Function(ScalarFunction.ToTitlecase, [ s ], LogicalTypes.String))
            | None -> None

        elif not (isNull m.DeclaringType) && typeof<Encoding>.IsAssignableFrom m.DeclaringType && m.Name = "GetByteCount" then
            // Strings are stored as UTF-8, so only that encoding has a native byte count; any other runs as a UDF.
            let isUtf8 =
                match PartialEvaluator.tryEvaluate target with
                | Some(:? Encoding as encoding) -> encoding.CodePage = Encoding.UTF8.CodePage
                | _ -> false

            match args with
            | [ strExpr ] when strExpr.Type = typeof<string> && isUtf8 ->
                eval ctx strExpr |> Option.map (fun s -> ScalarExpr.Function(ScalarFunction.LenBytes, [ s ], LogicalTypes.UInt32))
            | _ -> None

        elif m.DeclaringType = typeof<Enumerable> && isNull target then
            match m.Name, args with
            | "Take", [ strExpr; countExpr ] when strExpr.Type = typeof<string> ->
                match eval ctx strExpr, eval ctx countExpr with
                | Some strS, Some countS -> Some(ScalarExpr.Function(ScalarFunction.Head, [ strS; countS ], LogicalTypes.String))
                | _ -> None

            | "TakeLast", [ strExpr; countExpr ] when strExpr.Type = typeof<string> ->
                match eval ctx strExpr, eval ctx countExpr with
                | Some strS, Some countS -> Some(ScalarExpr.Function(ScalarFunction.Tail, [ strS; countS ], LogicalTypes.String))
                | _ -> None

            | "Reverse", [ strExpr ] when strExpr.Type = typeof<string> ->
                eval ctx strExpr |> Option.map (fun s -> ScalarExpr.Function(ScalarFunction.Reverse, [ s ], LogicalTypes.String))

            | _ -> None

        elif m.Name = "StrReverse" && isNull target && args.Length = 1 && args.[0].Type = typeof<string> then
            eval ctx args.[0] |> Option.map (fun s -> ScalarExpr.Function(ScalarFunction.Reverse, [ s ], LogicalTypes.String))

        elif m.DeclaringType = typeof<char> && isNull target then
            match m.Name, args with
            | ("ToUpper" | "ToUpperInvariant"), [ charExpr ] ->
                eval ctx charExpr |> Option.map (fun s -> ScalarExpr.Function(ScalarFunction.ToUpper, [ s ], LogicalTypes.String))
            | ("ToLower" | "ToLowerInvariant"), [ charExpr ] ->
                eval ctx charExpr |> Option.map (fun s -> ScalarExpr.Function(ScalarFunction.ToLower, [ s ], LogicalTypes.String))
            | _ -> translateCharClassification (eval ctx) m args

        elif
            m.DeclaringType = typeof<string>
            || not (isNull target) && target.Type = typeof<string>
        then
            translateStringMethod eval ctx m target args

        elif
            m.DeclaringType = typeof<DateTime>
            || m.DeclaringType = typeof<DateOnly>
            || m.DeclaringType = typeof<TimeOnly>
            || not (isNull target) && ClrTypeHelper.IsTemporalType target.Type
        then
            translateTemporalMethod eval ctx m target args

        elif isNumericDeclaringType m.DeclaringType && isNull target then
            translateNumericMethod (eval ctx) m args

        elif isFloatingClassification m then
            translateFloatingClassification (eval ctx) m args

        elif
            m.Name = "Sum"
            || m.Name = "Mean"
            || m.Name = "Average"
            || m.Name = "Min"
            || m.Name = "Max"
            || m.Name = "Count"
            || m.Name = "Median"
            || m.Name = "First"
            || m.Name = "Last"
            || m.Name = "Std"
            || m.Name = "StdDev"
            || m.Name = "Var"
            || m.Name = "Variance"
        then
            translateReductionMethod eval ctx m target args

        elif not (isNull target) && (enumReceiver target).IsSome then
            translateEnumMethod eval ctx m (enumReceiver target).Value args

        elif not (isNull target) && target.Type = typeof<decimal> then
            match m.Name, args with
            | "ToString", [] ->
                eval ctx target
                |> Option.bind (tryTranslateCast typeof<string>)

            | "ToString", [ formatExpr ] ->
                match eval ctx target, PartialEvaluator.tryEvaluate formatExpr with
                | Some t, Some(:? string as fmt) ->
                    if fmt.StartsWith("F", StringComparison.OrdinalIgnoreCase) then
                        let scale =
                            let s = fmt.Substring 1
                            match Int32.TryParse(s) with
                            | true, v -> v
                            | false, _ -> 2
                        let scaled = ScalarExpr.Cast(t, LogicalTypes.decimalOf 38 scale, false)
                        tryTranslateCast typeof<string> scaled
                    else
                        None
                | _ -> None
            | _ -> None

        else
            None