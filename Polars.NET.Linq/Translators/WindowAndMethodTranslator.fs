namespace Polars.NET.Linq

open Polars.NET.Core.Enums
open Polars.NET.Core.Arrow
open Polars.NET.Core.Helpers
open Polars.NET.Linq.IR
open System
open System.Text
open System.Linq
open System.Text.RegularExpressions
open System.Linq.Expressions
open System.Reflection

[<AutoOpen>]
module internal WindowAndMethodTranslator =

    /// Reads a method-enum argument as a defined member of the Polars enum `'E`.
    /// The public Polars.CSharp enums mirror the Pl* enums value for value, so any enum or integer whose value
    /// is a defined member is accepted. Anything else is rejected: silently substituting a default method
    /// would compute a different result than the one the caller asked for.
    let private toPlEnum<'E when 'E: enum<byte> and 'E: struct and 'E: (new: unit -> 'E) and 'E :> ValueType>
        (what: string)
        (value: obj)
        : 'E =
        let invalid () =
            InvalidQueryArgumentException(what, sprintf "%O is not a valid %s." value what) |> raise

        let raw =
            match value with
            | :? Enum
            | :? byte
            | :? sbyte
            | :? int16
            | :? uint16
            | :? int
            | :? uint32
            | :? int64 -> Convert.ToInt64 value
            | _ -> invalid ()

        if raw < 0L || raw > 255L || not (Enum.IsDefined(typeof<'E>, byte raw)) then
            invalid ()

        LanguagePrimitives.EnumOfValue<byte, 'E>(byte raw)

    let private rankMethodOf (value: obj) : RankMethod =
        match toPlEnum<PlRankMethod> "rank method" value with
        | PlRankMethod.Average -> RankMethod.Average
        | PlRankMethod.Min -> RankMethod.Min
        | PlRankMethod.Max -> RankMethod.Max
        | PlRankMethod.Dense -> RankMethod.Dense
        | PlRankMethod.Ordinal -> RankMethod.Ordinal
        | PlRankMethod.Random -> RankMethod.Random
        | other -> invalidOp (sprintf "[Polars.NET.Linq Bug] Rank method %O has no IR mapping." other)

    let private interpolationMethodOf (value: obj) : InterpolationMethod =
        match toPlEnum<PlInterpolationMethod> "interpolation method" value with
        | PlInterpolationMethod.Linear -> InterpolationMethod.Linear
        | PlInterpolationMethod.Nearest -> InterpolationMethod.Nearest
        | other -> invalidOp (sprintf "[Polars.NET.Linq Bug] Interpolation method %O has no IR mapping." other)

    let private nullBehaviorOf (value: obj) : NullBehavior =
        match toPlEnum<PlNullBehavior> "null behavior" value with
        | PlNullBehavior.Ignore -> NullBehavior.Ignore
        | PlNullBehavior.Drop -> NullBehavior.Drop
        | other -> invalidOp (sprintf "[Polars.NET.Linq Bug] Null behavior %O has no IR mapping." other)

    let internal quantileMethodOf (value: obj) : QuantileMethod =
        match toPlEnum<PlQuantileMethod> "quantile method" value with
        | PlQuantileMethod.Nearest -> QuantileMethod.Nearest
        | PlQuantileMethod.Higher -> QuantileMethod.Higher
        | PlQuantileMethod.Lower -> QuantileMethod.Lower
        | PlQuantileMethod.Midpoint -> QuantileMethod.Midpoint
        | PlQuantileMethod.Linear -> QuantileMethod.Linear
        | PlQuantileMethod.Equiprobable -> QuantileMethod.Equiprobable
        | other -> invalidOp (sprintf "[Polars.NET.Linq Bug] Quantile method %O has no IR mapping." other)

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
            |> Option.bind (translateReductionOp m.Name)

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
                    elif targetType = typeof<double> || targetType = typeof<float> || targetType = typeof<bool> then
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
                eval ctx strExpr
                |> Option.bind (tryTranslateCast typeof<bool>)

            | _ -> None

        else
            None

    /// The enum value a method is called on, seen through the boxing conversion to System.Enum or object.
    let private enumReceiver (target: Expression) : Expression option =
        match target with
        | :? UnaryExpression as u when u.NodeType = ExpressionType.Convert && u.Operand.Type.IsEnum -> Some u.Operand
        | t when t.Type.IsEnum -> Some t
        | _ -> None

    /// Instance methods of an enum value: ToString (the member name), Equals and HasFlag (on underlying values).
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

        if (m.Name = "IsNull" || m.Name = "isNull") && args.Length = 1 then
            eval ctx args.Head
            |> Option.orElseWith (fun () -> tryResolveEntityNullTarget ctx args.Head)
            |> Option.map (fun s -> ScalarExpr.Unary(UnaryOp.IsNull, s))
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

        elif
            m.DeclaringType = typeof<Convert>
            || (isNull target && (m.Name = "Parse" || m.Name = "TryParse"))
        then
            match translateConversionOrParseMethod eval ctx m target args with
            | Some s -> Some s
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
            match args with
            | [ strExpr ] when strExpr.Type = typeof<string> ->
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
            | _ -> None

        elif
            m.DeclaringType = typeof<string>
            || not (isNull target) && target.Type = typeof<string>
        then
            translateStringMethod eval ctx m target args

        elif
            m.DeclaringType = typeof<DateTime>
            || m.DeclaringType = typeof<DateOnly>
            || m.DeclaringType = typeof<TimeOnly>
            || not (isNull target) && PolarsTypeHelper.IsTemporalType target.Type
        then
            translateTemporalMethod eval ctx m target args

        elif isNumericDeclaringType m.DeclaringType && isNull target then
            translateNumericMethod (eval ctx) m args

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