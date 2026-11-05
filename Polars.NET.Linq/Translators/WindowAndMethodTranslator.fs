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

    /// Maps the user-facing Polars rank method to the IR vocabulary.
    let private ofPlRankMethod (m: PlRankMethod) : RankMethod =
        match m with
        | PlRankMethod.Min -> RankMethod.Min
        | PlRankMethod.Max -> RankMethod.Max
        | PlRankMethod.Dense -> RankMethod.Dense
        | PlRankMethod.Ordinal -> RankMethod.Ordinal
        | PlRankMethod.Random -> RankMethod.Random
        | _ -> RankMethod.Average

    /// Maps the user-facing Polars interpolation method to the IR vocabulary.
    let private ofPlInterpolationMethod (m: PlInterpolationMethod) : InterpolationMethod =
        match m with
        | PlInterpolationMethod.Nearest -> InterpolationMethod.Nearest
        | _ -> InterpolationMethod.Linear

    /// Maps the user-facing Polars quantile method to the IR vocabulary.
    let private ofPlQuantileMethod (m: PlQuantileMethod) : QuantileMethod =
        match m with
        | PlQuantileMethod.Higher -> QuantileMethod.Higher
        | PlQuantileMethod.Lower -> QuantileMethod.Lower
        | PlQuantileMethod.Midpoint -> QuantileMethod.Midpoint
        | PlQuantileMethod.Linear -> QuantileMethod.Linear
        | PlQuantileMethod.Equiprobable -> QuantileMethod.Equiprobable
        | _ -> QuantileMethod.Nearest

    /// Maps the user-facing Polars null behavior to the IR vocabulary.
    let private ofPlNullBehavior (nb: PlNullBehavior) : NullBehavior =
        match nb with
        | PlNullBehavior.Drop -> NullBehavior.Drop
        | _ -> NullBehavior.Ignore

    let private tryExtractPlRankMethod (value: obj) : PlRankMethod =
        match value with
        | :? PlRankMethod as pm -> pm
        | null -> PlRankMethod.Average
        | other ->
            try
                let byteVal = Convert.ToByte other
                LanguagePrimitives.EnumOfValue<byte, PlRankMethod> byteVal
            with _ ->
                PlRankMethod.Average

    let private tryExtractPlInterpolationMethod (value: obj) : PlInterpolationMethod =
        match value with
        | :? PlInterpolationMethod as pm -> pm
        | null -> PlInterpolationMethod.Linear
        | other ->
            try
                let byteVal = Convert.ToByte other
                LanguagePrimitives.EnumOfValue<byte, PlInterpolationMethod> byteVal
            with _ ->
                PlInterpolationMethod.Linear

    let private tryExtractPlQuantileMethod (value: obj) : PlQuantileMethod =
        match value with
        | :? PlQuantileMethod as pm -> pm
        | null -> PlQuantileMethod.Nearest
        | other ->
            try
                let byteVal = Convert.ToByte other
                LanguagePrimitives.EnumOfValue<byte, PlQuantileMethod> byteVal
            with _ ->
                PlQuantileMethod.Nearest

    let private tryExtractRankMethod (value: obj) : RankMethod = tryExtractPlRankMethod value |> ofPlRankMethod

    let private tryExtractInterpolationMethod (value: obj) : InterpolationMethod =
        tryExtractPlInterpolationMethod value |> ofPlInterpolationMethod

    let internal tryExtractQuantileMethod (value: obj) : QuantileMethod =
        tryExtractPlQuantileMethod value |> ofPlQuantileMethod

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
            match tryEvaluate expr with
            | Some nVal ->
                try
                    let intVal = Convert.ToInt64 nVal
                    Some(ScalarExpr.Literal(LiteralValue.Int64 intVal))
                with _ ->
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
            match tryEvaluate nExpr with
            | Some nVal ->
                try
                    let intVal = Convert.ToInt64 nVal
                    Some(ScalarExpr.Literal(LiteralValue.Int64 intVal))
                with _ ->
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
            match targetS (), resolveN nExpr, tryEvaluate nbExpr with
            | Some eS, Some nS, Some(:? PlNullBehavior as nb) ->
                Some(ScalarExpr.Function(ScalarFunction.Diff(ofPlNullBehavior nb), [ eS; nS ], LogicalType.Unknown))
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
                ScalarExpr.Function(ScalarFunction.Rank RankMethod.Average, [ eS; ScalarExpr.Literal(LiteralValue.Bool false) ], LogicalType.Unknown))

        | [ descExpr ] when descExpr.Type = typeof<bool> ->
            match targetS (), tryEvaluate descExpr with
            | Some eS, Some(:? bool as desc) ->
                Some(ScalarExpr.Function(ScalarFunction.Rank RankMethod.Average, [ eS; ScalarExpr.Literal(LiteralValue.Bool desc) ], LogicalType.Unknown))
            | _ -> None

        | [ methodExpr ] ->
            match targetS (), tryEvaluate methodExpr with
            | Some eS, Some mObj ->
                let m = tryExtractRankMethod mObj
                Some(ScalarExpr.Function(ScalarFunction.Rank m, [ eS; ScalarExpr.Literal(LiteralValue.Bool false) ], LogicalType.Unknown))
            | _ -> None

        | [ methodExpr; descExpr ] ->
            match targetS (), tryEvaluate methodExpr, tryEvaluate descExpr with
            | Some eS, Some mObj, Some(:? bool as desc) ->
                let m = tryExtractRankMethod mObj
                Some(ScalarExpr.Function(ScalarFunction.Rank m, [ eS; ScalarExpr.Literal(LiteralValue.Bool desc) ], LogicalType.Unknown))
            | _ -> None

        | [ methodExpr; descExpr; seedExpr ] ->
            match targetS (), tryEvaluate methodExpr, tryEvaluate descExpr, tryEvaluate seedExpr with
            | Some eS, Some mObj, Some(:? bool as desc), Some _seedObj ->
                let m = tryExtractRankMethod mObj
                Some(ScalarExpr.Function(ScalarFunction.Rank m, [ eS; ScalarExpr.Literal(LiteralValue.Bool desc) ], LogicalType.Unknown))
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
            match targetS (), tryEvaluate nExpr with
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
            let unwrapArg (arg: Expression) : Expression =
                match unwrap arg with
                | null -> null
                | other -> other

            let extractList (arg: Expression) : Expression list =
                match unwrapArg arg with
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
                    let desc = match tryEvaluate oArg with Some (:? bool as b) -> b | _ -> false
                    extractList pArg, [], desc
                | [ pArg; oArg ] ->
                    if isOrderSensitive target then
                        extractList pArg, extractList oArg, false
                    else
                        extractList pArg @ extractList oArg, [], false
                | pArg :: oArg :: descArg :: _ when descArg.Type = typeof<bool> ->
                    let desc = match tryEvaluate descArg with Some (:? bool as b) -> b | _ -> false
                    if desc || isOrderSensitive target then
                        extractList pArg, extractList oArg, desc
                    else
                        extractList pArg @ extractList oArg, [], false
                | other ->
                    other |> List.collect extractList, [], false

            match translateList partitionExprs, translateList orderExprs with
            | Some pExprs, Some oExprs when pExprs.Length > 0 || oExprs.Length > 0 ->
                let orderings =
                    oExprs
                    |> List.map (fun e ->
                        {
                            SortOrder.Expr = e
                            Descending = isDescending
                            NullsLast = false
                        })
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
            match eval ctx effectiveTarget, tryEvaluate ddofExpr with
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
                match tryEvaluate revExpr with
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
            match eval ctx effectiveTarget, tryEvaluate methodExpr with
            | Some targetS, Some methodVal ->
                let method = tryExtractInterpolationMethod methodVal
                Some(ScalarExpr.Function(ScalarFunction.Interpolate method, [ targetS ], LogicalType.Unknown))
            | Some targetS, None ->
                Some(ScalarExpr.Function(ScalarFunction.Interpolate InterpolationMethod.Linear, [ targetS ], LogicalType.Unknown))
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
            match eval ctx effectiveTarget, tryEvaluate qExpr with
            | Some targetS, Some qVal ->
                try
                    let q = Convert.ToDouble qVal
                    Some(ScalarExpr.Aggregation(AggKind.Quantile q, Some targetS, false))
                with _ ->
                    None
            | _ -> None

        | [ qExpr; _methodExpr ] ->
            match eval ctx effectiveTarget, tryEvaluate qExpr with
            | Some targetS, Some qVal ->
                try
                    let q = Convert.ToDouble qVal
                    Some(ScalarExpr.Aggregation(AggKind.Quantile q, Some targetS, false))
                with _ ->
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

        elif not (isNull target) && target.Type = typeof<decimal> then
            match m.Name, args with
            | "ToString", [] ->
                eval ctx target
                |> Option.bind (tryTranslateCast typeof<string>)

            | "ToString", [ formatExpr ] ->
                match eval ctx target, tryEvaluate formatExpr with
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