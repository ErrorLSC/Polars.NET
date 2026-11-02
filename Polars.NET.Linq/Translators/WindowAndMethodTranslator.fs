namespace Polars.NET.Linq

open Polars.NET.Core
open System
open System.Text
open System.Linq
open System.Text.RegularExpressions
open System.Linq.Expressions
open System.Reflection

[<AutoOpen>]
module internal WindowAndMethodTranslator =

    let private translateShiftMethod (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetH () = eval ctx target

        let resolveOffset (expr: Expression) : ExprHandle option =
            match tryEvaluate expr with
            | Some nVal ->
                try
                    let intVal = Convert.ToInt64 nVal
                    Some(PolarsWrapper.Lit intVal)
                with _ ->
                    eval ctx expr
            | None ->
                eval ctx expr

        let offsetHandleOpt =
            match args with
            | [] -> Some(PolarsWrapper.Lit 1L)
            | [ nExpr ] -> resolveOffset nExpr
            | _ -> None

        match targetH (), offsetHandleOpt with
        | Some eH, Some nH -> Some(PolarsWrapper.Shift(eH, nH))
        | _ -> None

    let private translateDiffMethod (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetH () = eval ctx target

        let resolveN nExpr =
            match tryEvaluate nExpr with
            | Some nVal ->
                try
                    let intVal = Convert.ToInt64 nVal
                    Some(PolarsWrapper.Lit intVal)
                with _ ->
                    eval ctx nExpr
            | None -> eval ctx nExpr

        match args with
        | [] ->
            targetH ()
            |> Option.map (fun eH -> PolarsWrapper.Diff(eH, PolarsWrapper.Lit 1L, PlNullBehavior.Ignore))
        | [ nExpr ] ->
            match targetH (), resolveN nExpr with
            | Some eH, Some nH -> Some(PolarsWrapper.Diff(eH, nH, PlNullBehavior.Ignore))
            | _ -> None
        | [ nExpr; nbExpr ] ->
            match targetH (), resolveN nExpr, tryEvaluate nbExpr with
            | Some eH, Some nH, Some(:? PlNullBehavior as nb) -> Some(PolarsWrapper.Diff(eH, nH, nb))
            | _ -> None
        | _ -> None

    let private tryExtractRankMethod (value: obj) : PlRankMethod =
        match value with
        | :? PlRankMethod as pm -> pm
        | null -> PlRankMethod.Average
        | other ->
            try
                let byteVal = Convert.ToByte other
                LanguagePrimitives.EnumOfValue<byte, PlRankMethod> byteVal
            with _ ->
                PlRankMethod.Average

    let private translateRankMethod (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetH () = eval ctx target

        match args with
        | [] ->
            targetH ()
            |> Option.map (fun eH -> PolarsWrapper.Rank(eH, PlRankMethod.Average, false, Nullable()))

        | [ descExpr ] when descExpr.Type = typeof<bool> ->
            match targetH (), tryEvaluate descExpr with
            | Some eH, Some(:? bool as desc) -> Some(PolarsWrapper.Rank(eH, PlRankMethod.Average, desc, Nullable()))
            | _ -> None

        | [ methodExpr ] ->
            match targetH (), tryEvaluate methodExpr with
            | Some eH, Some mObj ->
                let m = tryExtractRankMethod mObj
                Some(PolarsWrapper.Rank(eH, m, false, Nullable()))
            | _ -> None

        | [ methodExpr; descExpr ] ->
            match targetH (), tryEvaluate methodExpr, tryEvaluate descExpr with
            | Some eH, Some mObj, Some(:? bool as desc) ->
                let m = tryExtractRankMethod mObj
                Some(PolarsWrapper.Rank(eH, m, desc, Nullable()))
            | _ -> None

        | [ methodExpr; descExpr; seedExpr ] ->
            match targetH (), tryEvaluate methodExpr, tryEvaluate descExpr, tryEvaluate seedExpr with
            | Some eH, Some mObj, Some(:? bool as desc), Some seedObj ->
                let m = tryExtractRankMethod mObj

                let seedNullable =
                    match seedObj with
                    | :? uint64 as s -> Nullable<uint64>(s)
                    | _ -> Nullable<uint64>()

                Some(PolarsWrapper.Rank(eH, m, desc, seedNullable))
            | _ -> None

        | _ -> None

    let private translatePctChangeMethod (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetH () = eval ctx target

        match args with
        | [] -> targetH () |> Option.map (fun eH -> PolarsWrapper.PctChange(eH, 1L))
        | [ nExpr ] ->
            match targetH (), tryEvaluate nExpr with
            | Some eH, Some nVal -> Some(PolarsWrapper.PctChange(eH, Convert.ToInt64 nVal))
            | _ -> None
        | _ -> None

    let private translateOverMethod (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        match eval ctx target with
        | None -> None
        | Some targetH ->
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

            let translateList (exprs: Expression list) : ExprHandle array option =
                if exprs.IsEmpty then
                    Some [||]
                else
                    let handles =
                        exprs
                        |> List.choose (fun arg ->
                            match arg with
                            | :? LambdaExpression as lam ->
                                eval (TranslationContext.Entity lam.Parameters.[0].Name) lam.Body
                            | other -> eval ctx other)
                    if handles.Length = exprs.Length then
                        Some(Array.ofList handles)
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

            // Identify partitions, order by, and descending
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
            | Some partitionHandles, Some orderHandles when partitionHandles.Length > 0 || orderHandles.Length > 0 ->
                Some(
                    PolarsWrapper.Over(
                        targetH,
                        partitionHandles,
                        orderHandles,
                        descending = isDescending,
                        nullsLast = false,
                        multithreaded = true,
                        maintainOrder = true,
                        mappingCode = PlWindowMapping.GroupsToRows
                    )
                )
            | _ -> None

    let private translateScalarReductionMethod (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then
                args.Head, args.Tail
            else
                target, args

        match m.Name, effectiveArgs with
        | ("Sum" | "Mean" | "Average" | "Min" | "Max" | "Median" | "First" | "Last"), [] ->
            eval ctx effectiveTarget
            |> Option.bind (translateReductionOp m.Name)

        | "Count", [] -> eval ctx effectiveTarget |> Option.map PolarsWrapper.Count

        | ("Std" | "StdDev"), [] ->
            eval ctx effectiveTarget
            |> Option.bind (translateDispersionOp m.Name 1uy)

        | ("Var" | "Variance"), [] ->
            eval ctx effectiveTarget
            |> Option.bind (translateDispersionOp m.Name 1uy)

        | ("Std" | "StdDev" | "Var" | "Variance"), [ ddofExpr ] ->
            match eval ctx effectiveTarget, tryEvaluate ddofExpr with
            | Some colH, Some ddofVal ->
                let ddof = Convert.ToByte ddofVal
                translateDispersionOp m.Name ddof colH
            | _ -> None

        | _ -> None

    let private translateCumulativeMethod (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
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
        |> Option.bind (fun colH ->
            match m.Name with
            | "CumSum" -> Some(PolarsWrapper.CumSum(colH, reverse))
            | "CumMax" -> Some(PolarsWrapper.CumMax(colH, reverse))
            | "CumMin" -> Some(PolarsWrapper.CumMin(colH, reverse))
            | "CumProd" -> Some(PolarsWrapper.CumProd(colH, reverse))
            | "CumCount" -> Some(PolarsWrapper.CumCount(colH, reverse))
            | _ -> None)

    let private tryExtractInterpolationMethod (value: obj) : PlInterpolationMethod =
        match value with
        | :? PlInterpolationMethod as pm -> pm
        | null -> PlInterpolationMethod.Linear
        | other ->
            try
                let byteVal = Convert.ToByte other
                LanguagePrimitives.EnumOfValue<byte, PlInterpolationMethod> byteVal
            with _ ->
                PlInterpolationMethod.Linear

    let private translateInterpolateMethod (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then
                args.Head, args.Tail
            else
                target, args

        match m.Name, effectiveArgs with
        | "Interpolate", [] ->
            eval ctx effectiveTarget
            |> Option.map (fun h -> PolarsWrapper.Interpolate(h, PlInterpolationMethod.Linear))

        | "Interpolate", [ methodExpr ] ->
            match eval ctx effectiveTarget, tryEvaluate methodExpr with
            | Some targetH, Some methodVal ->
                let method = tryExtractInterpolationMethod methodVal
                Some(PolarsWrapper.Interpolate(targetH, method))
            | Some targetH, None -> Some(PolarsWrapper.Interpolate(targetH, PlInterpolationMethod.Linear))
            | _ -> None

        | "InterpolateBy", [ byExpr ] ->
            match eval ctx effectiveTarget, eval ctx byExpr with
            | Some targetH, Some byH -> Some(PolarsWrapper.InterpolateBy(targetH, byH))
            | _ -> None

        | _ -> None

    let internal tryExtractQuantileMethod (value: obj) : PlQuantileMethod =
        match value with
        | :? PlQuantileMethod as pm -> pm
        | null -> PlQuantileMethod.Nearest
        | other ->
            try
                let byteVal = Convert.ToByte other
                LanguagePrimitives.EnumOfValue<byte, PlQuantileMethod> byteVal
            with _ ->
                PlQuantileMethod.Nearest

    let internal translateQuantileMethod (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && not args.IsEmpty then
                args.Head, args.Tail
            else
                target, args

        match effectiveArgs with
        | [ qExpr ] ->
            match eval ctx effectiveTarget, tryEvaluate qExpr with
            | Some targetH, Some qVal ->
                try
                    let q = Convert.ToDouble qVal
                    Some(PolarsWrapper.Quantile(targetH, q, PlQuantileMethod.Nearest))
                with _ ->
                    None
            | _ -> None

        | [ qExpr; methodExpr ] ->
            match eval ctx effectiveTarget, tryEvaluate qExpr, tryEvaluate methodExpr with
            | Some targetH, Some qVal, Some mVal ->
                try
                    let q = Convert.ToDouble qVal
                    let method = tryExtractQuantileMethod mVal
                    Some(PolarsWrapper.Quantile(targetH, q, method))
                with _ ->
                    None
            | _ -> None

        | _ -> None

    let private isIntegerType (t: Type) =
        t = typeof<int>
        || t = typeof<int64>
        || t = typeof<int16>
        || t = typeof<sbyte>
        || t = typeof<byte>
        || t = typeof<uint32>
        || t = typeof<uint64>
        || t = typeof<uint16>

    let private translateConversionOrParseMethod (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetType = m.DeclaringType

        // 1. Static Parse on numeric types (int.Parse(s), long.Parse(s), decimal.Parse(s), double.Parse(s), etc.)
        if isNull target && (m.Name = "Parse" || m.Name = "TryParse") then
            match args with
            | strExpr :: _ when strExpr.Type = typeof<string> ->
                match eval ctx strExpr with
                | Some strH ->
                    if isIntegerType targetType then
                        tryGetPolarsDataType targetType
                        |> Option.map (fun dtype ->
                            PolarsWrapper.StrToInteger(strH, PolarsWrapper.Lit 10, dtype, strict = false))
                    elif targetType = typeof<decimal> then
                        Some(PolarsWrapper.StrToDecimal(strH, 4))
                    elif targetType = typeof<double> || targetType = typeof<float> || targetType = typeof<bool> then
                        tryTranslateCast targetType strH
                    else
                        None
                | None -> None
            | _ -> None

        // 2. Convert.* (Convert.ToInt32, Convert.ToHexString, Convert.FromHexString, etc.)
        elif isNull target && targetType = typeof<Convert> then
            match m.Name, args with
            | "FromHexString", [ strExpr ] ->
                eval ctx strExpr
                |> Option.map (fun strH -> PolarsWrapper.StrHexDecode(strH, strict = false))

            | "FromBase64String", [ strExpr ] ->
                eval ctx strExpr
                |> Option.map (fun strH -> PolarsWrapper.StrBase64Decode(strH, strict = false))

            | "ToHexString", [ strExpr ] ->
                eval ctx strExpr
                |> Option.map PolarsWrapper.StrHexEncode

            | "ToBase64String", [ strExpr ] ->
                eval ctx strExpr
                |> Option.map PolarsWrapper.StrBase64Encode

            // Convert.ToInt32(s), Convert.ToInt64(s), etc.
            | ("ToInt32" | "ToInt64" | "ToInt16" | "ToByte" | "ToSByte" | "ToUInt32" | "ToUInt64" | "ToUInt16") as convName, [ strExpr ] when strExpr.Type = typeof<string> ->
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

                match eval ctx strExpr, tryGetPolarsDataType intType with
                | Some strH, Some dtype ->
                    Some(PolarsWrapper.StrToInteger(strH, PolarsWrapper.Lit 10, dtype, strict = false))
                | _ -> None

            // Convert.ToInt32(s, fromBase), Convert.ToInt64(s, fromBase)
            | "ToInt32" | "ToInt64" as convName, [ strExpr; baseExpr ] when strExpr.Type = typeof<string> ->
                let intType = if convName = "ToInt32" then typeof<int> else typeof<int64>
                match eval ctx strExpr, eval ctx baseExpr, tryGetPolarsDataType intType with
                | Some strH, Some baseH, Some dtype ->
                    Some(PolarsWrapper.StrToInteger(strH, baseH, dtype, strict = false))
                | _ -> None

            | "ToDecimal", [ strExpr ] when strExpr.Type = typeof<string> ->
                eval ctx strExpr
                |> Option.map (fun strH -> PolarsWrapper.StrToDecimal(strH, 4))

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
        (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let effectiveTarget, effectiveArgs =
            if isNull target && args.Length > 0 then
                args.Head, args.Tail
            else
                target, args

        if (m.Name = "IsNull" || m.Name = "isNull") && args.Length = 1 then
            eval ctx args.Head
            |> Option.map PolarsWrapper.IsNull
        elif not (isNull effectiveTarget) && m.Name = "Shift" then
            translateShiftMethod eval ctx effectiveTarget effectiveArgs
        // Transparently unwrap client-side sequence materialization methods (ToArray, ToList, AsEnumerable)
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
        // Regex.Escape(str)
        elif m.DeclaringType = typeof<Regex> && isNull target && m.Name = "Escape" then
            match args with
            | [ strExpr ] ->
                eval ctx strExpr |> Option.map PolarsWrapper.StrEscapeRegex
            | _ -> None

        // 1. Conversion or Parse methods (int.Parse, Convert.ToInt32, Convert.FromHexString, etc.)
        elif
            m.DeclaringType = typeof<Convert>
            || (isNull target && (m.Name = "Parse" || m.Name = "TryParse"))
        then
            match translateConversionOrParseMethod eval ctx m target args with
            | Some h -> Some h
            | None ->
                if m.DeclaringType = typeof<DateTime> || m.DeclaringType = typeof<DateOnly> || m.DeclaringType = typeof<TimeOnly> then
                    translateTemporalMethod eval ctx m target args
                else
                    None

        // TextInfo.ToTitleCase(s)
        elif m.Name = "ToTitleCase" && args.Length >= 1 then
            match args |> List.tryFind (fun a -> a.Type = typeof<string>) with
            | Some strExpr ->
                eval ctx strExpr |> Option.map PolarsWrapper.StrToTitlecase
            | None -> None

        // Encoding.UTF8.GetByteCount(s)
        elif not (isNull m.DeclaringType) && typeof<Encoding>.IsAssignableFrom m.DeclaringType && m.Name = "GetByteCount" then
            match args with
            | [ strExpr ] when strExpr.Type = typeof<string> ->
                eval ctx strExpr |> Option.map PolarsWrapper.StrLenBytes
            | _ -> None

        // Enumerable string extensions: s.Take(n), s.TakeLast(n), s.Reverse()
        elif m.DeclaringType = typeof<Enumerable> && isNull target then
            match m.Name, args with
            | "Take", [ strExpr; countExpr ] when strExpr.Type = typeof<string> ->
                match eval ctx strExpr, eval ctx countExpr with
                | Some strH, Some countH -> Some(PolarsWrapper.StrHead(strH, countH))
                | _ -> None

            | "TakeLast", [ strExpr; countExpr ] when strExpr.Type = typeof<string> ->
                match eval ctx strExpr, eval ctx countExpr with
                | Some strH, Some countH -> Some(PolarsWrapper.StrTail(strH, countH))
                | _ -> None

            | "Reverse", [ strExpr ] when strExpr.Type = typeof<string> ->
                eval ctx strExpr |> Option.map PolarsWrapper.StrReverse

            | _ -> None

        // Microsoft.VisualBasic.Strings.StrReverse(s) or similar
        elif m.Name = "StrReverse" && isNull target && args.Length = 1 && args.[0].Type = typeof<string> then
            eval ctx args.[0] |> Option.map PolarsWrapper.StrReverse

        // char.ToUpper(c) / char.ToLower(c)
        elif m.DeclaringType = typeof<char> && isNull target then
            match m.Name, args with
            | ("ToUpper" | "ToUpperInvariant"), [ charExpr ] ->
                eval ctx charExpr |> Option.map PolarsWrapper.StrToUpper
            | ("ToLower" | "ToLowerInvariant"), [ charExpr ] ->
                eval ctx charExpr |> Option.map PolarsWrapper.StrToLower
            | _ -> None

        // 2. String methods (Static & Instance)
        elif
            m.DeclaringType = typeof<string>
            || not (isNull target) && target.Type = typeof<string>
        then
            translateStringMethod eval ctx m target args

        // 2. Temporal methods (DateTime / DateOnly / TimeOnly)
        elif
            m.DeclaringType = typeof<DateTime>
            || m.DeclaringType = typeof<DateOnly>
            || m.DeclaringType = typeof<TimeOnly>
            || not (isNull target) && isTemporalType target.Type
        then
            translateTemporalMethod eval ctx m target args

        // 3. Math / MathF / Decimal methods (All static)
        elif isNumericDeclaringType m.DeclaringType && isNull target then
            translateNumericMethod eval ctx m args

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
            translateScalarReductionMethod eval ctx m target args

        // 4. Decimal / Numeric Instance Methods (e.g. d.ToString())
        elif not (isNull target) && target.Type = typeof<decimal> then
            match m.Name, args with
            | "ToString", [] ->
                eval ctx target
                |> Option.bind (tryTranslateCast typeof<string>)

            | "ToString", [ formatExpr ] ->
                match eval ctx target, tryEvaluate formatExpr with
                | Some t, Some(:? string as fmt) ->
                    if fmt.StartsWith("F", StringComparison.OrdinalIgnoreCase) then
                        // Safely parse precision, defaulting to 2 decimal places if "F" / "f" has no explicit digits
                        let scale =
                            let s = fmt.Substring 1

                            match Int32.TryParse(s) with
                            | true, v -> v
                            | false, _ -> 2

                        // Two-phase cast: First cast to target scale Decimal (ensures rounding & zero-padding), then cast to String
                        let decType = PolarsWrapper.NewDecimalType(38, scale)
                        let decTypeExpr = PolarsWrapper.DataTypeExprFromDataType(decType)

                        let scaled =
                            PolarsWrapper.ExprCast(t, decTypeExpr, strict = false, wrapNumerical = false)

                        tryTranslateCast typeof<string> scaled
                    else
                        None
                | _ -> None
            | _ -> None

        else
            None

