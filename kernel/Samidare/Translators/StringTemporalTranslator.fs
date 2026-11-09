namespace Kaminari.Samidare

open Kaminari.Samidare.IR
open System
open System.Text
open System.Reflection
open System.Linq.Expressions
open System.Text.RegularExpressions

[<AutoOpen>]
module internal StringTemporalTranslator =

    /// Evaluates StringComparison expression to determine case insensitivity.
    let private tryEvaluateCaseInsensitive (compExpr: Expression) : bool option =
        match PartialEvaluator.tryEvaluate compExpr with
        | Some(:? StringComparison as sc) ->
            match sc with
            | StringComparison.OrdinalIgnoreCase
            | StringComparison.InvariantCultureIgnoreCase
            | StringComparison.CurrentCultureIgnoreCase -> Some true
            | _ -> Some false
        | Some(:? bool as ignoreCase) -> Some ignoreCase
        | Some other ->
            try
                let intVal = Convert.ToInt32 other

                match LanguagePrimitives.EnumOfValue<int, StringComparison> intVal with
                | StringComparison.OrdinalIgnoreCase
                | StringComparison.InvariantCultureIgnoreCase
                | StringComparison.CurrentCultureIgnoreCase -> Some true
                | _ -> Some false
            with ConversionFailure ->
                None
        | None -> None

    let private tryExtractCharSet (expr: Expression) : char array option =
        match unwrap expr with
        | :? NewArrayExpression as nae when nae.NodeType = ExpressionType.NewArrayInit ->
            let chars =
                nae.Expressions
                |> Seq.choose (fun e ->
                    match PartialEvaluator.tryEvaluate e with
                    | Some(:? char as c) -> Some c
                    | Some(:? string as s) when s.Length = 1 -> Some s.[0]
                    | _ -> None)
                |> Seq.toArray
            if chars.Length = nae.Expressions.Count then Some chars else None
        | other ->
            match PartialEvaluator.tryEvaluate other with
            | Some(:? (char array) as ca) -> Some ca
            | Some(:? string as s) -> Some(s.ToCharArray())
            | Some(:? (string array) as sa) ->
                let chars = sa |> Array.choose (fun s -> if s.Length = 1 then Some s.[0] else None)
                if chars.Length = sa.Length then Some chars else None
            | _ -> None

    let private buildCharClassRegex (chars: char array) : string =
        let escaped = chars |> Array.map (fun c -> Regex.Escape(string c))
        sprintf "(%s)" (String.concat "|" escaped)


    let internal translateStringMethod
        (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let targetS () = eval ctx target

        match m.Name, args with
        // --- 1. String.Concat ---
        | "Concat", _ when isNull target ->
            let flatArgs =
                match args with
                | [ single ] ->
                    match unwrap single with
                    | :? NewArrayExpression as nae when nae.NodeType = ExpressionType.NewArrayInit ->
                        nae.Expressions |> Seq.toList
                    | _ -> args
                | other -> other

            if flatArgs.IsEmpty then
                Some(ScalarExpr.Literal(LiteralValue.String ""))
            elif flatArgs.Length = 1 then
                eval ctx flatArgs.Head
            else
                let argExprs = flatArgs |> List.choose (eval ctx)
                if argExprs.Length = flatArgs.Length then
                    let rustFmt = String.replicate argExprs.Length "{}"
                    Some(ScalarExpr.Function(ScalarFunction.FormatString, ScalarExpr.Literal(LiteralValue.String rustFmt) :: argExprs, LogicalTypes.String))
                else
                    None

        // --- 1b. String.Join ---
        | "Join", sepExpr :: rawItems when isNull target && not rawItems.IsEmpty ->
            let flatItems =
                match rawItems with
                | [ single ] ->
                    match unwrap single with
                    | :? NewArrayExpression as nae when nae.NodeType = ExpressionType.NewArrayInit ->
                        nae.Expressions |> Seq.toList
                    | _ -> [ single ]
                | other -> other

            let sepStrOpt =
                match PartialEvaluator.tryEvaluate sepExpr with
                | Some(:? string as sep) -> Some sep
                | Some(:? char as sep) -> Some(string sep)
                | _ -> None

            match sepStrOpt with
            | Some sep ->
                let itemExprs = flatItems |> List.choose (eval ctx)
                if itemExprs.Length = flatItems.Length && not itemExprs.IsEmpty then
                    Some(ScalarExpr.Function(ScalarFunction.ConcatString, ScalarExpr.Literal(LiteralValue.String sep) :: itemExprs, LogicalTypes.String))
                else
                    None
            | None -> None

        // --- 2. String.Format ---
        | "Format", _ when isNull target ->
            let formatExprOpt, rawItems =
                match args with
                | providerExpr :: formatExpr :: rest when typeof<IFormatProvider>.IsAssignableFrom(providerExpr.Type) ->
                    let items =
                        match rest with
                        | [ :? NewArrayExpression as nae ] when nae.NodeType = ExpressionType.NewArrayInit ->
                            nae.Expressions |> Seq.toList
                        | other -> other
                    Some formatExpr, items
                | formatExpr :: rest ->
                    let items =
                        match rest with
                        | [ :? NewArrayExpression as nae ] when nae.NodeType = ExpressionType.NewArrayInit ->
                            nae.Expressions |> Seq.toList
                        | other -> other
                    Some formatExpr, items
                | [] -> None, []

            match formatExprOpt with
            | Some formatExpr ->
                match PartialEvaluator.tryEvaluate formatExpr with
                | Some(:? string as netFormat) ->
                    let matches = Regex.Matches(netFormat, @"\{(\d+)(?::[^}]+)?\}")
                    let referencedIndices =
                        matches |> Seq.map (fun m -> Int32.Parse(m.Groups.[1].Value)) |> Seq.toList

                    if referencedIndices.IsEmpty then
                        Some(ScalarExpr.Literal(LiteralValue.String netFormat))
                    elif referencedIndices |> List.forall (fun idx -> idx >= 0 && idx < rawItems.Length) then
                        let orderedExprs = referencedIndices |> List.map (fun idx -> rawItems.[idx])
                        let argExprs = orderedExprs |> List.choose (eval ctx)
                        if argExprs.Length = orderedExprs.Length then
                            let rustFmt = Regex.Replace(netFormat, @"\{(\d+)(?::[^}]+)?\}", "{}")
                            Some(ScalarExpr.Function(ScalarFunction.FormatString, ScalarExpr.Literal(LiteralValue.String rustFmt) :: argExprs, LogicalTypes.String))
                        else
                            None
                    else
                        None
                | _ -> None
            | None -> None

        | "IsNullOrEmpty", [ arg ] when isNull target ->
            eval ctx arg
            |> Option.map (fun expr ->
                let isNull = ScalarExpr.Unary(UnaryOp.IsNull, expr)
                let isEmpty = ScalarExpr.Binary(BinaryOp.Eq, expr, ScalarExpr.Literal(LiteralValue.String ""))
                ScalarExpr.Binary(BinaryOp.Or, isNull, isEmpty))

        | "IsNullOrWhiteSpace", [ arg ] when isNull target ->
            eval ctx arg
            |> Option.map (fun expr ->
                let isNull = ScalarExpr.Unary(UnaryOp.IsNull, expr)
                let stripped = ScalarExpr.Function(ScalarFunction.Strip, [ expr; ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String) ], LogicalTypes.String)
                let isEmpty = ScalarExpr.Binary(BinaryOp.Eq, stripped, ScalarExpr.Literal(LiteralValue.String ""))
                ScalarExpr.Binary(BinaryOp.Or, isNull, isEmpty))

        // --- Contains / StartsWith / EndsWith / IndexOf ---
        | ("Contains" | "StartsWith" | "EndsWith" | "IndexOf") as opName, arg :: rest when
            rest.IsEmpty || opName <> "Contains" || rest.Length = 1 ->
            let applyOp (t: ScalarExpr) (p: ScalarExpr) =
                match opName with
                | "Contains" -> ScalarExpr.Function(ScalarFunction.StrContains, [ t; p; ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.Boolean)
                | "StartsWith" -> ScalarExpr.Function(ScalarFunction.StartsWith, [ t; p ], LogicalTypes.Boolean)
                | "EndsWith" -> ScalarExpr.Function(ScalarFunction.EndsWith, [ t; p ], LogicalTypes.Boolean)
                | _ ->
                    let findExpr = ScalarExpr.Function(ScalarFunction.StrFind, [ t; p; ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.Int64)
                    let castedExpr = tryTranslateCast typeof<int> findExpr |> Option.defaultValue findExpr
                    ScalarExpr.Function(ScalarFunction.FillNull, [ castedExpr; ScalarExpr.Literal(LiteralValue.Int32 -1) ], LogicalTypes.Int32)

            match rest with
            | [] ->
                match targetS (), eval ctx arg with
                | Some t, Some p -> Some(applyOp t p)
                | _ -> None
            | compExpr :: _ ->
                match targetS (), eval ctx arg, tryEvaluateCaseInsensitive compExpr with
                | Some t, Some p, Some true ->
                    let tLower = ScalarExpr.Function(ScalarFunction.ToLower, [ t ], LogicalTypes.String)
                    let pLower = ScalarExpr.Function(ScalarFunction.ToLower, [ p ], LogicalTypes.String)
                    Some(applyOp tLower pLower)
                | Some t, Some p, Some false -> Some(applyOp t p)
                | _ -> None

        // --- IndexOfAny ---
        | "IndexOfAny", anyOfExpr :: rest ->
            match tryExtractCharSet anyOfExpr with
            | Some chars when chars.Length = 0 ->
                Some(ScalarExpr.Literal(LiteralValue.Int32 -1))
            | Some chars ->
                let regexPat = buildCharClassRegex chars
                let patExpr = ScalarExpr.Literal(LiteralValue.String regexPat)

                match rest with
                | [] ->
                    targetS ()
                    |> Option.map (fun t ->
                        let findExpr = ScalarExpr.Function(ScalarFunction.StrFind, [ t; patExpr; ScalarExpr.Literal(LiteralValue.Bool false) ], LogicalTypes.Int64)
                        let casted = tryTranslateCast typeof<int> findExpr |> Option.defaultValue findExpr
                        ScalarExpr.Function(ScalarFunction.FillNull, [ casted; ScalarExpr.Literal(LiteralValue.Int32 -1) ], LogicalTypes.Int32))

                | [ startExpr ] ->
                    match targetS (), eval ctx startExpr with
                    | Some t, Some startS ->
                        let startInt = tryTranslateCast typeof<int> startS |> Option.defaultValue startS
                        let sliced = ScalarExpr.Function(ScalarFunction.Slice, [ t; startInt; ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Int64) ], LogicalTypes.String)
                        let findExpr = ScalarExpr.Function(ScalarFunction.StrFind, [ sliced; patExpr; ScalarExpr.Literal(LiteralValue.Bool false) ], LogicalTypes.Int64)
                        let casted = tryTranslateCast typeof<int> findExpr |> Option.defaultValue findExpr
                        let fullIdx = ScalarExpr.Binary(BinaryOp.Add, startInt, casted)
                        Some(ScalarExpr.Function(ScalarFunction.FillNull, [ fullIdx; ScalarExpr.Literal(LiteralValue.Int32 -1) ], LogicalTypes.Int32))
                    | _ -> None

                | [ startExpr; countExpr ] ->
                    match targetS (), eval ctx startExpr, eval ctx countExpr with
                    | Some t, Some startS, Some countS ->
                        let startInt = tryTranslateCast typeof<int> startS |> Option.defaultValue startS
                        let countInt = tryTranslateCast typeof<int> countS |> Option.defaultValue countS
                        let sliced = ScalarExpr.Function(ScalarFunction.Slice, [ t; startInt; countInt ], LogicalTypes.String)
                        let findExpr = ScalarExpr.Function(ScalarFunction.StrFind, [ sliced; patExpr; ScalarExpr.Literal(LiteralValue.Bool false) ], LogicalTypes.Int64)
                        let casted = tryTranslateCast typeof<int> findExpr |> Option.defaultValue findExpr
                        let fullIdx = ScalarExpr.Binary(BinaryOp.Add, startInt, casted)
                        Some(ScalarExpr.Function(ScalarFunction.FillNull, [ fullIdx; ScalarExpr.Literal(LiteralValue.Int32 -1) ], LogicalTypes.Int32))
                    | _ -> None
                | _ -> None
            | None -> None

        | "ToUpper", []
        | "ToUpperInvariant", [] -> targetS () |> Option.map (fun t -> ScalarExpr.Function(ScalarFunction.ToUpper, [ t ], LogicalTypes.String))
        | "ToLower", []
        | "ToLowerInvariant", [] -> targetS () |> Option.map (fun t -> ScalarExpr.Function(ScalarFunction.ToLower, [ t ], LogicalTypes.String))
        | "Substring", [ startExpr ] ->
            match targetS (), eval ctx startExpr with
            | Some t, Some startS -> Some(ScalarExpr.Function(ScalarFunction.Slice, [ t; startS; ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Int64) ], LogicalTypes.String))
            | _ -> None
        | "Substring", [ startExpr; lenExpr ] ->
            match targetS (), eval ctx startExpr, eval ctx lenExpr with
            | Some t, Some startS, Some lenS -> Some(ScalarExpr.Function(ScalarFunction.Slice, [ t; startS; lenS ], LogicalTypes.String))
            | _ -> None

        // --- Replace ---
        | "Replace", [ oldExpr; newExpr ] ->
            match targetS (), eval ctx oldExpr, eval ctx newExpr with
            | Some t, Some o, Some n -> Some(ScalarExpr.Function(ScalarFunction.ReplaceAll, [ t; o; n; ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.String))
            | _ -> None

        | "Replace", [ oldExpr; newExpr; compExpr ] ->
            match targetS (), eval ctx newExpr, tryEvaluateCaseInsensitive compExpr with
            | Some t, Some n, Some false ->
                match eval ctx oldExpr with
                | Some o -> Some(ScalarExpr.Function(ScalarFunction.ReplaceAll, [ t; o; n; ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.String))
                | _ -> None
            | Some t, Some n, Some true ->
                match PartialEvaluator.tryEvaluate oldExpr with
                | Some(:? string as oldStr) ->
                    let escapedPattern = sprintf "(?i)%s" (Regex.Escape oldStr)
                    Some(ScalarExpr.Function(ScalarFunction.ReplaceAll, [ t; ScalarExpr.Literal(LiteralValue.String escapedPattern); n; ScalarExpr.Literal(LiteralValue.Bool false) ], LogicalTypes.String))
                | _ -> None
            | _ -> None

        // --- PadLeft / PadRight ---
        | "PadLeft", [ widthExpr ] ->
            match targetS (), eval ctx widthExpr with
            | Some t, Some w -> Some(ScalarExpr.Function(ScalarFunction.PadStart, [ t; w; ScalarExpr.Literal(LiteralValue.String " ") ], LogicalTypes.String))
            | _ -> None

        | "PadLeft", [ widthExpr; padCharExpr ] ->
            match targetS (), eval ctx widthExpr with
            | Some t, Some w ->
                match PartialEvaluator.tryEvaluate padCharExpr with
                | Some(:? char as c) -> Some(ScalarExpr.Function(ScalarFunction.PadStart, [ t; w; ScalarExpr.Literal(LiteralValue.String(string c)) ], LogicalTypes.String))
                | Some(:? string as s) -> Some(ScalarExpr.Function(ScalarFunction.PadStart, [ t; w; ScalarExpr.Literal(LiteralValue.String s) ], LogicalTypes.String))
                | _ -> None
            | _ -> None

        | "PadRight", [ widthExpr ] ->
            match targetS (), eval ctx widthExpr with
            | Some t, Some w -> Some(ScalarExpr.Function(ScalarFunction.PadEnd, [ t; w; ScalarExpr.Literal(LiteralValue.String " ") ], LogicalTypes.String))
            | _ -> None

        | "PadRight", [ widthExpr; padCharExpr ] ->
            match targetS (), eval ctx widthExpr with
            | Some t, Some w ->
                match PartialEvaluator.tryEvaluate padCharExpr with
                | Some(:? char as c) -> Some(ScalarExpr.Function(ScalarFunction.PadEnd, [ t; w; ScalarExpr.Literal(LiteralValue.String(string c)) ], LogicalTypes.String))
                | Some(:? string as s) -> Some(ScalarExpr.Function(ScalarFunction.PadEnd, [ t; w; ScalarExpr.Literal(LiteralValue.String s) ], LogicalTypes.String))
                | _ -> None
            | _ -> None

        // --- Normalize ---
        | "Normalize", [] ->
            targetS () |> Option.map (fun t -> ScalarExpr.Function(ScalarFunction.Normalize NormalizationForm.FormC, [ t ], LogicalTypes.String))

        | "Normalize", [ formExpr ] ->
            match targetS (), PartialEvaluator.tryEvaluate formExpr with
            | Some t, Some(:? NormalizationForm as form) ->
                Some(ScalarExpr.Function(ScalarFunction.Normalize form, [ t ], LogicalTypes.String))
            | _ -> None

        // --- Indexer: s[i] (get_Chars) ---
        | "get_Chars", [ idxExpr ] ->
            match targetS (), eval ctx idxExpr with
            | Some t, Some idxS ->
                Some(ScalarExpr.Function(ScalarFunction.Slice, [ t; idxS; ScalarExpr.Literal(LiteralValue.Int32 1) ], LogicalTypes.String))
            | _ -> None

        // --- Split ---
        | "Split", [ sepExpr ] ->
            let sepOpt =
                match PartialEvaluator.tryEvaluate sepExpr with
                | Some(:? char as c) -> Some(string c)
                | Some(:? string as s) -> Some s
                | Some(:? (char array) as ca) when ca.Length = 1 -> Some(string ca.[0])
                | Some(:? (string array) as sa) when sa.Length = 1 -> Some sa.[0]
                | _ -> None

            match targetS (), sepOpt with
            | Some t, Some s ->
                Some(ScalarExpr.Function(ScalarFunction.StrSplit, [ t; ScalarExpr.Literal(LiteralValue.String s); ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.ListOfUnknown))
            | _ -> None

        | "Split", [ sepExpr; countExpr ]
        | "Split", sepExpr :: countExpr :: _ when args.Length <= 3 ->
            let sepOpt =
                match PartialEvaluator.tryEvaluate sepExpr with
                | Some(:? char as c) -> Some(string c)
                | Some(:? string as s) -> Some s
                | Some(:? (char array) as ca) when ca.Length = 1 -> Some(string ca.[0])
                | Some(:? (string array) as sa) when sa.Length = 1 -> Some sa.[0]
                | _ -> None

            let countOpt =
                match PartialEvaluator.tryEvaluate countExpr with
                | Some(:? int as n) -> Some n
                | _ -> None

            match targetS (), sepOpt, countOpt with
            | Some t, Some s, Some n ->
                Some(ScalarExpr.Function(ScalarFunction.StrSplitN, [ t; ScalarExpr.Literal(LiteralValue.String s); ScalarExpr.Literal(LiteralValue.Int32 n) ], LogicalTypes.ListOfUnknown))
            | _ -> None

        // --- Trim / TrimStart / TrimEnd ---
        | ("Trim" | "TrimStart" | "TrimEnd") as trimOp, ([] | [ _ ]) ->
            let trimFn =
                match trimOp with
                | "TrimStart" -> ScalarFunction.StripStart
                | "TrimEnd" -> ScalarFunction.StripEnd
                | _ -> ScalarFunction.Strip

            match args with
            | [] -> targetS () |> Option.map (fun t -> ScalarExpr.Function(trimFn, [ t; ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String) ], LogicalTypes.String))
            | [ arg ] ->
                match unwrap arg with
                | :? ConstantExpression as ce when isNull ce.Value ->
                    targetS () |> Option.map (fun t -> ScalarExpr.Function(trimFn, [ t; ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String) ], LogicalTypes.String))
                | :? NewArrayExpression as nae when nae.Expressions.Count = 0 ->
                    targetS () |> Option.map (fun t -> ScalarExpr.Function(trimFn, [ t; ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String) ], LogicalTypes.String))
                | other ->
                    let charsOpt =
                        match PartialEvaluator.tryEvaluate other with
                        | Some(:? (char array) as chars) -> Some(ScalarExpr.Literal(LiteralValue.String(String(chars))))
                        | Some(:? char as c) -> Some(ScalarExpr.Literal(LiteralValue.String(string c)))
                        | Some(:? string as s) -> Some(ScalarExpr.Literal(LiteralValue.String s))
                        | _ -> eval ctx other

                    match targetS (), charsOpt with
                    | Some t, Some p -> Some(ScalarExpr.Function(trimFn, [ t; p ], LogicalTypes.String))
                    | _ -> None
            | _ -> None

        // --- Equals --- (null-safe, as string.Equals: see NullSemantics)
        | "Equals", [ aExpr; bExpr ] when isNull target ->
            match eval ctx aExpr, eval ctx bExpr with
            | Some a, Some b -> Some(NullSemantics.equality true (aExpr.Type, a) (bExpr.Type, b))
            | _ -> None

        | "Equals", [ aExpr; bExpr; compExpr ] when isNull target ->
            match eval ctx aExpr, eval ctx bExpr, tryEvaluateCaseInsensitive compExpr with
            | Some a, Some b, Some true ->
                let lower x = ScalarExpr.Function(ScalarFunction.ToLower, [ x ], LogicalTypes.String)
                Some(NullSemantics.equality true (aExpr.Type, lower a) (bExpr.Type, lower b))
            | Some a, Some b, Some false ->
                Some(NullSemantics.equality true (aExpr.Type, a) (bExpr.Type, b))
            | _ -> None

        | "Equals", [ otherExpr ] ->
            match targetS (), eval ctx otherExpr with
            | Some t, Some o -> Some(NullSemantics.equality true (target.Type, t) (otherExpr.Type, o))
            | _ -> None

        | "Equals", [ otherExpr; compExpr ] ->
            match targetS (), eval ctx otherExpr, tryEvaluateCaseInsensitive compExpr with
            | Some t, Some o, Some true ->
                let lower x = ScalarExpr.Function(ScalarFunction.ToLower, [ x ], LogicalTypes.String)
                Some(NullSemantics.equality true (target.Type, lower t) (otherExpr.Type, lower o))
            | Some t, Some o, Some false ->
                Some(NullSemantics.equality true (target.Type, t) (otherExpr.Type, o))
            | _ -> None

        // --- CompareTo ---
        | "CompareTo", [ otherExpr ] ->
            match targetS (), eval ctx otherExpr with
            | Some t, Some o ->
                let gtCond = ScalarExpr.Binary(BinaryOp.Gt, t, o)
                let ltCond = ScalarExpr.Binary(BinaryOp.Lt, t, o)
                Some(ScalarExpr.Conditional(gtCond, ScalarExpr.Literal(LiteralValue.Int32 1), ScalarExpr.Conditional(ltCond, ScalarExpr.Literal(LiteralValue.Int32 -1), ScalarExpr.Literal(LiteralValue.Int32 0))))
            | _ -> None

        // --- Remove ---
        | "Remove", [ startExpr ] ->
            match targetS (), eval ctx startExpr with
            | Some t, Some startS ->
                Some(ScalarExpr.Function(ScalarFunction.Slice, [ t; ScalarExpr.Literal(LiteralValue.Int32 0); startS ], LogicalTypes.String))
            | _ -> None

        | "Remove", [ startExpr; countExpr ] ->
            match targetS (), eval ctx startExpr, eval ctx countExpr with
            | Some t, Some startS, Some countS ->
                let prefix = ScalarExpr.Function(ScalarFunction.Slice, [ t; ScalarExpr.Literal(LiteralValue.Int32 0); startS ], LogicalTypes.String)
                let suffixOffset = ScalarExpr.Binary(BinaryOp.Add, startS, countS)
                let suffix = ScalarExpr.Function(ScalarFunction.Slice, [ t; suffixOffset; ScalarExpr.Literal(LiteralValue.Null LogicalTypes.Int64) ], LogicalTypes.String)
                Some(ScalarExpr.Function(ScalarFunction.FormatString, [ ScalarExpr.Literal(LiteralValue.String "{}{}"); prefix; suffix ], LogicalTypes.String))
            | _ -> None

        | _ -> None


    let internal translateTemporalMethod
        (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ScalarExpr option =
        let targetS () = eval ctx target

        match m.Name, args with
        | "ParseExact", strExpr :: formatExpr :: _ when isNull target && m.DeclaringType = typeof<DateTime> ->
            match eval ctx strExpr, PartialEvaluator.tryEvaluate formatExpr with
            | Some strS, Some(:? string as netFormat) ->
                Some(ScalarExpr.Function(ScalarFunction.StrToDatetime, [ strS; ScalarExpr.Literal(LiteralValue.String netFormat); ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.Datetime))
            | _ -> None

        | "Parse", [ strExpr ]
        | "Parse", strExpr :: _ when isNull target && m.DeclaringType = typeof<DateTime> ->
            eval ctx strExpr
            |> Option.map (fun strS ->
                ScalarExpr.Function(ScalarFunction.StrToDatetime, [ strS; ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String); ScalarExpr.Literal(LiteralValue.Bool false) ], LogicalTypes.Datetime))

        | "ParseExact", strExpr :: formatExpr :: _ when isNull target && m.DeclaringType = typeof<DateOnly> ->
            match eval ctx strExpr, PartialEvaluator.tryEvaluate formatExpr with
            | Some strS, Some(:? string as netFormat) ->
                Some(ScalarExpr.Function(ScalarFunction.StrToDate, [ strS; ScalarExpr.Literal(LiteralValue.String netFormat); ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.Date))
            | _ -> None

        | "Parse", [ strExpr ]
        | "Parse", strExpr :: _ when isNull target && m.DeclaringType = typeof<DateOnly> ->
            eval ctx strExpr
            |> Option.map (fun strS ->
                ScalarExpr.Function(ScalarFunction.StrToDate, [ strS; ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String); ScalarExpr.Literal(LiteralValue.Bool false) ], LogicalTypes.Date))

        | "ParseExact", strExpr :: formatExpr :: _ when isNull target && m.DeclaringType = typeof<TimeOnly> ->
            match eval ctx strExpr, PartialEvaluator.tryEvaluate formatExpr with
            | Some strS, Some(:? string as netFormat) ->
                Some(ScalarExpr.Function(ScalarFunction.StrToTime, [ strS; ScalarExpr.Literal(LiteralValue.String netFormat); ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.Time))
            | _ -> None

        | "Parse", [ strExpr ]
        | "Parse", strExpr :: _ when isNull target && m.DeclaringType = typeof<TimeOnly> ->
            eval ctx strExpr
            |> Option.map (fun strS ->
                ScalarExpr.Function(ScalarFunction.StrToTime, [ strS; ScalarExpr.Literal(LiteralValue.Null LogicalTypes.String); ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.Time))

        | "IsLeapYear", [ yearExpr ] when isNull target && m.DeclaringType = typeof<DateTime> ->
            eval ctx yearExpr
            |> Option.map (fun yearS ->
                let dateStrExpr = ScalarExpr.Function(ScalarFunction.FormatString, [ ScalarExpr.Literal(LiteralValue.String "{}-01-01"); yearS ], LogicalTypes.String)
                let dtExpr = ScalarExpr.Function(ScalarFunction.StrToDate, [ dateStrExpr; ScalarExpr.Literal(LiteralValue.String "yyyy-MM-dd"); ScalarExpr.Literal(LiteralValue.Bool true) ], LogicalTypes.Date)
                ScalarExpr.Function(ScalarFunction.DtIsLeapYear, [ dtExpr ], LogicalTypes.Boolean))

        | "ToString", [ formatExpr ] ->
            match targetS (), PartialEvaluator.tryEvaluate formatExpr with
            | Some t, Some(:? string as netFormat) ->
                if not (String.IsNullOrEmpty netFormat) then
                    Some(ScalarExpr.Function(ScalarFunction.DtToString, [ t; ScalarExpr.Literal(LiteralValue.String netFormat) ], LogicalTypes.String))
                else
                    None
            | _ -> None

        | "AddDays" | "AddHours" | "AddMinutes" | "AddSeconds" as addOp, [ offsetExpr ] ->
            let offsetOf (amount: float) =
                match addOp with
                | "AddDays" -> TimeSpan.FromDays amount
                | "AddHours" -> TimeSpan.FromHours amount
                | "AddMinutes" -> TimeSpan.FromMinutes amount
                | _ -> TimeSpan.FromSeconds amount

            match targetS (), PartialEvaluator.tryEvaluate offsetExpr with
            | Some t, Some v ->
                Some(ScalarExpr.Function(ScalarFunction.DtOffsetBy, [ t; ScalarExpr.Literal(LiteralValue.TimeSpan(offsetOf (Convert.ToDouble v))) ], LogicalTypes.Datetime))
            | _ -> None
        | _ -> None