namespace Polars.NET.Linq

open Polars.NET.Core
open System
open System.Text
open System.Reflection
open System.Linq.Expressions
open System.Text.RegularExpressions
open Polars.NET.Core.Helpers

[<AutoOpen>]
module internal StringTemporalTranslator =

    /// Evaluates StringComparison expression to determine case insensitivity.
    let private tryEvaluateCaseInsensitive (compExpr: Expression) : bool option =
        match tryEvaluate compExpr with
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
            with _ ->
                None
        | None -> None

    let private tryExtractCharSet (expr: Expression) : char array option =
        match unwrap expr with
        | :? NewArrayExpression as nae when nae.NodeType = ExpressionType.NewArrayInit ->
            let chars =
                nae.Expressions
                |> Seq.choose (fun e ->
                    match tryEvaluate e with
                    | Some(:? char as c) -> Some c
                    | Some(:? string as s) when s.Length = 1 -> Some s.[0]
                    | _ -> None)
                |> Seq.toArray
            if chars.Length = nae.Expressions.Count then Some chars else None
        | other ->
            match tryEvaluate other with
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
        (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetH () = eval ctx target

        match m.Name, args with
        // --- 1. String.Concat (Static string.Concat / flattened arguments) ---
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
                Some(PolarsWrapper.Lit "")
            elif flatArgs.Length = 1 then
                eval ctx flatArgs.Head
            else
                let argHandles = flatArgs |> List.choose (eval ctx)

                if argHandles.Length = flatArgs.Length then
                    let rustFmt = String.replicate argHandles.Length "{}"
                    Some(PolarsWrapper.FormatString(rustFmt, List.toArray argHandles))
                else
                    None

        // --- 1b. String.Join (Static string.Join with multiple arguments) ---
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
                match tryEvaluate sepExpr with
                | Some(:? string as sep) -> Some sep
                | Some(:? char as sep) -> Some(string sep)
                | _ -> None

            match sepStrOpt with
            | Some sep ->
                let itemHandles = flatItems |> List.choose (eval ctx)
                if itemHandles.Length = flatItems.Length && not itemHandles.IsEmpty then
                    Some(PolarsWrapper.ConcatString(List.toArray itemHandles, sep, ignoreNulls = false))
                else
                    None
            | None -> None

        // --- 2. String.Format & Interpolated Strings ($"{a} - {b}") ---
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
                match tryEvaluate formatExpr with
                | Some(:? string as netFormat) ->
                    // Matches standard .NET placeholders like {0}, {1}, {0:F2}, etc.
                    let matches = Regex.Matches(netFormat, @"\{(\d+)(?::[^}]+)?\}")

                    let referencedIndices =
                        matches |> Seq.map (fun m -> Int32.Parse(m.Groups.[1].Value)) |> Seq.toList

                    if referencedIndices.IsEmpty then
                        Some(PolarsWrapper.Lit netFormat)
                    elif referencedIndices |> List.forall (fun idx -> idx >= 0 && idx < rawItems.Length) then
                        let orderedExprs = referencedIndices |> List.map (fun idx -> rawItems.[idx])
                        let argHandles = orderedExprs |> List.choose (eval ctx)

                        if argHandles.Length = orderedExprs.Length then
                            // Convert {0}, {1:F2} to Polars/Rust {} positional placeholders
                            let rustFmt = Regex.Replace(netFormat, @"\{(\d+)(?::[^}]+)?\}", "{}")
                            Some(PolarsWrapper.FormatString(rustFmt, List.toArray argHandles))
                        else
                            None
                    else
                        None
                | _ -> None
            | None -> None

        | "IsNullOrEmpty", [ arg ] when isNull target ->
            eval ctx arg
            |> Option.map (fun exprH ->
                let cloned = PolarsWrapper.CloneExpr exprH
                let isNull = PolarsWrapper.IsNull exprH
                let isEmpty = PolarsWrapper.Eq(cloned, PolarsWrapper.Lit "")
                PolarsWrapper.Or(isNull, isEmpty))

        | "IsNullOrWhiteSpace", [ arg ] when isNull target ->
            eval ctx arg
            |> Option.map (fun exprH ->
                let cloned = PolarsWrapper.CloneExpr exprH
                let isNull = PolarsWrapper.IsNull exprH
                let stripped = PolarsWrapper.StrStripChars(cloned, PolarsWrapper.LitNull())
                let isEmpty = PolarsWrapper.Eq(stripped, PolarsWrapper.Lit "")
                PolarsWrapper.Or(isNull, isEmpty))

        // --- Contains / StartsWith / EndsWith / IndexOf ---
        | ("Contains" | "StartsWith" | "EndsWith" | "IndexOf") as opName, arg :: rest when
            rest.IsEmpty || opName <> "Contains" || rest.Length = 1
            ->
            let applyOp (t: ExprHandle) (p: ExprHandle) =
                match opName with
                | "Contains" -> PolarsWrapper.StrContains(t, p, literal = true, strict = false)
                | "StartsWith" -> PolarsWrapper.StrStartsWith(t, p)
                | "EndsWith" -> PolarsWrapper.StrEndsWith(t, p)
                | _ ->
                    let findH = PolarsWrapper.StrFind(t, p, literal = true, strict = false)
                    let castedH = tryTranslateCast typeof<int> findH |> Option.defaultValue findH
                    PolarsWrapper.FillNull(castedH, PolarsWrapper.Lit -1)

            match rest with
            | [] ->
                match targetH (), eval ctx arg with
                | Some t, Some p -> Some(applyOp t p)
                | _ -> None
            | compExpr :: _ ->
                match targetH (), eval ctx arg, tryEvaluateCaseInsensitive compExpr with
                | Some t, Some p, Some true ->
                    Some(applyOp (PolarsWrapper.StrToLower t) (PolarsWrapper.StrToLower p))
                | Some t, Some p, Some false -> Some(applyOp t p)
                | _ -> None

        // --- IndexOfAny ---
        | "IndexOfAny", anyOfExpr :: rest ->
            match tryExtractCharSet anyOfExpr with
            | Some chars when chars.Length = 0 ->
                Some(PolarsWrapper.Lit -1)
            | Some chars ->
                let regexPat = buildCharClassRegex chars
                let patH () = PolarsWrapper.Lit regexPat

                match rest with
                | [] ->
                    targetH ()
                    |> Option.map (fun t ->
                        let findH = PolarsWrapper.StrFind(t, patH (), literal = false, strict = false)
                        let castedH = tryTranslateCast typeof<int> findH |> Option.defaultValue findH
                        PolarsWrapper.FillNull(castedH, PolarsWrapper.Lit -1))

                | [ startExpr ] ->
                    match targetH (), eval ctx startExpr with
                    | Some t, Some startH ->
                        let startHInt = tryTranslateCast typeof<int> startH |> Option.defaultValue startH
                        let startClone = PolarsWrapper.CloneExpr startHInt
                        let sliced = PolarsWrapper.StrSlice(t, startHInt, PolarsWrapper.LitNull())
                        let findH = PolarsWrapper.StrFind(sliced, patH (), literal = false, strict = false)
                        let castedH = tryTranslateCast typeof<int> findH |> Option.defaultValue findH
                        let fullIdx = PolarsWrapper.Add(startClone, castedH)
                        Some(PolarsWrapper.FillNull(fullIdx, PolarsWrapper.Lit -1))
                    | _ -> None

                | [ startExpr; countExpr ] ->
                    match targetH (), eval ctx startExpr, eval ctx countExpr with
                    | Some t, Some startH, Some countH ->
                        let startHInt = tryTranslateCast typeof<int> startH |> Option.defaultValue startH
                        let startClone = PolarsWrapper.CloneExpr startHInt
                        let countHInt = tryTranslateCast typeof<int> countH |> Option.defaultValue countH
                        let sliced = PolarsWrapper.StrSlice(t, startHInt, countHInt)
                        let findH = PolarsWrapper.StrFind(sliced, patH (), literal = false, strict = false)
                        let castedH = tryTranslateCast typeof<int> findH |> Option.defaultValue findH
                        let fullIdx = PolarsWrapper.Add(startClone, castedH)
                        Some(PolarsWrapper.FillNull(fullIdx, PolarsWrapper.Lit -1))
                    | _ -> None

                | _ -> None
            | None -> None

        | "ToUpper", []
        | "ToUpperInvariant", [] -> targetH () |> Option.map PolarsWrapper.StrToUpper
        | "ToLower", []
        | "ToLowerInvariant", [] -> targetH () |> Option.map PolarsWrapper.StrToLower
        | "Substring", [ startExpr ] ->
            match targetH (), eval ctx startExpr with
            | Some t, Some startH -> Some(PolarsWrapper.StrSlice(t, startH, PolarsWrapper.LitNull()))
            | _ -> None
        | "Substring", [ startExpr; lenExpr ] ->
            match targetH (), eval ctx startExpr, eval ctx lenExpr with
            | Some t, Some startH, Some lenH -> Some(PolarsWrapper.StrSlice(t, startH, lenH))
            | _ -> None

        // --- Replace ---
        | "Replace", [ oldExpr; newExpr ] ->
            match targetH (), eval ctx oldExpr, eval ctx newExpr with
            | Some t, Some o, Some n -> Some(PolarsWrapper.StrReplaceAll(t, o, n, literal = true))
            | _ -> None

        | "Replace", [ oldExpr; newExpr; compExpr ] ->
            match targetH (), eval ctx newExpr, tryEvaluateCaseInsensitive compExpr with
            | Some t, Some n, Some false ->
                match eval ctx oldExpr with
                | Some o -> Some(PolarsWrapper.StrReplaceAll(t, o, n, literal = true))
                | _ -> None
            | Some t, Some n, Some true ->
                match tryEvaluate oldExpr with
                | Some(:? string as oldStr) ->
                    let escapedPattern = sprintf "(?i)%s" (Regex.Escape oldStr)
                    Some(PolarsWrapper.StrReplaceAll(t, PolarsWrapper.Lit escapedPattern, n, literal = false))
                | _ -> None
            | _ -> None

        // --- PadLeft / PadRight ---
        | "PadLeft", [ widthExpr ] ->
            match targetH (), eval ctx widthExpr with
            | Some t, Some w -> Some(PolarsWrapper.StrPadStart(t, w, " "))
            | _ -> None

        | "PadLeft", [ widthExpr; padCharExpr ] ->
            match targetH (), eval ctx widthExpr with
            | Some t, Some w ->
                match tryEvaluate padCharExpr with
                | Some(:? char as c) -> Some(PolarsWrapper.StrPadStart(t, w, string c))
                | Some(:? string as s) -> Some(PolarsWrapper.StrPadStart(t, w, s))
                | _ -> None
            | _ -> None

        | "PadRight", [ widthExpr ] ->
            match targetH (), eval ctx widthExpr with
            | Some t, Some w -> Some(PolarsWrapper.StrPadEnd(t, w, " "))
            | _ -> None

        | "PadRight", [ widthExpr; padCharExpr ] ->
            match targetH (), eval ctx widthExpr with
            | Some t, Some w ->
                match tryEvaluate padCharExpr with
                | Some(:? char as c) -> Some(PolarsWrapper.StrPadEnd(t, w, string c))
                | Some(:? string as s) -> Some(PolarsWrapper.StrPadEnd(t, w, s))
                | _ -> None
            | _ -> None

        // --- Normalize ---
        | "Normalize", [] ->
            targetH () |> Option.map (fun t -> PolarsWrapper.StrNormalize(t, NormalizationForm.FormC))

        | "Normalize", [ formExpr ] ->
            match targetH (), tryEvaluate formExpr with
            | Some t, Some(:? NormalizationForm as form) ->
                Some(PolarsWrapper.StrNormalize(t, form))
            | _ -> None

        // --- Indexer: s[i] (get_Chars) ---
        | "get_Chars", [ idxExpr ] ->
            match targetH (), eval ctx idxExpr with
            | Some t, Some idxH ->
                Some(PolarsWrapper.StrSlice(t, idxH, PolarsWrapper.Lit 1))
            | _ -> None

        // --- Split (Literal string.Split) ---
        | "Split", [ sepExpr ] ->
            let sepOpt =
                match tryEvaluate sepExpr with
                | Some(:? char as c) -> Some(string c)
                | Some(:? string as s) -> Some s
                | Some(:? (char array) as ca) when ca.Length = 1 -> Some(string ca.[0])
                | Some(:? (string array) as sa) when sa.Length = 1 -> Some sa.[0]
                | _ -> None

            match targetH (), sepOpt with
            | Some t, Some s ->
                Some(PolarsWrapper.StrSplit(t, PolarsWrapper.Lit s, inclusive = false, literal = true, strict = false))
            | _ -> None

        | "Split", [ sepExpr; countExpr ]
        | "Split", sepExpr :: countExpr :: _ when args.Length <= 3 ->
            let sepOpt =
                match tryEvaluate sepExpr with
                | Some(:? char as c) -> Some(string c)
                | Some(:? string as s) -> Some s
                | Some(:? (char array) as ca) when ca.Length = 1 -> Some(string ca.[0])
                | Some(:? (string array) as sa) when sa.Length = 1 -> Some sa.[0]
                | _ -> None

            let countOpt =
                match tryEvaluate countExpr with
                | Some(:? int as n) -> Some n
                | _ -> None

            match targetH (), sepOpt, countOpt with
            | Some t, Some s, Some n ->
                Some(PolarsWrapper.StrSplitN(t, PolarsWrapper.Lit s, n))
            | _ -> None

        // --- Trim / TrimStart / TrimEnd ---
        | ("Trim" | "TrimStart" | "TrimEnd") as trimOp, ([] | [ _ ]) ->
            let stripFn =
                match trimOp with
                | "TrimStart" -> PolarsWrapper.StrStripCharsStart
                | "TrimEnd" -> PolarsWrapper.StrStripCharsEnd
                | _ -> PolarsWrapper.StrStripChars

            match args with
            | [] -> targetH () |> Option.map (fun t -> stripFn (t, PolarsWrapper.LitNull()))
            | [ arg ] ->
                match unwrap arg with
                | :? ConstantExpression as ce when isNull ce.Value ->
                    targetH () |> Option.map (fun t -> stripFn (t, PolarsWrapper.LitNull()))
                | :? NewArrayExpression as nae when nae.Expressions.Count = 0 ->
                    targetH () |> Option.map (fun t -> stripFn (t, PolarsWrapper.LitNull()))
                | other ->
                    let charsOpt =
                        match tryEvaluate other with
                        | Some(:? (char array) as chars) -> Some(PolarsWrapper.Lit(String(chars)))
                        | Some(:? char as c) -> Some(PolarsWrapper.Lit(string c))
                        | Some(:? string as s) -> Some(PolarsWrapper.Lit s)
                        | _ -> eval ctx other

                    match targetH (), charsOpt with
                    | Some t, Some p -> Some(stripFn (t, p))
                    | _ -> None
            | _ -> None

        // --- Equals (Static & Instance) ---
        | "Equals", [ aExpr; bExpr ] when isNull target ->
            match eval ctx aExpr, eval ctx bExpr with
            | Some a, Some b -> Some(PolarsWrapper.Eq(a, b))
            | _ -> None

        | "Equals", [ aExpr; bExpr; compExpr ] when isNull target ->
            match eval ctx aExpr, eval ctx bExpr, tryEvaluateCaseInsensitive compExpr with
            | Some a, Some b, Some true ->
                Some(PolarsWrapper.Eq(PolarsWrapper.StrToLower a, PolarsWrapper.StrToLower b))
            | Some a, Some b, Some false ->
                Some(PolarsWrapper.Eq(a, b))
            | _ -> None

        | "Equals", [ otherExpr ] ->
            match targetH (), eval ctx otherExpr with
            | Some t, Some o -> Some(PolarsWrapper.Eq(t, o))
            | _ -> None

        | "Equals", [ otherExpr; compExpr ] ->
            match targetH (), eval ctx otherExpr, tryEvaluateCaseInsensitive compExpr with
            | Some t, Some o, Some true ->
                Some(PolarsWrapper.Eq(PolarsWrapper.StrToLower t, PolarsWrapper.StrToLower o))
            | Some t, Some o, Some false ->
                Some(PolarsWrapper.Eq(t, o))
            | _ -> None

        // --- CompareTo ---
        | "CompareTo", [ otherExpr ] ->
            match targetH (), eval ctx otherExpr with
            | Some t, Some o ->
                let tClone1 = PolarsWrapper.CloneExpr t
                let oClone1 = PolarsWrapper.CloneExpr o
                let gtCond = PolarsWrapper.Gt(t, o)
                let ltCond = PolarsWrapper.Lt(tClone1, oClone1)
                Some(PolarsWrapper.IfElse(gtCond, PolarsWrapper.Lit 1, PolarsWrapper.IfElse(ltCond, PolarsWrapper.Lit -1, PolarsWrapper.Lit 0)))
            | _ -> None

        // --- Remove ---
        | "Remove", [ startExpr ] ->
            match targetH (), eval ctx startExpr with
            | Some t, Some startH ->
                Some(PolarsWrapper.StrSlice(t, PolarsWrapper.Lit 0, startH))
            | _ -> None

        | "Remove", [ startExpr; countExpr ] ->
            match targetH (), eval ctx startExpr, eval ctx countExpr with
            | Some t, Some startH, Some countH ->
                let tClone = PolarsWrapper.CloneExpr t
                let startClone = PolarsWrapper.CloneExpr startH
                let prefix = PolarsWrapper.StrSlice(t, PolarsWrapper.Lit 0, startH)
                let suffixOffset = PolarsWrapper.Add(startClone, countH)
                let suffix = PolarsWrapper.StrSlice(tClone, suffixOffset, PolarsWrapper.LitNull())
                Some(PolarsWrapper.FormatString("{}{}", [| prefix; suffix |]))
            | _ -> None

        | _ -> None


    let internal translateTemporalMethod
        (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (m: MethodInfo)
        (target: Expression)
        (args: Expression list)
        : ExprHandle option =
        let targetH () = eval ctx target

        match m.Name, args with
        | "ParseExact", strExpr :: formatExpr :: _ when isNull target && m.DeclaringType = typeof<DateTime> ->
            match eval ctx strExpr, tryEvaluate formatExpr with
            | Some strH, Some(:? string as netFormat) ->
                let chronoFormat = DateTimeFormatHelper.ToChronoFormat netFormat
                let ambiguousExpr = PolarsWrapper.Lit "raise"

                Some(
                    PolarsWrapper.StrToDatetime(
                        e = strH,
                        unit = PlTimeUnit.Microseconds,
                        timeZone = null,
                        format = chronoFormat,
                        strict = false,
                        exact = true,
                        cache = true,
                        ambiguous = ambiguousExpr
                    )
                )
            | _ -> None

        | "Parse", [ strExpr ]
        | "Parse", strExpr :: _ when isNull target && m.DeclaringType = typeof<DateTime> ->
            eval ctx strExpr
            |> Option.map (fun strH ->
                let ambiguousExpr = PolarsWrapper.Lit "raise"

                PolarsWrapper.StrToDatetime(
                    e = strH,
                    unit = PlTimeUnit.Microseconds,
                    timeZone = null,
                    format = null,
                    strict = false,
                    exact = false,
                    cache = true,
                    ambiguous = ambiguousExpr
                ))

        | "ParseExact", strExpr :: formatExpr :: _ when isNull target && m.DeclaringType = typeof<DateOnly> ->
            match eval ctx strExpr, tryEvaluate formatExpr with
            | Some strH, Some(:? string as netFormat) ->
                let chronoFormat = DateTimeFormatHelper.ToChronoFormat netFormat
                Some(PolarsWrapper.StrToDate(strH, chronoFormat, strict = false, exact = true, cache = true))
            | _ -> None

        | "Parse", [ strExpr ]
        | "Parse", strExpr :: _ when isNull target && m.DeclaringType = typeof<DateOnly> ->
            eval ctx strExpr
            |> Option.map (fun strH ->
                PolarsWrapper.StrToDate(strH, format = null, strict = false, exact = false, cache = true))

        | "ParseExact", strExpr :: formatExpr :: _ when isNull target && m.DeclaringType = typeof<TimeOnly> ->
            match eval ctx strExpr, tryEvaluate formatExpr with
            | Some strH, Some(:? string as netFormat) ->
                let chronoFormat = DateTimeFormatHelper.ToChronoFormat netFormat
                Some(PolarsWrapper.StrToTime(strH, chronoFormat, strict = false, exact = true, cache = true))
            | _ -> None

        | "Parse", [ strExpr ]
        | "Parse", strExpr :: _ when isNull target && m.DeclaringType = typeof<TimeOnly> ->
            eval ctx strExpr
            |> Option.map (fun strH ->
                PolarsWrapper.StrToTime(strH, format = null, strict = false, exact = true, cache = true))

        | "IsLeapYear", [ yearExpr ] when isNull target && m.DeclaringType = typeof<DateTime> ->
            eval ctx yearExpr
            |> Option.map (fun yearH ->
                let dateStrExpr = PolarsWrapper.FormatString("{}-01-01", [| yearH |])

                let dtExpr =
                    PolarsWrapper.StrToDate(dateStrExpr, "%Y-%m-%d", strict = false, exact = true, cache = true)

                PolarsWrapper.DtIsLeapYear dtExpr)

        | "ToString", [ formatExpr ] ->
            match targetH (), tryEvaluate formatExpr with
            | Some t, Some(:? string as netFormat) ->
                let chronoFormat = DateTimeFormatHelper.ToChronoFormat netFormat

                if not (String.IsNullOrEmpty chronoFormat) then
                    Some(PolarsWrapper.DtToString(t, chronoFormat))
                else
                    None
            | _ -> None

        | "AddDays" | "AddHours" | "AddMinutes" | "AddSeconds" as addOp, [ offsetExpr ] ->
            let unitSuffix =
                match addOp with
                | "AddDays" -> "d"
                | "AddHours" -> "h"
                | "AddMinutes" -> "m"
                | _ -> "s"

            match targetH (), tryEvaluate offsetExpr with
            | Some t, Some v ->
                Some(PolarsWrapper.DtOffsetBy(t, PolarsWrapper.Lit(sprintf "%g%s" (Convert.ToDouble v) unitSuffix)))
            | _ -> None
        | _ -> None
