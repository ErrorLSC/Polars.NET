namespace Polars.NET.Linq

open Polars.NET.Core
open System
open System.Text.RegularExpressions

[<AutoOpen>]
module internal RegexTranslator =

    /// Synthesizes Rust-compatible inline flag expressions from .NET RegexOptions.
    /// Returns Some(validPattern) if compatible with Rust regex engine, otherwise None.
    let private tryBuildRustRegexPattern (pat: string) (options: RegexOptions) : string option =
        // Options that alter regex execution semantics unsupported natively by Rust regex
        let unsupported =
            RegexOptions.RightToLeft
            ||| RegexOptions.ECMAScript
            ||| RegexOptions.ExplicitCapture

        if options.HasFlag unsupported then
            None
        else
            let mutable flags = ""

            if options.HasFlag RegexOptions.IgnoreCase then
                flags <- flags + "i"

            if options.HasFlag RegexOptions.Multiline then
                flags <- flags + "m"

            if options.HasFlag RegexOptions.Singleline then
                flags <- flags + "s"

            if options.HasFlag RegexOptions.IgnorePatternWhitespace then
                flags <- flags + "x"

            let candidate =
                if String.IsNullOrEmpty flags then
                    pat
                else
                    sprintf "(?%s:%s)" flags pat

            // Validate that Rust's regex compiler can parse the candidate (e.g. rejects lookaround)
            if PolarsWrapper.RegexIsValid candidate then
                Some candidate
            else
                None

    let internal translateRegexInvocation
        (eval: ExprEvaluator)
        (ctx: TranslationContext)
        (invocation: RegexInvocation)
        : ExprHandle option =

        let getRegex rxOpt pat =
            match rxOpt with
            | Some(rx: Regex) -> rx
            | None -> Regex pat

        let getOptions rxOpt =
            match rxOpt with
            | Some(rx: Regex) -> rx.Options
            | None -> RegexOptions.None

        match invocation with
        // --- 1. IsMatch ---
        | IsMatch(target, pat, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    Some(PolarsWrapper.StrContains(t, PolarsWrapper.Lit validPat, literal = false, strict = false))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        let rx = getRegex rxOpt pat
                        Some(PolarsWrapper.MapStringPredicate(t, Func<string, bool>(rx.IsMatch))))

        // --- 2. ReplaceLiteral ---
        | ReplaceLiteral(target, pat, rep, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    Some(PolarsWrapper.StrReplaceAll(t, PolarsWrapper.Lit validPat, PolarsWrapper.Lit rep, literal = false))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        let rx = getRegex rxOpt pat
                        Some(
                            PolarsWrapper.MapStringTransform(
                                t,
                                Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, rep))
                            )
                        ))

        // --- 2b. ReplaceLiteralN (With count limit) ---
        | ReplaceLiteralN(target, pat, rep, count, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    Some(
                        PolarsWrapper.StrReplace(
                            t,
                            PolarsWrapper.Lit validPat,
                            PolarsWrapper.Lit rep,
                            literal = false,
                            n = int64 count
                        )
                    )
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        let rx = getRegex rxOpt pat
                        Some(
                            PolarsWrapper.MapStringTransform(
                                t,
                                Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, rep, count))
                            )
                        ))

        // --- 3. ReplaceEvaluator (Managed delegate: always UDF) ---
        | ReplaceEvaluator(target, pat, evaluator, rxOpt) ->
            if CoreConfig.DisallowUdfPushdown then
                None
            else
                eval ctx target
                |> Option.map (fun t ->
                    let rx = getRegex rxOpt pat

                    PolarsWrapper.MapStringTransform(
                        t,
                        Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, evaluator))
                    ))

        // --- 4. MatchValue & Groups[i].Value ---
        | MatchValue(target, pat, groupIndex, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt
                let rx = getRegex rxOpt pat

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    // Rust Native StrExtract returns Null when not matched.
                    // To conform to BCL Match.Value / Group.Value semantics, coalesce Null to empty string (""),
                    // while preserving genuine Null input if target was originally Null.
                    let tCloned = PolarsWrapper.CloneExpr t
                    let extracted = PolarsWrapper.StrExtract(t, PolarsWrapper.Lit validPat, groupIndex)
                    let nonNullExtracted = PolarsWrapper.Coalesce [| extracted; PolarsWrapper.Lit "" |]
                    let isTargetNull = PolarsWrapper.IsNull tCloned
                    Some(PolarsWrapper.IfElse(isTargetNull, PolarsWrapper.LitNull(), nonNullExtracted))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        // Managed Arrow UDF fallback
                        Some(
                            PolarsWrapper.MapStringTransform(
                                t,
                                Func<string, string>(fun s ->
                                    if isNull s then
                                        null
                                    else
                                        let m = rx.Match(s)

                                        if m.Success && groupIndex < m.Groups.Count then
                                            m.Groups.[groupIndex].Value
                                        else
                                            "")
                            )
                        ))

        // --- 4b. MatchGroupNamed (Regex.Match().Groups["name"].Value -> StrExtractGroups) ---
        | MatchGroupNamed(target, pat, groupName, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt
                let rx = getRegex rxOpt pat

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    let tCloned = PolarsWrapper.CloneExpr t
                    let structH = PolarsWrapper.StrExtractGroups(t, validPat)
                    let fieldH = PolarsWrapper.StructFieldByName(structH, [| groupName |])
                    let nonNullExtracted = PolarsWrapper.Coalesce [| fieldH; PolarsWrapper.Lit "" |]
                    let isTargetNull = PolarsWrapper.IsNull tCloned
                    Some(PolarsWrapper.IfElse(isTargetNull, PolarsWrapper.LitNull(), nonNullExtracted))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        Some(
                            PolarsWrapper.MapStringTransform(
                                t,
                                Func<string, string>(fun s ->
                                    if isNull s then null
                                    else
                                        let m = rx.Match s
                                        if m.Success then m.Groups.[groupName].Value else "")
                            )
                        ))

        // --- 5. MatchesAll ---
        | MatchesAll(target, pat, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt

                match tryBuildRustRegexPattern pat options with
                | Some validPat -> Some(PolarsWrapper.StrExtractAll(t, PolarsWrapper.Lit validPat))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        let rx = getRegex rxOpt pat
                        Some(
                            PolarsWrapper.MapStringToList(
                                t,
                                Func<string, Collections.Generic.IEnumerable<string>>(fun s ->
                                    if isNull s then
                                        null
                                    else
                                        let matches = rx.Matches(s)
                                        let list = Collections.Generic.List<string>(matches.Count)

                                        for m in matches do
                                            list.Add(m.Value)

                                        list :> Collections.Generic.IEnumerable<string>)
                            )
                        ))

        // --- 6. MatchesCount / Regex.Count (.NET 7+) ---
        | MatchesCount(target, pat, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    Some(PolarsWrapper.StrCountMatches(t, PolarsWrapper.Lit validPat, literal = false))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        let rx = getRegex rxOpt pat
                        let intDtype = PolarsWrapper.NewPrimitiveType(int PlDataType.Int32)

                        Some(
                            PolarsWrapper.Map(
                                t,
                                Func<Apache.Arrow.IArrowArray, Apache.Arrow.IArrowArray>(fun arr ->
                                    let strViewArr = arr :?> Apache.Arrow.StringViewArray
                                    let builder = Apache.Arrow.Int32Array.Builder()

                                    for i in 0 .. strViewArr.Length - 1 do
                                        if strViewArr.IsNull(i) then
                                            builder.AppendNull() |> ignore
                                        else
                                            builder.Append(rx.Matches(strViewArr.GetString(i)).Count) |> ignore

                                    builder.Build() :> Apache.Arrow.IArrowArray),
                                intDtype
                            )
                        ))

        // --- 7. Split ---
        | Split(target, pat, rxOpt) ->
            eval ctx target
            |> Option.map (fun t ->
                let options = getOptions rxOpt

                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    PolarsWrapper.StrSplit(
                        t,
                        PolarsWrapper.Lit validPat,
                        inclusive = false,
                        literal = false,
                        strict = false
                    )
                | None ->
                    let rx = getRegex rxOpt pat

                    PolarsWrapper.MapStringToList(
                        t,
                        Func<string, seq<string>>(fun s -> if isNull s then null else rx.Split(s) :> seq<string>)
                    ))
