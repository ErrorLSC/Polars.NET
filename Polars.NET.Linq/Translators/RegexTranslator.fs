namespace Polars.NET.Linq

open Polars.NET.Core
open Polars.NET.Linq.IR
open System
open System.Linq.Expressions
open System.Text.RegularExpressions

[<AutoOpen>]
module internal RegexTranslator =

    /// Represents extracted components of a Regex method invocation (Static or Instance/SG)
    type internal RegexInvocation =
        | IsMatch of target: Expression * pattern: string * instanceOpt: Regex option
        | ReplaceLiteral of target: Expression * pattern: string * replacement: string * instanceOpt: Regex option
        | ReplaceLiteralN of
            target: Expression *
            pattern: string *
            replacement: string *
            count: int *
            instanceOpt: Regex option
        | ReplaceEvaluator of
            target: Expression *
            pattern: string *
            evaluator: MatchEvaluator *
            instanceOpt: Regex option
        | MatchValue of target: Expression * pattern: string * groupIndex: int * instanceOpt: Regex option
        | MatchGroupNamed of target: Expression * pattern: string * groupName: string * instanceOpt: Regex option
        | MatchesAll of target: Expression * pattern: string * instanceOpt: Regex option
        | MatchesCount of target: Expression * pattern: string * instanceOpt: Regex option
        | Split of target: Expression * pattern: string * instanceOpt: Regex option

    /// Evaluates RegexOptions from an expression node.
    let private tryEvaluateRegexOptions (expr: Expression) : RegexOptions option =
        match tryEvaluate expr with
        | Some(:? RegexOptions as opt) -> Some opt
        | Some other ->
            try
                let intVal = Convert.ToInt32 other
                Some(LanguagePrimitives.EnumOfValue<int, RegexOptions> intVal)
            with _ ->
                None
        | None -> None

    /// Evaluates TimeSpan timeout from an expression node.
    let private tryEvaluateTimeout (expr: Expression) : TimeSpan option =
        match tryEvaluate expr with
        | Some(:? TimeSpan as ts) -> Some ts
        | _ -> None

    /// Constructs a Regex instance for static invocations containing options or timeout specifications.
    let private createRegexInstance
        (pattern: string)
        (optionsOpt: RegexOptions option)
        (timeoutOpt: TimeSpan option)
        : Regex option =
        try
            match optionsOpt, timeoutOpt with
            | Some opt, Some timeout -> Some(Regex(pattern, opt, timeout))
            | Some opt, None -> Some(Regex(pattern, opt))
            | None, Some timeout -> Some(Regex(pattern, RegexOptions.None, timeout))
            | None, None -> None
        with _ ->
            None

    /// Extracts literal pattern and optional configured Regex instance from static method arguments.
    let private tryExtractStaticPatternAndRegex (args: Expression list) : (string * Regex option) option =
        match args with
        | [ patternExpr ] ->
            match tryEvaluate patternExpr with
            | Some(:? string as pat) -> Some(pat, None)
            | _ -> None

        | [ patternExpr; optionsExpr ] ->
            match tryEvaluate patternExpr, tryEvaluateRegexOptions optionsExpr with
            | Some(:? string as pat), Some opt -> Some(pat, createRegexInstance pat (Some opt) None)
            | _ -> None

        | [ patternExpr; optionsExpr; timeoutExpr ] ->
            match tryEvaluate patternExpr, tryEvaluateRegexOptions optionsExpr, tryEvaluateTimeout timeoutExpr with
            | Some(:? string as pat), Some opt, timeoutOpt -> Some(pat, createRegexInstance pat (Some opt) timeoutOpt)
            | _ -> None

        | _ -> None

    /// Resolves group index from expression, supporting both numeric indices and group names.
    let private tryResolveGroupIndex (groupArgExpr: Expression) (pattern: string) (rxOpt: Regex option) : int option =
        match tryEvaluate groupArgExpr with
        | Some(:? int as idx) -> Some idx
        | Some(:? string as groupName) ->
            let rx =
                match rxOpt with
                | Some r -> r
                | None ->
                    try
                        Regex(pattern)
                    with _ ->
                        null

            if isNull rx then
                None
            else
                let num = rx.GroupNumberFromName groupName
                if num >= 0 then Some num else None
        | Some other ->
            try
                Some(Convert.ToInt32 other)
            with _ ->
                None
        | None -> None

    /// Unifies static Regex.Method(target, pat, options?, timeout?) and instance rx.Method(target) calls.
    let private (|RegexCall|_|) (expr: Expression) : (string * Expression * string * Regex option) option =
        match expr with
        | MethodCall(m, null, target :: rest) when m.DeclaringType = typeof<Regex> && m.Name <> "Replace" ->
            tryExtractStaticPatternAndRegex rest
            |> Option.map (fun (pat, rxOpt) -> m.Name, target, pat, rxOpt)
        | MethodCall(m, rxExpr, [ target ]) when
            not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom rxExpr.Type
            ->
            match tryEvaluate rxExpr with
            | Some(:? Regex as rx) -> Some(m.Name, target, rx.ToString(), Some rx)
            | _ -> None
        | _ -> None

    let private tryBuildReplace (target: Expression) (pat: string) (repExpr: Expression) (rxOpt: Regex option) =
        match tryEvaluate repExpr with
        | Some(:? string as rep) -> Some(ReplaceLiteral(target, pat, rep, rxOpt))
        | Some(:? MatchEvaluator as eval) -> Some(ReplaceEvaluator(target, pat, eval, rxOpt))
        | _ -> None

    let internal (|RegexInvocation|_|) (expr: Expression) : RegexInvocation option =
        match expr with
        // 1. Direct Static or Instance Regex calls: IsMatch, Match, Count, Split
        | RegexCall("IsMatch", target, pat, rxOpt) -> Some(IsMatch(target, pat, rxOpt))
        | RegexCall("Match", target, pat, rxOpt) -> Some(MatchValue(target, pat, 0, rxOpt))
        | RegexCall("Count", target, pat, rxOpt) -> Some(MatchesCount(target, pat, rxOpt))
        | RegexCall("Split", target, pat, rxOpt) -> Some(Split(target, pat, rxOpt))

        // 2. Replace (Static & Instance)
        | MethodCall(m, null, target :: patternExpr :: repExpr :: rest) when
            m.DeclaringType = typeof<Regex> && m.Name = "Replace"
            ->
            tryExtractStaticPatternAndRegex (patternExpr :: rest)
            |> Option.bind (fun (pat, rxOpt) -> tryBuildReplace target pat repExpr rxOpt)

        | MethodCall(m, rxExpr, [ target; repExpr ]) when
            not (isNull rxExpr)
            && typeof<Regex>.IsAssignableFrom rxExpr.Type
            && m.Name = "Replace"
            ->
            match tryEvaluate rxExpr with
            | Some(:? Regex as rx) -> tryBuildReplace target (rx.ToString()) repExpr (Some rx)
            | _ -> None

        | MethodCall(m, rxExpr, [ target; repExpr; countExpr ]) when
            not (isNull rxExpr)
            && typeof<Regex>.IsAssignableFrom rxExpr.Type
            && m.Name = "Replace"
            ->
            match tryEvaluate rxExpr, tryEvaluate repExpr, tryEvaluate countExpr with
            | Some(:? Regex as rx), Some(:? string as rep), Some(:? int as count) ->
                Some(ReplaceLiteralN(target, rx.ToString(), rep, count, Some rx))
            | _ -> None

        // 3. Match(...).Success / Match(...).Value / Matches(...).Count property chaining
        | MemberAccess(RegexCall(methodName, target, pat, rxOpt), memberInfo) ->
            match methodName, memberInfo.Name with
            | "Match", "Success" -> Some(IsMatch(target, pat, rxOpt))
            | "Match", "Value" -> Some(MatchValue(target, pat, 0, rxOpt))
            | "Matches", "Count" -> Some(MatchesCount(target, pat, rxOpt))
            | _ -> None

        // 4. Match(...).Groups[n].Value
        | MemberAccess(MethodCall(indexer,
                                  MemberAccess(RegexCall("Match", target, pat, rxOpt), groupsProp),
                                  [ groupIdxExpr ]),
                        valueProp) when
            groupsProp.Name = "Groups"
            && valueProp.Name = "Value"
            && indexer.Name = "get_Item"
            ->
            match tryEvaluate groupIdxExpr with
            | Some(:? string as groupName) ->
                Some(MatchGroupNamed(target, pat, groupName, rxOpt))
            | _ ->
                tryResolveGroupIndex groupIdxExpr pat rxOpt
                |> Option.map (fun idx -> MatchValue(target, pat, idx, rxOpt))

        // 5. Matches(...).Select(m => m.Value) (with or without .ToList() / .ToArray())
        | MethodCall(collMethod, null, [ MethodCall(selMethod, null, [ RegexCall("Matches", target, pat, rxOpt); _ ]) ]) when
            (collMethod.Name = "ToList" || collMethod.Name = "ToArray")
            && selMethod.Name = "Select"
            ->
            Some(MatchesAll(target, pat, rxOpt))

        | MethodCall(selMethod, null, [ RegexCall("Matches", target, pat, rxOpt); _ ]) when
            selMethod.Name = "Select"
            ->
            Some(MatchesAll(target, pat, rxOpt))

        | _ -> None

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

    let internal translateRegexInvocationScalar
        (eval: ScalarExprEvaluator)
        (ctx: TranslationContext)
        (invocation: RegexInvocation)
        : ScalarExpr option =

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
                    Some(ScalarExpr.Function("str_contains", [ t; ScalarExpr.Literal(LiteralValue.String validPat); ScalarExpr.Literal(LiteralValue.Bool false) ], PlDataType.Boolean))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        match ColumnResolver.tryResolveColumnNameWithContext ctx target with
                        | Some colName ->
                            let rx = getRegex rxOpt pat
                            let h = PolarsWrapper.MapStringPredicate(PolarsWrapper.Col colName, Func<string, bool>(rx.IsMatch))
                            Some(ScalarExpr.Opaque(h, [ colName ], "Regex.IsMatch UDF"))
                        | None -> None)

        // --- 2. ReplaceLiteral ---
        | ReplaceLiteral(target, pat, rep, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt
                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    Some(ScalarExpr.Function("str_replace_all", [ t; ScalarExpr.Literal(LiteralValue.String validPat); ScalarExpr.Literal(LiteralValue.String rep); ScalarExpr.Literal(LiteralValue.Bool false) ], PlDataType.String))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        match ColumnResolver.tryResolveColumnNameWithContext ctx target with
                        | Some colName ->
                            let rx = getRegex rxOpt pat
                            let h = PolarsWrapper.MapStringTransform(PolarsWrapper.Col colName, Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, rep)))
                            Some(ScalarExpr.Opaque(h, [ colName ], "Regex.Replace UDF"))
                        | None -> None)

        // --- 2b. ReplaceLiteralN ---
        | ReplaceLiteralN(target, pat, rep, count, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt
                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    Some(ScalarExpr.Function("str_replace", [ t; ScalarExpr.Literal(LiteralValue.String validPat); ScalarExpr.Literal(LiteralValue.String rep); ScalarExpr.Literal(LiteralValue.Int64 (int64 count)) ], PlDataType.String))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        match ColumnResolver.tryResolveColumnNameWithContext ctx target with
                        | Some colName ->
                            let rx = getRegex rxOpt pat
                            let h = PolarsWrapper.MapStringTransform(PolarsWrapper.Col colName, Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, rep, count)))
                            Some(ScalarExpr.Opaque(h, [ colName ], "Regex.ReplaceN UDF"))
                        | None -> None)

        // --- 3. ReplaceEvaluator ---
        | ReplaceEvaluator(target, pat, evaluator, rxOpt) ->
            if CoreConfig.DisallowUdfPushdown then
                None
            else
                match ColumnResolver.tryResolveColumnNameWithContext ctx target with
                | Some colName ->
                    let rx = getRegex rxOpt pat
                    let h = PolarsWrapper.MapStringTransform(PolarsWrapper.Col colName, Func<string, string>(fun s -> if isNull s then null else rx.Replace(s, evaluator)))
                    Some(ScalarExpr.Opaque(h, [ colName ], "Regex.ReplaceEvaluator UDF"))
                | None -> None

        // --- 4. MatchValue & Groups[i].Value ---
        | MatchValue(target, pat, groupIndex, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt
                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    let extracted = ScalarExpr.Function("str_extract", [ t; ScalarExpr.Literal(LiteralValue.String validPat); ScalarExpr.Literal(LiteralValue.Int32 groupIndex) ], PlDataType.String)
                    let nonNullExtracted = ScalarExpr.Coalesce [ extracted; ScalarExpr.Literal(LiteralValue.String "") ]
                    let isTargetNull = ScalarExpr.Unary(UnaryOp.IsNull, t)
                    Some(ScalarExpr.Conditional(isTargetNull, ScalarExpr.Literal(LiteralValue.Null PlDataType.String), nonNullExtracted))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        match ColumnResolver.tryResolveColumnNameWithContext ctx target with
                        | Some colName ->
                            let rx = getRegex rxOpt pat
                            let h =
                                PolarsWrapper.MapStringTransform(
                                    PolarsWrapper.Col colName,
                                    Func<string, string>(fun s ->
                                        if isNull s then null
                                        else
                                            let m = rx.Match(s)
                                            if m.Success && groupIndex < m.Groups.Count then m.Groups.[groupIndex].Value else "")
                                )
                            Some(ScalarExpr.Opaque(h, [ colName ], "Regex.MatchValue UDF"))
                        | None -> None)

        // --- 4b. MatchGroupNamed ---
        | MatchGroupNamed(target, pat, groupName, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt
                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    let structExpr = ScalarExpr.Function("str_extract_groups", [ t; ScalarExpr.Literal(LiteralValue.String validPat) ], PlDataType.Unknown)
                    let fieldExpr = ScalarExpr.Function("struct_field_by_name", [ structExpr; ScalarExpr.Literal(LiteralValue.String groupName) ], PlDataType.String)
                    let nonNullExtracted = ScalarExpr.Coalesce [ fieldExpr; ScalarExpr.Literal(LiteralValue.String "") ]
                    let isTargetNull = ScalarExpr.Unary(UnaryOp.IsNull, t)
                    Some(ScalarExpr.Conditional(isTargetNull, ScalarExpr.Literal(LiteralValue.Null PlDataType.String), nonNullExtracted))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        match ColumnResolver.tryResolveColumnNameWithContext ctx target with
                        | Some colName ->
                            let rx = getRegex rxOpt pat
                            let h =
                                PolarsWrapper.MapStringTransform(
                                    PolarsWrapper.Col colName,
                                    Func<string, string>(fun s ->
                                        if isNull s then null
                                        else
                                            let m = rx.Match(s)
                                            if m.Success && m.Groups.[groupName].Success then m.Groups.[groupName].Value else "")
                                )
                            Some(ScalarExpr.Opaque(h, [ colName ], "Regex.MatchGroupNamed UDF"))
                        | None -> None)

        // --- 5. MatchesAll ---
        | MatchesAll(target, pat, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt
                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    Some(ScalarExpr.Function("str_extract_all", [ t; ScalarExpr.Literal(LiteralValue.String validPat) ], PlDataType.List))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        match ColumnResolver.tryResolveColumnNameWithContext ctx target with
                        | Some colName ->
                            let rx = getRegex rxOpt pat
                            let h =
                                PolarsWrapper.MapStringToList(
                                    PolarsWrapper.Col colName,
                                    Func<string, seq<string>>(fun s -> if isNull s then null else rx.Matches(s) |> Seq.map (fun m -> m.Value))
                                )
                            Some(ScalarExpr.Opaque(h, [ colName ], "Regex.MatchesAll UDF"))
                        | None -> None)

        // --- 6. MatchesCount ---
        | MatchesCount(target, pat, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt
                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    Some(ScalarExpr.Function("str_count_matches", [ t; ScalarExpr.Literal(LiteralValue.String validPat); ScalarExpr.Literal(LiteralValue.Bool false) ], PlDataType.UInt32))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        match ColumnResolver.tryResolveColumnNameWithContext ctx target with
                        | Some colName ->
                            let rx = getRegex rxOpt pat
                            let intDtype = PolarsWrapper.NewPrimitiveType(int PlDataType.Int32)
                            let h =
                                PolarsWrapper.Map(
                                    PolarsWrapper.Col colName,
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
                            Some(ScalarExpr.Opaque(h, [ colName ], "Regex.MatchesCount UDF"))
                        | None -> None)

        // --- 7. Split ---
        | Split(target, pat, rxOpt) ->
            eval ctx target
            |> Option.bind (fun t ->
                let options = getOptions rxOpt
                match tryBuildRustRegexPattern pat options with
                | Some validPat ->
                    Some(ScalarExpr.Function("str_split", [ t; ScalarExpr.Literal(LiteralValue.String validPat); ScalarExpr.Literal(LiteralValue.Bool false) ], PlDataType.List))
                | None ->
                    if CoreConfig.DisallowUdfPushdown then
                        None
                    else
                        match ColumnResolver.tryResolveColumnNameWithContext ctx target with
                        | Some colName ->
                            let rx = getRegex rxOpt pat
                            let h =
                                PolarsWrapper.MapStringToList(
                                    PolarsWrapper.Col colName,
                                    Func<string, seq<string>>(fun s -> if isNull s then null else rx.Split(s) :> seq<string>)
                                )
                            Some(ScalarExpr.Opaque(h, [ colName ], "Regex.Split UDF"))
                        | None -> None)

