namespace Polars.NET.Linq

open System.Linq.Expressions
open System.Reflection
open System.Text.RegularExpressions
open System.Collections
open System

[<AutoOpen>]
module internal ExpressionPatterns =
    open System.Linq
    open Polars.NET.Core.Helpers

    /// Represents extracted components of a Regex method invocation (Static or Instance/SG)
    type internal RegexInvocation =
        | IsMatch of target: Expression * pattern: string * instanceOpt: Regex option
        | ReplaceLiteral of target: Expression * pattern: string * replacement: string * instanceOpt: Regex option
        | ReplaceEvaluator of target: Expression * pattern: string * evaluator: MatchEvaluator * instanceOpt: Regex option
        | MatchValue of target: Expression * pattern: string * groupIndex: int * instanceOpt: Regex option
        | MatchesAll of target: Expression * pattern: string * instanceOpt: Regex option
        | MatchesCount of target: Expression * pattern: string * instanceOpt: Regex option
        | Split of target: Expression * pattern: string * instanceOpt: Regex option

    /// Represents extracted components of list/array member access or method invocations
    type internal ListInvocation =
        // Length / Count: x.Tags.Length, x.Tags.Count, x.Tags.Count()
        | ListLen of target: Expression
        // Element containment: x.Tags.Contains("vip") or Enumerable.Contains(x.Tags, "vip")
        | ListContains of target: Expression * itemExpr: Expression
        // Index item access: x.Tags[0], x.Tags.ElementAt(n)
        | ListGet of target: Expression * indexExpr: Expression
        // Head / First: x.Tags.First()
        | ListFirst of target: Expression
        // Tail / Last: x.Tags.Last()
        | ListLast of target: Expression

        // Stage 2: Aggregations & Joins
        | ListSum of target: Expression
        | ListMean of target: Expression
        | ListMin of target: Expression
        | ListMax of target: Expression
        | ListJoin of target: Expression * separatorExpr: Expression

        // Stage 3: Slicing & Transformations
        | ListHead of target: Expression * countExpr: Expression
        | ListTail of target: Expression * countExpr: Expression
        | ListSlice of target: Expression * offsetExpr: Expression * lengthExpr: Expression option
        | ListUnique of target: Expression
        | ListReverse of target: Expression

        // Stage 4: In-Row Sort & Set Operations
        | ListSort of target: Expression * descending: bool
        | ListSetIntersection of target: Expression * otherExpr: Expression
        | ListSetUnion of target: Expression * otherExpr: Expression
        | ListSetDifference of target: Expression * otherExpr: Expression

        // Stage 5: In-Row Predicates (Any / All)
        | ListAny of target: Expression * predicateLambda: LambdaExpression option
        | ListAll of target: Expression * predicateLambda: LambdaExpression

        | ListSelect of target: Expression * mapperLambda: LambdaExpression

    /// Active pattern for MethodCallExpression
    let (|MethodCall|_|) (expr: Expression) =
        match expr with
        | :? MethodCallExpression as m -> Some(m.Method, m.Object, m.Arguments |> Seq.toList)
        | _ -> None

    /// Active pattern for LambdaExpression
    let (|Lambda|_|) (expr: Expression) =
        match expr with
        | :? LambdaExpression as l -> Some(l.Parameters |> Seq.toList, l.Body)
        | _ -> None

    /// Active pattern for UnaryExpression
    let (|Unary|_|) (expr: Expression) =
        match expr with
        | :? UnaryExpression as u -> Some(u.NodeType, u.Operand)
        | _ -> None

    /// Active pattern for BinaryExpression
    let (|Binary|_|) (expr: Expression) =
        match expr with
        | :? BinaryExpression as b -> Some(b.NodeType, b.Left, b.Right)
        | _ -> None

    /// Active pattern for MemberExpression
    let (|MemberAccess|_|) (expr: Expression) =
        match expr with
        | :? MemberExpression as m -> Some(m.Expression, m.Member)
        | _ -> None

    /// Active pattern for ConstantExpression
    let (|Constant|_|) (expr: Expression) =
        match expr with
        | :? ConstantExpression as c -> Some(c.Value, c.Type)
        | _ -> None

    /// Total Active pattern to strip quotation wrappers added by LINQ provider expression tree building
    let rec (|StripQuotes|) (expr: Expression) =
        match expr with
        | Unary(ExpressionType.Quote, operand) -> (|StripQuotes|) operand
        | _ -> expr
        
    /// Active pattern to recursively unwrap Quotations, Converts, and other Unary wrappers 
    /// to cleanly extract an underlying LambdaExpression
    let rec (|CleanLambda|_|) (expr: Expression) : LambdaExpression option =
        match expr with
        | null -> None
        | :? LambdaExpression as l -> Some l
        | Unary(ExpressionType.Quote, inner)
        | Unary(ExpressionType.Convert, inner)
        | Unary(ExpressionType.ConvertChecked, inner) -> (|CleanLambda|_|) inner
        | _ -> None

    /// Checks if a member belongs to F#'s internal AnonymousObject tuple wrapper
    let internal isFSharpAnonymousMember (m: MemberInfo) =
        if isNull m || isNull m.DeclaringType then false
        else
            let dt = m.DeclaringType
            let name = dt.FullName
            if isNull name then false
            else
                name.StartsWith "Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject" ||
                dt.IsGenericType && dt.Name.StartsWith "AnonymousObject`"

    /// Helper: Checks if a property access belongs to a synthetic Zip/Join tuple wrapper or transparent scope
    let internal isTransparentScopeMember (m: System.Reflection.MemberInfo) =
        if isNull m then false
        else
            let name = m.Name
            // Standard ValueTuple / Tuple properties: Item1, Item2, Item3...
            name.StartsWith "Item" ||
            // Zip / Join custom wrappers: First, Second, Third
            name = "First" || name = "Second" || name = "Third" ||
            // F# Anonymous / Transparent identifiers
            isFSharpAnonymousMember m

    let (|StructFieldAccess|_|) (expr: Expression) : (Expression * string) option =
        match expr with
        | MemberAccess(target, m) 
            when not (isNull target) && 
                 target.NodeType = ExpressionType.MemberAccess &&
                 PolarsTypeHelper.IsStructType target.Type ->
            
            // Check the target member itself
            match target with
            | :? MemberExpression as parentMe when isTransparentScopeMember parentMe.Member ->
                // This is a Zip / Join tuple projection (e.g. t.Item1, t.First, t.Second) -> NOT a Struct column!
                None
            | _ ->
                Some (target, m.Name)

        | _ -> None

    /// Active pattern to strip convert/quote wrappers and resolve root column names,
    /// seamlessly unwrapping F# AnonymousObject tuple chains
    let rec (|ExtractColumnName|_|) (expr: Expression) : string option =
        match expr with
        | null -> None
        | Unary(ExpressionType.Convert, inner)
        | Unary(ExpressionType.ConvertChecked, inner)
        | Unary(ExpressionType.Quote, inner) ->
            (|ExtractColumnName|_|) inner
        // Direct member: e.Name
        | MemberAccess(p, m) when not (isNull p) && p.NodeType = ExpressionType.Parameter ->
            Some m.Name
        // F# AnonymousObject nesting: tuple.Item1.Name
        | MemberAccess(MemberAccess(p, innerM), m) when not (isNull p) && isFSharpAnonymousMember innerM ->
            Some m.Name
        | _ -> None

    /// Evaluates expressions that resolve to values (e.g., closures, captured variables, local props, method calls)
    let rec tryEvaluate (expr: Expression) : obj option =
        match expr with
        | null -> None
        | Unary(ExpressionType.Convert, inner)
        | Unary(ExpressionType.ConvertChecked, inner)
        | Unary(ExpressionType.Quote, inner) ->
            tryEvaluate inner
        | Constant(value, _) ->
            Some value
        | MemberAccess(instanceExpr, memberInfo) ->
            let targetObj =
                match instanceExpr with
                | null -> None
                | exp -> tryEvaluate exp

            match memberInfo with
            | :? PropertyInfo as prop ->
                let inst = match targetObj with Some o -> o | None -> null
                Some (prop.GetValue(inst))
            | :? FieldInfo as field ->
                let inst = match targetObj with Some o -> o | None -> null
                Some (field.GetValue(inst))
            | _ -> None
        | :? MethodCallExpression as mc when mc.Arguments.Count = 0 ->
            // Supports evaluating parameterless factory methods such as [GeneratedRegex] partial methods
            try
                let targetObj =
                    if isNull mc.Object then null
                    else
                        match tryEvaluate mc.Object with
                        | Some o -> o
                        | None -> null
                Some (mc.Method.Invoke(targetObj, null))
            with _ -> None
        | _ -> None

    // =========================================================================
    // Regex Helpers
    // =========================================================================

    /// Evaluates RegexOptions from an expression node.
    let private tryEvaluateRegexOptions (expr: Expression) : RegexOptions option =
        match tryEvaluate expr with
        | Some (:? RegexOptions as opt) -> Some opt
        | Some other ->
            try
                let intVal = Convert.ToInt32 other
                Some (LanguagePrimitives.EnumOfValue<int, RegexOptions> intVal)
            with _ -> None
        | None -> None

    /// Evaluates TimeSpan timeout from an expression node.
    let private tryEvaluateTimeout (expr: Expression) : TimeSpan option =
        match tryEvaluate expr with
        | Some (:? TimeSpan as ts) -> Some ts
        | _ -> None

    /// Constructs a Regex instance for static invocations containing options or timeout specifications.
    let private createRegexInstance (pattern: string) (optionsOpt: RegexOptions option) (timeoutOpt: TimeSpan option) : Regex option =
        try
            match optionsOpt, timeoutOpt with
            | Some opt, Some timeout -> Some (Regex(pattern, opt, timeout))
            | Some opt, None         -> Some (Regex(pattern, opt))
            | None, Some timeout    -> Some (Regex(pattern, RegexOptions.None, timeout))
            | None, None            -> None
        with _ -> None

    /// Extracts literal pattern and optional configured Regex instance from static method arguments.
    let private tryExtractStaticPatternAndRegex (args: Expression list) : (string * Regex option) option =
        match args with
        | [ patternExpr ] ->
            match tryEvaluate patternExpr with
            | Some (:? string as pat) -> Some (pat, None)
            | _ -> None

        | [ patternExpr; optionsExpr ] ->
            match tryEvaluate patternExpr, tryEvaluateRegexOptions optionsExpr with
            | Some (:? string as pat), Some opt ->
                Some (pat, createRegexInstance pat (Some opt) None)
            | _ -> None

        | [ patternExpr; optionsExpr; timeoutExpr ] ->
            match tryEvaluate patternExpr, tryEvaluateRegexOptions optionsExpr, tryEvaluateTimeout timeoutExpr with
            | Some (:? string as pat), Some opt, timeoutOpt ->
                Some (pat, createRegexInstance pat (Some opt) timeoutOpt)
            | _ -> None

        | _ -> None

    /// Extracts literal pattern and configured Regex instance for static Replace invocations.
    let private tryExtractStaticReplacePatternAndRegex (patternExpr: Expression) (trailingArgs: Expression list) : (string * Regex option) option =
        match trailingArgs with
        | [] ->
            match tryEvaluate patternExpr with
            | Some (:? string as pat) -> Some (pat, None)
            | _ -> None
        | [ optionsExpr ] ->
            match tryEvaluate patternExpr, tryEvaluateRegexOptions optionsExpr with
            | Some (:? string as pat), Some opt ->
                Some (pat, createRegexInstance pat (Some opt) None)
            | _ -> None
        | [ optionsExpr; timeoutExpr ] ->
            match tryEvaluate patternExpr, tryEvaluateRegexOptions optionsExpr, tryEvaluateTimeout timeoutExpr with
            | Some (:? string as pat), Some opt, timeoutOpt ->
                Some (pat, createRegexInstance pat (Some opt) timeoutOpt)
            | _ -> None
        | _ -> None

    /// Resolves group index from expression, supporting both numeric indices and group names.
    let private tryResolveGroupIndex (groupArgExpr: Expression) (pattern: string) (rxOpt: Regex option) : int option =
        match tryEvaluate groupArgExpr with
        | Some (:? int as idx) -> Some idx
        | Some (:? string as groupName) ->
            let rx =
                match rxOpt with
                | Some r -> r
                | None ->
                    try Regex(pattern)
                    with _ -> null
            if isNull rx then None
            else
                let num = rx.GroupNumberFromName(groupName)
                if num >= 0 then Some num else None
        | Some other ->
            try Some (Convert.ToInt32 other)
            with _ -> None
        | None -> None

    // =========================================================================
    // Regex Invocation Active Pattern
    // =========================================================================

    let (|RegexInvocation|_|) (expr: Expression) : RegexInvocation option =
        match expr with
        // ---------------------------------------------------------------------
        // 1. Static Regex calls: Regex.IsMatch, Regex.Replace, Regex.Match
        // ---------------------------------------------------------------------
        | MethodCall(m, null, target :: rest) 
            when m.DeclaringType = typeof<Regex> && m.Name = "IsMatch" ->
            match tryExtractStaticPatternAndRegex rest with
            | Some (pat, rxOpt) -> Some (IsMatch(target, pat, rxOpt))
            | _ -> None

        | MethodCall(m, null, target :: patternExpr :: repExpr :: rest) 
            when m.DeclaringType = typeof<Regex> && m.Name = "Replace" ->
            match tryExtractStaticReplacePatternAndRegex patternExpr rest with
            | Some (pat, rxOpt) ->
                match tryEvaluate repExpr with
                | Some (:? string as rep) -> Some (ReplaceLiteral(target, pat, rep, rxOpt))
                | Some (:? MatchEvaluator as eval) -> Some (ReplaceEvaluator(target, pat, eval, rxOpt))
                | _ -> None
            | _ -> None

        | MethodCall(m, null, target :: rest) 
            when m.DeclaringType = typeof<Regex> && m.Name = "Match" ->
            match tryExtractStaticPatternAndRegex rest with
            | Some (pat, rxOpt) -> Some (MatchValue(target, pat, 0, rxOpt))
            | _ -> None

        // ---------------------------------------------------------------------
        // 2. Instance / Source-Generated Regex calls: rx.IsMatch, rx.Replace, rx.Match
        // ---------------------------------------------------------------------
        | MethodCall(m, rxExpr, [ target ]) 
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) ->
                let pat = rx.ToString()
                match m.Name with
                | "IsMatch" -> Some (IsMatch(target, pat, Some rx))
                | "Match"   -> Some (MatchValue(target, pat, 0, Some rx))
                | _ -> None
            | _ -> None

        | MethodCall(m, rxExpr, [ target; repExpr ]) 
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Replace" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) ->
                match tryEvaluate repExpr with
                | Some (:? string as rep) -> Some (ReplaceLiteral(target, rx.ToString(), rep, Some rx))
                | Some (:? MatchEvaluator as eval) -> Some (ReplaceEvaluator(target, rx.ToString(), eval, Some rx))
                | _ -> None
            | _ -> None

        // ---------------------------------------------------------------------
        // 3. Match.Success property chaining: Match(s, ...).Success / rx.Match(s).Success
        // ---------------------------------------------------------------------
        | MemberAccess(MethodCall(m, null, target :: rest), memberInfo)
            when m.DeclaringType = typeof<Regex> && m.Name = "Match" && memberInfo.Name = "Success" ->
            match tryExtractStaticPatternAndRegex rest with
            | Some (pat, rxOpt) -> Some (IsMatch(target, pat, rxOpt))
            | _ -> None

        | MemberAccess(MethodCall(m, rxExpr, [ target ]), memberInfo)
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Match" && memberInfo.Name = "Success" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) -> Some (IsMatch(target, rx.ToString(), Some rx))
            | _ -> None

        // ---------------------------------------------------------------------
        // 4. Match(s, ...).Value / rx.Match(s).Value property chaining
        // ---------------------------------------------------------------------
        | MemberAccess(MethodCall(m, null, target :: rest), memberInfo)
            when m.DeclaringType = typeof<Regex> && m.Name = "Match" && memberInfo.Name = "Value" ->
            match tryExtractStaticPatternAndRegex rest with
            | Some (pat, rxOpt) -> Some (MatchValue(target, pat, 0, rxOpt))
            | _ -> None

        | MemberAccess(MethodCall(m, rxExpr, [ target ]), memberInfo)
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Match" && memberInfo.Name = "Value" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) -> Some (MatchValue(target, rx.ToString(), 0, Some rx))
            | _ -> None

        // ---------------------------------------------------------------------
        // 5. Match(s, ...).Groups[n].Value / rx.Match(s).Groups[n].Value
        // ---------------------------------------------------------------------
        | MemberAccess(MethodCall(indexer, MemberAccess(MethodCall(m, null, target :: rest), groupsProp), [ groupIdxExpr ]), valueProp)
            when m.DeclaringType = typeof<Regex> && m.Name = "Match" && 
                 groupsProp.Name = "Groups" && valueProp.Name = "Value" && indexer.Name = "get_Item" ->
            match tryExtractStaticPatternAndRegex rest with
            | Some (pat, rxOpt) ->
                match tryResolveGroupIndex groupIdxExpr pat rxOpt with
                | Some idx -> Some (MatchValue(target, pat, idx, rxOpt))
                | _ -> None
            | _ -> None

        | MemberAccess(MethodCall(indexer, MemberAccess(MethodCall(m, rxExpr, [ target ]), groupsProp), [ groupIdxExpr ]), valueProp)
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Match" && 
                 groupsProp.Name = "Groups" && valueProp.Name = "Value" && indexer.Name = "get_Item" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) ->
                let pat = rx.ToString()
                match tryResolveGroupIndex groupIdxExpr pat (Some rx) with
                | Some idx -> Some (MatchValue(target, pat, idx, Some rx))
                | _ -> None
            | _ -> None

        // ---------------------------------------------------------------------
        // 6. Matches Count: Regex.Count(s, ...) / rx.Count(s) / Matches(...).Count
        // ---------------------------------------------------------------------
        // 6.1 Static: Regex.Count(input, pattern, options?) (.NET 7+)
        | MethodCall(m, null, target :: rest)
            when m.DeclaringType = typeof<Regex> && m.Name = "Count" ->
            match tryExtractStaticPatternAndRegex rest with
            | Some (pat, rxOpt) -> Some (MatchesCount(target, pat, rxOpt))
            | _ -> None

        // 6.2 Instance / SG Regex: rx.Count(input) (.NET 7+)
        | MethodCall(m, rxExpr, [ target ])
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Count" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) -> Some (MatchesCount(target, rx.ToString(), Some rx))
            | _ -> None

        // 6.3 Static: Regex.Matches(s, pat, options?).Count
        | MemberAccess(MethodCall(m, null, target :: rest), countProp)
            when m.DeclaringType = typeof<Regex> && m.Name = "Matches" && countProp.Name = "Count" ->
            match tryExtractStaticPatternAndRegex rest with
            | Some (pat, rxOpt) -> Some (MatchesCount(target, pat, rxOpt))
            | _ -> None

        // 6.4 Instance / SG: rx.Matches(s).Count
        | MemberAccess(MethodCall(m, rxExpr, [ target ]), countProp)
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && 
                 m.Name = "Matches" && countProp.Name = "Count" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) -> Some (MatchesCount(target, rx.ToString(), Some rx))
            | _ -> None

        // ---------------------------------------------------------------------
        // 7. Matches + Select(m => m.Value).ToList() / .ToArray()
        // ---------------------------------------------------------------------
        // Static: Regex.Matches(s, pat, options?).Select(m => m.Value).ToList() or .ToArray()
        | MethodCall(collMethod, null, [ MethodCall(selMethod, null, [ MethodCall(m, null, target :: rest); _ ]) ])
            when (collMethod.Name = "ToList" || collMethod.Name = "ToArray") && 
                 selMethod.Name = "Select" && 
                 m.DeclaringType = typeof<Regex> && m.Name = "Matches" ->
            match tryExtractStaticPatternAndRegex rest with
            | Some (pat, rxOpt) -> Some (MatchesAll(target, pat, rxOpt))
            | _ -> None

        // Instance / SG: rx.Matches(s).Select(m => m.Value).ToList() or .ToArray()
        | MethodCall(collMethod, null, [ MethodCall(selMethod, null, [ MethodCall(m, rxExpr, [ target ]); _ ]) ])
            when (collMethod.Name = "ToList" || collMethod.Name = "ToArray") && 
                 selMethod.Name = "Select" && 
                 not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Matches" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) -> Some (MatchesAll(target, rx.ToString(), Some rx))
            | _ -> None

        // Standalone .Select(m => m.Value) (e.g. without trailing .ToList())
        | MethodCall(selMethod, null, [ MethodCall(m, null, target :: rest); _ ])
            when selMethod.Name = "Select" && m.DeclaringType = typeof<Regex> && m.Name = "Matches" ->
            match tryExtractStaticPatternAndRegex rest with
            | Some (pat, rxOpt) -> Some (MatchesAll(target, pat, rxOpt))
            | _ -> None

        | MethodCall(selMethod, null, [ MethodCall(m, rxExpr, [ target ]); _ ])
            when selMethod.Name = "Select" && not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Matches" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) -> Some (MatchesAll(target, rx.ToString(), Some rx))
            | _ -> None

        // ---------------------------------------------------------------------
        // 8. Split: Regex.Split(s, pat, options?) / rx.Split(s)
        // ---------------------------------------------------------------------
        // Static: Regex.Split(input, pattern, options?)
        | MethodCall(m, null, target :: rest)
            when m.DeclaringType = typeof<Regex> && m.Name = "Split" ->
            match tryExtractStaticPatternAndRegex rest with
            | Some (pat, rxOpt) -> Some (Split(target, pat, rxOpt))
            | _ -> None

        // Instance / SG Regex: rx.Split(input)
        | MethodCall(m, rxExpr, [ target ])
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Split" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) -> Some (Split(target, rx.ToString(), Some rx))
            | _ -> None

        | _ -> None

    // =========================================================================
    // List / Array Operations
    // =========================================================================

    let private isListContainer (t: Type) =
        t <> typeof<string> &&
        (t.IsArray || typeof<IEnumerable>.IsAssignableFrom(t))

    let (|ArrayIndexOp|_|) (expr: Expression) : (Expression * Expression) option =
        match expr with
        | :? BinaryExpression as bin when bin.NodeType = ExpressionType.ArrayIndex ->
            Some (bin.Left, bin.Right)
        | :? MethodCallExpression as m when not (isNull m.Object) && m.Object.Type.IsArray && (m.Method.Name = "Get" || m.Method.Name = "Address") && m.Arguments.Count = 1 ->
            Some (m.Object, m.Arguments.[0])
        | :? IndexExpression as idx when idx.Arguments.Count = 1 && isListContainer idx.Object.Type ->
            Some (idx.Object, idx.Arguments.[0])
        | _ -> None

    let rec (|ListInvocation|_|) (expr: Expression) : ListInvocation option =
        match expr with
        // ---------------------------------------------------------------------
        // 1. Length & Count
        // ---------------------------------------------------------------------
        | :? UnaryExpression as u when u.NodeType = ExpressionType.ArrayLength ->
            Some (ListLen u.Operand)

        | MemberAccess(target, m) 
            when not (isNull target) && target.Type.IsArray && m.Name = "Length" ->
            Some (ListLen target)

        | MemberAccess(target, m) 
            when not (isNull target) && isListContainer target.Type && m.Name = "Count" ->
            Some (ListLen target)

        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Count" && isListContainer target.Type ->
            Some (ListLen target)

        // ---------------------------------------------------------------------
        // 2. Contains
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ target; itemExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Contains" && isListContainer target.Type ->
            Some (ListContains(target, itemExpr))

        | MethodCall(m, target, [ itemExpr ])
            when not (isNull target) && isListContainer target.Type && m.Name = "Contains" ->
            Some (ListContains(target, itemExpr))

        // ---------------------------------------------------------------------
        // 3. Index Access & ElementAt
        // ---------------------------------------------------------------------
        | :? BinaryExpression as bin when bin.NodeType = ExpressionType.ArrayIndex ->
            Some (ListGet(bin.Left, bin.Right))

        | MethodCall(m, target, [ indexExpr ])
            when not (isNull target) && target.Type.IsArray && (m.Name = "Get" || m.Name = "Address") ->
            Some (ListGet(target, indexExpr))

        | :? IndexExpression as idx when idx.Arguments.Count = 1 && isListContainer idx.Object.Type ->
            Some (ListGet(idx.Object, idx.Arguments.[0]))

        | MethodCall(m, target, [ indexExpr ])
            when not (isNull target) && isListContainer target.Type && m.Name = "get_Item" ->
            Some (ListGet(target, indexExpr))

        | MethodCall(m, null, [ target; indexExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "ElementAt" && isListContainer target.Type ->
            Some (ListGet(target, indexExpr))

        // ---------------------------------------------------------------------
        // 4. First & Last
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "First" && isListContainer target.Type ->
            Some (ListFirst target)

        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Last" && isListContainer target.Type ->
            Some (ListLast target)

        // ---------------------------------------------------------------------
        // 5. In-Row Numeric Aggregations: Sum, Average, Min, Max
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Sum" && isListContainer target.Type ->
            Some (ListSum target)

        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Average" && isListContainer target.Type ->
            Some (ListMean target)

        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Min" && isListContainer target.Type ->
            Some (ListMin target)

        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Max" && isListContainer target.Type ->
            Some (ListMax target)

        // ---------------------------------------------------------------------
        // 6. In-Row String Join: string.Join(sep, x.Tags)
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ sepExpr; target ])
            when m.DeclaringType = typeof<string> && m.Name = "Join" && isListContainer target.Type ->
            Some (ListJoin(target, sepExpr))

        // ---------------------------------------------------------------------
        // 7. In-Row Slicing: Take(n), Skip(n)
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ target; countExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Take" && isListContainer target.Type ->
            Some (ListHead(target, countExpr))

        | MethodCall(m, null, [ target; countExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "TakeLast" && isListContainer target.Type ->
            Some (ListTail(target, countExpr))

        | MethodCall(m, null, [ target; countExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Skip" && isListContainer target.Type ->
            Some (ListSlice(target, countExpr, None))

        // ---------------------------------------------------------------------
        // 8. In-Row Transformations: Distinct(), Reverse()
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Distinct" && isListContainer target.Type ->
            Some (ListUnique target)

        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Reverse" && isListContainer target.Type ->
            Some (ListReverse target)

        // ---------------------------------------------------------------------
        // 9. In-Row Sorting: OrderBy, Order, OrderByDescending, OrderDescending
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Order" && isListContainer target.Type ->
            Some (ListSort(target, false))

        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "OrderDescending" && isListContainer target.Type ->
            Some (ListSort(target, true))

        | MethodCall(m, null, [ target; Lambda([ p ], body) ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "OrderBy" && isListContainer target.Type && body = (p :> Expression) ->
            Some (ListSort(target, false))

        | MethodCall(m, null, [ target; Lambda([ p ], body) ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "OrderByDescending" && isListContainer target.Type && body = (p :> Expression) ->
            Some (ListSort(target, true))

        // ---------------------------------------------------------------------
        // 10. In-Row Set Operations: Intersect, Union, Except
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ target; otherExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Intersect" && isListContainer target.Type ->
            Some (ListSetIntersection(target, otherExpr))

        | MethodCall(m, null, [ target; otherExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Union" && isListContainer target.Type ->
            Some (ListSetUnion(target, otherExpr))

        | MethodCall(m, null, [ target; otherExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Except" && isListContainer target.Type ->
            Some (ListSetDifference(target, otherExpr))

        // ---------------------------------------------------------------------
        // 11. In-Row Predicates: Any(), Any(predicate), All(predicate)
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Any" && isListContainer target.Type ->
            Some (ListAny(target, None))

        | MethodCall(m, null, [ target; :? LambdaExpression as lam ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Any" && isListContainer target.Type ->
            Some (ListAny(target, Some lam))

        | MethodCall(m, null, [ target; :? LambdaExpression as lam ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "All" && isListContainer target.Type ->
            Some (ListAll(target, lam))

        // ---------------------------------------------------------------------
        // 12. In-Row Projection: Select(mapper)
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ target; :? LambdaExpression as lam ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Select" && isListContainer target.Type ->
            Some (ListSelect(target, lam))

        | _ -> None