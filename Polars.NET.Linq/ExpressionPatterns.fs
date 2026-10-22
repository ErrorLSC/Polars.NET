namespace Polars.NET.Linq

open System.Linq.Expressions
open System.Reflection
open System.Text.RegularExpressions
open System.Collections
open System
open System.Linq
open Polars.NET.Core.Helpers

[<AutoOpen>]
module internal OptionComputationExpression =
    type OptionBuilder() =
        member inline _.Bind(m: 'T option, [<InlineIfLambda>] f: 'T -> 'U option) : 'U option = Option.bind f m
        member inline _.Return(x: 'T) : 'T option = Some x
        member inline _.ReturnFrom(m: 'T option) : 'T option = m
        member inline _.Zero() : unit option = Some()

        member inline _.Combine(a: unit option, b: 'T option) : 'T option =
            match a with
            | Some() -> b
            | None -> None

        member inline _.Delay([<InlineIfLambda>] f: unit -> 'T option) : 'T option = f ()

    let option = OptionBuilder()

    let inline guard (cond: bool) : unit option = if cond then Some() else None

[<AutoOpen>]
module internal ExpressionPatterns =
    /// Represents extracted components of a Regex method invocation (Static or Instance/SG)
    type internal RegexInvocation =
        | IsMatch of target: Expression * pattern: string * instanceOpt: Regex option
        | ReplaceLiteral of target: Expression * pattern: string * replacement: string * instanceOpt: Regex option
        | ReplaceEvaluator of
            target: Expression *
            pattern: string *
            evaluator: MatchEvaluator *
            instanceOpt: Regex option
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

    /// Checks if a Type is a compiler-generated or runtime tuple/anonymous/transparent wrapper
    let internal isTupleOrAnonType (t: Type) =
        not (isNull t)
        && (t.Name.StartsWith "AnonymousObject"
            || t.Name.StartsWith "Tuple"
            || t.Name.StartsWith "ValueTuple"
            || t.Name.Contains "TransparentIdentifier")

    /// Checks if a ParameterExpression belongs to the active query context or a synthetic tuple/transparent scope
    let internal isSyntheticScopeParam (ctxContainsParam: string -> bool) (p: ParameterExpression) =
        not (isNull p)
        && (ctxContainsParam p.Name
            || not (isNull p.Name)
               && (p.Name.StartsWith "_arg"
                   || p.Name.StartsWith "tupled"
                   || p.Name.Contains "TransparentIdentifier"
                   || p.Name.StartsWith "<>h__")
            || isTupleOrAnonType p.Type
            || not (isNull p.Type) && p.Type.Name.Contains "AnonymousType")

    let private isNullableMember (memberName: string) (m: MemberInfo) =
        not (isNull m)
        && m.Name = memberName
        && not (isNull m.DeclaringType)
        && m.DeclaringType.IsGenericType
        && m.DeclaringType.GetGenericTypeDefinition() = typedefof<Nullable<_>>

    let internal isNullableValue (m: MemberInfo) = isNullableMember "Value" m
    let internal isNullableHasValue (m: MemberInfo) = isNullableMember "HasValue" m

    /// Checks if a member belongs to F#'s internal AnonymousObject tuple wrapper
    let internal isFSharpAnonymousMember (m: MemberInfo) =
        if isNull m || isNull m.DeclaringType then
            false
        else
            let dt = m.DeclaringType
            let name = dt.FullName

            if isNull name then
                false
            else
                name.StartsWith "Microsoft.FSharp.Linq.RuntimeHelpers.AnonymousObject"
                || dt.IsGenericType && dt.Name.StartsWith "AnonymousObject`"

    /// Unified check for whether a member access is a transparent navigation/scope wrapper
    /// (such as Nullable<T>.Value, Roslyn TransparentIdentifier, Zip/Join tuple item,
    /// or F#/C# anonymous scope holder) rather than a physical scalar column.
    let internal isNavigationMember (m: MemberInfo) =
        if isNull m || isNull m.DeclaringType then
            false
        elif isNullableValue m then
            true
        else
            let typeName = m.DeclaringType.Name
            let memberName = m.Name
            let isNonScalarMember =
                match m with
                | :? PropertyInfo as p -> not (PolarsTypeHelper.IsScalarType p.PropertyType)
                | :? FieldInfo as f -> not (PolarsTypeHelper.IsScalarType f.FieldType)
                | _ -> false

            typeName.Contains "TransparentIdentifier"
            || memberName.Contains "TransparentIdentifier"
            || memberName.StartsWith "<>h__"
            || (isTupleOrAnonType m.DeclaringType
                || typeName.Contains "AnonymousType"
                || isFSharpAnonymousMember m
                || memberName = "First"
                || memberName = "Second"
                || memberName = "Third"
                || memberName = "Rest")
               && isNonScalarMember

    let (|StructFieldAccess|_|) (expr: Expression) : (Expression * string) option =
        match expr with
        | MemberAccess(target, m) when
            not (isNull target)
            && target.NodeType = ExpressionType.MemberAccess
            && PolarsTypeHelper.IsStructType target.Type
            ->

            // Check the target member itself
            match target with
            | :? MemberExpression as parentMe when isNavigationMember parentMe.Member ->
                // This is a Zip / Join tuple or transparent scope projection -> NOT a Struct column!
                None
            | _ -> Some(target, m.Name)

        | _ -> None

    [<TailCall>]
    let rec internal isTransparentChain (expr: Expression) =
        match expr with
        | null -> false
        | :? ParameterExpression -> true
        | MemberAccess(target, m) when isNavigationMember m ->
            isTransparentChain target
        | _ -> false

    /// Active pattern to strip convert/quote wrappers and resolve root column names,
    /// seamlessly unwrapping F# AnonymousObject tuple chains
    let rec (|ExtractColumnName|_|) (expr: Expression) : string option =
        match expr with
        | null -> None
        | Unary(ExpressionType.Convert, inner)
        | Unary(ExpressionType.ConvertChecked, inner)
        | Unary(ExpressionType.Quote, inner) -> (|ExtractColumnName|_|) inner
        // Direct member: e.Name
        | MemberAccess(p, m) when not (isNull p) && p.NodeType = ExpressionType.Parameter -> Some m.Name
        // Arbitrary F# AnonymousObject / Tuple nesting: tuple.Item1.Name, tuple.Item8.Item1.Name, etc.
        | MemberAccess(target, m) when not (isNull target) && isTransparentChain target -> Some m.Name
        | _ -> None

    /// Evaluates expressions that resolve to values (e.g., closures, captured variables, local props, method calls)
    let rec tryEvaluate (expr: Expression) : obj option =
        if isNull expr then
            None
        else
            match PartialEvaluator.tryFastEvaluate expr with
            | Some v -> Some v
            | None ->
                match expr with
                | Unary(ExpressionType.Convert, inner)
                | Unary(ExpressionType.ConvertChecked, inner)
                | Unary(ExpressionType.Quote, inner) -> tryEvaluate inner
                | :? MethodCallExpression as mc when mc.Arguments.Count = 0 ->
                    // Supports evaluating parameterless factory methods such as [GeneratedRegex] partial methods
                    try
                        let targetObj =
                            if isNull mc.Object then
                                null
                            else
                                match tryEvaluate mc.Object with
                                | Some o -> o
                                | None -> null

                        Some(mc.Method.Invoke(targetObj, null))
                    with _ ->
                        None
                | :? NewArrayExpression as na when na.NodeType = ExpressionType.NewArrayInit ->
                    try
                        Some(PartialEvaluator.evaluateSubtree na)
                    with _ ->
                        None
                | :? NewExpression | :? ListInitExpression ->
                    try
                        Some(PartialEvaluator.evaluateSubtree expr)
                    with _ ->
                        None
                | _ -> None

    // =========================================================================
    // Regex Helpers
    // =========================================================================

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

    // =========================================================================
    // Regex Invocation Active Pattern
    // =========================================================================

    let (|RegexInvocation|_|) (expr: Expression) : RegexInvocation option =
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

    // =========================================================================
    // List / Array Operations
    // =========================================================================

    [<TailCall>]
    let rec internal unwrapSpanOrArray (expr: Expression) : Expression =
        match expr with
        | null -> null
        | :? UnaryExpression as u when
            u.NodeType = ExpressionType.Convert
            || u.NodeType = ExpressionType.ConvertChecked
            ->
            unwrapSpanOrArray u.Operand
        | :? MethodCallExpression as mc when
            mc.Method.Name = "op_Implicit"
            && mc.Arguments.Count = 1
            && (mc.Type.Name.StartsWith "ReadOnlySpan`1"
                || mc.Type.Name.StartsWith "Span`1"
                || mc.Type.Name.StartsWith "ReadOnlyMemory`1"
                || mc.Type.Name.StartsWith "Memory`1")
            ->
            unwrapSpanOrArray mc.Arguments.[0]
        | :? MethodCallExpression as mc when
            (mc.Method.Name = "AsSpan" || mc.Method.Name = "AsMemory")
            && not (isNull mc.Method.DeclaringType)
            && mc.Method.DeclaringType.Name = "MemoryExtensions"
            && (not (isNull mc.Object) || mc.Arguments.Count >= 1)
            ->
            let inner = if isNull mc.Object then mc.Arguments.[0] else mc.Object
            unwrapSpanOrArray inner
        | other -> other

    let private isListContainer (t: Type) =
        t <> typeof<string>
        && not (typeof<MatchCollection>.IsAssignableFrom t)
        && (t.IsArray || typeof<IEnumerable>.IsAssignableFrom t)

    let (|ArrayIndexOp|_|) (expr: Expression) : (Expression * Expression) option =
        match expr with
        | :? BinaryExpression as bin when bin.NodeType = ExpressionType.ArrayIndex -> Some(bin.Left, bin.Right)
        | :? MethodCallExpression as m when
            not (isNull m.Object)
            && m.Object.Type.IsArray
            && (m.Method.Name = "Get" || m.Method.Name = "Address")
            && m.Arguments.Count = 1
            ->
            Some(m.Object, m.Arguments.[0])
        | :? IndexExpression as idx when idx.Arguments.Count = 1 && isListContainer idx.Object.Type ->
            Some(idx.Object, idx.Arguments.[0])
        | _ -> None

    let (|ListInvocation|_|) (expr: Expression) : ListInvocation option =
        match expr with
        // 1. Array / Indexer / Property Access
        | :? UnaryExpression as u when u.NodeType = ExpressionType.ArrayLength -> Some(ListLen u.Operand)
        | ArrayIndexOp(target, indexExpr) -> Some(ListGet(target, indexExpr))

        | MemberAccess(target, m) when
            not (isNull target)
            && (target.Type.IsArray && m.Name = "Length"
                || isListContainer target.Type && m.Name = "Count")
            ->
            Some(ListLen target)

        // 2. Instance Methods (Contains, Indexer get_Item)
        | MethodCall(m, target, [ arg ]) when not (isNull target) && isListContainer target.Type ->
            match m.Name with
            | "Contains" -> Some(ListContains(target, arg))
            | "get_Item" -> Some(ListGet(target, arg))
            | _ -> None

        // 3. Special Static Methods (MemoryExtensions.Contains, string.Join)
        | MethodCall(m, null, [ spanExpr; itemExpr ]) when
            not (isNull m.DeclaringType)
            && m.DeclaringType.Name = "MemoryExtensions"
            && m.Name = "Contains"
            ->
            let target = unwrapSpanOrArray spanExpr
            if isListContainer target.Type then Some(ListContains(target, itemExpr)) else None

        | MethodCall(m, null, [ sepExpr; target ]) when
            m.DeclaringType = typeof<string>
            && m.Name = "Join"
            && isListContainer target.Type
            ->
            Some(ListJoin(target, sepExpr))

        // 4. Enumerable Static Extension Methods (1 & 2 args)
        | MethodCall(m, null, target :: args) when
            m.DeclaringType = typeof<Enumerable> && isListContainer target.Type
            ->
            match m.Name, args with
            // 1-arg operations
            | "Count", [] -> Some(ListLen target)
            | "First", [] -> Some(ListFirst target)
            | "Last", [] -> Some(ListLast target)
            | "Sum", [] -> Some(ListSum target)
            | "Average", [] -> Some(ListMean target)
            | "Min", [] -> Some(ListMin target)
            | "Max", [] -> Some(ListMax target)
            | "Distinct", [] -> Some(ListUnique target)
            | "Reverse", [] -> Some(ListReverse target)
            | "Order", [] -> Some(ListSort(target, false))
            | "OrderDescending", [] -> Some(ListSort(target, true))
            | "Any", [] -> Some(ListAny(target, None))

            // 2-arg value/collection operations
            | "Contains", [ itemExpr ] -> Some(ListContains(target, itemExpr))
            | "ElementAt", [ indexExpr ] -> Some(ListGet(target, indexExpr))
            | "Take", [ countExpr ] -> Some(ListHead(target, countExpr))
            | "TakeLast", [ countExpr ] -> Some(ListTail(target, countExpr))
            | "Skip", [ countExpr ] -> Some(ListSlice(target, countExpr, None))
            | "Intersect", [ otherExpr ] -> Some(ListSetIntersection(target, otherExpr))
            | "Union", [ otherExpr ] -> Some(ListSetUnion(target, otherExpr))
            | "Except", [ otherExpr ] -> Some(ListSetDifference(target, otherExpr))

            // 2-arg lambda operations
            | ("OrderBy" | "OrderByDescending"), [ Lambda([ p ], body) ] when body = (p :> Expression) ->
                Some(ListSort(target, m.Name = "OrderByDescending"))
            | "Any", [ :? LambdaExpression as lam ] -> Some(ListAny(target, Some lam))
            | "All", [ :? LambdaExpression as lam ] -> Some(ListAll(target, lam))
            | "Select", [ :? LambdaExpression as lam ] -> Some(ListSelect(target, lam))
            | _ -> None

        | _ -> None
