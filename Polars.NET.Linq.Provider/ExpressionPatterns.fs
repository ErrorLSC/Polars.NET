namespace Polars.NET.Linq.Provider

open System.Linq.Expressions
open System.Reflection
open System.Text.RegularExpressions
open System


[<AutoOpen>]
module internal ExpressionPatterns =
    /// Represents extracted components of a Regex method invocation (Static or Instance/SG)
    type internal RegexInvocation =
        | IsMatch of target: Expression * pattern: string * instanceOpt: Regex option
        | ReplaceLiteral of target: Expression * pattern: string * replacement: string * instanceOpt: Regex option
        | ReplaceEvaluator of target: Expression * pattern: string * evaluator: MatchEvaluator * instanceOpt: Regex option
        | MatchValue of target: Expression * pattern: string * groupIndex: int * instanceOpt: Regex option
        | MatchesAll of target: Expression * pattern: string * instanceOpt: Regex option
        | MatchesCount of target: Expression * pattern: string * instanceOpt: Regex option
        | Split of target: Expression * pattern: string * instanceOpt: Regex option

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

    /// Evaluates expressions that resolve to values (e.g., closures, captured variables, local props)
    let rec tryEvaluate (expr: Expression) : obj option =
        match expr with
        | Constant(value, _) -> Some value
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
        | _ -> None

    let (|RegexInvocation|_|) (expr: Expression) : RegexInvocation option =
        match expr with
        // ---------------------------------------------------------------------
        // 1. Static Regex calls: Regex.IsMatch, Regex.Replace, Regex.Match
        // ---------------------------------------------------------------------
        | MethodCall(m, null, [ target; patternExpr ]) 
            when m.DeclaringType = typeof<Regex> && m.Name = "IsMatch" ->
            match tryEvaluate patternExpr with
            | Some (:? string as pat) -> Some (IsMatch(target, pat, None))
            | _ -> None

        | MethodCall(m, null, [ target; patternExpr; repExpr ]) 
            when m.DeclaringType = typeof<Regex> && m.Name = "Replace" ->
            match tryEvaluate patternExpr, tryEvaluate repExpr with
            | Some (:? string as pat), Some (:? string as rep) ->
                Some (ReplaceLiteral(target, pat, rep, None))
            | Some (:? string as pat), Some (:? MatchEvaluator as eval) ->
                Some (ReplaceEvaluator(target, pat, eval, None))
            | _ -> None

        | MethodCall(m, null, [ target; patternExpr ]) 
            when m.DeclaringType = typeof<Regex> && m.Name = "Match" ->
            match tryEvaluate patternExpr with
            | Some (:? string as pat) -> Some (MatchValue(target, pat, 0, None))
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
            match tryEvaluate rxExpr, tryEvaluate repExpr with
            | Some (:? Regex as rx), Some (:? string as rep) ->
                Some (ReplaceLiteral(target, rx.ToString(), rep, Some rx))
            | Some (:? Regex as rx), Some (:? MatchEvaluator as eval) ->
                Some (ReplaceEvaluator(target, rx.ToString(), eval, Some rx))
            | _ -> None

        // ---------------------------------------------------------------------
        // 3. Match(s, pat).Value / rx.Match(s).Value property chaining
        // ---------------------------------------------------------------------
        | MemberAccess(MethodCall(m, null, [ target; patternExpr ]), memberInfo)
            when m.DeclaringType = typeof<Regex> && m.Name = "Match" && memberInfo.Name = "Value" ->
            match tryEvaluate patternExpr with
            | Some (:? string as pat) -> Some (MatchValue(target, pat, 0, None))
            | _ -> None

        | MemberAccess(MethodCall(m, rxExpr, [ target ]), memberInfo)
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Match" && memberInfo.Name = "Value" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) -> Some (MatchValue(target, rx.ToString(), 0, Some rx))
            | _ -> None

        // ---------------------------------------------------------------------
        // 4. Match(s, pat).Groups[n].Value / rx.Match(s).Groups[n].Value
        // ---------------------------------------------------------------------
        | MemberAccess(MethodCall(indexer, MemberAccess(MethodCall(m, null, [ target; patternExpr ]), groupsProp), [ groupIdxExpr ]), valueProp)
            when m.DeclaringType = typeof<Regex> && m.Name = "Match" && 
                 groupsProp.Name = "Groups" && valueProp.Name = "Value" && indexer.Name = "get_Item" ->
            match tryEvaluate patternExpr, tryEvaluate groupIdxExpr with
            | Some (:? string as pat), Some idx ->
                Some (MatchValue(target, pat, Convert.ToInt32(idx), None))
            | _ -> None

        | MemberAccess(MethodCall(indexer, MemberAccess(MethodCall(m, rxExpr, [ target ]), groupsProp), [ groupIdxExpr ]), valueProp)
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Match" && 
                 groupsProp.Name = "Groups" && valueProp.Name = "Value" && indexer.Name = "get_Item" ->
            match tryEvaluate rxExpr, tryEvaluate groupIdxExpr with
            | Some (:? Regex as rx), Some idx ->
                Some (MatchValue(target, rx.ToString(), Convert.ToInt32(idx), Some rx))
            | _ -> None

        // ---------------------------------------------------------------------
        // 5. Matches: Regex.Matches(s, pat) / rx.Matches(s)
        // ---------------------------------------------------------------------
        // 5.1 Static: Regex.Count(input, pattern)
        | MethodCall(m, null, [ target; patternExpr ])
            when m.DeclaringType = typeof<Regex> && m.Name = "Count" ->
            match tryEvaluate patternExpr with
            | Some (:? string as pat) -> Some (MatchesCount(target, pat, None))
            | _ -> None

        // 5.2 Instance / SG Regex: rx.Count(input)
        | MethodCall(m, rxExpr, [ target ])
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Count" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) -> Some (MatchesCount(target, rx.ToString(), Some rx))
            | _ -> None

        // 5.3 Static: Regex.Matches(s, pat).Count
        | MemberAccess(MethodCall(m, null, [ target; patternExpr ]), countProp)
            when m.DeclaringType = typeof<Regex> && m.Name = "Matches" && countProp.Name = "Count" ->
            match tryEvaluate patternExpr with
            | Some (:? string as pat) -> Some (MatchesCount(target, pat, None))
            | _ -> None

        // 5.4 Instance / SG: rx.Matches(s).Count
        | MemberAccess(MethodCall(m, rxExpr, [ target ]), countProp)
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && 
                 m.Name = "Matches" && countProp.Name = "Count" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) -> Some (MatchesCount(target, rx.ToString(), Some rx))
            | _ -> None

        // ---------------------------------------------------------------------
        // 5.5 Matches + Select(m => m.Value).ToList() / .ToArray()
        // Flattened matching without recursive self-invocation
        // ---------------------------------------------------------------------
        // Static: Regex.Matches(s, pat).Select(m => m.Value).ToList() or .ToArray()
        | MethodCall(collMethod, null, [ MethodCall(selMethod, null, [ MethodCall(m, null, [ target; patternExpr ]); _ ]) ])
            when (collMethod.Name = "ToList" || collMethod.Name = "ToArray") && 
                 selMethod.Name = "Select" && 
                 m.DeclaringType = typeof<Regex> && m.Name = "Matches" ->
            match tryEvaluate patternExpr with
            | Some (:? string as pat) -> Some (MatchesAll(target, pat, None))
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
        | MethodCall(selMethod, null, [ MethodCall(m, null, [ target; patternExpr ]); _ ])
            when selMethod.Name = "Select" && m.DeclaringType = typeof<Regex> && m.Name = "Matches" ->
            match tryEvaluate patternExpr with
            | Some (:? string as pat) -> Some (MatchesAll(target, pat, None))
            | _ -> None

        | MethodCall(selMethod, null, [ MethodCall(m, rxExpr, [ target ]); _ ])
            when selMethod.Name = "Select" && not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Matches" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) -> Some (MatchesAll(target, rx.ToString(), Some rx))
            | _ -> None

        // ---------------------------------------------------------------------
        // 6. Split: Regex.Split(s, pat) / rx.Split(s)
        // ---------------------------------------------------------------------
        // Static: Regex.Split(input, pattern)
        | MethodCall(m, null, [ target; patternExpr ])
            when m.DeclaringType = typeof<Regex> && m.Name = "Split" ->
            match tryEvaluate patternExpr with
            | Some (:? string as pat) -> Some (Split(target, pat, None))
            | _ -> None

        // Instance / SG Regex: rx.Split(input)
        | MethodCall(m, rxExpr, [ target ])
            when not (isNull rxExpr) && typeof<Regex>.IsAssignableFrom(rxExpr.Type) && m.Name = "Split" ->
            match tryEvaluate rxExpr with
            | Some (:? Regex as rx) -> Some (Split(target, rx.ToString(), Some rx))
            | _ -> None

        | _ -> None
