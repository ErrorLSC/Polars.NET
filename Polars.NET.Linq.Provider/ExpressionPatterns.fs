namespace Polars.NET.Linq.Provider

open System.Linq.Expressions
open System.Reflection
open System.Text.RegularExpressions
open System.Collections
open System


[<AutoOpen>]
module internal ExpressionPatterns =
    open System.Linq
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
        // ArrayLength Unary Expression
        | :? UnaryExpression as u when u.NodeType = ExpressionType.ArrayLength ->
            Some (ListLen u.Operand)

        // array.Length
        | MemberAccess(target, m) 
            when not (isNull target) && target.Type.IsArray && m.Name = "Length" ->
            Some (ListLen target)

        // list.Count
        | MemberAccess(target, m) 
            when not (isNull target) && isListContainer target.Type && m.Name = "Count" ->
            Some (ListLen target)

        // Enumerable.Count(target)
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Count" && isListContainer target.Type ->
            Some (ListLen target)

        // ---------------------------------------------------------------------
        // 2. Contains
        // ---------------------------------------------------------------------
        // Enumerable.Contains(target, item)
        | MethodCall(m, null, [ target; itemExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Contains" && isListContainer target.Type ->
            Some (ListContains(target, itemExpr))

        // IList.Contains(item)
        | MethodCall(m, target, [ itemExpr ])
            when not (isNull target) && isListContainer target.Type && m.Name = "Contains" ->
            Some (ListContains(target, itemExpr))

        // ---------------------------------------------------------------------
        // 3. Index Access & ElementAt
        // ---------------------------------------------------------------------
        // BinaryExpression: tags[i]
        | :? BinaryExpression as bin when bin.NodeType = ExpressionType.ArrayIndex ->
            Some (ListGet(bin.Left, bin.Right))

        // MethodCall: tags[i] on Array (Address or Get)
        | MethodCall(m, target, [ indexExpr ])
            when not (isNull target) && target.Type.IsArray && (m.Name = "Get" || m.Name = "Address") ->
            Some (ListGet(target, indexExpr))

        // IndexExpression: tags[i] or list[i]
        | :? IndexExpression as idx when idx.Arguments.Count = 1 && isListContainer idx.Object.Type ->
            Some (ListGet(idx.Object, idx.Arguments.[0]))

        // MethodCall: list[i] (get_Item)
        | MethodCall(m, target, [ indexExpr ])
            when not (isNull target) && isListContainer target.Type && m.Name = "get_Item" ->
            Some (ListGet(target, indexExpr))

        // Enumerable.ElementAt(target, i)
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
        // Enumerable.Sum(x.Scores)
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Sum" && isListContainer target.Type ->
            Some (ListSum target)

        // Enumerable.Average(x.Scores)
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Average" && isListContainer target.Type ->
            Some (ListMean target)

        // Enumerable.Min(x.Scores)
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Min" && isListContainer target.Type ->
            Some (ListMin target)

        // Enumerable.Max(x.Scores)
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Max" && isListContainer target.Type ->
            Some (ListMax target)

        // ---------------------------------------------------------------------
        // 6. In-Row String Join: string.Join(sep, x.Tags)
        // ---------------------------------------------------------------------
        // string.Join(separator, tags)
        | MethodCall(m, null, [ sepExpr; target ])
            when m.DeclaringType = typeof<string> && m.Name = "Join" && isListContainer target.Type ->
            Some (ListJoin(target, sepExpr))

        // ---------------------------------------------------------------------
        // 7. In-Row Slicing: Take(n), Skip(n)
        // ---------------------------------------------------------------------
        // Enumerable.Take(tags, n)
        | MethodCall(m, null, [ target; countExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Take" && isListContainer target.Type ->
            Some (ListHead(target, countExpr))

        // Enumerable.TakeLast(tags, n) -> list().tail(n)
        | MethodCall(m, null, [ target; countExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "TakeLast" && isListContainer target.Type ->
            Some (ListTail(target, countExpr))

        // Enumerable.Skip(tags, n)
        | MethodCall(m, null, [ target; countExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Skip" && isListContainer target.Type ->
            Some (ListSlice(target, countExpr, None))

        // ---------------------------------------------------------------------
        // 8. In-Row Transformations: Distinct(), Reverse()
        // ---------------------------------------------------------------------
        // Enumerable.Distinct(tags)
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Distinct" && isListContainer target.Type ->
            Some (ListUnique target)

        // Enumerable.Reverse(tags)
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Reverse" && isListContainer target.Type ->
            Some (ListReverse target)
        // ---------------------------------------------------------------------
        // 9. In-Row Sorting: OrderBy, Order, OrderByDescending, OrderDescending
        // ---------------------------------------------------------------------
        // Enumerable.Order(scores) (.NET 7+)
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Order" && isListContainer target.Type ->
            Some (ListSort(target, false))

        // Enumerable.OrderDescending(scores) (.NET 7+)
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "OrderDescending" && isListContainer target.Type ->
            Some (ListSort(target, true))

        // Enumerable.OrderBy(scores, s => s) (Identity key selector)
        | MethodCall(m, null, [ target; Lambda([ p ], body) ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "OrderBy" && isListContainer target.Type && body = (p :> Expression) ->
            Some (ListSort(target, false))

        // Enumerable.OrderByDescending(scores, s => s) (Identity key selector)
        | MethodCall(m, null, [ target; Lambda([ p ], body) ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "OrderByDescending" && isListContainer target.Type && body = (p :> Expression) ->
            Some (ListSort(target, true))

        // ---------------------------------------------------------------------
        // 10. In-Row Set Operations: Intersect, Union, Except
        // ---------------------------------------------------------------------
        // Enumerable.Intersect(tags, otherTags)
        | MethodCall(m, null, [ target; otherExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Intersect" && isListContainer target.Type ->
            Some (ListSetIntersection(target, otherExpr))

        // Enumerable.Union(tags, otherTags)
        | MethodCall(m, null, [ target; otherExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Union" && isListContainer target.Type ->
            Some (ListSetUnion(target, otherExpr))

        // Enumerable.Except(tags, otherTags)
        | MethodCall(m, null, [ target; otherExpr ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Except" && isListContainer target.Type ->
            Some (ListSetDifference(target, otherExpr))

        // ---------------------------------------------------------------------
        // 11. In-Row Predicates: Any(), Any(predicate), All(predicate)
        // ---------------------------------------------------------------------
        // Enumerable.Any(tags) -> check if list is non-empty (len > 0)
        | MethodCall(m, null, [ target ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Any" && isListContainer target.Type ->
            Some (ListAny(target, None))

        // Enumerable.Any(tags, predicate)
        | MethodCall(m, null, [ target; :? LambdaExpression as lam ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Any" && isListContainer target.Type ->
            Some (ListAny(target, Some lam))

        // Enumerable.All(tags, predicate)
        | MethodCall(m, null, [ target; :? LambdaExpression as lam ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "All" && isListContainer target.Type ->
            Some (ListAll(target, lam))

        // ---------------------------------------------------------------------
        // 12. In-Row Projection: Select(mapper)
        // ---------------------------------------------------------------------
        // Enumerable.Select(tags, t => ...)
        | MethodCall(m, null, [ target; :? LambdaExpression as lam ])
            when m.DeclaringType = typeof<Enumerable> && m.Name = "Select" && isListContainer target.Type ->
            Some (ListSelect(target, lam))
            
        | _ -> None