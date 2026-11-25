namespace Kaminari.Samidare

open Kaminari.Samidare.IR
open System
open System.Reflection
open System.Linq.Expressions
open System.Linq
open System.Collections.Generic
open Kaminari.Sazanami.Marshalling

/// Recognizing subqueries in LINQ expression trees: which expressions query a source, the query operators that
/// reduce a sequence (Any, Contains, aggregates), the rewrites that normalize them (All, DefaultIfEmpty), and the
/// translation contexts of a sequence's element.
module internal SubqueryPatterns =

    // =========================================================================
    // 1. AST Inspection & Reflection Helpers
    // =========================================================================

    /// Checks recursively whether an expression contains references to a specific parameter name.
    let rec containsParameter (paramName: string) (expr: Expression) : bool =
        match expr with
        | null -> false
        | :? ParameterExpression as p -> p.Name = paramName
        | :? NewExpression as ne -> ne.Arguments |> Seq.exists (containsParameter paramName)
        | :? MemberInitExpression as mie ->
            containsParameter paramName mie.NewExpression
            || mie.Bindings
               |> Seq.exists (function
                   | :? MemberAssignment as ma -> containsParameter paramName ma.Expression
                   | _ -> false)
        | :? NewArrayExpression as nae ->
            nae.Expressions |> Seq.exists (containsParameter paramName)
        | :? ConditionalExpression as cond ->
            containsParameter paramName cond.Test
            || containsParameter paramName cond.IfTrue
            || containsParameter paramName cond.IfFalse
        | MethodCall(_, target, args) ->
            not (isNull target) && containsParameter paramName target
            || args |> List.exists (containsParameter paramName)
        | MemberAccess(target, _) -> not (isNull target) && containsParameter paramName target
        | Unary(_, inner) -> containsParameter paramName inner
        | Binary(_, left, right) -> containsParameter paramName left || containsParameter paramName right
        | Lambda(_, body) -> containsParameter paramName body
        | _ -> false

    /// Whether the expression reads a queryable anywhere: a queryable constant or an IQueryable-typed node.
    let private readsQueryable (expr: Expression) : bool =
        let mutable found = false
        { new ExpressionVisitor() with
            override this.Visit(node: Expression) =
                if found || isNull node then
                    node
                elif typeof<IQueryable>.IsAssignableFrom node.Type then
                    found <- true
                    node
                else
                    match node with
                    | :? ConstantExpression as c when (c.Value :? IQueryable) ->
                        found <- true
                        node
                    | _ -> base.Visit node
        }.Visit expr |> ignore
        found

    /// `Enumerable.Contains(values, x)` over an in-memory collection of scalars (an IN list held in a variable,
    /// an array or a collection expression) and no query: a membership test the scalar translator turns into
    /// `is_in` with the collection's values, not a subquery to decorrelate into a join with a snapshot table.
    let isInMemoryScalarContains (expr: Expression) : bool =
        let isScalarCollection (values: Expression) =
            values.Type <> typeof<string>
            && not (readsQueryable values)
            && (let elemType = ClrTypeHelper.TryGetEnumerableElementType values.Type
                not (isNull elemType) && (ClrTypeHelper.IsScalarType elemType || elemType = typeof<string>))

        match expr with
        | MethodCall(m, null, [ values; _ ]) when m.Name = "Contains" && m.DeclaringType = typeof<Enumerable> ->
            isScalarCollection values
        // The collection's own Contains (List<T>, HashSet<T>, ...).
        | MethodCall(m, values, [ _ ]) when m.Name = "Contains" && not (isNull values) -> isScalarCollection values
        | _ -> false

    /// Checks whether an expression contains a queryable source or LINQ reduction call.
    let rec containsSubquery (expr: Expression) : bool =
        match expr with
        | MethodCall(_, null, [ _; item ])
        | MethodCall(_, _, [ item ]) when isInMemoryScalarContains expr -> containsSubquery item
        | null -> false
        | :? ConstantExpression as c ->
            match c.Value with
            | :? IQueryable
            | :? IInternalQueryExplainable -> true
            | _ -> false
        | :? NewExpression as ne -> ne.Arguments |> Seq.exists containsSubquery
        | :? MemberInitExpression as mie ->
            containsSubquery mie.NewExpression
            || mie.Bindings
               |> Seq.exists (function
                   | :? MemberAssignment as ma -> containsSubquery ma.Expression
                   | _ -> false)
        | :? ConditionalExpression as cond ->
            containsSubquery cond.Test
            || containsSubquery cond.IfTrue
            || containsSubquery cond.IfFalse
        | MethodCall(m, target, args) ->
            let isQueryMethod =
                m.DeclaringType = typeof<Queryable>
                || m.DeclaringType = typeof<Enumerable>
                || typeof<IQueryable>.IsAssignableFrom m.ReturnType

            isQueryMethod
            || not (isNull target) && containsSubquery target
            || args |> List.exists containsSubquery
        | MemberAccess(target, _) -> not (isNull target) && containsSubquery target
        | Unary(_, inner) -> containsSubquery inner
        | Binary(_, left, right) -> containsSubquery left || containsSubquery right
        | Lambda(_, body) -> containsSubquery body
        | _ -> false

    /// Flattens nested binary AndAlso expressions into a list of conjunct expressions.
    let rec flattenAnd (expr: Expression) : Expression list =
        match ExpressionPatterns.unwrap expr with
        | Binary(ExpressionType.AndAlso, left, right) -> flattenAnd left @ flattenAnd right
        | other -> [ other ]

    /// Creates an appropriate TranslationContext based on column names and element type.
    let createContextForTypeWithCols (elemType: Type) (colNames: string list) (paramName: string) : TranslationContext =
        if ClrTypeHelper.IsScalarType elemType then
            if colNames.Length = 1 then
                TranslationContext.Scalar(paramName, colNames.[0])
            else
                TranslationContext.EntityWithCols(paramName, colNames)
        elif
            elemType.IsGenericType
            && (elemType.Name.StartsWith "ValueTuple`2"
                || elemType.Name.StartsWith "Tuple`2"
                || elemType.Name.Contains "AnonymousType"
                   && not (isNull (elemType.GetProperty "Outer"))
                   && not (isNull (elemType.GetProperty "Inner")))
        then
            let genArgs = elemType.GetGenericArguments()
            let tOuter = genArgs.[0]

            let rec getLeftMemberNames (t: Type) : Set<string> =
                let actualT = Nullable.GetUnderlyingType t |> Option.ofObj |> Option.defaultValue t

                if
                    actualT.IsGenericType
                    && (actualT.Name.StartsWith "ValueTuple" || actualT.Name.StartsWith "Tuple")
                then
                    actualT.GetGenericArguments() |> Array.map getLeftMemberNames |> Set.unionMany
                elif ClrTypeHelper.IsScalarType actualT then
                    Set.empty
                else
                    let props =
                        ClrTypeHelper.GetModelProperties actualT
                        |> Array.map (fun p -> p.Name)

                    let fields =
                        actualT.GetFields(BindingFlags.Public ||| BindingFlags.Instance)
                        |> Array.map (fun f -> f.Name)

                    Array.append props fields |> Set.ofArray

            let typeCols = getLeftMemberNames tOuter
            let lfCols = colNames |> Set.ofList
            let leftCols = Set.union typeCols lfCols
            TranslationContext.TupleJoin(paramName, leftCols, LinqConstants.DefaultRightSuffix)
        else
            TranslationContext.EntityWithCols(paramName, colNames)

    /// Creates an appropriate TranslationContext based on whether the sequence element type
    /// is a scalar/primitive (bound to the underlying single physical column) or a composite entity.
    let createContextForType (elemType: Type) (plan: LogicalPlan) (paramName: string) : TranslationContext =
        let colNames = (SchemaInference.inferPlanSchema plan).ColumnNames
        createContextForTypeWithCols elemType colNames paramName


    // =========================================================================
    // 2. Sequence reductions
    // =========================================================================

    type AggReductionCall =
        {
            InnerSeq: Expression
            MethodName: string
            TargetType: Type
            SelectorLambdaOpt: LambdaExpression option
            ExtraWhereLambdaOpt: LambdaExpression option
        }

    let private isAggReductionMethod (name: string) : bool =
        match name with
        | "Average"
        | "Mean"
        | "Min"
        | "Max"
        | "Sum"
        | "Count"
        | "LongCount"
        | "Median"
        | "ToList"
        | "ToArray" -> true
        | _ -> false

    let rec unwrapWhere (seqExpr: Expression) : Expression * LambdaExpression list =
        match ExpressionPatterns.unwrap seqExpr with
        | MethodCall(mWhere, null, [ src; CleanLambda lam ]) when mWhere.Name = "Where" ->
            let rootSrc, lams = unwrapWhere src
            rootSrc, lams @ [ lam ]
        | MethodCall(mWhere, src, [ CleanLambda lam ]) when mWhere.Name = "Where" ->
            let rootSrc, lams = unwrapWhere src
            rootSrc, lams @ [ lam ]
        | MethodCall(mSel, null, [ src; CleanLambda selLam ]) when mSel.Name = "Select" ->
            let rootSrc, lams = unwrapWhere src
            if not lams.IsEmpty then rootSrc, lams else seqExpr, []
        | MethodCall(mSel, src, [ CleanLambda selLam ]) when mSel.Name = "Select" ->
            let rootSrc, lams = unwrapWhere src
            if not lams.IsEmpty then rootSrc, lams else seqExpr, []
        | other -> other, []

    // Pattern 1: Any(innerSeq, [predicate]) or innerSeq.Where(predicate).Any()
    let tryMatchAny (e: Expression) : (Expression * LambdaExpression option) option =
        match e with
        | MethodCall(m, null, [ innerSeq; CleanLambda lam ]) when m.Name = "Any" -> Some(innerSeq, Some lam)
        | MethodCall(m, innerSeq, [ CleanLambda lam ]) when m.Name = "Any" -> Some(innerSeq, Some lam)
        | MethodCall(m, null, [ innerSeq ]) when m.Name = "Any" ->
            match ExpressionPatterns.unwrap innerSeq with
            | MethodCall(mWhere, null, [ src; CleanLambda lam ]) when mWhere.Name = "Where" -> Some(src, Some lam)
            | MethodCall(mWhere, src, [ CleanLambda lam ]) when mWhere.Name = "Where" -> Some(src, Some lam)
            | _ -> Some(innerSeq, None)
        | MethodCall(m, innerSeq, []) when m.Name = "Any" ->
            match ExpressionPatterns.unwrap innerSeq with
            | MethodCall(mWhere, null, [ src; CleanLambda lam ]) when mWhere.Name = "Where" -> Some(src, Some lam)
            | MethodCall(mWhere, src, [ CleanLambda lam ]) when mWhere.Name = "Where" -> Some(src, Some lam)
            | _ -> Some(innerSeq, None)
        | _ -> None

    // Pattern 2: innerSeq.Contains(keyExpr) or Contains(innerSeq, keyExpr).
    // A trailing non-scalar Select is split off as the key selector; a scalar-element sequence is kept whole
    // (it resolves to a single-column plan, and splitting could leave a source that does not resolve on its
    // own, e.g. GroupBy(..).Where(g => ..) without its key Select).
    let tryMatchContains (e: Expression) : (Expression * LambdaExpression option * Expression) option =
        let matchSeqAndKey (innerSeq: Expression) (keyExpr: Expression) =
            match ExpressionPatterns.unwrap innerSeq with
            | MethodCall(mSel, null, [ src; CleanLambda selLam ]) when
                mSel.Name = "Select" && not (ClrTypeHelper.IsScalarType selLam.ReturnType) ->
                Some(src, Some selLam, keyExpr)
            | MethodCall(mSel, src, [ CleanLambda selLam ]) when
                mSel.Name = "Select" && not (ClrTypeHelper.IsScalarType selLam.ReturnType) ->
                Some(src, Some selLam, keyExpr)
            | otherSeq -> Some(otherSeq, None, keyExpr)

        match e with
        | _ when isInMemoryScalarContains e -> None
        | MethodCall(m, null, [ innerSeq; keyExpr ]) when m.Name = "Contains" -> matchSeqAndKey innerSeq keyExpr
        | MethodCall(m, innerSeq, [ keyExpr ]) when m.Name = "Contains" -> matchSeqAndKey innerSeq keyExpr
        | _ -> None

    // Pattern 3: AggReductionCall (Count, Sum, Avg, Min, Max, etc.)
    let tryMatchAggCall (expr: Expression) : AggReductionCall option =
        let unwrapClean (e: Expression) =
            match ExpressionPatterns.unwrap e with
            | :? MethodCallExpression as mc -> mc
            | _ -> null

        let mc = unwrapClean expr

        if isNull mc || not (isAggReductionMethod mc.Method.Name) then
            None
        else
            let methodName = mc.Method.Name
            let isCount = methodName = "Count" || methodName = "LongCount"

            let extractSeqAndSelector seqExpr =
                match ExpressionPatterns.unwrap seqExpr with
                | MethodCall(mSel, null, [ src; CleanLambda selLam ]) when mSel.Name = "Select" -> src, Some selLam
                | MethodCall(mSel, src, [ CleanLambda selLam ]) when mSel.Name = "Select" -> src, Some selLam
                | other -> other, None

            match mc.Object, mc.Arguments |> Seq.toList with
            | null, [ innerSeq ] ->
                let src, selLamOpt = extractSeqAndSelector innerSeq

                Some
                    {
                        InnerSeq = src
                        MethodName = methodName
                        TargetType = mc.Type
                        SelectorLambdaOpt = selLamOpt
                        ExtraWhereLambdaOpt = None
                    }
            | null, [ innerSeq; CleanLambda lam ] ->
                Some
                    {
                        InnerSeq = innerSeq
                        MethodName = methodName
                        TargetType = mc.Type
                        SelectorLambdaOpt = if isCount then None else Some lam
                        ExtraWhereLambdaOpt = if isCount then Some lam else None
                    }
            | target, [] when not (isNull target) ->
                let src, selLamOpt = extractSeqAndSelector target

                Some
                    {
                        InnerSeq = src
                        MethodName = methodName
                        TargetType = mc.Type
                        SelectorLambdaOpt = selLamOpt
                        ExtraWhereLambdaOpt = None
                    }
            | target, [ CleanLambda lam ] when not (isNull target) ->
                Some
                    {
                        InnerSeq = target
                        MethodName = methodName
                        TargetType = mc.Type
                        SelectorLambdaOpt = if isCount then None else Some lam
                        ExtraWhereLambdaOpt = if isCount then Some lam else None
                    }
            | _ -> None


    // =========================================================================
    // 3. Normalizing rewrites
    // =========================================================================

    /// Removes `DefaultIfEmpty` under an aggregate, which the decorrelator cannot read, using
    /// `Agg(s.DefaultIfEmpty(d)) = s is empty ? Agg([d]) : Agg(s)`:
    /// - T a non-nullable value type: Max / Min / Average become `s.Select(x => (T?)x).Agg() ?? (R)d` (an
    ///   aggregate over a nullable sequence is null exactly when it is empty); Sum with the default d is Sum(s).
    /// - T nullable or a reference type, with no d (or a null d): the aggregate of `[null]` is that of an empty
    ///   sequence, so the call is Agg(s).
    /// Other shapes (a selector on the aggregate, a non-default d for Sum, Count) are left as they are.
    let rewriteDefaultIfEmptyAggs (body: Expression) : Expression =
        let isStaticLinq (m: MethodInfo) =
            m.DeclaringType = typeof<Enumerable> || m.DeclaringType = typeof<Queryable>
        let isNullConstant (e: Expression) =
            match ExpressionPatterns.unwrap e with
            | :? ConstantExpression as c -> isNull c.Value
            | _ -> false
        let isDefaultConstant (t: Type) (e: Expression) =
            match ExpressionPatterns.unwrap e with
            | :? ConstantExpression as c -> obj.Equals(c.Value, (if t.IsValueType then Activator.CreateInstance t else null))
            | _ -> false
        let enumerableOf (t: Type) = typedefof<IEnumerable<_>>.MakeGenericType t
        /// `src.Select(x => (T?)x)`, folded into src's own trailing Select.
        let liftToNullable (src: Expression) (t: Type) (nullableT: Type) : Expression =
            let select = (typeof<Enumerable>.GetMethods() |> Array.find (fun mi -> mi.Name = "Select" && mi.GetParameters().[1].ParameterType.GetGenericArguments().Length = 2))
            match ExpressionPatterns.unwrap src with
            | MethodCall(mSel, null, [ s0; StripQuotes(:? LambdaExpression as lam) ]) when mSel.Name = "Select" && lam.Parameters.Count = 1 && isStaticLinq mSel ->
                let srcElem = lam.Parameters.[0].Type
                let lifted = Expression.Lambda(Expression.Convert(lam.Body, nullableT), lam.Parameters)
                Expression.Call(select.MakeGenericMethod(srcElem, nullableT), s0, lifted) :> Expression
            | _ ->
                let x = Expression.Parameter(t, "x")
                Expression.Call(select.MakeGenericMethod(t, nullableT), src, Expression.Lambda(Expression.Convert(x, nullableT), x)) :> Expression
        let aggOverNullable (name: string) (nullableT: Type) : MethodInfo option =
            let candidates = typeof<Enumerable>.GetMethods() |> Array.filter (fun mi -> mi.Name = name && mi.GetParameters().Length = 1)
            match candidates |> Array.tryFind (fun mi -> not mi.IsGenericMethodDefinition && mi.GetParameters().[0].ParameterType = enumerableOf nullableT) with
            | Some mi -> Some mi
            | None when name = "Max" || name = "Min" ->
                candidates
                |> Array.tryFind (fun mi -> mi.IsGenericMethodDefinition && mi.GetGenericArguments().Length = 1)
                |> Option.map (fun mi -> mi.MakeGenericMethod nullableT)
            | None -> None
        { new ExpressionVisitor() with
            override this.VisitMethodCall(node: MethodCallExpression) =
                let visited = base.VisitMethodCall node
                match visited with
                | MethodCall(agg, null, [ dieCall ]) when isStaticLinq agg && (agg.Name = "Max" || agg.Name = "Min" || agg.Name = "Average" || agg.Name = "Sum") ->
                    match ExpressionPatterns.unwrap dieCall with
                    | MethodCall(die, null, src :: rest) when die.Name = "DefaultIfEmpty" && isStaticLinq die && rest.Length <= 1 ->
                        let t = die.GetGenericArguments().[0]
                        let dOpt = List.tryHead rest
                        let stripped () =
                            let m =
                                if agg.IsGenericMethod then agg.GetGenericMethodDefinition().MakeGenericMethod(agg.GetGenericArguments())
                                else agg
                            let pt = m.GetParameters().[0].ParameterType
                            Expression.Call(m, (if pt = src.Type then src else Expression.Convert(src, pt) :> Expression)) :> Expression
                        if t.IsValueType && isNull (Nullable.GetUnderlyingType t) then
                            if agg.Name = "Sum" then
                                match dOpt with
                                | None -> stripped ()
                                | Some d when isDefaultConstant t d -> stripped ()
                                | Some _ -> visited
                            else
                                let nullableT = typedefof<Nullable<_>>.MakeGenericType t
                                match aggOverNullable agg.Name nullableT with
                                | Some nullableAgg ->
                                    let lifted = Expression.Call(nullableAgg, liftToNullable src t nullableT)
                                    let d = dOpt |> Option.defaultWith (fun () -> Expression.Constant(Activator.CreateInstance t, t) :> Expression)
                                    if Nullable.GetUnderlyingType lifted.Type = visited.Type then
                                        Expression.Coalesce(lifted, Expression.Convert(d, visited.Type)) :> Expression
                                    else
                                        visited
                                | None -> visited
                        elif dOpt.IsNone || isNullConstant dOpt.Value then
                            stripped ()
                        else
                            visited
                    | _ -> visited
                | _ -> visited
        }.Visit body

    /// `seq.All(p)` as `!seq.Any(x => !p(x))`, which decorrelates as an anti join or a negated match count.
    let rewriteAllAsNotAny (body: Expression) : Expression =
        // `!(x != y)` is `x == y` and `!(x == y)` is `x != y` under C# equality (two-valued, NaN unequal);
        // `!!x` is `x`. An ordering comparison is not inverted (NaN makes both sides false).
        let negate (e: Expression) : Expression =
            match e with
            | :? UnaryExpression as u when u.NodeType = ExpressionType.Not && u.Type = typeof<bool> -> u.Operand
            | :? BinaryExpression as b when b.NodeType = ExpressionType.NotEqual && b.Type = typeof<bool> ->
                Expression.Equal(b.Left, b.Right, b.IsLiftedToNull, b.Method) :> Expression
            | :? BinaryExpression as b when b.NodeType = ExpressionType.Equal && b.Type = typeof<bool> ->
                Expression.NotEqual(b.Left, b.Right, b.IsLiftedToNull, b.Method) :> Expression
            | _ -> Expression.Not e :> Expression
        { new ExpressionVisitor() with
            override this.VisitUnary(node: UnaryExpression) =
                // Collapses the `!!` an `!All(..)` leaves.
                match base.VisitUnary node with
                | :? UnaryExpression as u when
                    u.NodeType = ExpressionType.Not
                    && u.Type = typeof<bool>
                    && u.Operand.NodeType = ExpressionType.Not
                    && u.Operand.Type = typeof<bool>
                    ->
                    (u.Operand :?> UnaryExpression).Operand
                | other -> other
            override this.VisitMethodCall(node: MethodCallExpression) =
                let visited = base.VisitMethodCall node
                match visited with
                | :? MethodCallExpression as mc when
                    mc.Method.Name = "All"
                    && isNull mc.Object
                    && mc.Arguments.Count = 2
                    && (mc.Method.DeclaringType = typeof<Enumerable> || mc.Method.DeclaringType = typeof<Queryable>)
                    ->
                    let source = mc.Arguments.[0]
                    let isQueryable = mc.Method.DeclaringType = typeof<Queryable>
                    match ExpressionPatterns.unwrap mc.Arguments.[1] with
                    | :? LambdaExpression as pred ->
                        let negated = Expression.Lambda(pred.Type, negate pred.Body, pred.Parameters)
                        let elementType = pred.Parameters.[0].Type
                        let predicateArg = if isQueryable then Expression.Quote negated :> Expression else negated :> Expression
                        let any =
                            Expression.Call((if isQueryable then typeof<Queryable> else typeof<Enumerable>), "Any", [| elementType |], source, predicateArg)
                        Expression.Not any :> Expression
                    | _ -> visited
                | other -> other
        }.Visit body
