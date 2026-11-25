namespace Kaminari.Samidare

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open Kaminari.Samidare.IR
open Kaminari.Sazanami.Marshalling

/// LINQ subqueries as IR subqueries (SEMANTICS.md, Subqueries).
///
/// A subquery is a query operator over a source read inside a lambda: `Any`, `All`, `Contains`, an aggregate,
/// `ToList` / `ToArray`, or an element (`First`, `Single`, ...). Its sequence, with its predicate and selector
/// folded in as `Where` / `Select`, compiles like any query in a nested compilation, during which the lambda
/// parameters of the enclosing query are bound to the subquery's scope: an expression reading only them is
/// translated in the enclosing context and its columns become OuterColumns. The operator then becomes the
/// subquery's kind: Exists, In, or a Scalar over an aggregate. The Decorrelation pass replaces it by joins.
module internal SubqueryTranslator =

    /// The names of the lambda parameters an expression reads and does not declare itself.
    let private freeParameters (expr: Expression) : Set<string> =
        let read = HashSet<string>()
        let declared = HashSet<string>()
        { new ExpressionVisitor() with
            override _.VisitParameter(node: ParameterExpression) =
                if not (isNull node.Name) then read.Add node.Name |> ignore
                node :> Expression
            override this.VisitLambda<'T>(node: Expression<'T>) =
                for p in node.Parameters do
                    if not (isNull p.Name) then declared.Add p.Name |> ignore
                base.VisitLambda node
        }.Visit expr
        |> ignore
        read.ExceptWith declared
        Set.ofSeq read

    /// An expression that reads only lambda parameters of enclosing queries (of one subquery scope), translated
    /// in the enclosing context with its columns as that scope's OuterColumns.
    let tryTranslateOuterReference (eval: ScalarExprEvaluator) (ctx: TranslationContext) (expr: Expression) : ScalarExpr option =
        match CompilationScope.TryCurrent with
        | Some scope when scope.ReadsOuterRows ->
            let free = freeParameters expr
            if free.IsEmpty || free |> Set.exists ctx.ContainsParam then
                None
            else
                match free |> Set.toList |> List.map scope.TryFindOuterBinding with
                | Some binding :: rest when rest |> List.forall (function Some b -> b.Scope = binding.Scope | None -> false) ->
                    let outerCtx = binding.Context :?> TranslationContext
                    eval outerCtx expr
                    |> Option.map (
                        ScalarExprOps.substitute (fun name ->
                            let dataType =
                                binding.Schema.TryFindField name |> Option.map (fun f -> f.DataType) |> Option.defaultValue LogicalType.Unknown
                            Some(ScalarExpr.OuterColumn(binding.Scope, name, dataType)))
                    )
                | _ -> None
        | _ -> None

    let private elementType (seq: Expression) : Type =
        ClrTypeHelper.TryGetEnumerableElementType seq.Type

    /// The subquery with every lambda parameter it declares under the name of an enclosing parameter (of the
    /// enclosing context, or of a subquery around it) renamed apart: the translators resolve parameters by name, so
    /// inside a subquery a name then stands for one parameter only (C# query syntax, for instance, names the
    /// transparent identifiers of an outer and an inner `let` alike). The new name keeps the old one as its prefix,
    /// which the resolvers read (`<>h__TransparentIdentifier`, F#'s `_arg`, `tupledArg`).
    let private renameApart (ctx: TranslationContext) (expr: Expression) : Expression =
        let scope = CompilationScope.Current
        let taken (name: string) =
            not (isNull name) && (ctx.ContainsParam name || (scope.TryFindOuterBinding name).IsSome)
        let renames = Dictionary<ParameterExpression, ParameterExpression>(HashIdentity.Reference)
        { new ExpressionVisitor() with
            override this.VisitLambda<'T>(node: Expression<'T>) =
                for p in node.Parameters do
                    if taken p.Name && not (renames.ContainsKey p) then
                        renames.[p] <- Expression.Parameter(p.Type, $"{p.Name}__sq{scope.NextId()}")
                if node.Parameters |> Seq.exists renames.ContainsKey then
                    let parameters = node.Parameters |> Seq.map (fun p -> match renames.TryGetValue p with | true, r -> r | _ -> p)
                    Expression.Lambda(node.Type, this.Visit node.Body, parameters) :> Expression
                else
                    base.VisitLambda node
            override _.VisitParameter(node: ParameterExpression) =
                match renames.TryGetValue node with
                | true, renamed -> renamed :> Expression
                | _ -> node :> Expression
        }.Visit expr

    /// Whether the expression is worth the full subquery test: a static query operator call, possibly under
    /// member reads (`seq.Single(p).Value`).
    let rec private looksLikeSubquery (expr: Expression) : bool =
        match ExpressionPatterns.unwrap expr with
        | :? MethodCallExpression as mc ->
            mc.Method.DeclaringType = typeof<Queryable> || mc.Method.DeclaringType = typeof<Enumerable>
        | :? MemberExpression as me when not (isNull me.Expression) -> looksLikeSubquery me.Expression
        | _ -> false

    /// Runs `f` while the enclosing lambda parameters the expressions read are bound to the subquery's scope.
    let private bound (ctx: TranslationContext) (scopeId: int) (exprs: Expression list) (f: unit -> 'T) : 'T =
        let scope = CompilationScope.Current
        let bindings =
            exprs
            |> List.map freeParameters
            |> Set.unionMany
            |> Set.filter ctx.ContainsParam
            |> Set.toList
            |> List.map (fun name -> { Name = name; Scope = scopeId; Context = box ctx; Schema = LogicalSchema.Empty })
        let saved = scope.OuterBindings
        scope.OuterBindings <- bindings @ saved
        try
            f ()
        finally
            scope.OuterBindings <- saved

    let private columnsOf (plan: LogicalPlan) = (SchemaInference.inferPlanSchema plan).ColumnNames

    let private singleColumn (plan: LogicalPlan) : string option =
        match columnsOf plan with
        | [ name ] -> Some name
        | _ -> None

    /// A lambda over the sequence's element, translated over the sequence's plan (its reads of the enclosing row
    /// become OuterColumns).
    let private overElement (eval: ScalarExprEvaluator) (ctx: TranslationContext) (scopeId: int) (plan: LogicalPlan) (lam: LambdaExpression) =
        let p = lam.Parameters.[0]
        let elementCtx = SubqueryPatterns.createContextForTypeWithCols p.Type (columnsOf plan) p.Name
        bound ctx scopeId [ lam ] (fun () -> eval elementCtx lam.Body)

    /// A chain of Where calls over a source: the source and the predicates, innermost first.
    let rec private peelWheres (e: Expression) : Expression * LambdaExpression list =
        match ExpressionPatterns.unwrap e with
        | MethodCall(m, null, [ src; CleanLambda lam ]) when m.Name = "Where" && lam.Parameters.Count = 1 ->
            let root, lams = peelWheres src
            root, lams @ [ lam ]
        | _ -> e, []

    /// The sequence's plan filtered by the predicate. A chain of Where
    /// calls over a source that reads no enclosing row resolves the source alone (an uncorrelated query, cached)
    /// and translates each predicate over it; another sequence compiles whole with the enclosing parameters it
    /// reads bound (correlation under other operators, which the Decorrelation pass then reports).
    let private rowsOf (eval: ScalarExprEvaluator) (ctx: TranslationContext) (scopeId: int) (seq: Expression) (predOpt: LambdaExpression option) =
        match CompilationScope.Current.ResolveSequence with
        | None -> None
        | Some resolve ->
            let root, lams = peelWheres seq
            let fromRoot =
                if (freeParameters root).IsEmpty then
                    resolve root
                    |> Option.bind (fun plan ->
                        lams
                        |> List.fold
                            (fun acc lam ->
                                acc |> Option.bind (fun p -> overElement eval ctx scopeId p lam |> Option.map (fun pred -> LogicalPlan.Filter(p, pred))))
                            (Some plan))
                else
                    None
            fromRoot
            |> Option.orElseWith (fun () -> bound ctx scopeId [ seq ] (fun () -> resolve seq))
            |> Option.bind (fun plan ->
                match predOpt with
                | None -> Some plan
                | Some pred -> overElement eval ctx scopeId plan pred |> Option.map (fun p -> LogicalPlan.Filter(plan, p)))

    /// The value a selector (or the element itself, for a one-column sequence) gives over the plan's rows.
    let private valueOf (eval: ScalarExprEvaluator) (ctx: TranslationContext) (scopeId: int) (plan: LogicalPlan) (selOpt: LambdaExpression option) =
        match selOpt with
        | Some sel -> overElement eval ctx scopeId plan sel
        | None -> singleColumn plan |> Option.map ScalarExpr.Column

    /// First / Last / Single / ElementAt (and their OrDefault forms), with the members read from the element: the
    /// element of a one-row aggregate over the first rows, guarded by a count check where LINQ throws.
    let private tryElement (eval: ScalarExprEvaluator) (ctx: TranslationContext) (node: Expression) : ScalarExpr option =
        let isElementOp (name: string) =
            match name with
            | "First" | "FirstOrDefault" | "Last" | "LastOrDefault" | "Single" | "SingleOrDefault" | "ElementAt"
            | "ElementAtOrDefault" -> true
            | _ -> false
        let rec peel (e: Expression) (path: Reflection.MemberInfo list) =
            match ExpressionPatterns.unwrap e with
            | :? MemberExpression as me when not (isNull me.Expression) -> peel me.Expression (me.Member :: path)
            | :? MethodCallExpression as mc when isElementOp mc.Method.Name -> Some(mc, path)
            | _ -> None
        match peel node [] with
        | None -> None
        | Some(mc, path) ->
            let name = mc.Method.Name
            let isOrDefault = name.EndsWith "OrDefault"
            let baseName = if isOrDefault then name.Substring(0, name.Length - "OrDefault".Length) else name
            let seqExpr, predOpt, indexOpt =
                match baseName, mc.Object, List.ofSeq mc.Arguments with
                | ("First" | "Last" | "Single"), null, [ s ] -> Some s, None, None
                | ("First" | "Last" | "Single"), null, [ s; CleanLambda p ] -> Some s, Some p, None
                | "ElementAt", null, [ s; i ] when i.Type = typeof<int> ->
                    match PartialEvaluator.tryEvaluate i with
                    | Some(:? int as n) -> Some s, None, Some n
                    | _ -> None, None, None
                | _ -> None, None, None
            let noElements = if predOpt.IsSome then "Sequence contains no matching element" else "Sequence contains no elements"
            let manyElements = if predOpt.IsSome then "Sequence contains more than one matching element" else "Sequence contains more than one element"
            let invalid msg = { Kind = QueryErrorKind.InvalidOperation; Message = msg }
            let atLeastOne error = { Min = 1L; Max = None; TooFew = error; TooMany = error }
            let window, check =
                match baseName, indexOpt with
                | "First", _ -> Some(Some(0L, 1u), AggKind.First), (if isOrDefault then None else Some(atLeastOne (invalid noElements)))
                | "Last", _ -> Some(None, AggKind.Last), (if isOrDefault then None else Some(atLeastOne (invalid noElements)))
                | "Single", _ ->
                    Some(Some(0L, 2u), AggKind.First),
                    Some { Min = (if isOrDefault then 0L else 1L); Max = Some 1L; TooFew = invalid noElements; TooMany = invalid manyElements }
                | "ElementAt", Some n when n >= 0 ->
                    let outOfRange = { Kind = QueryErrorKind.ArgumentOutOfRange; Message = "" }
                    Some(Some(int64 n, 1u), AggKind.First), (if isOrDefault then None else Some(atLeastOne outOfRange))
                | _ -> None, None
            match seqExpr, window with
            | Some s, Some(slice, pick) when check.IsNone || CompilationScope.Current.Backend.SupportsAssertions ->
                let scopeId = CompilationScope.Current.NextId()
                rowsOf eval ctx scopeId s predOpt
                |> Option.bind (fun plan ->
                    let selector =
                        if path.IsEmpty then
                            None
                        else
                            let p = Expression.Parameter(elementType s, "e")
                            let body = path |> List.fold (fun acc m -> Expression.MakeMemberAccess(acc, m) :> Expression) (p :> Expression)
                            Some(Expression.Lambda(body, p))
                    valueOf eval ctx scopeId plan selector
                    |> Option.map (fun value ->
                        let column = "value"
                        let projected = LogicalPlan.Project(plan, [ (column, value) ])
                        let rows =
                            match slice with
                            | Some(offset, count) -> LogicalPlan.Limit(projected, offset, count)
                            | None -> projected
                        let picked = ScalarExpr.Aggregation(pick, Some(ScalarExpr.Column column), false)
                        // An OrDefault over no rows is default(T): null, or the zero of a non-nullable value type.
                        let element =
                            if isOrDefault && path.IsEmpty && mc.Type.IsValueType && isNull (Nullable.GetUnderlyingType mc.Type) then
                                ScalarExpr.Coalesce [ picked; ScalarExprOps.toLiteral (Activator.CreateInstance mc.Type) mc.Type ]
                            else
                                picked
                        let guarded =
                            match check with
                            | Some c ->
                                ScalarExpr.Function(ScalarFunction.AssertCount c, [ ScalarExpr.Aggregation(AggKind.Len, None, false); element ], LogicalType.Unknown)
                            | None -> element
                        ScalarExpr.Subquery(scopeId, LogicalPlan.Project(rows, [ (column, guarded) ]), SubqueryKind.Scalar)))
            | _ -> None

    /// An uncorrelated subquery the plan cannot hold (a Single on a backend that cannot raise, an operator with no
    /// plan form such as Aggregate), evaluated while compiling: its value becomes a literal, and the compilation is
    /// marked so that its plan is not cached.
    let private tryEvaluateAtCompile (expr: Expression) : ScalarExpr option =
        if not (freeParameters expr).IsEmpty || not (ClrTypeHelper.IsScalarType expr.Type || expr.Type = typeof<string>) then
            None
        else
            try
                let value = Expression.Lambda<Func<obj>>(Expression.Convert(expr, typeof<obj>)).Compile().Invoke()
                CompilationScope.Current.EvaluatedAtCompile <- true
                if isNull value then
                    Some(ScalarExpr.Literal(LiteralValue.Null(LogicalTypes.tryOfClrType expr.Type |> Option.defaultValue LogicalTypes.Null)))
                else
                    Some(ScalarExprOps.toLiteral value expr.Type)
            with _ ->
                None

    /// A subquery expression as an IR subquery; None when the expression is not one or its sequence does not
    /// compile natively.
    let rec tryTranslateSubquery (eval: ScalarExprEvaluator) (ctx: TranslationContext) (expr: Expression) : ScalarExpr option =
        if not (CompilationScope.TryCurrent |> Option.exists (fun s -> s.AllowSubqueries))
           || not (looksLikeSubquery expr)
           || not (SubqueryPatterns.containsSubquery expr)
           || SubqueryPatterns.isInMemoryScalarContains expr
        then
            None
        else
            let expr = renameApart ctx expr
            // All and DefaultIfEmpty aggregates are first rewritten to the shapes below.
            let rewritten = SubqueryPatterns.rewriteAllAsNotAny (SubqueryPatterns.rewriteDefaultIfEmptyAggs expr)
            if not (obj.ReferenceEquals(rewritten, expr)) then
                eval ctx rewritten
            else
            match translateShapes eval ctx expr with
            | Some s -> Some s
            | None -> tryEvaluateAtCompile expr

    /// The subquery shapes with a plan form.
    and private translateShapes (eval: ScalarExprEvaluator) (ctx: TranslationContext) (expr: Expression) : ScalarExpr option =
                match tryElement eval ctx expr with
                | Some s -> Some s
                | None ->
                match SubqueryPatterns.tryMatchAny (ExpressionPatterns.unwrap expr) with
                | Some(seq, predOpt) ->
                    let scopeId = CompilationScope.Current.NextId()
                    rowsOf eval ctx scopeId seq predOpt
                    |> Option.map (fun plan -> ScalarExpr.Subquery(scopeId, plan, SubqueryKind.Exists))
                | None ->
                match SubqueryPatterns.tryMatchContains (ExpressionPatterns.unwrap expr) with
                | Some(seq, selOpt, key) ->
                    match eval ctx key with
                    | Some target ->
                        let scopeId = CompilationScope.Current.NextId()
                        rowsOf eval ctx scopeId seq None
                        |> Option.bind (fun plan ->
                            valueOf eval ctx scopeId plan selOpt
                            |> Option.map (fun value ->
                                let values = LogicalPlan.Project(plan, [ ("value", value) ])
                                ScalarExpr.Subquery(scopeId, values, SubqueryKind.In(target, NullSemantics.nullKeysMatch ()))))
                    | None -> None
                | None ->
                match SubqueryPatterns.tryMatchAggCall expr with
                | Some agg ->
                    let scopeId = CompilationScope.Current.NextId()
                    rowsOf eval ctx scopeId agg.InnerSeq agg.ExtraWhereLambdaOpt
                    |> Option.bind (fun plan ->
                        let over kind =
                            valueOf eval ctx scopeId plan agg.SelectorLambdaOpt
                            |> Option.map (fun v -> ScalarExpr.Aggregation(kind, Some v, false))
                        let aggregate =
                            match agg.MethodName with
                            | "Count" | "LongCount" -> Some(ScalarExpr.Aggregation(AggKind.Len, None, false))
                            | "Sum" -> over AggKind.Sum
                            | "Average" | "Mean" -> over AggKind.Mean
                            | "Min" -> over AggKind.Min
                            | "Max" -> over AggKind.Max
                            | "Median" -> over AggKind.Median
                            | "ToList" | "ToArray" -> over AggKind.List
                            | _ -> None
                        aggregate |> Option.map (fun a -> ScalarExpr.Subquery(scopeId, LogicalPlan.Project(plan, [ ("value", a) ]), SubqueryKind.Scalar)))
                | None -> None
