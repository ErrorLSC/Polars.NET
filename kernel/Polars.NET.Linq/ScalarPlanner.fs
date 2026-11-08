namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core.Helpers
open Polars.NET.Linq.IR

/// Managed operation applied by the backend to the rows of a collected plan.
[<RequireQualifiedAccess>]
type internal RowsOp =
    /// Row count, optionally filtered by a managed predicate. Yields int64.
    | Count of sourceType: Type * predicate: Delegate
    /// "AnyPredicate" / "AllPredicate" over a managed predicate. Yields bool.
    | BoolPredicate of methodName: string * sourceType: Type * predicate: Delegate
    /// Any(), optionally with a managed predicate. Yields bool.
    | Any of sourceType: Type * predicate: Delegate
    /// First / Last / Single (OrDefault) row fetch. Yields the raw entity.
    | RowFetch of kindName: string * isOrDefault: bool * targetType: Type * predicate: Delegate * defaultValue: obj
    /// Managed numeric fold for Sum / Min / Max / Average. Yields NoRows on an empty input.
    | NumericFold of sourceType: Type * accumType: Type * methodName: string * selector: Delegate option
    /// Row at a fixed position of an already sliced plan. Yields NoRows on an empty input.
    | ElementAt of targetType: Type * index: int64
    /// Managed Contains(item, comparer). Yields bool.
    | Contains of elemType: Type * item: obj * comparer: obj
    /// First row of an already sorted and sliced plan. Yields NoRows on an empty input.
    | FirstRow of elemType: Type
    /// Managed MinBy / MaxBy. Yields the raw winner entity.
    | ExtremumBy of methodName: string * sourceType: Type * keyType: Type * keySelector: Delegate * comparer: obj
    /// Managed SequenceEqual against another plan. Yields bool.
    | SequenceEqualPlan of other: LogicalPlan * elemType: Type * comparer: obj
    /// Managed SequenceEqual against an in-memory sequence. Yields bool.
    | SequenceEqualSeq of other: IEnumerable * elemType: Type * comparer: obj

/// Declarative description of how a backend executes a scalar / single-row LINQ terminal.
[<RequireQualifiedAccess>]
type internal ScalarTerminal =
    /// Collects a single-cell row-count plan and reads it as int64.
    | RowCount of plan: LogicalPlan
    /// Collects a single-cell plan and reads the boxed value.
    | ScalarValue of plan: LogicalPlan
    /// Collects the plan (already limited to at most one row) and reports whether any row exists.
    | HasRows of plan: LogicalPlan
    /// Collects both plans and compares them for full frame equality (height first).
    | FramesEqual of left: LogicalPlan * right: LogicalPlan
    /// Collects the plan and applies a managed operation to its rows.
    | Rows of plan: LogicalPlan * op: RowsOp
    /// Pure managed computation that does not consume a native plan directly.
    | Deferred of compute: (unit -> obj)
    /// Result known at planning time.
    | Constant of value: obj

/// Raw outcome of running a ScalarTerminal, before managed post-processing.
[<RequireQualifiedAccess>]
type internal ScalarOutcome =
    | Value of obj
    | NoRows

/// A planned scalar terminal: what the backend executes and how its raw outcome becomes the LINQ result.
type internal ScalarQuery<'T> =
    {
        Terminal: ScalarTerminal
        Finish: ScalarOutcome -> 'T
    }

/// A planned scalar terminal together with the sources its plans scan, indexed by slot.
type internal PlannedScalar<'T> =
    {
        Query: ScalarQuery<'T>
        Sources: SourceBinding array
    }

/// Resolved predicate: native pushdown as a symbolic ScalarExpr, or a composed managed predicate.
type internal ResolvedPredicate =
    | Pushdown of ScalarExpr
    | Client of effectiveType: Type * pred: Delegate

/// Compiled source sequence of a scalar terminal.
type internal ScalarSource =
    {
        Plan: LogicalPlan
        /// True when the source has client predicates or requires full client fallback. Such sources are
        /// evaluated with LINQ-to-Objects over the enumerated rows and never reach the native terminal paths.
        RequiresClientEvaluation: bool
        ClientProjection: (Type * Func<obj, obj>) option
        /// True when the plan's rows are a transparent identifier whose entities are still flat columns
        /// (CompiledLogicalQuery.RowShape): its rows are read through the enumerated source, which packs them.
        HasRowScopes: bool
    }

/// Pure planner for LINQ scalar & single-row terminal operations
/// (Count, LongCount, Any, All, First*, Last*, Single*, Min, Max, Sum, Average, ElementAt*, Contains,
/// SequenceEqual, MinBy, MaxBy, Aggregate). Produces a backend-agnostic ScalarQuery; no native resources are touched.
module internal ScalarPlanner =

    /// Checks whether a LINQ method is a scalar or single-element terminal reduction.
    let isScalarReductionMethod (m: MethodInfo) : bool =
        match m.Name with
        | "Count"
        | "LongCount"
        | "Sum"
        | "Average"
        | "Min"
        | "Max"
        | "First"
        | "FirstOrDefault"
        | "Single"
        | "SingleOrDefault"
        | "Last"
        | "LastOrDefault"
        | "Any"
        | "All"
        | "Contains"
        | "ElementAt"
        | "ElementAtOrDefault"
        | "SequenceEqual"
        | "MinBy"
        | "MaxBy"
        | "Aggregate" -> true
        | _ -> false

    let private planColumns (plan: LogicalPlan) : string list =
        (SchemaInference.inferPlanSchema plan).ColumnNames

    /// Wraps a plan so that collecting it yields at most one row.
    let private firstRowOf (plan: LogicalPlan) : LogicalPlan = LogicalPlan.Limit(plan, 0L, 1u)

    let private filterOpt (plan: LogicalPlan) (predOpt: ScalarExpr option) : LogicalPlan =
        match predOpt with
        | Some p -> LogicalPlan.Filter(plan, p)
        | None -> plan

    /// Resolves the predicate into either native ScalarExpr pushdown or a composed client Func<obj, bool>.
    let private tryResolvePredicate
        (src: ScalarSource)
        (elemType: Type)
        (predLambdaOpt: LambdaExpression option)
        : ResolvedPredicate voption =
        match predLambdaOpt with
        | None -> ValueNone
        | Some predLambda ->
            let nativeFilterOpt =
                if src.ClientProjection.IsNone then
                    let ctx = SubqueryDecorrelator.createContextForType elemType src.Plan predLambda.Parameters.[0].Name
                    tryTranslateToScalar ctx predLambda.Body
                else
                    None

            match nativeFilterOpt with
            | Some filterExpr -> ValueSome(Pushdown filterExpr)
            | None ->
                let compiledPred = predLambda.Compile()
                let predArgType = predLambda.Parameters.[0].Type

                match src.ClientProjection with
                | Some(sourceType, projFunc) ->
                    let combinedPred =
                        LinqReflectionCache.CreateComposedPredicate(sourceType, projFunc, compiledPred, predArgType)
                    ValueSome(Client(sourceType, combinedPred))
                | None -> ValueSome(Client(predArgType, compiledPred))

    /// Maps standard reduction method names to symbolic aggregate expressions.
    let private resolveScalarAggExpr (methodName: string) (colExpr: ScalarExpr) : ScalarExpr =
        match methodName with
        | "Max" -> ScalarExpr.Aggregation(AggKind.Max, Some colExpr, false)
        | "Min" -> ScalarExpr.Aggregation(AggKind.Min, Some colExpr, false)
        | "Sum" -> ScalarExpr.Aggregation(AggKind.Sum, Some colExpr, false)
        | "Average" -> ScalarExpr.Aggregation(AggKind.Mean, Some colExpr, false)
        | _ -> ScalarExpr.Aggregation(AggKind.Len, None, false)

    /// Builds a symbolic equality filter matching an item (scalar primitive or composite entity/record)
    /// against the columns of the target plan.
    let private buildItemEqualityFilter (itemVal: obj) (targetPlan: LogicalPlan) : ScalarExpr option =
        if isNull itemVal then
            None
        else
            let itemType = itemVal.GetType()

            if PolarsTypeHelper.IsScalarType itemType then
                let targetCol =
                    match planColumns targetPlan with
                    | first :: _ -> first
                    | [] -> ""
                Some(ScalarExpr.Binary(BinaryOp.Eq, ScalarExpr.Column targetCol, ScalarExprOps.toLiteral itemVal itemType))
            else
                let conditions =
                    itemType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)
                    |> Array.choose (fun p ->
                        let v = p.GetValue itemVal

                        if isNull v then
                            None
                        else
                            ScalarExprOps.tryToLiteral v p.PropertyType
                            |> Option.map (fun literal -> ScalarExpr.Binary(BinaryOp.Eq, ScalarExpr.Column p.Name, literal)))

                if conditions.Length > 0 then
                    Some(conditions |> Array.reduce (fun acc c -> ScalarExpr.Binary(BinaryOp.And, acc, c)))
                else
                    None

    /// Extracts the effective source element type considering client projections.
    let private getEffectiveSourceType (clientProjOpt: (Type * Func<obj, obj>) option) (elemType: Type) : Type =
        clientProjOpt |> Option.map fst |> Option.defaultValue elemType

    /// Extracts the optional comparer instance from arguments if supplied.
    let private tryExtractComparer (rest: Expression list) : obj =
        match rest with
        | [ comparerExpr ] -> PartialEvaluator.tryEvaluate comparerExpr |> Option.toObj
        | _ -> null

    /// Projects a raw materializer result object through client projection, applying default fallback for null if needed.
    let private projectClientResult<'T>
        (clientProjOpt: (Type * Func<obj, obj>) option)
        (defaultVal: obj)
        (rawEntity: obj)
        : 'T =
        if isNull rawEntity && PolarsTypeHelper.AcceptsNull typeof<'T> then
            defaultVal :?> 'T
        else
            match clientProjOpt with
            | Some(_, projFunc) -> projFunc.Invoke rawEntity :?> 'T
            | None -> rawEntity :?> 'T

    /// Composes a client-side projection Func<obj, obj> with a downstream typed LambdaExpression selector,
    /// producing a single compiled Delegate of type Func<TSource, TReturn>.
    let private composeClientSelector
        (sourceType: Type)
        (elemType: Type)
        (projFunc: Func<obj, obj>)
        (selector: LambdaExpression)
        : Delegate =
        let compiledSel = selector.Compile()
        let p = Expression.Parameter(sourceType, "src")
        let castObj = Expression.Convert(p, typeof<obj>)
        let projCall = Expression.Invoke(Expression.Constant(projFunc), castObj)
        let castElem = Expression.Convert(projCall, elemType)
        let selCall = Expression.Invoke(Expression.Constant(compiledSel), castElem)
        let lambdaType = typedefof<Func<_, _>>.MakeGenericType(sourceType, selector.ReturnType)
        Expression.Lambda(lambdaType, selCall, p).Compile()

    let private valueOf (outcome: ScalarOutcome) : obj =
        match outcome with
        | ScalarOutcome.Value v -> v
        | ScalarOutcome.NoRows -> null

    let private castFinish<'T> : ScalarOutcome -> 'T = fun outcome -> valueOf outcome :?> 'T

    /// Plans First / Last / Single (and their OrDefault variants) over a compiled source.
    let private planRowFetch<'TResult>
        (kind: RowFetchKind)
        (isOrDefault: bool)
        (userDefValOpt: obj option)
        (src: ScalarSource)
        (elemType: Type)
        (predLambdaOpt: LambdaExpression option)
        : ScalarQuery<'TResult> =

        let resolvedPred = tryResolvePredicate src elemType predLambdaOpt
        let userDefVal = defaultArg userDefValOpt (PolarsTypeHelper.GetDefaultValue elemType)

        let offset, len, kindName =
            match kind with
            | RowFetchKind.First -> 0L, 1u, "First"
            | RowFetchKind.Last -> -1L, 1u, "Last"
            | RowFetchKind.Single -> 0L, 2u, "Single"

        let fetch targetType pred defVal = RowsOp.RowFetch(kindName, isOrDefault, targetType, pred, defVal)

        match resolvedPred with
        | ValueSome(Pushdown filterExpr) ->
            {
                Terminal = ScalarTerminal.Rows(LogicalPlan.Limit(LogicalPlan.Filter(src.Plan, filterExpr), offset, len), fetch elemType null userDefVal)
                Finish = castFinish<'TResult>
            }

        | ValueSome(Client(srcType, combinedPred)) ->
            enforceClientPredicatePolicy (kindName, predLambdaOpt)
            {
                Terminal = ScalarTerminal.Rows(src.Plan, fetch srcType combinedPred (PolarsTypeHelper.GetDefaultValue srcType))
                Finish = fun outcome -> projectClientResult<'TResult> src.ClientProjection userDefVal (valueOf outcome)
            }

        | ValueNone ->
            let targetType = getEffectiveSourceType src.ClientProjection elemType
            let plan = LogicalPlan.Limit(src.Plan, offset, len)
            let defVal = if src.ClientProjection.IsSome then PolarsTypeHelper.GetDefaultValue targetType else userDefVal
            {
                Terminal = ScalarTerminal.Rows(plan, fetch targetType null defVal)
                Finish = fun outcome -> projectClientResult<'TResult> src.ClientProjection userDefVal (valueOf outcome)
            }

    let private planWith<'TResult>
        (backend: IQueryBackend)
        (sources: SourceRegistry)
        (provider: IQueryProvider)
        (expression: Expression)
        : ScalarQuery<'TResult> =
        let targetType = typeof<'TResult>

        let sourceCache = Dictionary<Expression, ScalarSource>(HashIdentity.Reference)

        let resolveSource (src: Expression) : ScalarSource =
            match sourceCache.TryGetValue src with
            | true, cached -> cached
            | _ ->
                let resolved =
                    match provider.CreateQuery src with
                    | :? IInternalPlanSource as ps ->
                        let compiled, requiresClientEvaluation, clientProjOpt = ps.GetLogicalPlanInfo()
                        {
                            Plan = sources.Graft(compiled.Plan, compiled.Sources)
                            RequiresClientEvaluation = requiresClientEvaluation
                            ClientProjection = clientProjOpt
                            HasRowScopes = not compiled.RowShape.IsEmpty
                        }
                    | _ -> failwithf "Unable to extract a logical plan from query source: %A" src
                sourceCache.[src] <- resolved
                resolved

        /// Whether the terminal's source has client predicates or full client fallback, or reads rows whose
        /// entities are still flat join scopes (only a plain count or existence check does not). Grouping
        /// sources and Aggregate already evaluate over the enumerated source and are excluded.
        let requiresClientEvaluation (m: MethodInfo) (source: Expression) : bool =
            let elemType = getSequenceElementType source.Type
            let src = resolveSource source
            let isRowCountOnly =
                (m.Name = "Count" || m.Name = "LongCount" || m.Name = "Any") && m.GetParameters().Length = 1

            m.Name <> "Aggregate"
            && not (isGroupingType elemType)
            && (src.RequiresClientEvaluation || src.HasRowScopes && not isRowCountOnly)

        /// Evaluates the terminal with LINQ-to-Objects over the rows produced by enumerating the source query.
        /// Enumeration applies client predicates / full fallback (and the pipeline policy) exactly like ToList().
        let evaluateOverClientRows (m: MethodInfo) (source: Expression) (rest: Expression list) : ScalarQuery<'TResult> =
            let compute () : obj =
                let elemType = getSequenceElementType source.Type
                let rows = Expression.Constant(provider.CreateQuery source, typedefof<IEnumerable<_>>.MakeGenericType elemType)
                let localRows = Expression.Call(LinqReflectionCache.GetEnumerableMethod("ToList", 1, [| elemType |]), rows)
                let localQuery = Expression.Call(LinqReflectionCache.GetQueryableMethod("AsQueryable", 1, [| elemType |]), localRows)
                let terminalCall = Expression.Call(m, (localQuery :> Expression) :: rest)
                Expression.Lambda<Func<obj>>(Expression.Convert(terminalCall, typeof<obj>)).Compile().Invoke()

            { Terminal = ScalarTerminal.Deferred compute; Finish = castFinish<'TResult> }

        /// A terminal over a source whose rows are still flat join scopes, rewritten so that the native plan does
        /// the narrowing, or None to enumerate the whole source:
        /// - a predicate becomes a Where and a counted or existence terminal is planned again over it (a plain
        ///   Count / Any over such a source is native);
        /// - First / Single / Last / ElementAt read only the rows they need (Take / TakeLast / Skip), then run over
        ///   those rows;
        /// - an aggregate with a scalar selector is planned again over Select(selector), a plain native aggregate.
        let tryNarrowScopedTerminal (m: MethodInfo) (source: Expression) (rest: Expression list) : ScalarQuery<'TResult> option =
            let src = resolveSource source
            if not src.HasRowScopes || src.RequiresClientEvaluation then
                None
            else
                let elemType = getSequenceElementType source.Type
                let call (name: string) (typeArgs: Type array) (args: Expression list) : Expression =
                    Expression.Call(typeof<Queryable>, name, typeArgs, Array.ofList args)
                let where (pred: Expression) = call "Where" [| elemType |] [ source; pred ]
                let negated (pred: Expression) =
                    match ExpressionPatterns.unwrap pred with
                    | :? LambdaExpression as lam -> Some(Expression.Quote(Expression.Lambda(Expression.Not lam.Body, lam.Parameters)) :> Expression)
                    | _ -> None
                let replan (rewritten: Expression) =
                    let compute () = box (provider.Execute<'TResult> rewritten)
                    Some { Terminal = ScalarTerminal.Deferred compute; Finish = castFinish<'TResult> }
                let overRows (narrowed: Expression) (args: Expression list) = Some(evaluateOverClientRows m narrowed args)
                let takeOf (n: int) (seq: Expression) = call "Take" [| elemType |] [ seq; Expression.Constant n ]

                match m.Name, rest with
                | ("Count" | "LongCount" | "Any"), [ pred ] -> replan (call m.Name [| elemType |] [ where pred ])
                | "All", [ pred ] ->
                    // All(p) is !Any(!p); the provider executes the Any.
                    negated pred
                    |> Option.map (fun notPred ->
                        let anyFailing = call "Any" [| elemType |] [ where notPred ]
                        let compute () = box (not (provider.Execute<bool> anyFailing))
                        { Terminal = ScalarTerminal.Deferred compute; Finish = castFinish<'TResult> })
                | ("First" | "FirstOrDefault"), [] -> overRows (takeOf 1 source) rest
                | ("First" | "FirstOrDefault"), pred :: _ -> overRows (takeOf 1 (where pred)) rest
                | ("Single" | "SingleOrDefault"), [] -> overRows (takeOf 2 source) rest
                | ("Single" | "SingleOrDefault"), pred :: _ -> overRows (takeOf 2 (where pred)) rest
                | ("Last" | "LastOrDefault"), [] -> overRows (call "TakeLast" [| elemType |] [ source; Expression.Constant 1 ]) rest
                | ("Last" | "LastOrDefault"), pred :: _ -> overRows (call "TakeLast" [| elemType |] [ where pred; Expression.Constant 1 ]) rest
                | ("ElementAt" | "ElementAtOrDefault"), [ index ] when index.Type = typeof<int> ->
                    match PartialEvaluator.tryEvaluate index with
                    | Some(:? int as i) when i >= 0 ->
                        overRows (takeOf 1 (call "Skip" [| elemType |] [ source; Expression.Constant i ])) [ Expression.Constant 0 ]
                    | _ -> None
                | ("Min" | "Max" | "Sum" | "Average"), [ selector ] ->
                    match ExpressionPatterns.unwrap selector with
                    | :? LambdaExpression as lam when PolarsTypeHelper.IsScalarType lam.ReturnType ->
                        let projected = call "Select" [| elemType; lam.ReturnType |] [ source; selector ]
                        let typeArgs = if m.Name = "Min" || m.Name = "Max" then [| lam.ReturnType |] else [||]
                        replan (call m.Name typeArgs [ projected ])
                    | _ -> None
                | _ -> None

        let extractOptionalLambda (args: Expression list) : LambdaExpression option =
            match args with
            | [ CleanLambda l ] -> Some l
            | _ -> None

        let evaluatedExpr = PartialEvaluator.eval expression

        match evaluatedExpr with
        // =========================================================================
        // 0. Sources requiring client evaluation: LINQ-to-Objects over the enumerated rows
        // =========================================================================
        | MethodCall(m, null, source :: rest) when isScalarReductionMethod m && requiresClientEvaluation m source ->
            tryNarrowScopedTerminal m source rest |> Option.defaultWith (fun () -> evaluateOverClientRows m source rest)

        // =========================================================================
        // 1. Count() / LongCount()
        // =========================================================================
        | MethodCall(m, null, source :: rest) when m.Name = "Count" || m.Name = "LongCount" ->
            let chunkSizeOpt = tryExtractChunkSize source
            let src = resolveSource source
            let elemType = getSequenceElementType source.Type
            let predOpt = extractOptionalLambda rest

            let adjustForChunk (rowCount: int64) : int64 =
                match chunkSizeOpt with
                | Some size when size > 0 ->
                    if rowCount <= 0L then 0L else (rowCount + int64 size - 1L) / int64 size
                | _ -> rowCount

            let toResult (count: int64) : 'TResult = Convert.ChangeType(count, targetType) :?> 'TResult

            let nativeCount (predOpt: ScalarExpr option) : ScalarQuery<'TResult> =
                let lenPlan =
                    LogicalPlan.Project(filterOpt src.Plan predOpt, [ ("len", ScalarExpr.Aggregation(AggKind.Len, None, false)) ])
                {
                    Terminal = ScalarTerminal.RowCount lenPlan
                    Finish = fun outcome -> valueOf outcome :?> int64 |> adjustForChunk |> toResult
                }

            let clientCount (srcType: Type) (pred: Delegate) : ScalarQuery<'TResult> =
                {
                    Terminal = ScalarTerminal.Rows(src.Plan, RowsOp.Count(srcType, pred))
                    Finish =
                        fun outcome ->
                            let countVal = valueOf outcome :?> int64
                            (if srcType.IsArray then countVal else adjustForChunk countVal) |> toResult
                }

            match tryResolvePredicate src elemType predOpt with
            | ValueSome(Pushdown filterExpr) -> nativeCount (Some filterExpr)
            | ValueSome(Client(srcType, combinedPred)) ->
                enforceClientPredicatePolicy ("Count", predOpt)
                clientCount srcType combinedPred
            | ValueNone -> nativeCount None

        // =========================================================================
        // 2. Any / All
        // =========================================================================
        | MethodCall(m, null, source :: rest) when m.Name = "Any" || m.Name = "All" ->
            let isAll = m.Name = "All"
            let src = resolveSource source
            let elemType = getSequenceElementType source.Type
            let predOpt = extractOptionalLambda rest
            let boolFinish (negate: bool) (outcome: ScalarOutcome) : 'TResult =
                let b = valueOf outcome :?> bool
                box (if negate then not b else b) :?> 'TResult

            match tryResolvePredicate src elemType predOpt with
            | ValueSome(Pushdown filterExpr) ->
                let effectiveFilter = if isAll then ScalarExpr.Unary(UnaryOp.Not, filterExpr) else filterExpr
                {
                    Terminal = ScalarTerminal.HasRows(firstRowOf (LogicalPlan.Filter(src.Plan, effectiveFilter)))
                    Finish = boolFinish isAll
                }

            | ValueSome(Client(srcType, combinedPred)) ->
                enforceClientPredicatePolicy (m.Name, predOpt)
                let methodName = if isAll then "AllPredicate" else "AnyPredicate"
                {
                    Terminal = ScalarTerminal.Rows(src.Plan, RowsOp.BoolPredicate(methodName, srcType, combinedPred))
                    Finish = boolFinish false
                }

            | ValueNone ->
                if isAll then
                    { Terminal = ScalarTerminal.Constant(box true); Finish = boolFinish false }
                else
                    { Terminal = ScalarTerminal.HasRows(firstRowOf src.Plan); Finish = boolFinish false }

        // =========================================================================
        // 3. First / FirstOrDefault / Last / LastOrDefault / Single / SingleOrDefault
        // =========================================================================
        | MethodCall(m, null, source :: rest) when
            m.Name.StartsWith "First"
            || m.Name.StartsWith "Last"
            || m.Name.StartsWith "Single"
            ->

            let kind =
                if m.Name.StartsWith "First" then RowFetchKind.First
                elif m.Name.StartsWith "Last" then RowFetchKind.Last
                else RowFetchKind.Single

            let isOrDefault = m.Name.EndsWith "OrDefault"
            let elemType = getSequenceElementType source.Type

            let predLambdaOpt, defValOpt =
                match rest with
                | [ CleanLambda p ] -> Some p, None
                | [ CleanLambda p; defExpr ] -> Some p, PartialEvaluator.tryEvaluate defExpr
                | [ defExpr ] when not (typeof<LambdaExpression>.IsAssignableFrom defExpr.Type) -> None, PartialEvaluator.tryEvaluate defExpr
                | _ -> None, None

            if isGroupingType elemType then
                let compute () : obj =
                    let query = provider.CreateQuery source :> IEnumerable
                    let castMethod = LinqReflectionCache.GetEnumerableMethod("Cast", 1, [| elemType |])
                    let baseSeq = castMethod.Invoke(null, [| box query |]) :?> IEnumerable<'TResult>

                    let seq =
                        match predLambdaOpt with
                        | Some p ->
                            let compiled = p.Compile() :?> Func<'TResult, bool>
                            baseSeq |> Seq.filter compiled.Invoke
                        | None -> baseSeq

                    let defaultResult =
                        match defValOpt with
                        | Some v -> v :?> 'TResult
                        | None -> Unchecked.defaultof<'TResult>

                    let result =
                        match kind with
                        | RowFetchKind.First ->
                            if isOrDefault then Seq.tryHead seq |> Option.defaultValue defaultResult else Seq.head seq
                        | RowFetchKind.Last ->
                            if isOrDefault then Seq.tryLast seq |> Option.defaultValue defaultResult else Seq.last seq
                        | RowFetchKind.Single ->
                            if isOrDefault && Seq.isEmpty seq then defaultResult else Seq.exactlyOne seq

                    box result

                { Terminal = ScalarTerminal.Deferred compute; Finish = castFinish<'TResult> }
            else
                planRowFetch<'TResult> kind isOrDefault defValOpt (resolveSource source) elemType predLambdaOpt

        // =========================================================================
        // 4. Max, Min, Sum, Average with or without selector
        // =========================================================================
        | MethodCall(m, null, source :: rest) when
            m.Name = "Max" || m.Name = "Min" || m.Name = "Sum" || m.Name = "Average"
            ->

            let src = resolveSource source
            let elemType = getSequenceElementType source.Type
            let selectorOpt = extractOptionalLambda rest

            let translatedColExprOpt =
                if src.ClientProjection.IsNone then
                    match selectorOpt with
                    | Some selector ->
                        let ctx = SubqueryDecorrelator.createContextForType elemType src.Plan selector.Parameters.[0].Name
                        tryTranslateToScalar ctx selector.Body
                    | None ->
                        tryGetScalarColumnNameFromCols elemType (planColumns src.Plan)
                        |> Option.map ScalarExpr.Column
                else
                    None

            match translatedColExprOpt with
            | Some colExpr ->
                let aggPlan = LogicalPlan.Project(src.Plan, [ (m.Name, resolveScalarAggExpr m.Name colExpr) ])
                {
                    Terminal = ScalarTerminal.ScalarValue aggPlan
                    Finish = fun outcome -> boxScalarResult<'TResult> (valueOf outcome, m.Name)
                }

            | None ->
                let accumType = if m.Name = "Average" then typeof<double> else targetType

                let effectiveType, effectiveSelectorOpt =
                    match selectorOpt, src.ClientProjection with
                    | Some selector, Some(sourceType, projFunc) ->
                        sourceType, Some(composeClientSelector sourceType elemType projFunc selector)
                    | Some selector, None -> elemType, Some(selector.Compile())
                    | None, Some(sourceType, projFunc) -> sourceType, Some(projFunc :> Delegate)
                    | None, None -> elemType, None

                {
                    Terminal = ScalarTerminal.Rows(src.Plan, RowsOp.NumericFold(effectiveType, accumType, m.Name, effectiveSelectorOpt))
                    Finish =
                        fun outcome ->
                            match outcome with
                            | ScalarOutcome.NoRows -> boxScalarResult<'TResult> (null, m.Name)
                            | ScalarOutcome.Value finished -> Convert.ChangeType(finished, targetType) :?> 'TResult
                }

        // =========================================================================
        // 5. ElementAt / ElementAtOrDefault
        // =========================================================================
        | MethodCall(m, null, [ source; indexExpr ]) when m.Name = "ElementAt" || m.Name = "ElementAtOrDefault" ->
            let isOrDefault = m.Name = "ElementAtOrDefault"
            let src = resolveSource source
            let elemType = getSequenceElementType source.Type

            let isFromEnd, rawIndexVal =
                match PartialEvaluator.tryEvaluate indexExpr with
                | Some(:? Index as idx) -> idx.IsFromEnd, int64 idx.Value
                | Some idxVal -> false, Convert.ToInt64 idxVal
                | None -> failwithf "Failed to evaluate index for %s." m.Name

            let outOfRange () : 'TResult =
                if isOrDefault then
                    Unchecked.defaultof<'TResult>
                else
                    raise (ArgumentOutOfRangeException("index", "Index was out of range."))

            let isInvalidIndex = if isFromEnd then rawIndexVal <= 0L else rawIndexVal < 0L

            if isInvalidIndex then
                { Terminal = ScalarTerminal.Constant null; Finish = fun _ -> outOfRange () }
            else
                let targetSourceType = getEffectiveSourceType src.ClientProjection elemType
                let project rawEntity = projectClientResult<'TResult> src.ClientProjection (Unchecked.defaultof<'TResult>) rawEntity

                let sliceOffset = if isFromEnd then -rawIndexVal else rawIndexVal
                {
                    Terminal = ScalarTerminal.Rows(LogicalPlan.Limit(src.Plan, sliceOffset, 1u), RowsOp.ElementAt(targetSourceType, 0L))
                    Finish =
                        fun outcome ->
                            match outcome with
                            | ScalarOutcome.NoRows -> outOfRange ()
                            | ScalarOutcome.Value rawEntity -> project rawEntity
                }

        // =========================================================================
        // 6. Contains(item) / Contains(item, comparer)
        // =========================================================================
        | MethodCall(m, null, source :: itemExpr :: rest) when m.Name = "Contains" ->
            let elemType = getSequenceElementType source.Type

            let itemVal =
                match PartialEvaluator.tryEvaluate itemExpr with
                | Some v -> v
                | None -> failwith "Failed to evaluate item parameter for Contains."

            let comparerObj = tryExtractComparer rest
            let src = resolveSource source

            let filterExprOpt =
                if src.ClientProjection.IsNone && isNull comparerObj then
                    buildItemEqualityFilter itemVal src.Plan
                else
                    None

            match filterExprOpt with
            | Some combinedFilter ->
                {
                    Terminal = ScalarTerminal.HasRows(firstRowOf (LogicalPlan.Filter(src.Plan, combinedFilter)))
                    Finish = castFinish<'TResult>
                }

            | None ->
                match src.ClientProjection with
                | Some(sourceType, projFunc) ->
                    let compFunc =
                        if isNull comparerObj then
                            fun a b ->
                                if isNull a then isNull b
                                elif isNull b then false
                                else Object.Equals(a, b)
                        else
                            match comparerObj with
                            | :? IEqualityComparer as eq -> (fun a b -> eq.Equals(a, b))
                            | _ ->
                                let compMethod = comparerObj.GetType().GetMethod("Equals", [| typeof<obj>; typeof<obj> |])
                                if not (isNull compMethod) then
                                    (fun a b -> compMethod.Invoke(comparerObj, [| a; b |]) :?> bool)
                                else
                                    (fun a b -> Object.Equals(a, b))

                    let compiledPred = Func<obj, bool>(fun projectedObj -> compFunc projectedObj itemVal)
                    let combinedPred = LinqReflectionCache.CreateComposedPredicate(sourceType, projFunc, compiledPred, elemType)
                    { Terminal = ScalarTerminal.Rows(src.Plan, RowsOp.Any(sourceType, combinedPred)); Finish = castFinish<'TResult> }

                | None ->
                    { Terminal = ScalarTerminal.Rows(src.Plan, RowsOp.Contains(elemType, itemVal, comparerObj)); Finish = castFinish<'TResult> }

        // =========================================================================
        // 7. SequenceEqual(second) / SequenceEqual(second, comparer)
        // =========================================================================
        | MethodCall(m, null, source :: secondExpr :: rest) when m.Name = "SequenceEqual" ->
            let elemType = getSequenceElementType source.Type
            let compObj = tryExtractComparer rest
            let src = resolveSource source

            match SourceResolver.resolveSequencePlan backend sources secondExpr with
            | Some secondPlan when src.ClientProjection.IsNone && isNull compObj ->
                { Terminal = ScalarTerminal.FramesEqual(src.Plan, secondPlan); Finish = castFinish<'TResult> }

            | Some secondPlan ->
                { Terminal = ScalarTerminal.Rows(src.Plan, RowsOp.SequenceEqualPlan(secondPlan, elemType, compObj)); Finish = castFinish<'TResult> }

            | None ->
                let secondSeq =
                    match PartialEvaluator.tryEvaluate secondExpr with
                    | Some(:? IEnumerable as seq) -> seq
                    | _ -> provider.CreateQuery(secondExpr) :> IEnumerable

                { Terminal = ScalarTerminal.Rows(src.Plan, RowsOp.SequenceEqualSeq(secondSeq, elemType, compObj)); Finish = castFinish<'TResult> }

        // =========================================================================
        // 8. MinBy / MaxBy (with or without comparer)
        // =========================================================================
        | MethodCall(m, null, source :: CleanLambda keyLambda :: rest) when m.Name = "MinBy" || m.Name = "MaxBy" ->
            let isMax = m.Name = "MaxBy"
            let src = resolveSource source
            let elemType = getSequenceElementType source.Type
            let keyType = keyLambda.ReturnType
            let compObj = tryExtractComparer rest

            let nativeSortKeyOpt =
                if src.ClientProjection.IsNone && isNull compObj then
                    let ctx = SubqueryDecorrelator.createContextForType elemType src.Plan keyLambda.Parameters.[0].Name
                    tryTranslateToScalar ctx keyLambda.Body
                else
                    None

            match nativeSortKeyOpt with
            | Some keyExpr ->
                // MinBy / MaxBy skip null keys (sorted last); a NaN key is the least (NaNSemantics.sortOrders).
                let sortedPlan =
                    LogicalPlan.Sort(src.Plan, NaNSemantics.sortOrders keyType { Expr = keyExpr; Descending = isMax; NullsLast = true })
                {
                    Terminal = ScalarTerminal.Rows(firstRowOf sortedPlan, RowsOp.FirstRow elemType)
                    Finish =
                        fun outcome ->
                            match outcome with
                            | ScalarOutcome.NoRows ->
                                if PolarsTypeHelper.AcceptsNull elemType then
                                    LinqReflectionCache.GetEmptyOptionalValue elemType :?> 'TResult
                                else
                                    raise (InvalidOperationException "Sequence contains no elements")
                            | ScalarOutcome.Value winner -> winner :?> 'TResult
                }

            | None ->
                let targetSourceType, effectiveKeySel =
                    match src.ClientProjection with
                    | Some(sourceType, projFunc) -> sourceType, composeClientSelector sourceType elemType projFunc keyLambda
                    | None -> elemType, keyLambda.Compile()

                {
                    Terminal =
                        ScalarTerminal.Rows(src.Plan, RowsOp.ExtremumBy(m.Name, targetSourceType, keyType, effectiveKeySel, compObj))
                    Finish =
                        fun outcome ->
                            projectClientResult<'TResult> src.ClientProjection (Unchecked.defaultof<'TResult>) (valueOf outcome)
                }

        // =========================================================================
        // 9. Aggregate (Fold/Reduce)
        // =========================================================================
        | MethodCall(m, null, source :: rest) when m.Name = "Aggregate" ->
            let elemType = getSequenceElementType source.Type

            let evalSeed (seedExpr: Expression) =
                match PartialEvaluator.tryEvaluate seedExpr with
                | Some s -> s
                | None -> failwith "Failed to evaluate seed parameter for Aggregate."

            let compute () : obj =
                let rawRows = provider.CreateQuery source :> IEnumerable

                match rest with
                | [ CleanLambda func ] ->
                    let invoker = LinqReflectionCache.GetAggregateInvoker elemType
                    invoker.Invoke(rawRows, func.Compile())

                | [ seedExpr; CleanLambda func ] ->
                    let invoker = LinqReflectionCache.GetAggregateWithSeedInvoker(elemType, targetType)
                    invoker.Invoke(rawRows, evalSeed seedExpr, func.Compile())

                | [ seedExpr; CleanLambda func; CleanLambda resSel ] ->
                    let invoker = LinqReflectionCache.GetAggregateWithResultSelectorInvoker(elemType, func.ReturnType, targetType)
                    invoker.Invoke(rawRows, evalSeed seedExpr, func.Compile(), resSel.Compile())

                | _ -> failwithf "Unsupported Aggregate overload: %A" expression

            { Terminal = ScalarTerminal.Deferred compute; Finish = castFinish<'TResult> }

        | _ -> failwithf "Scalar execution for expression is not supported: %A" expression

    /// Plans a scalar / single-row LINQ terminal expression into a backend-agnostic ScalarQuery.
    /// Opens a compilation scope for the duration of expression translation.
    let plan<'TResult> (backend: IQueryBackend) (provider: IQueryProvider) (expression: Expression) : PlannedScalar<'TResult> =
        use _scope = CompilationScope.Enter backend
        let sources = SourceRegistry()
        let query = planWith<'TResult> backend sources provider expression
        { Query = query; Sources = sources.ToArray() }
