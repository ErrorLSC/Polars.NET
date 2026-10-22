namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core
open Polars.NET.Core.Helpers

/// Internal classification of resolved predicate pushdown state
type internal ResolvedPredicate =
    | Pushdown of ExprHandle
    | Client of effectiveType: Type * pred: Delegate

/// Row selection strategy for scalar single-row operations
type internal RowFetchKind =
    | First
    | Last
    | Single

type internal ExecutionPlan<'T> =
    {
        NativePlan: LazyFrameHandle
        ClientPredicates: Func<obj, bool> list
        ClientProjection: (Type * Func<obj, obj>) option
    }

/// Internal interface allowing direct zero-reflection plan querying between Provider and Query instances
type internal IInternalPlanSource =
    abstract member GetPlanInfo: unit -> LazyFrameHandle * bool * (Type * Func<obj, obj>) option

/// Internal normalized context capturing all GroupBy variants (C# 4-args, F# pipeline, Having, Sorts)
type internal GroupFoldContext =
    {
        KeyLambda: LambdaExpression
        ElemLambdaOpt: LambdaExpression option
        HavingPreds: LambdaExpression list
        SortStages: (LambdaExpression * bool) list
    }

/// Strongly-typed IGrouping implementation for in-memory GroupJoin materialization.
type internal MaterializedGrouping<'TKey, 'TElement>(key: 'TKey, elements: 'TElement array) =
    // 1. Explicit base interface implementations resolving FS0363 interface diamond collision
    interface IEnumerable<'TElement> with
        member _.GetEnumerator() : IEnumerator<'TElement> =
            (elements :> IEnumerable<'TElement>).GetEnumerator()

    interface IEnumerable with
        member _.GetEnumerator() : IEnumerator = elements.GetEnumerator()

    // 2. IGrouping specific implementation
    interface IGrouping<'TKey, 'TElement> with
        member _.Key = key

    // 3. Collection interfaces for O(1) counting and fast BCL compatibility
    interface IReadOnlyCollection<'TElement> with
        member _.Count = elements.Length

    interface ICollection with
        member _.Count = elements.Length
        member _.IsSynchronized = false
        member _.SyncRoot = elements.SyncRoot
        member _.CopyTo(array, index) = elements.CopyTo(array, index)

/// Represents the diagnostic inspection result of a Polars LINQ query pipeline.
type PlanInspection =
    {
        NativePlan: string
        HasClientPredicates: bool
        ClientPredicateExpressions: LambdaExpression list
        HasClientProjection: bool
        IsFullFallback: bool
        FallbackReason: string option
    }

    /// Formats the inspection result into a human-readable diagnostic report.
    member this.FormatReport() : string =
        let sb = System.Text.StringBuilder()

        if this.IsFullFallback then
            sb.AppendLine "[Polars.NET Query Plan - Full Fallback to In-Memory LINQ]"
            |> ignore

            sb.AppendLine "---------------------------------------------------------"
            |> ignore

            sb.AppendLine "Warning: The query contains unsupported expressions or transparent identifiers."
            |> ignore

            match this.FallbackReason with
            | Some reason ->
                sb.AppendLine(sprintf "   Diagnostic reason: %s" reason) |> ignore
            | None -> ()

            sb.AppendLine "   The entire pipeline will be executed in managed memory via LINQ-to-Objects."
            |> ignore
        elif not this.HasClientPredicates && not this.HasClientProjection then
            sb.AppendLine "[Polars.NET Query Plan - 100% Native Pushdown]" |> ignore
            sb.AppendLine "---------------------------------------------" |> ignore
            sb.AppendLine this.NativePlan |> ignore
        else
            sb.AppendLine "[Polars.NET Query Plan - Hybrid Execution (Partial Pushdown)]"
            |> ignore

            sb.AppendLine "------------------------------------------------------------"
            |> ignore

            sb.AppendLine "Phase 1: Native Polars Pushdown Plan:" |> ignore
            sb.AppendLine this.NativePlan |> ignore
            sb.AppendLine "Phase 2: Managed Client Fallback:" |> ignore

            if this.HasClientPredicates then
                sb.AppendLine " Client-side Predicates (Evaluated in .NET memory):" |> ignore

                for pred in this.ClientPredicateExpressions do
                    sb.AppendFormat("     • {0}\n", pred.ToString()) |> ignore

            if this.HasClientProjection then
                sb.AppendLine " Client-side Projection: Expression could not be translated natively."
                |> ignore

        sb.ToString().TrimEnd()

/// Public interface implemented by PolarsQueryable to expose query plan inspection.
type IPolarsExplainable =
    abstract member InspectPlan: optimized: bool -> PlanInspection
    abstract member Explain: optimized: bool -> string

type internal FallbackTracker =
    [<ThreadStatic; DefaultValue>]
    static val mutable private diagnostic: string option
    static member Diagnostic
        with get () = FallbackTracker.diagnostic
        and set (v: string option) = FallbackTracker.diagnostic <- v

/// Shared pure helpers for LINQ expression inspection, stage flattening, operator compilation, and policy enforcement.
[<AutoOpen>]
module internal QueryPipelineHelpers =

    // =========================================================================
    // 1. Type, Schema & Policy Helpers
    // =========================================================================

    /// Creates an appropriate TranslationContext based on whether the sequence element type
    /// is a scalar/primitive (bound to the underlying single physical column) or a composite entity.
    let createContextForType (elemType: Type) (lf: LazyFrameHandle) (paramName: string) : TranslationContext =
        SubqueryDecorrelator.createContextForType elemType lf paramName

    /// Extracts the underlying sequence element type from generic collections/arrays
    let getSequenceElementType (t: Type) : Type =
        if t.IsGenericType then t.GetGenericArguments().[0]
        elif t.IsArray then t.GetElementType()
        else t

    /// Resolves the single physical column name if the sequence is scalar (e.g. string, int, double).
    let tryGetScalarColumnName (elemType: Type) (lf: LazyFrameHandle) : string option =
        if PolarsTypeHelper.IsScalarType elemType || elemType = typeof<string> then
            let colNames = PolarsSchemaExtractor.GetColumnNames lf
            if colNames.Length = 1 then Some colNames.[0] else None
        else
            None

    /// Dispatches and resolves predicate into either native ExprHandle pushdown or composed client Func<obj, bool>
    let tryResolvePredicate
        (hasClientPreds: bool)
        (clientProjOpt: (Type * Func<obj, obj>) option)
        (elemType: Type)
        (nativeLf: LazyFrameHandle)
        (predLambdaOpt: LambdaExpression option)
        : ResolvedPredicate voption =
        match predLambdaOpt with
        | None -> ValueNone
        | Some predLambda ->
            let nativeFilterOpt =
                if not hasClientPreds && clientProjOpt.IsNone then
                    let ctx = createContextForType elemType nativeLf predLambda.Parameters.[0].Name
                    ExprTranslator.tryTranslateWithContext ctx predLambda.Body
                else
                    None

            match nativeFilterOpt with
            | Some filterExpr -> ValueSome(Pushdown filterExpr)
            | None ->
                let compiledPred = predLambda.Compile()
                let predArgType = predLambda.Parameters.[0].Type

                match clientProjOpt with
                | Some(sourceType, projFunc) ->
                    let combinedPred =
                        LinqReflectionCache.CreateComposedPredicate(sourceType, projFunc, compiledPred, predArgType)

                    ValueSome(Client(sourceType, combinedPred))
                | None -> ValueSome(Client(elemType, compiledPred))

    /// Enforces client evaluation policy on unpushed scalar/terminal predicates (e.g. First(pred), Any(pred))
    let enforceClientPredicatePolicy (stage: string, predLambdaOpt: LambdaExpression option) : unit =
        let policy = CoreConfig.ClientEvaluationPolicy
        match policy with
        | ClientEvaluationPolicy.Allow -> ()
        | ClientEvaluationPolicy.Warn ->
            let text = match predLambdaOpt with Some p -> p.ToString() | None -> "Predicate"
            System.Diagnostics.Trace.TraceWarning(
                $"[Polars.NET Warning] Predicate in '%s{stage}' (%s{text}) could not be pushed down and is evaluated on client."
            )
        | ClientEvaluationPolicy.Strict
        | _ ->
            let text = match predLambdaOpt with Some p -> p.ToString() | None -> "Predicate"
            raise (PolarsPushdownException(stage, text, $"Predicate in '%s{stage}' could not be pushed down to native Polars engine."))

    /// Enforces the ClientEvaluationPolicy against client predicates and full fallbacks
    let enforcePipelinePolicy
        (stage: string)
        (requiresClientFallback: bool)
        (clientPredsWithAst: (Func<obj, bool> * LambdaExpression) list)
        (expressionText: string)
        : unit =
        let policy = CoreConfig.ClientEvaluationPolicy
        match policy with
        | ClientEvaluationPolicy.Allow -> ()
        | ClientEvaluationPolicy.Warn ->
            if requiresClientFallback then
                System.Diagnostics.Trace.TraceWarning(
                    $"[Polars.NET Warning] Query pipeline at stage '%s{stage}' fell back to full client-side in-memory LINQ: %s{expressionText}"
                )
            elif not clientPredsWithAst.IsEmpty then
                let predTexts =
                    clientPredsWithAst
                    |> List.map (fun (_, l) -> l.ToString())
                    |> String.concat "; "
                System.Diagnostics.Trace.TraceWarning(
                    $"[Polars.NET Warning] Query predicate at stage '%s{stage}' fell back to client-side evaluation: %s{predTexts}"
                )
        | ClientEvaluationPolicy.Strict
        | _ ->
            if requiresClientFallback then
                raise (
                    PolarsPushdownException(
                        stage,
                        expressionText,
                        "Full in-memory LINQ fallback was triggered due to unsupported query operations or transparent identifiers."
                    )
                )
            elif not clientPredsWithAst.IsEmpty then
                let firstPred = clientPredsWithAst |> List.head |> snd
                raise (
                    PolarsPushdownException(
                        stage,
                        firstPred.ToString(),
                        sprintf "Method, member, or operator in predicate '%s' cannot be pushed down to native Polars engine." (firstPred.ToString())
                    )
                )

    /// Post-processes raw scalar aggregate results (handling NaN, empty collections, and F# Option/ValueOption types)
    let boxScalarResult<'TResult> (rawScalar: obj, methodName: string) : 'TResult =
        let targetType = typeof<'TResult>

        let isEmpty =
            isNull rawScalar || rawScalar :? double && Double.IsNaN(rawScalar :?> double)

        if isEmpty then
            match methodName with
            | "Sum" ->
                if PolarsTypeHelper.AcceptsNull targetType then
                    LinqReflectionCache.GetEmptyOptionalValue targetType :?> 'TResult
                else
                    Activator.CreateInstance targetType :?> 'TResult
            | "Average"
            | "Min"
            | "Max" ->
                if PolarsTypeHelper.AcceptsNull targetType then
                    LinqReflectionCache.GetEmptyOptionalValue targetType :?> 'TResult
                else
                    invalidOp "Sequence contains no elements."
            | _ -> invalidOp "Sequence contains no elements."
        else
            let struct (_, kind) = PolarsTypeHelper.GetOptionalInfo targetType

            match kind with
            | OptionalKind.FSharpOption ->
                let someMethod = targetType.GetMethod "Some"

                let converted =
                    Convert.ChangeType(rawScalar, PolarsTypeHelper.GetUnderlyingOrSelf targetType)

                someMethod.Invoke(null, [| converted |]) :?> 'TResult
            | OptionalKind.FSharpValueOption ->
                let vSomeMethod = targetType.GetMethod "NewValueSome"

                let converted =
                    Convert.ChangeType(rawScalar, PolarsTypeHelper.GetUnderlyingOrSelf targetType)

                vSomeMethod.Invoke(null, [| converted |]) :?> 'TResult
            | _ -> Convert.ChangeType(rawScalar, targetType) :?> 'TResult

    /// Maps standard reduction method names to Polars aggregate expressions
    let resolveScalarAggExpr (methodName: string) (colExpr: ExprHandle) : ExprHandle =
        match methodName with
        | "Max" -> PolarsWrapper.Max colExpr
        | "Min" -> PolarsWrapper.Min colExpr
        | "Sum" -> PolarsWrapper.Sum colExpr
        | "Average" -> PolarsWrapper.Mean colExpr
        | other -> failwithf "Unsupported scalar aggregate operator: %s" other

    let private invokeWithBoxedParam (lambda: LambdaExpression) : InvocationExpression * ParameterExpression =
        let paramType = lambda.Parameters.[0].Type
        let rawParam = Expression.Parameter(typeof<obj>, "rawItem")

        let castParam =
            if paramType = typeof<obj> then
                rawParam :> Expression
            else
                Expression.Convert(rawParam, paramType) :> Expression

        Expression.Invoke(lambda, castParam), rawParam

    /// Compiles any lambda predicate into a type-safe boxed delegate Func<obj, bool>
    /// to safely evaluate against raw materialized source entities without generic coupling.
    let compileClientPredicate (lambda: LambdaExpression) : Func<obj, bool> =
        let invoked, rawParam = invokeWithBoxedParam lambda
        Expression.Lambda<Func<obj, bool>>(invoked, rawParam).Compile()

    /// Compiles a lambda into a typed Func<obj, obj> without DynamicInvoke reflection overhead.
    let compileToBoxedFunc (lambda: LambdaExpression) : Func<obj, obj> =
        let invoked, rawParam = invokeWithBoxedParam lambda
        let boxedResult = Expression.Convert(invoked, typeof<obj>)
        Expression.Lambda<Func<obj, obj>>(boxedResult, rawParam).Compile()

    /// Helper to extract all column names and their corresponding DataTypeHandle from a LazyFrame schema
    let getLazySchemaFields (lf: LazyFrameHandle) : (string * DataTypeHandle) array =
        let schema = PolarsWrapper.GetLazySchema lf
        let count = PolarsWrapper.GetSchemaLen schema

        Array.init (int count) (fun i ->
            let mutable name = null
            let mutable dtHandle = new DataTypeHandle()
            PolarsWrapper.GetSchemaFieldAt(schema, uint64 i, &name, &dtHandle)
            name, dtHandle)

    /// Disambiguates duplicate column names against an existing set of columns by appending a given suffix,
    /// returning the safely renamed LazyFrame, updated column set, and a renaming map.
    let disambiguateColumns
        (existingCols: Set<string>)
        (lf: LazyFrameHandle)
        (suffix: string)
        : LazyFrameHandle * Set<string> * Map<string, string> =
        let cols = PolarsSchemaExtractor.GetColumnNames lf
        let duplicates = cols |> Array.filter existingCols.Contains

        if duplicates.Length > 0 then
            let newNames = duplicates |> Array.map (fun c -> c + suffix)
            let renamedLf = PolarsWrapper.LazyRename(lf, duplicates, newNames, strict = false)
            let suffixMap = Array.zip duplicates newNames |> Map.ofArray
            renamedLf, existingCols |> Set.union (Set.ofArray cols), suffixMap
        else
            lf, existingCols |> Set.union (Set.ofArray cols), Map.empty

    /// Applies eager row shuffling (sample_frac = 1.0, without replacement) to a DataFrame.
    let applyShuffle (df: DataFrameHandle) : DataFrameHandle =
        let fracHandle = PolarsWrapper.SeriesNew("", [| 1.0 |])
        PolarsWrapper.SampleFrac(df, fracHandle, false, Nullable true, Nullable())

    // =========================================================================
    // 2. AST & Member Binding Extractors
    // =========================================================================

    let rec private hasStaticMethodCall (methodName: string) (e: Expression) : bool =
        match e with
        | null -> false
        | MethodCall(m, null, _) when m.Name = methodName -> true
        | MethodCall(_, null, args) -> args |> List.exists (hasStaticMethodCall methodName)
        | _ -> false

    /// Recursively inspects the expression tree to locate Chunk(size) invocations.
    let rec tryExtractChunkSize (e: Expression) : int option =
        match e with
        | null -> None
        | MethodCall(m, null, [ _; sizeExpr ]) when m.Name = "Chunk" ->
            tryEvaluate sizeExpr |> Option.map Convert.ToInt32
        | MethodCall(_, null, args) -> args |> List.tryPick tryExtractChunkSize
        | _ -> None

    /// Recursively inspects the expression tree to detect if Shuffle() was called.
    let hasShuffle (e: Expression) : bool = hasStaticMethodCall "Shuffle" e

    /// Recursively inspects the expression tree to locate AggregateBy invocations.
    let rec tryExtractAggregateBy (e: Expression) =
        match e with
        | null -> None
        | MethodCall(m, null, source :: CleanLambda k :: seedExpr :: CleanLambda f :: rest) when
            m.Name = "AggregateBy"
            ->
            let seedLambdaOpt =
                match seedExpr with
                | CleanLambda s -> Some s
                | _ -> None

            Some(source, k, seedExpr, seedLambdaOpt, f, List.tryHead rest)
        | MethodCall(_, null, args) -> args |> List.tryPick tryExtractAggregateBy
        | _ -> None

    /// Recursively inspects the expression tree to locate DefaultIfEmpty invocations.
    let rec tryExtractDefaultIfEmpty (e: Expression) : (bool * obj option) option =
        match e with
        | null -> None
        | MethodCall(m, null, _ :: rest) when m.Name = "DefaultIfEmpty" && rest.Length <= 1 ->
            Some(true, rest |> List.tryHead |> Option.bind tryEvaluate)
        | MethodCall(_, null, args) -> args |> List.tryPick tryExtractDefaultIfEmpty
        | _ -> None

    /// Recursively inspects the expression tree to locate GroupBy key and optional element selectors.
    let rec tryExtractGroupByInfo (e: Expression) : (LambdaExpression * LambdaExpression option) option =
        match e with
        | null -> None
        | MethodCall(m, null, [ _; CleanLambda k; CleanLambda elem ]) when m.Name = "GroupBy" ->
            Some(k, Some elem)
        | MethodCall(m, null, [ _; CleanLambda k ]) when m.Name = "GroupBy" -> Some(k, None)
        | MethodCall(_, null, args) -> args |> List.tryPick tryExtractGroupByInfo
        | _ -> None

    /// Recursively inspects the expression tree to detect if GroupJoin() was called.
    let hasGroupJoin (e: Expression) : bool = hasStaticMethodCall "GroupJoin" e

    /// Recursively inspects the expression tree to locate OrderBy/OrderByDescending sort selectors.
    let rec tryExtractSortSelector (e: Expression) : (LambdaExpression * bool) option =
        match e with
        | null -> None
        | MethodCall(m, null, [ _; CleanLambda l ]) when m.Name = "OrderBy" || m.Name = "OrderByDescending" ->
            Some(l, m.Name = "OrderByDescending")
        | MethodCall(_, null, args) -> args |> List.tryPick tryExtractSortSelector
        | _ -> None

    /// Recursively locates the root IQueryable / IPolarsPlanSource ConstantExpression in a LINQ pipeline.
    let rec tryFindRootQueryable (e: Expression) : (Expression * Type) option =
        match e with
        | null -> None
        | :? ConstantExpression as ce when
            typeof<IQueryable>.IsAssignableFrom(ce.Type)
            || typeof<IPolarsPlanSource>.IsAssignableFrom(ce.Type)
            ->
            Some(ce :> Expression, ce.Type)
        | :? MethodCallExpression as mc when mc.Arguments.Count > 0 -> tryFindRootQueryable mc.Arguments.[0]
        | _ -> None

    /// Extracts member names matching properties or capitalizes them to PascalCase
    let extractPascalCaseMemberNames (newExpr: NewExpression) : string list =
        if not (isNull newExpr.Members) then
            newExpr.Members |> Seq.map (fun m -> m.Name) |> Seq.toList
        else
            let t = newExpr.Type
            newExpr.Constructor.GetParameters()
            |> Seq.map (fun p ->
                if String.IsNullOrEmpty p.Name then
                    "Col"
                else
                    match t.GetProperty(p.Name, BindingFlags.Public ||| BindingFlags.Instance ||| BindingFlags.IgnoreCase) with
                    | prop when not (isNull prop) -> prop.Name
                    | _ ->
                        Char.ToUpperInvariant(p.Name.[0]).ToString() + p.Name.Substring 1)
            |> Seq.toList

    /// Extracts column names from a DistinctBy keySelector lambda
    let extractDistinctColumns (keyLambda: LambdaExpression) : string array option =
        match keyLambda.Body with
        | ExtractColumnName colName -> Some [| colName |]
        | :? NewExpression as newExpr -> Some(extractPascalCaseMemberNames newExpr |> List.toArray)
        | :? MemberInitExpression as initExpr ->
            Some(initExpr.Bindings |> Seq.map (fun b -> b.Member.Name) |> Seq.toArray)
        | _ -> None

    /// Extracts target column name for SelectMany/Explode
    let extractExplodeColumn (colLambda: LambdaExpression) : string option =
        match colLambda.Body with
        | ExtractColumnName colName -> Some colName
        | _ -> None

    /// Uniformly extracts member name and expression pairs from NewExpression or MemberInitExpression
    let tryExtractMemberBindings (expr: Expression) : (string * Expression) list option =
        match expr with
        | :? NewExpression as newExpr ->
            let names = extractPascalCaseMemberNames newExpr
            Some(List.zip names (newExpr.Arguments |> Seq.toList))
        | :? MemberInitExpression as initExpr ->
            let bindings =
                initExpr.Bindings
                |> Seq.choose (function
                    | :? MemberAssignment as a -> Some(a.Member.Name, a.Expression)
                    | _ -> None)
                |> Seq.toList

            if bindings.Length = initExpr.Bindings.Count then
                Some bindings
            else
                None
        | _ -> None

    /// Inspects GroupJoin result selector to detect group-level aggregations (e.g., g.Count())
    let tryExtractGroupAgg
        (groupParamName: string)
        (resLambda: LambdaExpression)
        : (string * ExprHandle) option =
        match resLambda.Body with
        | :? NewExpression as newExpr ->
            let args = newExpr.Arguments |> Seq.toList
            let memberNames = extractPascalCaseMemberNames newExpr

            List.zip memberNames args
            |> List.tryPick (fun (memName, arg) ->
                match arg with
                | MethodCall(m, _, (:? ParameterExpression as p) :: _) when
                    p.Name = groupParamName && (m.Name = "Count" || m.Name = "LongCount")
                    ->
                    Some(memName, PolarsWrapper.Alias(PolarsWrapper.Len(), memName))
                | _ -> None)
        | _ -> None

    /// Desugars a .NET 11 GroupJoin downstream projection (g => body)
    /// into a classic 2-parameter result selector ((outer, innerGroup) => body)
    /// where g.Key maps to 'outer' and 'g' (as IEnumerable) maps to 'innerGroup'.
    let desugarGroupJoinProject
        (projLambda: LambdaExpression)
        (outerType: Type)
        (innerType: Type)
        : LambdaExpression =
        let gParam = projLambda.Parameters.[0]
        let pOuter = Expression.Parameter(outerType, "outer")
        let innerSeqType = typedefof<IEnumerable<_>>.MakeGenericType(innerType)
        let pInnerGroup = Expression.Parameter(innerSeqType, "innerGroup")

        let replacer =
            { new ExpressionVisitor() with
                override _.VisitMember(node: MemberExpression) =
                    match ExprTranslator.unwrap node.Expression with
                    | :? ParameterExpression as p when p.Name = gParam.Name && node.Member.Name = "Key" ->
                        pOuter :> Expression
                    | _ -> base.VisitMember(node)

                override _.VisitParameter(node: ParameterExpression) =
                    if node.Name = gParam.Name then
                        pInnerGroup :> Expression
                    else
                        base.VisitParameter(node)
            }

        let newBody = replacer.Visit(projLambda.Body)

        let delegateType =
            typedefof<Func<_, _, _>>.MakeGenericType(outerType, innerSeqType, projLambda.ReturnType)

        Expression.Lambda(delegateType, newBody, [| pOuter; pInnerGroup |])

    /// Universally extracts column renaming mappings from a two-parameter result selector
    let extractBinaryResultRenames
        (resLambda: LambdaExpression)
        (explodedInfoOpt: (string * string) option)
        : (string * string) list =
        match resLambda.Body with
        | :? NewExpression as newExpr ->
            let args = newExpr.Arguments |> Seq.toList
            let memberNames = extractPascalCaseMemberNames newExpr

            List.zip memberNames args
            |> List.choose (fun (memName, arg) ->
                match explodedInfoOpt with
                | Some(itemParamName, explodedCol) when
                    (match arg with
                     | :? ParameterExpression as p -> p.Name = itemParamName
                     | _ -> false)
                    && memName <> explodedCol
                    ->
                    Some(explodedCol, memName)
                | _ ->
                    match arg with
                    | ExtractColumnName srcCol when srcCol <> memName -> Some(srcCol, memName)
                    | _ -> None)
        | _ -> []

    /// Builds a Polars equality filter expression matching an item (scalar primitive or composite entity/record)
    /// against the columns of a target LazyFrame.
    let buildItemEqualityFilter (itemVal: obj) (targetLf: LazyFrameHandle) : ExprHandle option =
        if isNull itemVal then
            None
        else
            let itemType = itemVal.GetType()
            if
                PolarsTypeHelper.IsScalarType itemType
                || itemType = typeof<string>
                || itemType.IsEnum
            then
                let colNames = PolarsSchemaExtractor.GetColumnNames targetLf
                let targetCol = if colNames.Length > 0 then colNames.[0] else ""
                let colExpr = PolarsWrapper.Col targetCol
                let litExpr = ExprTranslator.toLiteralHandle itemVal itemType
                Some(PolarsWrapper.Eq(colExpr, litExpr))
            else
                let props = itemType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)

                let conditions =
                    props
                    |> Array.choose (fun p ->
                        let v = p.GetValue itemVal

                        if isNull v then
                            None
                        else
                            try
                                let colExpr = PolarsWrapper.Col p.Name
                                let lit = ExprTranslator.toLiteralHandle v p.PropertyType
                                Some(PolarsWrapper.Eq(colExpr, lit))
                            with _ ->
                                None)

                if conditions.Length > 0 then
                    Some(conditions |> Array.reduce (fun acc c -> PolarsWrapper.And(acc, c)))
                else
                    None

    // =========================================================================
    // 3. LINQ Expression Stage Flattener
    // =========================================================================

    /// Recursively flattens a LINQ expression tree into a linear sequence of LinqStage values.
    let rec flattenStages (e: Expression) (acc: LinqStage list) : LinqStage list =
        let evalUInt32 opName (expr: Expression) =
            match tryEvaluate expr with
            | Some c -> Convert.ToUInt32 c
            | None -> failwithf "Could not evaluate %s count argument: %A" opName expr

        match e with
        | null -> acc
        | MethodCall(m, null, [ source; StripQuotes(:? LambdaExpression as l) ]) when m.Name = "Select" ->
            let isPassthrough (body: Expression) =
                let p = l.Parameters.[0]

                let rec check (expr: Expression) =
                    match expr with
                    | null -> false
                    | :? ParameterExpression as pe when pe.Name = p.Name -> true
                    | Unary(ExpressionType.Convert, inner)
                    | Unary(ExpressionType.ConvertChecked, inner)
                    | Unary(ExpressionType.Quote, inner) -> check inner
                    | MemberAccess(:? ParameterExpression as pe, memInfo) when
                        pe.Name = p.Name && isFSharpAnonymousMember memInfo
                        ->
                        true
                    | :? NewExpression as ne when isTupleOrAnonType ne.Type ->
                        ne.Arguments
                        |> Seq.mapi (fun idx arg ->
                            match ExprTranslator.unwrap arg with
                            | MemberAccess(:? ParameterExpression as pe, mem) when
                                pe.Name = p.Name
                                && (mem.Name = sprintf "Item%d" (idx + 1)
                                    || mem.Name = "Outer" && idx = 0
                                    || mem.Name = "Inner" && idx = 1)
                                ->
                                true
                            | _ -> false)
                        |> Seq.forall id
                    | _ -> false

                check body

            if isPassthrough l.Body then
                flattenStages source acc
            else
                flattenStages source (LinqStage.Project l :: acc)

        // Single-lambda stages: Where, OrderBy/ThenBy, DistinctBy, GroupBy(key), TakeWhile, SkipWhile
        | MethodCall(m,
                     null,
                     [ source; StripQuotes(:? LambdaExpression as l) ]) when
            m.Name = "Where"
            || m.Name = "OrderBy"
            || m.Name = "ThenBy"
            || m.Name = "OrderByDescending"
            || m.Name = "ThenByDescending"
            || m.Name = "DistinctBy"
            || m.Name = "GroupBy"
            || m.Name = "TakeWhile"
            || m.Name = "SkipWhile"
            ->
            let stage =
                match m.Name with
                | "Where" -> LinqStage.Filter l
                | "OrderBy" | "ThenBy" -> LinqStage.Sort(l, isDescending = false)
                | "OrderByDescending" | "ThenByDescending" -> LinqStage.Sort(l, isDescending = true)
                | "DistinctBy" -> LinqStage.DistinctBy l
                | "GroupBy" -> LinqStage.GroupByKey l
                | "TakeWhile" -> LinqStage.TakeWhile l
                | _ -> LinqStage.SkipWhile l

            flattenStages source (stage :: acc)

        // Single-argument (source-only) stages
        | MethodCall(m, null, [ source ]) ->
            let stageOpt =
                match m.Name with
                | "Distinct" -> Some LinqStage.Distinct
                | "Index" -> Some LinqStage.Index
                | "Order" -> Some(LinqStage.SortDefault(isDescending = false))
                | "OrderDescending" -> Some(LinqStage.SortDefault(isDescending = true))
                | "Reverse" -> Some LinqStage.Reverse
                | "Shuffle" -> Some LinqStage.Shuffle
                | "DefaultIfEmpty" -> Some(LinqStage.DefaultIfEmpty None)
                | "Cast" when m.IsGenericMethod -> Some(LinqStage.Cast(m.GetGenericArguments().[0]))
                | "OfType" when m.IsGenericMethod ->
                    Some(LinqStage.OfType(getSequenceElementType source.Type, m.GetGenericArguments().[0]))
                | _ -> None

            match stageOpt with
            | Some stage -> flattenStages source (stage :: acc)
            | None -> acc

        // Count-based slicing stages: Take, Skip, TakeLast, SkipLast
        | MethodCall(m, null, [ source; countExpr ]) when
            m.Name = "Take" || m.Name = "Skip" || m.Name = "TakeLast" || m.Name = "SkipLast"
            ->
            let n = evalUInt32 m.Name countExpr

            let stage =
                match m.Name with
                | "Take" -> LinqStage.Take n
                | "Skip" -> LinqStage.Skip n
                | "TakeLast" -> LinqStage.TakeLast n
                | _ -> LinqStage.SkipLast n

            flattenStages source (stage :: acc)

        | MethodCall(m, null, [ source; CleanLambda k; CleanLambda e; CleanLambda r ]) when m.Name = "GroupBy" ->
            flattenStages source (LinqStage.GroupByWithElementAndResult(k, e, r) :: acc)

        | MethodCall(m,
                     null,
                     [ source; StripQuotes(:? LambdaExpression as k); StripQuotes(:? LambdaExpression as r) ]) when
            m.Name = "GroupBy"
            ->
            flattenStages source (LinqStage.GroupByWithResult(k, r) :: acc)

        // Join, LeftJoin, RightJoin, FullJoin, GroupJoin (with optional result selector)
        | MethodCall(m,
                     _,
                     outer :: inner :: StripQuotes(:? LambdaExpression as outerKey) :: StripQuotes(:? LambdaExpression as innerKey) :: rest) when
            m.Name = "Join"
            || m.Name = "LeftJoin"
            || m.Name = "RightJoin"
            || m.Name = "FullJoin"
            || m.Name = "GroupJoin"
            ->
            let resSelOpt =
                match rest with
                | StripQuotes(:? LambdaExpression as resSel) :: _ -> Some resSel
                | _ -> None

            let stage =
                if m.Name = "GroupJoin" then
                    LinqStage.GroupJoin(m, inner, outerKey, innerKey, resSelOpt)
                else
                    LinqStage.Join(m, inner, outerKey, innerKey, resSelOpt)

            flattenStages outer (stage :: acc)

        | MethodCall(m, null, [ first; second ]) when m.Name = "Intersect" || m.Name = "Except" ->
            flattenStages first (LinqStage.SetOp(m, second, None, None) :: acc)

        | MethodCall(m, null, [ first; second; StripQuotes(:? LambdaExpression as keySel) ]) when
            m.Name = "IntersectBy" || m.Name = "ExceptBy"
            ->
            flattenStages first (LinqStage.SetOp(m, second, Some keySel, None) :: acc)

        | MethodCall(m,
                     null,
                     [ first; second; StripQuotes(:? LambdaExpression as k1); StripQuotes(:? LambdaExpression as k2) ]) when
            m.Name = "IntersectBy" || m.Name = "ExceptBy"
            ->
            flattenStages first (LinqStage.SetOp(m, second, Some k1, Some k2) :: acc)

        | MethodCall(m, null, [ first; second ]) when m.Name = "Concat" ->
            flattenStages first (LinqStage.Concat second :: acc)

        | MethodCall(m, null, [ first; second ]) when m.Name = "Union" ->
            flattenStages first (LinqStage.Union(second, None) :: acc)

        | MethodCall(m, null, [ first; second; StripQuotes(:? LambdaExpression as keySel) ]) when m.Name = "UnionBy" ->
            flattenStages first (LinqStage.Union(second, Some keySel) :: acc)

        | MethodCall(m, null, source :: StripQuotes(:? LambdaExpression as colSel) :: rest) when
            m.Name = "SelectMany"
            ->
            let resSelOpt =
                match rest with
                | [ StripQuotes(:? LambdaExpression as r) ] -> Some r
                | _ -> None

            let paramName = colSel.Parameters.[0].Name
            let containsParam = SubqueryDecorrelator.containsParameter paramName colSel.Body

            let nestedStagesOpt =
                if containsParam then
                    match ExprTranslator.unwrap colSel.Body with
                    | :? MethodCallExpression as innerMc when innerMc.Method.Name <> "DefaultIfEmpty" ->
                        let innerStages = flattenStages innerMc []
                        if not innerStages.IsEmpty then Some innerStages else None
                    | _ -> None
                else
                    None

            match nestedStagesOpt with
            | Some innerStages -> flattenStages source (innerStages @ acc)
            | None ->
                if containsParam then
                    flattenStages source (LinqStage.Explode(colSel, resSelOpt) :: acc)
                else
                    flattenStages source (LinqStage.CrossJoin(colSel.Body, resSelOpt) :: acc)

        | MethodCall(m, null, [ source; second ]) when m.Name = "Zip" ->
            flattenStages source (LinqStage.Zip(second, None) :: acc)

        | MethodCall(m, null, [ source; second; StripQuotes(:? LambdaExpression as resSel) ]) when m.Name = "Zip" ->
            flattenStages source (LinqStage.Zip(second, Some resSel) :: acc)

        | MethodCall(m, null, [ first; second; third ]) when m.Name = "Zip" ->
            flattenStages first (LinqStage.Zip3(second, third) :: acc)

        | MethodCall(m, null, [ source; sizeExpr ]) when m.Name = "Chunk" ->
            match tryEvaluate sizeExpr with
            | Some s ->
                let size = Convert.ToInt32 s

                if size <= 0 then
                    raise (ArgumentOutOfRangeException("size", "Chunk size must be positive."))

                flattenStages source (LinqStage.Chunk size :: acc)
            | None -> failwithf "Could not evaluate Chunk size argument: %A" sizeExpr

        | MethodCall(m, null, [ source; elemExpr ]) when m.Name = "Append" || m.Name = "Prepend" ->
            let stage = if m.Name = "Append" then LinqStage.Append elemExpr else LinqStage.Prepend elemExpr
            flattenStages source (stage :: acc)

        | MethodCall(m, null, [ source; defaultExpr ]) when m.Name = "DefaultIfEmpty" ->
            flattenStages source (LinqStage.DefaultIfEmpty(Some defaultExpr) :: acc)

        | MethodCall(m, null, source :: CleanLambda keyLambda :: _) when m.Name = "CountBy" ->
            flattenStages source (LinqStage.CountBy keyLambda :: acc)

        | _ -> acc

    // =========================================================================
    // 4. Stage-to-QueryOp Compilers & Native Plan Builder
    // =========================================================================

    /// Attempts to decorrelate subquery expressions (EXISTS/NOT EXISTS via Any, and IN/NOT IN via Contains)
    /// into native Semi/Anti Joins.
    let tryDecorrelateSubquery
        (currentLf: LazyFrameHandle)
        (filterLambda: LambdaExpression)
        : JoinSpec option =
        match SubqueryDecorrelator.tryDecorrelateFilter currentLf filterLambda with
        | Some {
                   GeneratedOps = [ QueryOp.Join spec ]
                   ResidualLambda = None
               } -> Some spec
        | _ -> None

    /// Inspects whether a projection is an intermediate Roslyn Transparent Identifier (e.g. C# 'let' clause),
    /// which packages existing context and introduces new calculated columns via WithColumns.
    let tryCompileTransparentLet
        (lf: LazyFrameHandle)
        (projLambda: LambdaExpression)
        : QueryOp option =
        let param = projLambda.Parameters.[0]
        let ctx = createContextForType param.Type lf param.Name

        match tryExtractMemberBindings projLambda.Body with
        | Some bindings ->
            let isNavigationBinding (_name: string, expr: Expression) =
                match ExprTranslator.unwrap expr with
                | :? ParameterExpression as p when p.Name = param.Name -> true
                | MemberAccess(_, m) when isNavigationMember m -> true
                | _ -> false

            let navBindings, computedBindings = bindings |> List.partition isNavigationBinding

            if not navBindings.IsEmpty && not computedBindings.IsEmpty then
                let translatedCols =
                    computedBindings
                    |> List.choose (fun (name, expr) ->
                        ExprTranslator.tryTranslateWithContext ctx expr
                        |> Option.map (fun h -> PolarsWrapper.Alias(h, name)))

                if translatedCols.Length = computedBindings.Length then
                    Some(QueryOp.WithColumns(List.toArray translatedCols))
                else
                    None
            else
                None
        | None -> None

    /// Translates a Select projection argument, supporting null-coalescing conditional fallbacks and member access.
    let tryTranslateSelectArg (ctx: TranslationContext) (argExpr: Expression) : ExprHandle option =
        match ExprTranslator.tryTranslateWithContext ctx argExpr with
        | Some h -> Some h
        | None ->
            match argExpr with
            | :? ConditionalExpression as cond ->
                let hasParamRef (e: Expression) =
                    let rec check expr =
                        match ExprTranslator.unwrap expr with
                        | null -> false
                        | :? ParameterExpression as p ->
                            isSyntheticScopeParam ctx.ContainsParam p
                        | MethodCall(_, target, args) ->
                            (not (isNull target) && check target) || (args |> List.exists check)
                        | MemberAccess(target, _) -> not (isNull target) && check target
                        | Unary(_, inner) -> check inner
                        | Binary(_, left, right) -> check left || check right
                        | Lambda(_, body) -> check body
                        | _ -> false

                    check e

                let colExprOpt, fallbackExprOpt =
                    if hasParamRef cond.IfTrue && not (hasParamRef cond.IfFalse) then
                        Some cond.IfTrue, Some cond.IfFalse
                    elif hasParamRef cond.IfFalse && not (hasParamRef cond.IfTrue) then
                        Some cond.IfFalse, Some cond.IfTrue
                    else
                        None, None

                match colExprOpt, fallbackExprOpt with
                | Some colExpr, Some fallbackExpr ->
                    let colHandleOpt =
                        match ExprTranslator.tryResolveColumnNameWithContext ctx colExpr with
                        | Some c -> Some(PolarsWrapper.Col c)
                        | None -> ExprTranslator.tryTranslateWithContext ctx colExpr

                    let litHandleOpt =
                        let cleanFallback = ExprTranslator.unwrap fallbackExpr

                        match cleanFallback with
                        | :? ConstantExpression as ce when not (isNull ce.Value) ->
                            Some(ExprTranslator.toLiteralHandle ce.Value cleanFallback.Type)
                        | other ->
                            match tryEvaluate other with
                            | Some v when not (isNull v) -> Some(ExprTranslator.toLiteralHandle v other.Type)
                            | _ -> None

                    match colHandleOpt, litHandleOpt with
                    | Some colH, Some litH -> Some(PolarsWrapper.FillNull(colH, litH))
                    | _ -> None

                | _ -> None

            | MemberAccess _ as ma ->
                ExprTranslator.tryResolveColumnNameWithContext ctx ma
                |> Option.map PolarsWrapper.Col

            | _ -> None

    /// Compiles a Select projection lambda into native Polars Select expressions
    let compileSelectProjection
        (lf: LazyFrameHandle)
        (projLambda: LambdaExpression)
        : ExprHandle array option =
        let param = projLambda.Parameters.[0]
        let ctx = createContextForType param.Type lf param.Name

        match tryExtractMemberBindings projLambda.Body with
        | Some bindings ->
            let exprOpts =
                bindings
                |> List.choose (fun (name, argExpr) ->
                    tryTranslateSelectArg ctx argExpr
                    |> Option.map (fun h -> PolarsWrapper.Alias(h, name)))

            if exprOpts.Length = bindings.Length then
                Some(List.toArray exprOpts)
            else
                None

        | None ->
            match projLambda.Body with
            | ExtractColumnName colName -> Some [| PolarsWrapper.Col colName |]
            | expr -> ExprTranslator.tryTranslateWithContext ctx expr |> Option.map (fun h -> [| h |])

    /// Compiles a Join stage into a native JoinSpec (supporting single-column and multi-column keys across Inner, Left, Right, Full)
    let compileJoin
        (baseLf: LazyFrameHandle)
        (methodInfo: MethodInfo)
        (innerExpr: Expression)
        (outerKey: LambdaExpression)
        (innerKey: LambdaExpression)
        : JoinSpec option =
        let isRightJoin = methodInfo.Name = "RightJoin"

        let how =
            match methodInfo.Name with
            | "RightJoin"
            | "LeftJoin"
            | "GroupJoin" -> PlJoinType.Left
            | "FullJoin" -> PlJoinType.Outer
            | _ -> PlJoinType.Inner

        let extractKeyExprs (ctx: TranslationContext) (keyLambda: LambdaExpression) : ExprHandle array option =
            match tryExtractMemberBindings keyLambda.Body with
            | Some bindings ->
                let exprs =
                    bindings
                    |> List.choose (fun (_, e) -> ExprTranslator.tryTranslateWithContext ctx e)

                if exprs.Length = bindings.Length then
                    Some(List.toArray exprs)
                else
                    None
            | None ->
                ExprTranslator.tryTranslateWithContext ctx keyLambda.Body
                |> Option.map (fun h -> [| h |])

        match SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr with
        | Some rightLf ->
            let outerParam = outerKey.Parameters.[0]
            let innerParam = innerKey.Parameters.[0]

            let leftCtx = createContextForType outerParam.Type baseLf outerParam.Name
            let rightCtx = createContextForType innerParam.Type rightLf innerParam.Name

            match extractKeyExprs leftCtx outerKey, extractKeyExprs rightCtx innerKey with
            | Some leftKeys, Some rightKeys when leftKeys.Length = rightKeys.Length ->
                Some
                    {
                        RightLf = rightLf
                        LeftOn = leftKeys
                        RightOn = rightKeys
                        How = how
                        Suffix = if isRightJoin then Some "_left" else Some "_right"
                        InvertSides = isRightJoin
                        Coalesce = PlJoinCoalesce.KeepColumns
                    }
            | _ -> None
        | None -> None

    /// Compiles a two-parameter Join/LeftJoin/RightJoin result selector into a native Select projection operation
    let compileJoinResultSelector
        (leftCols: string array)
        (rightCols: string array)
        (spec: JoinSpec)
        (resLambda: LambdaExpression)
        : QueryOp option =
        if resLambda.Parameters.Count < 2 then
            None
        else
            let pLeft = resLambda.Parameters.[0]
            let pRight = resLambda.Parameters.[1]

            let firstParam, secondParam, firstCols, secondCols, secondSuffix =
                if spec.InvertSides then
                    pRight, pLeft, rightCols, leftCols, (spec.Suffix |> Option.defaultValue "_left")
                else
                    pLeft, pRight, leftCols, rightCols, (spec.Suffix |> Option.defaultValue "_right")

            let firstColSet = Set.ofArray firstCols
            let ctx = TranslationContext.BinaryJoin(firstParam.Name, secondParam.Name, firstColSet, secondSuffix)

            let unwrappedBody = ExprTranslator.unwrap resLambda.Body
            match unwrappedBody with
            | :? ParameterExpression as p when p.Name = firstParam.Name ->
                let selectExprs = firstCols |> Array.map PolarsWrapper.Col
                Some(QueryOp.Select selectExprs)

            | :? ParameterExpression as p when p.Name = secondParam.Name ->
                let selectExprs =
                    secondCols
                    |> Array.map (fun c ->
                        if firstColSet.Contains c then
                            PolarsWrapper.Alias(PolarsWrapper.Col(c + secondSuffix), c)
                        else
                            PolarsWrapper.Col c)
                Some(QueryOp.Select selectExprs)

            | _ ->
                match tryExtractMemberBindings unwrappedBody with
                | Some bindings ->
                    let exprOpts =
                        bindings
                        |> List.choose (fun (name, argExpr) ->
                            tryTranslateSelectArg ctx argExpr
                            |> Option.map (fun h -> PolarsWrapper.Alias(h, name)))

                    if exprOpts.Length = bindings.Length then
                        Some(QueryOp.Select(List.toArray exprOpts))
                    else
                        None

                | None ->
                    match unwrappedBody with
                    | MemberAccess(_, m) ->
                        ExprTranslator.tryTranslateWithContext ctx unwrappedBody
                        |> Option.map (fun h -> QueryOp.Select [| PolarsWrapper.Alias(h, m.Name) |])
                    | _ ->
                        ExprTranslator.tryTranslateWithContext ctx unwrappedBody
                        |> Option.map (fun h -> QueryOp.Select [| h |])

    /// Compiles Intersect/Except/IntersectBy/ExceptBy stages into a native JoinSpec (Semi/Anti Join)
    let compileSetOp
        (methodInfo: MethodInfo)
        (secondExpr: Expression)
        (firstKeyOpt: LambdaExpression option)
        (secondKeyOpt: LambdaExpression option)
        (baseLf: LazyFrameHandle)
        : JoinSpec option =
        let how =
            if methodInfo.Name.StartsWith "Intersect" then
                PlJoinType.Semi
            else
                PlJoinType.Anti

        SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr
        |> Option.bind (fun rightLf ->
            let keysOpt =
                match firstKeyOpt, secondKeyOpt with
                | Some k1, Some k2 ->
                    let p1 = k1.Parameters.[0]
                    let p2 = k2.Parameters.[0]
                    let ctx1 = createContextForType p1.Type baseLf p1.Name
                    let ctx2 = createContextForType p2.Type rightLf p2.Name

                    match
                        ExprTranslator.tryTranslateWithContext ctx1 k1.Body,
                        ExprTranslator.tryTranslateWithContext ctx2 k2.Body
                    with
                    | Some lk, Some rk -> Some([| lk |], [| rk |])
                    | _ -> None
                | Some k1, None ->
                    let p1 = k1.Parameters.[0]
                    let ctx1 = createContextForType p1.Type baseLf p1.Name

                    ExprTranslator.tryTranslateWithContext ctx1 k1.Body
                    |> Option.bind (fun lk ->
                        let rightCols = PolarsSchemaExtractor.GetColumnNames rightLf

                        if rightCols.Length > 0 then
                            Some([| lk |], [| PolarsWrapper.Col rightCols.[0] |])
                        else
                            None)
                | None, None ->
                    let colNames = PolarsSchemaExtractor.GetColumnNames baseLf
                    Some(colNames |> Array.map PolarsWrapper.Col, colNames |> Array.map PolarsWrapper.Col)
                | _ -> None

            keysOpt
            |> Option.map (fun (leftOn, rightOn) ->
                {
                    RightLf = rightLf
                    LeftOn = leftOn
                    RightOn = rightOn
                    How = how
                    Suffix = None
                    InvertSides = false
                    Coalesce = PlJoinCoalesce.JoinSpecific
                }))

    /// Compiles an Append or Prepend stage into a native QueryOp.Concat
    let tryCompileAppendOrPrepend (elemExpr: Expression) (prepend: bool) : QueryOp option =
        let evaluatedOpt =
            match tryEvaluate elemExpr with
            | Some elem -> Some elem
            | None ->
                try
                    let lambda = Expression.Lambda<Func<obj>>(Expression.Convert(elemExpr, typeof<obj>)).Compile()
                    Some(lambda.Invoke())
                with _ ->
                    None

        match evaluatedOpt with
        | Some elem when not (isNull elem) ->
            let elemType = elem.GetType()

            let fromRows =
                typeof<DataFrameBuilder>
                    .GetMethod("FromRows", BindingFlags.Public ||| BindingFlags.Static)
                    .MakeGenericMethod(elemType)

            let typedArray = Array.CreateInstance(elemType, 1)
            typedArray.SetValue(elem, 0)

            let singleDf = fromRows.Invoke(null, [| box typedArray |]) :?> DataFrameHandle
            let otherLf = PolarsWrapper.DataFrameToLazy singleDf
            Some(QueryOp.Concat { OtherLf = otherLf; Prepend = prepend })
        | _ -> None

    /// Converts a list of (sourceCol, targetCol) pairs into a single-element or empty QueryOp.Rename list
    let buildRenameOps (renames: (string * string) list) : QueryOp list =
        if renames.IsEmpty then
            []
        else
            [
                QueryOp.Rename
                    {
                        ExistingNames = renames |> List.map fst |> List.toArray
                        NewNames = renames |> List.map snd |> List.toArray
                    }
            ]

    /// Fallback translator for filters over tuple/transparent scopes (supporting null checks and binary comparisons)
    let rec tryTranslateTupleFilter (ctx: TranslationContext) (e: Expression) : ExprHandle option =
        let isNullConstant (expr: Expression) =
            match ExprTranslator.unwrap expr with
            | :? ConstantExpression as c -> isNull c.Value
            | _ -> false

        match e with
        | Binary(ExpressionType.NotEqual | ExpressionType.Equal as op, left, right) when
            isNullConstant left || isNullConstant right
            ->
            let target = if isNullConstant right then left else right
            let wrap = if op = ExpressionType.NotEqual then PolarsWrapper.IsNotNull else PolarsWrapper.IsNull
            tryTranslateTupleFilter ctx target |> Option.map wrap
        | Binary(op, left, right) ->
            match tryTranslateTupleFilter ctx left, tryTranslateTupleFilter ctx right with
            | Some l, Some r ->
                try
                    Some(ExprTranslator.translateBinary op l r)
                with _ ->
                    None
            | _ -> None
        | MemberAccess _ as ma ->
            ExprTranslator.tryResolveColumnNameWithContext ctx ma
            |> Option.map PolarsWrapper.Col
        | Constant _ -> ExprTranslator.tryTranslateWithContext TranslationContext.Empty e
        | _ -> None

    /// Checks whether a two-parameter Join result selector merely packages parameters into a transparent identifier/tuple
    let isTransparentJoinResSelector (resSel: LambdaExpression) : bool =
        match tryExtractMemberBindings resSel.Body with
        | Some bindings when resSel.Parameters.Count >= 2 ->
            let p1, p2 = resSel.Parameters.[0].Name, resSel.Parameters.[1].Name

            let rec isTransparentArg (e: Expression) =
                match ExprTranslator.unwrap e with
                | :? ParameterExpression as p -> p.Name = p1 || p.Name = p2
                | MemberAccess(target, m) when isNavigationMember m -> isTransparentArg target
                | :? NewExpression as ne when isTupleOrAnonType ne.Type ->
                    ne.Arguments |> Seq.forall isTransparentArg
                | _ -> false

            bindings |> List.forall (fun (_, expr) -> isTransparentArg expr)
        | _ -> false

    /// Compiles GroupJoin followed by SelectMany(DefaultIfEmpty) into a native Left Join
    let compileGroupJoinLeftOuter
        (baseLf: LazyFrameHandle)
        (innerExpr: Expression)
        (outerKey: LambdaExpression)
        (innerKey: LambdaExpression)
        : QueryOp option =
        SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr
        |> Option.bind (fun rightLf ->
            let outerParam = outerKey.Parameters.[0]
            let innerParam = innerKey.Parameters.[0]
            let leftCtx = createContextForType outerParam.Type baseLf outerParam.Name
            let rightCtx = createContextForType innerParam.Type rightLf innerParam.Name

            match
                ExprTranslator.tryTranslateWithContext leftCtx outerKey.Body,
                ExprTranslator.tryTranslateWithContext rightCtx innerKey.Body
            with
            | Some leftKey, Some rightKey ->
                Some(
                    QueryOp.Join
                        {
                            RightLf = rightLf
                            LeftOn = [| leftKey |]
                            RightOn = [| rightKey |]
                            How = PlJoinType.Left
                            Suffix = None
                            InvertSides = false
                            Coalesce = PlJoinCoalesce.KeepColumns
                        }
                )
            | _ -> None)

    /// Compiles GroupJoin with an explicit result selector (supporting pre-aggregated group counts and null-filling)
    let compileGroupJoinWithResult
        (baseLf: LazyFrameHandle)
        (innerExpr: Expression)
        (outerKey: LambdaExpression)
        (innerKey: LambdaExpression)
        (resSel: LambdaExpression)
        : QueryOp list option =
        SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr
        |> Option.bind (fun rightLf ->
            let outerParam = outerKey.Parameters.[0]
            let innerParam = innerKey.Parameters.[0]
            let leftCtx = createContextForType outerParam.Type baseLf outerParam.Name
            let rightCtx = createContextForType innerParam.Type rightLf innerParam.Name

            match
                ExprTranslator.tryTranslateWithContext leftCtx outerKey.Body,
                ExprTranslator.tryTranslateWithContext rightCtx innerKey.Body
            with
            | Some leftKey, Some rightKey ->
                let groupParam = resSel.Parameters.[1]

                let preAggRightLf, aggColOpt =
                    match tryExtractGroupAgg groupParam.Name resSel with
                    | Some(aggColName, aggExpr) ->
                        let aggKeyClone = PolarsWrapper.CloneExpr rightKey
                        let aggLf = PolarsWrapper.LazyGroupByAgg(rightLf, [| aggKeyClone |], [| aggExpr |], null, false)
                        aggLf, Some aggColName
                    | None -> rightLf, None

                let joinOp =
                    QueryOp.Join
                        {
                            RightLf = preAggRightLf
                            LeftOn = [| PolarsWrapper.CloneExpr leftKey |]
                            RightOn = [| PolarsWrapper.CloneExpr rightKey |]
                            How = PlJoinType.Left
                            Suffix = Some "_right"
                            InvertSides = false
                            Coalesce = PlJoinCoalesce.JoinSpecific
                        }

                let renameOps = extractBinaryResultRenames resSel None |> buildRenameOps

                let fillOps =
                    match aggColOpt with
                    | Some aggCol ->
                        let filledExpr = PolarsWrapper.FillNull(PolarsWrapper.Col aggCol, PolarsWrapper.Lit 0u)
                        [ QueryOp.WithColumns [| PolarsWrapper.Alias(filledExpr, aggCol) |] ]
                    | None -> []

                Some(renameOps @ fillOps @ [ joinOp ])
            | _ -> None)

    /// Compiles a 2-way or 3-way Zip stage into HorizontalConcat (and optional column renames)
    let compileZip
        (baseLf: LazyFrameHandle)
        (secondExpr: Expression)
        (resLambdaOpt: LambdaExpression option)
        : QueryOp list option =
        SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr
        |> Option.map (fun otherLf ->
            let baseCols = PolarsSchemaExtractor.GetColumnNames baseLf |> Set.ofArray
            let safeSecondLf, _, suffixMap = disambiguateColumns baseCols otherLf "_second"
            let zipOp = QueryOp.HorizontalConcat [| safeSecondLf |]

            let renameOps =
                match resLambdaOpt with
                | Some resLambda ->
                    extractBinaryResultRenames resLambda None
                    |> List.map (fun (srcCol, destCol) ->
                        suffixMap.TryFind srcCol |> Option.defaultValue srcCol, destCol)
                    |> buildRenameOps
                | None -> []

            renameOps @ [ zipOp ])

    let compileZip3 (baseLf: LazyFrameHandle) (secondExpr: Expression) (thirdExpr: Expression) : QueryOp option =
        match
            SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr,
            SubqueryDecorrelator.resolveSequenceLazyFrame thirdExpr
        with
        | Some secondLf, Some thirdLf ->
            let baseCols = PolarsSchemaExtractor.GetColumnNames baseLf |> Set.ofArray
            let safeSecondLf, colsAfter2, _ = disambiguateColumns baseCols secondLf "_second"
            let safeThirdLf, _, _ = disambiguateColumns colsAfter2 thirdLf "_third"
            Some(QueryOp.HorizontalConcat [| safeSecondLf; safeThirdLf |])
        | _ -> None

    /// Compiles Explode (SelectMany over collection property) into QueryOp.Explode and optional renames
    let compileExplode
        (colLambda: LambdaExpression)
        (resLambdaOpt: LambdaExpression option)
        : QueryOp list option =
        extractExplodeColumn colLambda
        |> Option.map (fun colName ->
            let explodeOp =
                QueryOp.Explode
                    {
                        ColumnNames = [| colName |]
                        EmptyAsNull = true
                        KeepNulls = true
                    }

            let renameOps =
                match resLambdaOpt with
                | Some resLambda when resLambda.Parameters.Count >= 2 ->
                    let itemParam = resLambda.Parameters.[1]
                    extractBinaryResultRenames resLambda (Some(itemParam.Name, colName))
                    |> buildRenameOps
                | _ -> []

            renameOps @ [ explodeOp ])

    /// Compiles CrossJoin (uncorrelated SelectMany) into a Cross Join and optional renames
    let compileCrossJoin (innerExpr: Expression) (resLambdaOpt: LambdaExpression option) : QueryOp list option =
        SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr
        |> Option.map (fun rightLf ->
            let joinOp =
                QueryOp.Join
                    {
                        RightLf = rightLf
                        LeftOn = [||]
                        RightOn = [||]
                        How = PlJoinType.Cross
                        Suffix = Some "_right"
                        InvertSides = false
                        Coalesce = PlJoinCoalesce.KeepColumns
                    }

            let renameOps =
                resLambdaOpt
                |> Option.map (fun r -> extractBinaryResultRenames r None |> buildRenameOps)
                |> Option.defaultValue []

            renameOps @ [ joinOp ])

    /// Compiles CountBy into a GroupBy(Key, Len() as Value) operation
    let compileCountBy (elemType: Type) (baseLf: LazyFrameHandle) (keyLambda: LambdaExpression) : QueryOp option =
        let keyParam = keyLambda.Parameters.[0]
        let ctx = createContextForType elemType baseLf keyParam.Name

        ExprTranslator.tryTranslateWithContext ctx keyLambda.Body
        |> Option.map (fun keyExpr ->
            QueryOp.GroupBy
                {
                    Keys = [| PolarsWrapper.Alias(keyExpr, "Key") |]
                    Aggs = [| PolarsWrapper.Alias(PolarsWrapper.Len(), "Value") |]
                    Having = None
                    MaintainOrder = false
                })

    /// Unified GroupBy compiler powering both single-key and multi-key GroupBy pipelines (C# LINQ & F# QueryBuilder)
    let compileUnifiedGroupBy
        (lf: LazyFrameHandle)
        (ctx: GroupFoldContext)
        (projBody: Expression)
        (groupParamName: string)
        (isKeyArg: Expression -> bool)
        : QueryOp list option =

        let keyParam = ctx.KeyLambda.Parameters.[0]
        let keyCtx = createContextForType keyParam.Type lf keyParam.Name

        let elemExprOpt =
            ctx.ElemLambdaOpt
            |> Option.bind (fun el ->
                let elParam = el.Parameters.[0]
                let elCtx = createContextForType elParam.Type lf elParam.Name
                ExprTranslator.tryTranslateWithContext elCtx el.Body)

        let elemType = keyParam.Type

        let wholeRowStructOpt: ExprHandle option =
            try
                let props = elemType.GetProperties(BindingFlags.Public ||| BindingFlags.Instance)

                if props.Length > 0 then
                    let colExprs = props |> Array.map (fun p -> PolarsWrapper.Col p.Name)
                    Some(PolarsWrapper.AsStruct colExprs)
                else
                    None
            with _ ->
                None

        let translateKeyPart (e: Expression) : ExprHandle option =
            match ExprTranslator.tryTranslateWithContext keyCtx e with
            | Some h -> Some h
            | None ->
                match e with
                | MemberAccess(:? ParameterExpression as p, mem) when p.Name = keyParam.Name ->
                    Some(PolarsWrapper.Col mem.Name)
                | _ -> None

        let keyPairsOpt: (string * Expression) list option =
            match tryExtractMemberBindings ctx.KeyLambda.Body with
            | Some pairs -> Some pairs
            | None ->
                let name =
                    match ctx.KeyLambda.Body with
                    | ExtractColumnName col -> col
                    | _ -> "Key"

                Some [ (name, ctx.KeyLambda.Body) ]

        let keyDefinitionsOpt: (string * ExprHandle) list option =
            keyPairsOpt
            |> Option.bind (fun pairs ->
                let translated =
                    pairs
                    |> List.choose (fun (name, expr) -> translateKeyPart expr |> Option.map (fun h -> (name, h)))

                if translated.Length = pairs.Length then
                    Some translated
                else
                    None)

        match keyDefinitionsOpt with
        | None -> None
        | Some keyDefs ->
            let isKeyExpression (e: Expression) : bool =
                let cleanE = ExprTranslator.unwrap e

                if isKeyArg cleanE then
                    true
                else
                    match cleanE with
                    | MemberAccess(target, _) when isKeyArg target -> true
                    | MemberAccess(MemberAccess(p, keyProp), _) when
                        not (isNull p) && p.NodeType = ExpressionType.Parameter && keyProp.Name = "Key"
                        ->
                        true
                    | _ -> false

            let memberBindingsOpt =
                match tryExtractMemberBindings projBody with
                | Some bindings -> Some bindings
                | None ->
                    let cleanProj = ExprTranslator.unwrap projBody
                    if isKeyExpression cleanProj then
                        let name =
                            match cleanProj with
                            | MemberAccess(_, m) when m.Name <> "Key" -> m.Name
                            | _ ->
                                if keyDefs.Length = 1 then fst keyDefs.[0] else "Key"
                        Some [ (name, projBody) ]
                    else
                        None

            match memberBindingsOpt with
            | None -> None
            | Some memberBindings ->
                let memberNames = memberBindings |> List.map fst
                let args = memberBindings |> List.map snd

                let keyNameToOutputAliasMap =
                    keyDefs
                    |> List.map (fun (origKeyName, _) ->
                        let outputColName =
                            List.zip memberNames args
                            |> List.tryPick (fun (memName, rawArg) ->
                                let arg = ExprTranslator.unwrap rawArg

                                match arg with
                                | MemberAccess(MemberAccess(_, k), m) when k.Name = "Key" && m.Name = origKeyName ->
                                    Some memName
                                | MemberAccess(p, m) when isKeyArg p && m.Name = origKeyName -> Some memName
                                | _ when isKeyArg arg && keyDefs.Length = 1 -> Some memName
                                | _ -> None)
                            |> Option.defaultValue origKeyName

                        origKeyName, outputColName)
                    |> Map.ofList

                let keyAliasedExprs =
                    keyDefs
                    |> List.map (fun (origKeyName, rawKeyExpr) ->
                        let outputColName = keyNameToOutputAliasMap.[origKeyName]
                        PolarsWrapper.Alias(rawKeyExpr, outputColName))
                    |> List.toArray

                let nonKeyArgs =
                    List.zip memberNames args
                    |> List.filter (fun (_, argExpr) -> not (isKeyExpression argExpr))

                let scalarColOpt =
                    match ctx.ElemLambdaOpt with
                    | Some el ->
                        match el.Body with
                        | ExtractColumnName c -> Some c
                        | _ -> None
                    | None ->
                        tryGetScalarColumnName keyParam.Type lf

                let targetColExprOpt =
                    elemExprOpt |> Option.orElse (scalarColOpt |> Option.map PolarsWrapper.Col)

                let tryTranslateSingleAgg (colName: string) (rawArgExpr: Expression) : ExprHandle option =
                    let argExpr = ExprTranslator.unwrap rawArgExpr

                    let isGroupParam (e: Expression) =
                        match ExprTranslator.unwrap e with
                        | :? ParameterExpression as p -> p.Name = groupParamName
                        | _ -> false

                    let isGroupReductionCall ( targetObj: Expression) (callArgs: Expression list) =
                        isNull targetObj && callArgs.Length = 1 && isGroupParam callArgs.Head
                        || not (isNull targetObj) && callArgs.IsEmpty && isGroupParam targetObj

                    let isGroupCountCall (m: MethodInfo) (targetObj: Expression) (callArgs: Expression list) =
                        (m.Name = "Count" || m.Name = "LongCount") && isGroupReductionCall targetObj callArgs

                    let tryMatchNestedListAgg (e: Expression) =
                        match e with
                        | MethodCall(mList, null, [ MethodCall(mSel, null, [ gArg; CleanLambda selLambda ]) ])
                        | MethodCall(mList, MethodCall(mSel, null, [ gArg; CleanLambda selLambda ]), [])
                        | MethodCall(mList, MethodCall(mSel, gArg, [ CleanLambda selLambda ]), []) when
                            (mList.Name = "ToList" || mList.Name = "ToArray")
                            && mSel.Name = "Select"
                            && isGroupParam gArg
                            ->
                            Some selLambda
                        | _ -> None

                    match argExpr with
                    | MethodCall(m, target, callArgs) when isGroupCountCall m target callArgs ->
                        let core =
                            if m.Name = "LongCount" then
                                let len = PolarsWrapper.Len()

                                let int64Dtype =
                                    PolarsWrapper.DataTypeExprFromDataType(
                                        PolarsWrapper.NewPrimitiveType(int PlDataType.Int64)
                                    )

                                PolarsWrapper.ExprCast(len, int64Dtype, strict = false, wrapNumerical = false)
                            else
                                PolarsWrapper.Len()

                        Some(PolarsWrapper.Alias(core, colName))

                    | MethodCall(m, target, callArgs) when
                        isGroupReductionCall target callArgs && targetColExprOpt.IsSome
                        ->
                        let clonedColExpr = PolarsWrapper.CloneExpr targetColExprOpt.Value

                        ExprTranslator.translateReductionOp m.Name clonedColExpr
                        |> Option.map (fun core -> PolarsWrapper.Alias(core, colName))

                    | _ ->
                        match tryMatchNestedListAgg argExpr with
                        | Some selLambda ->
                            let selParam = selLambda.Parameters.[0]
                            let selCtx = createContextForType selParam.Type lf selParam.Name

                            let colExprOpt =
                                match ExprTranslator.tryResolveColumnNameWithContext selCtx selLambda.Body with
                                | Some cName -> Some(PolarsWrapper.Col cName)
                                | None -> ExprTranslator.tryTranslateWithContext selCtx selLambda.Body

                            colExprOpt |> Option.map (fun cExpr -> PolarsWrapper.Alias(cExpr, colName))
                        | None ->
                            AggTranslator.tryTranslateAggWithScalarCol
                                groupParamName
                                scalarColOpt
                                wholeRowStructOpt
                                argExpr
                                colName

                let translatedAggOpts =
                    nonKeyArgs
                    |> List.map (fun (colName, argExpr) -> tryTranslateSingleAgg colName argExpr)

                if not (translatedAggOpts |> List.forall Option.isSome) then
                    None
                else
                    let aggs = translatedAggOpts |> List.choose id

                    let resolveResultColName (e: Expression) : string option =
                        let cleanE = ExprTranslator.unwrap e

                        match cleanE with
                        | MemberAccess(_, m) when m.Name = "Key" && keyDefs.Length = 1 ->
                            Some keyNameToOutputAliasMap.[fst keyDefs.[0]]
                        | MemberAccess(MemberAccess(_, k), m) when k.Name = "Key" ->
                            keyNameToOutputAliasMap.TryFind m.Name |> Option.orElse (Some m.Name)
                        | MemberAccess(p, m) when isKeyArg p ->
                            keyNameToOutputAliasMap.TryFind m.Name |> Option.orElse (Some m.Name)
                        | MemberAccess(p, m) when not (isNull p) && p.NodeType = ExpressionType.Parameter -> Some m.Name
                        | MethodCall _ as aggCall ->
                            List.zip memberNames args
                            |> List.tryPick (fun (colName, argExpr) ->
                                if (ExprTranslator.unwrap argExpr).ToString() = aggCall.ToString() then
                                    Some colName
                                else
                                    None)
                        | _ -> None

                    let extraHavingAggs = ResizeArray<string * ExprHandle>()

                    let resolveHavingAggColName (aggCall: MethodCallExpression) : string option =
                        match resolveResultColName aggCall with
                        | Some colName -> Some colName
                        | None ->
                            let aggStr = aggCall.ToString()
                            match extraHavingAggs |> Seq.tryFindIndex (fun (str, _) -> str = aggStr) with
                            | Some idx -> Some (sprintf "__having_agg_%d" idx)
                            | None ->
                                let colName = sprintf "__having_agg_%d" extraHavingAggs.Count
                                match tryTranslateSingleAgg colName aggCall with
                                | Some h ->
                                    extraHavingAggs.Add((aggStr, h))
                                    Some colName
                                | None -> None

                    let rec translateHaving (expr: Expression) : ExprHandle option =
                        match expr with
                        | Binary(op, left, right) ->
                            match translateHaving left, translateHaving right with
                            | Some l, Some r ->
                                try
                                    Some(ExprTranslator.translateBinary op l r)
                                with _ ->
                                    None
                            | _ -> None
                        | Constant _ -> ExprTranslator.tryTranslateWithContext TranslationContext.Empty expr
                        | other ->
                            match resolveResultColName other with
                            | Some colName -> Some(PolarsWrapper.Col colName)
                            | None ->
                                match ExprTranslator.unwrap other with
                                | :? MethodCallExpression as aggCall ->
                                    resolveHavingAggColName aggCall |> Option.map PolarsWrapper.Col
                                | _ -> None

                    let havingFilterOpt =
                        ctx.HavingPreds
                        |> List.map (fun havingPred -> translateHaving havingPred.Body)

                    if not (havingFilterOpt |> List.forall Option.isSome) then
                        None
                    else
                        let havingFilterOps = havingFilterOpt |> List.choose id |> List.map QueryOp.Filter
                        let allAggs = aggs @ (extraHavingAggs |> Seq.map snd |> Seq.toList)

                        let groupBySpec =
                            {
                                Keys = keyAliasedExprs
                                Aggs = allAggs |> List.toArray
                                Having = None
                                MaintainOrder = false
                            }

                        let groupByOp = QueryOp.GroupBy groupBySpec

                        let sortSpecs =
                            ctx.SortStages
                            |> List.choose (fun (sortLambda, isDesc) ->
                                resolveResultColName sortLambda.Body
                                |> Option.map (fun colName ->
                                    {
                                        Expr = PolarsWrapper.Col colName
                                        Descending = isDesc
                                        NullsLast = false
                                    }))

                        let sortOps = if sortSpecs.IsEmpty then [] else [ QueryOp.Sort sortSpecs ]

                        let finalSelectOp =
                            let orderedColExprs =
                                memberNames |> List.map PolarsWrapper.Col |> List.toArray

                            QueryOp.Select orderedColExprs

                        Some(finalSelectOp :: sortOps @ havingFilterOps @ [ groupByOp ])

    /// Applies a list of QueryOp to a target LazyFrameHandle in forward order.
    let applyNativeOps (sourceLf: LazyFrameHandle) (ops: QueryOp list) : LazyFrameHandle =
        ops
        |> List.fold
            (fun currentLf op ->
                match op with
                | QueryOp.WithColumns exprs -> PolarsWrapper.LazyWithColumns(currentLf, exprs)

                | QueryOp.WithRowIndex(name, offsetOpt) ->
                    let offsetNullable =
                        match offsetOpt with
                        | Some o -> Nullable(int o)
                        | None -> Nullable 0

                    PolarsWrapper.LazyFrameWithRowIndex(currentLf, name, offsetNullable)

                | QueryOp.Filter exprHandle -> PolarsWrapper.LazyFilter(currentLf, exprHandle)

                | QueryOp.Select exprs -> PolarsWrapper.LazySelect(currentLf, exprs)

                | QueryOp.Slice(offset, len) -> PolarsWrapper.LazySlice(currentLf, offset, len)

                | QueryOp.Sort specs ->
                    let exprs = specs |> List.map (fun s -> s.Expr) |> List.toArray
                    let descending = specs |> List.map (fun s -> s.Descending) |> List.toArray
                    let nullsLast = specs |> List.map (fun s -> s.NullsLast) |> List.toArray
                    PolarsWrapper.LazyFrameSort(currentLf, exprs, descending, nullsLast, maintainOrder = false)

                | QueryOp.Join spec ->
                    let leftLf, rightLf, leftOn, rightOn, suffix =
                        if spec.InvertSides then
                            spec.RightLf,
                            currentLf,
                            spec.RightOn,
                            spec.LeftOn,
                            Option.toObj (spec.Suffix |> Option.orElse (Some "_left"))
                        else
                            currentLf, spec.RightLf, spec.LeftOn, spec.RightOn, Option.toObj spec.Suffix

                    PolarsWrapper.Join(
                        leftLf,
                        rightLf,
                        leftOn,
                        rightOn,
                        spec.How,
                        suffix,
                        PlJoinValidation.ManyToMany,
                        spec.Coalesce,
                        PlJoinMaintainOrder.None,
                        PlJoinSide.None,
                        false,
                        Nullable(),
                        0UL
                    )

                | QueryOp.GroupBy spec ->
                    PolarsWrapper.LazyGroupByAgg(
                        currentLf,
                        spec.Keys,
                        spec.Aggs,
                        Option.toObj spec.Having,
                        spec.MaintainOrder
                    )

                | QueryOp.Unique spec ->
                    let selector =
                        match spec.SubsetCols with
                        | Some cols when cols.Length > 0 -> PolarsWrapper.SelectorCols cols
                        | _ -> null

                    PolarsWrapper.LazyUnique(currentLf, selector, spec.Keep, spec.MaintainOrder)

                | QueryOp.Concat spec ->
                    let alignSchema (baseLf: LazyFrameHandle) (targetLf: LazyFrameHandle) : LazyFrameHandle =
                        let baseFields = getLazySchemaFields baseLf
                        let targetFields = getLazySchemaFields targetLf

                        let baseNames = baseFields |> Array.map fst
                        let targetNames = targetFields |> Array.map fst

                        if Set.ofArray baseNames = Set.ofArray targetNames then
                            let targetFieldMap = targetFields |> Map.ofArray

                            let reorderedAndCastedExprs =
                                baseFields
                                |> Array.map (fun (colName, baseDt) ->
                                    let targetDt = targetFieldMap.[colName]
                                    let colExpr = PolarsWrapper.Col colName

                                    if not (PolarsWrapper.DataTypeEq(baseDt, targetDt)) then
                                        let dtExpr = PolarsWrapper.DataTypeExprFromDataType baseDt
                                        PolarsWrapper.ExprCast(colExpr, dtExpr, strict = false, wrapNumerical = false)
                                    else
                                        colExpr)

                            PolarsWrapper.LazySelect(targetLf, reorderedAndCastedExprs)
                        else
                            targetLf

                    let alignedOtherLf = alignSchema currentLf spec.OtherLf

                    let handles =
                        if spec.Prepend then
                            [| alignedOtherLf; currentLf |]
                        else
                            [| currentLf; alignedOtherLf |]

                    PolarsWrapper.LazyConcat(handles, PlConcatType.Vertical, false, true)

                | QueryOp.HorizontalConcat otherLfs ->
                    let allHandles = Array.append [| currentLf |] otherLfs
                    PolarsWrapper.LazyConcat(allHandles, PlConcatType.Horizontal, false, true)

                | QueryOp.Explode spec ->
                    let selector = PolarsWrapper.SelectorCols spec.ColumnNames
                    PolarsWrapper.LazyExplode(currentLf, selector, spec.EmptyAsNull, spec.KeepNulls)

                | QueryOp.Rename spec ->
                    PolarsWrapper.LazyRename(currentLf, spec.ExistingNames, spec.NewNames, strict = false)

                | QueryOp.SkipLast count ->
                    let withIndexLf =
                        PolarsWrapper.LazyFrameWithRowIndex(currentLf, "__row_idx", Nullable 0)

                    let rowIdxCol = PolarsWrapper.Col "__row_idx"
                    let countLit = PolarsWrapper.Lit(uint32 count)
                    let lhsExpr = PolarsWrapper.Add(rowIdxCol, countLit)
                    let totalLen = PolarsWrapper.Len()
                    let filterExpr = PolarsWrapper.Lt(lhsExpr, totalLen)

                    let filteredLf = PolarsWrapper.LazyFilter(withIndexLf, filterExpr)
                    let dropSelector = PolarsWrapper.SelectorCols [| "__row_idx" |]
                    PolarsWrapper.LazyFrameDrop(filteredLf, dropSelector)

                | QueryOp.Reverse -> PolarsWrapper.LazyFrameReverse currentLf

                | QueryOp.SelectPassthrough -> currentLf)
            (PolarsWrapper.LazyClone sourceLf)

