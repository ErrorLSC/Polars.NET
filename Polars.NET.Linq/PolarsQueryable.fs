namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core
open Polars.NET.Core.Helpers
open System.Threading
open System.Threading.Tasks

/// Internal classification of resolved predicate pushdown state
type private ResolvedPredicate =
    | Pushdown of ExprHandle
    | Client of effectiveType: Type * pred: Delegate

/// Row selection strategy for scalar single-row operations
type private RowFetchKind =
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
type private GroupFoldContext =
    {
        KeyLambda: LambdaExpression
        ElemLambdaOpt: LambdaExpression option
        HavingPreds: LambdaExpression list
        SortStages: (LambdaExpression * bool) list
    }

/// Strongly-typed IGrouping implementation for GroupJoin materialization.
type internal PolarsGrouping<'TKey, 'TElement>(key: 'TKey, elements: 'TElement array) =
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

type internal QueryMaterializerResolver =
    /// Resolves the effective IDataFrameMaterializer using explicit instance or registered fallback
    static member Resolve(matOpt: IDataFrameMaterializer option) : IDataFrameMaterializer =
        match matOpt with
        | Some mat when not (isNull (box mat)) -> mat
        | _ ->
            let defaultMat = DataFrameMaterializerRegistry.Default

            if not (isNull (box defaultMat)) then
                defaultMat
            else
                failwith "No DataFrameMaterializer registered. Ensure upper API layers configured a materializer."

/// Strongly-typed IQueryProvider backed by Polars.NET.Core
type internal PolarsQueryProvider(initialLazyFrame: LazyFrameHandle, materializer: IDataFrameMaterializer) =
    let clonedLf = PolarsWrapper.LazyClone initialLazyFrame

    /// Creates an appropriate TranslationContext based on whether the sequence element type
    /// is a scalar/primitive (bound to the underlying single physical column) or a composite entity.
    static member createContextForType (elemType: Type) (lf: LazyFrameHandle) (paramName: string) : TranslationContext =
        SubqueryDecorrelator.createContextForType elemType lf paramName

    /// Extracts the underlying sequence element type from generic collections/arrays
    static member private GetSequenceElementType(t: Type) =
        if t.IsGenericType then t.GetGenericArguments().[0]
        elif t.IsArray then t.GetElementType()
        else t

    /// Resolves the single physical column name if the sequence is scalar (e.g. string, int, double).
    static member internal tryGetScalarColumnName (elemType: Type) (lf: LazyFrameHandle) : string option =
        if PolarsTypeHelper.IsScalarType elemType || elemType = typeof<string> then
            let colNames = PolarsSchemaExtractor.GetColumnNames lf
            if colNames.Length = 1 then Some colNames.[0] else None
        else
            None

    /// Dispatches and resolves predicate into either native ExprHandle pushdown or composed client Func<obj, bool>
    static member private TryResolvePredicate
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
                if not hasClientPreds && Option.isNone clientProjOpt then
                    let ctx =
                        PolarsQueryProvider.createContextForType elemType nativeLf predLambda.Parameters.[0].Name

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

    /// Helper to enforce client evaluation policy on unpushed scalar/terminal predicates (e.g. First(pred), Any(pred))
    static member internal EnforceClientPredicatePolicy(stage: string, predLambdaOpt: LambdaExpression option) =
        let policy = CoreConfig.ClientEvaluationPolicy
        match policy with
        | ClientEvaluationPolicy.Allow -> ()
        | ClientEvaluationPolicy.Warn ->
            let text = match predLambdaOpt with Some p -> p.ToString() | None -> "Predicate"
            System.Diagnostics.Trace.TraceWarning(
                sprintf "[Polars.NET Warning] Predicate in '%s' (%s) could not be pushed down and is evaluated on client." stage text
            )
        | ClientEvaluationPolicy.Strict
        | _ ->
            let text = match predLambdaOpt with Some p -> p.ToString() | None -> "Predicate"
            raise (PolarsPushdownException(stage, text, sprintf "Predicate in '%s' could not be pushed down to native Polars engine." stage))

    /// Post-processes raw scalar aggregate results (handling NaN, empty collections, and F# Option/ValueOption types)
    static member private BoxScalarResult<'TResult>(rawScalar: obj, methodName: string) : 'TResult =
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

    /// Unified row cursor executor for First, Last, Single (and their OrDefault variants)
    member private this.ExecuteRowFetch<'TResult>
        (kind: RowFetchKind)
        (isOrDefault: bool)
        (userDefValOpt: obj option)
        (nativeLf: LazyFrameHandle)
        (hasClientPreds: bool)
        (clientProjOpt: (Type * Func<obj, obj>) option)
        (elemType: Type)
        (predLambdaOpt: LambdaExpression option)
        : 'TResult =

        let mat = QueryMaterializerResolver.Resolve(Some materializer)

        let resolvedPred =
            PolarsQueryProvider.TryResolvePredicate hasClientPreds clientProjOpt elemType nativeLf predLambdaOpt

        let userDefVal =
            defaultArg
                userDefValOpt
                (if elemType.IsValueType then
                     Activator.CreateInstance elemType
                 else
                     null)

        match resolvedPred with
        | ValueSome(Pushdown filterExpr) ->
            let filteredLf = PolarsWrapper.LazyFilter(nativeLf, filterExpr)

            let offset, len =
                match kind with
                | First -> 0L, 1u
                | Last -> -1L, 1u
                | Single -> 0L, 2u

            let slicedLf = PolarsWrapper.LazySlice(filteredLf, offset, len)
            let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto)

            match kind with
            | First ->
                if isOrDefault then
                    let invoker = LinqReflectionCache.GetMaterializerFirstOrDefaultInvoker elemType
                    invoker.Invoke(mat, dfHandle, null, userDefVal) :?> 'TResult
                else
                    let invoker = LinqReflectionCache.GetMaterializerFirstInvoker elemType
                    invoker.Invoke(mat, dfHandle, null) :?> 'TResult
            | Last ->
                if isOrDefault then
                    let invoker = LinqReflectionCache.GetMaterializerLastOrDefaultInvoker elemType
                    invoker.Invoke(mat, dfHandle, null, userDefVal) :?> 'TResult
                else
                    let invoker = LinqReflectionCache.GetMaterializerLastInvoker elemType
                    invoker.Invoke(mat, dfHandle, null) :?> 'TResult
            | Single ->
                if isOrDefault then
                    let invoker = LinqReflectionCache.GetMaterializerSingleOrDefaultInvoker elemType
                    invoker.Invoke(mat, dfHandle, null, userDefVal) :?> 'TResult
                else
                    let invoker = LinqReflectionCache.GetMaterializerSingleInvoker elemType
                    invoker.Invoke(mat, dfHandle, null) :?> 'TResult

        | ValueSome(Client(srcType, combinedPred)) ->
            PolarsQueryProvider.EnforceClientPredicatePolicy(kind.ToString(), predLambdaOpt)
            let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

            let defaultVal =
                if srcType.IsValueType then
                    Activator.CreateInstance srcType
                else
                    null

            let rawEntity =
                match kind with
                | First ->
                    if isOrDefault then
                        let invoker = LinqReflectionCache.GetMaterializerFirstOrDefaultInvoker srcType
                        invoker.Invoke(mat, dfHandle, combinedPred, defaultVal)
                    else
                        let invoker = LinqReflectionCache.GetMaterializerFirstInvoker srcType
                        invoker.Invoke(mat, dfHandle, combinedPred)
                | Last ->
                    if isOrDefault then
                        let invoker = LinqReflectionCache.GetMaterializerLastOrDefaultInvoker srcType
                        invoker.Invoke(mat, dfHandle, combinedPred, defaultVal)
                    else
                        let invoker = LinqReflectionCache.GetMaterializerLastInvoker srcType
                        invoker.Invoke(mat, dfHandle, combinedPred)
                | Single ->
                    if isOrDefault then
                        let invoker = LinqReflectionCache.GetMaterializerSingleOrDefaultInvoker srcType
                        invoker.Invoke(mat, dfHandle, combinedPred, defaultVal)
                    else
                        let invoker = LinqReflectionCache.GetMaterializerSingleInvoker srcType
                        invoker.Invoke(mat, dfHandle, combinedPred)

            match clientProjOpt with
            | Some(_, projFunc) ->
                if isNull rawEntity && not srcType.IsValueType then
                    userDefVal :?> 'TResult
                else
                    projFunc.Invoke rawEntity :?> 'TResult
            | None -> rawEntity :?> 'TResult

        | ValueNone ->
            // No predicate supplied: slice natively when no client filter exists
            let dfHandle =
                if not hasClientPreds then
                    let offset, len =
                        match kind with
                        | First -> 0L, 1u
                        | Last -> -1L, 1u
                        | Single -> 0L, 2u

                    let slicedLf = PolarsWrapper.LazySlice(nativeLf, offset, len)
                    PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto)
                else
                    PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

            match clientProjOpt with
            | Some(sourceType, projFunc) ->
                let defaultVal =
                    if sourceType.IsValueType then
                        Activator.CreateInstance sourceType
                    else
                        null

                let rawEntity =
                    match kind with
                    | First ->
                        if isOrDefault then
                            let invoker = LinqReflectionCache.GetMaterializerFirstOrDefaultInvoker sourceType
                            invoker.Invoke(mat, dfHandle, null, defaultVal)
                        else
                            let invoker = LinqReflectionCache.GetMaterializerFirstInvoker sourceType
                            invoker.Invoke(mat, dfHandle, null)
                    | Last ->
                        if isOrDefault then
                            let invoker = LinqReflectionCache.GetMaterializerLastOrDefaultInvoker sourceType
                            invoker.Invoke(mat, dfHandle, null, defaultVal)
                        else
                            let invoker = LinqReflectionCache.GetMaterializerLastInvoker sourceType
                            invoker.Invoke(mat, dfHandle, null)
                    | Single ->
                        let height = PolarsWrapper.DataFrameHeight dfHandle

                        if isOrDefault && height = 0L then
                            null
                        elif not isOrDefault && height = 0L then
                            invalidOp "Sequence contains no elements."
                        elif height > 1L then
                            invalidOp "Sequence contains more than one element."
                        else
                            let invoker = LinqReflectionCache.GetMaterializerSingleInvoker sourceType
                            invoker.Invoke(mat, dfHandle, null)

                if isNull rawEntity && not sourceType.IsValueType then
                    userDefVal :?> 'TResult
                else
                    projFunc.Invoke rawEntity :?> 'TResult

            | None ->
                match kind with
                | First ->
                    if isOrDefault then
                        let invoker = LinqReflectionCache.GetMaterializerFirstOrDefaultInvoker elemType
                        invoker.Invoke(mat, dfHandle, null, userDefVal) :?> 'TResult
                    else
                        let invoker = LinqReflectionCache.GetMaterializerFirstInvoker elemType
                        invoker.Invoke(mat, dfHandle, null) :?> 'TResult
                | Last ->
                    if isOrDefault then
                        let invoker = LinqReflectionCache.GetMaterializerLastOrDefaultInvoker elemType
                        invoker.Invoke(mat, dfHandle, null, userDefVal) :?> 'TResult
                    else
                        let invoker = LinqReflectionCache.GetMaterializerLastInvoker elemType
                        invoker.Invoke(mat, dfHandle, null) :?> 'TResult
                | Single ->
                    if isOrDefault then
                        let invoker = LinqReflectionCache.GetMaterializerSingleOrDefaultInvoker elemType
                        invoker.Invoke(mat, dfHandle, null, userDefVal) :?> 'TResult
                    else
                        let invoker = LinqReflectionCache.GetMaterializerSingleInvoker elemType
                        invoker.Invoke(mat, dfHandle, null) :?> 'TResult

    /// Core scalar execution pipeline: compiles native plan, executes native reductions,
    /// and extracts scalar results via IDataFrameMaterializer.
    member private this.ExecuteScalarInternal<'TResult>(expression: Expression) : 'TResult =
        let targetType = typeof<'TResult>
        let mat = QueryMaterializerResolver.Resolve(Some materializer)

        // Helper to extract native LazyFrame, check if client filter exists, and extract client projection (sourceType * (obj -> obj))
        let resolveQueryPlan (src: Expression) : LazyFrameHandle * bool * (Type * Func<obj, obj>) option =
            let query = (this :> IQueryProvider).CreateQuery src

            match query with
            | :? IInternalPlanSource as ps -> ps.GetPlanInfo()
            | :? IPolarsPlanSource as ps -> ps.GetCompiledLazyFrameHandle(), false, None
            | _ ->
                let getPlanMethod =
                    query
                        .GetType()
                        .GetMethod(
                            "GetCompiledPlan",
                            BindingFlags.Instance ||| BindingFlags.NonPublic ||| BindingFlags.Public
                        )

                if not (isNull getPlanMethod) then
                    let planResult = getPlanMethod.Invoke(query, null)
                    let fields = Reflection.FSharpValue.GetTupleFields planResult
                    let nativeLf = fields.[0] :?> LazyFrameHandle
                    let predsObj = fields.[1] :?> IEnumerable
                    let hasClientPreds = predsObj.GetEnumerator().MoveNext()

                    let clientProjOpt =
                        if fields.Length > 2 && not (isNull fields.[2]) then
                            fields.[2] :?> (Type * Func<obj, obj>) option
                        else
                            None

                    let hasFallback = fields.Length > 3 && fields.[3] :?> bool
                    nativeLf, hasClientPreds || hasFallback, clientProjOpt
                else
                    failwithf "Unable to extract LazyFrame from query source: %A" src

        // Helper to map standard reduction method names to Polars aggregate expressions
        let resolveScalarAggExpr (methodName: string) (colExpr: ExprHandle) : ExprHandle =
            match methodName with
            | "Max" -> PolarsWrapper.Max colExpr
            | "Min" -> PolarsWrapper.Min colExpr
            | "Sum" -> PolarsWrapper.Sum colExpr
            | "Average" -> PolarsWrapper.Mean colExpr
            | other -> failwithf "Unsupported scalar aggregate operator: %s" other

        let evaluatedExpr = PartialEvaluator.eval expression

        match evaluatedExpr with
        // =========================================================================
        // 1. Count() / LongCount()
        // =========================================================================
        | MethodCall(m, null, source :: rest) when m.Name = "Count" || m.Name = "LongCount" ->
            let chunkSizeOpt = PolarsQuery<obj>.TryExtractChunkSize source
            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source
            let elemType = PolarsQueryProvider.GetSequenceElementType source.Type

            let predOpt =
                match rest with
                | [ StripQuotes(:? LambdaExpression as p) ] -> Some p
                | _ -> None

            // Helper: adjusts raw row count to chunk count when no chunk-level predicate is present
            let adjustForChunk (rowCount: int64) : int64 =
                match chunkSizeOpt with
                | Some size when size > 0 ->
                    if rowCount <= 0L then
                        0L
                    else
                        (rowCount + int64 size - 1L) / int64 size
                | _ -> rowCount

            let resolvedPred =
                PolarsQueryProvider.TryResolvePredicate hasClientPreds clientProjOpt elemType nativeLf predOpt

            match resolvedPred with
            | ValueSome(Pushdown filterExpr) ->
                // Native pushdown predicate (filters applied BEFORE chunking)
                let filteredLf = PolarsWrapper.LazyFilter(nativeLf, filterExpr)
                let lenExpr = PolarsWrapper.Len()
                let aggLf = PolarsWrapper.LazySelect(filteredLf, [| lenExpr |])
                let dfHandle = PolarsWrapper.LazyCollect(aggLf, PlEngine.Auto)
                let rawRowCount = mat.MaterializeScalar<int64>(dfHandle)
                let effectiveCount = adjustForChunk rawRowCount
                Convert.ChangeType(effectiveCount, targetType) :?> 'TResult

            | ValueSome(Client(srcType, combinedPred)) ->
                PolarsQueryProvider.EnforceClientPredicatePolicy("Count", predOpt)
                // Client-side predicate: could be an un-pushable row filter OR a predicate on TElement[] itself
                let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
                let invoker = LinqReflectionCache.GetMaterializerCountInvoker srcType
                let countVal = invoker.Invoke(mat, dfHandle, combinedPred)
                // If the predicate was applied at row-level, adjust for chunks;
                // if it operated directly on chunks, countVal is already chunk count.
                let effectiveCount =
                    if srcType.IsArray then
                        Convert.ToInt64 countVal
                    else
                        adjustForChunk (Convert.ToInt64 countVal)

                Convert.ChangeType(effectiveCount, targetType) :?> 'TResult

            | ValueNone ->
                // Parameterless Count() over the sequence
                if not hasClientPreds then
                    let lenExpr = PolarsWrapper.Len()
                    let aggLf = PolarsWrapper.LazySelect(nativeLf, [| lenExpr |])
                    let dfHandle = PolarsWrapper.LazyCollect(aggLf, PlEngine.Auto)
                    let rawRowCount = mat.MaterializeScalar<int64>(dfHandle)
                    let effectiveCount = adjustForChunk rawRowCount
                    Convert.ChangeType(effectiveCount, targetType) :?> 'TResult
                else
                    let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

                    let elemTypeActual =
                        match clientProjOpt with
                        | Some(sourceType, _) -> sourceType
                        | None -> elemType

                    let invoker = LinqReflectionCache.GetMaterializerCountInvoker elemTypeActual
                    let countVal = invoker.Invoke(mat, dfHandle, null)
                    let effectiveCount = adjustForChunk (Convert.ToInt64 countVal)
                    Convert.ChangeType(effectiveCount, targetType) :?> 'TResult

        // =========================================================================
        // 2. Any
        // =========================================================================
        | MethodCall(m, null, source :: rest) when m.Name = "Any" ->
            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source
            let elemType = PolarsQueryProvider.GetSequenceElementType source.Type

            let predOpt =
                match rest with
                | [ StripQuotes(:? LambdaExpression as p) ] -> Some p
                | _ -> None

            let resolvedPred =
                PolarsQueryProvider.TryResolvePredicate hasClientPreds clientProjOpt elemType nativeLf predOpt

            match resolvedPred with
            | ValueSome(Pushdown filterExpr) ->
                let filteredLf = PolarsWrapper.LazyFilter(nativeLf, filterExpr)
                let slicedLf = PolarsWrapper.LazySlice(filteredLf, 0L, 1u)
                let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto)
                box (PolarsWrapper.DataFrameHeight dfHandle > 0L) :?> 'TResult

            | ValueSome(Client(srcType, combinedPred)) ->
                PolarsQueryProvider.EnforceClientPredicatePolicy("Any", predOpt)
                let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
                let invoker = LinqReflectionCache.GetMaterializerAnyInvoker srcType
                box (invoker.Invoke(mat, dfHandle, combinedPred)) :?> 'TResult

            | ValueNone ->
                if not hasClientPreds then
                    let slicedLf = PolarsWrapper.LazySlice(nativeLf, 0L, 1u)
                    let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto)
                    box (PolarsWrapper.DataFrameHeight dfHandle > 0L) :?> 'TResult
                else
                    let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

                    let srcType =
                        match clientProjOpt with
                        | Some(s, _) -> s
                        | None -> elemType

                    let invoker = LinqReflectionCache.GetMaterializerAnyInvoker srcType
                    box (invoker.Invoke(mat, dfHandle, null)) :?> 'TResult

        // =========================================================================
        // 3. All
        // =========================================================================
        | MethodCall(m, null, [ source; StripQuotes(:? LambdaExpression as predLambda) ]) when m.Name = "All" ->
            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source
            let elemType = PolarsQueryProvider.GetSequenceElementType source.Type

            let resolvedPred =
                PolarsQueryProvider.TryResolvePredicate hasClientPreds clientProjOpt elemType nativeLf (Some predLambda)

            match resolvedPred with
            | ValueSome(Pushdown filterExpr) ->
                // All(P) <=> Not Any(Not P)
                let notFilterExpr = PolarsWrapper.Not filterExpr
                let filteredLf = PolarsWrapper.LazyFilter(nativeLf, notFilterExpr)
                let slicedLf = PolarsWrapper.LazySlice(filteredLf, 0L, 1u)
                let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto)
                box (PolarsWrapper.DataFrameHeight dfHandle = 0L) :?> 'TResult

            | ValueSome(Client(srcType, combinedPred)) ->
                PolarsQueryProvider.EnforceClientPredicatePolicy("All", Some predLambda)
                let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
                let invoker = LinqReflectionCache.GetMaterializerAllInvoker srcType
                box (invoker.Invoke(mat, dfHandle, combinedPred)) :?> 'TResult

            | ValueNone -> box true :?> 'TResult

        // =========================================================================
        // 4. First / FirstOrDefault / Last / LastOrDefault / Single / SingleOrDefault
        // =========================================================================
        | MethodCall(m, null, source :: rest) when
            m.Name.StartsWith "First"
            || m.Name.StartsWith "Last"
            || m.Name.StartsWith "Single"
            ->

            let kind =
                if m.Name.StartsWith "First" then First
                elif m.Name.StartsWith "Last" then Last
                else Single

            let isOrDefault = m.Name.EndsWith "OrDefault"
            let elemType = PolarsQueryProvider.GetSequenceElementType source.Type

            // Guard: If sequence element is an IGrouping interface, evaluate via pipeline materializer
            if
                elemType.IsGenericType
                && elemType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>
            then
                let query = (this :> IQueryProvider).CreateQuery source :> IEnumerable
                let castMethod = typeof<Enumerable>.GetMethod("Cast").MakeGenericMethod elemType
                let seq = castMethod.Invoke(null, [| box query |]) :?> IEnumerable<'TResult>

                match kind with
                | First ->
                    if isOrDefault then
                        Seq.tryHead seq |> Option.defaultValue (Unchecked.defaultof<'TResult>)
                    else
                        Seq.head seq
                | Last ->
                    if isOrDefault then
                        Seq.tryLast seq |> Option.defaultValue (Unchecked.defaultof<'TResult>)
                    else
                        Seq.last seq
                | Single ->
                    if isOrDefault && Seq.isEmpty seq then
                        Unchecked.defaultof<'TResult>
                    else
                        Seq.exactlyOne seq
            else
                let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source

                let predLambdaOpt, defValOpt =
                    match rest with
                    | [ StripQuotes(:? LambdaExpression as p) ] -> Some p, None
                    | [ StripQuotes(:? LambdaExpression as p); defExpr ] -> Some p, tryEvaluate defExpr
                    | [ defExpr ] when not (typeof<LambdaExpression>.IsAssignableFrom defExpr.Type) ->
                        None, tryEvaluate defExpr
                    | _ -> None, None

                this.ExecuteRowFetch<'TResult>
                    kind
                    isOrDefault
                    defValOpt
                    nativeLf
                    hasClientPreds
                    clientProjOpt
                    elemType
                    predLambdaOpt

        // =========================================================================
        // 5. Max, Min, Sum, Average with or without selector
        // =========================================================================
        | MethodCall(m, null, source :: rest) when
            m.Name = "Max" || m.Name = "Min" || m.Name = "Sum" || m.Name = "Average"
            ->

            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source
            let elemType = PolarsQueryProvider.GetSequenceElementType source.Type
            let targetType = typeof<'TResult>

            let selectorOpt =
                match rest with
                | [ StripQuotes(:? LambdaExpression as sel) ] -> Some sel
                | _ -> None

            let translatedColExprOpt =
                if not hasClientPreds && clientProjOpt.IsNone then
                    match selectorOpt with
                    | Some selector ->
                        let ctx =
                            PolarsQueryProvider.createContextForType elemType nativeLf selector.Parameters.[0].Name

                        ExprTranslator.tryTranslateWithContext ctx selector.Body
                    | None ->
                        if PolarsTypeHelper.IsSupportedSimpleType targetType then
                            let colNames = PolarsSchemaExtractor.GetColumnNames nativeLf
                            let targetCol = if colNames.Length > 0 then colNames.[0] else ""
                            Some(PolarsWrapper.Col targetCol)
                        else
                            None
                else
                    None

            match translatedColExprOpt with
            | Some colExpr ->
                let aggExpr = resolveScalarAggExpr m.Name colExpr
                let aggLf = PolarsWrapper.LazySelect(nativeLf, [| aggExpr |])
                let dfHandle = PolarsWrapper.LazyCollect(aggLf, PlEngine.Auto)
                let rawScalar = mat.MaterializeScalar<obj>(dfHandle)
                PolarsQueryProvider.BoxScalarResult<'TResult>(rawScalar, m.Name)

            | None ->
                let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
                let h = PolarsWrapper.DataFrameHeight dfHandle

                if h = 0L then
                    PolarsQueryProvider.BoxScalarResult<'TResult>(null, m.Name)
                else
                    let accumType = if m.Name = "Average" then typeof<double> else targetType

                    match selectorOpt with
                    | Some selector ->
                        let compiledSel = selector.Compile()

                        match clientProjOpt with
                        | Some(sourceType, projFunc) ->
                            let composedSel =
                                let p = Expression.Parameter(sourceType, "x")
                                let castObj = Expression.Convert(p, typeof<obj>)
                                let projCall = Expression.Invoke(Expression.Constant(projFunc), castObj)
                                let castElem = Expression.Convert(projCall, elemType)
                                let selCall = Expression.Invoke(Expression.Constant(compiledSel), castElem)
                                let lType = typedefof<Func<_, _>>.MakeGenericType(sourceType, selector.ReturnType)
                                Expression.Lambda(lType, selCall, p).Compile()

                            let seed, folder, finisher =
                                LinqReflectionCache.CreateNumericFolder(sourceType, accumType, m.Name, Some composedSel)

                            let invoker =
                                LinqReflectionCache.GetMaterializerAggregateInvoker(sourceType, accumType)

                            let rawAcc = invoker.Invoke(mat, dfHandle, seed, folder)
                            Convert.ChangeType(finisher rawAcc h, targetType) :?> 'TResult

                        | None ->
                            let seed, folder, finisher =
                                LinqReflectionCache.CreateNumericFolder(elemType, accumType, m.Name, Some compiledSel)

                            let invoker =
                                LinqReflectionCache.GetMaterializerAggregateInvoker(elemType, accumType)

                            let rawAcc = invoker.Invoke(mat, dfHandle, seed, folder)
                            Convert.ChangeType(finisher rawAcc h, targetType) :?> 'TResult

                    | None ->
                        match clientProjOpt with
                        | Some(sourceType, projFunc) ->
                            let seed, folder, finisher =
                                LinqReflectionCache.CreateNumericFolder(sourceType, accumType, m.Name, Some projFunc)

                            let invoker =
                                LinqReflectionCache.GetMaterializerAggregateInvoker(sourceType, accumType)

                            let rawAcc = invoker.Invoke(mat, dfHandle, seed, folder)
                            Convert.ChangeType(finisher rawAcc h, targetType) :?> 'TResult

                        | None ->
                            let seed, folder, finisher =
                                LinqReflectionCache.CreateNumericFolder(elemType, accumType, m.Name, None)

                            let invoker =
                                LinqReflectionCache.GetMaterializerAggregateInvoker(elemType, accumType)

                            let rawAcc = invoker.Invoke(mat, dfHandle, seed, folder)
                            Convert.ChangeType(finisher rawAcc h, targetType) :?> 'TResult

        // =========================================================================
        // 6. ElementAt / ElementAtOrDefault
        // =========================================================================
        | MethodCall(m, null, [ source; indexExpr ]) when m.Name = "ElementAt" || m.Name = "ElementAtOrDefault" ->
            let isOrDefault = m.Name = "ElementAtOrDefault"
            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source
            let elemType = PolarsQueryProvider.GetSequenceElementType source.Type

            let isFromEnd, rawIndexVal =
                match tryEvaluate indexExpr with
                | Some(:? Index as idx) -> idx.IsFromEnd, int64 idx.Value
                | Some idxVal -> false, Convert.ToInt64 idxVal
                | None -> failwithf "Failed to evaluate index for %s." m.Name

            if not isFromEnd && rawIndexVal < 0L then
                if isOrDefault then
                    Unchecked.defaultof<'TResult>
                else
                    raise (ArgumentOutOfRangeException("index", "Index was out of range."))
            else
                let sliceOffset = if isFromEnd then -rawIndexVal else rawIndexVal

                if not hasClientPreds then
                    let slicedLf = PolarsWrapper.LazySlice(nativeLf, sliceOffset, 1u)
                    let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto)
                    let h = PolarsWrapper.DataFrameHeight dfHandle

                    if h = 0L then
                        if isOrDefault then
                            Unchecked.defaultof<'TResult>
                        else
                            raise (ArgumentOutOfRangeException("index", "Index was out of range."))
                    else
                        match clientProjOpt with
                        | Some(sourceType, projFunc) ->
                            let invoker = LinqReflectionCache.GetMaterializerElementAtInvoker sourceType
                            let rawEntity = invoker.Invoke(mat, dfHandle, 0L)
                            projFunc.Invoke rawEntity :?> 'TResult
                        | None ->
                            let invoker = LinqReflectionCache.GetMaterializerElementAtInvoker elemType
                            invoker.Invoke(mat, dfHandle, 0L) :?> 'TResult
                else
                    let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

                    let targetIndex =
                        if isFromEnd then
                            let totalHeight = PolarsWrapper.DataFrameHeight dfHandle
                            totalHeight - rawIndexVal
                        else
                            rawIndexVal

                    let targetSourceType =
                        match clientProjOpt with
                        | Some(sSrc, _) -> sSrc
                        | None -> elemType

                    let invoker =
                        if isOrDefault then
                            LinqReflectionCache.GetMaterializerElementAtOrDefaultInvoker targetSourceType
                        else
                            LinqReflectionCache.GetMaterializerElementAtInvoker targetSourceType

                    let rawEntity = invoker.Invoke(mat, dfHandle, targetIndex)

                    if isNull rawEntity && not targetSourceType.IsValueType then
                        Unchecked.defaultof<'TResult>
                    else
                        match clientProjOpt with
                        | Some(_, projFunc) -> projFunc.Invoke rawEntity :?> 'TResult
                        | None -> rawEntity :?> 'TResult

        // =========================================================================
        // 7. Contains(item) / Contains(item, comparer)
        // =========================================================================
        | MethodCall(m, null, source :: itemExpr :: rest) when m.Name = "Contains" ->
            let elemType = PolarsQueryProvider.GetSequenceElementType source.Type

            let itemVal =
                match tryEvaluate itemExpr with
                | Some v -> v
                | None -> failwith "Failed to evaluate item parameter for Contains."

            let comparerObj =
                match rest with
                | [ comparerExpr ] -> tryEvaluate comparerExpr |> Option.toObj
                | _ -> null

            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source

            let filterExprOpt =
                if not hasClientPreds && clientProjOpt.IsNone && isNull comparerObj then
                    PolarsQuery<obj>.BuildItemEqualityFilter itemVal nativeLf
                else
                    None

            match filterExprOpt with
            | Some combinedFilter ->
                let filteredLf = PolarsWrapper.LazyFilter(nativeLf, combinedFilter)
                let slicedLf = PolarsWrapper.LazySlice(filteredLf, 0L, 1u)
                let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto)
                box (PolarsWrapper.DataFrameHeight dfHandle > 0L) :?> 'TResult

            | None ->
                let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

                match clientProjOpt with
                | Some(sourceType, projFunc) ->
                    let compiledPred =
                        Func<obj, bool>(fun projectedObj ->
                            if isNull comparerObj then
                                if isNull itemVal then isNull projectedObj
                                elif isNull projectedObj then false
                                else Object.Equals(projectedObj, itemVal)
                            else
                                let compMethod =
                                    comparerObj.GetType().GetMethod("Equals", [| typeof<obj>; typeof<obj> |])

                                if not (isNull compMethod) then
                                    compMethod.Invoke(comparerObj, [| projectedObj; itemVal |]) :?> bool
                                else
                                    Object.Equals(projectedObj, itemVal))

                    let combinedPred =
                        LinqReflectionCache.CreateComposedPredicate(sourceType, projFunc, compiledPred, elemType)

                    let invoker = LinqReflectionCache.GetMaterializerAnyInvoker sourceType
                    box (invoker.Invoke(mat, dfHandle, combinedPred)) :?> 'TResult

                | None ->
                    let invoker = LinqReflectionCache.GetMaterializerContainsInvoker elemType
                    box (invoker.Invoke(mat, dfHandle, itemVal, comparerObj)) :?> 'TResult

        // =========================================================================
        // 8. SequenceEqual(second) / SequenceEqual(second, comparer)
        // =========================================================================
        | MethodCall(m, null, source :: secondExpr :: rest) when m.Name = "SequenceEqual" ->
            let elemType = PolarsQueryProvider.GetSequenceElementType source.Type

            let compObj =
                match rest with
                | [ comparerExpr ] -> tryEvaluate comparerExpr |> Option.toObj
                | _ -> null

            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source

            let secondPlanOpt =
                match SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr with
                | Some lf -> Some(lf, false, None)
                | None ->
                    match tryEvaluate secondExpr with
                    | Some(:? IInternalPlanSource as ps) -> Some(ps.GetPlanInfo())
                    | _ -> None

            match secondPlanOpt with
            | Some(secondLf, secondHasClient, secondProjOpt) when
                not hasClientPreds
                && clientProjOpt.IsNone
                && not secondHasClient
                && secondProjOpt.IsNone
                && isNull compObj
                ->
                let fullDf1 = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
                let fullDf2 = PolarsWrapper.LazyCollect(secondLf, PlEngine.Auto)

                if PolarsWrapper.DataFrameHeight fullDf1 <> PolarsWrapper.DataFrameHeight fullDf2 then
                    box false :?> 'TResult
                else
                    box (PolarsWrapper.DataFrameEquals(fullDf1, fullDf2, nullEqual = true)) :?> 'TResult

            | Some(secondLf, _, _) ->
                let df1 = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
                let df2 = PolarsWrapper.LazyCollect(secondLf, PlEngine.Auto)
                let invoker = LinqReflectionCache.GetMaterializerSeqEqualDfInvoker elemType
                box (invoker.Invoke(mat, df1, df2, compObj)) :?> 'TResult

            | None ->
                let df = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

                let secondSeq =
                    match tryEvaluate secondExpr with
                    | Some(:? IEnumerable as seq) -> seq
                    | _ -> (this :> IQueryProvider).CreateQuery(secondExpr) :> IEnumerable

                let invoker = LinqReflectionCache.GetMaterializerSeqEqualSeqInvoker elemType
                box (invoker.Invoke(mat, df, secondSeq, compObj)) :?> 'TResult

        // =========================================================================
        // 9. MinBy / MaxBy (with or without comparer)
        // =========================================================================
        | MethodCall(m, null, source :: CleanLambda keyLambda :: rest) when m.Name = "MinBy" || m.Name = "MaxBy" ->
            let isMax = m.Name = "MaxBy"
            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source
            let elemType = PolarsQueryProvider.GetSequenceElementType source.Type
            let keyType = keyLambda.ReturnType

            let compObj =
                match rest with
                | [ comparerExpr ] -> tryEvaluate comparerExpr |> Option.toObj
                | _ -> null

            // Detect if sequence is scalar (e.g. string, int) to map parameter to the underlying single column
            let isScalar = PolarsTypeHelper.IsScalarType elemType || elemType = typeof<string>

            let nativeSortColOpt =
                if not hasClientPreds && clientProjOpt.IsNone && isNull compObj then
                    let ctx =
                        PolarsQueryProvider.createContextForType elemType nativeLf keyLambda.Parameters.[0].Name

                    ExprTranslator.tryTranslateWithContext ctx keyLambda.Body
                else
                    None

            match nativeSortColOpt with
            | Some keyColExpr ->
                let sortedLf =
                    PolarsWrapper.LazyFrameSort(
                        nativeLf,
                        [| keyColExpr |],
                        [| isMax |],
                        [| true |],
                        maintainOrder = false
                    )

                let slicedLf = PolarsWrapper.LazySlice(sortedLf, 0L, 1u)
                let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto)
                let h = PolarsWrapper.DataFrameHeight dfHandle

                if h = 0L then
                    if PolarsTypeHelper.AcceptsNull elemType then
                        LinqReflectionCache.GetEmptyOptionalValue elemType :?> 'TResult
                    else
                        raise (InvalidOperationException("Sequence contains no elements."))
                else
                    let invoker = LinqReflectionCache.GetMaterializerFirstInvoker elemType
                    invoker.Invoke(mat, dfHandle, null) :?> 'TResult

            | None ->
                let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
                let compiledKeySel = keyLambda.Compile()

                match clientProjOpt with
                | Some(sourceType, projFunc) ->
                    let p = Expression.Parameter(sourceType, "src")
                    let boxed = Expression.Convert(p, typeof<obj>)
                    let projInv = Expression.Invoke(Expression.Constant(projFunc), boxed)
                    let castProj = Expression.Convert(projInv, elemType)
                    let keyInv = Expression.Invoke(Expression.Constant(compiledKeySel), castProj)
                    let lambdaType = typedefof<Func<_, _>>.MakeGenericType(sourceType, keyType)
                    let composedKeySelector = Expression.Lambda(lambdaType, keyInv, p).Compile()

                    let invoker =
                        if isMax then
                            LinqReflectionCache.GetMaterializerMaxByInvoker(sourceType, keyType)
                        else
                            LinqReflectionCache.GetMaterializerMinByInvoker(sourceType, keyType)

                    let rawWinner = invoker.Invoke(mat, dfHandle, composedKeySelector, compObj)
                    projFunc.Invoke rawWinner :?> 'TResult

                | None ->
                    let invoker =
                        if isMax then
                            LinqReflectionCache.GetMaterializerMaxByInvoker(elemType, keyType)
                        else
                            LinqReflectionCache.GetMaterializerMinByInvoker(elemType, keyType)

                    let winner = invoker.Invoke(mat, dfHandle, compiledKeySel, compObj)
                    winner :?> 'TResult

        // =========================================================================
        // 10. Aggregate (Fold/Reduce)
        // =========================================================================
        | MethodCall(m, null, source :: rest) when m.Name = "Aggregate" ->
            let rawRows = (this :> IQueryProvider).CreateQuery(source) :> IEnumerable
            let elemType = PolarsQueryProvider.GetSequenceElementType source.Type

            match rest with
            | [ StripQuotes(:? LambdaExpression as func) ] ->
                let compiledFunc = func.Compile()
                let invoker = LinqReflectionCache.GetAggregateInvoker(elemType)
                invoker.Invoke(rawRows, compiledFunc) :?> 'TResult

            | [ seedExpr; StripQuotes(:? LambdaExpression as func) ] ->
                let seedVal =
                    match tryEvaluate seedExpr with
                    | Some s -> s
                    | None -> failwith "Failed to evaluate seed parameter for Aggregate."

                let compiledFunc = func.Compile()
                let accumType = typeof<'TResult>
                let invoker = LinqReflectionCache.GetAggregateWithSeedInvoker(elemType, accumType)
                invoker.Invoke(rawRows, seedVal, compiledFunc) :?> 'TResult

            | [ seedExpr; StripQuotes(:? LambdaExpression as func); StripQuotes(:? LambdaExpression as resSel) ] ->
                let seedVal =
                    match tryEvaluate seedExpr with
                    | Some s -> s
                    | None -> failwith "Failed to evaluate seed parameter for Aggregate."

                let compiledFunc = func.Compile()
                let compiledResSel = resSel.Compile()
                let accumType = func.ReturnType

                let invoker =
                    LinqReflectionCache.GetAggregateWithResultSelectorInvoker(elemType, accumType, typeof<'TResult>)

                invoker.Invoke(rawRows, seedVal, compiledFunc, compiledResSel) :?> 'TResult

            | _ -> failwithf "Unsupported Aggregate overload: %A" expression

        | _ -> failwithf "Scalar execution for expression is not supported: %A" expression

    interface IQueryProvider with
        member this.CreateQuery(expression: Expression) : IQueryable =
            let elemType =
                match PolarsTypeHelper.TryGetEnumerableElementType expression.Type with
                | null -> typedefof<obj>
                | t -> t

            let queryType = typedefof<PolarsQuery<_>>.MakeGenericType elemType
            Activator.CreateInstance(queryType, [| box clonedLf; box materializer; box expression |]) :?> IQueryable

        member this.CreateQuery<'TElement>(expression: Expression) : IQueryable<'TElement> =
            PolarsQuery<'TElement>(clonedLf, materializer, expression) :> IQueryable<'TElement>

        // Non-generic Execute: dynamically invokes typed Execute<'TResult>
        member this.Execute(expression: Expression) : obj =
            let executeMethod =
                typeof<PolarsQueryProvider>.GetInterface("IQueryProvider").GetMethods()
                |> Array.find (fun m -> m.Name = "Execute" && m.IsGenericMethod)
                |> fun m -> m.MakeGenericMethod expression.Type

            executeMethod.Invoke(this, [| expression |])

        // Generic Execute<'TResult>: checks if expression is scalar vs sequence
        member this.Execute<'TResult>(expression: Expression) : 'TResult =
            let targetType = typeof<'TResult>

            // Standard scalar reductions
            let isScalarReductionMethod (m: MethodInfo) =
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
                | "Aggregate"
                | "AggregateBy" -> true
                | _ -> false

            match expression with
            // Branch A: Standard Scalar Reductions
            | MethodCall(m, _, _) when isScalarReductionMethod m -> this.ExecuteScalarInternal<'TResult>(expression)

            // Branch B: Target is primitive scalar or string without reduction operator
            | _ when PolarsTypeHelper.IsScalarType targetType || targetType = typeof<string> ->
                this.ExecuteScalarInternal<'TResult>(expression)

            // Branch C: Sequence or Composite object execution
            | _ ->
                let query = (this :> IQueryProvider).CreateQuery(expression)

                match box query with
                | :? 'TResult as matched -> matched
                | _ ->
                    let elemType =
                        match PolarsTypeHelper.TryGetEnumerableElementType targetType with
                        | null -> typeof<obj>
                        | t -> t

                    let toListMethod =
                        LinqReflectionCache.GetEnumerableMethod("ToList", 1, [| elemType |])

                    let listObj = toListMethod.Invoke(null, [| box query |])

                    if targetType.IsArray then
                        let toArrayMethod =
                            LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| elemType |])

                        toArrayMethod.Invoke(null, [| box query |]) :?> 'TResult
                    elif targetType.IsAssignableFrom(listObj.GetType()) then
                        listObj :?> 'TResult
                    else
                        failwithf "Cannot convert query sequence to requested terminal type '%s'" targetType.FullName

and PolarsQuery<'T>
    internal (lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, exprOpt: Expression option) as this
    =
    let lfCloned = PolarsWrapper.LazyClone lazyFrameHandle

    let expression =
        match exprOpt with
        | Some e -> e
        | None -> Expression.Constant(this, typeof<IQueryable<'T>>) :> Expression

    let provider = PolarsQueryProvider(lfCloned, materializer)

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, expression: Expression) =
        PolarsQuery<'T>(lazyFrameHandle, materializer, Some expression)

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer) =
        PolarsQuery<'T>(lazyFrameHandle, materializer, None)

    /// Enforces the ClientEvaluationPolicy against client predicates and full fallbacks
    static member internal EnforcePolicy
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
                    sprintf "[Polars.NET Warning] Query pipeline at stage '%s' fell back to full client-side in-memory LINQ: %s" stage expressionText
                )
            elif not clientPredsWithAst.IsEmpty then
                let predTexts =
                    clientPredsWithAst
                    |> List.map (fun (_, l) -> l.ToString())
                    |> String.concat "; "
                System.Diagnostics.Trace.TraceWarning(
                    sprintf "[Polars.NET Warning] Query predicate at stage '%s' fell back to client-side evaluation: %s" stage predTexts
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

    /// Attempts to decorrelate subquery expressions (EXISTS/NOT EXISTS via Any, and IN/NOT IN via Contains)
    /// into native Semi/Anti Joins.
    static member internal TryDecorrelateSubquery
        (currentLf: LazyFrameHandle)
        (filterLambda: LambdaExpression)
        : JoinSpec option =
        match SubqueryDecorrelator.tryDecorrelateFilter currentLf filterLambda with
        | Some {
                   GeneratedOps = [ QueryOp.Join spec ]
                   ResidualLambda = None
               } -> Some spec
        | _ -> None

    /// Builds a Polars equality filter expression matching an item (scalar primitive or composite entity/record)
    /// against the columns of a target LazyFrame.
    static member internal BuildItemEqualityFilter (itemVal: obj) (targetLf: LazyFrameHandle) : ExprHandle option =
        if isNull itemVal then
            None
        else
            let itemType = itemVal.GetType()
            // Branch 1: Single scalar literal matching against the first column
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
            // Branch 2: Composite entity/DTO/record property-wise equality joined by AND
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

    /// Recursively inspects the expression tree to locate Chunk(size) invocations.
    static member internal TryExtractChunkSize(e: Expression) : int option =
        let rec check expr =
            match expr with
            | null -> None
            | MethodCall(m, null, [ _; sizeExpr ]) when m.Name = "Chunk" ->
                match tryEvaluate sizeExpr with
                | Some s -> Some(Convert.ToInt32 s)
                | None -> None
            | MethodCall(_, null, args) -> args |> List.tryPick check
            | _ -> None

        check e

    /// Helper to extract all column names and their corresponding DataTypeHandle from a LazyFrame schema
    static member internal GetLazySchemaFields(lf: LazyFrameHandle) : (string * DataTypeHandle) array =
        let clonedLf = PolarsWrapper.LazyClone lf
        let schema = PolarsWrapper.GetLazySchema clonedLf
        let count = PolarsWrapper.GetSchemaLen schema

        Array.init (int count) (fun i ->
            let mutable name = null
            let mutable dtHandle = new DataTypeHandle()
            PolarsWrapper.GetSchemaFieldAt(schema, uint64 i, &name, &dtHandle)
            name, dtHandle)

    /// Extracts member names matching properties or capitalizes them to PascalCase
    static member private ExtractPascalCaseMemberNames(newExpr: NewExpression) : string list =
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
                    | prop when not (isNull prop) && prop.Name = p.Name -> p.Name
                    | prop when not (isNull prop) -> prop.Name
                    | _ ->
                        Char.ToUpperInvariant(p.Name.[0]).ToString() + p.Name.Substring 1)
            |> Seq.toList

    /// Extracts column names from a DistinctBy keySelector lambda
    static member private ExtractDistinctColumns(keyLambda: LambdaExpression) : string array option =
        match keyLambda.Body with
        | ExtractColumnName colName -> Some [| colName |]
        | :? NewExpression as newExpr -> Some(PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr |> List.toArray)
        | :? MemberInitExpression as initExpr ->
            Some(initExpr.Bindings |> Seq.map (fun b -> b.Member.Name) |> Seq.toArray)
        | _ -> None

    /// Extracts target column name for SelectMany/Explode
    static member private ExtractExplodeColumn(colLambda: LambdaExpression) : string option =
        match colLambda.Body with
        | ExtractColumnName colName -> Some colName
        | _ -> None

    /// Uniformly extracts member name and expression pairs from NewExpression or MemberInitExpression
    static member private TryExtractMemberBindings(expr: Expression) : (string * Expression) list option =
        match expr with
        | :? NewExpression as newExpr ->
            let names = PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr
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
    static member private TryExtractGroupAgg
        (groupParamName: string)
        (resLambda: LambdaExpression)
        : (string * ExprHandle) option =
        match resLambda.Body with
        | :? NewExpression as newExpr ->
            let args = newExpr.Arguments |> Seq.toList
            let memberNames = PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr

            List.zip memberNames args
            |> List.tryPick (fun (memName, arg) ->
                match arg with
                | MethodCall(m, _, [ :? ParameterExpression as p ]) when
                    p.Name = groupParamName && (m.Name = "Count" || m.Name = "LongCount")
                    ->
                    let countExpr = PolarsWrapper.Len()
                    let aliased = PolarsWrapper.Alias(countExpr, memName)
                    Some(memName, aliased)
                | MethodCall(m, _, target :: _) when
                    not (isNull target)
                    && (match target with
                        | :? ParameterExpression as p -> p.Name = groupParamName
                        | _ -> false)
                    && (m.Name = "Count" || m.Name = "LongCount")
                    ->
                    let countExpr = PolarsWrapper.Len()
                    let aliased = PolarsWrapper.Alias(countExpr, memName)
                    Some(memName, aliased)
                | _ -> None)
        | _ -> None

    /// Desugars a .NET 11 GroupJoin downstream projection (g => body)
    /// into a classic 2-parameter result selector ((outer, innerGroup) => body)
    /// where g.Key maps to 'outer' and 'g' (as IEnumerable) maps to 'innerGroup'.
    static member private DesugarGroupJoinProject
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
                override this.VisitMember(node: MemberExpression) =
                    // Replace g.Key access with direct outer parameter
                    match ExprTranslator.unwrap node.Expression with
                    | :? ParameterExpression as p when p.Name = gParam.Name && node.Member.Name = "Key" ->
                        pOuter :> Expression
                    | _ -> base.VisitMember(node)

                override this.VisitParameter(node: ParameterExpression) =
                    // Replace group collection references (e.g. g in g.Count()) with innerGroup parameter
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
    static member private ExtractBinaryResultRenames
        (resLambda: LambdaExpression)
        (explodedInfoOpt: (string * string) option)
        : (string * string) list =
        match resLambda.Body with
        | :? NewExpression as newExpr ->
            let args = newExpr.Arguments |> Seq.toList
            let memberNames = PolarsQuery<'T>.ExtractPascalCaseMemberNames newExpr

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

    /// Inspects whether a projection is an intermediate Roslyn Transparent Identifier (e.g. C# 'let' clause),
    /// which packages existing context and introduces new calculated columns via WithColumns.
    static member private TryCompileTransparentLet
        (lf: LazyFrameHandle)
        (projLambda: LambdaExpression)
        : QueryOp option =
        let param = projLambda.Parameters.[0]
        let ctx = PolarsQueryProvider.createContextForType param.Type lf param.Name

        match PolarsQuery<'T>.TryExtractMemberBindings projLambda.Body with
        | Some bindings ->
            let isNavigationBinding (_name: string, expr: Expression) =
                let cleanExpr = ExprTranslator.unwrap expr

                match cleanExpr with
                | :? ParameterExpression as p when p.Name = param.Name -> true
                | MemberAccess(_, m) when ExprTranslator.isNavigationMember m -> true
                | _ -> false

            let navBindings, computedBindings = bindings |> List.partition isNavigationBinding

            // Must contain at least one navigation holder (preserving context)
            // and at least one computed column (the let binding expression)
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

    /// Compiles any lambda predicate into a type-safe boxed delegate Func<obj, bool>
    /// to safely evaluate against raw materialized source entities without generic coupling.
    static member private CompileClientPredicate(lambda: LambdaExpression) : Func<obj, bool> =
        let paramType = lambda.Parameters.[0].Type
        let rawParam = Expression.Parameter(typeof<obj>, "rawItem")

        let castParam =
            if paramType = typeof<obj> then
                rawParam :> Expression
            else
                Expression.Convert(rawParam, paramType) :> Expression

        let invoked = Expression.Invoke(lambda, castParam)
        Expression.Lambda<Func<obj, bool>>(invoked, rawParam).Compile()

    /// Disambiguates duplicate column names against an existing set of columns by appending a given suffix,
    /// returning the safely renamed LazyFrame, updated column set, and a renaming map.
    static member private DisambiguateColumns
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

    /// Unified GroupBy compiler powering both single-key and multi-key GroupBy pipelines (C# LINQ & F# QueryBuilder)
    static member private CompileUnifiedGroupBy
        (lf: LazyFrameHandle)
        (ctx: GroupFoldContext)
        (projBody: Expression)
        (groupParamName: string)
        (isKeyArg: Expression -> bool)
        : QueryOp list option =

        let keyParam = ctx.KeyLambda.Parameters.[0]
        let keyCtx = PolarsQueryProvider.createContextForType keyParam.Type lf keyParam.Name

        let elemExprOpt =
            ctx.ElemLambdaOpt
            |> Option.bind (fun el ->
                let elParam = el.Parameters.[0]
                let elCtx = PolarsQueryProvider.createContextForType elParam.Type lf elParam.Name
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
            match PolarsQuery<'T>.TryExtractMemberBindings ctx.KeyLambda.Body with
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
            match PolarsQuery<'T>.TryExtractMemberBindings projBody with
            | None -> None
            | Some memberBindings ->
                let memberNames = memberBindings |> List.map fst
                let args = memberBindings |> List.map snd

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

                // Resolve scalar column name for group elements if grouping over scalar elements or projected scalars
                let scalarColOpt =
                    match ctx.ElemLambdaOpt with
                    | Some el ->
                        match el.Body with
                        | ExtractColumnName c -> Some c
                        | _ -> None
                    | None ->
                        if PolarsTypeHelper.IsScalarType keyParam.Type || keyParam.Type = typeof<string> then
                            let cols = PolarsSchemaExtractor.GetColumnNames lf
                            if cols.Length = 1 then Some cols.[0] else None
                        else
                            None

                // Resolve effective target column expression from element selector or scalar context
                let targetColExprOpt =
                    elemExprOpt |> Option.orElse (scalarColOpt |> Option.map PolarsWrapper.Col)

                let tryTranslateSingleAgg (colName: string) (rawArgExpr: Expression) : ExprHandle option =
                    let argExpr = ExprTranslator.unwrap rawArgExpr

                    let isGroupParam (e: Expression) =
                        match ExprTranslator.unwrap e with
                        | :? ParameterExpression as p -> p.Name = groupParamName
                        | _ -> false

                    let isGroupCountCall (m: MethodInfo) (targetObj: Expression) (callArgs: Expression list) =
                        (m.Name = "Count" || m.Name = "LongCount")
                        && (isNull targetObj
                            && callArgs.Length = 1
                            && (match ExprTranslator.unwrap callArgs.Head with
                                | :? ParameterExpression as p -> p.Name = groupParamName
                                | _ -> false)
                            || not (isNull targetObj)
                               && callArgs.IsEmpty
                               && match ExprTranslator.unwrap targetObj with
                                  | :? ParameterExpression as p -> p.Name = groupParamName
                                  | _ -> false)

                    // Helper identifying direct scalar reductions on group parameter: e.g. salaries.Sum(), salaries.Average()
                    let isGroupReductionCall (m: MethodInfo) (targetObj: Expression) (callArgs: Expression list) =
                        isNull targetObj && callArgs.Length = 1 && isGroupParam callArgs.Head
                        || not (isNull targetObj) && callArgs.IsEmpty && isGroupParam targetObj

                    let tryMatchNestedListAgg (e: Expression) =
                        match e with
                        | MethodCall(mList, null, [ MethodCall(mSel, null, [ gArg; CleanLambda selLambda ]) ])
                        | MethodCall(mList, MethodCall(mSel, null, [ gArg; CleanLambda selLambda ]), [])
                        | MethodCall(mList, MethodCall(mSel, gArg, [ CleanLambda selLambda ]), []) when
                            (mList.Name = "ToList" || mList.Name = "ToArray")
                            && mSel.Name = "Select"
                            && (match ExprTranslator.unwrap gArg with
                                | :? ParameterExpression as p -> p.Name = groupParamName
                                | _ -> false)
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

                    // Direct scalar reductions without lambda selector: ids.Min(), ids.Max(), salaries.Sum(), etc.
                    | MethodCall(m, target, callArgs) when
                        isGroupReductionCall m target callArgs && targetColExprOpt.IsSome
                        ->
                        let clonedColExpr = PolarsWrapper.CloneExpr targetColExprOpt.Value

                        ExprTranslator.translateReductionOp m.Name clonedColExpr
                        |> Option.map (fun core -> PolarsWrapper.Alias(core, colName))

                    | _ when (tryMatchNestedListAgg argExpr).IsSome ->
                        let selLambda = (tryMatchNestedListAgg argExpr).Value
                        let selParam = selLambda.Parameters.[0]
                        let selCtx = PolarsQueryProvider.createContextForType selParam.Type lf selParam.Name

                        let colExprOpt =
                            match ExprTranslator.tryResolveColumnNameWithContext selCtx selLambda.Body with
                            | Some cName -> Some(PolarsWrapper.Col cName)
                            | None -> ExprTranslator.tryTranslateWithContext selCtx selLambda.Body

                        colExprOpt |> Option.map (fun cExpr -> PolarsWrapper.Alias(cExpr, colName))

                    | _ ->
                        match elemExprOpt with
                        | Some _ ->
                            AggTranslator.tryTranslateAggWithScalarCol
                                groupParamName
                                scalarColOpt
                                wholeRowStructOpt
                                argExpr
                                colName
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

                    let groupBySpec =
                        {
                            Keys = keyAliasedExprs
                            Aggs = aggs |> List.toArray
                            Having = None
                            MaintainOrder = false
                        }

                    let groupByOp = QueryOp.GroupBy groupBySpec

                    let resolveResultColName (e: Expression) : string option =
                        let cleanE = ExprTranslator.unwrap e

                        match cleanE with
                        | MemberAccess(p, m) when m.Name = "Key" && keyDefs.Length = 1 ->
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

                    let havingFilterOps =
                        ctx.HavingPreds
                        |> List.choose (fun havingPred ->
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
                                        | MethodCall _ as aggCall ->
                                            AggTranslator.tryTranslateAggWithScalarCol
                                                havingPred.Parameters.[0].Name
                                                scalarColOpt
                                                wholeRowStructOpt
                                                aggCall
                                                ""
                                        | _ -> None

                            translateHaving havingPred.Body |> Option.map QueryOp.Filter)

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
                            memberNames |> List.map (fun mName -> PolarsWrapper.Col mName) |> List.toArray

                        QueryOp.Select orderedColExprs

                    Some(finalSelectOp :: sortOps @ havingFilterOps @ [ groupByOp ])

    /// Recursively inspects the expression tree to detect if Shuffle() was called.
    static member private HasShuffle(e: Expression) : bool =
        let rec check expr =
            match expr with
            | null -> false
            | MethodCall(m, null, _) when m.Name = "Shuffle" -> true
            | MethodCall(_, null, args) -> args |> List.exists check
            | _ -> false

        check e

    /// Applies eager row shuffling (sample_frac = 1.0, without replacement) to a DataFrame.
    static member private ApplyShuffle(df: DataFrameHandle) : DataFrameHandle =
        let fracHandle = PolarsWrapper.SeriesNew("", [| 1.0 |])
        PolarsWrapper.SampleFrac(df, fracHandle, false, Nullable true, Nullable())

    static member private TryTranslateSelectArg (ctx: TranslationContext) (argExpr: Expression) : ExprHandle option =
        match ExprTranslator.tryTranslateWithContext ctx argExpr with
        | Some h -> Some h
        | None ->
            match argExpr with
            // Pattern: Ternary null-coalescing / IIF (C# and F# compatible)
            | :? ConditionalExpression as cond ->
                // Helper: checks whether an expression references the query's input parameter/tuple
                let hasParamRef (e: Expression) =
                    let rec check expr =
                        match ExprTranslator.unwrap expr with
                        | null -> false
                        | :? ParameterExpression as p ->
                            ctx.ContainsParam p.Name
                            || p.Name.StartsWith "_arg"
                            || p.Name.StartsWith "tupled"
                            || p.Name.Contains "TransparentIdentifier"
                            || p.Name.StartsWith "<>h__"
                            || p.Type.Name.StartsWith "AnonymousObject"
                            || p.Type.Name.Contains "AnonymousType"
                            || p.Type.Name.StartsWith "Tuple"
                            || p.Type.Name.StartsWith "ValueTuple"
                        | MethodCall(_, target, args) ->
                            (not (isNull target) && check target) || (args |> List.exists check)
                        | MemberAccess(target, _) -> not (isNull target) && check target
                        | Unary(_, inner) -> check inner
                        | Binary(_, left, right) -> check left || check right
                        | Lambda(_, body) -> check body
                        | _ -> false

                    check e

                // Determine which branch is the physical column expression and which is the fallback literal
                let colExprOpt, fallbackExprOpt =
                    if hasParamRef cond.IfTrue && not (hasParamRef cond.IfFalse) then
                        // Pattern: e != null ? e.Name : "NO_EMPLOYEE"
                        Some cond.IfTrue, Some cond.IfFalse
                    elif hasParamRef cond.IfFalse && not (hasParamRef cond.IfTrue) then
                        // Pattern: if box e = null then "NO_EMPLOYEE" else e.Name
                        Some cond.IfFalse, Some cond.IfTrue
                    else
                        None, None

                match colExprOpt, fallbackExprOpt with
                | Some colExpr, Some fallbackExpr ->
                    let colHandleOpt =
                        match ExprTranslator.tryResolveColumnNameWithContext ctx colExpr with
                        | Some c -> Some(PolarsWrapper.Col c)
                        | None -> ExprTranslator.tryTranslateWithContext ctx colExpr

                    // fallbackExpr is guaranteed to be free of runtime parameters, so tryEvaluate is completely safe
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

            // Multi-level tuple or object member access (e.g. d.DeptName -> Col "DeptName")
            | MemberAccess _ as ma ->
                ExprTranslator.tryResolveColumnNameWithContext ctx ma
                |> Option.map PolarsWrapper.Col

            | _ -> None

    /// Compiles a Select projection lambda into native Polars Select expressions
    static member private CompileSelectProjection
        (lf: LazyFrameHandle)
        (projLambda: LambdaExpression)
        : ExprHandle array option =
        let param = projLambda.Parameters.[0]
        let ctx = PolarsQueryProvider.createContextForType param.Type lf param.Name

        // 1. Try unified composite projections (NewExpression & MemberInitExpression)
        match PolarsQuery<'T>.TryExtractMemberBindings projLambda.Body with
        | Some bindings ->
            let exprOpts =
                bindings
                |> List.choose (fun (name, argExpr) ->
                    PolarsQuery<'T>.TryTranslateSelectArg ctx argExpr
                    |> Option.map (fun h -> PolarsWrapper.Alias(h, name)))

            // Push down to Polars native LazySelect only if ALL members can be translated natively
            if exprOpts.Length = bindings.Length then
                Some(List.toArray exprOpts)
            else
                None

        // 2. Direct single column projection or expression
        | None ->
            match projLambda.Body with
            | ExtractColumnName colName -> Some [| PolarsWrapper.Col colName |]
            | expr -> ExprTranslator.tryTranslateWithContext ctx expr |> Option.map (fun h -> [| h |])

    /// Compiles a Join stage into a native JoinSpec (supporting single-column and multi-column keys across Inner, Left, Right, Full)
    static member private CompileJoin
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
            match PolarsQuery<'T>.TryExtractMemberBindings keyLambda.Body with
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

            let leftCtx =
                PolarsQueryProvider.createContextForType outerParam.Type baseLf outerParam.Name

            let rightCtx =
                PolarsQueryProvider.createContextForType innerParam.Type rightLf innerParam.Name

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
    static member private CompileJoinResultSelector
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
            // 1. Selector is directly the first parameter in the native joined frame (e.g. (d, e) => d)
            | :? ParameterExpression as p when p.Name = firstParam.Name ->
                let selectExprs = firstCols |> Array.map PolarsWrapper.Col
                Some(QueryOp.Select selectExprs)

            // 2. Selector is directly the second parameter in the native joined frame (e.g. (d, e) => e)
            | :? ParameterExpression as p when p.Name = secondParam.Name ->
                let selectExprs =
                    secondCols
                    |> Array.map (fun c ->
                        if firstColSet.Contains c then
                            PolarsWrapper.Alias(PolarsWrapper.Col(c + secondSuffix), c)
                        else
                            PolarsWrapper.Col c)
                Some(QueryOp.Select selectExprs)

            // 3. Composite member bindings (new { ... } or new Dto { ... })
            | _ ->
                match PolarsQuery<'T>.TryExtractMemberBindings unwrappedBody with
                | Some bindings ->
                    let exprOpts =
                        bindings
                        |> List.choose (fun (name, argExpr) ->
                            PolarsQuery<'T>.TryTranslateSelectArg ctx argExpr
                            |> Option.map (fun h -> PolarsWrapper.Alias(h, name)))

                    if exprOpts.Length = bindings.Length then
                        Some(QueryOp.Select(List.toArray exprOpts))
                    else
                        None

                // 4. Single member or expression projection (e.g. (d, e) => d.DeptName)
                | None ->
                    match unwrappedBody with
                    | MemberAccess(_, m) ->
                        ExprTranslator.tryTranslateWithContext ctx unwrappedBody
                        |> Option.map (fun h -> QueryOp.Select [| PolarsWrapper.Alias(h, m.Name) |])
                    | _ ->
                        ExprTranslator.tryTranslateWithContext ctx unwrappedBody
                        |> Option.map (fun h -> QueryOp.Select [| h |])

    /// Compiles Intersect/Except/IntersectBy/ExceptBy stages into a native JoinSpec (Semi/Anti Join)
    static member private CompileSetOp
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
                    let ctx1 = PolarsQueryProvider.createContextForType p1.Type baseLf p1.Name
                    let ctx2 = PolarsQueryProvider.createContextForType p2.Type rightLf p2.Name

                    match
                        ExprTranslator.tryTranslateWithContext ctx1 k1.Body,
                        ExprTranslator.tryTranslateWithContext ctx2 k2.Body
                    with
                    | Some lk, Some rk -> Some([| lk |], [| rk |])
                    | _ -> None
                | Some k1, None ->
                    let p1 = k1.Parameters.[0]
                    let ctx1 = PolarsQueryProvider.createContextForType p1.Type baseLf p1.Name

                    ExprTranslator.tryTranslateWithContext ctx1 k1.Body
                    |> Option.bind (fun lk ->
                        let rightCols = PolarsSchemaExtractor.GetColumnNames rightLf

                        if rightCols.Length > 0 then
                            Some([| lk |], [| PolarsWrapper.Col rightCols.[0] |])
                        else
                            None)
                | None, None ->
                    // Build distinct ExprHandle arrays for left and right to prevent pointer collision in Rust FFI
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

    /// Applies a list of QueryOp to a target LazyFrameHandle in forward order.
    static member private ApplyNativeOps (sourceLf: LazyFrameHandle) (ops: QueryOp list) : LazyFrameHandle =
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
                        let baseFields = PolarsQuery<'T>.GetLazySchemaFields baseLf
                        let targetFields = PolarsQuery<'T>.GetLazySchemaFields targetLf

                        let baseNames = baseFields |> Array.map fst
                        let targetNames = targetFields |> Array.map fst

                        if Set.ofArray baseNames = Set.ofArray targetNames then
                            let targetFieldMap = targetFields |> Map.ofArray

                            let reorderedAndCastedExprs =
                                baseFields
                                |> Array.map (fun (colName, baseDt) ->
                                    let targetDt = targetFieldMap.[colName]
                                    let colExpr = PolarsWrapper.Col colName

                                    let isSameType = PolarsWrapper.DataTypeEq(baseDt, targetDt)

                                    if not isSameType then
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

    /// Parses the LINQ expression tree into native ops, client predicates, optional projection, and fallback signal
    member private this.CompilePipeline
        ()
        : QueryOp list * ((Func<obj, bool> * LambdaExpression) list) * ((Type * Func<obj, obj>) option) * bool =
        let rec flatten (e: Expression) (acc: LinqStage list) : LinqStage list =
            match e with
            | null -> acc
            | MethodCall(m, null, [ source; StripQuotes(:? LambdaExpression as l) ]) when m.Name = "Select" ->
                let rec isPassthrough (body: Expression) =
                    let p = l.Parameters.[0]

                    let rec check (e: Expression) =
                        match e with
                        | null -> false
                        // 1. Direct parameter identity: x => x
                        | :? ParameterExpression as pe when pe.Name = p.Name -> true
                        // 2. Identity cast/conversion of the whole entity: d => (Department?)d, x => (object)x
                        | Unary(ExpressionType.Convert, inner)
                        | Unary(ExpressionType.ConvertChecked, inner)
                        | Unary(ExpressionType.Quote, inner) -> check inner
                        // 3. F# anonymous member passthrough
                        | MemberAccess(:? ParameterExpression as pe, memInfo) when
                            pe.Name = p.Name && isFSharpAnonymousMember memInfo
                            ->
                            true
                        // 4. Transparent tuple wrappers (F# & C# ValueTuple / Tuple / AnonymousObject identity projections)
                        | :? NewExpression as ne when
                            ne.Type.Name.StartsWith("AnonymousObject")
                            || ne.Type.Name.StartsWith("Tuple")
                            || ne.Type.Name.StartsWith("ValueTuple")
                            ->
                            let isIdentity =
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

                            isIdentity
                        | _ -> false

                    check body

                if isPassthrough l.Body then
                    flatten source acc
                else
                    flatten source (LinqStage.Project l :: acc)

            | MethodCall(m, null, [ source; StripQuotes(:? LambdaExpression as l) ]) when m.Name = "Where" ->
                flatten source (LinqStage.Filter l :: acc)

            | MethodCall(m, null, [ source; StripQuotes(:? LambdaExpression as l) ]) when
                m.Name = "OrderBy" || m.Name = "ThenBy"
                ->
                flatten source (LinqStage.Sort(l, isDescending = false) :: acc)

            | MethodCall(m, null, [ source; StripQuotes(:? LambdaExpression as l) ]) when
                m.Name = "OrderByDescending" || m.Name = "ThenByDescending"
                ->
                flatten source (LinqStage.Sort(l, isDescending = true) :: acc)

            | MethodCall(m, null, [ source; countExpr ]) when m.Name = "Take" ->
                match tryEvaluate countExpr with
                | Some c -> flatten source (LinqStage.Take(Convert.ToUInt32 c) :: acc)
                | None -> failwithf "Could not evaluate Take count argument: %A" countExpr

            | MethodCall(m, null, [ source; countExpr ]) when m.Name = "Skip" ->
                match tryEvaluate countExpr with
                | Some c -> flatten source (LinqStage.Skip(Convert.ToUInt32 c) :: acc)
                | None -> failwithf "Could not evaluate Skip count argument: %A" countExpr

            | MethodCall(m, null, [ source ]) when m.Name = "Distinct" -> flatten source (LinqStage.Distinct :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "Index" -> flatten source (LinqStage.Index :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "Order" ->
                flatten source (LinqStage.SortDefault(isDescending = false) :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "OrderDescending" ->
                flatten source (LinqStage.SortDefault(isDescending = true) :: acc)

            | MethodCall(m, null, [ source; StripQuotes(:? LambdaExpression as keySel) ]) when m.Name = "DistinctBy" ->
                flatten source (LinqStage.DistinctBy keySel :: acc)

            // 4-arg GroupBy: (source, keySelector, elementSelector, resultSelector)
            | MethodCall(m, null, [ source; CleanLambda k; CleanLambda e; CleanLambda r ]) when m.Name = "GroupBy" ->
                flatten source (LinqStage.GroupByWithElementAndResult(k, e, r) :: acc)

            | MethodCall(m,
                         null,
                         [ source; StripQuotes(:? LambdaExpression as k); StripQuotes(:? LambdaExpression as r) ]) when
                m.Name = "GroupBy"
                ->
                flatten source (LinqStage.GroupByWithResult(k, r) :: acc)

            | MethodCall(m, null, [ source; StripQuotes(:? LambdaExpression as k) ]) when m.Name = "GroupBy" ->
                flatten source (LinqStage.GroupByKey k :: acc)

            // Join with explicit resultSelector (5 or 6 args, including .NET 11 FullJoin):
            // (outer, inner, outerKeySelector, innerKeySelector, resultSelector, [comparer])
            | MethodCall(m,
                         _,
                         outer :: inner :: StripQuotes(:? LambdaExpression as outerKey) :: StripQuotes(:? LambdaExpression as innerKey) :: StripQuotes(:? LambdaExpression as resSel) :: _) when
                m.Name = "Join"
                || m.Name = "LeftJoin"
                || m.Name = "RightJoin"
                || m.Name = "FullJoin"
                ->
                flatten outer (LinqStage.Join(m, inner, outerKey, innerKey, Some resSel) :: acc)

            // .NET 11 Tuple Join without resultSelector (4 or 5 args returning (TOuter Outer, TInner Inner)):
            // (outer, inner, outerKeySelector, innerKeySelector, [comparer])
            | MethodCall(m,
                         _,
                         outer :: inner :: StripQuotes(:? LambdaExpression as outerKey) :: StripQuotes(:? LambdaExpression as innerKey) :: _) when
                m.Name = "Join"
                || m.Name = "LeftJoin"
                || m.Name = "RightJoin"
                || m.Name = "FullJoin"
                ->
                flatten outer (LinqStage.Join(m, inner, outerKey, innerKey, None) :: acc)

            | MethodCall(m, null, [ first; second ]) when m.Name = "Intersect" || m.Name = "Except" ->
                flatten first (LinqStage.SetOp(m, second, None, None) :: acc)

            | MethodCall(m, null, [ first; second; StripQuotes(:? LambdaExpression as keySel) ]) when
                m.Name = "IntersectBy" || m.Name = "ExceptBy"
                ->
                flatten first (LinqStage.SetOp(m, second, Some keySel, None) :: acc)

            | MethodCall(m,
                         null,
                         [ first; second; StripQuotes(:? LambdaExpression as k1); StripQuotes(:? LambdaExpression as k2) ]) when
                m.Name = "IntersectBy" || m.Name = "ExceptBy"
                ->
                flatten first (LinqStage.SetOp(m, second, Some k1, Some k2) :: acc)

            | MethodCall(m, null, [ first; second ]) when m.Name = "Concat" ->
                flatten first (LinqStage.Concat second :: acc)

            | MethodCall(m, null, [ first; second ]) when m.Name = "Union" ->
                flatten first (LinqStage.Union(second, None) :: acc)

            | MethodCall(m, null, [ first; second; StripQuotes(:? LambdaExpression as keySel) ]) when m.Name = "UnionBy" ->
                flatten first (LinqStage.Union(second, Some keySel) :: acc)

            | MethodCall(m, null, source :: StripQuotes(:? LambdaExpression as colSel) :: rest) when
                m.Name = "SelectMany"
                ->
                let resSelOpt =
                    match rest with
                    | [ StripQuotes(:? LambdaExpression as r) ] -> Some r
                    | _ -> None

                let rec unwrapQuote (e: Expression) =
                    match e with
                    | :? UnaryExpression as u when
                        u.NodeType = ExpressionType.Quote || u.NodeType = ExpressionType.Convert
                        ->
                        unwrapQuote u.Operand
                    | _ -> e

                let paramName = colSel.Parameters.[0].Name
                let containsParam = SubqueryDecorrelator.containsParameter paramName colSel.Body

                // Unpack inner pipeline ONLY if it references the current parameter AND yields non-empty stages (e.g. F# transparent join chains)
                let nestedStagesOpt =
                    if containsParam then
                        match unwrapQuote colSel.Body with
                        | :? MethodCallExpression as innerMc when innerMc.Method.Name <> "DefaultIfEmpty" ->
                            let innerStages = flatten innerMc []
                            if not innerStages.IsEmpty then Some innerStages else None
                        | _ -> None
                    else
                        None

                match nestedStagesOpt with
                | Some innerStages -> flatten source (innerStages @ acc)
                | None ->
                    if containsParam then
                        flatten source (LinqStage.Explode(colSel, resSelOpt) :: acc)
                    else
                        flatten source (LinqStage.CrossJoin(colSel.Body, resSelOpt) :: acc)

            | MethodCall(m, null, [ source; StripQuotes(:? LambdaExpression as l) ]) when m.Name = "TakeWhile" ->
                flatten source (LinqStage.TakeWhile l :: acc)

            | MethodCall(m, null, [ source; StripQuotes(:? LambdaExpression as l) ]) when m.Name = "SkipWhile" ->
                flatten source (LinqStage.SkipWhile l :: acc)

            | MethodCall(m,
                         null,
                         outer :: inner :: StripQuotes(:? LambdaExpression as outerKey) :: StripQuotes(:? LambdaExpression as innerKey) :: StripQuotes(:? LambdaExpression as resSel) :: _) when
                m.Name = "GroupJoin"
                ->
                flatten outer (LinqStage.GroupJoin(m, inner, outerKey, innerKey, Some resSel) :: acc)

            // .NET 11 GroupJoin without resultSelector (4 or 5 args, returning IGrouping<TOuter, TInner>):
            // (outer, inner, outerKeySelector, innerKeySelector, [comparer])
            | MethodCall(m,
                         null,
                         outer :: inner :: StripQuotes(:? LambdaExpression as outerKey) :: StripQuotes(:? LambdaExpression as innerKey) :: _) when
                m.Name = "GroupJoin"
                ->
                flatten outer (LinqStage.GroupJoin(m, inner, outerKey, innerKey, None) :: acc)

            | MethodCall(m, null, [ source; second ]) when m.Name = "Zip" ->
                flatten source (LinqStage.Zip(second, None) :: acc)

            | MethodCall(m, null, [ source; second; StripQuotes(:? LambdaExpression as resSel) ]) when m.Name = "Zip" ->
                flatten source (LinqStage.Zip(second, Some resSel) :: acc)

            | MethodCall(m, null, [ first; second; third ]) when m.Name = "Zip" ->
                flatten first (LinqStage.Zip3(second, third) :: acc)

            | MethodCall(m, null, [ source; sizeExpr ]) when m.Name = "Chunk" ->
                match tryEvaluate sizeExpr with
                | Some s ->
                    let size = Convert.ToInt32 s

                    if size <= 0 then
                        raise (ArgumentOutOfRangeException("size", "Chunk size must be positive."))

                    flatten source (LinqStage.Chunk size :: acc)
                | None -> failwithf "Could not evaluate Chunk size argument: %A" sizeExpr

            | MethodCall(m, null, [ source; countExpr ]) when m.Name = "TakeLast" ->
                match tryEvaluate countExpr with
                | Some c -> flatten source (LinqStage.TakeLast(Convert.ToUInt32 c) :: acc)
                | None -> failwithf "Could not evaluate TakeLast count: %A" countExpr

            | MethodCall(m, null, [ source; countExpr ]) when m.Name = "SkipLast" ->
                match tryEvaluate countExpr with
                | Some c -> flatten source (LinqStage.SkipLast(Convert.ToUInt32 c) :: acc)
                | None -> failwithf "Could not evaluate SkipLast count: %A" countExpr

            | MethodCall(m, null, [ source; elemExpr ]) when m.Name = "Append" ->
                flatten source (LinqStage.Append elemExpr :: acc)

            | MethodCall(m, null, [ source; elemExpr ]) when m.Name = "Prepend" ->
                flatten source (LinqStage.Prepend elemExpr :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "Reverse" -> flatten source (LinqStage.Reverse :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "Shuffle" -> flatten source (LinqStage.Shuffle :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "Cast" && m.IsGenericMethod ->
                let targetType = m.GetGenericArguments().[0]
                flatten source (LinqStage.Cast targetType :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "OfType" && m.IsGenericMethod ->
                let sourceType =
                    if source.Type.IsGenericType then
                        source.Type.GetGenericArguments().[0]
                    elif source.Type.IsArray then
                        source.Type.GetElementType()
                    else
                        typeof<obj>

                let targetType = m.GetGenericArguments().[0]
                flatten source (LinqStage.OfType(sourceType, targetType) :: acc)

            | MethodCall(m, null, [ source ]) when m.Name = "DefaultIfEmpty" ->
                flatten source (LinqStage.DefaultIfEmpty None :: acc)

            | MethodCall(m, null, [ source; defaultExpr ]) when m.Name = "DefaultIfEmpty" ->
                flatten source (LinqStage.DefaultIfEmpty(Some defaultExpr) :: acc)

            // CountBy: matches source, keySelector (via CleanLambda), and optional comparer
            | MethodCall(m, null, source :: CleanLambda keyLambda :: _) when m.Name = "CountBy" ->
                flatten source (LinqStage.CountBy keyLambda :: acc)

            | _ -> acc

        // printfn "\n\x1b[1;32m================ [ORIGINAL EXPRESSION TREE] ================\x1b[0m"
        // printfn "%s" (expression.ToString())
        let currentScalarCol =
            PolarsQueryProvider.tryGetScalarColumnName typeof<'T> lfCloned

        let rewrittenExpr = expression |> FSharpAst.rewrite
        let extraction = PartialEvaluator.extract rewrittenExpr
        let cacheKey = QueryCacheKey.Create(typeof<'T>, extraction.NormalizedExpression)

        let cached =
            QueryPlanCache.GetOrAdd(
                cacheKey,
                fun _ ->
                    {
                        Key = cacheKey
                        NormalizedExpression = extraction.NormalizedExpression
                        ParamNames = extraction.Parameters.Keys |> Seq.toArray
                        NormalizedStages = []
                        LastAccessTick = Environment.TickCount64
                    }
            )

        let bound =
            if extraction.Parameters.Count = 0 then
                rewrittenExpr
            else
                PartialEvaluator.substituteExtracted cached.NormalizedExpression extraction.Parameters

        let stages = flatten bound []
        FallbackTracker.Diagnostic <- None
        // eprintfn "[FLATTENED STAGES COUNT] %d" stages.Length
        // stages |> List.iteri (fun idx s -> eprintfn "  Stage[%d] = %A" idx s)

        let addSort (spec: SortSpec) (ops: QueryOp list) =
            match ops with
            | QueryOp.Sort specs :: tail -> QueryOp.Sort(specs @ [ spec ]) :: tail
            | _ -> QueryOp.Sort [ spec ] :: ops

        let isGrouping =
            typeof<'T>.IsGenericType
            && typeof<'T>.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

        let rec fuse
            (remaining: LinqStage list)
            (opsAcc: QueryOp list)
            (clientPreds: (Func<obj, bool> * LambdaExpression) list)
            (clientProjOpt: (Type * Func<obj, obj>) option)
            =
            // Unified GroupBy compilation and execution dispatcher
            let tryDispatchGroupBy
                (ctx: GroupFoldContext)
                (projBody: Expression)
                (groupParamName: string)
                (isKeyArg: Expression -> bool)
                (nextTail: LinqStage list)
                =
                if not clientPreds.IsEmpty then
                    [], [], None, true
                else
                    match PolarsQuery<'T>.CompileUnifiedGroupBy lfCloned ctx projBody groupParamName isKeyArg with
                    | Some compiledOps -> fuse nextTail (compiledOps @ opsAcc) clientPreds clientProjOpt
                    | None ->
                        if isGrouping then
                            fuse nextTail opsAcc clientPreds clientProjOpt
                        else
                            [], [], None, true

            // Helper to dispatch standard C# GroupBy overloads (3-arg and 4-arg)
            let dispatchCSharpGroupBy
                (keyLambda: LambdaExpression)
                (elemLambdaOpt: LambdaExpression option)
                (resLambda: LambdaExpression)
                (tail: LinqStage list)
                =
                let havingPreds, nextTail =
                    match tail with
                    | LinqStage.Filter havingPred :: rest -> [ havingPred ], rest
                    | _ -> [], tail

                let ctx =
                    {
                        KeyLambda = keyLambda
                        ElemLambdaOpt = elemLambdaOpt
                        HavingPreds = havingPreds
                        SortStages = []
                    }

                let resKeyParam = resLambda.Parameters.[0]

                let isKeyArg (e: Expression) =
                    match e with
                    | :? ParameterExpression as p when p.Name = resKeyParam.Name -> true
                    | MemberAccess(:? ParameterExpression as p, _) when p.Name = resKeyParam.Name -> true
                    | _ -> false

                tryDispatchGroupBy ctx resLambda.Body resLambda.Parameters.[1].Name isKeyArg nextTail

            match remaining with
            // 1. Universal Pipeline GroupBy (F# QueryBuilder / chained syntax)
            | LinqStage.GroupByKey keyLambda :: tail ->
                let rec slurp (stages: LinqStage list) (ctx: GroupFoldContext) =
                    match stages with
                    | LinqStage.Filter pred :: next ->
                        slurp
                            next
                            { ctx with
                                HavingPreds = ctx.HavingPreds @ [ pred ]
                            }
                    | LinqStage.Sort(sortLambda, isDesc) :: next ->
                        slurp
                            next
                            { ctx with
                                SortStages = ctx.SortStages @ [ (sortLambda, isDesc) ]
                            }
                    | LinqStage.Project projLambda :: next ->
                        let isKeyArg (e: Expression) =
                            match e with
                            | MemberAccess(p, m) when
                                not (isNull p) && p.NodeType = ExpressionType.Parameter && m.Name = "Key"
                                ->
                                true
                            | _ -> false

                        tryDispatchGroupBy ctx projLambda.Body projLambda.Parameters.[0].Name isKeyArg next
                    | [] when isGrouping ->
                        let clientHavingPreds =
                            ctx.HavingPreds
                            |> List.map (fun predLambda ->
                                let rawParam = Expression.Parameter(typeof<obj>, "rawItem")
                                let castParam = Expression.Convert(rawParam, predLambda.Parameters.[0].Type)
                                let invoked = Expression.Invoke(predLambda, castParam)
                                let safePred = Expression.Lambda<Func<obj, bool>>(invoked, rawParam).Compile()
                                safePred, predLambda)

                        fuse [] opsAcc (clientHavingPreds @ clientPreds) clientProjOpt
                    | _ -> [], [], None, true

                let initCtx =
                    {
                        KeyLambda = keyLambda
                        ElemLambdaOpt = None
                        HavingPreds = []
                        SortStages = []
                    }

                slurp tail initCtx

            // 2. C# 3-arg GroupBy: (keySelector, resultSelector) OR (keySelector, elementSelector)
            | LinqStage.GroupByWithResult(keyLambda, secondLambda) :: tail ->
                if secondLambda.Parameters.Count >= 2 then
                    // resultSelector: (key, group) => new { ... }
                    dispatchCSharpGroupBy keyLambda None secondLambda tail
                else
                    let rec slurp (stages: LinqStage list) (ctx: GroupFoldContext) =
                        match stages with
                        | LinqStage.Filter pred :: next ->
                            slurp
                                next
                                { ctx with
                                    HavingPreds = ctx.HavingPreds @ [ pred ]
                                }
                        | LinqStage.Sort(sortLambda, isDesc) :: next ->
                            slurp
                                next
                                { ctx with
                                    SortStages = ctx.SortStages @ [ (sortLambda, isDesc) ]
                                }
                        | LinqStage.Project projLambda :: next ->
                            let isKeyArg (e: Expression) =
                                let cleanE = ExprTranslator.unwrap e

                                match cleanE with
                                | MemberAccess(p, m) when
                                    not (isNull p) && p.NodeType = ExpressionType.Parameter && m.Name = "Key"
                                    ->
                                    true
                                | _ -> false

                            tryDispatchGroupBy ctx projLambda.Body projLambda.Parameters.[0].Name isKeyArg next
                        | [] when isGrouping ->
                            let clientHavingPreds =
                                ctx.HavingPreds
                                |> List.map (fun predLambda ->
                                    let rawParam = Expression.Parameter(typeof<obj>, "rawItem")
                                    let castParam = Expression.Convert(rawParam, predLambda.Parameters.[0].Type)
                                    let invoked = Expression.Invoke(predLambda, castParam)
                                    let safePred = Expression.Lambda<Func<obj, bool>>(invoked, rawParam).Compile()
                                    (safePred, predLambda))

                            fuse [] opsAcc (clientHavingPreds @ clientPreds) clientProjOpt
                        | _ -> [], [], None, true

                    let initCtx =
                        {
                            KeyLambda = keyLambda
                            ElemLambdaOpt = Some secondLambda
                            HavingPreds = []
                            SortStages = []
                        }

                    slurp tail initCtx

            // 3. C# 4-arg GroupBy (keySelector, elementSelector, resultSelector)
            | LinqStage.GroupByWithElementAndResult(keyLambda, elemLambda, resLambda) :: tail ->
                dispatchCSharpGroupBy keyLambda (Some elemLambda) resLambda tail

            | LinqStage.Filter l :: tail ->
                // Attempt subquery decorrelation (EXISTS/IN -> Semi-Join, NOT EXISTS/NOT IN -> Anti-Join)
                match SubqueryDecorrelator.tryDecorrelateFilter lfCloned l with
                | Some decorResult when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                    let newOpsAcc = (decorResult.GeneratedOps |> List.rev) @ opsAcc

                    match decorResult.ResidualLambda with
                    | Some residualLam ->
                        fuse (LinqStage.Filter residualLam :: tail) newOpsAcc clientPreds clientProjOpt
                    | None -> fuse tail newOpsAcc clientPreds clientProjOpt
                | _ ->
                    let p = l.Parameters.[0]
                    let ctx = PolarsQueryProvider.createContextForType p.Type lfCloned p.Name

                    match ExprTranslator.tryTranslateWithContext ctx l.Body with
                    | Some exprHandle when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                        fuse tail (QueryOp.Filter exprHandle :: opsAcc) clientPreds clientProjOpt
                    | _ ->
                        let rec tryTranslateTupleFilter (e: Expression) =
                            let isNullConstant (expr: Expression) =
                                match expr with
                                | :? ConstantExpression as c -> isNull c.Value
                                | :? UnaryExpression as u when
                                    u.NodeType = ExpressionType.Convert || u.NodeType = ExpressionType.Quote
                                    ->
                                    match u.Operand with
                                    | :? ConstantExpression as c -> isNull c.Value
                                    | _ -> false
                                | _ -> false

                            match e with
                            // 1. Null-safe checks to avoid Polars three-valued logic issues
                            | Binary(op, left, right) when op = ExpressionType.NotEqual && isNullConstant right ->
                                tryTranslateTupleFilter left |> Option.map PolarsWrapper.IsNotNull
                            | Binary(op, left, right) when op = ExpressionType.NotEqual && isNullConstant left ->
                                tryTranslateTupleFilter right |> Option.map PolarsWrapper.IsNotNull
                            | Binary(op, left, right) when op = ExpressionType.Equal && isNullConstant right ->
                                tryTranslateTupleFilter left |> Option.map PolarsWrapper.IsNull
                            | Binary(op, left, right) when op = ExpressionType.Equal && isNullConstant left ->
                                tryTranslateTupleFilter right |> Option.map PolarsWrapper.IsNull

                            // 2. Direct reuse of ExprTranslator.translateBinary
                            | Binary(op, left, right) ->
                                match tryTranslateTupleFilter left, tryTranslateTupleFilter right with
                                | Some l, Some r ->
                                    try
                                        Some(ExprTranslator.translateBinary op l r)
                                    with _ ->
                                        None
                                | _ -> None

                            // 3. Precise tuple member column resolution
                            | MemberAccess _ as ma ->
                                match ExprTranslator.tryResolveColumnNameWithContext ctx ma with
                                | Some colName -> Some(PolarsWrapper.Col colName)
                                | None -> None

                            | Constant _ -> ExprTranslator.tryTranslateWithContext TranslationContext.Empty e

                            | _ -> None

                        match tryTranslateTupleFilter l.Body with
                        | Some exprHandle when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                            fuse tail (QueryOp.Filter exprHandle :: opsAcc) clientPreds clientProjOpt
                        | _ ->
                            let paramType = l.Parameters.[0].Type
                            // If transparent identifier from F# 'let' or chained after client projection, fallback to in-memory LINQ safely
                            if
                                paramType.Name.StartsWith "AnonymousObject"
                                || paramType.Name.Contains "TransparentIdentifier"
                                || clientProjOpt.IsSome
                            then
                                FallbackTracker.Diagnostic <- Some (sprintf "Filter on transparent identifier %s: %s\nRewritten body: %s" paramType.Name (l.ToString()) (bound.ToString()))
                                [], [], None, true
                            else
                                // Strongly-typed Func<obj, bool> compilation without DynamicInvoke
                                let rawParam = Expression.Parameter(typeof<obj>, "rawItem")

                                let castParam =
                                    if paramType = typeof<obj> then
                                        rawParam :> Expression
                                    else
                                        Expression.Convert(rawParam, paramType) :> Expression

                                let invoked = Expression.Invoke(l, castParam)
                                let safePred = Expression.Lambda<Func<obj, bool>>(invoked, rawParam).Compile()
                                // let typedPred = Func<'T, bool>(fun (item: 'T) -> safePred.Invoke(box item))
                                fuse tail opsAcc ((safePred, l) :: clientPreds) clientProjOpt

            | LinqStage.Index :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let indexOp = QueryOp.WithRowIndex("Index", Some 0u)

                let int32Dtype =
                    PolarsWrapper.DataTypeExprFromDataType(PolarsWrapper.NewPrimitiveType(int PlDataType.Int32))

                let castIndexExpr =
                    PolarsWrapper.Col "Index"
                    |> fun c -> PolarsWrapper.ExprCast(c, int32Dtype, strict = false, wrapNumerical = false)

                let withCastOp = QueryOp.WithColumns [| castIndexExpr |]
                fuse tail (withCastOp :: indexOp :: opsAcc) clientPreds clientProjOpt

            | LinqStage.SortDefault isDesc :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let colNames = PolarsSchemaExtractor.GetColumnNames lfCloned
                let targetCol = if colNames.Length > 0 then colNames.[0] else ""
                let colExpr = PolarsWrapper.Col targetCol

                let spec =
                    {
                        Expr = colExpr
                        Descending = isDesc
                        NullsLast = false
                    }

                fuse tail (addSort spec opsAcc) clientPreds clientProjOpt

            | LinqStage.Sort(l, isDesc) :: tail ->
                let param = l.Parameters.[0]

                let isGrouping =
                    param.Type.IsGenericType
                    && param.Type.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

                if not isGrouping && clientPreds.IsEmpty && clientProjOpt.IsNone then
                    let ctx = PolarsQueryProvider.createContextForType param.Type lfCloned param.Name

                    let sortExprOpt =
                        match ExprTranslator.tryTranslateWithContext ctx l.Body with
                        | Some h -> Some h
                        | None -> PolarsQuery<'T>.TryTranslateSelectArg ctx l.Body

                    match sortExprOpt with
                    | Some exprHandle ->
                        let spec =
                            {
                                Expr = exprHandle
                                Descending = isDesc
                                NullsLast = false
                            }

                        fuse tail (addSort spec opsAcc) clientPreds clientProjOpt
                    | None -> fuse tail opsAcc clientPreds clientProjOpt
                else
                    fuse tail opsAcc clientPreds clientProjOpt

            | LinqStage.Join(m, inner, outerKey, innerKey, resSelOpt) :: tail when
                clientPreds.IsEmpty && clientProjOpt.IsNone
                ->
                match PolarsQuery<'T>.CompileJoin lfCloned m inner outerKey innerKey with
                | Some spec ->
                    match resSelOpt with
                    | None ->
                        // .NET 11 Tuple Join without resultSelector (transparent pair of Outer and Inner)
                        fuse tail (QueryOp.Join spec :: opsAcc) clientPreds clientProjOpt
                    | Some resSel ->
                        // Inspect whether resultSelector is just a transparent identifier (e.g. (d, e) => new { d, e })
                        let isTransparent =
                            match PolarsQuery<'T>.TryExtractMemberBindings resSel.Body with
                            | Some bindings when resSel.Parameters.Count >= 2 ->
                                let p1, p2 = resSel.Parameters.[0].Name, resSel.Parameters.[1].Name

                                let rec isTransparentArg (e: Expression) =
                                    match ExprTranslator.unwrap e with
                                    | :? ParameterExpression as p -> p.Name = p1 || p.Name = p2
                                    | MemberAccess(target, m) when
                                        ExprTranslator.isNavigationMember m
                                        || ExpressionPatterns.isTransparentScopeMember m ->
                                        isTransparentArg target
                                    | :? NewExpression as ne when
                                        ne.Type.Name.StartsWith "AnonymousObject"
                                        || ne.Type.Name.StartsWith "Tuple"
                                        || ne.Type.Name.StartsWith "ValueTuple" ->
                                        ne.Arguments |> Seq.forall isTransparentArg
                                    | _ -> false

                                bindings |> List.forall (fun (_, expr) -> isTransparentArg expr)
                            | _ -> false

                        if isTransparent then
                            fuse tail (QueryOp.Join spec :: opsAcc) clientPreds clientProjOpt
                        else
                            let leftCols = PolarsQuery<'T>.GetLazySchemaFields lfCloned |> Array.map fst
                            let rightCols = PolarsQuery<'T>.GetLazySchemaFields spec.RightLf |> Array.map fst

                            match PolarsQuery<'T>.CompileJoinResultSelector leftCols rightCols spec resSel with
                            | Some selectOp ->
                                // Apply Join first, then apply the result projection
                                fuse tail (selectOp :: QueryOp.Join spec :: opsAcc) clientPreds clientProjOpt
                            | None ->
                                FallbackTracker.Diagnostic <- Some (sprintf "CompileJoinResultSelector failed: %s\nLeftCols: %A\nRightCols: %A" (resSel.ToString()) leftCols rightCols)
                                [], [], None, true
                | None ->
                    FallbackTracker.Diagnostic <- Some (sprintf "CompileJoin failed for inner: %s, outerKey: %s, innerKey: %s" (inner.ToString()) (outerKey.ToString()) (innerKey.ToString()))
                    [], [], None, true

            | LinqStage.GroupJoin(m, inner, outerKey, innerKey, _) :: LinqStage.Explode(collSelector, _) :: tail when
                collSelector.ToString().Contains "DefaultIfEmpty"
                && clientPreds.IsEmpty
                && clientProjOpt.IsNone
                ->

                match SubqueryDecorrelator.resolveSequenceLazyFrame inner with
                | Some rightLf ->
                    let outerParam = outerKey.Parameters.[0]
                    let innerParam = innerKey.Parameters.[0]

                    let leftCtx =
                        PolarsQueryProvider.createContextForType outerParam.Type lfCloned outerParam.Name

                    let rightCtx =
                        PolarsQueryProvider.createContextForType innerParam.Type rightLf innerParam.Name

                    match
                        ExprTranslator.tryTranslateWithContext leftCtx outerKey.Body,
                        ExprTranslator.tryTranslateWithContext rightCtx innerKey.Body
                    with
                    | Some leftKey, Some rightKey ->
                        let joinSpec =
                            {
                                RightLf = rightLf
                                LeftOn = [| leftKey |]
                                RightOn = [| rightKey |]
                                How = PlJoinType.Left
                                Suffix = None
                                InvertSides = false
                                Coalesce = PlJoinCoalesce.KeepColumns
                            }

                        fuse tail (QueryOp.Join joinSpec :: opsAcc) clientPreds clientProjOpt
                    | _ -> [], [], None, true
                | None -> [], [], None, true

            | LinqStage.SetOp(m, second, k1Opt, k2Opt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match PolarsQuery<'T>.CompileSetOp m second k1Opt k2Opt lfCloned with
                | Some spec -> fuse tail (QueryOp.Join spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Skip s :: LinqStage.Take t :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.Slice(int64 s, t) :: opsAcc) clientPreds clientProjOpt

            | LinqStage.Take count :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.Slice(0L, count) :: opsAcc) clientPreds clientProjOpt

            | LinqStage.Skip count :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.Slice(int64 count, UInt32.MaxValue) :: opsAcc) clientPreds clientProjOpt

            | LinqStage.Distinct :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let spec =
                    {
                        SubsetCols = None
                        Keep = PlUniqueKeepStrategy.First
                        MaintainOrder = false
                    }

                fuse tail (QueryOp.Unique spec :: opsAcc) clientPreds clientProjOpt

            | LinqStage.DistinctBy keyLambda :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match PolarsQuery<'T>.ExtractDistinctColumns keyLambda with
                | Some cols ->
                    let spec =
                        {
                            SubsetCols = Some cols
                            Keep = PlUniqueKeepStrategy.First
                            MaintainOrder = false
                        }

                    fuse tail (QueryOp.Unique spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Concat otherExpr :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match SubqueryDecorrelator.resolveSequenceLazyFrame otherExpr with
                | Some otherCompiledLf ->
                    let spec =
                        {
                            OtherLf = otherCompiledLf
                            Prepend = false
                        }

                    fuse tail (QueryOp.Concat spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Zip(secondExpr, resLambdaOpt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr with
                | Some otherLf ->
                    let baseCols = PolarsSchemaExtractor.GetColumnNames lfCloned |> Set.ofArray

                    let safeSecondLf, _, suffixMap =
                        PolarsQuery<'T>.DisambiguateColumns baseCols otherLf "_second"

                    let zipOp = QueryOp.HorizontalConcat [| PolarsWrapper.LazyClone safeSecondLf |]

                    let renameOps =
                        match resLambdaOpt with
                        | Some resLambda ->
                            let renames = PolarsQuery<'T>.ExtractBinaryResultRenames resLambda None

                            if not renames.IsEmpty then
                                let mappedRenames =
                                    renames
                                    |> List.map (fun (srcCol, destCol) ->
                                        match suffixMap.TryFind srcCol with
                                        | Some actualColName -> actualColName, destCol
                                        | None -> srcCol, destCol)

                                [
                                    QueryOp.Rename
                                        {
                                            ExistingNames = mappedRenames |> List.map fst |> List.toArray
                                            NewNames = mappedRenames |> List.map snd |> List.toArray
                                        }
                                ]
                            else
                                []
                        | None -> []

                    fuse tail (renameOps @ zipOp :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Zip3(secondExpr, thirdExpr) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match
                    SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr,
                    SubqueryDecorrelator.resolveSequenceLazyFrame thirdExpr
                with
                | Some secondLf, Some thirdLf ->
                    let baseCols = PolarsSchemaExtractor.GetColumnNames lfCloned |> Set.ofArray

                    let safeSecondLf, colsAfter2, _ =
                        PolarsQuery<'T>.DisambiguateColumns baseCols secondLf "_second"

                    let safeThirdLf, _, _ =
                        PolarsQuery<'T>.DisambiguateColumns colsAfter2 thirdLf "_third"

                    let zip3Op =
                        QueryOp.HorizontalConcat
                            [| PolarsWrapper.LazyClone safeSecondLf; PolarsWrapper.LazyClone safeThirdLf |]

                    fuse tail (zip3Op :: opsAcc) clientPreds clientProjOpt
                | _ -> [], [], None, true

            | LinqStage.Union(secondExpr, keyLambdaOpt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match SubqueryDecorrelator.resolveSequenceLazyFrame secondExpr with
                | Some otherLf ->
                    let concatOp = QueryOp.Concat { OtherLf = otherLf; Prepend = false }

                    let uniqueSpec =
                        match keyLambdaOpt with
                        | Some keyLambda ->
                            match PolarsQuery<'T>.ExtractDistinctColumns keyLambda with
                            | Some cols ->
                                {
                                    SubsetCols = Some cols
                                    Keep = PlUniqueKeepStrategy.First
                                    MaintainOrder = false
                                }
                            | None ->
                                {
                                    SubsetCols = None
                                    Keep = PlUniqueKeepStrategy.First
                                    MaintainOrder = false
                                }
                        | None ->
                            {
                                SubsetCols = None
                                Keep = PlUniqueKeepStrategy.First
                                MaintainOrder = false
                            }

                    fuse tail (QueryOp.Unique uniqueSpec :: concatOp :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.Explode(colLambda, resLambdaOpt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match PolarsQuery<'T>.ExtractExplodeColumn colLambda with
                | Some colName ->
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

                            let renames =
                                PolarsQuery<'T>.ExtractBinaryResultRenames resLambda (Some(itemParam.Name, colName))

                            if not renames.IsEmpty then
                                [
                                    QueryOp.Rename
                                        {
                                            ExistingNames = renames |> List.map fst |> List.toArray
                                            NewNames = renames |> List.map snd |> List.toArray
                                        }
                                ]
                            else
                                []
                        | _ -> []

                    fuse tail (renameOps @ (explodeOp :: opsAcc)) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.CrossJoin(innerExpr, resLambdaOpt) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                match SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr with
                | Some rightLf ->
                    let joinSpec =
                        {
                            RightLf = rightLf
                            LeftOn = [||]
                            RightOn = [||]
                            How = PlJoinType.Cross
                            Suffix = Some "_right"
                            InvertSides = false
                            Coalesce = PlJoinCoalesce.KeepColumns
                        }

                    let joinOp = QueryOp.Join joinSpec

                    let renameOps =
                        match resLambdaOpt with
                        | Some resLambda ->
                            let renames = PolarsQuery<'T>.ExtractBinaryResultRenames resLambda None

                            if not renames.IsEmpty then
                                [
                                    QueryOp.Rename
                                        {
                                            ExistingNames = renames |> List.map fst |> List.toArray
                                            NewNames = renames |> List.map snd |> List.toArray
                                        }
                                ]
                            else
                                []
                        | None -> []

                    fuse tail (renameOps @ joinOp :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.TakeWhile l :: tail ->
                let param = l.Parameters.[0]
                let ctx = PolarsQueryProvider.createContextForType typeof<'T> lfCloned param.Name

                match ExprTranslator.tryTranslateWithContext ctx l.Body with
                | Some boolExpr when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                    let cumMinExpr = PolarsWrapper.CumMin(boolExpr, false)
                    fuse tail (QueryOp.Filter cumMinExpr :: opsAcc) clientPreds clientProjOpt
                | _ ->
                    let safePred = PolarsQuery<'T>.CompileClientPredicate l
                    fuse tail opsAcc ((safePred, l) :: clientPreds) clientProjOpt

            | LinqStage.SkipWhile l :: tail ->
                let param = l.Parameters.[0]
                let ctx = PolarsQueryProvider.createContextForType typeof<'T> lfCloned param.Name

                match ExprTranslator.tryTranslateWithContext ctx l.Body with
                | Some boolExpr when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                    let notExpr = PolarsWrapper.Not boolExpr
                    let cumMaxExpr = PolarsWrapper.CumMax(notExpr, false)
                    fuse tail (QueryOp.Filter cumMaxExpr :: opsAcc) clientPreds clientProjOpt
                | _ ->
                    let safePred = PolarsQuery<'T>.CompileClientPredicate l
                    fuse tail opsAcc ((safePred, l) :: clientPreds) clientProjOpt

            // GroupJoin followed immediately by a Project stage: Desugar g.Key and g.Count() into native 2-arg pre-aggregated pipeline
            | LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, None) :: LinqStage.Project projLambda :: tail when
                clientPreds.IsEmpty && clientProjOpt.IsNone
                ->
                let outerType = outerKey.Parameters.[0].Type
                let innerType = innerKey.Parameters.[0].Type

                let desugaredResSel =
                    PolarsQuery<'T>.DesugarGroupJoinProject projLambda outerType innerType
                // Re-inject as a standard 5-argument GroupJoin with explicit resultSelector
                fuse
                    (LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, Some desugaredResSel)
                     :: tail)
                    opsAcc
                    clientPreds
                    clientProjOpt

            // Standard GroupJoin with explicit resultSelector (or desugared from above)
            | LinqStage.GroupJoin(_, innerExpr, outerKey, innerKey, Some resSel) :: tail when
                clientPreds.IsEmpty && clientProjOpt.IsNone
                ->
                match SubqueryDecorrelator.resolveSequenceLazyFrame innerExpr with
                | Some rightLf ->
                    let outerParam = outerKey.Parameters.[0]
                    let innerParam = innerKey.Parameters.[0]

                    let leftCtx =
                        PolarsQueryProvider.createContextForType outerParam.Type lfCloned outerParam.Name

                    let rightCtx =
                        PolarsQueryProvider.createContextForType innerParam.Type rightLf innerParam.Name

                    match
                        ExprTranslator.tryTranslateWithContext leftCtx outerKey.Body,
                        ExprTranslator.tryTranslateWithContext rightCtx innerKey.Body
                    with
                    | Some leftKey, Some rightKey ->
                        let groupParam = resSel.Parameters.[1]

                        let preAggRightLf, aggColOpt =
                            match PolarsQuery<'T>.TryExtractGroupAgg groupParam.Name resSel with
                            | Some(aggColName, aggExpr) ->
                                let clonedRight = PolarsWrapper.LazyClone rightLf
                                let aggKeyClone = PolarsWrapper.CloneExpr rightKey

                                let aggLf =
                                    PolarsWrapper.LazyGroupByAgg(
                                        clonedRight,
                                        [| aggKeyClone |],
                                        [| aggExpr |],
                                        null,
                                        false
                                    )

                                aggLf, Some aggColName
                            | None -> PolarsWrapper.LazyClone rightLf, None

                        let leftKeyForJoin = PolarsWrapper.CloneExpr leftKey
                        let rightKeyForJoin = PolarsWrapper.CloneExpr rightKey

                        let joinSpec =
                            {
                                RightLf = preAggRightLf
                                LeftOn = [| leftKeyForJoin |]
                                RightOn = [| rightKeyForJoin |]
                                How = PlJoinType.Left
                                Suffix = Some "_right"
                                InvertSides = false
                                Coalesce = PlJoinCoalesce.JoinSpecific
                            }

                        let joinOp = QueryOp.Join joinSpec

                        let renames = PolarsQuery<'T>.ExtractBinaryResultRenames resSel None

                        let renameOps =
                            if not renames.IsEmpty then
                                [
                                    QueryOp.Rename
                                        {
                                            ExistingNames = renames |> List.map fst |> List.toArray
                                            NewNames = renames |> List.map snd |> List.toArray
                                        }
                                ]
                            else
                                []

                        let fillOps =
                            match aggColOpt with
                            | Some aggCol ->
                                let colExpr = PolarsWrapper.Col aggCol
                                let zeroLit = PolarsWrapper.Lit 0u
                                let filledExpr = PolarsWrapper.FillNull(colExpr, zeroLit)
                                let aliased = PolarsWrapper.Alias(filledExpr, aggCol)
                                [ QueryOp.WithColumns [| aliased |] ]
                            | None -> []

                        let nextOps = renameOps @ fillOps @ joinOp :: opsAcc
                        fuse tail nextOps clientPreds clientProjOpt
                    | _ -> [], [], None, true
                | None -> [], [], None, true

            // Standalone .NET 11 GroupJoin without resultSelector: compile natively as Left Join
            | LinqStage.GroupJoin(m, innerExpr, outerKey, innerKey, None) :: tail when
                clientPreds.IsEmpty && clientProjOpt.IsNone
                ->
                match PolarsQuery<'T>.CompileJoin lfCloned m innerExpr outerKey innerKey with
                | Some spec -> fuse tail (QueryOp.Join spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.TakeLast count :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let sliceOp = QueryOp.Slice(-int64 count, count)
                fuse tail (sliceOp :: opsAcc) clientPreds clientProjOpt

            | LinqStage.SkipLast count :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.SkipLast count :: opsAcc) clientPreds clientProjOpt

            | LinqStage.Append elemExpr :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let evaluatedOpt =
                    match tryEvaluate elemExpr with
                    | Some elem -> Some elem
                    | None ->
                        try
                            let lambda = Expression.Lambda<Func<obj>>(Expression.Convert(elemExpr, typeof<obj>)).Compile()
                            Some (lambda.Invoke())
                        with _ -> None

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
                    let concatOp = QueryOp.Concat { OtherLf = otherLf; Prepend = false }
                    fuse tail (concatOp :: opsAcc) clientPreds clientProjOpt
                | _ -> [], [], None, true

            | LinqStage.Prepend elemExpr :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let evaluatedOpt =
                    match tryEvaluate elemExpr with
                    | Some elem -> Some elem
                    | None ->
                        try
                            let lambda = Expression.Lambda<Func<obj>>(Expression.Convert(elemExpr, typeof<obj>)).Compile()
                            Some (lambda.Invoke())
                        with _ -> None

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
                    let concatOp = QueryOp.Concat { OtherLf = otherLf; Prepend = true }
                    fuse tail (concatOp :: opsAcc) clientPreds clientProjOpt
                | _ -> [], [], None, true

            | LinqStage.Reverse :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail (QueryOp.Reverse :: opsAcc) clientPreds clientProjOpt

            | LinqStage.DefaultIfEmpty _ :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail opsAcc clientPreds clientProjOpt

            // Universal Select Projection Pushdown vs. Client Func Fallback
            | LinqStage.Project projLambda :: tail ->
                let decorrelatedOpt =
                    if clientPreds.IsEmpty && clientProjOpt.IsNone then
                        SubqueryDecorrelator.tryDecorrelateProjection lfCloned projLambda
                    else
                        None

                match decorrelatedOpt with
                | Some(subqueryOps, rewrittenProjLambda) ->
                    let newOpsAcc = (subqueryOps |> List.rev) @ opsAcc
                    fuse (LinqStage.Project rewrittenProjLambda :: tail) newOpsAcc clientPreds None
                | None ->
                    match PolarsQuery<'T>.TryCompileTransparentLet lfCloned projLambda with
                    | Some withColOp when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                        fuse tail (withColOp :: opsAcc) clientPreds None
                    | _ ->
                        match PolarsQuery<'T>.CompileSelectProjection lfCloned projLambda with
                        | Some exprs when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                            fuse tail (QueryOp.Select exprs :: opsAcc) clientPreds None

                        | _ ->
                            let sourceType = projLambda.Parameters.[0].Type

                            // Compile a lambda into a typed Func<obj, obj> without DynamicInvoke reflection overhead
                            let compileToBoxedFunc (lambda: LambdaExpression) =
                                let rawParam = Expression.Parameter(typeof<obj>, "rawItem")

                                let castParam =
                                    if lambda.Parameters.[0].Type = typeof<obj> then
                                        rawParam :> Expression
                                    else
                                        Expression.Convert(rawParam, lambda.Parameters.[0].Type) :> Expression

                                let invoked = Expression.Invoke(lambda, castParam)
                                let boxedResult = Expression.Convert(invoked, typeof<obj>)
                                Expression.Lambda<Func<obj, obj>>(boxedResult, rawParam).Compile()

                            let currentFunc = compileToBoxedFunc projLambda

                            let nextProj =
                                match clientProjOpt with
                                | None ->
                                    // First projection stage: sourceType -> current intermediate
                                    Some(sourceType, currentFunc)
                                | Some(prevSrcType, prevFunc) ->
                                    // Composed projection chain: prevSrcType -> prevResult -> currentResult
                                    let composed =
                                        Func<obj, obj>(fun rawItem ->
                                            let mid = prevFunc.Invoke(rawItem)
                                            currentFunc.Invoke(mid))

                                    Some(prevSrcType, composed)

                            fuse tail (QueryOp.SelectPassthrough :: opsAcc) clientPreds nextProj

            | LinqStage.Cast _ :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail opsAcc clientPreds clientProjOpt

            | LinqStage.OfType(sourceType, targetType) :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                if targetType.IsAssignableFrom(sourceType) then
                    fuse tail opsAcc clientPreds clientProjOpt
                elif not (sourceType.IsAssignableFrom(targetType)) then
                    let falseFilter = PolarsWrapper.Lit false
                    let filterOp = QueryOp.Filter falseFilter
                    fuse tail (filterOp :: opsAcc) clientPreds clientProjOpt
                else
                    fuse tail opsAcc clientPreds clientProjOpt

            | LinqStage.CountBy keyLambda :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                let keyParam = keyLambda.Parameters.[0]
                let ctx = PolarsQueryProvider.createContextForType typeof<'T> lfCloned keyParam.Name

                match ExprTranslator.tryTranslateWithContext ctx keyLambda.Body with
                | Some keyExpr ->
                    let keyAliased = PolarsWrapper.Alias(keyExpr, "Key")
                    let lenExpr = PolarsWrapper.Len()
                    let valueAliased = PolarsWrapper.Alias(lenExpr, "Value")

                    let spec =
                        {
                            Keys = [| keyAliased |]
                            Aggs = [| valueAliased |]
                            Having = None
                            MaintainOrder = false
                        }

                    fuse tail (QueryOp.GroupBy spec :: opsAcc) clientPreds clientProjOpt
                | None -> [], [], None, true

            | LinqStage.AggregateBy _ :: tail when clientPreds.IsEmpty && clientProjOpt.IsNone ->
                fuse tail opsAcc clientPreds clientProjOpt

            | [] -> List.rev opsAcc, List.rev clientPreds, clientProjOpt, false

            | _ :: tail -> fuse tail opsAcc clientPreds clientProjOpt

        let ops, clientPreds, clientProjOpt, requiresClientFallback = fuse stages [] [] None
        if requiresClientFallback && FallbackTracker.Diagnostic.IsNone then
            FallbackTracker.Diagnostic <- Some (sprintf "Stages failed to fuse. stages count: %d\nbound: %s\nstages: %A" stages.Length (bound.ToString()) stages)
        ops, clientPreds, clientProjOpt, requiresClientFallback

    /// Compiles pipeline and builds the target native LazyFrameHandle alongside client predicates, projection, and fallback signal
    member private this.GetCompiledPlan
        ()
        : LazyFrameHandle * (Func<obj, bool> list) * ((Type * Func<obj, obj>) option) * bool =
        let ops, clientPredsWithAst, clientProjOpt, requiresClientFallback =
            this.CompilePipeline()

        PolarsQuery<'T>.EnforcePolicy
            "QueryExecution"
            requiresClientFallback
            clientPredsWithAst
            (expression.ToString())

        let nativeLf = PolarsQuery<'T>.ApplyNativeOps lfCloned ops
        // Extract pure Func<obj, bool> delegates for in-memory materializers
        let clientPredicates = clientPredsWithAst |> List.map fst
        nativeLf, clientPredicates, clientProjOpt, requiresClientFallback

    /// Executes the full LINQ expression pipeline in-memory via .NET EnumerableQuery LINQ-to-Objects engine
    member private this.ExecuteInMemory() : IEnumerable<'T> =
        // 1. Rewrite expression tree to eliminate F# transparent identifiers
        let effectiveExpr = FSharpAst.rewrite expression

        // 2. Extract root queryable source
        let rec findRootQueryable (e: Expression) : (Expression * Type) option =
            match e with
            | null -> None
            | :? ConstantExpression as ce when
                typeof<IQueryable>.IsAssignableFrom(ce.Type)
                || typeof<IPolarsPlanSource>.IsAssignableFrom(ce.Type)
                ->
                Some(ce :> Expression, ce.Type)
            | :? MethodCallExpression as mc when mc.Arguments.Count > 0 -> findRootQueryable mc.Arguments.[0]
            | _ -> None

        match findRootQueryable effectiveExpr with
        | Some(rootExpr, rootType) ->
            let rootConst = rootExpr :?> ConstantExpression
            let rootQueryable = rootConst.Value :?> IQueryable

            let rootElemType =
                match PolarsTypeHelper.TryGetEnumerableElementType rootType with
                | null -> rootQueryable.ElementType
                | t -> t

            // 3. Materialize the base table natively from Polars into an in-memory array once
            let toArrayMethod =
                LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| rootElemType |])

            let castMethod =
                LinqReflectionCache.GetEnumerableMethod("Cast", 1, [| rootElemType |])

            let casted = castMethod.Invoke(null, [| box rootQueryable |])
            let inMemoryArray = toArrayMethod.Invoke(null, [| casted |])

            // 4. Wrap into standard BCL EnumerableQuery (disconnects from Polars QueryProvider to prevent infinite recursion)
            let asQueryableMethod =
                typeof<Queryable>.GetMethods()
                |> Array.find (fun m -> m.Name = "AsQueryable" && m.IsGenericMethodDefinition)
                |> fun m -> m.MakeGenericMethod(rootElemType)

            let inMemoryQueryable =
                asQueryableMethod.Invoke(null, [| inMemoryArray |]) :?> IQueryable

            // 5. Replace root expression constant with the pure in-memory EnumerableQuery instance
            let targetQueryType = typedefof<IQueryable<_>>.MakeGenericType(rootElemType)
            let replacement = Expression.Constant(inMemoryQueryable, targetQueryType)

            let replacer =
                { new ExpressionVisitor() with
                    override this.VisitConstant(node: ConstantExpression) =
                        if
                            not (isNull node.Value)
                            && (obj.ReferenceEquals(node.Value, rootConst.Value)
                                || typeof<IPolarsPlanSource>.IsAssignableFrom(node.Type))
                        then
                            replacement
                        else
                            base.VisitConstant(node)
                }

            let inMemoryExpr = replacer.Visit(effectiveExpr)

            // 6. Execute using standard BCL LINQ-to-Objects provider (completely bypassing Polars compilation pipeline)
            let resultQueryable = inMemoryQueryable.Provider.CreateQuery<'T>(inMemoryExpr)
            resultQueryable :> IEnumerable<'T>

        | None -> failwithf "Failed to locate root queryable for in-memory execution: %A" expression

    /// Evaluates the complete pipeline, executing native plan first, then materializing
    member private this.ExecuteQuery() : IEnumerable<'T> =
        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback =
            this.GetCompiledPlan()

        // Branch -1: Universal In-Memory LINQ Fallback
        if requiresClientFallback then
            this.ExecuteInMemory()
        else
            let collectedDf = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
            // let dfHeight = PolarsWrapper.DataFrameHeight collectedDf
            // printfn "\x1b[1;32m[EXECUTE QUERY COLLECTED] Height=%d\x1b[0m" dfHeight

            let targetType = typeof<'T>

            let isGrouping =
                targetType.IsGenericType
                && targetType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

            let shouldShuffle = PolarsQuery<'T>.HasShuffle expression

            let dfHandle =
                if shouldShuffle then
                    PolarsQuery<'T>.ApplyShuffle collectedDf
                else
                    collectedDf

            // Branch 0: Client-side UDF Projection Fallback
            match clientProjOpt with
            | Some(sourceType, func) ->
                let mat = QueryMaterializerResolver.Resolve(Some materializer)
                let invoker = LinqReflectionCache.GetMaterializerInvoker sourceType
                let rawSourceRows = invoker.Invoke(mat, dfHandle)

                // Apply client-side predicates to materialized rows, then apply composed projection
                let filteredSource =
                    if clientPredicates.IsEmpty then
                        rawSourceRows
                    else
                        seq {
                            for item in rawSourceRows do
                                if clientPredicates |> List.forall (fun p -> p.Invoke item) then
                                    yield item
                        }

                seq {
                    for item in filteredSource do
                        yield func.Invoke item :?> 'T
                }

            | None ->
                let rec findChunkSize (e: Expression) : int option =
                    match e with
                    | null -> None
                    | MethodCall(m, null, [ _; sizeExpr ]) when m.Name = "Chunk" ->
                        match tryEvaluate sizeExpr with
                        | Some s -> Some(Convert.ToInt32 s)
                        | None -> None
                    | MethodCall(_, null, args) -> args |> List.tryPick findChunkSize
                    | _ -> None

                let chunkSizeOpt =
                    if targetType.IsArray then
                        findChunkSize expression
                    else
                        None

                let rec findAggregateBy (e: Expression) =
                    match e with
                    | null -> None
                    | MethodCall(m, null, source :: CleanLambda k :: seedExpr :: CleanLambda f :: rest) when
                        m.Name = "AggregateBy"
                        ->
                        let seedLambdaOpt =
                            match seedExpr with
                            | CleanLambda s -> Some s
                            | _ -> None

                        let comparerExprOpt = rest |> List.tryHead
                        Some(source, k, seedExpr, seedLambdaOpt, f, comparerExprOpt)
                    | MethodCall(_, null, args) -> args |> List.tryPick findAggregateBy
                    | _ -> None

                let aggByInfoOpt = findAggregateBy expression

                // Case 1: GroupBy & GroupJoin materialization (produces IEnumerable<IGrouping<TKey, TElement>>)
                if isGrouping then
                    let genericArgs = targetType.GetGenericArguments()
                    let keyType, elemType = genericArgs.[0], genericArgs.[1]

                    let mat = QueryMaterializerResolver.Resolve(Some materializer)

                    let rec findGroupByInfo (e: Expression) : (LambdaExpression * LambdaExpression option) option =
                        match e with
                        | null -> None
                        | MethodCall(m, null, [ _; CleanLambda k; CleanLambda elem ]) when m.Name = "GroupBy" ->
                            Some(k, Some elem)
                        | MethodCall(m, null, [ _; CleanLambda k ]) when m.Name = "GroupBy" -> Some(k, None)
                        | MethodCall(_, null, args) -> args |> List.tryPick findGroupByInfo
                        | _ -> None

                    let rec findGroupJoin (e: Expression) =
                        match e with
                        | null -> false
                        | MethodCall(m, null, _) when m.Name = "GroupJoin" -> true
                        | MethodCall(_, null, args) -> args |> List.exists findGroupJoin
                        | _ -> false

                    let rec findSortSelector (e: Expression) : (LambdaExpression * bool) option =
                        match e with
                        | null -> None
                        | MethodCall(m, null, [ _; CleanLambda l ]) when m.Name = "OrderBy" -> Some(l, false)
                        | MethodCall(m, null, [ _; CleanLambda l ]) when m.Name = "OrderByDescending" -> Some(l, true)
                        | MethodCall(_, null, args) -> args |> List.tryPick findSortSelector
                        | _ -> None

                    match findGroupByInfo expression with
                    | Some(keyLambda, elemLambdaOpt) ->
                        // Standard GroupBy materialization path
                        let compiledKeySelector = keyLambda.Compile()

                        let grouped =
                            match elemLambdaOpt with
                            | Some elemLambda ->
                                let sourceType = elemLambda.Parameters.[0].Type
                                let invoker = LinqReflectionCache.GetMaterializerInvoker sourceType
                                let rawRows = invoker.Invoke(mat, dfHandle)
                                let compiledElemSelector = elemLambda.Compile()

                                let groupByMethod =
                                    typeof<Enumerable>.GetMethods()
                                    |> Array.find (fun m ->
                                        m.Name = "GroupBy"
                                        && m.GetParameters().Length = 3
                                        && m.GetGenericArguments().Length = 3)
                                    |> fun m -> m.MakeGenericMethod(sourceType, keyType, elemType)

                                groupByMethod.Invoke(
                                    null,
                                    [| box rawRows; box compiledKeySelector; box compiledElemSelector |]
                                )
                                :?> IEnumerable<'T>
                            | None ->
                                let invoker = LinqReflectionCache.GetMaterializerInvoker elemType
                                let rawRows = invoker.Invoke(mat, dfHandle)

                                let groupByMethod =
                                    typeof<Enumerable>.GetMethods()
                                    |> Array.find (fun m ->
                                        m.Name = "GroupBy"
                                        && m.GetParameters().Length = 2
                                        && m.GetGenericArguments().Length = 2)
                                    |> fun m -> m.MakeGenericMethod(elemType, keyType)

                                groupByMethod.Invoke(null, [| box rawRows; box compiledKeySelector |])
                                :?> IEnumerable<'T>

                        let filtered =
                            if clientPredicates.IsEmpty then
                                grouped
                            else
                                grouped
                                |> Seq.filter (fun g -> clientPredicates |> List.forall (fun p -> p.Invoke(box g)))

                        match findSortSelector expression with
                        | Some(sortLambda, false) ->
                            let compiled = sortLambda.Compile()

                            filtered
                            |> Seq.sortBy (fun g -> compiled.DynamicInvoke([| box g |]) :?> IComparable)
                        | Some(sortLambda, true) ->
                            let compiled = sortLambda.Compile()

                            filtered
                            |> Seq.sortByDescending (fun g -> compiled.DynamicInvoke([| box g |]) :?> IComparable)
                        | None -> filtered

                    | None when findGroupJoin expression ->
                        // .NET 11 Standalone GroupJoin materialization:
                        // keyType = TOuter, elemType = TInner
                        let innerNullableType =
                            if elemType.IsValueType then
                                typedefof<Nullable<_>>.MakeGenericType elemType
                            else
                                elemType

                        let pairType =
                            typedefof<ValueTuple<_, _>>.MakeGenericType(keyType, innerNullableType)

                        let invoker = LinqReflectionCache.GetMaterializerInvoker pairType
                        let rawPairs = invoker.Invoke(mat, dfHandle)

                        // Hydrate hierarchical groupings preserving outer sequence encounter order
                        let keyOrder = List<obj>()
                        let groupMap = Dictionary<obj, List<obj>>()

                        for pairObj in rawPairs do
                            let tuple = pairObj :?> System.Runtime.CompilerServices.ITuple
                            let outerObj = tuple.[0]
                            let innerObj = tuple.[1]

                            match groupMap.TryGetValue outerObj with
                            | true, list ->
                                if not (isNull innerObj) then
                                    list.Add innerObj
                            | false, _ ->
                                let list = List<obj>()

                                if not (isNull innerObj) then
                                    list.Add innerObj

                                groupMap.[outerObj] <- list
                                keyOrder.Add outerObj

                        // Build strongly-typed PolarsGrouping<TOuter, TInner> instances
                        let groupingType =
                            typedefof<PolarsGrouping<_, _>>.MakeGenericType(keyType, elemType)

                        let ctor = groupingType.GetConstructors().[0]

                        let toArrayMethod =
                            typeof<Enumerable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                            |> Array.find (fun m -> m.Name = "ToArray" && m.GetParameters().Length = 1)
                            |> fun m -> m.MakeGenericMethod(elemType)

                        let castMethod =
                            typeof<Enumerable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                            |> Array.find (fun m -> m.Name = "Cast" && m.GetParameters().Length = 1)
                            |> fun m -> m.MakeGenericMethod(elemType)

                        let resultList = List<'T>()

                        for k in keyOrder do
                            let rawList = groupMap.[k]
                            let castedSeq = castMethod.Invoke(null, [| box rawList |])
                            let typedArray = toArrayMethod.Invoke(null, [| castedSeq |])
                            let grpInstance = ctor.Invoke [| k; typedArray |] :?> 'T
                            resultList.Add grpInstance

                        match findSortSelector expression with
                        | Some(sortLambda, false) ->
                            let compiled = sortLambda.Compile()

                            resultList
                            |> Seq.sortBy (fun g -> compiled.DynamicInvoke [| box g |] :?> IComparable)
                        | Some(sortLambda, true) ->
                            let compiled = sortLambda.Compile()

                            resultList
                            |> Seq.sortByDescending (fun g -> compiled.DynamicInvoke [| box g |] :?> IComparable)
                        | None -> resultList :> IEnumerable<'T>

                    | None -> failwith "Could not locate keySelector for GroupBy pipeline."

                // Case 2: Chunk slicing via stack-based cursor
                elif chunkSizeOpt.IsSome then
                    let chunkSize = chunkSizeOpt.Value
                    let elemType = targetType.GetElementType()

                    let mat = QueryMaterializerResolver.Resolve(Some materializer)
                    let invoker = LinqReflectionCache.GetMaterializerChunkInvoker elemType
                    let chunkedSeq = invoker.Invoke(mat, dfHandle, chunkSize)
                    chunkedSeq.Cast<'T>()

                // Case 3: AggregateBy (produces IEnumerable<KeyValuePair<TKey, TAccum>>)
                elif aggByInfoOpt.IsSome then
                    let sourceExpr, keyLambda, seedExpr, seedLambdaOpt, funcLambda, comparerExprOpt =
                        aggByInfoOpt.Value

                    let genericArgs = targetType.GetGenericArguments()
                    let keyType, accumType = genericArgs.[0], genericArgs.[1]
                    let sourceElemType = keyLambda.Parameters.[0].Type

                    let compiledKey = keyLambda.Compile()
                    let compiledFunc = funcLambda.Compile()

                    let comparerObj =
                        match comparerExprOpt with
                        | Some ce -> tryEvaluate ce |> Option.toObj
                        | None -> null

                    let mat = QueryMaterializerResolver.Resolve(Some materializer)

                    // 1. Resolve source query plan so that upstream filters/projections are properly compiled
                    let sourceQuery = (provider :> IQueryProvider).CreateQuery(sourceExpr)

                    let sourceLf =
                        match box sourceQuery with
                        | :? IPolarsPlanSource as ps -> ps.GetCompiledLazyFrameHandle()
                        | _ -> nativeLf

                    // 2. Collect filtered source LazyFrame into DataFrameHandle
                    let sourceDfHandle = PolarsWrapper.LazyCollect(sourceLf, PlEngine.Auto)

                    match seedLambdaOpt with
                    | Some seedLambda ->
                        let compiledSeed = seedLambda.Compile()

                        let invoker =
                            LinqReflectionCache.GetMaterializerAggByFactoryInvoker(sourceElemType, keyType, accumType)

                        let res =
                            invoker.Invoke(mat, sourceDfHandle, compiledKey, compiledSeed, compiledFunc, comparerObj)

                        res.Cast<'T>()

                    | None ->
                        let seedVal =
                            match tryEvaluate seedExpr with
                            | Some s -> s
                            | None -> failwith "Failed to evaluate static seed for AggregateBy."

                        let invoker =
                            LinqReflectionCache.GetMaterializerAggBySeedInvoker(sourceElemType, keyType, accumType)

                        let res =
                            invoker.Invoke(mat, sourceDfHandle, compiledKey, seedVal, compiledFunc, comparerObj)

                        res.Cast<'T>()

                // Case 4: Flat row materialization (standard records, anonymous types, DTOs)
                else
                    let height = PolarsWrapper.DataFrameHeight dfHandle

                    let rec findDefaultIfEmpty (e: Expression) : (bool * obj option) option =
                        match e with
                        | null -> None
                        | MethodCall(m, null, [ _ ]) when m.Name = "DefaultIfEmpty" -> Some(true, None)
                        | MethodCall(m, null, [ _; defExpr ]) when m.Name = "DefaultIfEmpty" ->
                            Some(true, tryEvaluate defExpr)
                        | MethodCall(_, null, args) -> args |> List.tryPick findDefaultIfEmpty
                        | _ -> None

                    match findDefaultIfEmpty expression with
                    | Some(true, customDefaultOpt) when height = 0L ->
                        let defaultItem =
                            match customDefaultOpt with
                            | Some v when not (isNull v) -> v :?> 'T
                            | _ -> Unchecked.defaultof<'T>

                        Seq.singleton defaultItem
                    | _ ->
                        let mat = QueryMaterializerResolver.Resolve(Some materializer)
                        let rows = mat.Materialize<'T>(dfHandle)

                        if clientPredicates.IsEmpty then
                            rows
                        else
                            rows
                            |> Seq.filter (fun item -> clientPredicates |> List.forall (fun p -> p.Invoke(box item)))

    /// Compiles pipeline to LazyFrameHandle. If client fallback, predicates or projections exist, materializes and transposes back.
    member this.CompileToLazyFrameHandle() : LazyFrameHandle =
        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback =
            this.GetCompiledPlan()

        let schema = PolarsWrapper.GetLazySchema nativeLf
        let width = PolarsWrapper.GetSchemaLen schema
        let targetType = typeof<'T>

        let isScalar =
            PolarsTypeHelper.IsScalarType targetType || targetType = typeof<string>

        let isGrouping =
            targetType.IsGenericType
            && targetType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

        let requiresClientEvaluation =
            requiresClientFallback
            || not clientPredicates.IsEmpty
            || clientProjOpt.IsSome
            || (isScalar && width > 1UL)
            || isGrouping

        if not requiresClientEvaluation then
            nativeLf
        else
            let filteredRows = this.ExecuteQuery()
            let newDfHandle = DataFrameBuilder.FromRows<'T>(filteredRows)
            PolarsWrapper.DataFrameToLazy newDfHandle

    /// Compiles the query and materializes directly into a native DataFrameHandle
    member this.CompileToDataFrameHandle() : DataFrameHandle =
        let chunkSizeOpt = PolarsQuery<'T>.TryExtractChunkSize expression

        if chunkSizeOpt.IsSome then
            raise (
                InvalidOperationException(
                    "Cannot materialize a chunked sequence (IEnumerable<T[]>) into a single flat DataFrame. "
                    + "If you want to iterate over DataFrames in batches, use 'ToBatchesAsync(chunkSize)' or 'CollectBatchesStream'. "
                    + "If you want in-memory array chunks, enumerate the query directly via ToList() / foreach."
                )
            )

        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback =
            this.GetCompiledPlan()

        let schema = PolarsWrapper.GetLazySchema nativeLf
        let width = PolarsWrapper.GetSchemaLen schema

        let isScalar =
            PolarsTypeHelper.IsScalarType typeof<'T> || typeof<'T> = typeof<string>

        let isGrouping =
            typeof<'T>.IsGenericType
            && typeof<'T>.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

        let shouldShuffle = PolarsQuery<'T>.HasShuffle expression

        let canDirectlyCollectNative =
            not requiresClientFallback
            && clientPredicates.IsEmpty
            && clientProjOpt.IsNone
            && not (isScalar && width > 1UL)
            && not isGrouping

        if canDirectlyCollectNative then
            let collectedDf = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

            if shouldShuffle then
                PolarsQuery<'T>.ApplyShuffle collectedDf
            else
                collectedDf
        else
            let rows = this.ExecuteQuery()
            DataFrameBuilder.FromRows<'T>(rows)

    interface IInternalPlanSource with
        member this.GetPlanInfo() =
            let nativeLf, clientPreds, clientProjOpt, requiresClientFallback =
                this.GetCompiledPlan()

            nativeLf, requiresClientFallback || not clientPreds.IsEmpty, clientProjOpt

    interface IEnumerable<'T> with
        member this.GetEnumerator() : IEnumerator<'T> = (this.ExecuteQuery()).GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() : IEnumerator =
            (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator

    interface IQueryable with
        member _.ElementType = typeof<'T>
        member _.Expression = expression
        member _.Provider = provider :> IQueryProvider

    interface IQueryable<'T>

    interface IOrderedQueryable<'T>

    interface IPolarsPlanSource with
        member this.GetCompiledLazyFrameHandle() = this.CompileToLazyFrameHandle()

    member this.GetPlanInspection(optimized: bool) : PlanInspection =
        let ops, clientPreds, clientProjOpt, requiresClientFallback = this.CompilePipeline()
        let nativeLf = PolarsQuery<'T>.ApplyNativeOps lfCloned ops
        let planStr = PolarsWrapper.Explain(nativeLf, optimized)
        let chunkSizeOpt = PolarsQuery<'T>.TryExtractChunkSize expression
        let shouldShuffle = PolarsQuery<'T>.HasShuffle expression

        let isGrouping =
            typeof<'T>.IsGenericType
            && typeof<'T>.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

        let mutable postOps = []

        if shouldShuffle then
            postOps <-
                "[Eager Post-Processing]: Shuffle(fraction = 1.0, with_replacement = false)"
                :: postOps

        if isGrouping then
            postOps <-
                "[Client Post-Processing]: Materialize to IEnumerable<IGrouping<TKey, TElement>>"
                :: postOps

        match chunkSizeOpt with
        | Some size ->
            postOps <-
                sprintf "[Client Post-Processing]: Chunk(size = %d) -> Materialize as TSource[] batches" size
                :: postOps
        | None -> ()

        let planWithPostOps =
            if postOps.IsEmpty then
                planStr
            else
                planStr + "\n" + String.concat "\n" (List.rev postOps)

        {
            NativePlan = planWithPostOps
            HasClientPredicates = not clientPreds.IsEmpty
            ClientPredicateExpressions = clientPreds |> List.map snd
            HasClientProjection = clientProjOpt.IsSome || chunkSizeOpt.IsSome || shouldShuffle || isGrouping
            IsFullFallback = requiresClientFallback
            FallbackReason = FallbackTracker.Diagnostic
        }

    interface IPolarsExplainable with
        member this.InspectPlan(optimized: bool) = this.GetPlanInspection optimized

        member this.Explain(optimized: bool) =
            (this.GetPlanInspection optimized).FormatReport()

    interface IAsyncEnumerable<'T> with
        member this.GetAsyncEnumerator(cancellationToken: CancellationToken) : IAsyncEnumerator<'T> =
            // Fail-fast immediately if token is already cancelled
            cancellationToken.ThrowIfCancellationRequested()

            let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback =
                this.GetCompiledPlan()

            let targetType = typeof<'T>

            let isGrouping =
                targetType.IsGenericType
                && targetType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

            let rec findChunkSize (e: Expression) : int option =
                match e with
                | null -> None
                | MethodCall(m, null, [ _; sizeExpr ]) when m.Name = "Chunk" ->
                    match tryEvaluate sizeExpr with
                    | Some s -> Some(Convert.ToInt32 s)
                    | None -> None
                | MethodCall(_, null, args) -> args |> List.tryPick findChunkSize
                | _ -> None

            let rec findAggregateBy (e: Expression) =
                match e with
                | null -> None
                | MethodCall(m, null, source :: CleanLambda k :: seedExpr :: CleanLambda f :: rest) when
                    m.Name = "AggregateBy"
                    ->
                    let seedLambdaOpt =
                        match seedExpr with
                        | CleanLambda s -> Some s
                        | _ -> None

                    let comparerExprOpt = rest |> List.tryHead
                    Some(source, k, seedExpr, seedLambdaOpt, f, comparerExprOpt)
                | MethodCall(_, null, args) -> args |> List.tryPick findAggregateBy
                | _ -> None

            let rec findDefaultIfEmpty (e: Expression) : bool =
                match e with
                | null -> false
                | MethodCall(m, null, _) when m.Name = "DefaultIfEmpty" -> true
                | MethodCall(_, null, args) -> args |> List.exists findDefaultIfEmpty
                | _ -> false

            let canStreamBatches =
                not requiresClientFallback
                && not (PolarsQuery<'T>.HasShuffle expression)
                && not isGrouping
                && not (findDefaultIfEmpty expression)
                && (findChunkSize expression).IsNone
                && (findAggregateBy expression).IsNone

            if not canStreamBatches then
                // Fallback path: offload full in-memory LINQ or specialized reduction pipeline to thread pool
                let enumeratorRef = ref (None: IEnumerator<'T> option)

                { new IAsyncEnumerator<'T> with
                    member _.Current =
                        match enumeratorRef.Value with
                        | Some e -> e.Current
                        | None -> invalidOp "Enumeration has not started."

                    member _.MoveNextAsync() : ValueTask<bool> =
                        cancellationToken.ThrowIfCancellationRequested()

                        match enumeratorRef.Value with
                        | Some e -> ValueTask<bool>(e.MoveNext())
                        | None ->
                            ValueTask<bool>(
                                task {
                                    cancellationToken.ThrowIfCancellationRequested()
                                    let rows = this.ExecuteQuery()
                                    let e = rows.GetEnumerator()
                                    enumeratorRef.Value <- Some e
                                    return e.MoveNext()
                                }
                            )

                    member _.DisposeAsync() : ValueTask =
                        match enumeratorRef.Value with
                        | Some e -> e.Dispose()
                        | None -> ()

                        ValueTask()
                }
            else
                // Native streaming batch path: streams batches from Rust engine with O(batch) memory overhead
                let batchStream =
                    PolarsWrapper.LazyCollectBatchesStreamAsync(
                        nativeLf,
                        PlEngine.Auto,
                        maintainOrder = false,
                        chunkSize = 0L,
                        cancellationToken = cancellationToken
                    )

                let batchEnumerator = batchStream.GetAsyncEnumerator cancellationToken
                let mutable currentBatchHandle: DataFrameHandle = null
                let mutable currentBatchRows: IEnumerator<'T> = null
                let mutable hasActiveRows = false
                let mutable isDisposed = false

                let materializeBatch (dfHandle: DataFrameHandle) : IEnumerable<'T> =
                    let mat = QueryMaterializerResolver.Resolve(Some materializer)

                    match clientProjOpt with
                    | Some(sourceType, func) ->
                        let invoker = LinqReflectionCache.GetMaterializerInvoker sourceType
                        let rawSourceRows = invoker.Invoke(mat, dfHandle)
                        // Directly enumerate non-generic IEnumerable with single-pass filter and projection
                        seq {
                            for item in rawSourceRows do
                                if
                                    clientPredicates.IsEmpty
                                    || clientPredicates |> List.forall (fun p -> p.Invoke item)
                                then
                                    yield func.Invoke item :?> 'T
                        }
                    | None ->
                        let rows = mat.Materialize<'T>(dfHandle)

                        if clientPredicates.IsEmpty then
                            rows
                        else
                            rows
                            |> Seq.filter (fun item -> clientPredicates |> List.forall (fun p -> p.Invoke(box item)))

                { new IAsyncEnumerator<'T> with
                    member _.Current =
                        if hasActiveRows && not (isNull currentBatchRows) then
                            currentBatchRows.Current
                        else
                            invalidOp "Enumeration has not started or is completed."

                    member _.MoveNextAsync() : ValueTask<bool> =
                        cancellationToken.ThrowIfCancellationRequested()

                        // Fast-path: tight synchronous iteration within active batch without Task allocation
                        if hasActiveRows && currentBatchRows.MoveNext() then
                            ValueTask<bool>(true)
                        else
                            // Slow-path: fetch next native batch asynchronously
                            ValueTask<bool>(
                                task {
                                    cancellationToken.ThrowIfCancellationRequested()
                                    let mutable advanced = false
                                    let mutable doneStreaming = false

                                    while not advanced && not doneStreaming do
                                        if hasActiveRows && currentBatchRows.MoveNext() then
                                            advanced <- true
                                        else
                                            // Immediately dispose unmanaged memory of exhausted batch
                                            if not (isNull currentBatchHandle) then
                                                currentBatchHandle.Dispose()
                                                currentBatchHandle <- null
                                                currentBatchRows <- null
                                                hasActiveRows <- false

                                            let! hasNextBatch = batchEnumerator.MoveNextAsync()

                                            if hasNextBatch then
                                                let dfHandle = batchEnumerator.Current
                                                let h = PolarsWrapper.DataFrameHeight dfHandle

                                                if h > 0L then
                                                    currentBatchHandle <- dfHandle
                                                    let rows = materializeBatch dfHandle
                                                    currentBatchRows <- rows.GetEnumerator()
                                                    hasActiveRows <- true
                                                else
                                                    dfHandle.Dispose()
                                            else
                                                doneStreaming <- true

                                    return advanced
                                }
                            )

                    member _.DisposeAsync() : ValueTask =
                        ValueTask(
                            task {
                                if not isDisposed then
                                    isDisposed <- true

                                    if not (isNull currentBatchHandle) then
                                        currentBatchHandle.Dispose()
                                        currentBatchHandle <- null
                                        currentBatchRows <- null
                                        hasActiveRows <- false

                                    do! batchEnumerator.DisposeAsync()
                            }
                        )
                }
