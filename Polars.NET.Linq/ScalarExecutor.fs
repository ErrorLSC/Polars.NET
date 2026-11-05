namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core
open Polars.NET.Core.Enums
open Polars.NET.Core.Helpers
open Polars.NET.Linq.Backend.Polars

/// Dedicated execution engine for LINQ scalar & single-row terminal operations
/// (Count, LongCount, Any, All, First*, Last*, Single*, Min, Max, Sum, Average, ElementAt*, Contains, SequenceEqual, MinBy, MaxBy, Aggregate).
module internal ScalarExecutor =

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

    /// Dispatches and resolves predicate into either native ExprHandle pushdown or composed client Func<obj, bool>
    let internal tryResolvePredicate
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
                | None ->
                    ValueSome(Client(predArgType, compiledPred))

    /// Maps standard reduction method names to Polars aggregate expressions
    let internal resolveScalarAggExpr (methodName: string) (colExpr: ExprHandle) : ExprHandle =
        match methodName with
        | "Max" -> PolarsWrapper.Max colExpr
        | "Min" -> PolarsWrapper.Min colExpr
        | "Sum" -> PolarsWrapper.Sum colExpr
        | "Average" -> PolarsWrapper.Mean colExpr
        | _ -> PolarsWrapper.Len()

    /// Builds a Polars equality filter expression matching an item (scalar primitive or composite entity/record)
    /// against the columns of a target LazyFrame.
    let internal buildItemEqualityFilter (itemVal: obj) (targetLf: LazyFrameHandle) : ExprHandle option =
        if isNull itemVal then
            None
        else
            let itemType = itemVal.GetType()
            if PolarsTypeHelper.IsScalarType itemType then
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

    /// Extracts the effective source element type considering client projections.
    let private getEffectiveSourceType (clientProjOpt: (Type * Func<obj, obj>) option) (elemType: Type) : Type =
        clientProjOpt |> Option.map fst |> Option.defaultValue elemType

    /// Extracts the optional comparer instance from arguments if supplied.
    let private tryExtractComparer (rest: Expression list) : obj =
        match rest with
        | [ comparerExpr ] -> tryEvaluate comparerExpr |> Option.toObj
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

    /// Attempts to extract plan info (LazyFrame, hasClientPreds, clientProjOpt) from an expression,
    /// supporting both uncorrelated query handles, decorrelated subtrees, and queryables.
    let private tryResolvePlanFromExpr
        (provider: IQueryProvider)
        (expr: Expression)
        : (LazyFrameHandle * bool * (Type * Func<obj, obj>) option) option =
        match PolarsObjectConverter.resolveSequenceLazyFrame expr with
        | Some lf -> Some(lf, false, None)
        | None ->
            match tryEvaluate expr with
            | Some(:? IInternalPlanSource as ps) -> Some(ps.GetPlanInfo())
            | Some(:? IPolarsPlanSource as ps) -> Some(ps.GetCompiledLazyFrameHandle(), false, None)
            | _ -> None

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

    /// Checks whether a LazyFrame (optionally filtered by filterOpt) contains at least 1 row via LazySlice(0, 1).
    let private hasAnyRow (lf: LazyFrameHandle) (filterOpt: ExprHandle option) : bool =
        let targetLf =
            match filterOpt with
            | Some f -> PolarsWrapper.LazyFilter(lf, f)
            | None -> lf

        let slicedLf = PolarsWrapper.LazySlice(targetLf, 0L, 1u)
        let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto)
        PolarsWrapper.DataFrameHeight dfHandle > 0L

    /// Executes a native Polars Len() query on a LazyFrame (optionally filtered) and returns the raw int64 row count.
    let private collectNativeRowCount
        (mat: IDataFrameMaterializer)
        (lf: LazyFrameHandle)
        (filterOpt: ExprHandle option)
        : int64 =
        let targetLf =
            match filterOpt with
            | Some f -> PolarsWrapper.LazyFilter(lf, f)
            | None -> lf

        let aggLf = PolarsWrapper.LazySelect(targetLf, [| PolarsWrapper.Len() |])
        let dfHandle = PolarsWrapper.LazyCollect(aggLf, PlEngine.Auto)
        mat.MaterializeScalar<int64>(dfHandle)

    /// Unified row cursor executor for First, Last, Single (and their OrDefault variants)
    let executeRowFetch<'TResult>
        (mat: IDataFrameMaterializer)
        (kind: RowFetchKind)
        (isOrDefault: bool)
        (userDefValOpt: obj option)
        (nativeLf: LazyFrameHandle)
        (hasClientPreds: bool)
        (clientProjOpt: (Type * Func<obj, obj>) option)
        (elemType: Type)
        (predLambdaOpt: LambdaExpression option)
        : 'TResult =

        let resolvedPred =
            tryResolvePredicate hasClientPreds clientProjOpt elemType nativeLf predLambdaOpt

        let userDefVal = defaultArg userDefValOpt (PolarsTypeHelper.GetDefaultValue elemType)

        let offset, len, kindName =
            match kind with
            | RowFetchKind.First -> 0L, 1u, "First"
            | RowFetchKind.Last -> -1L, 1u, "Last"
            | RowFetchKind.Single -> 0L, 2u, "Single"

        let invokeRowFetch (targetType: Type) (df: DataFrameHandle) (pred: Delegate) (defVal: obj) =
            if isOrDefault then
                let invoker =
                    LinqReflectionCache.GetMaterializerRowFetchOrDefaultInvoker(kindName + "OrDefault", targetType)

                invoker.Invoke(mat, df, pred, defVal)
            else
                let invoker = LinqReflectionCache.GetMaterializerRowFetchInvoker(kindName, targetType)
                invoker.Invoke(mat, df, pred)

        match resolvedPred with
        | ValueSome(Pushdown filterExpr) ->
            let filteredLf = PolarsWrapper.LazyFilter(nativeLf, filterExpr)
            let slicedLf = PolarsWrapper.LazySlice(filteredLf, offset, len)
            let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto)
            invokeRowFetch elemType dfHandle null userDefVal :?> 'TResult

        | ValueSome(Client(srcType, combinedPred)) ->
            enforceClientPredicatePolicy (kindName, predLambdaOpt)
            let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
            let rawEntity = invokeRowFetch srcType dfHandle combinedPred (PolarsTypeHelper.GetDefaultValue srcType)
            projectClientResult<'TResult> clientProjOpt userDefVal rawEntity

        | ValueNone ->
            let targetType = getEffectiveSourceType clientProjOpt elemType
            let dfHandle =
                if not hasClientPreds then
                    let slicedLf = PolarsWrapper.LazySlice(nativeLf, offset, len)
                    PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto)
                else
                    PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

            let defVal = if clientProjOpt.IsSome then PolarsTypeHelper.GetDefaultValue targetType else userDefVal
            let rawEntity = invokeRowFetch targetType dfHandle null defVal
            projectClientResult<'TResult> clientProjOpt userDefVal rawEntity

    /// Core scalar execution pipeline: compiles native plan, executes native reductions,
    /// and extracts scalar results via IDataFrameMaterializer.
    let executeScalar<'TResult>
        (provider: IQueryProvider)
        (mat: IDataFrameMaterializer)
        (expression: Expression)
        : 'TResult =
        let targetType = typeof<'TResult>

        let resolveQueryPlan (src: Expression) : LazyFrameHandle * bool * (Type * Func<obj, obj>) option =
            let query = provider.CreateQuery src

            match query with
            | :? IInternalPlanSource as ps -> ps.GetPlanInfo()
            | :? IPolarsPlanSource as ps -> ps.GetCompiledLazyFrameHandle(), false, None
            | _ -> failwithf "Unable to extract LazyFrame from query source: %A" src

        let extractOptionalLambda (args: Expression list) : LambdaExpression option =
            match args with
            | [ CleanLambda l ] -> Some l
            | _ -> None

        let evaluatedExpr = PartialEvaluator.eval expression

        match evaluatedExpr with
        // =========================================================================
        // 1. Count() / LongCount()
        // =========================================================================
        | MethodCall(m, null, source :: rest) when m.Name = "Count" || m.Name = "LongCount" ->
            let chunkSizeOpt = tryExtractChunkSize source
            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source
            let elemType = getSequenceElementType source.Type
            let predOpt = extractOptionalLambda rest

            let adjustForChunk (rowCount: int64) : int64 =
                match chunkSizeOpt with
                | Some size when size > 0 ->
                    if rowCount <= 0L then 0L else (rowCount + int64 size - 1L) / int64 size
                | _ -> rowCount

            let collectClientCount (srcType: Type) (pred: Delegate) : int64 =
                let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
                let invoker = LinqReflectionCache.GetMaterializerCountInvoker srcType
                let countVal = Convert.ToInt64(invoker.Invoke(mat, dfHandle, pred))
                if srcType.IsArray then countVal else adjustForChunk countVal

            let effectiveCount =
                match tryResolvePredicate hasClientPreds clientProjOpt elemType nativeLf predOpt with
                | ValueSome(Pushdown filterExpr) ->
                    collectNativeRowCount mat nativeLf (Some filterExpr) |> adjustForChunk
                | ValueSome(Client(srcType, combinedPred)) ->
                    enforceClientPredicatePolicy ("Count", predOpt)
                    collectClientCount srcType combinedPred
                | ValueNone ->
                    if not hasClientPreds then
                        collectNativeRowCount mat nativeLf None |> adjustForChunk
                    else
                        let elemTypeActual = getEffectiveSourceType clientProjOpt elemType
                        collectClientCount elemTypeActual null

            Convert.ChangeType(effectiveCount, targetType) :?> 'TResult

        // =========================================================================
        // 2. Any / All
        // =========================================================================
        | MethodCall(m, null, source :: rest) when m.Name = "Any" || m.Name = "All" ->
            let isAll = m.Name = "All"
            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source
            let elemType = getSequenceElementType source.Type
            let predOpt = extractOptionalLambda rest

            let resultBool =
                match tryResolvePredicate hasClientPreds clientProjOpt elemType nativeLf predOpt with
                | ValueSome(Pushdown filterExpr) ->
                    if isAll then
                        not (hasAnyRow nativeLf (Some(PolarsWrapper.Not filterExpr)))
                    else
                        hasAnyRow nativeLf (Some filterExpr)

                | ValueSome(Client(srcType, combinedPred)) ->
                    enforceClientPredicatePolicy (m.Name, predOpt)
                    let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
                    let methodName = if isAll then "AllPredicate" else "AnyPredicate"
                    let invoker = LinqReflectionCache.GetMaterializerBoolPredicateInvoker(methodName, srcType)
                    invoker.Invoke(mat, dfHandle, combinedPred)

                | ValueNone ->
                    if isAll then
                        true
                    elif not hasClientPreds then
                        hasAnyRow nativeLf None
                    else
                        let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
                        let srcType = getEffectiveSourceType clientProjOpt elemType
                        let invoker = LinqReflectionCache.GetMaterializerAnyInvoker srcType
                        invoker.Invoke(mat, dfHandle, null)

            box resultBool :?> 'TResult

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
                | [ CleanLambda p; defExpr ] -> Some p, tryEvaluate defExpr
                | [ defExpr ] when not (typeof<LambdaExpression>.IsAssignableFrom defExpr.Type) ->
                    None, tryEvaluate defExpr
                | _ -> None, None

            if
                elemType.IsGenericType
                && elemType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>
            then
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

                match kind with
                | RowFetchKind.First ->
                    if isOrDefault then
                        Seq.tryHead seq |> Option.defaultValue defaultResult
                    else
                        Seq.head seq
                | RowFetchKind.Last ->
                    if isOrDefault then
                        Seq.tryLast seq |> Option.defaultValue defaultResult
                    else
                        Seq.last seq
                | RowFetchKind.Single ->
                    if isOrDefault && Seq.isEmpty seq then
                        defaultResult
                    else
                        Seq.exactlyOne seq
            else
                let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source

                executeRowFetch<'TResult>
                    mat
                    kind
                    isOrDefault
                    defValOpt
                    nativeLf
                    hasClientPreds
                    clientProjOpt
                    elemType
                    predLambdaOpt

        // =========================================================================
        // 4. Max, Min, Sum, Average with or without selector
        // =========================================================================
        | MethodCall(m, null, source :: rest) when
            m.Name = "Max" || m.Name = "Min" || m.Name = "Sum" || m.Name = "Average"
            ->

            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source
            let elemType = getSequenceElementType source.Type
            let selectorOpt = extractOptionalLambda rest

            let translatedColExprOpt =
                if not hasClientPreds && clientProjOpt.IsNone then
                    match selectorOpt with
                    | Some selector ->
                        let ctx = createContextForType elemType nativeLf selector.Parameters.[0].Name
                        ExprTranslator.tryTranslateWithContext ctx selector.Body
                    | None ->
                        tryGetScalarColumnName elemType nativeLf
                        |> Option.map PolarsWrapper.Col
                else
                    None

            match translatedColExprOpt with
            | Some colExpr ->
                let aggExpr = resolveScalarAggExpr m.Name colExpr
                let aggLf = PolarsWrapper.LazySelect(nativeLf, [| aggExpr |])
                let dfHandle = PolarsWrapper.LazyCollect(aggLf, PlEngine.Auto)
                let rawScalar = mat.MaterializeScalar<obj>(dfHandle)
                boxScalarResult<'TResult> (rawScalar, m.Name)

            | None ->
                let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)
                let h = PolarsWrapper.DataFrameHeight dfHandle

                if h = 0L then
                    boxScalarResult<'TResult> (null, m.Name)
                else
                    let accumType = if m.Name = "Average" then typeof<double> else targetType

                    let effectiveType, effectiveSelectorOpt =
                        match selectorOpt, clientProjOpt with
                        | Some selector, Some(sourceType, projFunc) ->
                            sourceType, Some(composeClientSelector sourceType elemType projFunc selector)
                        | Some selector, None ->
                            elemType, Some(selector.Compile())
                        | None, Some(sourceType, projFunc) ->
                            sourceType, Some(projFunc :> Delegate)
                        | None, None ->
                            elemType, None

                    let seed, folder, finisher =
                        LinqReflectionCache.CreateNumericFolder(effectiveType, accumType, m.Name, effectiveSelectorOpt)

                    let invoker =
                        LinqReflectionCache.GetMaterializerAggregateInvoker(effectiveType, accumType)

                    let rawAcc = invoker.Invoke(mat, dfHandle, seed, folder)
                    Convert.ChangeType(finisher rawAcc h, targetType) :?> 'TResult

        // =========================================================================
        // 5. ElementAt / ElementAtOrDefault
        // =========================================================================
        | MethodCall(m, null, [ source; indexExpr ]) when m.Name = "ElementAt" || m.Name = "ElementAtOrDefault" ->
            let isOrDefault = m.Name = "ElementAtOrDefault"
            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source
            let elemType = getSequenceElementType source.Type

            let isFromEnd, rawIndexVal =
                match tryEvaluate indexExpr with
                | Some(:? Index as idx) -> idx.IsFromEnd, int64 idx.Value
                | Some idxVal -> false, Convert.ToInt64 idxVal
                | None -> failwithf "Failed to evaluate index for %s." m.Name

            let isInvalidIndex =
                if isFromEnd then rawIndexVal <= 0L
                else rawIndexVal < 0L

            if isInvalidIndex then
                if isOrDefault then
                    Unchecked.defaultof<'TResult>
                else
                    raise (ArgumentOutOfRangeException("index", "Index was out of range."))
            else
                let targetSourceType = getEffectiveSourceType clientProjOpt elemType

                if not hasClientPreds then
                    let sliceOffset = if isFromEnd then -rawIndexVal else rawIndexVal
                    let slicedLf = PolarsWrapper.LazySlice(nativeLf, sliceOffset, 1u)
                    let dfHandle = PolarsWrapper.LazyCollect(slicedLf, PlEngine.Auto)

                    if PolarsWrapper.DataFrameHeight dfHandle = 0L then
                        if isOrDefault then
                            Unchecked.defaultof<'TResult>
                        else
                            raise (ArgumentOutOfRangeException("index", "Index was out of range."))
                    else
                        let invoker = LinqReflectionCache.GetMaterializerElementAtInvoker targetSourceType
                        let rawEntity = invoker.Invoke(mat, dfHandle, 0L)
                        projectClientResult<'TResult> clientProjOpt (Unchecked.defaultof<'TResult>) rawEntity
                else
                    let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

                    let targetIndex =
                        if isFromEnd then
                            PolarsWrapper.DataFrameHeight dfHandle - rawIndexVal
                        else
                            rawIndexVal

                    let invoker =
                        LinqReflectionCache.GetMaterializerElementAtByNameInvoker(m.Name, targetSourceType)

                    let rawEntity = invoker.Invoke(mat, dfHandle, targetIndex)
                    projectClientResult<'TResult> clientProjOpt (Unchecked.defaultof<'TResult>) rawEntity

        // =========================================================================
        // 6. Contains(item) / Contains(item, comparer)
        // =========================================================================
        | MethodCall(m, null, source :: itemExpr :: rest) when m.Name = "Contains" ->
            let elemType = getSequenceElementType source.Type

            let itemVal =
                match tryEvaluate itemExpr with
                | Some v -> v
                | None -> failwith "Failed to evaluate item parameter for Contains."

            let comparerObj = tryExtractComparer rest
            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source

            let filterExprOpt =
                if not hasClientPreds && clientProjOpt.IsNone && isNull comparerObj then
                    buildItemEqualityFilter itemVal nativeLf
                else
                    None

            match filterExprOpt with
            | Some combinedFilter ->
                box (hasAnyRow nativeLf (Some combinedFilter)) :?> 'TResult

            | None ->
                let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

                match clientProjOpt with
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

                    let combinedPred =
                        LinqReflectionCache.CreateComposedPredicate(sourceType, projFunc, compiledPred, elemType)

                    let invoker = LinqReflectionCache.GetMaterializerAnyInvoker sourceType
                    box (invoker.Invoke(mat, dfHandle, combinedPred)) :?> 'TResult

                | None ->
                    let invoker = LinqReflectionCache.GetMaterializerContainsInvoker elemType
                    box (invoker.Invoke(mat, dfHandle, itemVal, comparerObj)) :?> 'TResult

        // =========================================================================
        // 7. SequenceEqual(second) / SequenceEqual(second, comparer)
        // =========================================================================
        | MethodCall(m, null, source :: secondExpr :: rest) when m.Name = "SequenceEqual" ->
            let elemType = getSequenceElementType source.Type
            let compObj = tryExtractComparer rest
            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source

            let secondPlanOpt = tryResolvePlanFromExpr provider secondExpr

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
                    | _ -> provider.CreateQuery(secondExpr) :> IEnumerable

                let invoker = LinqReflectionCache.GetMaterializerSeqEqualSeqInvoker elemType
                box (invoker.Invoke(mat, df, secondSeq, compObj)) :?> 'TResult

        // =========================================================================
        // 8. MinBy / MaxBy (with or without comparer)
        // =========================================================================
        | MethodCall(m, null, source :: CleanLambda keyLambda :: rest) when m.Name = "MinBy" || m.Name = "MaxBy" ->
            let isMax = m.Name = "MaxBy"
            let nativeLf, hasClientPreds, clientProjOpt = resolveQueryPlan source
            let elemType = getSequenceElementType source.Type
            let keyType = keyLambda.ReturnType
            let compObj = tryExtractComparer rest

            let nativeSortColOpt =
                if not hasClientPreds && clientProjOpt.IsNone && isNull compObj then
                    let ctx = createContextForType elemType nativeLf keyLambda.Parameters.[0].Name
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

                if PolarsWrapper.DataFrameHeight dfHandle = 0L then
                    if PolarsTypeHelper.AcceptsNull elemType then
                        LinqReflectionCache.GetEmptyOptionalValue elemType :?> 'TResult
                    else
                        raise (InvalidOperationException "Sequence contains no elements.")
                else
                    let invoker = LinqReflectionCache.GetMaterializerFirstInvoker elemType
                    invoker.Invoke(mat, dfHandle, null) :?> 'TResult

            | None ->
                let dfHandle = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

                let targetSourceType, effectiveKeySel =
                    match clientProjOpt with
                    | Some(sourceType, projFunc) ->
                        sourceType, composeClientSelector sourceType elemType projFunc keyLambda
                    | None ->
                        elemType, keyLambda.Compile()

                let invoker =
                    LinqReflectionCache.GetMaterializerExtremumByInvoker(m.Name, targetSourceType, keyType)

                let rawWinner = invoker.Invoke(mat, dfHandle, effectiveKeySel, compObj)
                projectClientResult<'TResult> clientProjOpt (Unchecked.defaultof<'TResult>) rawWinner

        // =========================================================================
        // 9. Aggregate (Fold/Reduce)
        // =========================================================================
        | MethodCall(m, null, source :: rest) when m.Name = "Aggregate" ->
            let rawRows = provider.CreateQuery source :> IEnumerable
            let elemType = getSequenceElementType source.Type

            let evalSeed (seedExpr: Expression) =
                match tryEvaluate seedExpr with
                | Some s -> s
                | None -> failwith "Failed to evaluate seed parameter for Aggregate."

            match rest with
            | [ CleanLambda func ] ->
                let invoker = LinqReflectionCache.GetAggregateInvoker elemType
                invoker.Invoke(rawRows, func.Compile()) :?> 'TResult

            | [ seedExpr; CleanLambda func ] ->
                let invoker = LinqReflectionCache.GetAggregateWithSeedInvoker(elemType, targetType)
                invoker.Invoke(rawRows, evalSeed seedExpr, func.Compile()) :?> 'TResult

            | [ seedExpr; CleanLambda func; CleanLambda resSel ] ->
                let invoker =
                    LinqReflectionCache.GetAggregateWithResultSelectorInvoker(elemType, func.ReturnType, targetType)

                invoker.Invoke(rawRows, evalSeed seedExpr, func.Compile(), resSel.Compile()) :?> 'TResult

            | _ -> failwithf "Unsupported Aggregate overload: %A" expression

        | _ -> failwithf "Scalar execution for expression is not supported: %A" expression
