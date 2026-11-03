namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Core.Helpers
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open Polars.NET.Linq.IR
open Polars.NET.Linq.Backend.Polars

/// Strongly-typed IQueryProvider backed by Polars.NET.Core
type internal PolarsQueryProvider(initialLazyFrame: LazyFrameHandle, materializer: IDataFrameMaterializer) =
    let rootTemplate = PolarsWrapper.LazyClone initialLazyFrame

    member _.RootLazyFrame = PolarsWrapper.LazyClone rootTemplate
    member _.Materializer = materializer

    interface IQueryProvider with
        member this.CreateQuery(expression: Expression) : IQueryable =
            let elemType =
                match PolarsTypeHelper.TryGetEnumerableElementType expression.Type with
                | null -> typedefof<obj>
                | t -> t

            let queryType = typedefof<PolarsQuery<_>>.MakeGenericType elemType
            Activator.CreateInstance(
                queryType,
                Reflection.BindingFlags.Instance ||| Reflection.BindingFlags.NonPublic ||| Reflection.BindingFlags.Public,
                null,
                [| box this; box (Some expression) |],
                null
            )
            :?> IQueryable

        member this.CreateQuery<'TElement>(expression: Expression) : IQueryable<'TElement> =
            PolarsQuery<'TElement>(this, Some expression) :> IQueryable<'TElement>

        member this.Execute(expression: Expression) : obj =
            let executeMethod = LinqReflectionCache.GetQueryProviderExecuteMethod expression.Type
            executeMethod.Invoke(this, [| expression |])

        member this.Execute<'TResult>(expression: Expression) : 'TResult =
            let targetType = typeof<'TResult>

            match expression with
            | MethodCall(m, _, _) when ScalarExecutor.isScalarReductionMethod m ->
                ScalarExecutor.executeScalar<'TResult> (this :> IQueryProvider) materializer expression
            | _ when PolarsTypeHelper.IsScalarType targetType ->
                ScalarExecutor.executeScalar<'TResult> (this :> IQueryProvider) materializer expression
            | _ ->
                let query = (this :> IQueryProvider).CreateQuery expression

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
    internal (provider: PolarsQueryProvider, exprOpt: Expression option) as this
    =
    let materializer = provider.Materializer
    let targetType = typeof<'T>
    let isScalar = PolarsTypeHelper.IsScalarType targetType
    let isGrouping =
        targetType.IsGenericType
        && targetType.GetGenericTypeDefinition() = typedefof<IGrouping<_, _>>

    let expression =
        match exprOpt with
        | Some e -> e
        | None -> Expression.Constant(this, typeof<IQueryable<'T>>) :> Expression

    let getEffectiveExpr () =
        let isCSharp = not (isNull materializer) && materializer.GetType().Name.StartsWith "CSharp"
        if not isCSharp && FSharpAst.needsRewrite expression then
            FSharpAst.rewrite expression
        else
            expression

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, expression: Expression) =
        PolarsQuery<'T>(PolarsQueryProvider(lazyFrameHandle, materializer), Some expression)

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer) =
        PolarsQuery<'T>(PolarsQueryProvider(lazyFrameHandle, materializer), None)

    /// Parses the LINQ expression tree into native ops, client predicates, optional projection, fallback signal, and normalized stages
    member private _.CompilePipeline
        ()
        : LazyFrameHandle * QueryOp list * ((Func<obj, bool> * LambdaExpression) list) * ((Type * Func<obj, obj>) option) * bool * LinqStage list =
        let lfCloned = provider.RootLazyFrame
        let rewrittenExpr = getEffectiveExpr ()
        let extraction = PartialEvaluator.extract rewrittenExpr
        let cacheKey = QueryCacheKey.Create(typeof<'T>, extraction.NormalizedExpression)

        let cached =
            QueryPlanCache.GetOrAdd(
                cacheKey,
                fun _ ->
                    let normalizedStages =
                        try
                            let s = flattenStages extraction.NormalizedExpression []
                            let hasSecondarySource =
                                s |> List.exists (function
                                    | LinqStage.Join _
                                    | LinqStage.JoinAsOf _
                                    | LinqStage.GroupJoin _
                                    | LinqStage.CrossJoin _
                                    | LinqStage.Zip _
                                    | LinqStage.Zip3 _
                                    | LinqStage.SetOp _
                                    | LinqStage.Union _
                                    | LinqStage.Concat _ -> true
                                    | _ -> false)
                            if hasSecondarySource then [] else s
                        with _ ->
                            []
                    {
                        Key = cacheKey
                        NormalizedExpression = extraction.NormalizedExpression
                        ParamNames = extraction.Parameters.Keys |> Seq.toArray
                        NormalizedStages = normalizedStages
                        LastAccessTick = Environment.TickCount64
                    }
            )

        let bound =
            if extraction.Parameters.Count = 0 then
                extraction.NormalizedExpression
            else
                PartialEvaluator.substituteExtracted extraction.NormalizedExpression extraction.Parameters

        let stages =
            if not cached.NormalizedStages.IsEmpty then
                if extraction.Parameters.Count = 0 then
                    cached.NormalizedStages
                else
                    cached.NormalizedStages
                    |> List.map (QueryPlanCache.SubstituteStage extraction.Parameters)
            else
                flattenStages bound []
        FallbackTracker.Diagnostic <- None
        UdfTracker.Reset()

        let finalLf, ops, clientPreds, clientProjOpt, requiresClientFallback =
            LinqPipelineCompiler.compileStages lfCloned stages targetType isScalar isGrouping bound

        finalLf, ops, clientPreds, clientProjOpt, requiresClientFallback, stages

    /// Compiles pipeline and builds the target native LazyFrameHandle alongside client predicates, projection, fallback signal, and normalized stages
    member private this.GetCompiledPlan
        ()
        : LazyFrameHandle * (Func<obj, bool> list) * ((Type * Func<obj, obj>) option) * bool * LinqStage list =
        let baseLf, ops, clientPredsWithAst, clientProjOpt, requiresClientFallback, stages =
            this.CompilePipeline()

        enforcePipelinePolicy
            "QueryExecution"
            requiresClientFallback
            clientPredsWithAst
            (expression.ToString())

        let nativeLf = applyNativeOps baseLf ops
        let clientPredicates = clientPredsWithAst |> List.map fst
        nativeLf, clientPredicates, clientProjOpt, requiresClientFallback, stages

    /// Compiles pipeline and builds the target LogicalPlan IR tree.
    member this.GetLogicalPlan() : LogicalPlan =
        let baseLf, ops, _, _, _, stages = this.CompilePipeline()
        let initialPlan = PolarsPhysicalLowerer.scanLazyFrame baseLf
        let logicalPlan =
            if not stages.IsEmpty then
                TreeBuilder.fromStages initialPlan stages
            else
                PolarsPhysicalLowerer.fromOpsOnPlan initialPlan ops
        LogicalOptimizer.optimize logicalPlan

    /// Explains the intermediate representation (IR) LogicalPlan tree.
    member this.ExplainLogicalPlan() : string =
        LogicalPlanOps.formatPlan (this.GetLogicalPlan())

    member private _.MaterializeFlatRows
        (dfHandle: DataFrameHandle, clientPredicates: Func<obj, bool> list, clientProjOpt: (Type * Func<obj, obj>) option)
        : IEnumerable<'T> =
        match clientProjOpt with
        | Some(sourceType, func) ->
            let invoker = LinqReflectionCache.GetMaterializerInvoker sourceType
            let rawSourceRows = invoker.Invoke(materializer, dfHandle)

            seq {
                for item in rawSourceRows do
                    if clientPredicates.IsEmpty || clientPredicates |> List.forall (fun p -> p.Invoke item) then
                        yield func.Invoke item :?> 'T
            }
        | None ->
            let rows = materializer.Materialize<'T> dfHandle

            if clientPredicates.IsEmpty then
                rows
            else
                rows
                |> Seq.filter (fun item -> clientPredicates |> List.forall (fun p -> p.Invoke(box item)))

    member private _.CanCollectNativeDirectly
        (
            nativeLf: LazyFrameHandle,
            clientPredicates: Func<obj, bool> list,
            clientProjOpt: (Type * Func<obj, obj>) option,
            requiresClientFallback: bool
        ) : bool =
        let schema = PolarsWrapper.GetLazySchema nativeLf
        let width = PolarsWrapper.GetSchemaLen schema

        not requiresClientFallback
        && clientPredicates.IsEmpty
        && clientProjOpt.IsNone
        && not (isScalar && width > 1UL)
        && not isGrouping

    /// Executes the full LINQ expression pipeline in-memory via .NET EnumerableQuery LINQ-to-Objects engine
    member private _.ExecuteInMemory() : IEnumerable<'T> =
        let effectiveExpr = getEffectiveExpr ()

        match tryFindRootQueryable effectiveExpr with
        | Some(rootExpr, rootType) ->
            let rootConst = rootExpr :?> ConstantExpression
            let rootQueryable = rootConst.Value :?> IQueryable

            let rootElemType =
                match PolarsTypeHelper.TryGetEnumerableElementType rootType with
                | null -> rootQueryable.ElementType
                | t -> t

            let toArrayMethod =
                LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| rootElemType |])

            let castMethod =
                LinqReflectionCache.GetEnumerableMethod("Cast", 1, [| rootElemType |])

            let casted = castMethod.Invoke(null, [| box rootQueryable |])
            let inMemoryArray = toArrayMethod.Invoke(null, [| casted |])

            let asQueryableMethod =
                LinqReflectionCache.GetQueryableMethod("AsQueryable", 1, [| rootElemType |])

            let inMemoryQueryable =
                asQueryableMethod.Invoke(null, [| inMemoryArray |]) :?> IQueryable

            let targetQueryType = typedefof<IQueryable<_>>.MakeGenericType(rootElemType)
            let replacement = Expression.Constant(inMemoryQueryable, targetQueryType)

            let replacer =
                { new ExpressionVisitor() with
                    override _.VisitConstant(node: ConstantExpression) =
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
            let resultQueryable = inMemoryQueryable.Provider.CreateQuery<'T>(inMemoryExpr)
            resultQueryable :> IEnumerable<'T>

        | None -> failwithf "Failed to locate root queryable for in-memory execution: %A" expression

    /// Evaluates the complete pipeline, executing native plan first, then materializing
    member private this.ExecuteQuery() : IEnumerable<'T> =
        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback, stages =
            this.GetCompiledPlan()

        if requiresClientFallback then
            this.ExecuteInMemory()
        else
            let collectedDf = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

            let dfHandle =
                if stages |> List.exists (function LinqStage.Shuffle -> true | _ -> false) then
                    applyShuffle collectedDf
                else
                    collectedDf

            if clientProjOpt.IsSome then
                this.MaterializeFlatRows(dfHandle, clientPredicates, clientProjOpt)
            else
                let chunkSizeOpt =
                    if targetType.IsArray then
                        stages |> List.tryPick (function LinqStage.Chunk s -> Some s | _ -> None)
                    else
                        None

                let aggByInfoOpt =
                    stages |> List.tryPick (function LinqStage.AggregateBy(k, seed, sf, f) -> Some(k, seed, sf, f) | _ -> None)

                // Case 1: GroupBy & GroupJoin materialization (produces IEnumerable<IGrouping<TKey, TElement>>)
                if isGrouping then
                    let genericArgs = targetType.GetGenericArguments()
                    let keyType, elemType = genericArgs.[0], genericArgs.[1]

                    let applyGroupSort (seqToSort: IEnumerable<'T>) : IEnumerable<'T> =
                        match stages |> List.tryPick (function LinqStage.Sort(sortLambda, isDesc, _) -> Some(sortLambda, isDesc) | _ -> None) with
                        | Some(sortLambda, isDesc) ->
                            let param = Expression.Parameter(typeof<'T>, "g")
                            let typedCall = Expression.Invoke(sortLambda, param)
                            let boxed = Expression.Convert(typedCall, typeof<IComparable>)
                            let typedLambda = Expression.Lambda<Func<'T, IComparable>>(boxed, param)
                            let keySel = typedLambda.Compile()
                            if isDesc then Seq.sortByDescending keySel.Invoke seqToSort else Seq.sortBy keySel.Invoke seqToSort
                        | None -> seqToSort

                    let groupByInfoOpt =
                        stages |> List.tryPick (function
                            | LinqStage.GroupByKey k -> Some(k, None)
                            | LinqStage.GroupByWithResult(k, r) when r.Parameters.Count < 2 -> Some(k, Some r)
                            | LinqStage.GroupByWithElementAndResult(k, e, _) -> Some(k, Some e)
                            | _ -> None)

                    let hasGroupJoin = stages |> List.exists (function LinqStage.GroupJoin _ -> true | _ -> false)

                    match groupByInfoOpt with
                    | Some(keyLambda, elemLambdaOpt) ->
                        let compiledKeySelector = keyLambda.Compile()

                        let grouped =
                            match elemLambdaOpt with
                            | Some elemLambda ->
                                let sourceType = elemLambda.Parameters.[0].Type
                                let invoker = LinqReflectionCache.GetMaterializerInvoker sourceType
                                let rawRows = invoker.Invoke(materializer, dfHandle)
                                let compiledElemSelector = elemLambda.Compile()

                                let groupByMethod =
                                    LinqReflectionCache.GetEnumerableMethod("GroupBy", 3, [| sourceType; keyType; elemType |])

                                groupByMethod.Invoke(
                                    null,
                                    [| box rawRows; box compiledKeySelector; box compiledElemSelector |]
                                )
                                :?> IEnumerable<'T>
                            | None ->
                                let invoker = LinqReflectionCache.GetMaterializerInvoker elemType
                                let rawRows = invoker.Invoke(materializer, dfHandle)

                                let groupByMethod =
                                    LinqReflectionCache.GetEnumerableMethod("GroupBy", 2, [| elemType; keyType |])

                                groupByMethod.Invoke(null, [| box rawRows; box compiledKeySelector |])
                                :?> IEnumerable<'T>

                        let filtered =
                            if clientPredicates.IsEmpty then
                                grouped
                            else
                                grouped
                                |> Seq.filter (fun g -> clientPredicates |> List.forall (fun p -> p.Invoke(box g)))

                        applyGroupSort filtered

                    | None when hasGroupJoin ->
                        let innerNullableType =
                            if elemType.IsValueType then
                                typedefof<Nullable<_>>.MakeGenericType elemType
                            else
                                elemType

                        let pairType =
                            typedefof<ValueTuple<_, _>>.MakeGenericType(keyType, innerNullableType)

                        let invoker = LinqReflectionCache.GetMaterializerInvoker pairType
                        let rawPairs = invoker.Invoke(materializer, dfHandle)

                        let keyOrder = List<obj>()
                        let groupMap = Dictionary<obj, List<obj>>()

                        for pairObj in rawPairs do
                            let tuple = pairObj :?> ITuple
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

                        let groupingType =
                            typedefof<MaterializedGrouping<_, _>>.MakeGenericType(keyType, elemType)

                        let ctor = groupingType.GetConstructors().[0]

                        let toArrayMethod =
                            LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| elemType |])

                        let castMethod =
                            LinqReflectionCache.GetEnumerableMethod("Cast", 1, [| elemType |])

                        let resultList = List<'T>()

                        for k in keyOrder do
                            let rawList = groupMap.[k]
                            let castedSeq = castMethod.Invoke(null, [| box rawList |])
                            let typedArray = toArrayMethod.Invoke(null, [| castedSeq |])
                            let grpInstance = ctor.Invoke [| k; typedArray |] :?> 'T
                            resultList.Add grpInstance

                        applyGroupSort resultList

                    | None -> failwith "Could not locate keySelector for GroupBy pipeline."

                // Case 2: Chunk slicing via stack-based cursor
                elif chunkSizeOpt.IsSome then
                    let chunkSize = chunkSizeOpt.Value
                    let elemType = targetType.GetElementType()

                    let invoker = LinqReflectionCache.GetMaterializerChunkInvoker elemType
                    let chunkedSeq = invoker.Invoke(materializer, dfHandle, chunkSize)
                    chunkedSeq.Cast<'T>()

                // Case 3: AggregateBy (produces IEnumerable<KeyValuePair<TKey, TAccum>>)
                elif aggByInfoOpt.IsSome then
                    let keyLambda, seedExpr, seedLambdaOpt, funcLambda = aggByInfoOpt.Value

                    let genericArgs = targetType.GetGenericArguments()
                    let keyType, accumType = genericArgs.[0], genericArgs.[1]
                    let sourceElemType = keyLambda.Parameters.[0].Type

                    let compiledKey = keyLambda.Compile()
                    let compiledFunc = funcLambda.Compile()

                    let res =
                        match seedLambdaOpt with
                        | Some seedLambda ->
                            let invoker =
                                LinqReflectionCache.GetMaterializerAggByFactoryInvoker(sourceElemType, keyType, accumType)

                            invoker.Invoke(materializer, dfHandle, compiledKey, seedLambda.Compile(), compiledFunc, null)
                        | None ->
                            let seedVal =
                                match tryEvaluate seedExpr with
                                | Some s -> s
                                | None -> failwith "Failed to evaluate static seed for AggregateBy."

                            let invoker =
                                LinqReflectionCache.GetMaterializerAggBySeedInvoker(sourceElemType, keyType, accumType)

                            invoker.Invoke(materializer, dfHandle, compiledKey, seedVal, compiledFunc, null)

                    res.Cast<'T>()

                // Case 4: Flat row materialization (standard records, anonymous types, DTOs)
                else
                    let height = PolarsWrapper.DataFrameHeight dfHandle
                    let defaultIfEmptyOpt = stages |> List.tryPick (function LinqStage.DefaultIfEmpty d -> Some d | _ -> None)

                    match defaultIfEmptyOpt with
                    | Some customDefaultOpt when height = 0L ->
                        let defaultItem =
                            match customDefaultOpt with
                            | Some expr ->
                                match tryEvaluate expr with
                                | Some v when not (isNull v) -> v :?> 'T
                                | _ -> Unchecked.defaultof<'T>
                            | None -> Unchecked.defaultof<'T>

                        Seq.singleton defaultItem
                    | _ ->
                        this.MaterializeFlatRows(dfHandle, clientPredicates, None)

    /// Compiles pipeline to LazyFrameHandle. If client fallback, predicates or projections exist, materializes and transposes back.
    member this.CompileToLazyFrameHandle() : LazyFrameHandle =
        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback, _ =
            this.GetCompiledPlan()

        if this.CanCollectNativeDirectly(nativeLf, clientPredicates, clientProjOpt, requiresClientFallback) then
            nativeLf
        else
            let filteredRows = this.ExecuteQuery()
            let newDfHandle = DataFrameBuilder.FromRows<'T> filteredRows
            PolarsWrapper.DataFrameToLazy newDfHandle

    /// Compiles the query and materializes directly into a native DataFrameHandle
    member this.CompileToDataFrameHandle() : DataFrameHandle =
        let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback, stages =
            this.GetCompiledPlan()

        if stages |> List.exists (function LinqStage.Chunk _ -> true | _ -> false) then
            raise (
                InvalidOperationException(
                    "Cannot materialize a chunked sequence (IEnumerable<T[]>) into a single flat DataFrame. "
                    + "If you want to iterate over DataFrames in batches, use 'ToBatchesAsync(chunkSize)' or 'CollectBatchesStream'. "
                    + "If you want in-memory array chunks, enumerate the query directly via ToList() / foreach."
                )
            )

        if this.CanCollectNativeDirectly(nativeLf, clientPredicates, clientProjOpt, requiresClientFallback) then
            let collectedDf = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

            if stages |> List.exists (function LinqStage.Shuffle -> true | _ -> false) then
                applyShuffle collectedDf
            else
                collectedDf
        else
            let rows = this.ExecuteQuery()
            DataFrameBuilder.FromRows<'T>(rows)

    interface IInternalPlanSource with
        member this.GetPlanInfo() =
            let nativeLf, clientPreds, clientProjOpt, requiresClientFallback, _ =
                this.GetCompiledPlan()

            nativeLf, requiresClientFallback || not clientPreds.IsEmpty, clientProjOpt

    interface IEnumerable<'T> with
        member this.GetEnumerator() : IEnumerator<'T> = (this.ExecuteQuery()).GetEnumerator()

    interface IEnumerable with
        member this.GetEnumerator() : IEnumerator =
            (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator

    interface IQueryable with
        member _.ElementType = targetType
        member _.Expression = expression
        member _.Provider = provider :> IQueryProvider

    interface IQueryable<'T>

    interface IOrderedQueryable<'T>

    interface IPolarsPlanSource with
        member this.GetCompiledLazyFrameHandle() = this.CompileToLazyFrameHandle()

    member this.GetPlanInspection(optimized: bool) : PlanInspection =
        let baseLf, ops, clientPreds, clientProjOpt, requiresClientFallback, stages =
            this.CompilePipeline()

        let nativeLf = applyNativeOps baseLf ops
        let planStr = PolarsWrapper.Explain(nativeLf, optimized)

        let initialPlan = PolarsPhysicalLowerer.scanLazyFrame baseLf
        let logicalPlan =
            if not stages.IsEmpty then
                TreeBuilder.fromStages initialPlan stages
            else
                PolarsPhysicalLowerer.fromOpsOnPlan initialPlan ops
        let optimizedLogicalPlan = LogicalOptimizer.optimize logicalPlan
        let chunkSizeOpt = stages |> List.tryPick (function LinqStage.Chunk s -> Some s | _ -> None)
        let shouldShuffle = stages |> List.exists (function LinqStage.Shuffle -> true | _ -> false)
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

        let detectedUdfs = UdfTracker.DetectedUdfs

        {
            NativePlan = planWithPostOps
            LogicalPlan = Some(LogicalPlanOps.formatPlan optimizedLogicalPlan)
            HasClientPredicates = not clientPreds.IsEmpty
            ClientPredicateExpressions = clientPreds |> List.map snd
            HasClientProjection = clientProjOpt.IsSome || chunkSizeOpt.IsSome || shouldShuffle || isGrouping
            IsFullFallback = requiresClientFallback
            FallbackReason = FallbackTracker.Diagnostic
            HasUdf = not detectedUdfs.IsEmpty
            UdfExpressions = detectedUdfs
        }

    interface IPolarsExplainable with
        member this.InspectPlan(optimized: bool) = this.GetPlanInspection optimized

        member this.Explain(optimized: bool) =
            (this.GetPlanInspection optimized).FormatReport()

        member this.ExplainLogicalPlan() = this.ExplainLogicalPlan()

        member this.GetLogicalPlan() = this.GetLogicalPlan()

    interface IAsyncEnumerable<'T> with
        member this.GetAsyncEnumerator(cancellationToken: CancellationToken) : IAsyncEnumerator<'T> =
            cancellationToken.ThrowIfCancellationRequested()

            let nativeLf, clientPredicates, clientProjOpt, requiresClientFallback, stages =
                this.GetCompiledPlan()

            let canStreamBatches =
                not requiresClientFallback
                && not isGrouping
                && not (stages |> List.exists (function LinqStage.Shuffle | LinqStage.DefaultIfEmpty _ | LinqStage.Chunk _ | LinqStage.AggregateBy _ -> true | _ -> false))

            if not canStreamBatches then
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
                    this.MaterializeFlatRows(dfHandle, clientPredicates, clientProjOpt)

                { new IAsyncEnumerator<'T> with
                    member _.Current =
                        if hasActiveRows && not (isNull currentBatchRows) then
                            currentBatchRows.Current
                        else
                            invalidOp "Enumeration has not started or is completed."

                    member _.MoveNextAsync() : ValueTask<bool> =
                        cancellationToken.ThrowIfCancellationRequested()

                        if hasActiveRows && currentBatchRows.MoveNext() then
                            ValueTask<bool>(true)
                        else
                            ValueTask<bool>(
                                task {
                                    cancellationToken.ThrowIfCancellationRequested()
                                    let mutable advanced = false
                                    let mutable doneStreaming = false

                                    while not (advanced || doneStreaming) do
                                        if hasActiveRows && currentBatchRows.MoveNext() then
                                            advanced <- true
                                        else
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
