namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Core.Wrappers
open Polars.NET.Core.Enums
open Polars.NET.Core.Helpers
open System.Runtime.CompilerServices
open System.Threading
open System.Threading.Tasks
open Polars.NET.Linq.IR
open Polars.NET.Linq.Backend.Polars

[<AutoOpen>]
module internal PolarsQueryHelpers =
    /// Applies eager row shuffling (sample_frac = 1.0, without replacement) to a DataFrame.
    let applyShuffle (df: DataFrameHandle) : DataFrameHandle =
        let fracHandle = PolarsWrapper.SeriesNew("", [| 1.0 |])
        PolarsWrapper.SampleFrac(df, fracHandle, false, Nullable true, Nullable())

/// The language whose compiler built a query's expression trees. F# query expressions are rewritten
/// (quotations, curried closures, AnonymousObject tuples, nested SelectMany chains) before translation;
/// C# expression trees are translated as they are.
type QueryLanguage =
    | CSharp = 0
    | FSharp = 1

/// Strongly-typed IQueryProvider backed by Polars.NET.Core
type internal PolarsQueryProvider(initialLazyFrame: LazyFrameHandle, materializer: IDataFrameMaterializer, language: QueryLanguage) =
    // The root frame's schema is read once per provider. Lowering clones the handle, so the binding never hands it out.
    let rootSource = lazy (PolarsPhysicalLowerer.tableSourceOfLazyFrame initialLazyFrame)

    /// Root table source, bound to slot 0 of every query this provider compiles.
    member _.RootSource : ILogicalTableSource = rootSource.Value
    member _.Materializer = materializer
    /// Shared by every query derived from this provider's root.
    member _.Language = language

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
            | MethodCall(m, _, _) when ScalarPlanner.isScalarReductionMethod m ->
                ScalarPlanner.plan<'TResult> PolarsQueryBackend.Instance (this :> IQueryProvider) expression
                |> PolarsScalarRunner.execute materializer
            | _ when PolarsTypeHelper.IsScalarType targetType ->
                ScalarPlanner.plan<'TResult> PolarsQueryBackend.Instance (this :> IQueryProvider) expression
                |> PolarsScalarRunner.execute materializer
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
    let isGrouping = isGroupingType targetType

    /// The type the rows of the native plan are materialized as: the grouped element when the query yields
    /// groups, the source of AggregateBy, the element of Chunk's arrays, otherwise the query's element type.
    let materializedRowType (stages: LinqStage list) : Type =
        stages
        |> List.tryPick (function
            | LinqStage.GroupByKey k
            | LinqStage.GroupByWithResult(k, _)
            | LinqStage.GroupByWithElementAndResult(k, _, _) when isGrouping -> Some k.Parameters.[0].Type
            | LinqStage.AggregateBy(k, _, _, _) -> Some k.Parameters.[0].Type
            | LinqStage.Chunk _ when targetType.IsArray -> Some(targetType.GetElementType())
            | _ -> None)
        |> Option.defaultValue targetType

    /// Packs the entities of the join scopes still in effect over the plan's rows into struct columns, so the
    /// rows materialize as their element type; escalates to LINQ-to-Objects when an entity has no columns.
    let forMaterialization (stages: LinqStage list) (compiled: CompiledLogicalQuery) : CompiledLogicalQuery =
        if compiled.RowScopes.IsEmpty then
            compiled
        else
            match EntityPacking.packRow compiled.RowScopes compiled.Plan (materializedRowType stages) with
            | RowPacking.NotNeeded -> compiled
            | RowPacking.Packed plan -> { compiled with Plan = plan; RowScopes = Map.empty }
            | RowPacking.Unresolvable -> { compiled with Residual = ClientResidual.LinqToObjects }

    let expression =
        match exprOpt with
        | Some e -> e
        | None -> Expression.Constant(this, typeof<IQueryable<'T>>) :> Expression

    let getEffectiveExpr () =
        if provider.Language = QueryLanguage.FSharp && FSharpAst.needsRewrite expression then
            FSharpAst.rewrite expression
        else
            expression

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, language: QueryLanguage, expression: Expression) =
        PolarsQuery<'T>(PolarsQueryProvider(lazyFrameHandle, materializer, language), Some expression)

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, language: QueryLanguage) =
        PolarsQuery<'T>(PolarsQueryProvider(lazyFrameHandle, materializer, language), None)

    /// Compiles the pipeline to optimized IR, reusing the cached compilation of an identical query
    /// (same expression shape, parameter values, root columns and configuration) when one exists.
    /// The pipeline carries its own fallback and UDF diagnostics. With `mergeDiagnostics` (this query is
    /// grafted into an outer one being compiled), the outer compilation also takes them.
    member private _.CompilePipeline(mergeDiagnostics: bool) : CompiledPipeline =
        let extraction = PartialEvaluator.extract (getEffectiveExpr ())
        let root = provider.RootSource
        let rootBinding = SourceBinding.Table root

        let keyOpt =
            QueryPlanCache.TryCreateKey(typeof<'T>, extraction.NormalizedExpression, extraction.Parameters, root.Schema.ColumnNames)

        let pipeline =
            match keyOpt |> Option.bind QueryPlanCache.TryGet with
            | Some cached ->
                { cached.Pipeline with Compiled = { cached.Pipeline.Compiled with Sources = [| rootBinding |] } }
            | None ->
                let bound =
                    if extraction.Parameters.Count = 0 then
                        extraction.NormalizedExpression
                    else
                        PartialEvaluator.substituteExtracted extraction.NormalizedExpression extraction.Parameters

                let stages = flattenStages bound []
                use scope = CompilationScope.Enter PolarsQueryBackend.Instance
                let compiled = TreeBuilder.buildCompiledQuery rootBinding root.Schema isGrouping stages

                let pipeline =
                    {
                        Compiled = { compiled with Plan = LogicalOptimizer.optimize compiled.Plan }
                        Stages = stages
                        FallbackDiagnostic = scope.FallbackReason
                        Udfs = scope.Udfs
                    }

                // Only root-only pipelines are cached: any other source is bound from this query's values.
                match keyOpt with
                | Some key when compiled.Sources.Length = 1 ->
                    QueryPlanCache.Admit
                        {
                            Key = key
                            Pipeline = { pipeline with Compiled = { pipeline.Compiled with Sources = [||] } }
                            LastAccessTick = 0L
                        }
                | _ -> ()

                pipeline

        if mergeDiagnostics then
            CompilationScope.TryCurrent
            |> Option.iter (fun outer -> outer.Absorb(pipeline.FallbackDiagnostic, pipeline.Udfs))

        pipeline

    /// Compiles pipeline into a pure symbolic CompiledLogicalQuery IR.
    member internal this.CompileLogicalQuery() : CompiledLogicalQuery =
        (this.CompilePipeline false).Compiled

    /// For a pipeline that needs full LINQ-to-Objects fallback, finds the longest prefix of its
    /// method-call chain that compiles without it. Returns the call at which LINQ-to-Objects takes over
    /// and the prefix expression that call consumes, or None when no non-trivial prefix runs natively.
    /// `effectiveExpr` must be the tree the caller later rewrites, since the prefix is identified by reference.
    member internal _.TryFindNativePrefix(effectiveExpr: Expression) : (MethodCallExpression * Expression) option =
        let isQueryable (t: Type) = typeof<IQueryable>.IsAssignableFrom t

        // Method calls along the source spine, outermost first.
        let rec spine (e: Expression) =
            match e with
            | :? MethodCallExpression as call when
                isNull call.Object && call.Arguments.Count > 0 && isQueryable call.Arguments.[0].Type
                ->
                call :: spine call.Arguments.[0]
            | _ -> []

        let compilesNatively (prefix: Expression) =
            try
                match (provider :> IQueryProvider).CreateQuery prefix with
                | :? IPolarsInternalExplainable as q -> q.IsNativelyExecutable()
                | _ -> false
            with _ ->
                false

        effectiveExpr
        |> spine
        |> List.tryPick (fun consumer ->
            let prefix = consumer.Arguments.[0]
            // ThenBy needs the ordered sequence its OrderBy produced, so the two are never split.
            let splitsOrdering = consumer.Method.Name = "ThenBy" || consumer.Method.Name = "ThenByDescending"
            match prefix with
            | :? MethodCallExpression when not splitsOrdering && compilesNatively prefix -> Some(consumer, prefix)
            | _ -> None)

    /// Attempts to extract a pure symbolic LogicalPlan (with its source bindings) for grafting into another
    /// query, merging its diagnostics into the outer ones and never lowering to native handles.
    member internal this.TryGetPureLogicalPlan() : (LogicalPlan * SourceBinding array) option =
        let pipeline = this.CompilePipeline true
        let compiled = pipeline.Compiled
        let stages = pipeline.Stages
        let optimizedPlan = compiled.Plan
        let schema = SchemaInference.inferPlanSchema optimizedPlan
        let canUsePurePlan =
            ClientResidualOps.isNative compiled.Residual
            && not (isScalar && schema.Fields.Length > 1)
            && not isGrouping
            && not (stages |> List.exists LinqStageTraits.isPostCollect)
        if canUsePurePlan then
            Some(optimizedPlan, compiled.Sources)
        else
            None

    /// Compiles the pipeline into the native LazyFrameHandle, the managed residual and the normalized stages,
    /// enforcing the client evaluation policy.
    member private this.GetCompiledPlan() : LazyFrameHandle * ClientResidual * LinqStage list =
        let pipeline = this.CompilePipeline false
        let compiled = forMaterialization pipeline.Stages pipeline.Compiled
        let nativeLf = PolarsPhysicalLowerer.lowerPlan compiled.Sources compiled.Plan

        enforcePipelinePolicy
            "QueryExecution"
            (compiled.Residual = ClientResidual.LinqToObjects)
            (ClientResidualOps.filters compiled.Residual)
            (expression.ToString())

        nativeLf, compiled.Residual, pipeline.Stages

    /// Compiles pipeline and builds the target LogicalPlan IR tree.
    member this.GetLogicalPlan() : LogicalPlan =
        (this.CompileLogicalQuery()).Plan

    /// Explains the intermediate representation (IR) LogicalPlan tree.
    member this.ExplainLogicalPlan() : string =
        LogicalPlanOps.formatPlan (this.GetLogicalPlan())

    /// Materializes flat native rows and applies the residual row operations in pipeline order.
    member private _.MaterializeFlatRows(dfHandle: DataFrameHandle, residual: ClientResidual) : IEnumerable<'T> =
        match residual with
        | ClientResidual.RowOps ops ->
            let invoker = MaterializerInvokers.GetMaterializerInvoker(ClientResidualOps.rowType ops)
            invoker.Invoke(materializer, dfHandle) |> ClientResidualOps.apply ops |> Seq.cast<'T>
        | _ -> materializer.Materialize<'T> dfHandle

    member private _.CanCollectNativeDirectly(nativeLf: LazyFrameHandle, residual: ClientResidual) : bool =
        let schema = PolarsWrapper.GetLazySchema nativeLf
        let width = PolarsWrapper.GetSchemaLen schema

        ClientResidualOps.isNative residual
        && not (isScalar && width > 1UL)
        && not isGrouping

    /// Executes a pipeline that needs LINQ-to-Objects: the longest natively compilable prefix runs in Polars,
    /// and the rest of the pipeline runs via EnumerableQuery over the materialized prefix rows.
    /// Falls back to replaying the whole pipeline over the in-memory root when no prefix runs natively.
    member private this.ExecuteInMemory() : IEnumerable<'T> =
        let effectiveExpr = getEffectiveExpr ()

        let split =
            this.TryFindNativePrefix effectiveExpr
            |> Option.bind (fun (_, prefix) ->
                match PolarsTypeHelper.TryGetEnumerableElementType prefix.Type with
                | null -> None
                | rowType -> Some(prefix, rowType))

        match split with
        | Some(prefix, rowType) ->
            let prefixQuery = (provider :> IQueryProvider).CreateQuery prefix
            let toArrayMethod = LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [| rowType |])
            let rows = toArrayMethod.Invoke(null, [| box prefixQuery |])

            let asQueryableMethod = LinqReflectionCache.GetQueryableMethod("AsQueryable", 1, [| rowType |])
            let localRows = asQueryableMethod.Invoke(null, [| rows |]) :?> IQueryable

            if prefix.Type.IsAssignableFrom(localRows.GetType()) then
                let replacement = Expression.Constant(localRows, prefix.Type)

                let replacer =
                    { new ExpressionVisitor() with
                        override _.Visit(node: Expression) =
                            if obj.ReferenceEquals(node, prefix) then replacement :> Expression
                            else base.Visit(node)
                    }

                let suffixExpr = replacer.Visit effectiveExpr
                localRows.Provider.CreateQuery<'T>(suffixExpr) :> IEnumerable<'T>
            else
                this.ExecuteRootInMemory()
        | None -> this.ExecuteRootInMemory()

    /// Executes the full LINQ expression pipeline in-memory via .NET EnumerableQuery LINQ-to-Objects engine
    member private _.ExecuteRootInMemory() : IEnumerable<'T> =
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
        let nativeLf, residual, stages = this.GetCompiledPlan()

        if residual = ClientResidual.LinqToObjects then
            this.ExecuteInMemory()
        else
            let collectedDf = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

            let dfHandle =
                if stages |> List.exists (function LinqStage.Shuffle -> true | _ -> false) then
                    applyShuffle collectedDf
                else
                    collectedDf

            let isRowOps = match residual with ClientResidual.RowOps _ -> true | _ -> false

            if isRowOps && not isGrouping then
                this.MaterializeFlatRows(dfHandle, residual)
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
                                let invoker = MaterializerInvokers.GetMaterializerInvoker sourceType
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
                                let invoker = MaterializerInvokers.GetMaterializerInvoker elemType
                                let rawRows = invoker.Invoke(materializer, dfHandle)

                                let groupByMethod =
                                    LinqReflectionCache.GetEnumerableMethod("GroupBy", 2, [| elemType; keyType |])

                                groupByMethod.Invoke(null, [| box rawRows; box compiledKeySelector |])
                                :?> IEnumerable<'T>

                        // Client HAVING predicates are residual filters over the groups.
                        let filtered =
                            match residual with
                            | ClientResidual.RowOps ops -> grouped |> ClientResidualOps.apply ops |> Seq.cast<'T>
                            | _ -> grouped

                        applyGroupSort filtered

                    | None when hasGroupJoin ->
                        let innerNullableType =
                            if elemType.IsValueType then
                                typedefof<Nullable<_>>.MakeGenericType elemType
                            else
                                elemType

                        let pairType =
                            typedefof<ValueTuple<_, _>>.MakeGenericType(keyType, innerNullableType)

                        let invoker = MaterializerInvokers.GetMaterializerInvoker pairType
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

                    let invoker = MaterializerInvokers.GetMaterializerChunkInvoker elemType
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
                                MaterializerInvokers.GetMaterializerAggByFactoryInvoker(sourceElemType, keyType, accumType)

                            invoker.Invoke(materializer, dfHandle, compiledKey, seedLambda.Compile(), compiledFunc, null)
                        | None ->
                            let seedVal =
                                match PartialEvaluator.tryEvaluate seedExpr with
                                | Some s -> s
                                | None -> failwith "Failed to evaluate static seed for AggregateBy."

                            let invoker =
                                MaterializerInvokers.GetMaterializerAggBySeedInvoker(sourceElemType, keyType, accumType)

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
                                match PartialEvaluator.tryEvaluate expr with
                                | Some v when not (isNull v) -> v :?> 'T
                                | _ -> Unchecked.defaultof<'T>
                            | None -> Unchecked.defaultof<'T>

                        Seq.singleton defaultItem
                    | _ ->
                        this.MaterializeFlatRows(dfHandle, residual)

    /// Compiles pipeline to LazyFrameHandle. If client fallback, predicates or projections exist, materializes and transposes back.
    member this.CompileToLazyFrameHandle() : LazyFrameHandle =
        let nativeLf, residual, _ = this.GetCompiledPlan()

        if this.CanCollectNativeDirectly(nativeLf, residual) then
            nativeLf
        else
            let filteredRows = this.ExecuteQuery()
            let newDfHandle = DataFrameBuilder.FromRows<'T> filteredRows
            PolarsWrapper.DataFrameToLazy newDfHandle

    /// Compiles the query and materializes directly into a native DataFrameHandle
    member this.CompileToDataFrameHandle() : DataFrameHandle =
        let nativeLf, residual, stages = this.GetCompiledPlan()

        if stages |> List.exists (function LinqStage.Chunk _ -> true | _ -> false) then
            raise (
                InvalidOperationException(
                    "Cannot materialize a chunked sequence (IEnumerable<T[]>) into a single flat DataFrame. "
                    + "If you want to iterate over DataFrames in batches, use 'ToBatchesAsync(chunkSize)' or 'CollectBatchesStream'. "
                    + "If you want in-memory array chunks, enumerate the query directly via ToList() / foreach."
                )
            )

        if this.CanCollectNativeDirectly(nativeLf, residual) then
            let collectedDf = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

            if stages |> List.exists (function LinqStage.Shuffle -> true | _ -> false) then
                applyShuffle collectedDf
            else
                collectedDf
        else
            let rows = this.ExecuteQuery()
            DataFrameBuilder.FromRows<'T>(rows)

    interface IInternalPlanSource with
        member this.GetLogicalPlanInfo() =
            // No policy check here: sources needing client evaluation are enumerated by the scalar planner,
            // and enumeration (GetCompiledPlan) enforces the pipeline policy; for pure native sources it is a no-op.
            let compiled = this.CompileLogicalQuery()

            match ClientResidualOps.tryProjectionOnly compiled.Residual with
            | Some projection -> compiled, false, Some projection
            | None -> compiled, not (ClientResidualOps.isNative compiled.Residual), None

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
        let pipeline = this.CompilePipeline false
        let compiled = forMaterialization pipeline.Stages pipeline.Compiled
        let nativeLf = PolarsPhysicalLowerer.lowerPlan compiled.Sources compiled.Plan
        let planStr = PolarsWrapper.Explain(nativeLf, optimized)
        let stages = pipeline.Stages

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

        let detectedUdfs = pipeline.Udfs
        let fallbackReason = pipeline.FallbackDiagnostic
        let isFullFallback = compiled.Residual = ClientResidual.LinqToObjects

        let fallbackBoundary =
            if isFullFallback then
                this.TryFindNativePrefix(getEffectiveExpr ()) |> Option.map (fun (consumer, _) -> consumer.Method.Name)
            else
                None

        {
            NativePlan = planWithPostOps
            LogicalPlan = Some(LogicalPlanOps.formatPlan compiled.Plan)
            HasClientPredicates = not (ClientResidualOps.filters compiled.Residual).IsEmpty
            ClientPredicateExpressions = ClientResidualOps.filters compiled.Residual |> List.map snd
            HasClientProjection =
                (match compiled.Residual with
                 | ClientResidual.RowOps ops -> ops |> List.exists (function ClientRowOp.Map _ -> true | _ -> false)
                 | _ -> false)
                || chunkSizeOpt.IsSome || shouldShuffle || isGrouping
            IsFullFallback = isFullFallback
            FallbackBoundary = fallbackBoundary
            FallbackReason = fallbackReason
            HasUdf = not detectedUdfs.IsEmpty
            UdfExpressions = detectedUdfs
        }

    interface IPolarsExplainable with
        member this.InspectPlan(optimized: bool) = this.GetPlanInspection optimized

        member this.Explain(optimized: bool) =
            (this.GetPlanInspection optimized).FormatReport()

        member this.ExplainLogicalPlan() = this.ExplainLogicalPlan()

        member this.GetLogicalPlan() = this.GetLogicalPlan()

    interface IPolarsInternalExplainable with
        member this.CompileLogicalQuery() = this.CompileLogicalQuery()
        member this.TryGetPureLogicalPlan() = this.TryGetPureLogicalPlan()

        member this.IsNativelyExecutable() =
            let pipeline = this.CompilePipeline false
            (forMaterialization pipeline.Stages pipeline.Compiled).Residual <> ClientResidual.LinqToObjects

    interface IAsyncEnumerable<'T> with
        member this.GetAsyncEnumerator(cancellationToken: CancellationToken) : IAsyncEnumerator<'T> =
            cancellationToken.ThrowIfCancellationRequested()

            let nativeLf, residual, stages = this.GetCompiledPlan()

            // Residual row operations are row-local, so they can be applied batch by batch.
            let canStreamBatches =
                residual <> ClientResidual.LinqToObjects
                && not isGrouping
                && not (stages |> List.exists LinqStageTraits.isPostCollect)

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
                    this.MaterializeFlatRows(dfHandle, residual)

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
