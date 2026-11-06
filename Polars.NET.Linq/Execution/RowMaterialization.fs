namespace Polars.NET.Linq

open System
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open System.Runtime.CompilerServices
open Polars.NET.Core
open Polars.NET.Core.Wrappers
open Polars.NET.Core.Enums
open Polars.NET.Linq.IR
open Polars.NET.Linq.Backend.Polars

/// Turning a collected native plan into the query's elements: packing join scopes before lowering, then
/// materializing rows, groups, chunks or AggregateBy pairs and applying the client residual.
module internal RowMaterialization =

    /// Applies eager row shuffling (sample_frac = 1.0, without replacement) to a DataFrame.
    let applyShuffle (df: DataFrameHandle) : DataFrameHandle =
        let fracHandle = PolarsWrapper.SeriesNew("", [| 1.0 |])
        PolarsWrapper.SampleFrac(df, fracHandle, false, Nullable true, Nullable())

    /// Collects the native plan, shuffling the rows when the pipeline asks for it.
    let collect (nativeLf: LazyFrameHandle) (stages: LinqStage list) : DataFrameHandle =
        let collectedDf = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto)

        if stages |> List.exists (function LinqStage.Shuffle -> true | _ -> false) then
            applyShuffle collectedDf
        else
            collectedDf

    /// The type the rows of the native plan are materialized as, for a query of elements `targetType`: the
    /// grouped element when the query yields groups, the source of AggregateBy, the element of Chunk's arrays,
    /// otherwise the query's element type.
    let materializedRowType (targetType: Type) (stages: LinqStage list) : Type =
        let isGrouping = isGroupingType targetType

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
    let forMaterialization (targetType: Type) (stages: LinqStage list) (compiled: CompiledLogicalQuery) : CompiledLogicalQuery =
        if compiled.RowScopes.IsEmpty then
            compiled
        else
            match EntityPacking.packRow compiled.RowScopes compiled.Plan (materializedRowType targetType stages) with
            | RowPacking.NotNeeded -> compiled
            | RowPacking.Packed plan -> { compiled with Plan = plan; RowScopes = Map.empty }
            | RowPacking.Unresolvable -> { compiled with Residual = ClientResidual.LinqToObjects }

    /// Materializes flat native rows and applies the residual row operations in pipeline order.
    let materializeFlatRows<'T> (materializer: IDataFrameMaterializer) (dfHandle: DataFrameHandle) (residual: ClientResidual) : IEnumerable<'T> =
        match residual with
        | ClientResidual.RowOps ops ->
            let invoker = MaterializerInvokers.GetMaterializerInvoker(ClientResidualOps.rowType ops)
            invoker.Invoke(materializer, dfHandle) |> ClientResidualOps.apply ops |> Seq.cast<'T>
        | _ -> materializer.Materialize<'T> dfHandle

    /// Materializes a collected frame as the query's elements: groups, chunks, AggregateBy pairs, or rows.
    let materialize<'T>
        (materializer: IDataFrameMaterializer)
        (stages: LinqStage list)
        (residual: ClientResidual)
        (dfHandle: DataFrameHandle)
        : IEnumerable<'T> =
        let targetType = typeof<'T>
        let isGrouping = isGroupingType targetType
        let isRowOps = match residual with ClientResidual.RowOps _ -> true | _ -> false

        if isRowOps && not isGrouping then
            materializeFlatRows<'T> materializer dfHandle residual
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
                    materializeFlatRows<'T> materializer dfHandle residual
