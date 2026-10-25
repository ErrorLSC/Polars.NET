namespace Polars.NET.Linq

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open Polars.NET.Core

/// Represents a compiled query plan template stored in the Query Plan Cache.
[<NoEquality; NoComparison>]
type internal CachedQueryPlan =
    {
        Key: QueryCacheKey
        NormalizedExpression: Expression
        ParamNames: string array
        NormalizedStages: LinqStage list
        mutable LastAccessTick: int64
    }

/// Global thread-safe Query Plan Cache for Polars.NET LINQ queries.
/// Caches normalized query pipelines keyed by alpha-equivalent QueryCacheKey.
[<AbstractClass; Sealed>]
type QueryPlanCache private () =

    static let cache = ConcurrentDictionary<QueryCacheKey, CachedQueryPlan>()
    static let evictionLock = obj ()
    static let mutable hits = 0L
    static let mutable misses = 0L

    /// Gets or sets whether query plan caching is globally enabled.
    static member Enabled
        with get () = CoreConfig.QueryCacheEnabled.GetValueOrDefault true
        and set (v: bool) = CoreConfig.QueryCacheEnabled <- Nullable v

    /// Gets or sets the maximum number of query plans stored in the cache. Defaults to 1024.
    static member MaxCapacity
        with get () = CoreConfig.QueryCacheCapacity.GetValueOrDefault 1024
        and set (v: int) = CoreConfig.QueryCacheCapacity <- Nullable v

    /// Gets the total number of cached query plans.
    static member Count = cache.Count

    /// Gets the number of query cache hits since startup or last clear.
    static member Hits = Interlocked.Read(&hits)

    /// Gets the number of query cache misses since startup or last clear.
    static member Misses = Interlocked.Read(&misses)

    /// Resets all cache statistics and clears all cached query plans.
    static member Clear() =
        cache.Clear()
        Interlocked.Exchange(&hits, 0L) |> ignore
        Interlocked.Exchange(&misses, 0L) |> ignore

    /// Evicts the oldest 20% of entries when the cache exceeds MaxCapacity.
    static member private PruneOldestEntries() =
        lock evictionLock (fun () ->
            let maxCap = QueryPlanCache.MaxCapacity

            if cache.Count >= maxCap then
                let pruneTargetCount = max 1 (int (float maxCap * 0.20))

                let sortedOldest =
                    cache
                    |> Seq.sortBy (fun kvp -> kvp.Value.LastAccessTick)
                    |> Seq.truncate pruneTargetCount
                    |> Seq.map (fun kvp -> kvp.Key)
                    |> Seq.toArray

                for key in sortedOldest do
                    cache.TryRemove(key) |> ignore)

    /// Attempts to retrieve a cached query plan for the given cache key.
    static member internal TryGet(key: QueryCacheKey) : CachedQueryPlan option =
        if not QueryPlanCache.Enabled then
            None
        else
            match cache.TryGetValue key with
            | true, plan ->
                Interlocked.Increment(&hits) |> ignore
                Volatile.Write(&plan.LastAccessTick, Environment.TickCount64)
                Some plan
            | false, _ ->
                Interlocked.Increment(&misses) |> ignore
                None

    /// Attempts to retrieve a cached query plan for C# callers using the standard TryGet pattern without affecting hit/miss metrics.
    static member internal TryGetPlan
        (key: QueryCacheKey, [<System.Runtime.InteropServices.Out>] plan: byref<CachedQueryPlan>)
        : bool =
        if not QueryPlanCache.Enabled then
            plan <- Unchecked.defaultof<CachedQueryPlan>
            false
        elif cache.TryGetValue(key, &plan) then
            Volatile.Write(&plan.LastAccessTick, Environment.TickCount64)
            true
        else
            false

    /// Stores or replaces a cached query plan.
    static member internal Set(plan: CachedQueryPlan) : unit =
        if QueryPlanCache.Enabled then
            if cache.Count >= QueryPlanCache.MaxCapacity then
                QueryPlanCache.PruneOldestEntries()

            plan.LastAccessTick <- Environment.TickCount64
            cache.[plan.Key] <- plan

    /// Gets an existing plan or computes and adds it atomically with capacity bounds.
    static member internal GetOrAdd(key: QueryCacheKey, factory: QueryCacheKey -> CachedQueryPlan) : CachedQueryPlan =
        if not QueryPlanCache.Enabled then
            factory key
        else
            match cache.TryGetValue key with
            | true, existingPlan ->
                Interlocked.Increment(&hits) |> ignore
                Volatile.Write(&existingPlan.LastAccessTick, Environment.TickCount64)
                existingPlan
            | false, _ ->
                Interlocked.Increment(&misses) |> ignore
                let newPlan = factory key
                newPlan.LastAccessTick <- Environment.TickCount64

                if cache.Count >= QueryPlanCache.MaxCapacity then
                    QueryPlanCache.PruneOldestEntries()

                cache.GetOrAdd(key, newPlan)

    /// Substitutes extracted runtime parameter values into a LinqStage.
    static member internal SubstituteStage
        (paramsDict: IReadOnlyDictionary<string, ExtractedParameter>)
        (stage: LinqStage)
        : LinqStage =
        if isNull paramsDict || paramsDict.Count = 0 then
            stage
        else
            let subExpr (e: Expression) =
                if isNull e then
                    null
                else
                    PartialEvaluator.substituteExtracted e paramsDict

            let subLambda (l: LambdaExpression) =
                if isNull l then null else subExpr l :?> LambdaExpression

            let subLambdaOpt (lo: LambdaExpression option) = lo |> Option.map subLambda
            let subExprOpt (eo: Expression option) = eo |> Option.map subExpr

            match stage with
            | LinqStage.Filter l -> LinqStage.Filter(subLambda l)
            | LinqStage.Sort(l, desc) -> LinqStage.Sort(subLambda l, desc)
            | LinqStage.Project l -> LinqStage.Project(subLambda l)
            | LinqStage.Join(m, inner, ok, ik, rs) ->
                LinqStage.Join(m, subExpr inner, subLambda ok, subLambda ik, subLambdaOpt rs)
            | LinqStage.JoinAsOf(m, inner, ok, ik, obOpt, ibOpt, rsOpt, ts, ti, tf, strat) ->
                LinqStage.JoinAsOf(m, subExpr inner, subLambda ok, subLambda ik, subLambdaOpt obOpt, subLambdaOpt ibOpt, subLambdaOpt rsOpt, ts, ti, tf, strat)
            | LinqStage.GroupJoin(m, inner, ok, ik, rs) ->
                LinqStage.GroupJoin(m, subExpr inner, subLambda ok, subLambda ik, subLambdaOpt rs)
            | LinqStage.CrossJoin(inner, rs) -> LinqStage.CrossJoin(subExpr inner, subLambdaOpt rs)
            | LinqStage.Explode(col, rs) -> LinqStage.Explode(subLambda col, subLambdaOpt rs)
            | LinqStage.Zip(sec, rs) -> LinqStage.Zip(subExpr sec, subLambdaOpt rs)
            | LinqStage.Zip3(sec, thd) -> LinqStage.Zip3(subExpr sec, subExpr thd)
            | LinqStage.SetOp(m, sec, k1, k2) -> LinqStage.SetOp(m, subExpr sec, subLambdaOpt k1, subLambdaOpt k2)
            | LinqStage.Union(sec, kl) -> LinqStage.Union(subExpr sec, subLambdaOpt kl)
            | LinqStage.Concat sec -> LinqStage.Concat(subExpr sec)
            | LinqStage.Append elem -> LinqStage.Append(subExpr elem)
            | LinqStage.Prepend elem -> LinqStage.Prepend(subExpr elem)
            | LinqStage.TakeWhile l -> LinqStage.TakeWhile(subLambda l)
            | LinqStage.SkipWhile l -> LinqStage.SkipWhile(subLambda l)
            | LinqStage.DistinctBy l -> LinqStage.DistinctBy(subLambda l)
            | LinqStage.CountBy l -> LinqStage.CountBy(subLambda l)
            | LinqStage.GroupByKey l -> LinqStage.GroupByKey(subLambda l)
            | LinqStage.GroupPartitionSlice(kl, inner, resOpt) ->
                LinqStage.GroupPartitionSlice(subLambda kl, inner |> List.map (QueryPlanCache.SubstituteStage paramsDict), subLambdaOpt resOpt)
            | LinqStage.GroupByWithResult(kl, rl) -> LinqStage.GroupByWithResult(subLambda kl, subLambda rl)
            | LinqStage.GroupByWithElementAndResult(kl, el, rl) ->
                LinqStage.GroupByWithElementAndResult(subLambda kl, subLambda el, subLambda rl)
            | LinqStage.AggregateBy(kl, seed, factoryOpt, f) ->
                LinqStage.AggregateBy(subLambda kl, subExpr seed, subLambdaOpt factoryOpt, subLambda f)
            | LinqStage.DefaultIfEmpty defOpt -> LinqStage.DefaultIfEmpty(subExprOpt defOpt)
            | other -> other
