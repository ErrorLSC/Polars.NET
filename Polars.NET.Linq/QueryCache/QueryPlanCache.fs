namespace Polars.NET.Linq

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Linq.Expressions
open System.Threading
open Polars.NET.Core.Config
open Polars.NET.Linq.IR

/// A compiled pipeline: the optimized IR, the bound stages it was built from, and the diagnostics
/// recorded while building it.
[<NoEquality; NoComparison>]
type internal CompiledPipeline =
    {
        Compiled: CompiledLogicalQuery
        Stages: LinqStage list
        FallbackDiagnostic: string option
        Udfs: string list
    }

/// Identity of a compiled pipeline: the expression shape plus every input the IR compilation depends on.
/// Parameter values are part of the key, so a hit reuses a pipeline compiled for exactly these values.
[<NoComparison>]
type internal CompiledQueryKey =
    {
        Shape: QueryCacheKey
        /// Normalized (declared type, runtime type, value) of each extracted parameter, ordered by name.
        Values: obj list
        RootColumns: string list
        DisallowUdfPushdown: bool
    }

[<NoEquality; NoComparison>]
type internal CachedQueryPlan =
    {
        Key: CompiledQueryKey
        /// The pipeline with no source bindings; the caller binds its own root to slot 0.
        Pipeline: CompiledPipeline
        mutable LastAccessTick: int64
    }

/// Global thread-safe Query Plan Cache for Polars.NET LINQ queries.
/// Caches compiled pipelines keyed by CompiledQueryKey: alpha-equivalent expression shape, parameter values,
/// root columns and the translation-relevant configuration.
[<AbstractClass; Sealed>]
type QueryPlanCache private () =

    static let cache = ConcurrentDictionary<CompiledQueryKey, CachedQueryPlan>()
    // Admission doorkeeper: hashes of keys compiled once but not yet admitted, direct-mapped by hash.
    // Fixed size and lock-free (ConcurrentDictionary.Count takes every lock, ~20us per call).
    // A slot collision only overwrites the older sighting, so that key needs one more sighting.
    static let seenOnce: int array = Array.zeroCreate 4096
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
        Array.Clear seenOnce
        Interlocked.Exchange(&hits, 0L) |> ignore
        Interlocked.Exchange(&misses, 0L) |> ignore

    /// Evicts the oldest 20% of entries when the cache exceeds MaxCapacity.
    static member private PruneOldestEntries() =
        lock evictionLock (fun () ->
            let maxCap = QueryPlanCache.MaxCapacity

            if cache.Count >= maxCap then
                let excess = cache.Count - maxCap + 1
                let pruneTargetCount = max excess (int (float maxCap * 0.20))

                let sortedOldest =
                    cache
                    |> Seq.sortBy (fun kvp -> kvp.Value.LastAccessTick)
                    |> Seq.truncate pruneTargetCount
                    |> Seq.map (fun kvp -> kvp.Key)
                    |> Seq.toArray

                for key in sortedOldest do
                    cache.TryRemove(key) |> ignore)

    /// Attempts to retrieve a cached query plan for the given cache key.
    static member internal TryGet(key: CompiledQueryKey) : CachedQueryPlan option =
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

    /// Offers a freshly compiled plan to the cache. A key is admitted on its second sighting only, so
    /// pipelines compiled once (e.g. with a one-off parameter value) never occupy the cache.
    static member internal Admit(plan: CachedQueryPlan) : unit =
        if QueryPlanCache.Enabled then
            // 0 marks an empty slot, so a zero hash is stored as 1.
            let hash = match plan.Key.GetHashCode() with 0 -> 1 | h -> h
            let slot = hash &&& (seenOnce.Length - 1)

            if Volatile.Read(&seenOnce.[slot]) = hash then
                Volatile.Write(&seenOnce.[slot], 0)

                if cache.Count >= QueryPlanCache.MaxCapacity then
                    QueryPlanCache.PruneOldestEntries()

                plan.LastAccessTick <- Environment.TickCount64
                cache.[plan.Key] <- plan
            else
                Volatile.Write(&seenOnce.[slot], hash)

    /// Normalizes a parameter value into an equality key, or None when the value is not an immutable scalar
    /// (collections, closures, queryables, arbitrary objects), in which case the query is not cached.
    /// Normalization is strict where Equals is lenient: DateTime Kind, DateTimeOffset offset, decimal scale
    /// and the sign of floating-point zero all stay distinguishable.
    static member private TryNormalizeValue(declaredType: Type, value: obj) : obj option =
        let keyed (normalized: obj) =
            let runtimeType = if isNull value then null else value.GetType()
            Some(box (struct (declaredType, runtimeType, normalized)))

        match value with
        | null -> keyed null
        | :? string
        | :? bool
        | :? char
        | :? sbyte
        | :? byte
        | :? int16
        | :? uint16
        | :? int
        | :? uint32
        | :? int64
        | :? uint64
        | :? DateOnly
        | :? TimeOnly
        | :? TimeSpan
        | :? Guid -> keyed value
        | :? double as d -> keyed (box (BitConverter.DoubleToInt64Bits d))
        | :? float32 as f -> keyed (box (BitConverter.SingleToInt32Bits f))
        | :? decimal as m -> keyed (box (String.Join(",", Decimal.GetBits m)))
        | :? DateTime as dt -> keyed (box (struct (dt.Ticks, dt.Kind)))
        | :? DateTimeOffset as dto -> keyed (box (struct (dto.Ticks, dto.Offset)))
        | v when v.GetType().IsEnum -> keyed value
        | _ -> None

    /// Number of inline queryable constants (query roots) in the expression. The shape key identifies them
    /// by element type only, so a pipeline with more than one root cannot be told apart from another by its key.
    static member private CountQueryableRoots(expression: Expression) : int =
        let mutable count = 0

        let visitor =
            { new ExpressionVisitor() with
                override _.VisitConstant(node: ConstantExpression) =
                    if node.Value :? System.Linq.IQueryable then
                        count <- count + 1
                    base.VisitConstant node
            }

        visitor.Visit expression |> ignore
        count

    /// Builds the cache key for a pipeline, or None when caching is disabled, a parameter value is not
    /// cacheable, or the pipeline reads more than one queryable root.
    static member internal TryCreateKey
        (
            sourceType: Type,
            normalizedExpression: Expression,
            parameters: IReadOnlyDictionary<string, ExtractedParameter>,
            rootColumns: string list
        ) : CompiledQueryKey option =
        if not QueryPlanCache.Enabled || QueryPlanCache.CountQueryableRoots normalizedExpression > 1 then
            None
        else
            let values =
                parameters
                |> Seq.sortBy (fun kv -> kv.Key)
                |> Seq.map (fun kv -> QueryPlanCache.TryNormalizeValue(kv.Value.Type, kv.Value.Value))
                |> Seq.toList

            if values |> List.forall Option.isSome then
                Some
                    {
                        Shape = QueryCacheKey.Create(sourceType, normalizedExpression)
                        Values = values |> List.map Option.get
                        RootColumns = rootColumns
                        DisallowUdfPushdown = CoreConfig.DisallowUdfPushdown
                    }
            else
                None
