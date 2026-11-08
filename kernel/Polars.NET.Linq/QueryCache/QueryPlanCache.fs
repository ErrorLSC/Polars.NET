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
        /// Whether translation ran part of the query (a subquery folded into a constant) against the data
        /// it read then. Such a pipeline is not cached: a hit would bind other tables to the folded value.
        EvaluatedAtCompile: bool
    }

/// How one queryable root of an expression enters a pipeline's identity. The shape key tells roots apart
/// by element type only; a root also brings the schema of the table it reads, and which earlier root
/// (if any) reads the very same table, as in a self-join.
[<NoComparison>]
type internal RootKey =
    {
        Schema: LogicalSchema
        /// Index of the first root reading the same table; its own index when none before it does.
        SameAs: int
    }

/// Identity of a compiled pipeline: the expression shape plus every input the IR compilation depends on.
/// Parameter values are part of the key, so a hit reuses a pipeline compiled for exactly these values.
[<NoComparison>]
type internal CompiledQueryKey =
    {
        Shape: QueryCacheKey
        /// Normalized (declared type, runtime type, value) of each extracted parameter, ordered by name.
        Values: obj list
        /// The queryable roots, in expression traversal order.
        Roots: RootKey list
        DisallowUdfPushdown: bool
        UseRelationalNulls: bool
        AllowUnstableOrdering: bool
    }

[<NoEquality; NoComparison>]
type internal CachedQueryPlan =
    {
        Key: CompiledQueryKey
        /// The pipeline with no source bindings.
        Pipeline: CompiledPipeline
        /// For each source slot of the plan, the index of the root whose table it reads. A hit binds every
        /// slot to the table of that root in the expression being compiled.
        SlotRoots: int array
        mutable LastAccessTick: int64
    }

/// A pipeline's cache key, with what binding a cached plan to this expression needs.
[<NoEquality; NoComparison>]
type internal QueryCacheProbe =
    {
        Key: CompiledQueryKey
        /// The table each queryable root reads, in the key's root order.
        RootTables: ILogicalTableSource array
        /// Whether a root occurs inside a lambda (a correlated subquery): delegates compiled from such a
        /// lambda would keep reading the roots of the expression they were compiled from.
        RootsInLambdas: bool
    }

/// Global thread-safe Query Plan Cache for Polars.NET LINQ queries.
/// Caches compiled pipelines keyed by CompiledQueryKey: alpha-equivalent expression shape, parameter values,
/// the schemas of the tables the query's roots read (and which roots share a table), and the
/// translation-relevant configuration. A hit binds every source slot to the corresponding root of the query
/// being compiled, so a pipeline over several tables is reused as well.
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

    /// The most elements a captured collection (an IN list) may have for its query to be cached.
    static member internal MaxCollectionKeyLength = 10_000

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
    static member private Admit(plan: CachedQueryPlan) : unit =
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
        // A captured collection (an IN list: `ids.Contains(x.Id)`, an array or a collection expression) keys by
        // its elements in order: the cached plan holds the values it was compiled with, so equal contents may
        // reuse it. Not for queries (the inlined roots key those), nor for collections large enough that the key
        // would cost more than it saves.
        | :? System.Collections.IEnumerable as items when not (value :? System.Linq.IQueryable) ->
            let elements = ResizeArray<obj>()
            let mutable ok = true
            let e = items.GetEnumerator()

            while ok && e.MoveNext() do
                if elements.Count >= QueryPlanCache.MaxCollectionKeyLength then
                    ok <- false
                else
                    let item = e.Current
                    let itemType = if isNull item then typeof<obj> else item.GetType()

                    match QueryPlanCache.TryNormalizeValue(itemType, item) with
                    | Some normalized -> elements.Add normalized
                    | None -> ok <- false

            if ok then keyed (box (List.ofSeq elements)) else None
        | _ -> None

    /// The expression with every composed Polars query constant (a query built on a root, e.g. the inner side of
    /// a Join or Concat, or a subquery captured as a value or read from a closure) replaced by that query's own
    /// expression, recursively.
    /// The shape key identifies a query constant by its element type only, so without this two different
    /// subqueries over the same root would share a key. After inlining, the constants left are the roots
    /// themselves, which read the same tables as the composed queries did.
    static member internal InlineComposedQueries(expression: Expression) : Expression =
        let visitor =
            { new ExpressionVisitor() with
                override this.VisitConstant(node: ConstantExpression) =
                    match node.Value with
                    | :? System.Linq.IQueryable as q when (q :? IPolarsInternalExplainable) ->
                        match q.Expression with
                        | :? ConstantExpression as c when obj.ReferenceEquals(c.Value, q) -> node :> Expression
                        | composed ->
                            let inlined = this.Visit composed
                            if node.Type.IsAssignableFrom inlined.Type then inlined
                            else Expression.Convert(inlined, node.Type) :> Expression
                    | _ -> node :> Expression

                // A query captured in a closure (`var ids = q2...; q.Where(r => ids.Contains(..))`): partial
                // evaluation leaves IQueryable-typed reads alone and extracts the closure object itself, which no
                // key can hold. Reading the field here turns it into a query constant, inlined like the others.
                override this.VisitMember(node: MemberExpression) =
                    if
                        typeof<System.Collections.IEnumerable>.IsAssignableFrom node.Type
                        && node.Type <> typeof<string>
                    then
                        match PartialEvaluator.tryFastEvaluate node with
                        | Some(:? IPolarsInternalExplainable as q) ->
                            this.VisitConstant(Expression.Constant(q, node.Type))
                        | _ -> base.VisitMember node
                    else
                        base.VisitMember node
            }

        visitor.Visit expression

    /// The inline queryable constants (query roots) of the expression in traversal order, and whether any of
    /// them occurs inside a lambda.
    static member private CollectRoots(expression: Expression) : System.Linq.IQueryable list * bool =
        let roots = ResizeArray<System.Linq.IQueryable>()
        let mutable lambdaDepth = 0
        let mutable inLambda = false

        let visitor =
            { new ExpressionVisitor() with
                override this.VisitLambda<'D>(node: Expression<'D>) =
                    lambdaDepth <- lambdaDepth + 1
                    let visited = base.VisitLambda node
                    lambdaDepth <- lambdaDepth - 1
                    visited

                override _.VisitConstant(node: ConstantExpression) =
                    match node.Value with
                    | :? System.Linq.IQueryable as q ->
                        roots.Add q
                        if lambdaDepth > 0 then
                            inLambda <- true
                    | _ -> ()

                    base.VisitConstant node
            }

        visitor.Visit expression |> ignore
        List.ofSeq roots, inLambda

    /// Builds the cache key for a pipeline whose roots read `rootTables`, or None when caching is disabled
    /// or a parameter value is not cacheable.
    static member internal TryCreateKey
        (
            sourceType: Type,
            normalizedExpression: Expression,
            parameters: IReadOnlyDictionary<string, ExtractedParameter>,
            rootTables: ILogicalTableSource array
        ) : CompiledQueryKey option =
        if not QueryPlanCache.Enabled then
            None
        else
            let roots =
                rootTables
                |> Array.mapi (fun i table ->
                    {
                        Schema = table.Schema
                        SameAs = rootTables |> Array.findIndex (fun t -> obj.ReferenceEquals(t, table))
                    })
                |> List.ofArray

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
                        Roots = roots
                        DisallowUdfPushdown = CoreConfig.DisallowUdfPushdown
                        UseRelationalNulls = CoreConfig.UseRelationalNulls
                        AllowUnstableOrdering = CoreConfig.AllowUnstableOrdering
                    }
            else
                None

    /// The cache key of a normalized expression and the tables of its roots, or None when the expression is
    /// not cacheable: caching is disabled, a parameter value is not cacheable, or a root is not a Polars query
    /// (an in-memory sequence is snapshotted into the plan's bindings, not read from a table).
    static member internal TryCreateProbe
        (
            sourceType: Type,
            normalizedExpression: Expression,
            parameters: IReadOnlyDictionary<string, ExtractedParameter>
        ) : QueryCacheProbe option =
        if not QueryPlanCache.Enabled then
            None
        else
            let roots, rootsInLambdas = QueryPlanCache.CollectRoots normalizedExpression

            let tables =
                roots
                |> List.map (function
                    | :? IPolarsInternalExplainable as q -> Some q.RootTable
                    | _ -> None)

            if tables |> List.forall Option.isSome then
                let rootTables = tables |> List.map Option.get |> Array.ofList

                QueryPlanCache.TryCreateKey(sourceType, normalizedExpression, parameters, rootTables)
                |> Option.map (fun key -> { Key = key; RootTables = rootTables; RootsInLambdas = rootsInLambdas })
            else
                None

    /// The cached pipeline for the probe's key, bound to the probe's root tables.
    static member internal TryGetBound(probe: QueryCacheProbe) : CompiledPipeline option =
        QueryPlanCache.TryGet probe.Key
        |> Option.map (fun cached ->
            let sources = cached.SlotRoots |> Array.map (fun root -> SourceBinding.Table probe.RootTables.[root])
            { cached.Pipeline with Compiled = { cached.Pipeline.Compiled with Sources = sources } })

    /// Offers a pipeline compiled for the probe's expression. It is cacheable when every source slot reads
    /// the table of one of the expression's roots, so a later hit can bind the slots to its own roots; and,
    /// when a root occurs inside a lambda, when no delegate compiled from the expression runs at execution
    /// (a fully native plan whose rows are materialized directly).
    static member internal Offer(probe: QueryCacheProbe, pipeline: CompiledPipeline, isGrouping: bool) : unit =
        let compiled = pipeline.Compiled

        let slotRoots =
            compiled.Sources
            |> Array.map (function
                | SourceBinding.Table table -> probe.RootTables |> Array.tryFindIndex (fun t -> obj.ReferenceEquals(t, table))
                | SourceBinding.InMemory _ -> None)

        let runsCompiledDelegates =
            compiled.Residual <> ClientResidual.None
            || isGrouping
            || pipeline.Stages |> List.exists LinqStageTraits.isPostCollect

        if
            slotRoots |> Array.forall Option.isSome
            && not (probe.RootsInLambdas && runsCompiledDelegates)
            && not pipeline.EvaluatedAtCompile
        then
            QueryPlanCache.Admit
                {
                    Key = probe.Key
                    Pipeline = { pipeline with Compiled = { compiled with Sources = [||] } }
                    SlotRoots = slotRoots |> Array.map Option.get
                    LastAccessTick = 0L
                }

