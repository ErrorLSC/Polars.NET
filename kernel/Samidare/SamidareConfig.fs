namespace Kaminari.Samidare.Config

open System
open System.Collections.Generic

/// <summary>
/// Defines the policy for handling operations or expressions that cannot be pushed down to the execution engine
/// and would otherwise require client-side (.NET managed) evaluation.
/// </summary>
type ClientEvaluationPolicy =
    /// <summary>
    /// Strict mode (default): terminal projections may evaluate on the client; intermediate operations
    /// (e.g. Where, GroupBy, Join) and full pipeline fallbacks that cannot be pushed down throw.
    /// </summary>
    | Strict = 0
    /// <summary>Emits a diagnostic warning when pushdown fails, but allows falling back to client-side evaluation.</summary>
    | Warn = 1
    /// <summary>Silently allows client-side evaluation fallback without warnings or exceptions.</summary>
    | Allow = 2

/// <summary>
/// Process-wide settings of the query compiler. Each is also readable from an environment variable.
/// </summary>
[<Sealed>]
type SamidareConfig private () =
    static let [<Literal>] QueryCacheCapacityKey = "POLARS_QUERY_CACHE_CAPACITY"
    static let [<Literal>] QueryCacheEnabledKey = "POLARS_QUERY_CACHE_ENABLED"
    static let [<Literal>] ClientEvaluationPolicyKey = "POLARS_CLIENT_EVAL"
    static let [<Literal>] NetClientEvaluationPolicyKey = "POLARS_NET_CLIENT_EVAL"
    static let [<Literal>] DisallowUdfPushdownKey = "POLARS_DISALLOW_UDF_PUSHDOWN"
    static let [<Literal>] AllowUnstableOrderingKey = "POLARS_ALLOW_UNSTABLE_ORDERING"
    static let [<Literal>] CollectColumnStatisticsKey = "POLARS_COLLECT_COLUMN_STATISTICS"
    static let [<Literal>] UseRelationalNullsKey = "POLARS_USE_RELATIONAL_NULLS"

    static let envKeys: IReadOnlyList<string> =
        [| QueryCacheCapacityKey
           QueryCacheEnabledKey
           ClientEvaluationPolicyKey
           NetClientEvaluationPolicyKey
           DisallowUdfPushdownKey
           AllowUnstableOrderingKey
           CollectColumnStatisticsKey
           UseRelationalNullsKey |]

    static let env (key: string) = Environment.GetEnvironmentVariable key
    static let setEnv (key: string) (value: string) = Environment.SetEnvironmentVariable(key, value)
    static let isTrue (value: string) = match value with "1" | "true" | "TRUE" | "True" -> true | _ -> false
    static let flag (value: bool) = if value then "1" else "0"

    static let positiveIntFromEnv (key: string) =
        match Int32.TryParse(env key) with
        | true, value when value > 0 -> Nullable value
        | _ -> Nullable()

    static let policyFromEnv () =
        let value =
            match env ClientEvaluationPolicyKey with
            | null -> env NetClientEvaluationPolicyKey
            | value -> value

        if String.IsNullOrEmpty value then
            ClientEvaluationPolicy.Strict
        else
            match Enum.TryParse<ClientEvaluationPolicy>(value, true) with
            | true, parsed -> parsed
            | _ -> ClientEvaluationPolicy.Strict

    static let mutable clientEvaluationPolicy = policyFromEnv ()
    static let mutable disallowUdfPushdown = isTrue (env DisallowUdfPushdownKey)
    static let mutable allowUnstableOrdering = isTrue (env AllowUnstableOrderingKey)
    static let mutable useRelationalNulls = isTrue (env UseRelationalNullsKey)

    static let mutable collectColumnStatistics =
        match env CollectColumnStatisticsKey with
        | "0" | "false" | "FALSE" | "False" -> false
        | _ -> true

    /// <summary>The environment variables that back these settings.</summary>
    static member EnvKeys = envKeys

    /// <summary>
    /// Maximum number of compiled query plans to retain in the Query Plan Cache. Default is 1024 if unset.
    /// </summary>
    static member QueryCacheCapacity
        with get () = positiveIntFromEnv QueryCacheCapacityKey
        and set (value: Nullable<int>) =
            if value.HasValue && value.Value <= 0 then
                raise (ArgumentOutOfRangeException("value", "Must be greater than 0"))

            setEnv QueryCacheCapacityKey (if value.HasValue then string value.Value else null)

    /// <summary>Whether the Query Plan Cache is globally enabled. Default is true if unset.</summary>
    static member QueryCacheEnabled
        with get () =
            let value = env QueryCacheEnabledKey
            if String.IsNullOrEmpty value then Nullable() else Nullable(isTrue value)
        and set (value: Nullable<bool>) =
            setEnv QueryCacheEnabledKey (if value.HasValue then flag value.Value else null)

    /// <summary>
    /// The policy for operations or expressions that cannot be pushed down to the execution engine and would
    /// otherwise require client-side evaluation. Default is Strict.
    /// </summary>
    static member ClientEvaluationPolicy
        with get () = clientEvaluationPolicy
        and set (value: ClientEvaluationPolicy) =
            clientEvaluationPolicy <- value
            setEnv ClientEvaluationPolicyKey (value.ToString().ToLowerInvariant())

    /// <summary>
    /// Whether to disallow pushing down managed UDF callbacks to the execution engine. Default is false. When true,
    /// an expression that would need a managed UDF callback is untranslatable, which triggers the client evaluation policy.
    /// </summary>
    static member DisallowUdfPushdown
        with get () = disallowUdfPushdown
        and set (value: bool) =
            disallowUdfPushdown <- value
            setEnv DisallowUdfPushdownKey (flag value)

    /// <summary>
    /// Whether the query optimizer may read the filtered columns of small file tables while translating a query, once
    /// per column, to estimate how many rows a filter keeps. On by default. It affects plan choices only, never results.
    /// </summary>
    static member CollectColumnStatistics
        with get () = collectColumnStatistics
        and set (value: bool) =
            collectColumnStatistics <- value
            setEnv CollectColumnStatisticsKey (flag value)

    /// <summary>
    /// Whether LINQ orderings (OrderBy / ThenBy) may be executed as unstable sorts. Default is false: orderings are
    /// stable, as in LINQ to Objects. When true, the order of rows with equal sort keys is unspecified, as with SQL
    /// ORDER BY, and the optimizer may reorder the joins and groupings beneath the sort.
    /// </summary>
    static member AllowUnstableOrdering
        with get () = allowUnstableOrdering
        and set (value: bool) =
            allowUnstableOrdering <- value
            setEnv AllowUnstableOrderingKey (flag value)

    /// <summary>
    /// Whether LINQ queries compare nulls with relational (SQL) three-valued semantics, as EF Core's
    /// <c>UseRelationalNulls()</c>. Default is false: comparisons follow C# semantics. When true, a comparison with a
    /// null operand is null (a filter drops the row), and correlated subqueries never match null keys. Comparing with
    /// a null constant or a null captured value is still an IS NULL test, <c>Contains</c> over an in-memory collection
    /// keeps .NET semantics, and explicit joins never match null keys under either setting.
    /// </summary>
    static member UseRelationalNulls
        with get () = useRelationalNulls
        and set (value: bool) =
            useRelationalNulls <- value
            setEnv UseRelationalNullsKey (flag value)
