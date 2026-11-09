namespace Polars.NET.Linq

open System
open System.Collections.Generic
open Kaminari.Samidare.Config
open Polars.NET.Core.Config

/// The whole configuration of a Polars.NET process: the native settings CoreConfig holds and the compiler settings
/// Samidare holds. The compiler settings keep their POLARS_* environment names here: their setters mirror them, and
/// EnsureImported reads them once.
[<Sealed>]
type internal PolarsRuntimeConfig private () =
    static let [<Literal>] QueryCacheCapacityKey = "POLARS_QUERY_CACHE_CAPACITY"
    static let [<Literal>] QueryCacheEnabledKey = "POLARS_QUERY_CACHE_ENABLED"
    static let [<Literal>] ClientEvaluationPolicyKey = "POLARS_CLIENT_EVAL"
    static let [<Literal>] NetClientEvaluationPolicyKey = "POLARS_NET_CLIENT_EVAL"
    static let [<Literal>] DisallowUdfPushdownKey = "POLARS_DISALLOW_UDF_PUSHDOWN"
    static let [<Literal>] AllowUnstableOrderingKey = "POLARS_ALLOW_UNSTABLE_ORDERING"
    static let [<Literal>] CollectColumnStatisticsKey = "POLARS_COLLECT_COLUMN_STATISTICS"
    static let [<Literal>] UseRelationalNullsKey = "POLARS_USE_RELATIONAL_NULLS"

    static let env (key: string) = Environment.GetEnvironmentVariable key
    static let setEnv (key: string) (value: string) = Environment.SetEnvironmentVariable(key, value)
    static let isTrue (value: string) = match value with "1" | "true" | "TRUE" | "True" -> true | _ -> false
    static let isFalse (value: string) = match value with "0" | "false" | "FALSE" | "False" -> true | _ -> false
    static let flag (value: bool) = if value then "1" else "0"
    static let policyText (policy: ClientEvaluationPolicy) = policy.ToString().ToLowerInvariant()

    static let positiveIntFromEnv (key: string) =
        match Int32.TryParse(env key) with
        | true, value when value > 0 -> Nullable value
        | _ -> Nullable()

    static let setQueryCacheCapacity (value: Nullable<int>) =
        SamidareConfig.QueryCacheCapacity <- value
        setEnv QueryCacheCapacityKey (if value.HasValue then string value.Value else null)

    static let setQueryCacheEnabled (value: Nullable<bool>) =
        SamidareConfig.QueryCacheEnabled <- value
        setEnv QueryCacheEnabledKey (if value.HasValue then flag value.Value else null)

    static let setClientEvaluationPolicy (value: ClientEvaluationPolicy) =
        SamidareConfig.ClientEvaluationPolicy <- value
        setEnv ClientEvaluationPolicyKey (policyText value)

    static let setDisallowUdfPushdown (value: bool) =
        SamidareConfig.DisallowUdfPushdown <- value
        setEnv DisallowUdfPushdownKey (flag value)

    static let setAllowUnstableOrdering (value: bool) =
        SamidareConfig.AllowUnstableOrdering <- value
        setEnv AllowUnstableOrderingKey (flag value)

    static let setCollectColumnStatistics (value: bool) =
        SamidareConfig.CollectColumnStatistics <- value
        setEnv CollectColumnStatisticsKey (flag value)

    static let setUseRelationalNulls (value: bool) =
        SamidareConfig.UseRelationalNulls <- value
        setEnv UseRelationalNullsKey (flag value)

    static let importLegacyEnvironment () =
        let capacity = positiveIntFromEnv QueryCacheCapacityKey
        if capacity.HasValue then SamidareConfig.QueryCacheCapacity <- capacity

        let enabled = env QueryCacheEnabledKey
        if not (String.IsNullOrEmpty enabled) then SamidareConfig.QueryCacheEnabled <- Nullable(isTrue enabled)

        let policy =
            match env ClientEvaluationPolicyKey with
            | null -> env NetClientEvaluationPolicyKey
            | value -> value

        if not (String.IsNullOrEmpty policy) then
            match Enum.TryParse<ClientEvaluationPolicy>(policy, true) with
            | true, parsed -> SamidareConfig.ClientEvaluationPolicy <- parsed
            | _ -> ()

        let importFlag (key: string) (apply: string -> unit) =
            match env key with
            | null -> ()
            | value -> apply value

        importFlag DisallowUdfPushdownKey (fun value -> SamidareConfig.DisallowUdfPushdown <- isTrue value)
        importFlag AllowUnstableOrderingKey (fun value -> SamidareConfig.AllowUnstableOrdering <- isTrue value)
        importFlag UseRelationalNullsKey (fun value -> SamidareConfig.UseRelationalNulls <- isTrue value)
        importFlag CollectColumnStatisticsKey (fun value -> SamidareConfig.CollectColumnStatistics <- not (isFalse value))

    static let imported = lazy (importLegacyEnvironment ())

    // A saved state carries the query cache settings only as environment variables.
    static let syncQueryCacheFromEnvironment () =
        SamidareConfig.QueryCacheCapacity <- positiveIntFromEnv QueryCacheCapacityKey
        let enabled = env QueryCacheEnabledKey
        SamidareConfig.QueryCacheEnabled <- (if String.IsNullOrEmpty enabled then Nullable() else Nullable(isTrue enabled))

    static let applyDirect (key: string) (value: string) =
        match key with
        | "client_evaluation_policy" ->
            if String.IsNullOrEmpty value then
                setClientEvaluationPolicy ClientEvaluationPolicy.Strict
            else
                match Enum.TryParse<ClientEvaluationPolicy>(value, true) with
                | true, parsed -> setClientEvaluationPolicy parsed
                | _ -> ()
        | "disallow_udf_pushdown" -> setDisallowUdfPushdown (isTrue value)
        | "allow_unstable_ordering" -> setAllowUnstableOrdering (isTrue value)
        | "collect_column_statistics" -> setCollectColumnStatistics (isTrue value)
        | "use_relational_nulls" -> setUseRelationalNulls (isTrue value)
        | _ -> ()

    static let restoreDefaults () =
        setClientEvaluationPolicy ClientEvaluationPolicy.Strict
        setDisallowUdfPushdown false
        setAllowUnstableOrdering false
        setCollectColumnStatistics true
        setUseRelationalNulls false
        setQueryCacheCapacity (Nullable())
        setQueryCacheEnabled (Nullable())

    static let extension =
        ConfigExtension(
            [| QueryCacheCapacityKey
               QueryCacheEnabledKey
               ClientEvaluationPolicyKey
               NetClientEvaluationPolicyKey
               DisallowUdfPushdownKey
               AllowUnstableOrderingKey
               CollectColumnStatisticsKey
               UseRelationalNullsKey |]
            :> IReadOnlyList<string>,
            [| struct ("client_evaluation_policy", Func<string>(fun () -> policyText SamidareConfig.ClientEvaluationPolicy))
               struct ("disallow_udf_pushdown", Func<string>(fun () -> flag SamidareConfig.DisallowUdfPushdown))
               struct ("allow_unstable_ordering", Func<string>(fun () -> flag SamidareConfig.AllowUnstableOrdering))
               struct ("collect_column_statistics", Func<string>(fun () -> flag SamidareConfig.CollectColumnStatistics))
               struct ("use_relational_nulls", Func<string>(fun () -> flag SamidareConfig.UseRelationalNulls)) |]
            :> IReadOnlyList<struct (string * Func<string>)>,
            Action<string, string>(applyDirect),
            Action(syncQueryCacheFromEnvironment),
            Action(restoreDefaults)
        )

    /// Reads the POLARS_* environment names of the compiler settings, once. Runs before anything reads those settings.
    static member EnsureImported() = imported.Force()

    /// Reads the POLARS_* environment names of the compiler settings now.
    static member ImportLegacyEnvironment() = importLegacyEnvironment ()

    static member QueryCacheCapacity
        with get () = PolarsRuntimeConfig.EnsureImported(); SamidareConfig.QueryCacheCapacity
        and set (value: Nullable<int>) = PolarsRuntimeConfig.EnsureImported(); setQueryCacheCapacity value

    static member QueryCacheEnabled
        with get () = PolarsRuntimeConfig.EnsureImported(); SamidareConfig.QueryCacheEnabled
        and set (value: Nullable<bool>) = PolarsRuntimeConfig.EnsureImported(); setQueryCacheEnabled value

    static member ClientEvaluationPolicy
        with get () = PolarsRuntimeConfig.EnsureImported(); SamidareConfig.ClientEvaluationPolicy
        and set (value: ClientEvaluationPolicy) = PolarsRuntimeConfig.EnsureImported(); setClientEvaluationPolicy value

    static member DisallowUdfPushdown
        with get () = PolarsRuntimeConfig.EnsureImported(); SamidareConfig.DisallowUdfPushdown
        and set (value: bool) = PolarsRuntimeConfig.EnsureImported(); setDisallowUdfPushdown value

    static member AllowUnstableOrdering
        with get () = PolarsRuntimeConfig.EnsureImported(); SamidareConfig.AllowUnstableOrdering
        and set (value: bool) = PolarsRuntimeConfig.EnsureImported(); setAllowUnstableOrdering value

    static member CollectColumnStatistics
        with get () = PolarsRuntimeConfig.EnsureImported(); SamidareConfig.CollectColumnStatistics
        and set (value: bool) = PolarsRuntimeConfig.EnsureImported(); setCollectColumnStatistics value

    static member UseRelationalNulls
        with get () = PolarsRuntimeConfig.EnsureImported(); SamidareConfig.UseRelationalNulls
        and set (value: bool) = PolarsRuntimeConfig.EnsureImported(); setUseRelationalNulls value

    static member GetState(ifSet: bool, envOnly: bool) =
        PolarsRuntimeConfig.EnsureImported()
        CoreConfig.GetState(ifSet, envOnly, extension)

    static member Save(ifSet: bool) =
        PolarsRuntimeConfig.EnsureImported()
        CoreConfig.Save(ifSet, extension)

    static member SaveToFile(path: string, ifSet: bool) =
        PolarsRuntimeConfig.EnsureImported()
        CoreConfig.SaveToFile(path, ifSet, extension)

    static member Load(cfg: string) =
        PolarsRuntimeConfig.EnsureImported()
        CoreConfig.Load(cfg, extension)

    static member LoadFromFile(path: string) =
        PolarsRuntimeConfig.EnsureImported()
        CoreConfig.LoadFromFile(path, extension)

    static member RestoreDefaults() =
        PolarsRuntimeConfig.EnsureImported()
        CoreConfig.RestoreDefaults(extension)
