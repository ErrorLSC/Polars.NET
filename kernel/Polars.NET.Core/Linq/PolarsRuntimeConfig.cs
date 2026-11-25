using Kaminari.Samidare.Config;
using Polars.NET.Core.Config;

namespace Polars.NET.Core.Linq;

/// <summary>
/// The whole configuration of a Polars.NET process: the native settings CoreConfig holds and the compiler settings
/// Samidare holds. The compiler settings keep their POLARS_* environment names here: their setters mirror them, and
/// <see cref="EnsureImported"/> reads them once.
/// </summary>
internal static class PolarsRuntimeConfig
{
    private const string QueryCacheCapacityKey = "POLARS_QUERY_CACHE_CAPACITY";
    private const string QueryCacheEnabledKey = "POLARS_QUERY_CACHE_ENABLED";
    private const string ClientEvaluationPolicyKey = "POLARS_CLIENT_EVAL";
    private const string NetClientEvaluationPolicyKey = "POLARS_NET_CLIENT_EVAL";
    private const string DisallowUdfPushdownKey = "POLARS_DISALLOW_UDF_PUSHDOWN";
    private const string AllowUnstableOrderingKey = "POLARS_ALLOW_UNSTABLE_ORDERING";
    private const string CollectColumnStatisticsKey = "POLARS_COLLECT_COLUMN_STATISTICS";
    private const string UseRelationalNullsKey = "POLARS_USE_RELATIONAL_NULLS";

    private static readonly Lazy<bool> Imported = new(() =>
    {
        ImportLegacyEnvironment();
        return true;
    });

    private static readonly ConfigExtension Extension = new(
        [
            QueryCacheCapacityKey,
            QueryCacheEnabledKey,
            ClientEvaluationPolicyKey,
            NetClientEvaluationPolicyKey,
            DisallowUdfPushdownKey,
            AllowUnstableOrderingKey,
            CollectColumnStatisticsKey,
            UseRelationalNullsKey,
        ],
        [
            ("client_evaluation_policy", () => PolicyText(SamidareConfig.ClientEvaluationPolicy)),
            ("disallow_udf_pushdown", () => Flag(SamidareConfig.DisallowUdfPushdown)),
            ("allow_unstable_ordering", () => Flag(SamidareConfig.AllowUnstableOrdering)),
            ("collect_column_statistics", () => Flag(SamidareConfig.CollectColumnStatistics)),
            ("use_relational_nulls", () => Flag(SamidareConfig.UseRelationalNulls)),
        ],
        ApplyDirect,
        SyncQueryCacheFromEnvironment,
        RestoreDefaultValues);

    private static string? Env(string key) => Environment.GetEnvironmentVariable(key);

    private static bool IsTrue(string? value) => value is "1" or "true" or "TRUE" or "True";

    private static bool IsFalse(string? value) => value is "0" or "false" or "FALSE" or "False";

    private static string Flag(bool value) => value ? "1" : "0";

    private static string PolicyText(ClientEvaluationPolicy policy) => policy.ToString().ToLowerInvariant();

    private static int? PositiveIntFromEnv(string key) =>
        int.TryParse(Env(key), out int value) && value > 0 ? value : null;

    private static void SetQueryCacheCapacity(int? value)
    {
        SamidareConfig.QueryCacheCapacity = value;
        Environment.SetEnvironmentVariable(QueryCacheCapacityKey, value?.ToString());
    }

    private static void SetQueryCacheEnabled(bool? value)
    {
        SamidareConfig.QueryCacheEnabled = value;
        Environment.SetEnvironmentVariable(QueryCacheEnabledKey, value is { } v ? Flag(v) : null);
    }

    private static void SetClientEvaluationPolicy(ClientEvaluationPolicy value)
    {
        SamidareConfig.ClientEvaluationPolicy = value;
        Environment.SetEnvironmentVariable(ClientEvaluationPolicyKey, PolicyText(value));
    }

    private static void SetDisallowUdfPushdown(bool value)
    {
        SamidareConfig.DisallowUdfPushdown = value;
        Environment.SetEnvironmentVariable(DisallowUdfPushdownKey, Flag(value));
    }

    private static void SetAllowUnstableOrdering(bool value)
    {
        SamidareConfig.AllowUnstableOrdering = value;
        Environment.SetEnvironmentVariable(AllowUnstableOrderingKey, Flag(value));
    }

    private static void SetCollectColumnStatistics(bool value)
    {
        SamidareConfig.CollectColumnStatistics = value;
        Environment.SetEnvironmentVariable(CollectColumnStatisticsKey, Flag(value));
    }

    private static void SetUseRelationalNulls(bool value)
    {
        SamidareConfig.UseRelationalNulls = value;
        Environment.SetEnvironmentVariable(UseRelationalNullsKey, Flag(value));
    }

    /// <summary>Reads the POLARS_* environment names of the compiler settings now.</summary>
    public static void ImportLegacyEnvironment()
    {
        if (PositiveIntFromEnv(QueryCacheCapacityKey) is { } capacity) SamidareConfig.QueryCacheCapacity = capacity;

        string? enabled = Env(QueryCacheEnabledKey);
        if (!string.IsNullOrEmpty(enabled)) SamidareConfig.QueryCacheEnabled = IsTrue(enabled);

        string? policy = Env(ClientEvaluationPolicyKey) ?? Env(NetClientEvaluationPolicyKey);
        if (!string.IsNullOrEmpty(policy) && Enum.TryParse(policy, true, out ClientEvaluationPolicy parsed))
            SamidareConfig.ClientEvaluationPolicy = parsed;

        if (Env(DisallowUdfPushdownKey) is { } disallow) SamidareConfig.DisallowUdfPushdown = IsTrue(disallow);
        if (Env(AllowUnstableOrderingKey) is { } unstable) SamidareConfig.AllowUnstableOrdering = IsTrue(unstable);
        if (Env(UseRelationalNullsKey) is { } relational) SamidareConfig.UseRelationalNulls = IsTrue(relational);
        if (Env(CollectColumnStatisticsKey) is { } statistics) SamidareConfig.CollectColumnStatistics = !IsFalse(statistics);
    }

    // A saved state carries the query cache settings only as environment variables.
    private static void SyncQueryCacheFromEnvironment()
    {
        SamidareConfig.QueryCacheCapacity = PositiveIntFromEnv(QueryCacheCapacityKey);
        string? enabled = Env(QueryCacheEnabledKey);
        SamidareConfig.QueryCacheEnabled = string.IsNullOrEmpty(enabled) ? null : IsTrue(enabled);
    }

    private static void ApplyDirect(string key, string? value)
    {
        switch (key)
        {
            case "client_evaluation_policy":
                if (string.IsNullOrEmpty(value)) SetClientEvaluationPolicy(ClientEvaluationPolicy.Strict);
                else if (Enum.TryParse(value, true, out ClientEvaluationPolicy parsed)) SetClientEvaluationPolicy(parsed);
                break;
            case "disallow_udf_pushdown": SetDisallowUdfPushdown(IsTrue(value)); break;
            case "allow_unstable_ordering": SetAllowUnstableOrdering(IsTrue(value)); break;
            case "collect_column_statistics": SetCollectColumnStatistics(IsTrue(value)); break;
            case "use_relational_nulls": SetUseRelationalNulls(IsTrue(value)); break;
        }
    }

    private static void RestoreDefaultValues()
    {
        SetClientEvaluationPolicy(ClientEvaluationPolicy.Strict);
        SetDisallowUdfPushdown(false);
        SetAllowUnstableOrdering(false);
        SetCollectColumnStatistics(true);
        SetUseRelationalNulls(false);
        SetQueryCacheCapacity(null);
        SetQueryCacheEnabled(null);
    }

    /// <summary>Reads the POLARS_* environment names of the compiler settings, once. Runs before anything reads those settings.</summary>
    public static void EnsureImported() => _ = Imported.Value;

    public static int? QueryCacheCapacity
    {
        get { EnsureImported(); return SamidareConfig.QueryCacheCapacity; }
        set { EnsureImported(); SetQueryCacheCapacity(value); }
    }

    public static bool? QueryCacheEnabled
    {
        get { EnsureImported(); return SamidareConfig.QueryCacheEnabled; }
        set { EnsureImported(); SetQueryCacheEnabled(value); }
    }

    public static ClientEvaluationPolicy ClientEvaluationPolicy
    {
        get { EnsureImported(); return SamidareConfig.ClientEvaluationPolicy; }
        set { EnsureImported(); SetClientEvaluationPolicy(value); }
    }

    public static bool DisallowUdfPushdown
    {
        get { EnsureImported(); return SamidareConfig.DisallowUdfPushdown; }
        set { EnsureImported(); SetDisallowUdfPushdown(value); }
    }

    public static bool AllowUnstableOrdering
    {
        get { EnsureImported(); return SamidareConfig.AllowUnstableOrdering; }
        set { EnsureImported(); SetAllowUnstableOrdering(value); }
    }

    public static bool CollectColumnStatistics
    {
        get { EnsureImported(); return SamidareConfig.CollectColumnStatistics; }
        set { EnsureImported(); SetCollectColumnStatistics(value); }
    }

    public static bool UseRelationalNulls
    {
        get { EnsureImported(); return SamidareConfig.UseRelationalNulls; }
        set { EnsureImported(); SetUseRelationalNulls(value); }
    }

    public static Dictionary<string, string?> GetState(bool ifSet, bool envOnly)
    {
        EnsureImported();
        return CoreConfig.GetState(ifSet, envOnly, Extension);
    }

    public static string Save(bool ifSet)
    {
        EnsureImported();
        return CoreConfig.Save(ifSet, Extension);
    }

    public static void SaveToFile(string path, bool ifSet)
    {
        EnsureImported();
        CoreConfig.SaveToFile(path, ifSet, Extension);
    }

    public static void Load(string cfg)
    {
        EnsureImported();
        CoreConfig.Load(cfg, Extension);
    }

    public static void LoadFromFile(string path)
    {
        EnsureImported();
        CoreConfig.LoadFromFile(path, Extension);
    }

    public static void RestoreDefaults()
    {
        EnsureImported();
        CoreConfig.RestoreDefaults(Extension);
    }
}
