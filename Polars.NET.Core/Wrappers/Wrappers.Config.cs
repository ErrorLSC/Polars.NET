using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

/// <summary>
/// Config for Polars.NET
/// </summary>
public readonly partial struct PolarsWrapper
{
    /// <summary>
    /// Background Async Prefetch batch size 
    /// Default is 2. Can change this by GetEnvironmentVariable("POLARS_NET_PREFETCH_SIZE")
    /// </summary>
    public static int DefaultPrefetchBufferSize { get; set; } = GetDefaultPrefetchSizeFromEnv();

    private static int GetDefaultPrefetchSizeFromEnv()
    {
        var envValue = Environment.GetEnvironmentVariable("POLARS_NET_PREFETCH_SIZE");
        
        if (int.TryParse(envValue, out int size) && size > 0)
        {
            return size;
        }
        
        return 2;
    }
    /// <summary>
    /// Inject Environment var to Rust
    /// </summary>
    public static void SetEnvVar(string key, string? value)
    {
        // Set .NET environment variable
        Environment.SetEnvironmentVariable(key, value);
        // Set Rust environment variable
        if (value == null)
        {
            NativeBindings.pl_set_env_var(key, nint.Zero);
        }
        else
        {
            nint utf8Ptr = Marshal.StringToCoTaskMemUTF8(value);
            try
            {
                NativeBindings.pl_set_env_var(key, utf8Ptr);
            }
            finally
            {
                Marshal.FreeCoTaskMem(utf8Ptr); 
            }
        }

        // ReloadEnvVar(key);
        ErrorHelper.CheckVoid();
    }
    public static void ReloadEnvVar(string key)
    {
        NativeBindings.pl_config_reload_var(key);
        ErrorHelper.CheckVoid();
    }
    public static void ReloadEnvVarAll()
    {
        NativeBindings.pl_config_reload_all();
        ErrorHelper.CheckVoid();
    }
    public static ulong ConfigGetMaxThreads()
    {
        int status = NativeBindings.pl_config_get_max_threads(out ulong threads);
        ErrorHelper.CheckStatus(status);
        return threads;
    }
    public static bool ConfigGetVerbose()
    {
        int status = NativeBindings.pl_config_get_verbose(out bool verbose);
        ErrorHelper.CheckStatus(status);
        return verbose;
    }
   public static bool ConfigGetWarnUnstable()
    {
        int status = NativeBindings.pl_config_get_warn_unstable(out bool warn);
        ErrorHelper.CheckStatus(status);
        return warn;
    }
    public static bool ConfigGetAllowNestedCspe()
    {
        int status = NativeBindings.pl_config_get_allow_nested_cspe(out bool active);
        ErrorHelper.CheckStatus(status);
        return active;
    }
    public static bool ConfigGetVerboseSensitive()
    {
        int status = NativeBindings.pl_config_get_verbose_sensitive(out bool active);
        ErrorHelper.CheckStatus(status);
        return active;
    }
    public static bool ConfigGetImportIntervalAsStruct()
    {
        int status = NativeBindings.pl_config_get_import_interval_as_struct(out bool active);
        ErrorHelper.CheckStatus(status);
        return active;
    }
    public static bool ConfigGetForceAsync()
    {
        int status = NativeBindings.pl_config_get_force_async(out bool active);
        ErrorHelper.CheckStatus(status);
        return active;
    }
    public static PlEngine ConfigGetEngineAffinity()
    {
        int status = NativeBindings.pl_config_get_engine_affinity(out PlEngine engine);
        ErrorHelper.CheckStatus(status);
        return engine;     
    }
    public static PlResolveMode ConfigGetResolveMetadataLevel()
    {
        int status = NativeBindings.pl_config_get_resolve_metadata_level(out PlResolveMode mode);
        ErrorHelper.CheckStatus(status);
        return mode;     
    }
    public static ulong ConfigGetIdealMorselSize()
    {
        int status = NativeBindings.pl_config_get_ideal_morsel_size(out ulong size);
        ErrorHelper.CheckStatus(status);
        return size;     
    }
    public static ulong ConfigGetParquetBinaryStatisticsTruncateLength()
    {
        int status = NativeBindings.pl_config_get_parquet_binary_statistics_truncate_length(out ulong length);
        ErrorHelper.CheckStatus(status);
        return length;     
    }
    public static bool ConfigGetPruneParquetMetadata()
    {
        int status = NativeBindings.pl_config_get_prune_parquet_metadata(out bool active);
        ErrorHelper.CheckStatus(status);
        return active;
    }
    public static ulong ConfigGetOOCDriftThreshold()
    {
        int status = NativeBindings.pl_config_get_ooc_drift_threshold(out ulong threshold);
        ErrorHelper.CheckStatus(status);
        return threshold;
    }
    public static PlOOCSpillPolicy ConfigGetOOCSpillPolicy()
    {
        int status = NativeBindings.pl_config_get_ooc_spill_policy(out PlOOCSpillPolicy policy);
        ErrorHelper.CheckStatus(status);
        return policy;     
    }
    public static PlOOCSpillFormat ConfigGetOOCSpillFormat()
    {
        int status = NativeBindings.pl_config_get_ooc_spill_format(out PlOOCSpillFormat fmt);
        ErrorHelper.CheckStatus(status);
        return fmt;     
    }
    public static double ConfigGetOOCMemoryBudgetFraction()
    {
        int status = NativeBindings.pl_config_get_ooc_memory_budget_fraction(out double fraction);
        ErrorHelper.CheckStatus(status);
        return fraction;     
    }
    public static ulong ConfigGetOOCSpillMinBytes()
    {
        int status = NativeBindings.pl_config_get_ooc_spill_min_bytes(out ulong bytes);
        ErrorHelper.CheckStatus(status);
        return bytes;     
    }
    public static string ConfigGetOOCSpillDir()
    {
        nint dir = NativeBindings.pl_config_get_ooc_spill_dir();
        return ErrorHelper.CheckString(dir);
    }
    public static ulong ConfigGetJoinSampleLimit()
    {
        int status = NativeBindings.pl_config_get_join_sample_limit(out ulong limit);
        ErrorHelper.CheckStatus(status);
        return limit;     
    }
    public static bool ConfigGetProjectionPushdownPruneStrictHconcatInputs()
    {
        int status = NativeBindings.pl_config_get_projection_pushdown_prune_strict_hconcat_inputs(out bool active);
        ErrorHelper.CheckStatus(status);
        return active;
    }
    public static PlFloatFormat ConfigGetFloatFormat()
    {
        int status = NativeBindings.pl_config_get_float_fmt(out PlFloatFormat fmt);
        ErrorHelper.CheckStatus(status);
        return fmt;     
    }
    public static void ConfigSetFloatFormat(PlFloatFormat fmt)
    {
        NativeBindings.pl_config_set_float_fmt(fmt);
        ErrorHelper.CheckVoid();
    }
    public static long ConfigGetFloatPrecision()
    {
        int status = NativeBindings.pl_config_get_float_precision(out long precision);
        ErrorHelper.CheckStatus(status);
        return precision;     
    }
    public static void ConfigSetFloatPrecision(long precision)
    {
        NativeBindings.pl_config_set_float_precision(precision);
        ErrorHelper.CheckVoid();
    }
    public static string ConfigGetDecimalSeparator()
    {
        nint sep = NativeBindings.pl_config_get_decimal_separator();
        return ErrorHelper.CheckString(sep);
    }
    public static void ConfigSetDecimalSeparator(string? separator)
    {
        NativeBindings.pl_config_set_decimal_separator(separator);
        ErrorHelper.CheckVoid();
    }
    public static string ConfigGetThousandsSeparator()
    {
        nint sep = NativeBindings.pl_config_get_thousands_separator();
        return ErrorHelper.CheckString(sep);
    }
    public static void ConfigSetThousandsSeparator(string? separator)
    {
        NativeBindings.pl_config_set_thousands_separator(separator);
        ErrorHelper.CheckVoid();
    }
    public static bool ConfigGetTrimDecimalZeros()
    {
        int status = NativeBindings.pl_config_get_trim_decimal_zeros(out bool trim);
        ErrorHelper.CheckStatus(status);
        return trim;
    }
    public static void ConfigSetTrimDecimalZeros(bool active,bool hasValue)
    {
        NativeBindings.pl_config_set_trim_decimal_zeros(active,hasValue);
        ErrorHelper.CheckVoid();
    }


}