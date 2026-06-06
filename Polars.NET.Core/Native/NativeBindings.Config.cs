using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

internal partial class NativeBindings
{
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_set_env_var(
        string key, 
        nint value);
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_config_reload_var(string key);
    [LibraryImport(LibName)]
    public static partial void pl_config_reload_all();
    [LibraryImport(LibName)]
    public static partial int pl_config_get_max_threads(out ulong threads);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_warn_unstable([MarshalAs(UnmanagedType.U1)]out bool warn);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_verbose([MarshalAs(UnmanagedType.U1)]out bool verbose);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_engine_affinity(out PlEngine engine);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_ideal_morsel_size(out ulong size);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_parquet_binary_statistics_truncate_length(out ulong length);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_prune_parquet_metadata([MarshalAs(UnmanagedType.U1)]out bool active);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_allow_nested_cspe([MarshalAs(UnmanagedType.U1)]out bool active);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_verbose_sensitive([MarshalAs(UnmanagedType.U1)]out bool active);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_force_async([MarshalAs(UnmanagedType.U1)]out bool active);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_import_interval_as_struct([MarshalAs(UnmanagedType.U1)]out bool active);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_resolve_metadata_level(out PlResolveMode mode);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_ooc_drift_threshold(out ulong threshold);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_ooc_spill_policy(out PlOOCSpillPolicy policy);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_ooc_spill_format(out PlOOCSpillFormat format);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_ooc_memory_budget_fraction(out double fraction);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_ooc_spill_min_bytes(out ulong bytes);
    [LibraryImport(LibName)]
    public static partial nint pl_config_get_ooc_spill_dir();
    [LibraryImport(LibName)]
    public static partial int pl_config_get_projection_pushdown_prune_strict_hconcat_inputs([MarshalAs(UnmanagedType.U1)]out bool active);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_join_sample_limit(out ulong limit);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_float_fmt(out PlFloatFormat fmt);
    [LibraryImport(LibName)]
    public static partial void pl_config_set_float_fmt(PlFloatFormat fmt);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_float_precision(out long precision);
    [LibraryImport(LibName)]
    public static partial void pl_config_set_float_precision(long precision);
    [LibraryImport(LibName)]
    public static partial nint pl_config_get_decimal_separator();
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_config_set_decimal_separator(string? separator);
    [LibraryImport(LibName)]
    public static partial nint pl_config_get_thousands_separator();
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_config_set_thousands_separator(string? separator);
    [LibraryImport(LibName)]
    public static partial int pl_config_get_trim_decimal_zeros([MarshalAs(UnmanagedType.U1)]out bool trim);
    [LibraryImport(LibName)]
    public static partial void pl_config_set_trim_decimal_zeros(
        [MarshalAs(UnmanagedType.U1)] bool active,
        [MarshalAs(UnmanagedType.U1)] bool hasValue);

}