using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

internal partial class NativeBindings
{
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_vacuum(
        string path,
        int retentionHours,
        [MarshalAs(UnmanagedType.U1)] bool enforceRetention,
        [MarshalAs(UnmanagedType.U1)] bool dryRun,
        [MarshalAs(UnmanagedType.U1)] bool vacuumModeFull,
        // Cloud Args (Simplified)
        string[]? keys,
        string[]? values,
        nuint len,
        out nuint filesDeleted
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_restore(
        string path,
        long targetVersion,
        long targetTimestampMs,
        [MarshalAs(UnmanagedType.U1)] bool ignoreMissingFiles,
        [MarshalAs(UnmanagedType.U1)] bool protocolDowngradeAllowed,
        
        // Cloud Options
        string[]? keys,
        string[]? values,
        nuint len,
        
        // Output
        out long newVersion
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_history(
        string path,
        nuint limit,
        // Cloud Options
        string[]? keys,
        string[]? values,
        nuint len,
        // Output: ptr to json
        out IntPtr jsonPtr
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_optimize(
        string path,
        long target_size_mb,
        string? filter_json,
        // Z-Order
        string[]? z_order_cols,
        nuint z_order_len,
        // Cloud Options
        PlCloudProvider cloud_provider,
        UIntPtr cloud_retries,
        ulong cloud_retry_timeout_ms,
        ulong cloud_retry_init_backoff_ms,
        ulong cloud_retry_max_backoff_ms,
        ulong cloud_cache_ttl,
        string[]? keys,
        string[]? values,
        nuint cloud_len,

        out nuint optimized_files
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_add_feature(
        string path,
        string feature_name,
        [MarshalAs(UnmanagedType.U1)] bool allowProtocolIncrease,
        string[]? keys,
        string[]? values,
        nuint cloud_len
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_io_delta_set_table_properties(
        string path,
        string[] props_keys,
        string[] props_values,
        nuint props_len,
        [MarshalAs(UnmanagedType.U1)] bool raise_if_not_exists,
        string[]? keys,
        string[]? values,
        nuint cloud_len
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_delta(
        string path,
        // --- Time Travel ---
        IntPtr version,
        string? datetime,
        // --- Scan Args ---
        IntPtr n_rows, // null for None
        PlParallelStrategy parallel_code,
        [MarshalAs(UnmanagedType.I1)] bool low_memory,
        [MarshalAs(UnmanagedType.I1)] bool use_statistics,
        [MarshalAs(UnmanagedType.I1)] bool glob,
        [MarshalAs(UnmanagedType.I1)] bool rechunk, 
        [MarshalAs(UnmanagedType.I1)] bool cache,   
        // --- Option Names ---
        string? row_index_name,
        uint row_index_offset,
        string? include_path_col,
        // --- Schema ---
        IntPtr schema,
        [MarshalAs(UnmanagedType.I1)] bool hive_partitioning,
        IntPtr hive_schema,
        [MarshalAs(UnmanagedType.I1)] bool try_parse_hive_dates,
        // --- Cloud Options (Must match pl_scan_parquet pattern) ---
        PlCloudProvider cloud_provider,
        UIntPtr cloud_retries,
        ulong cloud_retry_timeout_ms,
        ulong cloud_retry_init_backoff_ms,
        ulong cloud_retry_max_backoff_ms,
        ulong cloud_cache_ttl,
        string[]? cloud_keys,
        string[]? cloud_values,
        UIntPtr cloud_len
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_sink_delta(
        LazyFrameHandle lf,
        string base_path,
        // --- Delta Options --- 
        PlDeltaSaveMode mode,
        [MarshalAs(UnmanagedType.U1)] bool can_evolve,
        // --- Partition Params ---
        IntPtr partition_by,
        [MarshalAs(UnmanagedType.U1)] bool include_keys,
        [MarshalAs(UnmanagedType.U1)] bool keys_pre_grouped,
        nuint max_rows_per_file,
        ulong approx_bytes_per_file,

        // --- Parquet Options ---
        PlParquetCompression compression,
        int compression_level,
        [MarshalAs(UnmanagedType.U1)] bool statistics,
        nuint row_group_size,
        nuint data_page_size,
        int compat_level,
    
        // --- Unified Options ---
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
        PlSyncOnClose sync_on_close,
        [MarshalAs(UnmanagedType.U1)] bool mkdir,

        // --- Cloud Params ---
        PlCloudProvider cloud_provider,
        nuint cloud_retries,
        ulong cloud_retry_timeout_ms,
        ulong cloud_retry_init_backoff_ms,
        ulong cloud_retry_max_backoff_ms,
        ulong cloud_cache_ttl,
        string[]? cloud_keys,
        string[]? cloud_values,
        nuint cloud_len
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial void pl_io_delta_delete(
        string path,
        ExprHandle predicate,
        // Cloud Options
        PlCloudProvider cloud_provider,
        UIntPtr cloud_retries,
        ulong cloud_retry_timeout_ms,
        ulong cloud_retry_init_backoff_ms,
        ulong cloud_retry_max_backoff_ms,
        ulong cloud_cache_ttl,
        string[]? cloud_keys,
        string[]? cloud_values,
        nuint cloud_len
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial void pl_io_delta_merge(
        LazyFrameHandle source_lf,
        string path,
        string[] merge_key,
        nuint merge_key_len,
        IntPtr matched_update_cond,
        IntPtr matched_delete_cond,
        IntPtr not_matched_insert_cond,
        IntPtr not_matched_by_source_deleted_cond,
        [MarshalAs(UnmanagedType.U1)] bool can_evolve,
        // Cloud Options
        PlCloudProvider cloud_provider,
        UIntPtr cloud_retries,
        ulong cloud_retry_timeout_ms,
        ulong cloud_retry_init_backoff_ms,
        ulong cloud_retry_max_backoff_ms,
        ulong cloud_cache_ttl,
        string[]? cloud_keys,
        string[]? cloud_values,
        nuint cloud_len
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial void pl_io_delta_merge_ordered(
        LazyFrameHandle source_lf,
        string path,
        string[] merge_key,
        nuint merge_key_len,

        PlMergeActionType[] action_types,    
        IntPtr[] action_exprs,    
        nuint actions_count,      

        [MarshalAs(UnmanagedType.U1)] bool can_evolve,
        
        // --- Cloud Options ---
        PlCloudProvider cloud_provider,
        UIntPtr cloud_retries,
        ulong cloud_retry_timeout_ms,
        ulong cloud_retry_init_backoff_ms,
        ulong cloud_retry_max_backoff_ms,
        ulong cloud_cache_ttl,
        string[]? cloud_keys,
        string[]? cloud_values,
        nuint cloud_len
    );
}