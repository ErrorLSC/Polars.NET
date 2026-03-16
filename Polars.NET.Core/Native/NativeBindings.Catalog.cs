using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

internal partial class NativeBindings
{
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    internal static partial CatalogHandle pl_catalog_unity_new(string workspaceUrl, string bearerToken);

    [LibraryImport(LibName)]
    internal static partial void pl_catalog_unity_free(IntPtr ptr);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_catalog_table(
        CatalogHandle handle,
        // --- catalog info ---
        string catalogName,
        string schemaName,
        string tableName,
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
    public static partial void pl_sink_catalog_table(
        CatalogHandle handle,
        // --- catalog info ---
        string catalogName,
        string schemaName,
        string tableName,
        LazyFrameHandle lf,
        // --- Delta Options --- 
        PlDeltaSaveMode mode,
        [MarshalAs(UnmanagedType.U1)] bool can_evolve,
        // --- Partition Params---
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
    public static partial void pl_catalog_delete_records(
        CatalogHandle handle,
        // --- catalog info ---
        string catalogName,
        string schemaName,
        string tableName,
        ExprHandle predicate,

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
    public static partial void pl_catalog_create_table(
        CatalogHandle handle,
        // --- catalog info ---
        string catalogName,
        string schemaName,
        string tableName,
        // --- schema ---
        SchemaHandle schema,
        PlCatalogTableType table_type,
        string? storage_location
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_catalog_delete_table(
        CatalogHandle handle,
        // --- catalog info ---
        string catalogName,
        string schemaName,
        string tableName
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_catalog_merge_ordered(
        CatalogHandle handle,
        // --- catalog info ---
        string catalogName,
        string schemaName,
        string tableName,
        LazyFrameHandle source_lf,
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
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_catalog_optimize(
        CatalogHandle handle,
        // --- catalog info ---
        string catalogName,
        string schemaName,
        string tableName,
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
}