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
}