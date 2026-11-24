using System.Runtime.InteropServices;
using Polars.NET.Core.Enums;

namespace Polars.NET.Core.Native;

[StructLayout(LayoutKind.Sequential)]
public struct FfiBuffer
{
    public nuint Data;
    public nuint Length;
    public nuint Capacity;
}


unsafe internal partial class NativeBindings
{
    const string LibName = "native_shim";
    [LibraryImport(LibName)]
    public static partial void pl_free_ffi_buffer(FfiBuffer buffer);
    // Parquet
    // Credentials the host renews (see HostCredentials)
    [LibraryImport(LibName)]
    public static unsafe partial void pl_cloud_set_credential_callback(
        delegate* unmanaged[Cdecl]<ulong, byte*, nuint, nuint*, ulong*, int> callback);

    // Storage: a file-system view over an object store
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial StorageHandle pl_storage_open(
        string root_url,
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

    [LibraryImport(LibName)]
    public static partial void pl_storage_free(IntPtr ptr);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_storage_list(StorageHandle storage, string directory, string? start_from, out FfiBuffer out_buffer);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_storage_list_all(StorageHandle storage, string directory, out FfiBuffer out_buffer);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_storage_read(
        StorageHandle storage, string path, ulong offset, long length, out FfiBuffer out_buffer,
        [MarshalAs(UnmanagedType.U1)] out bool out_found);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_storage_read_tail(
        StorageHandle storage, string path, ulong length, out FfiBuffer out_buffer, out ulong out_size,
        [MarshalAs(UnmanagedType.U1)] out bool out_found);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static unsafe partial void pl_storage_put(
        StorageHandle storage, string path, byte* data, UIntPtr len,
        [MarshalAs(UnmanagedType.U1)] bool create_only,
        [MarshalAs(UnmanagedType.U1)] out bool out_created);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_storage_delete(StorageHandle storage, string path, [MarshalAs(UnmanagedType.U1)] out bool out_deleted);

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_parquet_files(
        string[] paths,
        UIntPtr paths_len,
        IntPtr n_rows, // null for None
        PlParallelStrategy parallel_code,
        [MarshalAs(UnmanagedType.U1)] bool low_memory,
        [MarshalAs(UnmanagedType.U1)] bool use_statistics,
        [MarshalAs(UnmanagedType.U1)] bool allow_missing_columns,
        [MarshalAs(UnmanagedType.U1)] bool ignore_extra_columns,
        [MarshalAs(UnmanagedType.U1)] bool lenient_casts,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        string? row_index_name,
        uint row_index_offset,
        string? include_path_col,
        IntPtr schema,
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
    public static partial LazyFrameHandle pl_scan_parquet(
        string path,
        IntPtr n_rows, // null for None
        PlParallelStrategy parallel_code,
        [MarshalAs(UnmanagedType.U1)] bool low_memory,
        [MarshalAs(UnmanagedType.U1)] bool use_statistics,
        [MarshalAs(UnmanagedType.U1)] bool glob,
        [MarshalAs(UnmanagedType.U1)] bool allow_missing_columns,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        string? row_index_name,
        uint row_index_offset,
        string? include_path_col,
        IntPtr schema,
        [MarshalAs(UnmanagedType.U1)] bool hive_partitioning,
        IntPtr hive_schema,
        [MarshalAs(UnmanagedType.U1)] bool try_parse_hive_dates,
        // Cloud Options
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
    public static partial LazyFrameHandle pl_scan_parquet_memory(
        IntPtr buffer_ptr,
        UIntPtr buffer_len,
        delegate* unmanaged[Cdecl]<void*, void> release, // null: the engine copies the buffer
        void* userData,
        IntPtr n_rows,
        PlParallelStrategy parallel_code,
        [MarshalAs(UnmanagedType.U1)] bool low_memory,
        [MarshalAs(UnmanagedType.U1)] bool use_statistics,
        [MarshalAs(UnmanagedType.U1)] bool glob,
        [MarshalAs(UnmanagedType.U1)] bool allow_missing_columns,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        string? row_index_name,
        uint row_index_offset,
        string? include_path_col,
        IntPtr schema,
        [MarshalAs(UnmanagedType.U1)] bool hive_partitioning,
        IntPtr hive_schema,
        [MarshalAs(UnmanagedType.U1)] bool try_parse_hive_dates
    );

    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazyframe_sink_parquet(
        LazyFrameHandle lf,
        string path,
        // --- ParquetWriteOptions ---
        PlParquetCompression compression,
        int compression_level,
        [MarshalAs(UnmanagedType.U1)] bool statistics,
        nuint row_group_size,
        nuint data_page_size,
        int compat_level,
        // --- UnifiedSinkArgs ---
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
        PlSyncOnClose sync_on_close,
        [MarshalAs(UnmanagedType.U1)] bool mkdir,
        PlCloudProvider cloud_provider,
        // --- Cloud Params ---
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
    public static partial void pl_lazyframe_sink_parquet_partitioned(
        LazyFrameHandle lf,
        string base_path,

        // --- Partition Params ---
        SelectorHandle partition_by,
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
    [LibraryImport(LibName)]
    public static partial void pl_lazyframe_sink_parquet_memory(
        LazyFrameHandle lf,
        out FfiBuffer out_buffer,

        // --- Parquet Options ---
        PlParquetCompression compression,
        int compression_level,
        [MarshalAs(UnmanagedType.U1)] bool statistics,
        nuint row_group_size,
        nuint data_page_size,
        int compat_level,

        // --- Unified Options ---
        [MarshalAs(UnmanagedType.U1)] bool maintain_order
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazyframe_sink_parquet_host_paths(
        LazyFrameHandle lf,
        string base_path,
        string[] key_names,
        nuint key_len,
        [MarshalAs(UnmanagedType.U1)] bool keys_pre_grouped,
        nuint max_rows_per_file,
        ulong approx_bytes_per_file,
        nint path_callback,
        nint path_user,
        PlParquetCompression compression,
        int compression_level,
        nuint row_group_size,
        nuint data_page_size,
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
        PlSyncOnClose sync_on_close,
        PlCloudProvider cloud_provider,
        nuint cloud_retries,
        ulong cloud_retry_timeout_ms,
        ulong cloud_retry_init_backoff_ms,
        ulong cloud_retry_max_backoff_ms,
        ulong cloud_cache_ttl,
        string[]? cloud_keys,
        string[]? cloud_values,
        nuint cloud_len,
        string? field_ids
    );
    [LibraryImport(LibName)]
    public static partial void pl_lazyframe_sink_parquet_callback(
        LazyFrameHandle lf,
        nint callback,
        nint user_data,
        nuint buffer_size,

        // --- Parquet Options ---
        PlParquetCompression compression,
        int compression_level,
        [MarshalAs(UnmanagedType.U1)] bool statistics,
        nuint row_group_size,
        nuint data_page_size,
        int compat_level,

        // --- Unified Options ---
        [MarshalAs(UnmanagedType.U1)] bool maintain_order
    );
    // IPC
    // ---------------------------------------------------------
    // Scan IPC (File / Cloud)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_ipc(
        string path,

        // --- Unified Args ---
        IntPtr nRows,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        [MarshalAs(UnmanagedType.U1)] bool glob,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,

        // --- Schema & Hive ---
        IntPtr schema,
        [MarshalAs(UnmanagedType.U1)] bool hivePartitioning,
        IntPtr hiveSchema,
        [MarshalAs(UnmanagedType.U1)] bool tryParseHiveDates,

        // --- Cloud Params ---
        PlCloudProvider cloudProvider,
        nuint cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues,
        nuint cloudLen
    );

    // ---------------------------------------------------------
    // Scan IPC (Memory)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_ipc_memory(
        byte* buffer,
        nuint bufferLen,
        delegate* unmanaged[Cdecl]<void*, void> release, // null: the engine copies the buffer
        void* userData,

        // --- Unified Args ---
        nint nRows,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,

        // --- Schema & Hive ---
        nint schema,
        [MarshalAs(UnmanagedType.U1)] bool hivePartitioning,
        nint hiveSchema,
        [MarshalAs(UnmanagedType.U1)] bool tryParseHiveDates
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_ipc_stream(
        string path,
        string[]? columns,
        nuint columnsLen,
        nuint[]? projection,
        nuint projectionLen,
        nint nRows,
        string? rowIndexName,
        uint rowIndexOffset,
        [MarshalAs(UnmanagedType.U1)] bool rechunk
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_ipc_stream_memory(
        ReadOnlySpan<byte> buffer,
        nuint bufferLen,
        string[]? columns,
        nuint columnsLen,
        nuint[]? projection,
        nuint projectionLen,
        nint nRows,
        string? rowIndexName,
        uint rowIndexOffset,
        [MarshalAs(UnmanagedType.U1)] bool rechunk
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial SchemaHandle pl_read_ipc_stream_schema(string path);

    [LibraryImport(LibName)]
    public static partial SchemaHandle pl_read_ipc_stream_schema_memory(
        ReadOnlySpan<byte> buffer,
        nuint bufferLen
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_dataframe_write_ipc_stream(
        DataFrameHandle df,
        string path,
        PlIpcCompression compression,
        int compatLevel
    );
    [LibraryImport(LibName)]
    public static partial void pl_dataframe_write_ipc_stream_memory(
        DataFrameHandle df,
        out FfiBuffer out_buffer,
        PlIpcCompression compression,
        int compatLevel
    );
    [LibraryImport(LibName)]
    public static partial void pl_dataframe_write_ipc_stream_callback(
        DataFrameHandle df,
        nint callback,
        nint user_data,
        nuint buffer_size,
        PlIpcCompression compression,
        int compatLevel
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazyframe_sink_ipc(
        LazyFrameHandle lf,
        string path,

        // --- IpcWriterOptions params ---
        PlIpcCompression compression,
        int compat_level,
        nuint record_batch_size,
        [MarshalAs(UnmanagedType.U1)] bool record_batch_statistics,

        // --- UnifiedSinkArgs params ---
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
        PlSyncOnClose sync_on_close,
        [MarshalAs(UnmanagedType.U1)] bool mkdir,

        // --- Cloud Options ---
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
    public static partial void pl_sink_ipc_partitioned(
        LazyFrameHandle lf,
        string path,
        // --- Partition Params ---
        SelectorHandle partition_by,
        [MarshalAs(UnmanagedType.U1)] bool include_keys,
        [MarshalAs(UnmanagedType.U1)] bool keys_pre_grouped,
        nuint max_rows_per_file,
        ulong approx_bytes_per_file,
        // --- IpcWriterOptions params ---
        PlIpcCompression compression,
        int compat_level,
        nuint record_batch_size,
        [MarshalAs(UnmanagedType.U1)] bool record_batch_statistics,

        // --- UnifiedSinkArgs params ---
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
        PlSyncOnClose sync_on_close,
        [MarshalAs(UnmanagedType.U1)] bool mkdir,

        // --- Cloud Options ---
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
    [LibraryImport(LibName)]
    public static partial void pl_lazyframe_sink_ipc_memory(
        LazyFrameHandle lf,
        out FfiBuffer out_buffer,

        // --- IpcWriterOptions params ---
        PlIpcCompression compression,
        int compat_level,
        nuint record_batch_size,
        [MarshalAs(UnmanagedType.U1)] bool record_batch_statistics,

        // --- UnifiedSinkArgs params ---
        [MarshalAs(UnmanagedType.U1)] bool maintain_order
    );
    [LibraryImport(LibName)]
    public static partial void pl_lazyframe_sink_ipc_callback(
        LazyFrameHandle lf,
        nint callback,
        nint user_data,
        nuint buffer_size,

        // --- IpcWriterOptions params ---
        PlIpcCompression compression,
        int compat_level,
        nuint record_batch_size,
        [MarshalAs(UnmanagedType.U1)] bool record_batch_statistics,

        // --- UnifiedSinkArgs params ---
        [MarshalAs(UnmanagedType.U1)] bool maintain_order
    );

}