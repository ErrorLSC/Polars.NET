using System.Runtime.InteropServices;

namespace Polars.NET.Core.Native;

[StructLayout(LayoutKind.Sequential)]
public struct FfiBuffer
{
    public IntPtr Data;
    public nuint Length;
    public nuint Capacity;
}


unsafe internal partial class NativeBindings
{
    const string LibName = "native_shim";
    [LibraryImport(LibName)]
    public static partial void pl_free_ffi_buffer(FfiBuffer buffer);
    // CSV
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_csv(
        string path,
        
        // --- 1. Core Configs ---
        [MarshalAs(UnmanagedType.U1)] bool has_header,
        byte separator,
        byte quote_char,       
        byte eol_char,         
        [MarshalAs(UnmanagedType.U1)] bool ignore_errors,
        [MarshalAs(UnmanagedType.U1)] bool try_parse_dates,
        [MarshalAs(UnmanagedType.U1)] bool low_memory,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        [MarshalAs(UnmanagedType.U1)] bool glob,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool raise_if_empty,

        // --- 2. Sizes & Threads ---
        nuint skip_rows,
        nuint skip_rows_after_header,
        nuint skip_lines,
        IntPtr n_rows_ptr,
        IntPtr infer_schema_len_ptr,
        IntPtr n_threads_ptr,
        nuint chunk_size,

        // --- 3. Row Index & Path ---
        string? row_index_name,
        nuint row_index_offset,
        string? include_file_paths,

        // --- 4. Schema & Encoding ---
        SchemaHandle schema,
        SchemaHandle dtype_overwrite,
        PlCsvEncoding encoding,

        // --- 5. Advanced Parsing ---
        string[]? null_values, 
        nuint null_values_len,
        [MarshalAs(UnmanagedType.U1)] bool missing_is_null,
        string? comment_prefix,
        [MarshalAs(UnmanagedType.U1)] bool decimal_comma,
        [MarshalAs(UnmanagedType.U1)] bool truncate_ragged_lines,
        
        // --- 6. Cloud Params ---
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
    public static partial LazyFrameHandle pl_scan_csv_mem(
        byte* bufferPtr,        // Buffer Pointer
        UIntPtr bufferLen,      // Buffer Length
        
        // --- 1. Core Configs ---
        [MarshalAs(UnmanagedType.U1)] bool has_header,
        byte separator,
        byte quote_char,       
        byte eol_char,         
        [MarshalAs(UnmanagedType.U1)] bool ignore_errors,
        [MarshalAs(UnmanagedType.U1)] bool try_parse_dates,
        [MarshalAs(UnmanagedType.U1)] bool low_memory,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        [MarshalAs(UnmanagedType.U1)] bool glob,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool raise_if_empty,

        // --- 2. Sizes & Threads ---
        nuint skip_rows,
        nuint skip_rows_after_header,
        nuint skip_lines,
        IntPtr n_rows_ptr,
        IntPtr infer_schema_len_ptr,
        IntPtr n_threads_ptr,
        nuint chunk_size,

        // --- 3. Row Index & Path ---
        string? row_index_name,
        nuint row_index_offset,
        string? include_file_paths,

        // --- 4. Schema & Encoding ---
        SchemaHandle schema,
        SchemaHandle dtype_override,
        PlCsvEncoding encoding,

        // --- 5. Advanced Parsing ---
        string[]? null_values, 
        nuint null_values_len,
        [MarshalAs(UnmanagedType.U1)] bool missing_is_null,
        string? comment_prefix,
        [MarshalAs(UnmanagedType.U1)] bool decimal_comma,
        [MarshalAs(UnmanagedType.U1)] bool truncate_ragged_lines
    );
 
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazyframe_sink_csv(
        LazyFrameHandle lf,
        string path,

        // --- CSV Writer Options ---
        [MarshalAs(UnmanagedType.U1)] bool include_bom,
        [MarshalAs(UnmanagedType.U1)] bool include_header,
        nuint batch_size,
        [MarshalAs(UnmanagedType.U1)] bool check_extension, 

        // --- Compression ---
        PlExternalCompression compression_code, 
        int compression_level, 

        // --- SerializeOptions  ---
        string? date_format,
        string? time_format,
        string? datetime_format,
        int float_scientific, // -1: None, 0: False, 1: True
        int float_precision,  // -1: None, >=0: value
        [MarshalAs(UnmanagedType.U1)] bool decimal_comma,
        byte separator,
        byte quote_char,
        string? null_value,
        string? line_terminator,
        PlQuoteStyle quote_style, 

        // --- UnifiedSinkArgs ---
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
    public static partial void pl_lazyframe_sink_csv_partitioned(
        LazyFrameHandle lf,
        string path,
        // --- Partition Params ---
        SelectorHandle partition_by,
        [MarshalAs(UnmanagedType.U1)] bool include_keys,
        [MarshalAs(UnmanagedType.U1)] bool keys_pre_grouped,
        nuint max_rows_per_file,
        ulong approx_bytes_per_file,
        // --- CSV Writer Options ---
        [MarshalAs(UnmanagedType.U1)] bool include_bom,
        [MarshalAs(UnmanagedType.U1)] bool include_header,
        nuint batch_size,
        [MarshalAs(UnmanagedType.U1)] bool check_extension, 

        // --- Compression ---
        PlExternalCompression compression_code, 
        int compression_level, 

        // --- SerializeOptions  ---
        string? date_format,
        string? time_format,
        string? datetime_format,
        int float_scientific, // -1: None, 0: False, 1: True
        int float_precision,  // -1: None, >=0: value
        [MarshalAs(UnmanagedType.U1)] bool decimal_comma,
        byte separator,
        byte quote_char,
        string? null_value,
        string? line_terminator,
        PlQuoteStyle quote_style, 

        // --- UnifiedSinkArgs ---
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
    public static partial void pl_lazyframe_sink_csv_memory(
        LazyFrameHandle lf,
        out FfiBuffer out_buffer,
        // --- CSV Specific Params ---
        [MarshalAs(UnmanagedType.U1)] bool include_bom,
        [MarshalAs(UnmanagedType.U1)] bool include_header,
        nuint batch_size,
        [MarshalAs(UnmanagedType.U1)] bool check_extension,
        // Compression
        PlExternalCompression compression_code,
        int compression_level,
        // Serialize Options
        string? date_format,
        string? time_format,
        string? datetime_format,
        int float_scientific,
        int float_precision,
        [MarshalAs(UnmanagedType.U1)] bool decimal_comma,
        byte separator,
        byte quote_char,
        string? null_value,
        string? line_terminator,
        PlQuoteStyle quote_style,
        // --- Unified Options ---
        [MarshalAs(UnmanagedType.U1)] bool maintain_order
    );
    // Parquet
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
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
    [LibraryImport(LibName,StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_parquet_memory(
        IntPtr buffer_ptr,
        UIntPtr buffer_len,
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
    // ---------------------------------------------------------
    // Read JSON (File)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_json(
        string path,
        // columns (Option<Vec<String>>)
        string[]? columns, UIntPtr columnsLen,
        // schema (Option<Schema>)
        IntPtr schema, 
        // infer_schema_len (Option<usize>) -> pointer to usize
        IntPtr inferSchemaLen, 
        // batch_size (Option<usize>) -> pointer to usize
        IntPtr batchSize,
        // ignore_errors (bool)
        [MarshalAs(UnmanagedType.U1)] bool ignoreErrors,
        // format (u8 enum)
        PlJsonFormat jsonFormat
    );

    // ---------------------------------------------------------
    // Read JSON (Memory)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_json_memory(
        IntPtr buffer, UIntPtr bufferLen,
        string[]? columns, UIntPtr columnsLen,
        IntPtr schema,
        IntPtr inferSchemaLen,
        IntPtr batchSize,
        [MarshalAs(UnmanagedType.U1)] bool ignoreErrors,
        PlJsonFormat jsonFormat
    );
    // ---------------------------------------------------------
    // Scan NDJSON (File)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_ndjson(
        string path,
        // Options
        IntPtr batchSize,           // *const usize
        [MarshalAs(UnmanagedType.U1)] bool lowMemory,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        IntPtr schema,              // *mut SchemaContext (Overwrite)
        IntPtr inferSchemaLen,      // *const usize
        IntPtr nRows,               // *const usize
        [MarshalAs(UnmanagedType.U1)] bool ignoreErrors,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        // --- Cloud Params ---
        PlCloudProvider cloud_provider,         // u8
        nuint cloud_retries,                    // usize
        ulong cloud_retry_timeout_ms,           // u64
        ulong cloud_retry_init_backoff_ms,      // u64
        ulong cloud_retry_max_backoff_ms,       // u64
        ulong cloud_cache_ttl,                  // u64
        string[]? cloud_keys,
        string[]? cloud_values,
        nuint cloud_len
    );

    // ---------------------------------------------------------
    // Scan NDJSON (Memory)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial LazyFrameHandle pl_scan_ndjson_memory(
        IntPtr buffer, UIntPtr bufferLen,
        // Options (Same as above)
        IntPtr batchSize,
        [MarshalAs(UnmanagedType.U1)] bool lowMemory,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        IntPtr schema,
        IntPtr inferSchemaLen,
        IntPtr nRows,
        [MarshalAs(UnmanagedType.U1)] bool ignoreErrors,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn
    );
    // -----------------------------------------------------
    // Write & Sink JSON
    // -----------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_dataframe_write_json(
        DataFrameHandle df, 
        string path,
        PlJsonFormat jsonFormat
    );
    [LibraryImport(LibName)]
    public static partial void pl_dataframe_write_json_memory(
        DataFrameHandle df, 
        out FfiBuffer out_buffer,
        PlJsonFormat jsonFormat
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazyframe_sink_json(
        LazyFrameHandle lf,
        string path,

        // --- NDJson Params ---
        PlExternalCompression compression_code, // u8
        int compression_level,                  // i32
        [MarshalAs(UnmanagedType.U1)] bool check_extension, // bool

        // --- UnifiedSinkArgs ---
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
        PlSyncOnClose sync_on_close,            // u8
        [MarshalAs(UnmanagedType.U1)] bool mkdir,

        // --- Cloud Params ---
        PlCloudProvider cloud_provider,         // u8
        nuint cloud_retries,                    // usize
        ulong cloud_retry_timeout_ms,           // u64
        ulong cloud_retry_init_backoff_ms,      // u64
        ulong cloud_retry_max_backoff_ms,       // u64
        ulong cloud_cache_ttl,                  // u64
        string[]? cloud_keys,
        string[]? cloud_values,
        nuint cloud_len
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazyframe_sink_json_partitioned(
        LazyFrameHandle lf,
        string path,
        // --- Partition Params ---
        SelectorHandle partition_by,
        [MarshalAs(UnmanagedType.U1)] bool include_keys,
        [MarshalAs(UnmanagedType.U1)] bool keys_pre_grouped,
        nuint max_rows_per_file,
        ulong approx_bytes_per_file,
        // --- NDJson Params ---
        PlExternalCompression compression_code, // u8
        int compression_level,                  // i32
        [MarshalAs(UnmanagedType.U1)] bool check_extension, // bool

        // --- UnifiedSinkArgs ---
        [MarshalAs(UnmanagedType.U1)] bool maintain_order,
        PlSyncOnClose sync_on_close,            // u8
        [MarshalAs(UnmanagedType.U1)] bool mkdir,

        // --- Cloud Params ---
        PlCloudProvider cloud_provider,         // u8
        nuint cloud_retries,                    // usize
        ulong cloud_retry_timeout_ms,           // u64
        ulong cloud_retry_init_backoff_ms,      // u64
        ulong cloud_retry_max_backoff_ms,       // u64
        ulong cloud_cache_ttl,                  // u64
        string[]? cloud_keys,
        string[]? cloud_values,
        nuint cloud_len
    );
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_lazyframe_sink_json_memory(
        LazyFrameHandle lf,
        out FfiBuffer out_buffer,

        // --- NDJson Params ---
        PlExternalCompression compression_code, // u8
        int compression_level,                  // i32
        [MarshalAs(UnmanagedType.U1)] bool check_extension, // bool

        // --- UnifiedSinkArgs ---
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
        UIntPtr bufferLen,
        
        // --- Unified Args ---
        IntPtr nRows,
        [MarshalAs(UnmanagedType.U1)] bool rechunk,
        [MarshalAs(UnmanagedType.U1)] bool cache,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        
        // --- Schema & Hive ---
        IntPtr schema,
        [MarshalAs(UnmanagedType.U1)] bool hivePartitioning,
        IntPtr hiveSchema,
        [MarshalAs(UnmanagedType.U1)] bool tryParseHiveDates
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
    public static partial void pl_lazyframe_sink_ipc_partitioned(
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
    // ---------------------------------------------------------
    // Read Excel (Calamine Engine)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_excel(
        string path,
        string? sheetName,
        UIntPtr sheetIndex,
        IntPtr schema,               // *mut SchemaContext
        [MarshalAs(UnmanagedType.U1)] bool hasHeader,
        UIntPtr inferSchemaLen,      // usize
        [MarshalAs(UnmanagedType.U1)] bool dropEmptyRows,
        [MarshalAs(UnmanagedType.U1)] bool raiseIfEmpty
    );

    // ---------------------------------------------------------
    // Write Excel (Native Rust Engine)
    // ---------------------------------------------------------
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_write_excel(
        DataFrameHandle df,
        string path,
        string? sheetName,       // Option<&str>
        string? dateFormat,      // Option<&str>
        string? datetimeFormat   // Option<&str>
    );

    // Avro
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial DataFrameHandle pl_read_avro(
        string path,
        IntPtr nRowsPtr,
        string[]? columns,
        nuint columnsLen,
        nuint[]? projection,
        nuint projectionLen
    );
    // Avro - Memory Buffer
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static unsafe partial DataFrameHandle pl_read_avro_mem(
        byte* bufferPtr,    
        nuint bufferLen,
        IntPtr nRowsPtr,
        string[]? columns,
        nuint columnsLen,
        nuint[]? projection,
        nuint projectionLen
    );
    // Write Avro
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_write_avro(
        DataFrameHandle df, 
        string path, 
        PlAvroCompression compressionType, 
        string name
    );
    // Write Avro Buffer
    [LibraryImport(LibName, StringMarshalling = StringMarshalling.Utf8)]
    public static partial void pl_write_avro_mem(
        DataFrameHandle df,
        out FfiBuffer out_buffer,
        PlAvroCompression compressionType,
        string name
    );

}