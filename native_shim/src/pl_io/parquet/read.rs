use polars::prelude::*;
use polars_buffer::Buffer;
use std::os::raw::c_char;
use crate::pl_io::parquet::parquet_utils::build_scan_args;
use crate::types::{ LazyFrameContext, SchemaContext};
use crate::utils::ptr_to_str;

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_parquet(
    path_ptr: *const c_char,
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    glob: bool,
    allow_missing_columns: bool,
    rechunk: bool, 
    cache: bool,   
    // --- Optional Names ---
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    // --- Schema ---
    schema_ptr: *mut SchemaContext,
    hive_partitioning: bool,
    hive_schema_ptr: *mut SchemaContext,
    try_parse_hive_dates: bool,
    // --- Cloud Options ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, 
    cloud_retry_max_backoff_ms: u64,  
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics, 
            glob, allow_missing_columns, 
            row_index_name_ptr, row_index_offset, include_path_col_ptr,
            schema_ptr,hive_partitioning, hive_schema_ptr, try_parse_hive_dates,
            rechunk, cache,
            cloud_provider, cloud_retries,cloud_retry_timeout_ms,      
            cloud_retry_init_backoff_ms, 
            cloud_retry_max_backoff_ms,cloud_cache_ttl, 
            cloud_keys, cloud_values, cloud_len
        );

        let lf = LazyFrame::scan_parquet(PlRefPath::new(path), args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner:lf })))
    })
}

// ---------------------------------------------------------
// Scan Parquet (Memory / Buffers)
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_parquet_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    glob: bool,
    allow_missing_columns: bool,
    rechunk: bool, 
    cache: bool,   
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    schema_ptr: *mut SchemaContext,
    hive_partitioning: bool,
    hive_schema_ptr: *mut SchemaContext,
    try_parse_hive_dates: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let buffer = Buffer::from(slice.to_vec()); 
        let sources = ScanSources::Buffers(Arc::new([buffer]));

        let args = build_scan_args(
            n_rows, 
            parallel_code, 
            low_memory, 
            use_statistics, 
            glob, 
            allow_missing_columns, 
            row_index_name_ptr, 
            row_index_offset, 
            include_path_col_ptr,
            schema_ptr, 
            hive_partitioning,
            hive_schema_ptr, 
            try_parse_hive_dates,
            rechunk,  
            cache,    
            // --- Cloud Options  ---
            0,                // provider_code = None
            0,                // retries
            0,
            0,
            0,
            0,                // cache_ttl
            std::ptr::null(), // keys_ptr
            std::ptr::null(), // vals_ptr
            0                 // len
        );

        let lf = LazyFrame::scan_parquet_sources(sources, args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner:lf })))
    })
}
