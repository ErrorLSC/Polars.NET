use polars::prelude::*;
use polars_buffer::Buffer;
use std::ffi::CStr;
use std::os::raw::c_char;
use crate::pl_io::csv::csv_utils::apply_scan_csv_options;
use crate::pl_io::io_utils::build_cloud_options;
use crate::types::{LazyFrameContext, SchemaContext};

// ==========================================
// CSV Scan (Unified Lazy Pathway)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_csv(
    path: *const c_char,
    
    // --- 1. Core Configs ---
    has_header: bool, separator: u8, quote_char: u8, eol_char: u8,             
    ignore_errors: bool, try_parse_dates: bool, low_memory: bool,
    cache: bool, glob: bool, rechunk: bool, raise_if_empty: bool,

    // --- 2. Sizes & Threads ---
    skip_rows: usize, skip_rows_after_header: usize, skip_lines: usize,
    n_rows_ptr: *const usize, infer_schema_len_ptr: *const usize, 
    n_threads_ptr: *const usize, chunk_size: usize,

    // --- 3. Row Index & Path ---
    row_index_name: *const c_char, row_index_offset: usize, include_file_paths: *const c_char,

    // --- 4. Schema & Encoding ---
    schema_ptr: *mut SchemaContext,dtype_override_ptr: *mut SchemaContext, encoding: u8, 
    
    // --- 5. Advanced Parsing ---
    null_values_ptr: *const *const c_char, null_values_len: usize,                
    missing_is_null: bool, comment_prefix: *const c_char, decimal_comma: bool, truncate_ragged_lines: bool,
    
    // --- 6. Cloud Params ---
    cloud_provider: u8, cloud_retries: usize, cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, cloud_retry_max_backoff_ms: u64,  
    cloud_cache_ttl: u64, cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char, cloud_len: usize

) -> *mut LazyFrameContext {
    ffi_try!({
        let p_cow = unsafe { CStr::from_ptr(path).to_string_lossy() };
        let p_ref = p_cow.as_ref();

        let cloud_opts = unsafe {
            build_cloud_options(
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms,
                cloud_cache_ttl, cloud_keys, cloud_values, cloud_len
            )
        };

        let mut reader = LazyCsvReader::new(PlRefPath::new(p_ref as &str));

        reader = unsafe {
            apply_scan_csv_options(
                reader, has_header, separator, quote_char, eol_char,
                ignore_errors, try_parse_dates, low_memory, cache, glob, rechunk, raise_if_empty,
                skip_rows, skip_rows_after_header, skip_lines,
                n_rows_ptr, infer_schema_len_ptr, n_threads_ptr, chunk_size,
                row_index_name, row_index_offset, include_file_paths,
                schema_ptr,dtype_override_ptr, encoding,
                null_values_ptr, null_values_len, missing_is_null,
                comment_prefix, decimal_comma, truncate_ragged_lines,
                cloud_opts
            )
        };

        let inner = reader.finish()?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_csv_mem(
    // --- Memory Buffer Input ---
    buffer_ptr: *const u8,
    buffer_len: usize,

    // --- 1. Core Configs ---
    has_header: bool, separator: u8, quote_char: u8, eol_char: u8,             
    ignore_errors: bool, try_parse_dates: bool, low_memory: bool,
    cache: bool, glob: bool, rechunk: bool, raise_if_empty: bool, 

    // --- 2. Sizes & Threads ---
    skip_rows: usize, skip_rows_after_header: usize, skip_lines: usize,
    n_rows_ptr: *const usize, infer_schema_len_ptr: *const usize, 
    n_threads_ptr: *const usize, chunk_size: usize,

    // --- 3. Row Index & Path ---
    row_index_name: *const c_char, row_index_offset: usize, include_file_paths: *const c_char,

    // --- 4. Schema & Encoding ---
    schema_ptr: *mut SchemaContext,dtype_override_ptr: *mut SchemaContext, encoding: u8, 
    
    // --- 5. Advanced Parsing ---
    null_values_ptr: *const *const c_char, null_values_len: usize,                
    missing_is_null: bool, comment_prefix: *const c_char, decimal_comma: bool, truncate_ragged_lines: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        // Prepare Buffer Source
        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let buffer = Buffer::from(slice.to_vec()); 
        let sources = ScanSources::Buffers(Arc::new([buffer]));

        let mut reader = LazyCsvReader::new_with_sources(sources);

        reader = unsafe {
            apply_scan_csv_options(
                reader, has_header, separator, quote_char, eol_char,
                ignore_errors, try_parse_dates, low_memory, cache, glob, rechunk, raise_if_empty,
                skip_rows, skip_rows_after_header, skip_lines,
                n_rows_ptr, infer_schema_len_ptr, n_threads_ptr, chunk_size,
                row_index_name, row_index_offset, include_file_paths,
                schema_ptr,dtype_override_ptr, encoding,
                null_values_ptr, null_values_len, missing_is_null,
                comment_prefix, decimal_comma, truncate_ragged_lines,
                None // Memory buffer does not need Cloud Options
            )
        };

        let inner = reader.finish()?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}