use polars::prelude::*;
use std::os::raw::c_char;
use crate::pl_io::ipc::ipc_utils::apply_unified_scan_args;
use crate::types::{LazyFrameContext, SchemaContext};
use crate::utils::ptr_to_str;

// ==========================================
// Scan IPC (File / Cloud)
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_ipc(
    path_ptr: *const c_char,
    // --- Unified Args ---
    n_rows: *const usize,
    rechunk: bool,
    cache: bool,
    glob: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    // --- Schema & Hive ---
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

        let cloud_opts = unsafe {
            crate::pl_io::io_utils::build_cloud_options(
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms,
                cloud_cache_ttl, cloud_keys, cloud_values, cloud_len
            )
        };

        let ipc_options = IpcScanOptions::default();

        let unified_args = unsafe {
            apply_unified_scan_args(
                schema_ptr, n_rows, rechunk, cache, glob,
                row_index_name_ptr, row_index_offset,
                include_path_col_ptr, 
                hive_partitioning, hive_schema_ptr, try_parse_hive_dates,
                cloud_opts
            )?
        };

        let inner = LazyFrame::scan_ipc(
            PlRefPath::new(path), 
            ipc_options, 
            unified_args
        )?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}

// ==========================================
// Scan IPC (Memory / Buffers)
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_ipc_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
    release: Option<crate::pl_io::managed_buffer::ReleaseCallback>,
    user_data: *mut std::ffi::c_void,
    // --- Unified Args ---
    n_rows: *const usize,
    rechunk: bool,
    cache: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    // --- Schema & Hive ---
    schema_ptr: *mut SchemaContext,
    hive_partitioning: bool,
    hive_schema_ptr: *mut SchemaContext,
    try_parse_hive_dates: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        // 1. Deep Copy
        let buffer = unsafe { crate::pl_io::managed_buffer::buffer_from_ffi(buffer_ptr, buffer_len, release, user_data) };
        let sources = ScanSources::Buffers(Arc::new([buffer]));

        // 2. Options & Args
        let ipc_options = IpcScanOptions::default();
        
        let unified_args = unsafe {
            apply_unified_scan_args(
                schema_ptr, n_rows, rechunk, cache, false, // glob = false
                row_index_name_ptr, row_index_offset,
                include_path_col_ptr, 
                hive_partitioning, hive_schema_ptr, try_parse_hive_dates,
                None // No cloud options for memory
            )?
        };

        // 3. Scan
        let inner = LazyFrame::scan_ipc_sources(
            sources,
            ipc_options,
            unified_args
        )?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}