use polars::prelude::*;
use std::ffi::CStr;
use std::io::{BufReader,Cursor};
use std::num::NonZeroUsize;
use std::os::raw::c_char;
use std::fs::File;
use crate::pl_io::io_utils::build_cloud_options;
use crate::pl_io::json::json_utils::{apply_json_options, apply_scan_ndjson_options};
use crate::types::{DataFrameContext, LazyFrameContext, SchemaContext};
use crate::utils::{ptr_to_schema_ref, ptr_to_str};

// ---------------------------------------------------------
// 1. Read JSON (File)
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_json(
    path_ptr: *const c_char,
    columns_ptr: *const *const c_char, columns_len: usize,
    schema_ptr: *mut SchemaContext,
    infer_schema_len: *const usize,
    batch_size: *const usize,
    ignore_errors: bool,
    format_code: u8
) -> *mut DataFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::open(path)
            .map_err(|e| PolarsError::ComputeError(format!("File not found: {}", e).into()))?;
        let schema_holder = unsafe { ptr_to_schema_ref(schema_ptr) };
        let schema_ref = schema_holder.as_deref();
        let reader = BufReader::new(file);
        
        let json_reader = JsonReader::new(reader);
        let configured_reader = unsafe { 
            apply_json_options(
                json_reader, 
                columns_ptr, columns_len, 
                schema_ref, infer_schema_len, batch_size, 
                ignore_errors, format_code
            )? 
        };

        let df = configured_reader.finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

// ---------------------------------------------------------
// 2. Read JSON (Memory)
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_json_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
    columns_ptr: *const *const c_char, columns_len: usize,
    schema_ptr: *mut SchemaContext,
    infer_schema_len: *const usize,
    batch_size: *const usize,
    ignore_errors: bool,
    format_code: u8
) -> *mut DataFrameContext {
    ffi_try!({
        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let cursor = Cursor::new(slice);

        let schema_holder = unsafe { ptr_to_schema_ref(schema_ptr) };
        let schema_ref = schema_holder.as_deref();
        
        let json_reader = JsonReader::new(cursor);
        let configured_reader = unsafe { 
            apply_json_options(
                json_reader, 
                columns_ptr, columns_len, 
                schema_ref, infer_schema_len, batch_size, 
                ignore_errors, format_code
            )? 
        };

        let df = configured_reader.finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_ndjson(
    path_ptr: *const c_char,
    // --- NDJSON Specific Options ---
    batch_size: *const usize,
    low_memory: bool,
    rechunk: bool,
    schema_ptr: *mut SchemaContext,
    infer_schema_len: *const usize,
    
    // --- Unified Args ---
    n_rows: *const usize,
    ignore_errors: bool,
    // cache: bool,
    // glob: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    // --- Schema & Hive ---
    
    // hive_partitioning: bool,
    // hive_schema_ptr: *mut SchemaContext,
    // try_parse_hive_dates: bool,
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
            build_cloud_options(
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms,
                cloud_cache_ttl, cloud_keys, cloud_values, cloud_len
            )
        };

        let mut reader = LazyJsonLineReader::new(PlRefPath::new(path))
            .low_memory(low_memory)
            .with_ignore_errors(ignore_errors)
            .with_rechunk(rechunk)
            .with_cloud_options(cloud_opts); 

        // --- NDJSON Options ---
        if !batch_size.is_null() {
            let val = unsafe { *batch_size };
            if val > 0 {
                reader = reader.with_batch_size(NonZeroUsize::new(val));
            }
        }
        
        // Null (and 0) mean the whole input; the C# default is 100.
        reader = reader.with_infer_schema_length(
            unsafe { crate::pl_io::io_utils::infer_schema_length(infer_schema_len) }.and_then(NonZeroUsize::new));

        // --- Unified Args ---
        if !n_rows.is_null() {
            reader = reader.with_n_rows(Some(unsafe { *n_rows }));
        }

        // --- Schema ---
        if !schema_ptr.is_null() {
            let schema = unsafe { &*schema_ptr }.schema.clone();
            reader = reader.with_schema_overwrite(Some(schema));
        }

        // --- Row Index ---
        if !row_index_name_ptr.is_null() {
            let name = unsafe { CStr::from_ptr(row_index_name_ptr).to_string_lossy().into_owned() };
            reader = reader.with_row_index(Some(RowIndex {
                name: name.into(),
                offset: row_index_offset,
            }));
        }

        // --- Include Path Column ---
        if !include_path_col_ptr.is_null() {
            let name = unsafe { CStr::from_ptr(include_path_col_ptr).to_string_lossy().into_owned() };
            reader = reader.with_include_file_paths(Some(name.into()));
        }

        let inner = reader.finish()?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner }))) 
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_ndjson_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
    release: Option<crate::pl_io::managed_buffer::ReleaseCallback>,
    user_data: *mut std::ffi::c_void,
    // --- Options ---
    batch_size: *const usize,
    low_memory: bool,
    rechunk: bool,
    schema_ptr: *mut SchemaContext,
    infer_schema_len: *const usize,
    n_rows: *const usize,
    ignore_errors: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
) -> *mut LazyFrameContext {
    ffi_try!({

        let buffer = unsafe { crate::pl_io::managed_buffer::buffer_from_ffi(buffer_ptr, buffer_len, release, user_data) };
        let sources = ScanSources::Buffers(Arc::new([buffer]));

        let reader = LazyJsonLineReader::new_with_sources(sources);

        let configured_reader = unsafe {
            apply_scan_ndjson_options(
                reader,
                batch_size, low_memory, rechunk, schema_ptr, 
                infer_schema_len, n_rows, ignore_errors, 
                row_index_name_ptr, row_index_offset, include_path_col_ptr
            )?
        };

        let inner = configured_reader.finish()?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}