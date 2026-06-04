use polars::prelude::*;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::fs::File;
use crate::pl_io::ffi_buffer::FfiBuffer;
use crate::pl_io::io_utils::{build_memory_sink_destination, build_unified_sink_args};
use crate::pl_io::json::json_utils::build_ndjson_writer_options;
use crate::types::{DataFrameContext, LazyFrameContext, SelectorContext};
use crate::utils::{map_json_format, ptr_to_str};

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_json(
    df_ptr: *mut DataFrameContext, 
    path: *const c_char,
    json_format: u8 // 0: Json, 1: JsonLines
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        let file = File::create(&*p)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        let format = map_json_format(json_format);
        
        JsonWriter::new(file)
            .with_json_format(format)
            .finish(&mut ctx.df)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_json_memory(
    df_ptr: *mut DataFrameContext,
    out_buffer: *mut FfiBuffer,
    json_format: u8, // 0: Json, 1: JsonLines
) {
    ffi_try_void!({
        if df_ptr.is_null() || out_buffer.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to dataframe json memory writer".into()));
        }

        let ctx = unsafe { &mut *df_ptr };
        let format = map_json_format(json_format);
        
        let mut mem_writer = Vec::new();

        JsonWriter::new(&mut mem_writer)
            .with_json_format(format)
            .finish(&mut ctx.df)?;

        let mut vec = std::mem::ManuallyDrop::new(mem_writer);

        unsafe {
            (*out_buffer).data = vec.as_mut_ptr();
            (*out_buffer).len = vec.len();
            (*out_buffer).capacity = vec.capacity();
        }

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_json(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // --- NDJson Params ---
    compression_code: u8,
    compression_level: i32,
    check_extension: bool, 
    // --- UnifiedSinkArgs ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
    // --- Cloud Params ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,
    cloud_retry_init_backoff_ms: u64,
    cloud_retry_max_backoff_ms: u64,
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // UnifiedSinkArgs
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir,
                maintain_order,
                sync_on_close,
                cloud_provider,
                cloud_retries,
                cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms,
                cloud_retry_max_backoff_ms,
                cloud_cache_ttl,
                cloud_keys,
                cloud_values,
                cloud_len
            )
        };

        let json_options = build_ndjson_writer_options(
            compression_code,
            compression_level,
            check_extension
        );

        let file_format = FileWriteFormat::NDJson(json_options);
        
        let target = SinkTarget::Path(PlRefPath::from(path_str));
        let destination = SinkDestination::File { target };

        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;

        Ok(())
    })
}

// ==========================================
// Sink JSON / NDJSON Partitioned
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_json_partitioned(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    
    // --- Partition Params ---
    partition_by_ptr: *mut SelectorContext,
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,

    // --- NDJson Params ---
    compression_code: u8,
    compression_level: i32,
    check_extension: bool, 
    
    // --- UnifiedSinkArgs ---
    maintain_order: bool,
    sync_on_close_code: u8,
    mkdir: bool,
    
    // --- Cloud Params ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,
    cloud_retry_init_backoff_ms: u64,
    cloud_retry_max_backoff_ms: u64,
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) {
    ffi_try_void!({
        let mut lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        let unified_args = unsafe {
            crate::pl_io::io_utils::build_unified_sink_args(
                mkdir,
                maintain_order,
                sync_on_close_code,
                cloud_provider,
                cloud_retries,
                cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms,
                cloud_retry_max_backoff_ms,
                cloud_cache_ttl,
                cloud_keys,
                cloud_values,
                cloud_len
            )
        };

        let json_options = build_ndjson_writer_options(
            compression_code,
            compression_level,
            check_extension
        );
        let file_format = FileWriteFormat::NDJson(json_options);

        let schema = lf_ctx.inner.collect_schema()?;

        let destination = unsafe {
            crate::pl_io::io_utils::build_partitioned_destination(
                path_ptr,
                ".json",
                &schema,
                partition_by_ptr,
                include_keys,
                keys_pre_grouped,
                max_rows_per_file,
                approx_bytes_per_file
            )?
        };

        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_json_memory(
    lf_ptr: *mut LazyFrameContext,
    out_buffer: *mut FfiBuffer,
    // --- NDJson Params ---
    compression_code: u8,
    compression_level: i32,
    check_extension: bool,
    // --- UnifiedSinkArgs ---
    maintain_order: bool,
) {
    ffi_try_void!({
        if lf_ptr.is_null() || out_buffer.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to memory sink".into()));
        }

        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        let (mem_writer, destination) = build_memory_sink_destination();

        let json_options = build_ndjson_writer_options(
            compression_code,
            compression_level,
            check_extension
        );
        let file_format = FileWriteFormat::NDJson(json_options);

        let unified_args = UnifiedSinkArgs {
            mkdir: false,
            maintain_order,
            sync_on_close: Default::default(),
            cloud_options: None,
            sinked_paths_callback:None
        };

        let sink_lf = lf_ctx.inner
            .sink(destination, file_format, unified_args)?;
            
        let _ = sink_lf.collect()?;

        let vec = mem_writer.into_inner();
        let mut vec = std::mem::ManuallyDrop::new(vec);

        unsafe {
            (*out_buffer).data = vec.as_mut_ptr();
            (*out_buffer).len = vec.len();
            (*out_buffer).capacity = vec.capacity();
        }

        Ok(())
    })
}