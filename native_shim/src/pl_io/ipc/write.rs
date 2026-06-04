use polars::prelude::*;
use std::os::raw::c_char;
use crate::pl_io::ffi_buffer::FfiBuffer;
use crate::pl_io::io_utils::{build_memory_sink_destination, build_partitioned_destination, build_unified_sink_args};
use crate::pl_io::ipc::ipc_utils::build_ipc_write_options;
use crate::types::{LazyFrameContext, SelectorContext};
use crate::utils::ptr_to_str;

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_ipc(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // --- IpcWriterOptions params ---
    compression: u8,      
    compat_level: i32,
    record_batch_size: usize,      
    record_batch_statistics: bool, 
    // --- UnifiedSinkArgs params ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
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
) {
    ffi_try_void!({
        // 1. Consume LazyFrame & Path
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // 2. Build UnifiedSinkArgs
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

        // 3. Build IpcWriterOptions
        let ipc_options = build_ipc_write_options(
            compression,
            compat_level,
            record_batch_size,
            record_batch_statistics
        )?;

        // 4. Construct FileWriteFormat
        let file_format = FileWriteFormat::Ipc(ipc_options);

        // 5. Construct SinkDestination
        let target = SinkTarget::Path(PlRefPath::from(path_str));
        let destination = SinkDestination::File { target };

        // 6. Execute via Unified Sink Pipeline
        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;
        
        Ok(())
    })
}

// ==========================================
// Sink IPC Partitioned
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_sink_ipc_partitioned(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    
    // --- Partition Params ---
    partition_by_ptr: *mut SelectorContext,
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,

    // --- IpcWriterOptions params ---
    compression: u8,
    compat_level: i32,
    record_batch_size: usize,      
    record_batch_statistics: bool, 
    
    // --- UnifiedSinkArgs params  ---
    maintain_order: bool,
    sync_on_close_code: u8,
    mkdir: bool,
    
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
) {
    ffi_try_void!({
        let mut lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        let unified_args = unsafe {
            crate::pl_io::io_utils::build_unified_sink_args(
                mkdir, maintain_order, sync_on_close_code,
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms,
                cloud_cache_ttl, cloud_keys, cloud_values, cloud_len
            )
        };

        let ipc_options = build_ipc_write_options(
            compression, compat_level, record_batch_size, record_batch_statistics
        )?;
        let file_format = FileWriteFormat::Ipc(ipc_options);

        let schema = lf_ctx.inner.collect_schema()?;

        let destination = unsafe {
            build_partitioned_destination(
                path_ptr,
                ".ipc",
                &schema,
                partition_by_ptr,
                include_keys,
                keys_pre_grouped,
                max_rows_per_file,
                approx_bytes_per_file
            )?
        };

        let _ = lf_ctx.inner.sink(destination, file_format, unified_args)?
                .collect()?;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_ipc_memory(
    lf_ptr: *mut LazyFrameContext,
    out_buffer: *mut FfiBuffer,
    compression: u8,
    compat_level: i32,
    record_batch_size: usize,
    record_batch_statistics: bool,
    maintain_order: bool,
) {
    ffi_try_void!({
        if lf_ptr.is_null() || out_buffer.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to memory sink".into()));
        }

        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        let (mem_writer, destination) = build_memory_sink_destination();

        let ipc_options = build_ipc_write_options(
            compression,
            compat_level,
            record_batch_size,
            record_batch_statistics
        )?;
        let file_format = FileWriteFormat::Ipc(ipc_options);

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