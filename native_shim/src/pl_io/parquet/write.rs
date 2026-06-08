use polars::prelude::*;
use std::os::raw::c_char;
use crate::pl_io::ffi_buffer::FfiBuffer;
use crate::pl_io::io_utils::{build_memory_sink_destination, build_partitioned_destination, build_unified_sink_args};
use crate::pl_io::parquet::parquet_utils::build_parquet_write_options;
use crate::types::{LazyFrameContext, SelectorContext};
use crate::utils::ptr_to_str;

// ==========================================
// Write&Sink Parquet
// ==========================================

fn fallback_eager_parquet(
    lf: LazyFrame,
    path: &str,
    options: &ParquetWriteOptions
) -> PolarsResult<()> {
    // 1. Collect
    let mut df = lf.collect()?;

    // 2. Create File
    let file = std::fs::File::create(path)
        .map_err(|e| PolarsError::ComputeError(format!("Fallback: failed to create file '{}': {}", path, e).into()))?;

    // 3. Configure Writer
    let mut writer = ParquetWriter::new(file)
        .with_compression(options.compression)
        .with_statistics(options.statistics.clone())
        .set_parallel(true);

    if let Some(s) = options.row_group_size { writer = writer.with_row_group_size(Some(s)); }
    if let Some(s) = options.data_page_size { writer = writer.with_data_page_size(Some(s)); }
    
    if let Some(_compat) = options.compat_level {
    }

    // 4. Write
    writer.finish(&mut df)?;

    Ok(())
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // --- Parquet Options ---
    compression: u8,
    compression_level: i32,
    statistics: bool,
    row_group_size: usize,
    data_page_size: usize,
    compat_level: i32,
    // --- Unified Options ---
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
        let ctx = unsafe { Box::from_raw(lf_ptr) };
        let lf = ctx.inner;
        let lf_clone = lf.clone();

        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let write_options_arc = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size,
            compat_level
        )?;
        
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

        let options_for_stream = write_options_arc.clone();
        
        let result = catch_unwind(AssertUnwindSafe(|| {
            let target = SinkTarget::Path(PlRefPath::from(path_str));
            let destination = SinkDestination::File { target };

            let file_format = FileWriteFormat::Parquet(options_for_stream);

            lf.sink(destination, file_format, unified_args)?
              .collect()
        }));

        match result {
            Ok(Ok(_)) => {
            },
            Ok(Err(e)) => {
                if cloud_provider != 0 {
                    return Err(e);
                }
                eprintln!("Streaming Sink failed: {}. Attempting fallback to Eager Write...", e);
                fallback_eager_parquet(lf_clone, path_str, &write_options_arc)?;
            },
            Err(_) => {
                if cloud_provider != 0 {
                    return Err(PolarsError::ComputeError("Streaming sink panicked on Cloud path (Eager fallback not supported for Cloud).".into()));
                }
                eprintln!("Streaming Sink panicked. Falling back to Eager Write...");
                fallback_eager_parquet(lf_clone, path_str, &write_options_arc)?;
            }
        }

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet_partitioned(
    lf_ptr: *mut LazyFrameContext,
    base_path_ptr: *const c_char,
    
    // --- Partition Params ---
    partition_by_ptr: *mut SelectorContext,
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,

    // --- Parquet Options ---
    compression: u8,        
    compression_level: i32, 
    statistics: bool,       
    row_group_size: usize,  
    data_page_size: usize,
    compat_level: i32,
    
    // --- Unified Options ---
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
        let mut lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        let schema = lf_ctx.inner.collect_schema()?;

        let destination = unsafe {
            build_partitioned_destination(
                base_path_ptr,
                ".parquet", 
                &schema,
                partition_by_ptr,
                include_keys,
                keys_pre_grouped,
                max_rows_per_file,
                approx_bytes_per_file
            )?
        };

        let write_options_arc = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size,
            compat_level
        )?;
        let file_format = FileWriteFormat::Parquet(write_options_arc);

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

        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet_memory(
    lf_ptr: *mut LazyFrameContext,
    out_buffer: *mut FfiBuffer,
    
    // --- Parquet Options ---
    compression: u8,        
    compression_level: i32, 
    statistics: bool,       
    row_group_size: usize,  
    data_page_size: usize,
    compat_level: i32,
    
    // --- Unified Options ---
    maintain_order: bool,
) {
    ffi_try_void!({
        if lf_ptr.is_null() || out_buffer.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to memory sink".into()));
        }

        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        let (mem_writer, destination) = build_memory_sink_destination();

        let write_options_arc = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size,
            compat_level
        )?;
        let file_format = FileWriteFormat::Parquet(write_options_arc);

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