use polars::prelude::*;
use std::os::raw::c_char;
use crate::pl_io::csv::csv_utils::{build_csv_writer_options};
use crate::pl_io::ffi_buffer::FfiBuffer;
use crate::pl_io::io_utils::{build_memory_sink_destination, build_partitioned_destination, build_unified_sink_args};
use crate::types::{LazyFrameContext, SelectorContext};
use crate::utils::ptr_to_str;

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_csv(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // --- CSV Specific Params ---
    include_bom: bool,
    include_header: bool,
    batch_size: usize,
    check_extension: bool,
    // Compression
    compression_code: u8,
    compression_level: i32,
    // Serialize Options
    date_format: *const c_char,
    time_format: *const c_char,
    datetime_format: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value: *const c_char,
    line_terminator: *const c_char,
    quote_style: u8,
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

        // CsvWriterOptions
        let csv_options = unsafe {
            build_csv_writer_options(
                include_bom, include_header, batch_size, check_extension,
                compression_code, compression_level,
                date_format, time_format, datetime_format,
                float_scientific, float_precision, decimal_comma,
                separator, quote_char, null_value, line_terminator, quote_style
            )
        };

        // Format and Target
        let file_format = FileWriteFormat::Csv(csv_options);
        
        let target = SinkTarget::Path(PlRefPath::from(path_str));
        let destination = SinkDestination::File { target };

        // Sink
        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_csv_partitioned(
    lf_ptr: *mut LazyFrameContext,
    base_path_ptr: *const c_char,
    
    // --- Partition Specific Params ---
    partition_by_ptr: *mut SelectorContext,
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,     
    approx_bytes_per_file: u64,

    // --- CSV Specific Params ---
    include_bom: bool,
    include_header: bool,
    batch_size: usize,
    check_extension: bool, 
    // Compression
    compression_code: u8,
    compression_level: i32,

    // Serialize Options 
    date_format: *const c_char,
    time_format: *const c_char,
    datetime_format: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value: *const c_char,
    line_terminator: *const c_char,
    quote_style: u8,

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
        let mut lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        // Get Schema
        let schema = lf_ctx.inner.collect_schema()?;

        //  Partitioned Destination
        let destination = unsafe {
            build_partitioned_destination(
                base_path_ptr,
                ".csv", 
                &schema,
                partition_by_ptr,
                include_keys,
                keys_pre_grouped,
                max_rows_per_file,
                approx_bytes_per_file
            )?
        };

        // Unified Args
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

        // CSV Options
        let csv_options = unsafe {
            build_csv_writer_options(
                include_bom, include_header, batch_size, check_extension,
                compression_code, compression_level,
                date_format, time_format, datetime_format,
                float_scientific, float_precision, decimal_comma,
                separator, quote_char, null_value, line_terminator, quote_style
            )
        };
        let file_format = FileWriteFormat::Csv(csv_options);

        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_csv_memory(
    lf_ptr: *mut LazyFrameContext,
    out_buffer: *mut FfiBuffer,
    // --- CSV Specific Params ---
    include_bom: bool,
    include_header: bool,
    batch_size: usize,
    check_extension: bool,
    // Compression
    compression_code: u8,
    compression_level: i32,
    // Serialize Options
    date_format: *const c_char,
    time_format: *const c_char,
    datetime_format: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value: *const c_char,
    line_terminator: *const c_char,
    quote_style: u8,
    // --- UnifiedSinkArgs ---
    maintain_order: bool,
) {
    ffi_try_void!({
        if lf_ptr.is_null() || out_buffer.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to memory sink".into()));
        }

        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        let (mem_writer, destination) = build_memory_sink_destination();

        let csv_options = unsafe {
            build_csv_writer_options(
                include_bom, include_header, batch_size, check_extension,
                compression_code, compression_level,
                date_format, time_format, datetime_format,
                float_scientific, float_precision, decimal_comma,
                separator, quote_char, null_value, line_terminator, quote_style
            )
        };
        let file_format = FileWriteFormat::Csv(csv_options);

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