use std::ffi::c_char;
use polars::prelude::*;
use polars::error::{PolarsError, PolarsResult};

use crate::catalog::utils::get_catalog_table_info_and_options;
use crate::utils::ptr_to_str;
use crate::types::{LazyFrameContext, SelectorContext};
use crate::pl_io::{io_utils::build_unified_sink_args, parquet::parquet_utils::build_parquet_write_options};
use crate::delta::utils::{build_delta_storage_options_map, get_runtime, map_savemode};
use crate::delta::write::sink_delta_internal; 
use super::ffi::CatalogContext;

#[unsafe(no_mangle)]
pub extern "C" fn pl_sink_catalog_table(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    lf_ptr: *mut LazyFrameContext,
    mode: u8,
    can_evolve: bool,
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
    cloud_provider: u8, cloud_retries: usize, cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, cloud_retry_max_backoff_ms: u64,  
    cloud_cache_ttl: u64, cloud_keys: *const *const c_char, cloud_values: *const *const c_char, cloud_len: usize
) {
    ffi_try_void!({
        let ctx = unsafe {
            if ctx_ptr.is_null() { return Err(PolarsError::ComputeError("CatalogContext pointer is null".into())); }
            &*ctx_ptr
        };
        let catalog_name = ptr_to_str(catalog_name_ptr).unwrap();
        let schema_name = ptr_to_str(schema_name_ptr).unwrap();
        let table_name = ptr_to_str(table_name_ptr).unwrap();

        let mut lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let schema = lf_ctx.inner.collect_schema()
            .map_err(|e| PolarsError::ComputeError(format!("Failed to collect schema: {}", e).into()))?;

        let partition_cols = if !partition_by_ptr.is_null() {
            let selector_ctx = unsafe { &*partition_by_ptr };
            let ignored = PlIndexSet::new();
            selector_ctx.inner.into_columns(&schema, &ignored)?
                .iter().map(|s| s.to_string()).collect::<Vec<String>>()
        } else {
            Vec::new()
        };

        let save_mode = map_savemode(mode);
        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        
        let write_options_arc = build_parquet_write_options(
            compression, compression_level, statistics, row_group_size, data_page_size, compat_level
        )?;
        let file_format = FileWriteFormat::Parquet(write_options_arc);
        
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir, maintain_order, sync_on_close,
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                cloud_keys, cloud_values, cloud_len
            )
        };

        let rt = get_runtime();

        let (table_url_str, final_options) = rt.block_on(async {
            let (_, url, options) = get_catalog_table_info_and_options(
                ctx, catalog_name, schema_name, table_name, true, base_options
            ).await?;
            
            Ok::<(String, std::collections::HashMap<String, String>), PolarsError>((url.to_string(), options))
        })?;
        sink_delta_internal(
            lf_ctx.inner,
            schema,
            table_url_str,     
            final_options, 
            save_mode,
            can_evolve,
            partition_cols,
            include_keys,
            keys_pre_grouped,
            max_rows_per_file,
            approx_bytes_per_file,
            file_format,
            unified_args,
            mkdir 
        )?;
        Ok(())
    })
}