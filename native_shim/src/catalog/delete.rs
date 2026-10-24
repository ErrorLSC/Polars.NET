use std::ffi::c_char;
use polars::error::PolarsResult;

use crate::catalog::ffi::CatalogContext;
use crate::catalog::utils::{load_catalog_table_sync, parse_catalog_table_args};
use crate::delta::delete::delete_delta_internal;
use crate::delta::utils::{RawCloudArgs, build_delta_storage_options_map};
use crate::types::ExprContext;

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_delete_records(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    predicate_ptr: *mut ExprContext, 
    
    // --- Cloud Args ---
    cloud_provider: u8, cloud_retries: usize, cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, cloud_retry_max_backoff_ms: u64, 
    cloud_cache_ttl: u64, cloud_keys: *const *const c_char, cloud_values: *const *const c_char, cloud_len: usize
) {
    ffi_try_void!({
        let (ctx, catalog_name, schema_name, table_name) = 
            parse_catalog_table_args(ctx_ptr, catalog_name_ptr, schema_name_ptr, table_name_ptr)?;
        let predicate_expr = unsafe { *Box::from_raw(predicate_ptr) }.inner;
        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        let (_, table_url, final_options) = load_catalog_table_sync(
            ctx, catalog_name, schema_name, table_name, true, base_options
        )?;

        let cloud_args = RawCloudArgs::new(
            cloud_provider,
            cloud_retries,
            cloud_retry_timeout_ms,
            cloud_retry_init_backoff_ms,
            cloud_retry_max_backoff_ms,
            cloud_cache_ttl,
            final_options,
        );

        delete_delta_internal(table_url, predicate_expr, cloud_args)?;
        Ok(())
    })
}