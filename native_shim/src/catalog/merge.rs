use std::ffi::c_char;
use polars::error::PolarsResult;

use crate::catalog::ffi::CatalogContext;
use crate::catalog::utils::{load_catalog_table_sync, parse_catalog_table_args};
use crate::delta::merge_ordered::{MergeActionRule, MergeActionType, merge_delta_internal};
use crate::delta::utils::{RawCloudArgs, build_delta_storage_options_map};
use crate::types::{ExprContext, LazyFrameContext};
use crate::utils::ptr_to_vec_string;

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_merge_ordered(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    
    source_lf_ptr: *mut LazyFrameContext, 
    merge_keys_ptr: *const *const c_char,
    merge_keys_len: usize,
    
    action_types_ptr: *const u8,               
    action_exprs_ptr: *const *mut ExprContext, 
    actions_count: usize,                      
    
    can_evolve: bool, 
    // --- Cloud Args ---
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
        let (ctx, catalog_name, schema_name, table_name) = 
            parse_catalog_table_args(ctx_ptr, catalog_name_ptr, schema_name_ptr, table_name_ptr)?;
        
        let merge_keys = unsafe { ptr_to_vec_string(merge_keys_ptr, merge_keys_len) };
        let source_lf_ctx = unsafe { Box::from_raw(source_lf_ptr) };

        let mut rules = Vec::with_capacity(actions_count);
        if actions_count > 0 && !action_types_ptr.is_null() && !action_exprs_ptr.is_null() {
            let types_slice = unsafe { std::slice::from_raw_parts(action_types_ptr, actions_count) };
            let exprs_slice = unsafe { std::slice::from_raw_parts(action_exprs_ptr, actions_count) };
            for i in 0..actions_count {
                rules.push(MergeActionRule { 
                    action_type: MergeActionType::try_from(types_slice[i])?, 
                    condition: unsafe { *Box::from_raw(exprs_slice[i]) }.inner 
                });
            }
        }

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

        merge_delta_internal(
            source_lf_ctx.inner, 
            table_url, 
            merge_keys, 
            rules, 
            can_evolve, 
            cloud_args
        )?;
        
        Ok(())
    })
}