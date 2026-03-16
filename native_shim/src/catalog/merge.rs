use std::ffi::c_char;
use polars::error::{PolarsError, PolarsResult};

use crate::catalog::ffi::CatalogContext;
use crate::catalog::utils::convert_catalog_creds;
use crate::delta::merge_ordered::{MergeActionRule, MergeActionType, merge_delta_internal};
use crate::delta::utils::{RawCloudArgs, build_delta_storage_options_map, get_runtime};
use crate::types::{ExprContext, LazyFrameContext};
use crate::utils::{ptr_to_str, ptr_to_vec_string};

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
    cloud_provider: u8, cloud_retries: usize, cloud_retry_timeout_ms: u64, cloud_retry_init_backoff_ms: u64, cloud_retry_max_backoff_ms: u64, cloud_cache_ttl: u64, cloud_keys: *const *const c_char, cloud_values: *const *const c_char, cloud_len: usize
) {
    ffi_try_void!({
        let ctx = unsafe { &*ctx_ptr };
        let catalog_name = ptr_to_str(catalog_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        let schema_name = ptr_to_str(schema_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        let table_name = ptr_to_str(table_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        
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

        let mut final_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let cloud_args = RawCloudArgs { provider: cloud_provider, retries: cloud_retries, retry_timeout_ms: cloud_retry_timeout_ms, retry_init_backoff_ms: cloud_retry_init_backoff_ms, retry_max_backoff_ms: cloud_retry_max_backoff_ms, cache_ttl: cloud_cache_ttl, keys: cloud_keys, values: cloud_values, len: cloud_len };

        let rt = get_runtime();

        let table_url = rt.block_on(async {
            let info = ctx.client.get_table_info(&catalog_name, &schema_name, &table_name).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get table info: {}", e).into()))?;
            
            let creds_wrapper = ctx.client.get_table_credentials(&info.table_id, true).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get write credentials: {}", e).into()))?;
                
            let creds = creds_wrapper.into_enum().ok_or_else(|| {
                PolarsError::ComputeError("Unsupported or missing credentials".into())
            })?;

            final_options.extend(convert_catalog_creds(creds));

            let location_str = info.storage_location.clone().ok_or_else(|| {
                PolarsError::ComputeError("Table storage location is missing".into())
            })?;
            
            url::Url::parse(&location_str).map_err(|_| PolarsError::ComputeError("Invalid URL".into()))
        })?;

        merge_delta_internal(source_lf_ctx.inner, table_url, merge_keys, rules, can_evolve, final_options, cloud_args)?;
        Ok(())
    })
}