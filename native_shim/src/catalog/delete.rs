use std::ffi::c_char;
use polars::error::{PolarsError, PolarsResult};

use crate::catalog::ffi::CatalogContext;
use crate::catalog::utils::load_catalog_table;
use crate::delta::delete::delete_delta_internal;
use crate::delta::utils::{RawCloudArgs, build_delta_storage_options_map, get_runtime};
use crate::types::ExprContext;
use crate::utils::ptr_to_str;

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
        let ctx = unsafe { 
            if ctx_ptr.is_null() { 
                return Err(PolarsError::ComputeError("CatalogContext pointer is null".into())); 
            }
            &*ctx_ptr 
        };
        let catalog_name = ptr_to_str(catalog_name_ptr).unwrap().to_string();
        let schema_name = ptr_to_str(schema_name_ptr).unwrap().to_string();
        let table_name = ptr_to_str(table_name_ptr).unwrap().to_string();
        
        let predicate_expr = unsafe { *Box::from_raw(predicate_ptr) }.inner;

        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let rt = get_runtime();

        // 1. Query catalog metadata and resolve vended storage credentials first
        let (table_url, final_options) = rt.block_on(async {
            let (_, url, options) = load_catalog_table(
                ctx, &catalog_name, &schema_name, &table_name, true, base_options
            ).await?;
            Ok::<(url::Url, std::collections::HashMap<String, String>), PolarsError>((url, options))
        })?;

        // 2. Re-encode the merged final_options back to raw pointer arrays without breaking delete_delta_internal's signature
        let keys_cstrings: Vec<std::ffi::CString> = final_options.keys()
            .map(|k| std::ffi::CString::new(k.as_str()).unwrap())
            .collect();
        let values_cstrings: Vec<std::ffi::CString> = final_options.values()
            .map(|v| std::ffi::CString::new(v.as_str()).unwrap())
            .collect();

        let keys_ptrs: Vec<*const c_char> = keys_cstrings.iter().map(|s| s.as_ptr()).collect();
        let values_ptrs: Vec<*const c_char> = values_cstrings.iter().map(|s| s.as_ptr()).collect();

        let cloud_args = RawCloudArgs {
            provider: cloud_provider,
            retries: cloud_retries,
            retry_timeout_ms: cloud_retry_timeout_ms,
            retry_init_backoff_ms: cloud_retry_init_backoff_ms,
            retry_max_backoff_ms: cloud_retry_max_backoff_ms,
            cache_ttl: cloud_cache_ttl,
            keys: keys_ptrs.as_ptr(),
            values: values_ptrs.as_ptr(),
            len: final_options.len(),
        };

        // 3. delete_delta_internal signature remains completely untouched
        delete_delta_internal(table_url, predicate_expr, final_options, cloud_args)?;
        Ok(())
    })
}