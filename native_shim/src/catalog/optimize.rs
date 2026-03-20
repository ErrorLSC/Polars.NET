use std::{ffi::c_char};
use polars::prelude::*;

use crate::catalog::ffi::CatalogContext;
use crate::catalog::utils::{load_catalog_table};
use crate::delta::optimize::{OptimizeContext, optimize_delta_internal};
use crate::utils::{ptr_to_str, ptr_to_vec_string};
use crate::delta::utils::{RawCloudArgs, build_delta_storage_options_map, get_runtime};

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_optimize(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    target_size_mb: i64,
    filter_json_ptr: *const c_char,
    z_order_cols_ptr: *const *const c_char,
    z_order_len: usize,
    cloud_provider: u8, cloud_retries: usize, cloud_retry_timeout_ms: u64, cloud_retry_init_backoff_ms: u64, cloud_retry_max_backoff_ms: u64, cloud_cache_ttl: u64, cloud_keys: *const *const c_char, cloud_values: *const *const c_char, cloud_len: usize,
    out_num_files_optimized: *mut usize,
) {
    ffi_try_void!({
        let catalog_ctx = unsafe { &*ctx_ptr };
        let catalog_name = ptr_to_str(catalog_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        let schema_name = ptr_to_str(schema_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        let table_name = ptr_to_str(table_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();

        let partition_filters = if !filter_json_ptr.is_null() {
            let json_str = ptr_to_str(filter_json_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            if json_str.trim().is_empty() { None } else {
                let map: std::collections::HashMap<String, String> = serde_json::from_str(json_str)
                    .map_err(|e| PolarsError::ComputeError(format!("Invalid filter JSON: {}", e).into()))?;
                Some(map)
            }
        } else { None };

        let z_order_columns = if z_order_len > 0 && !z_order_cols_ptr.is_null() {
            unsafe { Some(ptr_to_vec_string(z_order_cols_ptr, z_order_len)) }
        } else { None };

        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let cloud_args = RawCloudArgs { provider: cloud_provider, retries: cloud_retries, retry_timeout_ms: cloud_retry_timeout_ms, retry_init_backoff_ms: cloud_retry_init_backoff_ms, retry_max_backoff_ms: cloud_retry_max_backoff_ms, cache_ttl: cloud_cache_ttl, keys: cloud_keys, values: cloud_values, len: cloud_len };

        let rt = get_runtime();

        let (table_url, final_options) = rt.block_on(async {
            let (_, url, options) = load_catalog_table(catalog_ctx, &catalog_name, &schema_name, &table_name, true, base_options).await?;
            Ok::<(url::Url, std::collections::HashMap<String, String>), PolarsError>((url, options))
        })?;

        let ctx = OptimizeContext {
            table_url,
            target_size_bytes: target_size_mb * 1024 * 1024,
            partition_filters, 
            z_order_columns,
        };

        let total_optimized = optimize_delta_internal(ctx, final_options, cloud_args)?;

        if !out_num_files_optimized.is_null() {
            unsafe { *out_num_files_optimized = total_optimized; }
        }

        Ok(())
    })
}