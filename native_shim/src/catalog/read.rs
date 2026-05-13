use std::ffi::c_char;
use polars::error::{PolarsError, PolarsResult};
use polars_io::catalog::unity::models::DataSourceFormat;
use polars_io::catalog::unity::schema::table_info_to_schemas;

use crate::catalog::ffi::CatalogContext;
use crate::catalog::utils::get_catalog_table_info_and_options;
use crate::delta::utils::{build_delta_storage_options_map, get_runtime};
use crate::pl_io::parquet::parquet_utils::build_scan_args;
use crate::types::{LazyFrameContext, SchemaContext};
use crate::utils::ptr_to_str;
use crate::delta::read::scan_delta_internal; 

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_catalog_table(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char, schema_name_ptr: *const c_char, table_name_ptr: *const c_char,
    version: *const u64, datetime_ptr: *const c_char,
    n_rows: *const usize, parallel_code: u8, low_memory: bool, use_statistics: bool,
    glob: bool, rechunk: bool, cache: bool, row_index_name_ptr: *const c_char,
    row_index_offset: u32, include_path_col_ptr: *const c_char, schema_ptr: *mut SchemaContext, 
    hive_partitioning: bool, hive_schema_ptr: *mut SchemaContext, try_parse_hive_dates: bool,
    cloud_provider: u8, cloud_retries: usize, cloud_retry_timeout_ms: u64, cloud_retry_init_backoff_ms: u64, 
    cloud_retry_max_backoff_ms: u64, cloud_cache_ttl: u64, cloud_keys: *const *const c_char, 
    cloud_values: *const *const c_char, cloud_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe {
            if ctx_ptr.is_null() { return Err(PolarsError::ComputeError("CatalogContext pointer is null".into())); }
            &*ctx_ptr
        };
        let version_val = if version.is_null() { None } else { unsafe { Some(*version) } };
        let datetime_str = if datetime_ptr.is_null() { None } else { 
            Some(ptr_to_str(datetime_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string()) 
        };

        let catalog_name = ptr_to_str(catalog_name_ptr).unwrap();
        let schema_name = ptr_to_str(schema_name_ptr).unwrap();
        let table_name = ptr_to_str(table_name_ptr).unwrap();

        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        let mut args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics, glob, true, 
            row_index_name_ptr, row_index_offset, include_path_col_ptr, schema_ptr, hive_partitioning,
            hive_schema_ptr, try_parse_hive_dates, rechunk, cache, cloud_provider, cloud_retries, 
            cloud_retry_timeout_ms, cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, 
            cloud_cache_ttl, cloud_keys, cloud_values, cloud_len
        );

        let hive_schema_is_null = hive_schema_ptr.is_null();
        let rt = get_runtime();

        let (table_info, table_url, final_options) = rt.block_on(async {
            get_catalog_table_info_and_options(ctx, catalog_name, schema_name, table_name, false, base_options).await
        })?;

        let (uc_schema, _uc_hive_schema) = table_info_to_schemas(&table_info)?;
        if args.schema.is_none() && uc_schema.is_some() {
            args.schema = uc_schema;
        }

        let final_lf = match table_info.data_source_format.as_ref() {
            Some(DataSourceFormat::Delta) => {
                scan_delta_internal(
                    table_url, final_options, version_val, datetime_str, args, 
                    hive_schema_is_null, try_parse_hive_dates
                )?
            },
            Some(DataSourceFormat::Parquet) => {
                return Err(PolarsError::ComputeError("Direct Parquet catalog scan pending implementation".into()));
            },
            Some(fmt) => return Err(PolarsError::ComputeError(format!("Unsupported data source format: {:?}", fmt).into())),
            None => return Err(PolarsError::ComputeError("Data source format is missing from Unity Catalog response".into())),
        };
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: final_lf })))
    })
}