use std::ffi::c_char;
use polars::error::{PolarsError, PolarsResult};
use polars_io::catalog::unity::models::DataSourceFormat;
use polars_io::catalog::unity::schema::table_info_to_schemas;

use crate::catalog::ffi::CatalogContext;
use crate::catalog::utils::{
    get_catalog_table_info_and_options_sync, load_catalog_table, load_catalog_table_sync,
    parse_catalog_table_args,
};
use crate::delta::read::scan_delta_internal;
use crate::delta::utils::{build_delta_storage_options_map, get_runtime, RawCloudArgs};
use crate::pl_io::parquet::parquet_utils::build_scan_args;
use crate::types::{LazyFrameContext, SchemaContext};
use crate::utils::ptr_to_str; 

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
        let (ctx, catalog_name, schema_name, table_name) = parse_catalog_table_args(
            ctx_ptr, catalog_name_ptr, schema_name_ptr, table_name_ptr,
        )?;
        let version_val = if version.is_null() { None } else { unsafe { Some(*version) } };
        let datetime_str = if datetime_ptr.is_null() { None } else { 
            Some(ptr_to_str(datetime_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string()) 
        };

        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        // 1. Query catalog metadata and resolve vended storage credentials (needs_write = false)
        let (table_info, table_url, final_options) = get_catalog_table_info_and_options_sync(
            ctx, catalog_name, schema_name, table_name, false, base_options,
        )?;

        // 2. Build base scan arguments
        let mut args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics, glob, true, 
            row_index_name_ptr, row_index_offset, include_path_col_ptr, schema_ptr, hive_partitioning,
            hive_schema_ptr, try_parse_hive_dates, rechunk, cache, cloud_provider, cloud_retries, 
            cloud_retry_timeout_ms, cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, 
            cloud_cache_ttl, cloud_keys, cloud_values, cloud_len
        );

        // 3. Inject the merged catalog credentials into args.cloud_options so Polars engine 
        // can authenticate when reading Parquet data files during Collect()
        let cloud_args = RawCloudArgs::new(
            cloud_provider,
            cloud_retries,
            cloud_retry_timeout_ms,
            cloud_retry_init_backoff_ms,
            cloud_retry_max_backoff_ms,
            cloud_cache_ttl,
            final_options.clone(),
        );
        args.cloud_options = cloud_args.to_cloud_options();

        let (uc_schema, _uc_hive_schema) = table_info_to_schemas(&table_info)?;
        if args.schema.is_none() && uc_schema.is_some() {
            args.schema = uc_schema;
        }

        let hive_schema_is_null = hive_schema_ptr.is_null();

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

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_delta_read_cdc(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    start_version: u64,
    end_version: u64,
    // --- Cloud Args ---
    cloud_provider: u8, 
    cloud_retries: usize, 
    cloud_retry_timeout_ms: u64,
    cloud_retry_init_backoff_ms: u64, 
    cloud_retry_max_backoff_ms: u64,
    cloud_cache_ttl: u64, 
    cloud_keys: *const *const c_char, 
    cloud_values: *const *const c_char, 
    cloud_len: usize,
) -> *mut LazyFrameContext {
    ffi_try!({
        let (ctx, catalog_name, schema_name, table_name) = parse_catalog_table_args(
            ctx_ptr, catalog_name_ptr, schema_name_ptr, table_name_ptr,
        )?;
        let cloud_args = RawCloudArgs::from_raw(
            cloud_provider, cloud_retries, cloud_retry_timeout_ms,
            cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
            cloud_keys, cloud_values, cloud_len,
        );
        let (table, table_url, final_options) = load_catalog_table_sync(
            ctx, catalog_name, schema_name, table_name, false, cloud_args.storage_options(),
        )?;

        let effective_cloud_args = RawCloudArgs::new(
            cloud_args.provider, cloud_args.retries, cloud_args.retry_timeout_ms,
            cloud_args.retry_init_backoff_ms, cloud_args.retry_max_backoff_ms, cloud_args.cache_ttl,
            final_options,
        );
        let scan_args = polars::prelude::ScanArgsParquet {
            cloud_options: effective_cloud_args.to_cloud_options(),
            ..Default::default()
        };

        let lf = get_runtime().block_on(crate::delta::cdc::read_change_data_stream(
            &table, &table_url, start_version, end_version, scan_args,
        ))?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_delta_read_cdc_by_time(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    start_timestamp_ms: i64,
    end_timestamp_ms: i64,
    // --- Cloud Args ---
    cloud_provider: u8, 
    cloud_retries: usize, 
    cloud_retry_timeout_ms: u64,
    cloud_retry_init_backoff_ms: u64, 
    cloud_retry_max_backoff_ms: u64,
    cloud_cache_ttl: u64, 
    cloud_keys: *const *const c_char, 
    cloud_values: *const *const c_char, 
    cloud_len: usize,
) -> *mut LazyFrameContext {
    ffi_try!({
        let (ctx, catalog_name, schema_name, table_name) = parse_catalog_table_args(
            ctx_ptr, catalog_name_ptr, schema_name_ptr, table_name_ptr,
        )?;
        let cloud_args = RawCloudArgs::from_raw(
            cloud_provider, cloud_retries, cloud_retry_timeout_ms,
            cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
            cloud_keys, cloud_values, cloud_len,
        );

        let rt = get_runtime();
        let lf = rt.block_on(async {
            use chrono::{TimeZone, Utc};
            let start_dt = Utc.timestamp_millis_opt(start_timestamp_ms).single()
                .ok_or_else(|| PolarsError::ComputeError("Invalid start timestamp".into()))?;
            let end_dt = Utc.timestamp_millis_opt(end_timestamp_ms).single()
                .ok_or_else(|| PolarsError::ComputeError("Invalid end timestamp".into()))?;

            let (mut table, table_url, final_options) = load_catalog_table(
                ctx, 
                catalog_name, 
                schema_name, 
                table_name, 
                false, 
                cloud_args.storage_options(),
            ).await?;

            let v_start = match table.load_with_datetime(start_dt).await {
                Ok(_) => table.version().unwrap_or(0), 
                Err(deltalake::errors::DeltaTableError::InvalidDateTimeString { .. }) => 0, 
                Err(e) => return Err(PolarsError::ComputeError(format!("Failed to parse start datetime: {}", e).into())),
            };
            let v_end = match table.load_with_datetime(end_dt).await {
                Ok(_) => table.version().unwrap_or(0), 
                Err(deltalake::errors::DeltaTableError::InvalidDateTimeString { .. }) => {
                    return Err(PolarsError::ComputeError("End datetime is before the table was created.".into()));
                },
                Err(e) => return Err(PolarsError::ComputeError(format!("Failed to parse end datetime: {}", e).into())),
            };

            let effective_cloud_args = RawCloudArgs::new(
                cloud_args.provider, cloud_args.retries, cloud_args.retry_timeout_ms,
                cloud_args.retry_init_backoff_ms, cloud_args.retry_max_backoff_ms, cloud_args.cache_ttl,
                final_options,
            );
            let scan_args = polars::prelude::ScanArgsParquet {
                cloud_options: effective_cloud_args.to_cloud_options(),
                ..Default::default()
            };

            let mut lf = crate::delta::cdc::read_change_data_stream(&table, &table_url, v_start, v_end, scan_args).await?;

            use polars::prelude::*;
            let dt_type = DataType::Datetime(polars::datatypes::TimeUnit::Milliseconds, None);

            lf = lf.filter(
                col("_commit_timestamp")
                    .gt_eq(lit(start_timestamp_ms).cast(dt_type.clone()))
                    .and(
                        col("_commit_timestamp")
                            .lt_eq(lit(end_timestamp_ms).cast(dt_type))
                    )
            );

            Ok::<LazyFrame, PolarsError>(lf)
        })?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}