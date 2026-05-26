use deltalake::DeltaTable;
use deltalake::kernel::Action;
use polars::prelude::*;
use polars::error::{PolarsResult, PolarsError};
use polars_buffer::Buffer;
use std::ffi::c_char;
use std::io::{BufRead, Cursor};

use crate::delta::utils::RawCloudArgs;

pub(crate) async fn read_change_data_stream(
    table: &DeltaTable,
    table_url: &url::Url,
    start_version: u64,
    end_version: u64,
    scan_args: ScanArgsParquet,
) -> PolarsResult<LazyFrame> {
    
    let root_trimmed = table_url.as_str().trim_end_matches('/');
    let log_store = table.log_store();
    let mut lfs = Vec::new();

    for version in start_version..=end_version {
        let commit_bytes = log_store.read_commit_entry(version).await
            .map_err(|e| PolarsError::ComputeError(format!("Failed to read commit {}: {}", version, e).into()))?
            .ok_or_else(|| PolarsError::ComputeError(format!("Version {} does not exist", version).into()))?;

        let cursor = Cursor::new(commit_bytes);
        let lines = std::io::BufReader::new(cursor).lines();

        let mut adds = Vec::new();
        let mut removes = Vec::new();
        let mut cdcs = Vec::new();
        let mut commit_timestamp: i64 = 0;

        for line in lines {
            let line_str = line.unwrap_or_default();
            if line_str.is_empty() { continue; }

            if let Ok(action) = serde_json::from_str::<Action>(&line_str) {
                match action {
                    Action::Add(a) => if a.data_change { adds.push(a) },
                    Action::Remove(r) => if r.data_change { removes.push(r) },
                    Action::Cdc(c) => cdcs.push(c),
                    Action::CommitInfo(ci) => commit_timestamp = ci.timestamp.unwrap_or(0),
                    _ => {}
                }
            }
        }

        if !cdcs.is_empty() {
            let paths: Vec<PlRefPath> = cdcs.iter()
                .map(|c| PlRefPath::new(format!("{}/{}", root_trimmed, c.path)))
                .collect();
            
            let lf = LazyFrame::scan_parquet_files(Buffer::from(paths), scan_args.clone())?;
            lfs.push(lf);
        } else {
            if !adds.is_empty() {
                let paths: Vec<PlRefPath> = adds.iter()
                    .map(|a| PlRefPath::new(format!("{}/{}", root_trimmed, a.path)))
                    .collect();
                
                let mut lf = LazyFrame::scan_parquet_files(Buffer::from(paths), scan_args.clone())?;
                
                lf = lf.with_column(lit("insert").alias("_change_type"))
                       .with_column(lit(version).alias("_commit_version"))
                       .with_column(
                           lit(commit_timestamp)
                           .cast(DataType::Datetime(polars::datatypes::TimeUnit::Milliseconds, None))
                           .alias("_commit_timestamp"));
                lfs.push(lf);
            }

            if !removes.is_empty() {
                let paths: Vec<PlRefPath> = removes.iter()
                    .map(|r| PlRefPath::new(format!("{}/{}", root_trimmed, r.path)))
                    .collect();
                
                let mut lf = LazyFrame::scan_parquet_files(Buffer::from(paths), scan_args.clone())?;
                
                lf = lf.with_column(lit("delete").alias("_change_type"))
                       .with_column(lit(version).alias("_commit_version"))
                       .with_column(
                           lit(commit_timestamp)
                           .cast(DataType::Datetime(polars::datatypes::TimeUnit::Milliseconds, None))
                           .alias("_commit_timestamp"));
                lfs.push(lf);
            }
        }
    }

    if lfs.is_empty() {
        return Err(PolarsError::ComputeError("No change data found in the specified version range.".into()));
    }

    let args = UnionArgs {
        parallel: true,
        rechunk: false,
        to_supertypes: true,
        diagonal: true, 
        ..Default::default()
    };

    let final_lf = polars::prelude::concat(lfs, args)?;

    Ok(final_lf.sort(["_commit_version"], SortMultipleOptions::default()))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_read_cdc(
    table_path_ptr: *const c_char,
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
    // --- Output ---
    out_lf_ptr: *mut *mut LazyFrame,
) {
    ffi_try_void!({
        let path_str = crate::utils::ptr_to_str(table_path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = crate::delta::utils::parse_table_url(path_str)?;
        
        let cloud_args = RawCloudArgs {
            provider: cloud_provider, 
            retries: cloud_retries, 
            retry_timeout_ms: cloud_retry_timeout_ms,
            retry_init_backoff_ms: cloud_retry_init_backoff_ms, 
            retry_max_backoff_ms: cloud_retry_max_backoff_ms,
            cache_ttl: cloud_cache_ttl, 
            keys: cloud_keys, 
            values: cloud_values, 
            len: cloud_len,
        };
        
        let delta_storage_options = crate::delta::utils::build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        let mut scan_args = ScanArgsParquet::default();
        scan_args.cloud_options = unsafe { crate::pl_io::io_utils::build_cloud_options(
            cloud_args.provider, cloud_args.retries, cloud_args.retry_timeout_ms,
            cloud_args.retry_init_backoff_ms, cloud_args.retry_max_backoff_ms, cloud_args.cache_ttl,
            cloud_args.keys, cloud_args.values, cloud_args.len,
        ) };

        let rt = crate::delta::utils::get_runtime();

        let lf = rt.block_on(async {
            let table = deltalake::DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options)
                .await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to load table: {}", e).into()))?;

            crate::delta::cdc::read_change_data_stream(&table, &table_url, start_version, end_version, scan_args).await
        })?;

        unsafe {
            *out_lf_ptr = Box::into_raw(Box::new(lf));
        }

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_read_cdc_by_time(
    table_path_ptr: *const c_char,
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
    // --- Output ---
    out_lf_ptr: *mut *mut LazyFrame,
) {
    ffi_try_void!({
        let path_str = crate::utils::ptr_to_str(table_path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = crate::delta::utils::parse_table_url(path_str)?;
        
        let cloud_args = crate::delta::utils::RawCloudArgs {
            provider: cloud_provider, retries: cloud_retries, retry_timeout_ms: cloud_retry_timeout_ms,
            retry_init_backoff_ms: cloud_retry_init_backoff_ms, retry_max_backoff_ms: cloud_retry_max_backoff_ms,
            cache_ttl: cloud_cache_ttl, keys: cloud_keys, values: cloud_values, len: cloud_len,
        };
        
        let delta_storage_options = crate::delta::utils::build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        let mut scan_args = ScanArgsParquet::default();
        scan_args.cloud_options = unsafe { crate::pl_io::io_utils::build_cloud_options(
            cloud_args.provider, cloud_args.retries, cloud_args.retry_timeout_ms,
            cloud_args.retry_init_backoff_ms, cloud_args.retry_max_backoff_ms, cloud_args.cache_ttl,
            cloud_args.keys, cloud_args.values, cloud_args.len,
        ) };

        let rt = crate::delta::utils::get_runtime();
        let lf = rt.block_on(async {
            use chrono::{TimeZone, Utc};
            let start_dt = Utc.timestamp_millis_opt(start_timestamp_ms).single()
                .ok_or_else(|| PolarsError::ComputeError("Invalid start timestamp".into()))?;
            let end_dt = Utc.timestamp_millis_opt(end_timestamp_ms).single()
                .ok_or_else(|| PolarsError::ComputeError("Invalid end timestamp".into()))?;
            let mut table = deltalake::DeltaTable::try_from_url_with_storage_options(
                table_url.clone(), 
                delta_storage_options
            )
            .await
            .map_err(|e| PolarsError::ComputeError(format!("Failed to safely load table: {}", e).into()))?;
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

        unsafe {
            *out_lf_ptr = Box::into_raw(Box::new(lf));
        }

        Ok(())
    })
}