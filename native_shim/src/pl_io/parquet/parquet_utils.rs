use polars::prelude::*;
use polars_core::prelude::CompatLevel;
use polars_utils::compression::{BrotliLevel, GzipLevel, ZstdLevel};
use polars_io::{HiveOptions, RowIndex};
use std::os::raw::c_char;
use crate::pl_io::io_utils::build_cloud_options;
use crate::types::SchemaContext;
use crate::utils::{ map_parallel_strategy,ptr_to_schema_ref, ptr_to_str};


pub(crate)fn build_scan_args(
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    glob: bool,
    allow_missing_columns: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    schema_ptr: *mut SchemaContext,        
    hive_partitioning: bool,
    hive_schema_ptr: *mut SchemaContext,   
    try_parse_hive_dates: bool,      
    rechunk: bool,
    cache: bool,       
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
) -> ScanArgsParquet {
    let mut args = ScanArgsParquet::default();

    if !n_rows.is_null() { unsafe { args.n_rows = Some(*n_rows); } }
    args.parallel = map_parallel_strategy(parallel_code);
    args.low_memory = low_memory;
    args.use_statistics = use_statistics;
    args.glob = glob;
    args.allow_missing_columns = allow_missing_columns;
    args.rechunk = rechunk; 
    args.cache = cache;     

    if !row_index_name_ptr.is_null() {
            let name = ptr_to_str(row_index_name_ptr).unwrap();
            args.row_index = Some(RowIndex {
                name: PlSmallStr::from_str(name),
                offset: row_index_offset as u32,
            });
    }

    if !include_path_col_ptr.is_null() {
            let col_name = ptr_to_str(include_path_col_ptr).unwrap();
            args.include_file_paths = Some(PlSmallStr::from_str(col_name));
    }

    // --- Schema ---
    args.schema = unsafe { ptr_to_schema_ref(schema_ptr) };

    // --- HiveOptions ---
    let hive_schema = unsafe { ptr_to_schema_ref(hive_schema_ptr) };
    
    let hive_enabled = hive_partitioning;
    
    args.hive_options = HiveOptions {
        enabled: Some(hive_enabled), 
        hive_start_idx: 0,
        schema: hive_schema, 
        try_parse_dates: try_parse_hive_dates,
    };

    args.cloud_options = unsafe {
        build_cloud_options(
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

    args
}

pub(crate) fn build_parquet_write_options(
    compression: u8,        // 0:Uncompressed, 1:Snappy, 2:Gzip, 3:Brotli, 4:Zstd, 5:Lz4Raw
    compression_level: i32, // -1: Default
    statistics: bool,
    row_group_size: usize,
    data_page_size: usize,
    compat_level: i32,      // -1: Default(None), 0: Oldest, 1: Newest
) -> PolarsResult<Arc<ParquetWriteOptions>> {
    
    // 1. Map Compression Options 
    let compression_opts = match compression {
        1 => ParquetCompression::Snappy,
        2 => {
            let lvl = if compression_level >= 0 {
                Some(
                    GzipLevel::try_new(compression_level as u8)
                        .map_err(|_| PolarsError::ComputeError("Invalid Gzip Level".into()))?
                )
            } else {
                None
            };
            ParquetCompression::Gzip(lvl)
        },
        3 => {
            let lvl = if compression_level >= 0 {
                Some(
                    BrotliLevel::try_new(compression_level as u32)
                        .map_err(|_| PolarsError::ComputeError("Invalid Brotli Level".into()))?
                )
            } else {
                None
            };
            ParquetCompression::Brotli(lvl)
        },
        4 => {
            let lvl = if compression_level >= 0 {
                Some(
                    ZstdLevel::try_new(compression_level)
                        .map_err(|_| PolarsError::ComputeError("Invalid Zstd Level".into()))?
                )
            } else {
                None
            };
            ParquetCompression::Zstd(lvl)
        },
        5 => ParquetCompression::Lz4Raw,
        _ => ParquetCompression::Uncompressed,
    };

    // 2. Build StatisticsOptions
    let stats_opts = StatisticsOptions {
        min_value: statistics,
        max_value: statistics,
        distinct_count: statistics,
        null_count: statistics,
        binary_statistics_truncate_length: None
    };

    // 3. Handle Compat Level
    let compat_opt = if compat_level < 0 {
        None
    } else {
        Some(CompatLevel::with_level(compat_level as u16)?)
    };

    // 4. Construct Struct and Wrap in Arc
    let options = ParquetWriteOptions {
        compression: compression_opts,
        statistics: stats_opts,
        row_group_size: if row_group_size > 0 { Some(row_group_size) } else { None },
        data_page_size: if data_page_size > 0 { Some(data_page_size) } else { None },
        key_value_metadata: None, 
        arrow_schema: None,
        compat_level: compat_opt,
    };

    Ok(Arc::new(options))
}
