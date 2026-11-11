use polars::prelude::*;
use std::os::raw::c_char;
use crate::pl_io::parquet::parquet_utils::build_scan_args;
use crate::types::{ LazyFrameContext, SchemaContext};
use crate::utils::ptr_to_str;

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_parquet(
    path_ptr: *const c_char,
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    glob: bool,
    allow_missing_columns: bool,
    rechunk: bool, 
    cache: bool,   
    // --- Optional Names ---
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    // --- Schema ---
    schema_ptr: *mut SchemaContext,
    hive_partitioning: bool,
    hive_schema_ptr: *mut SchemaContext,
    try_parse_hive_dates: bool,
    // --- Cloud Options ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, 
    cloud_retry_max_backoff_ms: u64,  
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics, 
            glob, allow_missing_columns, 
            row_index_name_ptr, row_index_offset, include_path_col_ptr,
            schema_ptr,hive_partitioning, hive_schema_ptr, try_parse_hive_dates,
            rechunk, cache,
            cloud_provider, cloud_retries,cloud_retry_timeout_ms,      
            cloud_retry_init_backoff_ms, 
            cloud_retry_max_backoff_ms,cloud_cache_ttl, 
            cloud_keys, cloud_values, cloud_len
        );

        let lf = LazyFrame::scan_parquet(PlRefPath::new(path), args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner:lf })))
    })
}

// ---------------------------------------------------------
// Scan Parquet (an explicit list of files)
// ---------------------------------------------------------
/// Scans the given files as one source, in the given order. No globbing and no directory expansion:
/// the caller names every file.
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_parquet_files(
    paths_ptr: *const *const c_char,
    paths_len: usize,
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    allow_missing_columns: bool,
    ignore_extra_columns: bool,
    lenient_casts: bool,
    rechunk: bool,
    cache: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    schema_ptr: *mut SchemaContext,
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,
    cloud_retry_init_backoff_ms: u64,
    cloud_retry_max_backoff_ms: u64,
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        if paths_ptr.is_null() || paths_len == 0 {
            return Err(PolarsError::ComputeError("pl_scan_parquet_files needs at least one path".into()));
        }
        let paths: Vec<PlRefPath> = unsafe { crate::utils::ptr_to_vec_string(paths_ptr, paths_len) }
            .iter()
            .map(|p| PlRefPath::new(p))
            .collect();

        let args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics,
            false, allow_missing_columns,
            row_index_name_ptr, row_index_offset, include_path_col_ptr,
            schema_ptr, false, std::ptr::null_mut(), false,
            rechunk, cache,
            cloud_provider, cloud_retries, cloud_retry_timeout_ms,
            cloud_retry_init_backoff_ms,
            cloud_retry_max_backoff_ms, cloud_cache_ttl,
            cloud_keys, cloud_values, cloud_len
        );

        // Built from the plan pieces rather than `LazyFrame::scan_parquet_files`, which always raises on a column
        // the schema does not name. A table format lists the columns it wants; anything else in a file is not its.
        use polars_plan::dsl::{DslBuilder, ExtraColumnsPolicy, MissingColumnsPolicy, CastColumnsPolicy, ScanSources, UnifiedScanArgs};
        use polars_utils::slice_enum::Slice;

        let parquet_options = polars_io::prelude::ParquetOptions {
            schema: args.schema,
            parallel: args.parallel,
            low_memory: args.low_memory,
            use_statistics: args.use_statistics,
        };
        let unified_scan_args = UnifiedScanArgs {
            schema: None,
            cloud_options: args.cloud_options,
            hive_options: args.hive_options,
            rechunk: args.rechunk,
            cache: args.cache,
            glob: false,
            hidden_file_prefix: None,
            projection: None,
            column_mapping: None,
            default_values: None,
            row_index: None,
            pre_slice: args.n_rows.map(|len| Slice::Positive { offset: 0, len }),
            // A table's files are written over time and by different engines: older files may hold a narrower
            // type than the table schema now has, nanosecond timestamps, or structs from before a field was added.
            cast_columns_policy: if lenient_casts {
                CastColumnsPolicy {
                    integer_upcast: true,
                    integer_to_float_cast: true,
                    float_upcast: true,
                    datetime_nanoseconds_downcast: true,
                    datetime_convert_timezone: true,
                    missing_struct_fields: MissingColumnsPolicy::Insert,
                    extra_struct_fields: ExtraColumnsPolicy::Ignore,
                    ..CastColumnsPolicy::ERROR_ON_MISMATCH
                }
            } else {
                CastColumnsPolicy::ERROR_ON_MISMATCH
            },
            missing_columns_policy: if args.allow_missing_columns { MissingColumnsPolicy::Insert } else { MissingColumnsPolicy::Raise },
            extra_columns_policy: if ignore_extra_columns { ExtraColumnsPolicy::Ignore } else { ExtraColumnsPolicy::Raise },
            include_file_paths: args.include_file_paths,
            deletion_files: None,
            table_statistics: None,
            row_count: None,
        };
        let mut lf: LazyFrame = DslBuilder::scan_parquet(
            ScanSources::Paths(polars_buffer::Buffer::from(paths)), parquet_options, unified_scan_args,
        )?.build().into();
        if let Some(row_index) = args.row_index {
            lf = lf.with_row_index(row_index.name, Some(row_index.offset));
        }
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

// ---------------------------------------------------------
// Scan Parquet (Memory / Buffers)
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_parquet_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
    release: Option<crate::pl_io::managed_buffer::ReleaseCallback>,
    user_data: *mut std::ffi::c_void,
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    glob: bool,
    allow_missing_columns: bool,
    rechunk: bool, 
    cache: bool,   
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    schema_ptr: *mut SchemaContext,
    hive_partitioning: bool,
    hive_schema_ptr: *mut SchemaContext,
    try_parse_hive_dates: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        let buffer = unsafe { crate::pl_io::managed_buffer::buffer_from_ffi(buffer_ptr, buffer_len, release, user_data) };
        let sources = ScanSources::Buffers(Arc::new([buffer]));

        let args = build_scan_args(
            n_rows, 
            parallel_code, 
            low_memory, 
            use_statistics, 
            glob, 
            allow_missing_columns, 
            row_index_name_ptr, 
            row_index_offset, 
            include_path_col_ptr,
            schema_ptr, 
            hive_partitioning,
            hive_schema_ptr, 
            try_parse_hive_dates,
            rechunk,  
            cache,    
            // --- Cloud Options  ---
            0,                // provider_code = None
            0,                // retries
            0,
            0,
            0,
            0,                // cache_ttl
            std::ptr::null(), // keys_ptr
            std::ptr::null(), // vals_ptr
            0                 // len
        );

        let lf = LazyFrame::scan_parquet_sources(sources, args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner:lf })))
    })
}
