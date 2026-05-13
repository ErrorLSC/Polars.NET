use deltalake::{DeltaTable, DeltaTableBuilder, kernel::DeletionVectorDescriptor};
use futures::StreamExt;
use polars::{error::{PolarsError, PolarsResult}, frame::DataFrame, prelude::{IntoLazy, LazyFrame, PlRefPath, Schema}};
use polars_buffer::Buffer;
use polars_io::HiveOptions;
use url::Url;
use std::{collections::HashMap, ffi::c_char, sync::Arc};

use crate::{delta::utils::{build_delta_storage_options_map, get_polars_schema_from_delta, get_runtime}};
use crate::pl_io::parquet::parquet_utils::build_scan_args;
use crate::types::{LazyFrameContext, SchemaContext};
use crate::utils::ptr_to_str;
use crate::delta::deletion_vector::*;

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_delta(
    path_ptr: *const c_char,
    // --- Time Travel Args ---
    version: *const u64,
    datetime_ptr: *const c_char,
    // --- Scan Args ---
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    glob: bool,
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
        // =========================================================
        // Phase 0: FFI Data Cleaning & Parameter Preparation
        // =========================================================
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let version_val = if version.is_null() { None } else { unsafe { Some(*version) } };
        let datetime_str = if datetime_ptr.is_null() { 
            None 
        } else { 
            let s = ptr_to_str(datetime_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            Some(s.to_string()) 
        };

        let table_url = if let Ok(u) = Url::parse(path_str) { u } else {
            let abs = std::fs::canonicalize(path_str).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            Url::from_directory_path(abs).map_err(|_| PolarsError::ComputeError("Invalid path".into()))?
        };

        let delta_opts = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let allow_missing_columns = true;

        let args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics, 
            glob, allow_missing_columns, 
            row_index_name_ptr, row_index_offset, include_path_col_ptr,
            schema_ptr, hive_partitioning,
            hive_schema_ptr, try_parse_hive_dates,
            rechunk, cache,
            cloud_provider, cloud_retries, cloud_retry_timeout_ms,      
            cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl, 
            cloud_keys, cloud_values, cloud_len
        );

        let hive_schema_is_null = hive_schema_ptr.is_null();

        let final_lf = scan_delta_internal(
            table_url, 
            delta_opts, 
            version_val, 
            datetime_str, 
            args, 
            hive_schema_is_null,
            try_parse_hive_dates
        )?;
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: final_lf })))
    })
}

pub(crate) fn scan_delta_internal(
    table_url: Url,
    delta_opts: HashMap<String, String>,
    version_val: Option<u64>,
    datetime_str: Option<String>,
    mut args: polars::prelude::ScanArgsParquet,
    hive_schema_is_null: bool,
    try_parse_hive_dates: bool,
) -> PolarsResult<LazyFrame> {
    
    let rt = get_runtime();

    // =========================================================
    // Phase 1: Physical Table Load & Metadata Extraction (Async)
    // =========================================================
    let (table, polars_schema, partition_cols, clean_paths, dirty_infos) = rt.block_on(
        phase_load_metadata(table_url, delta_opts, version_val, datetime_str)
    )?;

    // =========================================================
    // Phase 2: Schema & Hive Partition Injection
    // =========================================================
    phase_inject_schema(
        &mut args, 
        &polars_schema, 
        &partition_cols, 
        hive_schema_is_null, 
        try_parse_hive_dates
    );
    // =========================================================
    // Phase 3: Build LazyFrames (Clean + Dirty with DVs)
    // =========================================================
    let lfs = phase_build_lazyframes(&table, args, clean_paths, dirty_infos)?;

    // =========================================================
    // Phase 4: Union / Concat
    // =========================================================
    let final_lf = phase_concat_lazyframes(lfs, &polars_schema)?;

    Ok(final_lf)
}

pub(crate) async fn phase_load_metadata(
    table_url: Url,
    delta_opts: HashMap<String, String>,
    version_val: Option<u64>,
    datetime_str: Option<String>,
) -> PolarsResult<(
    DeltaTable,
    Arc<Schema>,                                // Polars Schema
    Vec<String>,                                // Partition Columns
    Vec<String>,                                // Clean Paths
    Vec<(String, DeletionVectorDescriptor)>     // Dirty Paths & DV Descriptors
)> {
    // 1. Build DeltaTable
    let mut builder = DeltaTableBuilder::from_url(table_url)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
    if let Some(v) = version_val { 
        builder = builder.with_version(v); 
    } else if let Some(dt) = datetime_str { 
        builder = builder.with_datestring(dt).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?; 
    }
    
    let table = builder.with_storage_options(delta_opts).load().await
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

    // 2. Extract Polars Schema
    let final_schema = get_polars_schema_from_delta(&table)?;
    let final_schema_arc = Arc::new(final_schema);

    // 3. Extract Partition Columns
    let snapshot = table.snapshot()
        .map_err(|e| PolarsError::ComputeError(format!("Snapshot error: {}", e).into()))?;
    let partition_cols = snapshot.metadata().partition_columns().to_vec();

    // 4. Sort files into Clean and Dirty (with Deletion Vectors)
    let mut clean_paths = Vec::new();
    let mut dirty_infos = Vec::new(); 

    let binding = table.clone();
    let mut stream = binding.get_active_add_actions_by_partitions(&[]);
    let binding_table_url = table.table_url().to_string();
    let table_root = binding_table_url.trim_end_matches('/'); 

    while let Some(item) = stream.next().await {
        let view = item.map_err(|e| PolarsError::ComputeError(format!("Action stream error: {}", e).into()))?;
        let full_path = format!("{}/{}", table_root, view.path());

        if let Some(dv_descriptor) = view.deletion_vector_descriptor() {
            dirty_infos.push((full_path, dv_descriptor));
        } else {
            clean_paths.push(full_path);
        }
    }

    Ok((table, final_schema_arc, partition_cols, clean_paths, dirty_infos))
}

pub(crate) fn phase_inject_schema(
    args: &mut polars::prelude::ScanArgsParquet,
    polars_schema: &Arc<Schema>,
    partition_cols: &[String],
    hive_schema_is_null: bool,
    try_parse_hive_dates: bool,
) {
    // 1. Inject Delta Schema 
    // 如果用户没有显式指定读取 Schema，我们就把 Delta 表原本的 Schema 塞进去
    if args.schema.is_none() {
        // 由于 polars_schema 已经是 Arc，这里的 clone 只是增加引用计数，极其轻量
        args.schema = Some(polars_schema.clone());
    }

    // 2. Inject Hive Partition Schema 
    // 如果存在分区，且用户没有传入自定义的 Hive Schema，我们自动推断并构建 HiveOptions
    if hive_schema_is_null && !partition_cols.is_empty() {
        let mut part_schema = polars::prelude::Schema::with_capacity(partition_cols.len());
        
        // 遍历 Delta 日志里的分区列名
        for col_name in partition_cols {
            // 从完整 Schema 中提取对应的 DataType
            if let Some(dtype) = polars_schema.get(col_name) {
                part_schema.insert(col_name.into(), dtype.clone());
            }
        }
        
        // 组装并注入 HiveOptions
        if !part_schema.is_empty() {
            args.hive_options = HiveOptions {
                enabled: Some(true),
                hive_start_idx: 0,
                schema: Some(Arc::new(part_schema)), 
                try_parse_dates: try_parse_hive_dates, 
            };
        }
    }
}

pub(crate) fn phase_build_lazyframes(
    table: &DeltaTable,
    args: polars::prelude::ScanArgsParquet,
    clean_paths: Vec<String>,
    dirty_infos: Vec<(String, DeletionVectorDescriptor)>,
) -> PolarsResult<Vec<LazyFrame>> {
    let mut lfs = Vec::new();

    // 3.1 Handle Clean Files (无删除向量的文件)
    // 纯净文件可以直接使用 scan_parquet_files 进行批量极速扫描
    if !clean_paths.is_empty() {
        let pl_paths: Vec<PlRefPath> = clean_paths.iter().map(|s| PlRefPath::new(s)).collect();
        let buffer: Buffer<PlRefPath> = pl_paths.into();
        
        let lf_clean = LazyFrame::scan_parquet_files(buffer, args.clone())?;
        lfs.push(lf_clean);
    }

    // 3.2 Handle Dirty Files (包含删除向量的文件)
    if !dirty_infos.is_empty() {
        let object_store = table.object_store(); 
        let table_root = deltalake::Path::from(table.table_url().to_string());
        let rt = crate::delta::utils::get_runtime();

        let dirty_lfs = rt.block_on(async {
            let mut processed = Vec::with_capacity(dirty_infos.len());
            
            // 逐个文件扫描，并应用对应的 Deletion Vector
            for (full_path, dv_descriptor) in dirty_infos {
                
                // A. Scan single file
                let mut lf = LazyFrame::scan_parquet(
                    PlRefPath::new(&full_path), 
                    args.clone() // 注意：每个 LazyFrame 需要独立的 args 副本
                )?;

                // B. Read and apply DV
                let bitmap = read_deletion_vector(
                    object_store.clone(), 
                    &dv_descriptor, 
                    &table_root
                ).await?;

                lf = apply_deletion_vector(lf, bitmap)?;
                
                processed.push(lf);
            }
            Ok::<_, PolarsError>(processed)
        })?;
        
        lfs.extend(dirty_lfs);
    }

    Ok(lfs)
}

pub(crate) fn phase_concat_lazyframes(
    mut lfs: Vec<LazyFrame>,
    polars_schema: &Arc<Schema>,
) -> PolarsResult<LazyFrame> {
    // 4.1 处理空表的情况（没有找到任何可读的 parquet 文件）
    if lfs.is_empty() {
        // 利用 Phase 1 解析出的结构生成一个空 DataFrame，然后转化为 LazyFrame
        let df = DataFrame::empty_with_schema(polars_schema);
        return Ok(df.lazy());
    }

    // 4.2 处理只有一个 LazyFrame 的情况（避免不必要的 Concat 开销）
    if lfs.len() == 1 {
        return Ok(lfs.pop().unwrap());
    }

    // 4.3 处理多个 LazyFrame 的拼接
    let args = polars::prelude::UnionArgs {
        parallel: true,           
        rechunk: false,           
        to_supertypes: true,      
        maintain_order: false,    
        strict: false,            
        diagonal: true,           
        from_partitioned_ds: false, // 视 Delta 目录结构而定
    };

    let final_lf = polars::prelude::concat(lfs, args)
        .map_err(|e| PolarsError::ComputeError(format!("Concat failed: {}", e).into()))?;
    
    Ok(final_lf)
}