use std::ffi::c_char;
use polars::error::{PolarsError, PolarsResult};
use polars_io::catalog::unity::models::{DataSourceFormat};
use polars_io::catalog::unity::schema::table_info_to_schemas;
use url::Url;

use crate::catalog::ffi::CatalogContext;
use crate::catalog::utils::convert_catalog_creds;
use crate::delta::utils::{build_delta_storage_options_map, get_runtime};
use crate::pl_io::parquet::parquet_utils::build_scan_args;
use crate::types::{LazyFrameContext, SchemaContext};
use crate::utils::ptr_to_str;
use crate::delta::read::scan_delta_internal; 

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_catalog_table(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    // --- Delta ---
    version: *const i64,
    datetime_ptr: *const c_char,
    // --- Standard Scan Args ---
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    glob: bool,
    rechunk: bool, 
    cache: bool,   
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    schema_ptr: *mut SchemaContext, 
    hive_partitioning: bool,
    hive_schema_ptr: *mut SchemaContext,
    try_parse_hive_dates: bool,
    
    // --- Cloud Network Overrides ---
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
        // 1. 安全解引用 CatalogContext (注意这里不拿走所有权，只是借用)
        let ctx = unsafe {
            if ctx_ptr.is_null() {
                return Err(PolarsError::ComputeError("CatalogContext pointer is null".into()));
            }
            &*ctx_ptr
        };
        let version_val = if version.is_null() { None } else { unsafe { Some(*version) } };
        let datetime_str = if datetime_ptr.is_null() { 
            None 
        } else { 
            let s = ptr_to_str(datetime_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            Some(s.to_string()) 
        };

        let catalog_name = ptr_to_str(catalog_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let schema_name = ptr_to_str(schema_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_name = ptr_to_str(table_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        // 2. 解析由 C# 传过来的底层 Cloud Options (比如 MinIO 的 endpoint)
        let mut final_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        // 构建基础的 Polars ScanArgs
        let mut args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics, 
            glob, true, // allow_missing_columns
            row_index_name_ptr, row_index_offset, include_path_col_ptr,
            schema_ptr, hive_partitioning,
            hive_schema_ptr, try_parse_hive_dates,
            rechunk, cache,
            cloud_provider, cloud_retries, cloud_retry_timeout_ms,      
            cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl, 
            cloud_keys, cloud_values, cloud_len
        );

        let hive_schema_is_null = hive_schema_ptr.is_null();
        let rt = get_runtime();

        // 3. 核心：通过复用的 Client 向 Databricks 索要 TableInfo 和 凭证
        let (table_info, dynamic_creds) = rt.block_on(async {
            let info = ctx.client.get_table_info(catalog_name, schema_name, table_name).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get table info: {}", e).into()))?;
            
            let creds_wrapper = ctx.client.get_table_credentials(&info.table_id, false).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get credentials: {}", e).into()))?;
                
            let creds = creds_wrapper.into_enum().ok_or_else(|| {
                PolarsError::ComputeError("Unsupported or missing credentials".into())
            })?;

            // 提取封装好的转换逻辑
            Ok::<_, PolarsError>((info, convert_catalog_creds(creds)))
        })?;

        // 4. 配置大融合！(凭证 + C# 传来的 Endpoint/HTTP 规则)
        final_options.extend(dynamic_creds);

        let location_str = table_info.storage_location.clone().ok_or_else(|| {
            PolarsError::ComputeError("Table storage location is missing".into())
        })?;
        let table_url = Url::parse(&location_str).map_err(|_| {
            PolarsError::ComputeError(format!("Invalid storage URL: {}", location_str).into())
        })?;

        // 5. [白嫖官方功能]：直接从 UC JSON 里榨取出 Polars Schema 和 Hive Schema
        let (uc_schema, _uc_hive_schema) = table_info_to_schemas(&table_info)?;
        if args.schema.is_none() && uc_schema.is_some() {
            args.schema = uc_schema;
        }

        // 6. 终极智能路由调度器：根据 data_source_format 分发任务
        let final_lf = match table_info.data_source_format.as_ref() {
            Some(DataSourceFormat::Delta) => {
                // 如果是 Delta 表，直接复用咱们千锤百炼的内部调度器
                scan_delta_internal(
                    table_url, 
                    final_options, 
                    version_val, 
                    datetime_str, 
                    args, 
                    hive_schema_is_null,
                    try_parse_hive_dates
                )?
            },
            Some(DataSourceFormat::Parquet) => {
                // 如果以后支持了普通 Parquet 目录，这里可以直接调 scan_parquet
                return Err(PolarsError::ComputeError("Direct Parquet catalog scan pending implementation".into()));
            },
            Some(fmt) => {
                return Err(PolarsError::ComputeError(format!("Unsupported data source format: {:?}", fmt).into()));
            },
            None => {
                return Err(PolarsError::ComputeError("Data source format is missing from Unity Catalog response".into()));
            }
        };
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: final_lf })))
    })
}