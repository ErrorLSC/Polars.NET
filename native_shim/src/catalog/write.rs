use std::ffi::c_char;
use polars::prelude::*;
use polars::error::{PolarsError, PolarsResult};

use crate::catalog::utils::convert_catalog_creds;
use crate::utils::ptr_to_str;
use crate::types::{LazyFrameContext, SelectorContext};
use crate::pl_io::{io_utils::build_unified_sink_args, parquet::parquet_utils::build_parquet_write_options};
use crate::delta::utils::{build_delta_storage_options_map, get_runtime, map_savemode};

// 引入咱们刚在 write.rs 写好的内部调度器
use crate::delta::write::sink_delta_internal; 

// 引入 catalog 上下文和凭证转换助手 (scan 的时候写的)
use super::ffi::{CatalogContext}; 

#[unsafe(no_mangle)]
pub extern "C" fn pl_sink_catalog_table(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    lf_ptr: *mut LazyFrameContext,
    mode: u8,
    can_evolve: bool,
    // --- Partition Params ---
    partition_by_ptr: *mut SelectorContext,
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,
    // --- Parquet Options ---
    compression: u8,        
    compression_level: i32, 
    statistics: bool,       
    row_group_size: usize,  
    data_page_size: usize,
    compat_level: i32,
    // --- Unified Options ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
    // --- Cloud Params ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, 
    cloud_retry_max_backoff_ms: u64,  
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) {
    ffi_try_void!({
        // 1. 安全解引用上下文和基础字符串
        let ctx = unsafe {
            if ctx_ptr.is_null() {
                return Err(PolarsError::ComputeError("CatalogContext pointer is null".into()));
            }
            &*ctx_ptr
        };
        let catalog_name = ptr_to_str(catalog_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let schema_name = ptr_to_str(schema_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_name = ptr_to_str(table_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // 2. 解析 DataFrame 状态
        let mut lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let schema = lf_ctx.inner.collect_schema()
            .map_err(|e| PolarsError::ComputeError(format!("Failed to collect schema: {}", e).into()))?;

        // 提取分区列
        let partition_cols = if !partition_by_ptr.is_null() {
            let selector_ctx = unsafe { &*partition_by_ptr };
            let ignored = PlHashSet::new();
            selector_ctx.inner.into_columns(&schema, &ignored)?
                .iter().map(|s| s.to_string()).collect::<Vec<String>>()
        } else {
            Vec::new()
        };

        // 3. 构建基础云配置与格式参数
        let save_mode = map_savemode(mode);
        let mut final_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        
        let write_options_arc = build_parquet_write_options(
            compression, compression_level, statistics, row_group_size, data_page_size, compat_level
        )?;
        let file_format = FileWriteFormat::Parquet(write_options_arc);
        
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir, maintain_order, sync_on_close,
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                cloud_keys, cloud_values, cloud_len
            )
        };

        let rt = get_runtime();

        // 4. 核心：通过 Catalog 索要物理地址和【写入凭证】！
        let table_url = rt.block_on(async {
            let info = ctx.client.get_table_info(catalog_name, schema_name, table_name).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get table info: {}", e).into()))?;
            
            // 【关键点】：严格使用 write = true 申请写入权限！
            let creds_wrapper = ctx.client.get_table_credentials(&info.table_id, true).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get write credentials: {}", e).into()))?;
                
            let creds = creds_wrapper.into_enum().ok_or_else(|| {
                PolarsError::ComputeError("Unsupported or missing credentials".into())
            })?;

            // 融合临时 STS 凭证到咱们的 final_options 中
            final_options.extend(convert_catalog_creds(creds));

            let location_str = info.storage_location.clone().ok_or_else(|| {
                PolarsError::ComputeError("Table storage location is missing".into())
            })?;
            
            Ok::<_, PolarsError>(location_str)
        })?;

        // 5. 调用咱们千锤百炼的纯 Rust 写入调度器！
        sink_delta_internal(
            lf_ctx.inner,
            schema,
            table_url,     // UC 返回的真实 S3/Blob 路径
            final_options, // 融合了 Write Token 和 Local Endpoint 的终极字典
            save_mode,
            can_evolve,
            partition_cols,
            include_keys,
            keys_pre_grouped,
            max_rows_per_file,
            approx_bytes_per_file,
            file_format,
            unified_args,
            mkdir // 通常在 UC 环境下这是 false，远端存储不需要本地 mkdir
        )?;
        Ok(())
    })
}