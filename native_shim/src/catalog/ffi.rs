// native_shim/src/catalog/ffi.rs
use std::ffi::c_char;
use polars::error::{PolarsError, PolarsResult};
use polars_io::catalog::unity::{client::{CatalogClient, CatalogClientBuilder}, models::{DataSourceFormat, TableType}};

use crate::{delta::utils::get_runtime, types::SchemaContext, utils::ptr_to_str};
// ==========================================
// 1. Rust 核心结构体
// ==========================================
/// 包装官方的 CatalogClient，作为一个 Opaque Pointer (不透明指针) 传给 C#
pub struct CatalogContext {
    pub client: CatalogClient,
    pub workspace_url: String, // 留着备用，方便后续组装完整路径
}

impl CatalogContext {
    pub fn new(workspace_url: String, bearer_token: String) -> PolarsResult<Self> {
        let client = CatalogClientBuilder::new()
            .with_workspace_url(&workspace_url)
            .with_bearer_token(bearer_token)
            .build()?;
            
        Ok(Self { 
            client,
            workspace_url 
        })
    }
}

// ==========================================
// 2. FFI 生命周期暴露
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_unity_new(
    workspace_url_ptr: *const c_char,
    bearer_token_ptr: *const c_char,
) -> *mut CatalogContext {
    ffi_try!({
        let url = ptr_to_str(workspace_url_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        let token = ptr_to_str(bearer_token_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        
        let ctx = CatalogContext::new(url, token)?;
        Ok(Box::into_raw(Box::new(ctx)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_unity_free(ptr: *mut CatalogContext) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_create_table(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    schema_ptr: *mut SchemaContext, // 直接接收 C# 传来的 Schema
    table_type: u8, // 0: Managed, 1: External
    storage_location_ptr: *const c_char, // External 表需传物理路径
) {
    ffi_try_void!({
        let ctx = unsafe { &*ctx_ptr };
        let catalog_name = ptr_to_str(catalog_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        let schema_name = ptr_to_str(schema_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        let table_name = ptr_to_str(table_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        
        // 提取 Schema
        let schema = unsafe { &(*schema_ptr).schema };

        let t_type = match table_type {
            0 => TableType::Managed,
            1 => TableType::External,
            _ => return Err(PolarsError::ComputeError("Invalid table_type".into())),
        };

        // 处理可选的存储路径
        let storage_location = if storage_location_ptr.is_null() {
            None
        } else {
            Some(ptr_to_str(storage_location_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string())
        };

        let rt = crate::delta::utils::get_runtime();

        // 执行异步建表
        rt.block_on(async {
            let mut empty_props = std::iter::empty();
            
            // 调用咱们刚才看到的底层 API
            ctx.client.create_table(
                &catalog_name,
                &schema_name,
                &table_name,
                Some(schema), // 【魔法核心】：Polars 自动翻译类型！
                &t_type,
                Some(&DataSourceFormat::Delta), // 固定为 Delta 格式
                None, // comment 暂不暴露
                storage_location.as_deref(),
                &mut empty_props
            ).await
            .map_err(|e| PolarsError::ComputeError(format!("Failed to create table: {}", e).into()))?;
            
            Ok::<_, PolarsError>(())
        })?;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_delete_table(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
) {
    ffi_try_void!({
        let ctx = unsafe { &*ctx_ptr };
        let catalog_name = ptr_to_str(catalog_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        let schema_name = ptr_to_str(schema_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        let table_name = ptr_to_str(table_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();

        let rt = get_runtime();

        rt.block_on(async {
            ctx.client.delete_table(&catalog_name, &schema_name, &table_name).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to delete table: {}", e).into()))?;
            Ok::<_, PolarsError>(())
        })?;
        Ok(())
    })
}