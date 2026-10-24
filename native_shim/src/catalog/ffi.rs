// native_shim/src/catalog/ffi.rs
use std::ffi::c_char;
use polars::error::{PolarsError, PolarsResult};
use polars_io::catalog::unity::{client::{CatalogClient, CatalogClientBuilder}, models::{DataSourceFormat, TableType}};

use crate::{delta::utils::get_runtime, types::SchemaContext, utils::ptr_to_str};

pub struct CatalogContext {
    pub client: CatalogClient,

}

impl CatalogContext {
    pub fn new(workspace_url: String, bearer_token: String) -> PolarsResult<Self> {
        let client = CatalogClientBuilder::new()
            .with_workspace_url(&workspace_url)
            .with_bearer_token(bearer_token)
            .build()?;
            
        Ok(Self { 
            client,
        })
    }
}

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
    schema_ptr: *mut SchemaContext, 
    table_type: u8, 
    storage_location_ptr: *const c_char, 
) {
    ffi_try_void!({
        let (ctx, catalog_name, schema_name, table_name) = crate::catalog::utils::parse_catalog_table_args(
            ctx_ptr, catalog_name_ptr, schema_name_ptr, table_name_ptr,
        )?;
        let schema = unsafe {
            if schema_ptr.is_null() { return Err(PolarsError::ComputeError("Schema pointer is null".into())); }
            &(*schema_ptr).schema
        };

        let t_type = match table_type {
            0 => TableType::Managed,
            1 => TableType::External,
            _ => return Err(PolarsError::ComputeError("Invalid table_type".into())),
        };

        let storage_location = if storage_location_ptr.is_null() {
            None
        } else {
            Some(ptr_to_str(storage_location_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string())
        };

        let rt = crate::delta::utils::get_runtime();
        rt.block_on(async {
            let mut empty_props = std::iter::empty();
            ctx.client.create_table(
                catalog_name,
                schema_name,
                table_name,
                Some(schema), 
                &t_type,
                Some(&DataSourceFormat::Delta), 
                None, 
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
        let (ctx, catalog_name, schema_name, table_name) = crate::catalog::utils::parse_catalog_table_args(
            ctx_ptr, catalog_name_ptr, schema_name_ptr, table_name_ptr,
        )?;

        let rt = get_runtime();
        rt.block_on(async {
            ctx.client.delete_table(catalog_name, schema_name, table_name).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to delete table: {}", e).into()))?;
            Ok::<_, PolarsError>(())
        })?;
        Ok(())
    })
}