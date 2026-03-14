// use std::collections::HashMap;

// use polars::prelude::*;
// use polars_io::catalog::unity::client::CatalogClientBuilder;
// use url::Url;

// use crate::catalog::utils::convert_catalog_creds;

// // pub(crate) async fn get_catalog_table_info(
// //     workspace_url: String,
// //     bearer_token: String,
// //     catalog: &str,
// //     schema: &str,
// //     table: &str,
// //     write:bool
// // ) -> PolarsResult<(Url, HashMap<String, String>)> {
    
// //     // 1. 初始化 Unity Catalog Client (使用官方 Builder)
// //     let client = CatalogClientBuilder::new()
// //         .with_workspace_url(workspace_url)
// //         .with_bearer_token(bearer_token)
// //         .build()?;

// //     // 2. 获取 Table Info (获取物理路径)
// //     // 官方 API 直接接受分离的 catalog, namespace, table_name
// //     let table_info = client.get_table_info(catalog, schema, table).await
// //         .map_err(|e| PolarsError::ComputeError(format!("Failed to get table info: {}", e).into()))?;
        
// //     let location_str = table_info.storage_location.ok_or_else(|| {
// //         PolarsError::ComputeError("Table storage location is missing".into())
// //     })?;
    
// //     let table_url = Url::parse(&location_str).map_err(|_| {
// //         PolarsError::ComputeError(format!("Invalid storage location URL: {}", location_str).into())
// //     })?;

// //     // 3. 获取临时云凭证
// //     let creds_wrapper = client.get_table_credentials(&table_info.table_id, write).await
// //         .map_err(|e| PolarsError::ComputeError(format!("Failed to get credentials: {}", e).into()))?;
        
// //     let creds = creds_wrapper.into_enum().ok_or_else(|| {
// //         PolarsError::ComputeError("Unsupported or missing credentials".into())
// //     })?;

// //     Ok((table_url, convert_catalog_creds(creds)))
// // }