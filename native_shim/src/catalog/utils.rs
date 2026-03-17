use polars::error::PolarsError;
use polars_io::catalog::unity::models::{TableCredentialsVariants, TableInfo};

use crate::catalog::ffi::CatalogContext;

pub(crate) fn convert_catalog_creds(creds: TableCredentialsVariants) -> std::collections::HashMap<String, String> {
    let mut options = std::collections::HashMap::new();
    match creds {
        TableCredentialsVariants::Aws(aws) => {
            options.insert("aws_access_key_id".to_string(), aws.access_key_id);
            options.insert("aws_secret_access_key".to_string(), aws.secret_access_key);
            if let Some(token) = aws.session_token {
                options.insert("aws_session_token".to_string(), token);
            }
        },
        TableCredentialsVariants::Azure(azure) => {
            options.insert("azure_storage_sas_token".to_string(), azure.sas_token);
        },
        TableCredentialsVariants::Gcp(gcp) => {
            options.insert("google_oauth_token".to_string(), gcp.oauth_token);
        }
    }
    options
}

pub(crate) async fn get_catalog_table_info_and_options(
    ctx: &CatalogContext,
    catalog_name: &str,
    schema_name: &str,
    table_name: &str,
    needs_write: bool, 
    mut base_options: std::collections::HashMap<String, String>,
) -> Result<(TableInfo, url::Url, std::collections::HashMap<String, String>), PolarsError> {
    
    let info = ctx.client.get_table_info(catalog_name, schema_name, table_name).await
        .map_err(|e| PolarsError::ComputeError(format!("Failed to get table info: {}", e).into()))?;
    
    let creds_wrapper = ctx.client.get_table_credentials(&info.table_id, needs_write).await
        .map_err(|e| PolarsError::ComputeError(format!("Failed to get credentials: {}", e).into()))?;
        
    let creds = creds_wrapper.into_enum().ok_or_else(|| {
        PolarsError::ComputeError("Unsupported or missing credentials".into())
    })?;

    base_options.extend(convert_catalog_creds(creds)); 

    let location_str = info.storage_location.clone().ok_or_else(|| {
        PolarsError::ComputeError("Table storage location is missing".into())
    })?;
    let table_url = url::Url::parse(&location_str).map_err(|_| PolarsError::ComputeError("Invalid URL".into()))?;

    Ok((info, table_url, base_options))
}

pub(crate) async fn load_catalog_table(
    ctx: &CatalogContext, catalog_name: &str, schema_name: &str, table_name: &str,
    needs_write: bool, base_options: std::collections::HashMap<String, String>,
) -> Result<(deltalake::DeltaTable, url::Url, std::collections::HashMap<String, String>), PolarsError> {
    
    let (_, url, options) = get_catalog_table_info_and_options(ctx, catalog_name, schema_name, table_name, needs_write, base_options).await?;
    let table = deltalake::DeltaTable::try_from_url_with_storage_options(url.clone(), options.clone())
        .await
        .map_err(|e| PolarsError::ComputeError(format!("Failed to load table: {}", e).into()))?;
        
    Ok((table, url, options))
}