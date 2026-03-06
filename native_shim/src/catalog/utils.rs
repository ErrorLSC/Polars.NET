use polars_io::catalog::unity::models::TableCredentialsVariants;

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