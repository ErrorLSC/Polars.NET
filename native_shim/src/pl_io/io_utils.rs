use polars::prelude::file_provider::HivePathProvider;
use polars::prelude::*;
use polars_io::cloud::CloudOptions;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::Arc;
use crate::pl_io::ffi_buffer::SharedMemoryWriter;
use crate::types::SelectorContext;
use crate::utils::{map_sync_on_close, ptr_to_str};

/// Above this many rows a schema-inference length means the whole input, as null does: Polars preallocates the
/// rows it samples, and a "read everything" value such as `int.MaxValue` would abort the process on allocation.
const MAX_INFER_SCHEMA_LENGTH: usize = 1 << 24;

/// The rows to infer a schema from, as Polars takes them: null (and values above
/// [`MAX_INFER_SCHEMA_LENGTH`]) mean the whole input, `Some(0)` reads every CSV column as String.
pub(crate) unsafe fn infer_schema_length(ptr: *const usize) -> Option<usize> {
    if ptr.is_null() {
        return None;
    }
    let n = unsafe { *ptr };
    (n <= MAX_INFER_SCHEMA_LENGTH).then_some(n)
}

fn ms_to_duration(ms: u64) -> Option<std::time::Duration> {
    if ms == 0 {
        None
    } else {
        Some(std::time::Duration::from_millis(ms))
    }
}

/// A credential provider that always hands out `token` (it never expires; a vended token is replaced by asking
/// the catalog again).
fn static_gcs_token(token: String) -> polars_io::cloud::credential_provider::PlCredentialProvider {
    use polars_io::cloud::credential_provider::{GcpCredential, ObjectStoreCredential, PlCredentialProvider};
    let credential = Arc::new(GcpCredential { bearer: token });
    PlCredentialProvider::from_func(move || {
        let credential = credential.clone();
        Box::pin(async move { Ok((ObjectStoreCredential::Gcp(credential), u64::MAX)) })
    })
}

/// Core internal builder that parses cloud scheme, untyped parameters and sets retry/cache policies.
/// Returns raw `Option<CloudOptions>` without Arc wrapping, suitable for direct use by `ScanArgsParquet`.
pub(crate) fn build_cloud_options_from_params(
    provider_code: u8,
    retries: usize,
    retry_timeout_ms: u64,      
    retry_init_backoff_ms: u64, 
    retry_max_backoff_ms: u64,  
    cache_ttl: u64,
    params: Vec<(String, String)>,
) -> Option<CloudOptions> {
    if provider_code == 0 {
        return None;
    }

    let scheme = match provider_code {
        1 => Some(CloudScheme::S3),     // AWS, S3, S3a 
        2 => Some(CloudScheme::Azure),  // Azure, Abfs, Abfss 
        3 => Some(CloudScheme::Gcs),    // Gcs, Gs 
        4 => Some(CloudScheme::Http),   // Http, Https
        5 => Some(CloudScheme::Hf),     // Hugging Face
        _ => {
            eprintln!("Warning: Unknown cloud provider code: {}", provider_code);
            None
        }
    };

    // Credentials the host renews: the source's id is taken out before the library reads the configuration (it
    // would refuse the key and drop every option); the provider then serves the credentials, over any static ones.
    let mut params = params;
    let host_source = params
        .iter()
        .position(|(k, _)| k == crate::pl_io::host_credentials::CREDENTIAL_SOURCE_KEY)
        .map(|i| params.remove(i).1);
    let host_source = match host_source.map(|v| v.parse::<u64>()) {
        Some(Ok(id)) => Some(id),
        Some(Err(_)) => {
            eprintln!("Cloud config error: the host credential source id is not a number");
            return None;
        },
        None => None,
    };

    // A static OAuth token (as Unity Catalog vends them for GCS) has no configuration key in the object store
    // library; it is handed over as a credential provider, which also leaves the GOOGLE_* environment out.
    let gcs_token = if matches!(scheme, Some(CloudScheme::Gcs)) {
        params
            .iter()
            .position(|(k, _)| k.eq_ignore_ascii_case("google_oauth_token"))
            .map(|i| params.remove(i).1)
    } else {
        None
    };

    let mut opts = match CloudOptions::from_untyped_config(scheme, params) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Cloud config error: {}", e);
            return None;
        }
    };

    if let Some(id) = host_source {
        opts = opts.with_credential_provider(Some(crate::pl_io::host_credentials::host_credential_provider(id, scheme)));
    } else if let Some(token) = gcs_token {
        opts = opts.with_credential_provider(Some(static_gcs_token(token)));
    }

    opts.retry_config.max_retries = Some(retries);
    opts.retry_config.retry_timeout = ms_to_duration(retry_timeout_ms);
    opts.retry_config.retry_init_backoff = ms_to_duration(retry_init_backoff_ms);
    opts.retry_config.retry_max_backoff = ms_to_duration(retry_max_backoff_ms);
    opts.file_cache_ttl = cache_ttl;

    Some(opts)
}


/// FFI helper: Parses C raw pointers and delegates to the core builder.
pub(crate) unsafe fn build_cloud_options(
    provider_code: u8,
    retries: usize,
    retry_timeout_ms: u64,      
    retry_init_backoff_ms: u64, 
    retry_max_backoff_ms: u64,  
    cache_ttl: u64,
    keys_ptr: *const *const c_char,
    vals_ptr: *const *const c_char,
    len: usize,
) -> Option<CloudOptions> {
    if provider_code == 0 {
        return None;
    }

    let mut params = Vec::with_capacity(len);
    if !keys_ptr.is_null() && !vals_ptr.is_null() && len > 0 {
        let keys_slice = unsafe { std::slice::from_raw_parts(keys_ptr, len) };
        let vals_slice = unsafe { std::slice::from_raw_parts(vals_ptr, len) };

        for i in 0..len {
            let k_ptr = keys_slice[i];
            let v_ptr = vals_slice[i];
            if !k_ptr.is_null() && !v_ptr.is_null() {
                let k = unsafe { CStr::from_ptr(k_ptr).to_string_lossy().into_owned() };
                let v = unsafe { CStr::from_ptr(v_ptr).to_string_lossy().into_owned() };
                params.push((k, v));
            }
        }
    }

    build_cloud_options_from_params(
        provider_code,
        retries,
        retry_timeout_ms,
        retry_init_backoff_ms,
        retry_max_backoff_ms,
        cache_ttl,
        params,
    )
}


/// FFI helper: Builds `UnifiedSinkArgs` from raw pointers.
#[inline]
pub(crate) unsafe fn build_unified_sink_args(
    mkdir: bool,
    maintain_order: bool,
    sync_on_close_code: u8,
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,
    cloud_retry_init_backoff_ms: u64,
    cloud_retry_max_backoff_ms: u64,
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize,
) -> UnifiedSinkArgs {
    let cloud_options = unsafe {
        build_cloud_options(
            cloud_provider,
            cloud_retries,
            cloud_retry_timeout_ms,
            cloud_retry_init_backoff_ms,
            cloud_retry_max_backoff_ms,
            cloud_cache_ttl,
            cloud_keys,
            cloud_values,
            cloud_len,
        )
        .map(Arc::new)
    };

    let sync_on_close = map_sync_on_close(sync_on_close_code);

    UnifiedSinkArgs {
        mkdir,
        maintain_order,
        sync_on_close,
        cloud_options,
        sinked_paths_callback: None,
    }
}

pub(crate) unsafe fn build_partitioned_destination(
    base_path_ptr: *const c_char,
    file_extension: &str, // ".parquet", ".ipc", ".csv"
    schema: &Schema,      
    partition_by_ptr: *mut SelectorContext, // nullable
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,
) -> PolarsResult<SinkDestination> {
    let base_path_str = ptr_to_str(base_path_ptr)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

    let strategy = if !partition_by_ptr.is_null() {
        let selector_ctx = unsafe { Box::from_raw(partition_by_ptr) };
        let ignored = PlIndexSet::new();
        let names_set = selector_ctx.inner.into_columns(schema, &ignored)?;
        
        let keys: Vec<Expr> = names_set.iter()
            .map(|name| col(name.clone()))
            .collect();

        if keys.is_empty() {
            PartitionStrategy::FileSize
        } else {
            PartitionStrategy::Keyed {
                keys,
                include_keys,
                keys_pre_grouped,
            }
        }
    } else {
        PartitionStrategy::FileSize
    };

    let hive_provider = HivePathProvider {
        extension: PlSmallStr::from_str(file_extension),
    };
    let file_path_provider = Some(file_provider::FileProviderType::Hive(hive_provider));

    Ok(SinkDestination::Partitioned {
        base_path: PlRefPath::from(base_path_str),
        file_path_provider,
        partition_strategy: strategy,
        max_rows_per_file: max_rows_per_file as IdxSize,
        approximate_bytes_per_file: approx_bytes_per_file,
    })
}

pub(crate) fn build_memory_sink_destination() -> (SharedMemoryWriter, SinkDestination) {
    let mem_writer = SharedMemoryWriter::new();
    let destination = crate::pl_io::callback_writer::build_writer_sink_destination(mem_writer.clone());
    (mem_writer, destination)
}