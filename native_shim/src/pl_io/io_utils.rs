use polars::prelude::file::Writable;
use polars::prelude::file_provider::HivePathProvider;
use polars::prelude::*;
use polars_io::cloud::CloudOptions;
use std::ffi::CStr;
use std::os::raw::c_char;
use std::sync::{Arc, Mutex};
use crate::pl_io::ffi_buffer::SharedMemoryWriter;
use crate::types::SelectorContext;
use crate::utils::{map_sync_on_close, ptr_to_str};

fn ms_to_duration(ms: u64) -> Option<std::time::Duration> {
    if ms == 0 {
        None
    } else {
        Some(std::time::Duration::from_millis(ms))
    }
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

    let mut opts = match CloudOptions::from_untyped_config(scheme, params) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Cloud config error: {}", e);
            return None;
        }
    };

    opts.retry_config.max_retries = Some(retries);
    opts.retry_config.retry_timeout = ms_to_duration(retry_timeout_ms);
    opts.retry_config.retry_init_backoff = ms_to_duration(retry_init_backoff_ms);
    opts.retry_config.retry_max_backoff = ms_to_duration(retry_max_backoff_ms);
    opts.file_cache_ttl = cache_ttl;

    Some(opts)
}

/// Builds `CloudOptions` directly from an options HashMap.
/// Output type matches `polars_lazy::scan::parquet::ScanArgsParquet.cloud_options` (`Option<CloudOptions>`).
pub(crate) fn build_cloud_options_from_map(
    provider_code: u8,
    retries: usize,
    retry_timeout_ms: u64,      
    retry_init_backoff_ms: u64, 
    retry_max_backoff_ms: u64,  
    cache_ttl: u64,
    options: &std::collections::HashMap<String, String>,
) -> Option<CloudOptions> {
    if provider_code == 0 {
        return None;
    }

    let params: Vec<(String, String)> = options
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

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

/// Builds `UnifiedSinkArgs` from an options HashMap, wrapping `CloudOptions` in `Arc` for sinks.
pub(crate) fn build_unified_sink_args_from_map(
    mkdir: bool,
    maintain_order: bool,
    sync_on_close_code: u8,
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,
    cloud_retry_init_backoff_ms: u64,
    cloud_retry_max_backoff_ms: u64,
    cloud_cache_ttl: u64,
    options: &std::collections::HashMap<String, String>,
) -> UnifiedSinkArgs {
    let cloud_options = build_cloud_options_from_map(
        cloud_provider,
        cloud_retries,
        cloud_retry_timeout_ms,
        cloud_retry_init_backoff_ms,
        cloud_retry_max_backoff_ms,
        cloud_cache_ttl,
        options,
    )
    .map(Arc::new);

    let sync_on_close = map_sync_on_close(sync_on_close_code);

    UnifiedSinkArgs {
        mkdir,
        maintain_order,
        sync_on_close,
        cloud_options,
        sinked_paths_callback: None,
    }
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
    let writable = Writable::Dyn(Box::new(mem_writer.clone()));
    let dyn_target = SpecialEq::new(Arc::new(Mutex::new(Some(writable))));
    
    let target = SinkTarget::Dyn(dyn_target);
    let destination = SinkDestination::File { target };
    
    (mem_writer, destination)
}