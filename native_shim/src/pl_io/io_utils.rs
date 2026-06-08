use polars::prelude::file::Writeable;
use polars::prelude::file_provider::HivePathProvider;
use polars::prelude::*;
use polars_io::cloud::CloudOptions;
// use polars_plan::dsl::sink::{SinkedPathsCallback, SinkedPathsCallbackArgs};
use std::ffi::{CStr};
use std::os::raw::c_char;
use std::sync::Mutex;
use crate::pl_io::ffi_buffer::SharedMemoryWriter;
use crate::types::SelectorContext;
use crate::utils::{map_sync_on_close,ptr_to_str};
// use crate::utils::{FreeHandleCallback, FreeStringCallback, GcHandleGuard, MapStringCallback};

fn ms_to_duration(ms: u64) -> Option<std::time::Duration> {
    if ms == 0 {
        None
    } else {
        Some(std::time::Duration::from_millis(ms))
    }
}

pub(crate) unsafe fn build_cloud_options(
    provider_code: u8,
    retries: usize,
    retry_timeout_ms: u64,      
    retry_init_backoff_ms: u64, 
    retry_max_backoff_ms: u64,  
    cache_ttl: u64,
    keys_ptr: *const *const c_char,
    vals_ptr: *const *const c_char,
    len: usize
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

    let mut params = Vec::with_capacity(len);
    if !keys_ptr.is_null() && !vals_ptr.is_null() && len > 0 {
        let keys_slice = unsafe {std::slice::from_raw_parts(keys_ptr, len)};
        let vals_slice = unsafe {std::slice::from_raw_parts(vals_ptr, len)};

        for i in 0..len {
            let k_ptr = keys_slice[i];
            let v_ptr = vals_slice[i];
            if !k_ptr.is_null() && !v_ptr.is_null() {
                let k = unsafe {CStr::from_ptr(k_ptr).to_string_lossy().into_owned()};
                let v = unsafe {CStr::from_ptr(v_ptr).to_string_lossy().into_owned()};
                params.push((k, v));
            }
        }
    }

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

// pub(crate) fn build_sinked_paths_callback(
//     callback: MapStringCallback,
//     free_string_cb: FreeStringCallback,
//     gc_handle_ptr: *mut c_void,
//     free_handle_cb: FreeHandleCallback,
// ) -> SinkedPathsCallback {
//     let handle_guard = GcHandleGuard {
//         handle_ptr: gc_handle_ptr,
//         free_cb: free_handle_cb,
//     };

//     let inner_plan_callback = PlanCallback::Rust(SpecialEq::new(Arc::new(
//         move |args: SinkedPathsCallbackArgs| -> PolarsResult<()> {
//             let _keep_alive = &handle_guard;

//             let paths_joined = args.path_info_list
//                 .iter()
//                 .map(|info| info.path.as_str())
//                 .collect::<Vec<&str>>()
//                 .join(";");

//             let c_paths = std::ffi::CString::new(paths_joined)
//                 .map_err(|_| PolarsError::ComputeError("Invalid UTF-8 path list".into()))?;

//             let result_ptr = callback(c_paths.as_ptr());
            
//             if !result_ptr.is_null() {
//                 free_string_cb(result_ptr);
//             }

//             Ok(())
//         }
//     )));

//     SinkedPathsCallback::Callback(inner_plan_callback)
// }

#[inline]
pub(crate) unsafe fn build_unified_sink_args(
    mkdir: bool,
    maintain_order: bool,
    sync_on_close_code: u8,
    // --- Cloud Options (Flattened) ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,
    cloud_retry_init_backoff_ms: u64,
    cloud_retry_max_backoff_ms: u64,
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize,
    // path_callback: Option<MapStringCallback>, 
    // free_string_cb: Option<FreeStringCallback>,
    // gc_handle_ptr: *mut c_void,
    // free_handle_cb: Option<FreeHandleCallback>,
) -> UnifiedSinkArgs {
    
    // CloudOptions
    let cloud_options = unsafe {build_cloud_options(
        cloud_provider,
        cloud_retries,
        cloud_retry_timeout_ms,
        cloud_retry_init_backoff_ms,
        cloud_retry_max_backoff_ms,
        cloud_cache_ttl,
        cloud_keys,
        cloud_values,
        cloud_len
    ).map(Arc::new)};

    // SyncOnClose
    let sync_on_close = map_sync_on_close(sync_on_close_code);

    // let sinked_paths_callback = if let (Some(cb), Some(free_str), Some(free_handle)) = (path_callback, free_string_cb, free_handle_cb) {
    //         Some(build_sinked_paths_callback(
    //             cb,
    //             free_str,
    //             gc_handle_ptr,
    //             free_handle,
    //         ))
    //     } else {
    //         None
    //     };
    // Return
    UnifiedSinkArgs {
        mkdir,
        maintain_order,
        sync_on_close,
        cloud_options,
        sinked_paths_callback:None
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
    
    // Parse base path
    let base_path_str = ptr_to_str(base_path_ptr)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

    // Partition Strategy (Keyed vs FileSize)
    let strategy = if !partition_by_ptr.is_null() {
        // A. Keyed Strategy (Hive Style: key=value/...)
        let selector_ctx = unsafe {Box::from_raw(partition_by_ptr)};
        
        let ignored = PlHashSet::new();
        // Use schema to analyze column name
        let names_set = selector_ctx.inner.into_columns(schema, &ignored)?;
        
        // Convert to col("name") expression
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
        // FileSize Strategy
        PartitionStrategy::FileSize
    };

    // Build HivePathProvider
    let hive_provider = HivePathProvider {
        extension: PlSmallStr::from_str(file_extension),
    };
    let file_path_provider = Some(file_provider::FileProviderType::Hive(hive_provider));

    // Return SinkDestination
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
    let writeable = Writeable::Dyn(Box::new(mem_writer.clone()));
    let dyn_target = SpecialEq::new(Arc::new(Mutex::new(Some(writeable))));
    
    let target = SinkTarget::Dyn(dyn_target);
    let destination = SinkDestination::File { target };
    
    (mem_writer, destination)
}

