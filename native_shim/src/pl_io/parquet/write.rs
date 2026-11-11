use crate::lazy::anonymous_scan::{AnonymousScanMode, resolve_anonymous_scans};
use polars::prelude::*;
use std::os::raw::c_char;
use crate::pl_io::ffi_buffer::FfiBuffer;
use crate::pl_io::callback_writer::{FfiWriteCallback, build_writer_sink_destination, with_callback_writer};
use crate::pl_io::io_utils::{build_memory_sink_destination, build_partitioned_destination, build_unified_sink_args};
use crate::pl_io::parquet::parquet_utils::build_parquet_write_options;
use std::ffi::c_void;
use crate::types::{LazyFrameContext, SelectorContext};
use crate::utils::ptr_to_str;

// ==========================================
// Write&Sink Parquet
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
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
        let ctx = unsafe { Box::from_raw(lf_ptr) };
        let lf = resolve_anonymous_scans(ctx.inner, AnonymousScanMode::Spool)?;

        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let write_options_arc = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size,
            compat_level
        )?;
        
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir,
                maintain_order,
                sync_on_close,
                cloud_provider,
                cloud_retries,
                cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms,
                cloud_retry_max_backoff_ms,
                cloud_cache_ttl,
                cloud_keys,
                cloud_values,
                cloud_len
            )
        };

        let target = SinkTarget::Path(PlRefPath::from(path_str));
        let destination = SinkDestination::File { target };
        let file_format = FileWriteFormat::Parquet(write_options_arc);

        lf.sink(destination, file_format, unified_args)?
          .collect()?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet_partitioned(
    lf_ptr: *mut LazyFrameContext,
    base_path_ptr: *const c_char,
    
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
        let mut lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        let schema = lf_ctx.inner.collect_schema()?;

        let destination = unsafe {
            build_partitioned_destination(
                base_path_ptr,
                ".parquet", 
                &schema,
                partition_by_ptr,
                include_keys,
                keys_pre_grouped,
                max_rows_per_file,
                approx_bytes_per_file
            )?
        };

        let write_options_arc = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size,
            compat_level
        )?;
        let file_format = FileWriteFormat::Parquet(write_options_arc);

        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir,
                maintain_order,
                sync_on_close,
                cloud_provider,
                cloud_retries,
                cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms,
                cloud_retry_max_backoff_ms,
                cloud_cache_ttl,
                cloud_keys,
                cloud_values,
                cloud_len
            )
        };

        let _ = resolve_anonymous_scans(lf_ctx.inner, AnonymousScanMode::Spool)?
            .sink(destination, file_format, unified_args)?
            .collect()?;

        Ok(())
    })
}

/// Shared body of the in-memory / callback Parquet sinks.
fn sink_parquet_to(
    lf_ctx: Box<LazyFrameContext>,
    destination: SinkDestination,
    compression: u8,
    compression_level: i32,
    statistics: bool,
    row_group_size: usize,
    data_page_size: usize,
    compat_level: i32,
    maintain_order: bool,
) -> PolarsResult<()> {
    let write_options_arc = build_parquet_write_options(
        compression,
        compression_level,
        statistics,
        row_group_size,
        data_page_size,
        compat_level
    )?;
    let file_format = FileWriteFormat::Parquet(write_options_arc);

    let unified_args = UnifiedSinkArgs {
        mkdir: false,
        maintain_order,
        sync_on_close: Default::default(),
        cloud_options: None,
        sinked_paths_callback:None
    };

    let _ = resolve_anonymous_scans(lf_ctx.inner, AnonymousScanMode::Spool)?
        .sink(destination, file_format, unified_args)?
        .collect()?;
    Ok(())
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet_memory(
    lf_ptr: *mut LazyFrameContext,
    out_buffer: *mut FfiBuffer,
    
    // --- Parquet Options ---
    compression: u8,        
    compression_level: i32, 
    statistics: bool,       
    row_group_size: usize,  
    data_page_size: usize,
    compat_level: i32,
    
    // --- Unified Options ---
    maintain_order: bool,
) {
    ffi_try_void!({
        if lf_ptr.is_null() || out_buffer.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to memory sink".into()));
        }

        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        let (mem_writer, destination) = build_memory_sink_destination();

        sink_parquet_to(lf_ctx, destination, compression, compression_level, statistics,
            row_group_size, data_page_size, compat_level, maintain_order)?;

        let vec = mem_writer.into_inner();
        let mut vec = std::mem::ManuallyDrop::new(vec);

        unsafe {
            (*out_buffer).data = vec.as_mut_ptr();
            (*out_buffer).len = vec.len();
            (*out_buffer).capacity = vec.capacity();
        }

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet_callback(
    lf_ptr: *mut LazyFrameContext,
    callback: FfiWriteCallback,
    user_data: *mut c_void,
    buffer_size: usize,

    // --- Parquet Options ---
    compression: u8,
    compression_level: i32,
    statistics: bool,
    row_group_size: usize,
    data_page_size: usize,
    compat_level: i32,

    // --- Unified Options ---
    maintain_order: bool,
) {
    ffi_try_void!({
        if lf_ptr.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to callback sink".into()));
        }

        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        with_callback_writer(callback, user_data, buffer_size, |w| {
            sink_parquet_to(lf_ctx, build_writer_sink_destination(w), compression, compression_level,
                statistics, row_group_size, data_page_size, compat_level, maintain_order)
        })
    })
}

// ==========================================
// Partitioned sink whose file paths the host chooses
// ==========================================

/// Host callback naming the next file of a partition.
///
/// * `user`      - opaque host state
/// * `keys`      - the partition key values (one row, key columns only); owned by the host from here on
/// * `index`     - the index of the file within its partition
/// * `out_path`  - buffer of `capacity` bytes for the UTF-8 path, relative to the base path
/// * `out_len`   - receives the path length
///
/// Returns 0 on success, non-zero on failure (the host keeps the actual exception).
pub type FfiFilePathCallback = extern "C" fn(
    user: *mut c_void,
    keys: *mut crate::types::DataFrameContext,
    index: usize,
    out_path: *mut u8,
    capacity: usize,
    out_len: *mut usize,
) -> i32;

/// The host state, disabled once the FFI call returns: Polars may keep the callback beyond it.
struct HostPathProvider {
    callback: FfiFilePathCallback,
    user: *mut c_void,
    active: bool,
}

// The host callback may be invoked from any thread; calls are serialized by the Mutex around this.
unsafe impl Send for HostPathProvider {}

/// Sinks `lf` as Parquet files under `base_path` (local directory or cloud URL), split by the partition key
/// columns (left out of the files) and by size, streaming. Every file's path comes from the host callback.
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet_host_paths(
    lf_ptr: *mut LazyFrameContext,
    base_path_ptr: *const c_char,
    key_names: *const *const c_char,
    key_len: usize,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,
    path_callback: FfiFilePathCallback,
    path_user: *mut c_void,

    // --- Parquet Options ---
    compression: u8,
    compression_level: i32,
    row_group_size: usize,
    data_page_size: usize,

    // --- Unified Options ---
    maintain_order: bool,
    sync_on_close: u8,

    // --- Cloud Params ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,
    cloud_retry_init_backoff_ms: u64,
    cloud_retry_max_backoff_ms: u64,
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize,

    // Parquet field ids (null: none), see `with_field_ids`.
    field_ids: *const c_char,
) {
    ffi_try_void!({
        let provider = Arc::new(std::sync::Mutex::new(HostPathProvider { callback: path_callback, user: path_user, active: true }));
        // On every way out (also a panic), the host state may be freed: Polars must not call back any more.
        let _disable = DisableOnDrop(provider.clone());
        if lf_ptr.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to the host-path sink".into()));
        }
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let base_path = ptr_to_str(base_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let mut keys: Vec<Expr> = Vec::with_capacity(key_len);
        let mut key_set: Vec<String> = Vec::with_capacity(key_len);
        for i in 0..key_len {
            let name = unsafe { ptr_to_str(*key_names.add(i)) }.map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            keys.push(col(name));
            key_set.push(name.to_string());
        }
        let partition_strategy = if keys.is_empty() {
            PartitionStrategy::FileSize
        } else {
            PartitionStrategy::Keyed { keys, include_keys: false, keys_pre_grouped }
        };

        let for_polars = provider.clone();
        let function: file_provider::FileProviderFunction = PlanCallback::Rust(SpecialEq::new(Arc::new(
            move |args: file_provider::FileProviderArgs| -> PolarsResult<file_provider::FileProviderReturn> {
                let guard = for_polars.lock().unwrap_or_else(|p| p.into_inner());
                if !guard.active {
                    return Err(PolarsError::ComputeError("The host path callback was used after the sink returned".into()));
                }
                let keys = Box::into_raw(Box::new(crate::types::DataFrameContext { df: args.partition_keys.as_ref().clone() }));
                let mut buffer = vec![0u8; 4096];
                let mut len: usize = 0;
                let code = (guard.callback)(guard.user, keys, args.index_in_partition, buffer.as_mut_ptr(), buffer.len(), &mut len);
                if code != 0 {
                    return Err(PolarsError::ComputeError(format!("The host path callback failed (code {code})").into()));
                }
                buffer.truncate(len.min(4096));
                let path = String::from_utf8(buffer).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                Ok(file_provider::FileProviderReturn::Path(path))
            },
        )));

        let destination = SinkDestination::Partitioned {
            base_path: PlRefPath::from(base_path),
            file_path_provider: Some(file_provider::FileProviderType::Function(function)),
            partition_strategy,
            max_rows_per_file: if max_rows_per_file == 0 { IdxSize::MAX } else { max_rows_per_file as IdxSize },
            approximate_bytes_per_file: if approx_bytes_per_file == 0 { u64::MAX } else { approx_bytes_per_file },
        };

        // Statistics always on: the host reads them back from the footers.
        let mut write_options = build_parquet_write_options(compression, compression_level, true, row_group_size, data_page_size, 0)?;
        let mut lf = resolve_anonymous_scans(lf_ctx.inner, AnonymousScanMode::Spool)?;
        if !field_ids.is_null() {
            let ids = ptr_to_str(field_ids).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            // The files hold every column but the partition keys.
            let mut file_schema = (*lf.collect_schema()?).clone();
            for key in &key_set {
                file_schema.shift_remove(key.as_str());
            }
            let compat = write_options.compat_level();
            Arc::make_mut(&mut write_options).arrow_schema = Some(Arc::new(with_field_ids(&file_schema, ids, compat)?));
        }
        let unified_args = unsafe {
            build_unified_sink_args(
                false, maintain_order, sync_on_close, cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl, cloud_keys, cloud_values, cloud_len,
            )
        };
        lf
            .sink(destination, FileWriteFormat::Parquet(write_options), unified_args)?
            .collect_with_engine(Engine::Streaming)?;
        Ok(())
    })
}

struct DisableOnDrop(Arc<std::sync::Mutex<HostPathProvider>>);

impl Drop for DisableOnDrop {
    fn drop(&mut self) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).active = false;
    }
}

/// The separator of the steps of a field path in `with_field_ids`.
const PATH_SEPARATOR: char = '\u{1f}';

/// The arrow schema the Parquet writer would use for `schema`, with Parquet field ids (as Delta's column mapping
/// needs them) on the fields `ids` names: `path TAB id` lines, where a path is the field names from the top joined
/// by U+001F and `[]` stands for a list's element. Fields not named get no id.
fn with_field_ids(schema: &Schema, ids: &str, compat: CompatLevel) -> PolarsResult<ArrowSchema> {
    let mut by_path = std::collections::HashMap::new();
    for line in ids.lines().filter(|l| !l.is_empty()) {
        let (path, id) = line
            .split_once('\t')
            .ok_or_else(|| PolarsError::ComputeError(format!("Malformed field id line '{line}'").into()))?;
        let id: i32 = id.parse().map_err(|_| PolarsError::ComputeError(format!("Malformed field id '{id}'").into()))?;
        by_path.insert(path.to_string(), id);
    }
    let arrow = polars_io::schema_to_arrow_checked(schema, compat, "")?;
    let mut used = 0usize;
    let fields: Vec<ArrowField> = arrow.iter_values().map(|f| annotate(f, "", &by_path, &mut used)).collect();
    if used != by_path.len() {
        return Err(PolarsError::ComputeError(
            format!("{} of the {} field ids name no field of the file", by_path.len() - used, by_path.len()).into(),
        ));
    }
    Ok(ArrowSchema::from_iter(fields))
}

fn annotate(field: &ArrowField, parent: &str, ids: &std::collections::HashMap<String, i32>, used: &mut usize) -> ArrowField {
    let path = if parent.is_empty() { field.name.to_string() } else { format!("{parent}{PATH_SEPARATOR}{}", field.name) };
    annotate_at(field, &path, ids, used)
}

fn annotate_at(field: &ArrowField, path: &str, ids: &std::collections::HashMap<String, i32>, used: &mut usize) -> ArrowField {
    let dtype = match &field.dtype {
        ArrowDataType::Struct(children) => {
            ArrowDataType::Struct(children.iter().map(|c| annotate(c, path, ids, used)).collect())
        },
        ArrowDataType::LargeList(child) => {
            ArrowDataType::LargeList(Box::new(annotate_at(child, &format!("{path}{PATH_SEPARATOR}[]"), ids, used)))
        },
        ArrowDataType::List(child) => {
            ArrowDataType::List(Box::new(annotate_at(child, &format!("{path}{PATH_SEPARATOR}[]"), ids, used)))
        },
        ArrowDataType::FixedSizeList(child, size) => ArrowDataType::FixedSizeList(
            Box::new(annotate_at(child, &format!("{path}{PATH_SEPARATOR}[]"), ids, used)),
            *size,
        ),
        other => other.clone(),
    };
    let mut out = ArrowField { name: field.name.clone(), dtype, is_nullable: field.is_nullable, metadata: field.metadata.clone() };
    if let Some(id) = ids.get(path) {
        *used += 1;
        let mut metadata = field.metadata.as_deref().cloned().unwrap_or_default();
        metadata.insert("PARQUET:field_id".into(), id.to_string().into());
        out.metadata = Some(Arc::new(metadata));
    }
    out
}
