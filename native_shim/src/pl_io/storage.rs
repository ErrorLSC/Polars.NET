//! A small file-system view over an object store: list one directory, read a file or a range of it.
//! Used by managed code that needs to look at the files of a table itself (its log, its deletion vectors)
//! rather than hand a path to a scan.

use std::os::raw::c_char;

use object_store::{ObjectStore, ObjectStoreExt};
use polars::error::{PolarsError, PolarsResult};
use polars_core::runtime::ASYNC;
use polars_io::cloud::{build_object_store, object_path_from_str, PolarsObjectStore};
use polars_utils::pl_path::PlRefPath;

use crate::pl_io::ffi_buffer::FfiBuffer;
use crate::pl_io::io_utils::build_cloud_options;
use crate::utils::ptr_to_str;

pub struct StorageContext {
    store: PolarsObjectStore,
    /// The location of the root inside the store, without a trailing slash.
    root: String,
}

fn compute_err(e: impl std::fmt::Display) -> PolarsError {
    PolarsError::ComputeError(e.to_string().into())
}

impl StorageContext {
    fn path(&self, relative: &str) -> PolarsResult<object_store::path::Path> {
        let relative = relative.trim_matches('/');
        if relative.is_empty() {
            object_path_from_str(&self.root)
        } else {
            object_path_from_str(&format!("{}/{}", self.root, relative))
        }
    }
}

fn hand_out(mut bytes: Vec<u8>, out_buffer: *mut FfiBuffer) {
    unsafe {
        (*out_buffer).data = bytes.as_mut_ptr();
        (*out_buffer).len = bytes.len();
        (*out_buffer).capacity = bytes.capacity();
    }
    std::mem::forget(bytes);
}

/// Opens the store behind `root_url` (a local path or a cloud URL), rooted at that location.
#[unsafe(no_mangle)]
pub extern "C" fn pl_storage_open(
    root_url_ptr: *const c_char,
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,
    cloud_retry_init_backoff_ms: u64,
    cloud_retry_max_backoff_ms: u64,
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize,
) -> *mut StorageContext {
    ffi_try!({
        let root_url = ptr_to_str(root_url_ptr).map_err(compute_err)?;
        let options = unsafe {
            build_cloud_options(
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                cloud_keys, cloud_values, cloud_len,
            )
        };

        let path = PlRefPath::new(root_url.trim_end_matches('/'));
        let (location, store) = ASYNC.block_on(build_object_store(path, options.as_ref(), false))?;

        Ok(Box::into_raw(Box::new(StorageContext {
            store,
            root: location.prefix.trim_end_matches('/').to_string(),
        })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_storage_free(ptr: *mut StorageContext) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}

/// Lists the files directly inside `directory` (relative to the root). The result is UTF-8 text, one file per
/// line: `name<TAB>size<TAB>last modified in milliseconds since the epoch`. A directory that does not exist
/// lists as empty. With `start_from`, only names not less than it are returned.
#[unsafe(no_mangle)]
pub extern "C" fn pl_storage_list(
    ctx_ptr: *mut StorageContext,
    directory_ptr: *const c_char,
    start_from_ptr: *const c_char,
    out_buffer: *mut FfiBuffer,
) {
    ffi_try_void!({
        if ctx_ptr.is_null() || out_buffer.is_null() {
            return Err(compute_err("storage context or output buffer is null"));
        }
        let ctx = unsafe { &*ctx_ptr };
        let directory = ptr_to_str(directory_ptr).map_err(compute_err)?;
        let start_from = if start_from_ptr.is_null() { None } else { Some(ptr_to_str(start_from_ptr).map_err(compute_err)?) };
        let prefix = ctx.path(directory)?;

        let listing: object_store::Result<Vec<object_store::ObjectMeta>> = ASYNC.block_on(async {
            let store = ctx.store.to_dyn_object_store().await;
            match start_from {
                None => store.list_with_delimiter(Some(&prefix)).await.map(|r| r.objects),
                Some(from) => {
                    // Listing from an offset lets a store skip the start of a long log (S3 start-after, GCS
                    // startOffset). The offset is exclusive and the listing recursive: the file named exactly
                    // `from` is fetched on its own, and only direct children are kept.
                    use futures::TryStreamExt;
                    let offset = prefix.clone().join(from);
                    let mut objects: Vec<object_store::ObjectMeta> =
                        store.list_with_offset(Some(&prefix), &offset).try_collect().await?;
                    match store.head(&offset).await {
                        Ok(meta) => objects.push(meta),
                        Err(object_store::Error::NotFound { .. }) => {}
                        Err(e) => return Err(e),
                    }
                    objects.retain(|meta| meta.location.prefix_match(&prefix).is_some_and(|parts| parts.count() == 1));
                    Ok(objects)
                }
            }
        });
        let objects = match listing {
            Ok(objects) => objects,
            Err(object_store::Error::NotFound { .. }) => Vec::new(),
            Err(e) => return Err(compute_err(format!("Failed to list '{}': {}", prefix, e))),
        };

        let mut text = String::new();
        for meta in objects {
            let Some(name) = meta.location.filename() else { continue };
            if let Some(from) = start_from {
                if name < from { continue; }
            }
            text.push_str(name);
            text.push('\t');
            text.push_str(&meta.size.to_string());
            text.push('\t');
            text.push_str(&meta.last_modified.timestamp_millis().to_string());
            text.push('\n');
        }
        hand_out(text.into_bytes(), out_buffer);
        Ok(())
    })
}

/// Reads a file (relative to the root): all of it when `length` is negative, else `length` bytes from `offset`
/// (fewer when the file ends first). `out_found` is false, with an empty buffer, when the file does not exist.
#[unsafe(no_mangle)]
pub extern "C" fn pl_storage_read(
    ctx_ptr: *mut StorageContext,
    path_ptr: *const c_char,
    offset: u64,
    length: i64,
    out_buffer: *mut FfiBuffer,
    out_found: *mut bool,
) {
    ffi_try_void!({
        if ctx_ptr.is_null() || out_buffer.is_null() || out_found.is_null() {
            return Err(compute_err("storage context or output pointer is null"));
        }
        let ctx = unsafe { &*ctx_ptr };
        let relative = ptr_to_str(path_ptr).map_err(compute_err)?;
        let path = ctx.path(relative)?;

        let result: Result<Vec<u8>, object_store::Error> = ASYNC.block_on(async {
            let store = ctx.store.to_dyn_object_store().await;
            if length < 0 {
                Ok(store.get(&path).await?.bytes().await?.to_vec())
            } else {
                // Asking past the end is an error for some stores, so clamp to the size first.
                let size = store.head(&path).await?.size as u64;
                let start = offset.min(size);
                let end = offset.saturating_add(length as u64).min(size);
                if start >= end {
                    return Ok(Vec::new());
                }
                Ok(store.get_range(&path, start..end).await?.to_vec())
            }
        });

        match result {
            Ok(bytes) => {
                unsafe { *out_found = true };
                hand_out(bytes, out_buffer);
                Ok(())
            },
            Err(object_store::Error::NotFound { .. }) => {
                unsafe { *out_found = false };
                hand_out(Vec::new(), out_buffer);
                Ok(())
            },
            Err(e) => Err(compute_err(format!("Failed to read '{}': {}", path, e))),
        }
    })
}

/// Writes a file (relative to the root, or an absolute URI inside the root). With `create_only` the write is a
/// put-if-absent: it succeeds only when no file exists at the path, and `out_created` is false otherwise (the
/// commit protocol of a table log). Without it an existing file is replaced. Either way a reader sees the old
/// state or the whole new file, never a part of it.
#[unsafe(no_mangle)]
pub extern "C" fn pl_storage_put(
    ctx_ptr: *mut StorageContext,
    path_ptr: *const c_char,
    data: *const u8,
    len: usize,
    create_only: bool,
    out_created: *mut bool,
) {
    ffi_try_void!({
        if ctx_ptr.is_null() || out_created.is_null() || (data.is_null() && len > 0) {
            return Err(compute_err("storage context, data or output pointer is null"));
        }
        let ctx = unsafe { &*ctx_ptr };
        let relative = ptr_to_str(path_ptr).map_err(compute_err)?;
        let path = ctx.path(relative)?;
        let bytes = if len == 0 { Vec::new() } else { unsafe { std::slice::from_raw_parts(data, len) }.to_vec() };

        let mode = if create_only { object_store::PutMode::Create } else { object_store::PutMode::Overwrite };
        let options = object_store::PutOptions { mode, ..Default::default() };
        let result = ASYNC.block_on(async {
            let store = ctx.store.to_dyn_object_store().await;
            store.put_opts(&path, object_store::PutPayload::from(bytes), options).await
        });

        match result {
            Ok(_) => {
                unsafe { *out_created = true };
                Ok(())
            },
            Err(object_store::Error::AlreadyExists { .. }) if create_only => {
                unsafe { *out_created = false };
                Ok(())
            },
            Err(e) => Err(compute_err(format!("Failed to write '{}': {}", path, e))),
        }
    })
}

/// Deletes a file (relative to the root). `out_deleted` is false when there was no file.
#[unsafe(no_mangle)]
pub extern "C" fn pl_storage_delete(ctx_ptr: *mut StorageContext, path_ptr: *const c_char, out_deleted: *mut bool) {
    ffi_try_void!({
        if ctx_ptr.is_null() || out_deleted.is_null() {
            return Err(compute_err("storage context or output pointer is null"));
        }
        let ctx = unsafe { &*ctx_ptr };
        let relative = ptr_to_str(path_ptr).map_err(compute_err)?;
        let path = ctx.path(relative)?;
        let result = ASYNC.block_on(async {
            let store = ctx.store.to_dyn_object_store().await;
            // Some stores report success for a missing object; ask first so the answer is the same everywhere.
            match store.head(&path).await {
                Ok(_) => store.delete(&path).await.map(|_| true),
                Err(object_store::Error::NotFound { .. }) => Ok(false),
                Err(e) => Err(e),
            }
        });
        match result {
            Ok(deleted) => {
                unsafe { *out_deleted = deleted };
                Ok(())
            },
            Err(object_store::Error::NotFound { .. }) => {
                unsafe { *out_deleted = false };
                Ok(())
            },
            Err(e) => Err(compute_err(format!("Failed to delete '{}': {}", path, e))),
        }
    })
}
