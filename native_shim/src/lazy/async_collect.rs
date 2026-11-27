use std::num::NonZeroUsize;
use polars::prelude::*;
use polars_config::Engine;

use crate::types::{DataFrameContext, LazyFrameContext};

/// Context wrapper holding the active streaming batch iterator and background channel receiver.
pub struct CollectBatchesContext {
    pub inner: CollectBatches,
}

/// Initiates streaming batch collection on the LazyFrame.
///
/// # Arguments
/// * `lf_ptr` - Raw pointer to the `LazyFrameContext` (consumed by this call).
/// * `engine_code` - 1: InMemory, 2: Streaming, 3: Gpu, other: Auto.
/// * `maintain_order` - Whether to enforce original row order across batches.
/// * `chunk_size` - Maximum batch row size (0 means Polars default chunk size).
///
/// # Returns
/// Pointer to `CollectBatchesContext` on success, or null on error.
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_collect_batches(
    lf_ptr: *mut LazyFrameContext,
    engine_code: u8,
    maintain_order: bool,
    chunk_size: usize,
) -> *mut CollectBatchesContext {
    ffi_try!({
        // Consume the LazyFrame handle matching pl_lazy_collect semantics
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let lf = lf_ctx.inner;

        let engine = match engine_code {
            1 => Engine::InMemory,
            2 => Engine::Streaming,
            3 => Engine::Gpu,
            _ => Engine::Auto,
        };

        let chunk_size_opt = NonZeroUsize::new(chunk_size);

        // Start background runner immediately (lazy = false)
        let batches = lf.collect_batches(engine, maintain_order, chunk_size_opt, false)?;

        Ok(Box::into_raw(Box::new(CollectBatchesContext { inner: batches })))
    })
}

/// Fetches the next materialized DataFrame batch from the channel.
///
/// # Arguments
/// * `batches_ptr` - Raw pointer to `CollectBatchesContext`.
/// * `is_completed` - Out pointer set to `true` when all batches have been consumed.
///
/// # Returns
/// Pointer to `DataFrameContext` if a batch is available, or null on completion/error.
#[unsafe(no_mangle)]
pub extern "C" fn pl_collect_batches_next(
    batches_ptr: *mut CollectBatchesContext,
    is_completed: *mut bool,
) -> *mut DataFrameContext {
    ffi_try!({
        if batches_ptr.is_null() {
            polars_bail!(ComputeError: "batches_ptr cannot be null");
        }
        let batches_ctx = unsafe { &mut *batches_ptr };

        match batches_ctx.inner.next() {
            Some(Ok(df)) => {
                if !is_completed.is_null() {
                    unsafe { *is_completed = false; }
                }
                Ok(Box::into_raw(Box::new(DataFrameContext { df })))
            }
            Some(Err(e)) => Err(e),
            None => {
                if !is_completed.is_null() {
                    unsafe { *is_completed = true; }
                }
                Ok(std::ptr::null_mut())
            }
        }
    })
}

/// Frees the batch iterator context.
/// Dropping `CollectBatches` disconnects the channel receiver, signaling the
/// background Polars query execution engine to immediately abort remaining work.
#[unsafe(no_mangle)]
pub extern "C" fn pl_collect_batches_free(batches_ptr: *mut CollectBatchesContext) {
    if !batches_ptr.is_null() {
        unsafe {
            drop(Box::from_raw(batches_ptr));
        }
    }
}