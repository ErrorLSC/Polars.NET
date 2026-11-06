//! Streaming writer that forwards bytes to a host (C#) callback.
//!
//! Used by the `pl_*_callback` writers so that output can be streamed into a .NET `Stream`
//! / `PipeWriter` chunk by chunk, without materializing the whole file in memory.

use std::ffi::c_void;
use std::io::{self, BufWriter, Write};
use std::sync::{Arc, Mutex};

use polars::prelude::file::{Writable, WritableTrait};
use polars::prelude::*;

/// Host write callback.
///
/// * `user`  - opaque host state (a GCHandle on the C# side)
/// * `data`  - pointer to `len` bytes, valid only for the duration of the call
///
/// Returns 0 on success, non-zero on failure (the host keeps the actual exception).
pub type FfiWriteCallback = extern "C" fn(user: *mut c_void, data: *const u8, len: usize) -> i32;

const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

struct RawCallbackSink {
    cb: FfiWriteCallback,
    user: *mut c_void,
    /// Upper bound for a single callback. Large writes (e.g. a whole record batch, which `BufWriter`
    /// passes straight through) are split, so the host's backpressure applies at this granularity
    /// and the host never has to buffer more than one chunk at a time.
    max_chunk: usize,
    /// Once `finish` ran, the host state may already be freed: never call back again.
    active: bool,
}

// The host callback is thread-safe to invoke from any thread; calls are serialized by the Mutex below.
unsafe impl Send for RawCallbackSink {}

impl Write for RawCallbackSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if !self.active {
            return Err(io::Error::other("write callback used after the writer was finished"));
        }
        // A short write is allowed by the `Write` contract; `write_all` callers loop over the rest.
        let n = buf.len().min(self.max_chunk);
        match (self.cb)(self.user, buf.as_ptr(), n) {
            0 => Ok(n),
            code => {
                // Disable further calls: the host has already recorded its failure.
                self.active = false;
                Err(io::Error::other(format!("host write callback failed (code {code})")))
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Cloneable handle around a buffered callback sink.
///
/// A clone is handed to Polars (which may keep it in a `Writable`), the original is kept by the
/// FFI function to `finish` it before returning to the host.
#[derive(Clone)]
pub struct SharedCallbackWriter {
    inner: Arc<Mutex<BufWriter<RawCallbackSink>>>,
}

impl SharedCallbackWriter {
    pub fn new(cb: FfiWriteCallback, user: *mut c_void, buffer_size: usize) -> Self {
        let cap = if buffer_size == 0 { DEFAULT_BUFFER_SIZE } else { buffer_size };
        let sink = RawCallbackSink { cb, user, max_chunk: cap, active: true };
        Self {
            inner: Arc::new(Mutex::new(BufWriter::with_capacity(cap, sink))),
        }
    }

    /// Flushes remaining bytes to the host and disables the callback.
    ///
    /// Must be called before the FFI function returns, on both success and error paths:
    /// after return the host frees `user`, so any later write (e.g. a `BufWriter` drop-flush
    /// from a clone Polars still holds) must not reach the callback.
    pub fn finish(&self) -> io::Result<()> {
        let mut guard = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let res = guard.flush();
        guard.get_mut().active = false;
        res
    }
}

impl Write for SharedCallbackWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.lock().unwrap().write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.inner.lock().unwrap().write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.lock().unwrap().flush()
    }
}

impl WritableTrait for SharedCallbackWriter {
    fn close(&mut self) -> io::Result<()> {
        self.flush()
    }

    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }
}

/// Builds a single-file `SinkDestination` targeting an arbitrary writable (memory buffer, host callback, ...).
pub(crate) fn build_writer_sink_destination<W: WritableTrait + Send + 'static>(writer: W) -> SinkDestination {
    let writable = Writable::Dyn(Box::new(writer));
    let dyn_target = SpecialEq::new(Arc::new(Mutex::new(Some(writable))));
    SinkDestination::File { target: SinkTarget::Dyn(dyn_target) }
}

/// Runs `body` with a callback writer, then always finishes the writer.
///
/// Error precedence: an error from `body` wins over an error from the final flush.
pub(crate) fn with_callback_writer<F>(
    cb: FfiWriteCallback,
    user: *mut c_void,
    buffer_size: usize,
    body: F,
) -> PolarsResult<()>
where
    F: FnOnce(SharedCallbackWriter) -> PolarsResult<()>,
{
    let writer = SharedCallbackWriter::new(cb, user, buffer_size);
    let result = body(writer.clone());
    let finish = writer.finish();
    result?;
    finish.map_err(PolarsError::from)
}
