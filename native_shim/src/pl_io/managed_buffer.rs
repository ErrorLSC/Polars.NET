//! Byte buffers handed over by .NET for in-memory scans.

use std::ffi::c_void;

use polars_buffer::Buffer;

/// Called (on any thread) when the engine no longer needs memory that .NET pinned for it.
pub(crate) type ReleaseCallback = unsafe extern "C" fn(*mut c_void);

/// Memory .NET keeps pinned until the last plan reading it is dropped.
struct PinnedManagedBytes {
    ptr: *const u8,
    len: usize,
    release: ReleaseCallback,
    user_data: *mut c_void,
}

// The memory stays valid and unchanged until `release` runs; the release callback is thread-safe.
unsafe impl Send for PinnedManagedBytes {}
unsafe impl Sync for PinnedManagedBytes {}

impl AsRef<[u8]> for PinnedManagedBytes {
    fn as_ref(&self) -> &[u8] {
        if self.len == 0 { &[] } else { unsafe { std::slice::from_raw_parts(self.ptr, self.len) } }
    }
}

impl Drop for PinnedManagedBytes {
    fn drop(&mut self) {
        unsafe { (self.release)(self.user_data) }
    }
}

/// The bytes as an engine buffer. Without a release callback they are copied (the caller's memory is only valid
/// during the call); with one, the buffer shares the memory and calls `release` when the engine drops it. Call
/// this first thing in an FFI function, so that an error later still releases the memory.
pub(crate) unsafe fn buffer_from_ffi(
    ptr: *const u8,
    len: usize,
    release: Option<ReleaseCallback>,
    user_data: *mut c_void,
) -> Buffer<u8> {
    match release {
        Some(release) => Buffer::from_owner(PinnedManagedBytes { ptr, len, release, user_data }),
        None if len == 0 => Buffer::from(Vec::new()),
        None => Buffer::from(unsafe { std::slice::from_raw_parts(ptr, len) }.to_vec()),
    }
}
