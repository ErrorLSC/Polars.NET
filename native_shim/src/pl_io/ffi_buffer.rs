use std::io::{Write, Seek, SeekFrom, Cursor};
use std::sync::{Arc, Mutex};

use polars::prelude::file::WritableTrait;

#[repr(C)]
pub struct FfiBuffer {
    pub data: *mut u8,
    pub len: usize,
    pub capacity: usize,
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_free_ffi_buffer(buf: FfiBuffer) {
    if !buf.data.is_null() {
        unsafe {
            let _ = Vec::from_raw_parts(buf.data, buf.len, buf.capacity);
        }
    }
}

#[derive(Clone)]
pub struct SharedMemoryWriter {
    pub buffer: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl SharedMemoryWriter {
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Cursor::new(Vec::new()))),
        }
    }
    
    pub fn into_inner(self) -> Vec<u8> {
        Arc::try_unwrap(self.buffer)
            .expect("Buffer is still shared, cannot unwrap")
            .into_inner()
            .expect("Mutex poisoned")
            .into_inner()
    }
}

impl Write for SharedMemoryWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer.lock().unwrap().flush()
    }
}

impl Seek for SharedMemoryWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.buffer.lock().unwrap().seek(pos)
    }
}

impl WritableTrait for SharedMemoryWriter {
    fn close(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn sync_all(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> std::io::Result<()> {
        Ok(())
    }
}


