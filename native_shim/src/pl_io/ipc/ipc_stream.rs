use polars::prelude::*;
use polars_io::RowIndex;
use polars_utils::compression::ZstdLevel;
use std::fs::File;
use std::os::raw::c_char;
use crate::pl_io::ffi_buffer::{FfiBuffer, SharedMemoryWriter};
use crate::utils::{ptr_to_str,ptr_to_vec_string};
use crate::types::{DataFrameContext, SchemaContext};

// ==========================================
// IPC Stream Reader / Writer (FFI Layer)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_read_ipc_stream(
    path_ptr: *const c_char,
    columns_ptr: *const *const c_char,
    columns_len: usize,
    projection_ptr: *const usize, 
    projection_len: usize,       
    n_rows_ptr: *const usize,
    row_index_name: *const c_char,
    row_index_offset: u32,
    rechunk: bool,
) -> *mut DataFrameContext {
    ffi_try!({
        // 1. Resolve file path using the existing helper
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::open(path)
            .map_err(|e| PolarsError::ComputeError(format!("IPC Stream file not found: {}", e).into()))?;

        let mut reader = IpcStreamReader::new(file)
            .set_rechunk(rechunk);

        // 2. Handle n_rows limit if the pointer is not null
        if !n_rows_ptr.is_null() {
            let limit = unsafe { *n_rows_ptr };
            reader = reader.with_n_rows(Some(limit));
        }

        // 3. Handle columns selection using your helper function
        if !columns_ptr.is_null() && columns_len > 0 {
            let cols_vec = unsafe { ptr_to_vec_string(columns_ptr, columns_len) };
            if !cols_vec.is_empty() {
                reader = reader.with_columns(Some(cols_vec));
            }
        }

        // 4. Projection
        if !projection_ptr.is_null() && projection_len > 0 {
            let proj_slice = unsafe { std::slice::from_raw_parts(projection_ptr, projection_len) };
            reader = reader.with_projection(Some(proj_slice.to_vec()));
        }

        // 5. Handle row index column if name is provided
        if !row_index_name.is_null() {
            let name_str = ptr_to_str(row_index_name)
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            
            let ri = RowIndex {
                name: PlSmallStr::from_str(name_str),
                offset: row_index_offset,
            };
            reader = reader.with_row_index(Some(ri));
        }

        // Execute reader and return DataFrameContext
        let df = reader.finish()?;
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

use std::io::Cursor;

#[unsafe(no_mangle)]
pub extern "C" fn pl_read_ipc_stream_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
    columns_ptr: *const *const c_char,
    columns_len: usize,
    projection_ptr: *const usize, 
    projection_len: usize,      
    n_rows_ptr: *const usize,
    row_index_name: *const c_char,
    row_index_offset: u32,
    rechunk: bool,
) -> *mut DataFrameContext {
    ffi_try!({
        if buffer_ptr.is_null() || buffer_len == 0 {
            return Err(PolarsError::ComputeError("Empty or null memory buffer passed to pl_read_ipc_stream_memory".into()));
        }

        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let memory_stream = Cursor::new(slice);

        let mut reader = IpcStreamReader::new(memory_stream)
            .set_rechunk(rechunk);

        if !n_rows_ptr.is_null() {
            let limit = unsafe { *n_rows_ptr };
            reader = reader.with_n_rows(Some(limit));
        }

        if !columns_ptr.is_null() && columns_len > 0 {
            let cols_vec = unsafe { ptr_to_vec_string(columns_ptr, columns_len) };
            if !cols_vec.is_empty() {
                reader = reader.with_columns(Some(cols_vec));
            }
        }

        // Projection
        if !projection_ptr.is_null() && projection_len > 0 {
            let proj_slice = unsafe { std::slice::from_raw_parts(projection_ptr, projection_len) };
            reader = reader.with_projection(Some(proj_slice.to_vec()));
        }

        if !row_index_name.is_null() {
            let name_str = ptr_to_str(row_index_name)
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            
            let ri = RowIndex {
                name: PlSmallStr::from_str(name_str),
                offset: row_index_offset,
            };
            reader = reader.with_row_index(Some(ri));
        }

        let df = reader.finish()?;
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_ipc_stream(
    df_ptr: *mut DataFrameContext,
    path_ptr: *const c_char,
    compression: u8,   // 0: None, 1: LZ4, 2: ZSTD
    compat_level: i32, // -1: Newest, >=0: Custom Level
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let mut file = File::create(path)
            .map_err(|e| PolarsError::ComputeError(format!("Could not create IPC Stream target file: {}", e).into()))?;

        let mut writer = IpcStreamWriter::new(&mut file);

        let comp_opt = match compression {
            1 => Some(IpcCompression::LZ4),
            2 => {
                let level = ZstdLevel::try_new(3)
                    .map_err(|_| PolarsError::ComputeError("Invalid ZSTD Level".into()))?;
                Some(IpcCompression::ZSTD(level))
            },
            _ => None,
        };
        if comp_opt.is_some() {
            writer = writer.with_compression(comp_opt);
        }

        let compat = if compat_level < 0 {
            CompatLevel::newest()
        } else {
            CompatLevel::with_level(compat_level as u16)
                .unwrap_or(CompatLevel::newest())
        };
        writer = writer.with_compat_level(compat);

        writer.finish(&mut ctx.df)?;
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_ipc_stream_memory(
    df_ptr: *mut DataFrameContext,
    out_buffer: *mut FfiBuffer,
    compression: u8,   // 0: None, 1: LZ4, 2: ZSTD
    compat_level: i32, // -1: Newest
) {
    ffi_try_void!({
        if df_ptr.is_null() || out_buffer.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to dataframe memory writer".into()));
        }

        let ctx = unsafe { &mut *df_ptr };

        let mut mem_writer = SharedMemoryWriter::new();
        let mut writer = IpcStreamWriter::new(&mut mem_writer);

        let comp_opt = match compression {
            1 => Some(IpcCompression::LZ4),
            2 => {
                let level = ZstdLevel::try_new(3)
                    .map_err(|_| PolarsError::ComputeError("Invalid ZSTD Level".into()))?;
                Some(IpcCompression::ZSTD(level))
            },
            _ => None,
        };
        if comp_opt.is_some() {
            writer = writer.with_compression(comp_opt);
        }

        let compat = if compat_level < 0 {
            CompatLevel::newest()
        } else {
            CompatLevel::with_level(compat_level as u16)
                .unwrap_or(CompatLevel::newest())
        };
        writer = writer.with_compat_level(compat);

        writer.finish(&mut ctx.df)?;

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
pub extern "C" fn pl_read_ipc_stream_schema(
    path_ptr: *const c_char,
) -> *mut SchemaContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::open(path)
            .map_err(|e| PolarsError::ComputeError(format!("IPC Stream file not found: {}", e).into()))?;

        let mut reader = IpcStreamReader::new(file);
        let schema = reader.schema()?;

        Ok(Box::into_raw(Box::new(SchemaContext { schema:schema.into() })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_read_ipc_stream_schema_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
) -> *mut SchemaContext {
    ffi_try!({
        if buffer_ptr.is_null() || buffer_len == 0 {
            return Err(PolarsError::ComputeError("Null or empty pointer passed to pl_read_ipc_stream_schema_memory".into()));
        }

        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let memory_stream = Cursor::new(slice);

        let mut reader = IpcStreamReader::new(memory_stream);
        let schema = reader.schema()?;

        Ok(Box::into_raw(Box::new(SchemaContext { schema:schema.into() })))
    })
}