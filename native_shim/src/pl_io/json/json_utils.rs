use polars::prelude::*;
use polars_io::mmap::MmapBytesReader;
use polars_io::RowIndex;
use std::os::raw::c_char;
use std::num::NonZeroUsize;
use crate::types::SchemaContext;
use crate::utils::{map_external_compression, map_json_format,ptr_to_schema_ref, ptr_to_str};

pub(crate) unsafe fn apply_json_options<'a, R: MmapBytesReader>(
    mut reader: JsonReader<'a, R>,
    // Params
    columns_ptr: *const *const c_char, 
    columns_len: usize,
    schema_overwrite: Option<&'a Schema>,
    infer_schema_len: *const usize,
    batch_size: *const usize,
    ignore_errors: bool,
    format_code: u8
) -> PolarsResult<JsonReader<'a, R>> {
    
    // 1. Format
    reader = reader.with_json_format(map_json_format(format_code));

    // 2. Schema
    if let Some(schema) = schema_overwrite {
        reader = reader.with_schema_overwrite(schema);
    }

    // 3. Columns (Projection)
    if columns_len > 0 {
        let mut cols = Vec::with_capacity(columns_len);
        for &p in unsafe {std::slice::from_raw_parts(columns_ptr, columns_len)} {
            if !p.is_null() {
                let s = ptr_to_str(p).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                cols.push(PlSmallStr::from_str(s));
            }
        }
        reader = reader.with_projection(Some(cols));
    }

    // 4. Infer Schema Length
    // Null (and 0) mean the whole input; the C# default is 100.
    reader = reader.infer_schema_len(
        unsafe { crate::pl_io::io_utils::infer_schema_length(infer_schema_len) }.and_then(NonZeroUsize::new));

    // 5. Batch Size
    if !batch_size.is_null() {
        let n = unsafe {*batch_size};
        if let Some(nz) = NonZeroUsize::new(n) {
            reader = reader.with_batch_size(nz);
        }
    }

    // 6. Ignore Errors
    if ignore_errors {
        reader = reader.with_ignore_errors(true);
    }

    Ok(reader)
}

pub(crate) unsafe fn apply_scan_ndjson_options(
    mut reader: LazyJsonLineReader,
    // --- Params ---
    batch_size: *const usize,
    low_memory: bool,
    rechunk: bool,
    schema_ptr: *mut SchemaContext,
    infer_schema_len: *const usize,
    n_rows: *const usize,
    ignore_errors: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
) -> PolarsResult<LazyJsonLineReader> {
    
    // 1. Batch Size
    if !batch_size.is_null() {
        let n = unsafe {*batch_size};
        if let Some(nz) = NonZeroUsize::new(n) {
             reader = reader.with_batch_size(Some(nz));
        }
    }

    // 2. Flags
    reader = reader.low_memory(low_memory);
    reader = reader.with_rechunk(rechunk);
    reader = reader.with_ignore_errors(ignore_errors);

    // 3. Schema
    if let Some(schema) = unsafe {ptr_to_schema_ref(schema_ptr)} {
        reader = reader.with_schema_overwrite(Some(schema));
    }

    // 4. Infer Schema Length
    // Null (and 0) mean the whole input; the C# default is 100.
    reader = reader.with_infer_schema_length(
        unsafe { crate::pl_io::io_utils::infer_schema_length(infer_schema_len) }.and_then(NonZeroUsize::new));

    // 5. N Rows
    if !n_rows.is_null() {
        reader = unsafe {reader.with_n_rows(Some(*n_rows))};
    }

    // 6. Row Index
    if !row_index_name_ptr.is_null() {
        let name = ptr_to_str(row_index_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        reader = reader.with_row_index(Some(RowIndex {
            name: PlSmallStr::from_str(name),
            offset: row_index_offset as u32,
        }));
    }

    // 7. Include Path
    if !include_path_col_ptr.is_null() {
        let name = ptr_to_str(include_path_col_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        reader = reader.with_include_file_paths(Some(PlSmallStr::from_str(name)));
    }

    Ok(reader)
}

pub(crate) fn build_ndjson_writer_options(
    compression_code: u8,
    compression_level: i32,
    check_extension: bool,
) -> NDJsonWriterOptions {
    let compression = map_external_compression(compression_code, compression_level);

    NDJsonWriterOptions {
        compression,
        check_extension,
    }
}
