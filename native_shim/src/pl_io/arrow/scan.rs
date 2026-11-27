use polars::prelude::*;
use polars_arrow::ffi::{self, ArrowArray, ArrowArrayStream, ArrowArrayStreamReader, ArrowSchema, export_array_to_c, export_field_to_c};
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_core::prelude::CompatLevel;
use crate::types::DataFrameContext;
use polars::prelude::Field as PolarsField;
use crate::series::series::{needs_conversion, series_from_arrow_with_metadata};

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_from_arrow_record_batch(
    c_array_ptr: *mut ffi::ArrowArray, 
    c_schema_ptr: *mut ffi::ArrowSchema
) -> *mut DataFrameContext {
    ffi_try!({
        if c_array_ptr.is_null() || c_schema_ptr.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to pl_from_arrow".into()));
        }

        // Take the array first, leaving the caller's struct marked released (C data interface "move"): from here on
        // it is released exactly once, by this side, whether the import succeeds, fails or panics. The schema is
        // only borrowed; the caller releases it.
        let arrow_array_struct = unsafe { std::ptr::replace(c_array_ptr, ffi::ArrowArray::empty()) };

        // Import Arrow Schema
        let field = unsafe { ffi::import_field_from_c(&*c_schema_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))? };

        // Import Array
        let array = unsafe { 
            ffi::import_array_from_c(arrow_array_struct, field.dtype.clone())
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))? 
        };
        
        let df = match array.as_any().downcast_ref::<StructArray>() {
            Some(struct_arr) => {
                let columns: Vec<Column> = struct_arr
                    .values()
                    .iter()
                    .zip(struct_arr.fields())
                    .map(|(arr, field)| {
                        let name = PlSmallStr::from_str(&field.name);

                        series_from_arrow_with_metadata(name, arr.clone(), field.metadata.clone())
                            .map(|s| Column::from(s))
                    })
                    .collect::<PolarsResult<Vec<_>>>()?;

                let height = struct_arr.len();
                DataFrame::new(height,columns)?
            },
            None => {
                let name = PlSmallStr::from_str(&field.name);
                let series = series_from_arrow_with_metadata(name, array, field.metadata.clone())?;

                DataFrame::new_infer_height(vec![Column::from(series)])?
            }
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
// ==========================================
// Unified Stream Parse Engine
// ==========================================

/// What a query needs from a C# stream: the columns it reads, in this order (None: all), and at most how many
/// leading rows (None: all). A source may ignore it; the reading side applies it either way.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub(crate) struct ScanRequest {
    pub(crate) columns: Option<Arc<[PlSmallStr]>>,
    pub(crate) n_rows: Option<usize>,
}


/// Reads an Arrow C stream batch by batch, keeping only the requested columns (in the request's order) and
/// stopping once the requested rows are read. The stream is moved out of the caller's struct (which is left
/// released, C stream interface "move") and released when reading ends, early or on error, so a C# source's
/// enumerator is disposed then. `on_chunk` gets the kept columns' fields and arrays and the chunk's row count.
/// Returns the kept columns' fields.
unsafe fn read_stream_chunks(
    stream_ptr: *mut ArrowArrayStream,
    request: &ScanRequest,
    mut on_chunk: impl FnMut(&[Field], Vec<Box<dyn polars_arrow::array::Array>>, usize) -> PolarsResult<()>,
) -> PolarsResult<Vec<Field>> {
    if stream_ptr.is_null() {
        return Err(PolarsError::ComputeError("Stream pointer is null".into()));
    }
    let stream = Box::new(unsafe { std::ptr::replace(stream_ptr, ArrowArrayStream::empty()) });
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(stream) }
        .map_err(|e| PolarsError::ComputeError(format!("Failed to create Reader: {}", e).into()))?;
    let ArrowDataType::Struct(fields) = reader.field().dtype().clone() else {
        return Err(PolarsError::ComputeError("Stream data type is not Struct".into()));
    };
    let keep: Vec<usize> = match request.columns.as_deref() {
        None => (0..fields.len()).collect(),
        Some(names) => names
            .iter()
            .map(|name| {
                fields
                    .iter()
                    .position(|f| f.name == *name)
                    .ok_or_else(|| polars_err!(ColumnNotFound: "the stream has no column \"{name}\""))
            })
            .collect::<PolarsResult<_>>()?,
    };
    let kept: Vec<Field> = keep.iter().map(|&i| fields[i].clone()).collect();
    let mut remaining = request.n_rows.unwrap_or(usize::MAX);
    while remaining > 0 {
        let Some(chunk_result) = (unsafe { reader.next() }) else { break };
        let chunk = chunk_result.map_err(|e| PolarsError::ComputeError(format!("Read error: {}", e).into()))?;
        let struct_array = chunk.as_any().downcast_ref::<StructArray>()
            .ok_or_else(|| PolarsError::ComputeError("Batch is not a StructArray".into()))?;
        let rows = struct_array.len().min(remaining);
        remaining -= rows;
        let values = struct_array.values();
        let arrays = keep
            .iter()
            .map(|&i| if rows < struct_array.len() { values[i].sliced(0, rows) } else { values[i].clone() })
            .collect();
        on_chunk(&kept, arrays, rows)?;
    }
    Ok(kept)
}

/// The column of one stream field from its arrays: strict converts every array through the Arrow import (any
/// standard layout); otherwise the arrays are taken as the engine's physical layout under the field's dtype,
/// except dictionary data, which must become Categorical / Enum.
fn stream_column(field: &Field, chunks: Vec<Box<dyn polars_arrow::array::Array>>, strict: bool) -> PolarsResult<Column> {
    let name = PlSmallStr::from_str(&field.name);
    let series = if strict || needs_conversion(&field.dtype) {
        let mut safe_chunks = Vec::with_capacity(chunks.len());
        let mut safe_dtype = None;
        for arr in chunks {
            let temp_s = series_from_arrow_with_metadata(name.clone(), arr, field.metadata.clone())?;
            if safe_dtype.is_none() {
                safe_dtype = Some(temp_s.dtype().clone());
            }
            safe_chunks.extend(temp_s.chunks().iter().cloned());
        }
        match safe_dtype {
            Some(dtype) => unsafe { Series::from_chunks_and_dtype_unchecked(name, safe_chunks, &dtype) },
            None => Series::new_empty(name, &PolarsField::from(field).dtype),
        }
    } else if chunks.is_empty() {
        Series::new_empty(name, &PolarsField::from(field).dtype)
    } else {
        unsafe { Series::from_chunks_and_dtype_unchecked(name, chunks, &PolarsField::from(field).dtype) }
    };
    Ok(Column::from(series))
}

/// Reads a whole Arrow C stream into one DataFrame (the requested columns and rows); an empty stream gives an
/// empty DataFrame with the stream's schema.
unsafe fn consume_stream_impl(
    stream_ptr: *mut ArrowArrayStream,
    strict: bool,
    request: &ScanRequest,
) -> PolarsResult<DataFrame> {
    let mut columns_chunks: Vec<Vec<Box<dyn polars_arrow::array::Array>>> = Vec::new();
    let mut height = 0usize;
    let fields = unsafe {
        read_stream_chunks(stream_ptr, request, |fields, arrays, rows| {
            columns_chunks.resize_with(fields.len(), Vec::new);
            for (chunks, array) in columns_chunks.iter_mut().zip(arrays) {
                chunks.push(array);
            }
            height += rows;
            Ok(())
        })?
    };
    columns_chunks.resize_with(fields.len(), Vec::new);
    let columns = fields
        .iter()
        .zip(columns_chunks)
        .map(|(field, chunks)| stream_column(field, chunks, strict))
        .collect::<PolarsResult<Vec<_>>>()?;
    DataFrame::new(height, columns)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_dataframe_new_from_stream(
    stream_ptr: *mut ArrowArrayStream,
) -> *mut DataFrameContext {
    ffi_try!({
        let df = unsafe { consume_stream_impl(stream_ptr, false, &ScanRequest::default())? };
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_dataframe_new_from_stream_strict_type(
    stream_ptr: *mut ArrowArrayStream,
) -> *mut DataFrameContext {
    ffi_try!({
        let df = unsafe { consume_stream_impl(stream_ptr, true, &ScanRequest::default())? };
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

// ==========================================
// Memory and Convert Ops
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_to_arrow(
    ctx_ptr: *mut DataFrameContext, 
    out_chunk: *mut ArrowArray, 
    out_schema: *mut ArrowSchema
) {
    ffi_try_void!({
        if ctx_ptr.is_null() {
             return Err(PolarsError::ComputeError("Null pointer passed to pl_to_arrow".into()));
        }
        
        let ctx = unsafe { &mut *ctx_ptr };
        let df = &mut ctx.df;

        let columns = df.columns()
            .iter()
            .map(|s| s.clone().rechunk_to_arrow(CompatLevel::newest()))
            .collect::<Vec<_>>();

        let arrow_schema = df.schema().to_arrow(CompatLevel::newest());
        let fields: Vec<Field> = arrow_schema.iter_values().cloned().collect();

        let struct_array = StructArray::new(
            ArrowDataType::Struct(fields.clone()), 
            df.height(),
            columns,
            None
        );

        unsafe {
            *out_chunk = export_array_to_c(Box::new(struct_array));
            let root_field = Field::new("".into(), ArrowDataType::Struct(fields), false);
            *out_schema = export_field_to_c(&root_field);
        }
        
        Ok(())
    })
}
