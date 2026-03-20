use std::any::Any;
use polars::prelude::*;
use polars_arrow::ffi::{self, ArrowArray, ArrowArrayStream, ArrowArrayStreamReader, ArrowSchema, export_array_to_c, export_field_to_c};
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_core::prelude::CompatLevel;
use polars_core::utils::Container;
use crate::types::{DataFrameContext, LazyFrameContext};

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_from_arrow_record_batch(
    c_array_ptr: *mut ffi::ArrowArray, 
    c_schema_ptr: *mut ffi::ArrowSchema
) -> *mut DataFrameContext {
    ffi_try!({
        if c_array_ptr.is_null() || c_schema_ptr.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to pl_from_arrow".into()));
        }

        // Import Arrow Schema
        let field = unsafe { ffi::import_field_from_c(&*c_schema_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))? };
        
        // Import Array
        let arrow_array_struct = unsafe { std::ptr::read(c_array_ptr) };
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
                        
                        Series::from_arrow(name, arr.clone())
                            .map(|s| Column::from(s)) 
                    })
                    .collect::<PolarsResult<Vec<_>>>()?;

                let height = struct_arr.len();
                DataFrame::new(height,columns)?
            },
            None => {
                let name = PlSmallStr::from_str(&field.name);
                let series = Series::from_arrow(name, array)?;
                let height = series.len();

                DataFrame::new(height,vec![Column::from(series)])?
            }
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
// ==========================================
// Unified Stream Parse Engine
// ==========================================
#[inline(always)]
unsafe fn consume_stream_impl(
    stream_ptr: *mut polars_arrow::ffi::ArrowArrayStream,
    strict: bool,
) -> PolarsResult<DataFrame> {
    if stream_ptr.is_null() {
        return Err(PolarsError::ComputeError("Stream pointer is null".into()));
    }

    let stream = unsafe { &mut *stream_ptr };
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(stream)
        .map_err(|e| PolarsError::ComputeError(format!("Failed to create Reader: {}", e).into())) }?;

    let root_field = reader.field().clone();
    let ArrowDataType::Struct(fields) = root_field.dtype() else {
        return Err(PolarsError::ComputeError("Stream data type is not Struct".into()));
    };

    let num_cols = fields.len();
    let mut columns_chunks: Vec<Vec<Box<dyn polars_arrow::array::Array>>> = vec![Vec::new(); num_cols];
    let mut has_data = false;

    // ==========================================
    // Public Path
    // ==========================================
    while let Some(chunk_result) = unsafe {reader.next()} {
        has_data = true;
        let chunk = chunk_result.map_err(|e| PolarsError::ComputeError(format!("Read error: {}", e).into()))?;
        let struct_array = chunk.as_any().downcast_ref::<polars_arrow::array::StructArray>()
            .ok_or_else(|| PolarsError::ComputeError("Batch is not a StructArray".into()))?;

        for (col_idx, column) in struct_array.values().iter().enumerate() {
            if col_idx < num_cols {
                columns_chunks[col_idx].push(column.clone());
            }
        }
    }

    // Blank Flow to Blank DataFrame with Schema
    if !has_data {
        let columns = fields.iter().map(|f| {
            let p_field = PolarsField::from(f);
            Column::from(Series::new_empty(PlSmallStr::from_str(&f.name), &p_field.dtype))
        }).collect::<Vec<_>>();
        return DataFrame::new(0, columns);
    }

    // ==========================================
    // Assemble by column
    // ==========================================
    let mut series_vec = Vec::with_capacity(num_cols);

    for (i, chunks) in columns_chunks.into_iter().enumerate() {
        let arrow_field = &fields[i];
        let name = PlSmallStr::from_str(&arrow_field.name);

        let final_series = if strict {
            // ----------------------------------------
            // Strict Path(with type safety) 
            // ----------------------------------------
            let mut safe_chunks = Vec::with_capacity(chunks.len());
            let mut safe_dtype = None;

            for arr in chunks {
                // Logic type kept here
                let temp_s = Series::from_arrow(name.clone(), arr)?;
                
                // Lock Polars DataType
                if safe_dtype.is_none() {
                    safe_dtype = Some(temp_s.dtype().clone());
                }
                
                // Get Memory Chunk
                safe_chunks.extend(temp_s.chunks().iter().cloned());
            }

            // Assemble to series
            unsafe { Series::from_chunks_and_dtype_unchecked(name, safe_chunks, safe_dtype.as_ref().unwrap()) }
        } else {
            // ----------------------------------------
            // Unstrict Path
            // ----------------------------------------
            let p_field = PolarsField::from(arrow_field); 
            unsafe { Series::from_chunks_and_dtype_unchecked(name, chunks, &p_field.dtype) }
        };

        series_vec.push(Column::from(final_series));
    }

    // Assemble Final DataFrame
    let height = series_vec.first().map(|s: &Column| s.len()).unwrap_or(0);
    DataFrame::new(height, series_vec)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_dataframe_new_from_stream(
    stream_ptr: *mut ArrowArrayStream,
) -> *mut DataFrameContext {
    ffi_try!({
        let df = unsafe {consume_stream_impl(stream_ptr, false)?};
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_dataframe_new_from_stream_strict_type(
    stream_ptr: *mut ArrowArrayStream,
) -> *mut DataFrameContext {
    ffi_try!({
        let df = unsafe {consume_stream_impl(stream_ptr, true)?};
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

// Define Callback：C# will return ArrowArrayStream Pointer
type StreamFactoryCallback = unsafe extern "C" fn(*mut core::ffi::c_void) -> *mut polars_arrow::ffi::ArrowArrayStream;
type DestroyUserDataCallback = unsafe extern "C" fn(*mut core::ffi::c_void); 
// Define scanner struct
struct CSharpStreamScanner {
    schema: SchemaRef,
    callback: StreamFactoryCallback,
    destroy_callback: Option<DestroyUserDataCallback>,
    user_data: *mut core::ffi::c_void, 
}

unsafe impl Send for CSharpStreamScanner {}
unsafe impl Sync for CSharpStreamScanner {}

impl Drop for CSharpStreamScanner {
    fn drop(&mut self) {
        if let Some(destroy) = self.destroy_callback {
            unsafe {
                destroy(self.user_data);
            }
        }
    }
}

impl AnonymousScan for CSharpStreamScanner {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn scan(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        unsafe {
            // Call C# for new pointer for stream
            let stream_ptr = (self.callback)(self.user_data);
            
            if stream_ptr.is_null() {
                return Err(PolarsError::ComputeError("C# callback returned null stream".into()));
            }

            let ctx_ptr = pl_dataframe_new_from_stream(stream_ptr);
            
            if ctx_ptr.is_null() {
                return Err(PolarsError::ComputeError("Failed to consume stream".into()));
            }

            let ctx = Box::from_raw(ctx_ptr);
            Ok(ctx.df) 
        }
    }

    // Tell Polars the schema of data
    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn allows_predicate_pushdown(&self) -> bool {
        false 
    }
    fn allows_projection_pushdown(&self) -> bool {
        true 
    }
}
use polars::prelude::{Field as PolarsField};
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_lazy_frame_scan_stream(
    ptr_schema: *mut polars_arrow::ffi::ArrowSchema,
    callback: StreamFactoryCallback,
    destroy_callback: DestroyUserDataCallback,
    user_data: *mut core::ffi::c_void,
) -> *mut LazyFrameContext {
    ffi_try!({
        // Parse C Schema
        let field = unsafe { polars_arrow::ffi::import_field_from_c(&*ptr_schema)? };
        
        // Arrow Field -> Polars Schema
        let arrow_dtype = field.dtype; 
        
        let schema = match arrow_dtype {
            ArrowDataType::Struct(fields) => {
                let mut schema = Schema::with_capacity(fields.len());
                for f in fields {
                    let p_field = PolarsField::from(&f);
                    schema.insert(p_field.name, p_field.dtype);
                }
                Arc::new(schema)
            },
            _ => return Err(PolarsError::ComputeError("Schema must be a Struct".into())),
        };

        let scanner = CSharpStreamScanner {
            schema,
            callback,
            destroy_callback: Some(destroy_callback),
            user_data,
        };

        let lf = LazyFrame::anonymous_scan(
            std::sync::Arc::new(scanner),
            ScanArgsAnonymous::default()
        )?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

