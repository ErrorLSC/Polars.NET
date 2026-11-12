use std::any::Any;
use std::io::Write;
use polars::prelude::*;
use polars_arrow::ffi::{self, ArrowArray, ArrowArrayStream, ArrowArrayStreamReader, ArrowSchema, export_array_to_c, export_field_to_c};
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_core::prelude::CompatLevel;
use crate::types::{DataFrameContext, LazyFrameContext};
use crate::series::series::{contains_dictionary, series_from_arrow_with_metadata};

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

        // Dictionary data must be converted to its Categorical / Enum physical form; the unstrict path's
        // reinterpretation under the field's dtype would be invalid for it.
        let final_series = if strict || contains_dictionary(&arrow_field.dtype) {
            // ----------------------------------------
            // Strict Path(with type safety) 
            // ----------------------------------------
            let mut safe_chunks = Vec::with_capacity(chunks.len());
            let mut safe_dtype = None;

            for arr in chunks {
                // Logic type kept here
                let temp_s = series_from_arrow_with_metadata(name.clone(), arr, arrow_field.metadata.clone())?;
                
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
pub(crate) struct CSharpStreamScanner {
    schema: SchemaRef,
    /// Convert every column through the engine's Arrow import (any standard Arrow layout), rather than taking
    /// the arrays as the engine's own physical layout (only true for batches built by ArrowConverter).
    strict: bool,
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

impl CSharpStreamScanner {
    /// Writes the rows of a new C# stream to an anonymous temporary Arrow IPC file, batch by batch (memory stays
    /// bounded by one batch), and returns the file: removed from the directory already (on Windows, removed when
    /// its last handle closes), so it lives exactly as long as the plans that scan it.
    pub(crate) fn spool_to_ipc(&self) -> PolarsResult<SpooledIpc> {
        let stream_ptr = unsafe { (self.callback)(self.user_data) };
        if stream_ptr.is_null() {
            return Err(PolarsError::ComputeError("C# callback returned null stream".into()));
        }
        let file = anonymous_temp_file()?;
        type Writer<'f> = polars_arrow::io::ipc::write::FileWriter<std::io::BufWriter<&'f std::fs::File>>;
        let mut writer: Option<Writer> = None;
        let mut restore: Vec<(PlSmallStr, DataType)> = Vec::new();
        // An empty stream still gives one (empty) batch, so the file always has a schema.
        unsafe {
            for_each_stream_batch(stream_ptr, self.strict, |mut df| {
                if writer.is_none() {
                    // The first batch's dtypes are the ones the scan restores.
                    restore = df
                        .columns()
                        .iter()
                        .filter_map(|c| without_categoricals(c.dtype()).map(|_| (c.name().clone(), c.dtype().clone())))
                        .collect();
                }
                // Categorical / Enum columns are written as strings: each batch brings its own dictionary, and an
                // IPC file allows one dictionary per field.
                for (name, dtype) in &restore {
                    let column = df.column(name)?;
                    let as_string = column.cast(&without_categoricals(dtype).unwrap())?;
                    df.with_column(as_string)?;
                }
                if writer.is_none() {
                    writer = Some(polars_arrow::io::ipc::write::FileWriter::try_new(
                        std::io::BufWriter::new(&file),
                        Arc::new(df.schema().to_arrow(CompatLevel::newest())),
                        None,
                        polars_arrow::io::ipc::write::WriteOptions { compression: None },
                    )?);
                }
                let w = writer.as_mut().unwrap();
                for batch in df.iter_chunks(CompatLevel::newest(), true) {
                    w.write(&batch, None)?;
                }
                Ok(())
            })?
        };
        let mut w = writer.ok_or_else(|| PolarsError::ComputeError("C# stream gave no schema".into()))?;
        w.finish()?;
        w.into_inner().flush().map_err(PolarsError::from)?;
        Ok(SpooledIpc { file, restore })
    }
}

/// A C# stream spooled to a temporary Arrow IPC file, with the columns whose dtype the scan restores.
pub(crate) struct SpooledIpc {
    file: std::fs::File,
    restore: Vec<(PlSmallStr, DataType)>,
}

/// The dtype with every Categorical / Enum (also nested) replaced by String; None when it has none.
fn without_categoricals(dtype: &DataType) -> Option<DataType> {
    match dtype {
        DataType::Categorical(..) | DataType::Enum(..) => Some(DataType::String),
        DataType::List(inner) => without_categoricals(inner).map(|d| DataType::List(Box::new(d))),
        DataType::Array(inner, width) => without_categoricals(inner).map(|d| DataType::Array(Box::new(d), *width)),
        DataType::Struct(fields) => {
            let stripped: Vec<_> = fields.iter().map(|f| without_categoricals(f.dtype())).collect();
            stripped.iter().any(Option::is_some).then(|| {
                DataType::Struct(
                    fields
                        .iter()
                        .zip(stripped)
                        .map(|(f, d)| PolarsField::new(f.name().clone(), d.unwrap_or_else(|| f.dtype().clone())))
                        .collect(),
                )
            })
        },
        _ => None,
    }
}

/// Creates a read/write temporary file in Polars's temporary directory (`POLARS_TEMP_DIR`) that nobody else can
/// reach and that disappears when its last handle closes.
fn anonymous_temp_file() -> PolarsResult<std::fs::File> {
    let dir: &std::path::Path = &polars_io::path_utils::POLARS_TEMP_DIR_BASE_PATH;
    let path = dir.join(format!("polars-net-spool-{}-{:016x}.arrow", std::process::id(), rand::random::<u64>()));
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE; FILE_FLAG_DELETE_ON_CLOSE
        options.share_mode(0x1 | 0x2 | 0x4).custom_flags(0x0400_0000);
    }
    let file = options.open(&path).map_err(PolarsError::from)?;
    #[cfg(not(windows))]
    {
        let _ = std::fs::remove_file(&path);
    }
    Ok(file)
}

/// Reads an Arrow C stream batch by batch, converting each batch to a DataFrame as the stream import does
/// (strict: every column through the Arrow import; non-strict: dictionary columns convert to Categorical / Enum,
/// the rest take the field's dtype as is). An empty stream gives one empty DataFrame with the stream's schema.
/// The stream is released when reading ends.
pub(crate) unsafe fn for_each_stream_batch(
    stream_ptr: *mut polars_arrow::ffi::ArrowArrayStream,
    strict: bool,
    mut on_batch: impl FnMut(DataFrame) -> PolarsResult<()>,
) -> PolarsResult<()> {
    let stream = unsafe { &mut *stream_ptr };
    let mut reader = unsafe { ArrowArrayStreamReader::try_new(stream)
        .map_err(|e| PolarsError::ComputeError(format!("Failed to create Reader: {}", e).into())) }?;
    let root_field = reader.field().clone();
    let ArrowDataType::Struct(fields) = root_field.dtype() else {
        return Err(PolarsError::ComputeError("Stream data type is not Struct".into()));
    };
    let mut has_data = false;
    while let Some(chunk_result) = unsafe { reader.next() } {
        has_data = true;
        let chunk = chunk_result.map_err(|e| PolarsError::ComputeError(format!("Read error: {}", e).into()))?;
        let struct_array = chunk.as_any().downcast_ref::<StructArray>()
            .ok_or_else(|| PolarsError::ComputeError("Batch is not a StructArray".into()))?;
        let columns = fields.iter().zip(struct_array.values()).map(|(field, arr)| {
            let name = PlSmallStr::from_str(&field.name);
            let series = if strict || contains_dictionary(&field.dtype) {
                series_from_arrow_with_metadata(name, arr.clone(), field.metadata.clone())?
            } else {
                let p_field = PolarsField::from(field);
                unsafe { Series::from_chunks_and_dtype_unchecked(name, vec![arr.clone()], &p_field.dtype) }
            };
            Ok(Column::from(series))
        }).collect::<PolarsResult<Vec<_>>>()?;
        on_batch(DataFrame::new(struct_array.len(), columns)?)?;
    }
    if !has_data {
        let columns = fields.iter().map(|f| {
            let p_field = PolarsField::from(f);
            Column::from(Series::new_empty(PlSmallStr::from_str(&f.name), &p_field.dtype))
        }).collect::<Vec<_>>();
        on_batch(DataFrame::new(0, columns)?)?;
    }
    Ok(())
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

            consume_stream_impl(stream_ptr, self.strict)
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
/// The scanner over a C# stream factory; the schema is the C# stream's (a struct field).
unsafe fn csharp_scanner(
    ptr_schema: *mut polars_arrow::ffi::ArrowSchema,
    callback: StreamFactoryCallback,
    destroy_callback: DestroyUserDataCallback,
    user_data: *mut core::ffi::c_void,
    strict: bool,
) -> PolarsResult<CSharpStreamScanner> {
    // From here on the scanner owns user_data: dropping it (also on error) releases it.
    let mut scanner = CSharpStreamScanner {
        schema: Arc::new(Schema::default()),
        strict,
        callback,
        destroy_callback: Some(destroy_callback),
        user_data,
    };
    let field = unsafe { polars_arrow::ffi::import_field_from_c(&*ptr_schema)? };
    scanner.schema = match field.dtype {
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
    Ok(scanner)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_lazy_frame_scan_stream(
    ptr_schema: *mut polars_arrow::ffi::ArrowSchema,
    callback: StreamFactoryCallback,
    destroy_callback: DestroyUserDataCallback,
    user_data: *mut core::ffi::c_void,
    strict: bool,
) -> *mut LazyFrameContext {
    ffi_try!({
        let scanner = unsafe { csharp_scanner(ptr_schema, callback, destroy_callback, user_data, strict)? };
        let lf = LazyFrame::anonymous_scan(
            std::sync::Arc::new(scanner),
            ScanArgsAnonymous::default()
        )?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

/// Reads a C# stream now into an anonymous temporary Arrow IPC file and scans the file. The file goes away with
/// the last plan that scans it; the stream factory is released before returning.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_lazy_frame_scan_stream_buffered(
    ptr_schema: *mut polars_arrow::ffi::ArrowSchema,
    callback: StreamFactoryCallback,
    destroy_callback: DestroyUserDataCallback,
    user_data: *mut core::ffi::c_void,
    strict: bool,
) -> *mut LazyFrameContext {
    ffi_try!({
        let scanner = unsafe { csharp_scanner(ptr_schema, callback, destroy_callback, user_data, strict)? };
        let inner = scan_spooled_ipc(scanner.spool_to_ipc()?)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}

/// Scans a spooled C# stream, casting its Categorical / Enum columns back from strings.
pub(crate) fn scan_spooled_ipc(spooled: SpooledIpc) -> PolarsResult<LazyFrame> {
    let mut args = UnifiedScanArgs::default();
    // An opened file has no path to read partitions from.
    args.hive_options.enabled = Some(false);
    let lf = LazyFrame::scan_ipc_sources(ScanSources::Files(Arc::from([spooled.file])), IpcScanOptions::default(), args)?;
    if spooled.restore.is_empty() {
        return Ok(lf);
    }
    Ok(lf.with_columns(spooled.restore.into_iter().map(|(name, dtype)| col(name).strict_cast(dtype)).collect::<Vec<_>>()))
}

