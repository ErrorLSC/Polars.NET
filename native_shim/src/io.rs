use polars::prelude::*;
use polars_arrow::ffi::{self, ArrowArray, ArrowSchema, export_array_to_c, export_field_to_c};
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_core::prelude::CompatLevel;
use std::ffi::CStr;
use std::io::BufReader;
use std::os::raw::c_char;
use std::fs::File;
use crate::types::{DataFrameContext,LazyFrameContext, ptr_to_str};
use crate::datatypes::DataTypeContext;

// ==========================================
// 读取 csv
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_csv(
    path: *const c_char,
    schema_names: *const *const c_char,
    schema_types: *const *mut DataTypeContext,
    schema_len: usize,
    has_header: bool,
    separator: u8,
    skip_rows: usize,
    try_parse_dates: bool
) -> *mut DataFrameContext {
    ffi_try!({
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        // 1. 构建 ParseOptions (处理分隔符和日期解析)
        // 使用 builder 方法链式调用
        let parse_options = CsvParseOptions::default()
            .with_separator(separator)
            .with_try_parse_dates(try_parse_dates);

        // 2. 构建 ReadOptions (注入 parse_options)
        let mut options = CsvReadOptions::default()
            .with_has_header(has_header)
            .with_skip_rows(skip_rows)
            .with_parse_options(parse_options);

        // 3. 处理 Schema Overrides
        if !schema_names.is_null() && schema_len > 0 {
            let names_slice = unsafe { std::slice::from_raw_parts(schema_names, schema_len) };
            let types_slice = unsafe { std::slice::from_raw_parts(schema_types, schema_len) };
            
            // 使用 with_capacity
            let mut schema = Schema::with_capacity(schema_len);
            for i in 0..schema_len {
                let name = unsafe { CStr::from_ptr(names_slice[i]).to_string_lossy().to_string() };
                let ctx = unsafe { &*types_slice[i] };
                schema.with_column(name.into(), ctx.dtype.clone());
            }
            
            options = options.with_schema_overwrite(Some(Arc::new(schema)));
        }

        // 4. 执行读取
        // p.into_owned().into() -> String -> PathBuf
        let df = options
            .try_into_reader_with_file_path(Some(p.into_owned().into()))?
            .finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_csv(
    path: *const c_char,
    schema_names: *const *const c_char,
    schema_types: *const *mut DataTypeContext,
    schema_len: usize,
    has_header: bool,
    separator: u8,
    skip_rows: usize,
    try_parse_dates: bool // [新增参数]
) -> *mut LazyFrameContext {
    ffi_try!({
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        let mut reader = LazyCsvReader::new(PlPath::new(&p))
            .with_has_header(has_header)
            .with_separator(separator)
            .with_skip_rows(skip_rows)
            .with_try_parse_dates(try_parse_dates); // LazyReader 通常直接支持这个

        // ... schema 逻辑 (记得用 Schema::with_capacity) ...
        if !schema_names.is_null() && schema_len > 0 {
             let names_slice = unsafe { std::slice::from_raw_parts(schema_names, schema_len) };
             let types_slice = unsafe { std::slice::from_raw_parts(schema_types, schema_len) };
             let mut schema = Schema::with_capacity(schema_len);
             for i in 0..schema_len {
                 let name = unsafe { CStr::from_ptr(names_slice[i]).to_string_lossy().to_string() };
                 let ctx = unsafe { &*types_slice[i] };
                 schema.with_column(name.into(), ctx.dtype.clone());
             }
             reader = reader.with_schema(Some(Arc::new(schema)));
        }

        let inner = reader.finish()?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}
// ==========================================
// 读取 Parquet
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_parquet(path_ptr: *const c_char) -> *mut DataFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::open(path)
            .map_err(|e| PolarsError::ComputeError(format!("File not found: {}", e).into()))?;

        let df = ParquetReader::new(file)
            .finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_parquet(path_ptr: *const c_char) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        let args = ScanArgsParquet::default();
        // LazyFrame::scan_parquet 返回 Result，用 ? 抛出
        let lf = LazyFrame::scan_parquet(PlPath::new(path), args)?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

// ==========================================
// 读取 JSON
// ==========================================
// Read JSON (Eager)
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_json(path_ptr: *const c_char) -> *mut DataFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let file = File::open(path).map_err(|e| PolarsError::ComputeError(format!("File not found: {}", e).into()))?;
        
        // JsonReader 需要 BufReader
        let reader = BufReader::new(file);
        let df = JsonReader::new(reader).finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

// Scan NDJSON (Lazy)
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_ndjson(path_ptr: *const c_char) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        // LazyJsonLineReader 接受路径
        let lf = LazyJsonLineReader::new(PlPath::new(path)).finish()?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}
// ==========================================
// IPC
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_ipc(path_ptr: *const c_char) -> *mut DataFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr).unwrap();
        let file = File::open(path).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        let df = IpcReader::new(file).finish()?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_ipc(path_ptr: *const c_char) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr).unwrap();
        // 0.50: ScanArgsIpc::default()
        let args = ScanArgsIpc::default();
        let lf = LazyFrame::scan_ipc(PlPath::new(path), args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sink_ipc(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path = ptr_to_str(path_ptr).unwrap();

        // 1. 准备选项
        let writer_options = IpcWriterOptions::default();
        let sink_options = SinkOptions::default();

        // 2. 构造 Target (使用 PlPath::new 自动处理本地/云路径)
        let target = SinkTarget::Path(PlPath::new(path));

        // 3. [修复] 调用 sink_ipc (4个参数)
        // target, options, cloud_options, sink_options
        let sink_lf = lf_ctx.inner.sink_ipc(
            target, 
            writer_options, 
            None, // CloudOptions
            sink_options
        )?;

        
        let _ = sink_lf
        .with_new_streaming(true)
        .collect()?;
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_from_arrow_record_batch(
    c_array_ptr: *mut ffi::ArrowArray, 
    c_schema_ptr: *mut ffi::ArrowSchema
) -> *mut DataFrameContext {
    ffi_try!({
        // 1. 安全检查: 指针不能为空
        if c_array_ptr.is_null() || c_schema_ptr.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to pl_from_arrow".into()));
        }

        // 2. 导入 Arrow Schema
        let field = unsafe { ffi::import_field_from_c(&*c_schema_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))? };
        
        // 3. 导入 Array
        // import_array_from_c 接收的是 ArrowArray 结构体本身(move)，而不是指针
        // 所以我们需要读取指针指向的内容: unsafe { std::ptr::read(c_array_ptr) }
        let arrow_array_struct = unsafe { std::ptr::read(c_array_ptr) };
        let array = unsafe { 
            ffi::import_array_from_c(arrow_array_struct, field.dtype.clone())
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))? 
        };
        
        let df = match array.as_any().downcast_ref::<StructArray>() {
            Some(struct_arr) => {
                // [修复] 类型注解改为 Vec<Column>
                let columns: Vec<Column> = struct_arr
                    .values()
                    .iter()
                    .zip(struct_arr.fields())
                    .map(|(arr, field)| {
                        let name = PlSmallStr::from_str(&field.name);
                        
                        // Series::from_arrow 返回 PolarsResult<Series>
                        // 我们需要 map 它，把 Series 转为 Column
                        Series::from_arrow(name, arr.clone())
                            .map(|s| Column::from(s)) // [关键] Series -> Column
                    })
                    .collect::<PolarsResult<Vec<_>>>()?;
                
                DataFrame::new(columns)?
            },
            None => {
                // 单列情况也要改
                let name = PlSmallStr::from_str(&field.name);
                let series = Series::from_arrow(name, array)?;
                
                // [修复] vec![Column::from(series)]
                DataFrame::new(vec![Column::from(series)])?
            }
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
// ==========================================
// 2. 写操作 (Void 返回值)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_write_csv(df_ptr: *mut DataFrameContext, path_ptr: *const c_char) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let mut file = File::create(path)
            .map_err(|e| PolarsError::ComputeError(format!("Could not create file: {}", e).into()))?;

        CsvWriter::new(&mut file)
            .finish(&mut ctx.df)?;
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_write_parquet(df_ptr: *mut DataFrameContext, path_ptr: *const c_char) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::create(path)
            .map_err(|e| PolarsError::ComputeError(format!("Could not create file: {}", e).into()))?;

        ParquetWriter::new(file)
            .finish(&mut ctx.df)?;
            
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_ipc(df_ptr: *mut DataFrameContext, path: *const c_char) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        let file = File::create(p.as_ref()).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        IpcWriter::new(file)
            .finish(&mut ctx.df)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_json(df_ptr: *mut DataFrameContext, path: *const c_char) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        let file = File::create(p.as_ref()).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        // 默认输出为标准 JSON Array 格式
        JsonWriter::new(file)
        .with_json_format(JsonFormat::Json)
        .finish(&mut ctx.df)
    })
}
// ==========================================
// 3. 内存与转换操作
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_free_dataframe(ptr: *mut DataFrameContext) {
    ffi_try_void!({
        if !ptr.is_null() {
        // 拿回所有权，离开作用域时自动 Drop (释放内存)
        unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_to_arrow(
    ctx_ptr: *mut DataFrameContext, 
    out_chunk: *mut ArrowArray, 
    out_schema: *mut ArrowSchema
) {
    // 这是一个非常关键的函数，必须捕获 Panic，否则内存越界会崩掉宿主进程
    ffi_try_void!({
        if ctx_ptr.is_null() {
             return Err(PolarsError::ComputeError("Null pointer passed to pl_to_arrow".into()));
        }
        
        let ctx = unsafe { &mut *ctx_ptr };
        let df = &mut ctx.df;

        let columns = df.get_columns()
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

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sink_parquet(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).unwrap();

        let pl_path = PlPath::new(path_str);
        let target = SinkTarget::Path(pl_path.into());

        // 4. 配置项
        let write_options = ParquetWriteOptions::default();
        let sink_options = SinkOptions::default();

        // 5. 执行
        let sink_lf = lf_ctx.inner.sink_parquet(
            target, 
            write_options, 
            None, // cloud_options
            sink_options
        )?;

        let _ = sink_lf
        .with_new_streaming(true)
        .collect()?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sink_json(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).unwrap();
        let pl_path = PlPath::new(path_str);
        
        let target = SinkTarget::Path(pl_path.into());
        let writer_options = JsonWriterOptions::default();
        let sink_options = SinkOptions::default();

        let sink_lf = lf_ctx.inner.sink_json(
            target, 
            writer_options, 
            None, 
            sink_options
        )?;
        
        let _ = sink_lf
        .with_new_streaming(true)
        .collect()?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sink_csv(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).unwrap();
        let pl_path = PlPath::new(path_str);
        
        let target = SinkTarget::Path(pl_path.into());
        let writer_options = CsvWriterOptions::default();
        let sink_options = SinkOptions::default();

        let sink_lf = lf_ctx.inner.sink_csv(
            target, 
            writer_options, 
            None, 
            sink_options
        )?;
        
        let _ = sink_lf
        .with_new_streaming(true)
        .collect()?;

        Ok(())
    })
}



