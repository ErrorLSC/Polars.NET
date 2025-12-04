use polars::prelude::*;
use polars_arrow::ffi::{ArrowArray, ArrowSchema, export_array_to_c, export_field_to_c};
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_core::prelude::CompatLevel;
use std::io::BufReader;
use std::os::raw::c_char;
use std::fs::File;
use crate::types::{DataFrameContext, LazyFrameContext, ptr_to_str};

// ==========================================
// 读取 csv
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_csv(path_ptr: *const c_char, 
    try_parse_dates: bool
) -> *mut DataFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr).unwrap();

        let mut options = CsvReadOptions::default();
        
        // 使用 Arc::make_mut 获取可变引用
        // 这句话的意思是："我要修改 parse_options 里的东西，帮我搞个可写的引用来"
        std::sync::Arc::make_mut(&mut options.parse_options).try_parse_dates = try_parse_dates;

        // 读取数据，但 *不* 转换成 Arrow
        let df = options
            .try_into_reader_with_file_path(Some(path.into()))
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))? // 处理可能的 io error
            .finish()?;

        // 把 DataFrame 装箱，放到堆内存上，并返回指针
        // Box::into_raw 告诉 Rust："我放弃管理这块内存，你把地址给我，别自动回收"
        let context = Box::new(DataFrameContext { df });
        Ok(Box::into_raw(context))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_csv(path_ptr: *const c_char,
    try_parse_dates: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr).unwrap();
        
        // 使用方案 A (PathBuf) 或方案 C (String)，取决于你刚才哪个跑通了
        // 这里假设是用 PathBuf + cloud 关闭，或者 Path + cloud 开启 + new()
        // 无论哪种，重点是最后一行：
        
        let lf = LazyCsvReader::new(PlPath::new(path))
            .with_try_parse_dates(try_parse_dates)
            .finish()?; // ? 使用合法

        // [重要修改] 必须包裹在 Ok() 里
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
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
        let _ = lf_ctx.inner.sink_ipc(
            target, 
            writer_options, 
            None, // CloudOptions
            sink_options
        )?;
        
        Ok(())
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
        let target = SinkTarget::Path(pl_path);

        // 4. 配置项
        let write_options = ParquetWriteOptions::default();
        let sink_options = SinkOptions::default();

        // 5. 执行
        let _ = lf_ctx.inner.sink_parquet(
            target, 
            write_options, 
            None, // cloud_options
            sink_options
        )?;
        
        Ok(())
    })
}


