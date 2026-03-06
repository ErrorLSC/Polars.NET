use polars::prelude::*;
use polars_arrow::ffi::{self, ArrowArray, ArrowSchema, export_array_to_c, export_field_to_c};
use polars_arrow::array::{StructArray};
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_core::prelude::CompatLevel;
use polars_core::utils::Container;
use std::ffi::c_void;
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

// ==========================================
// Streaming Sink to DataBase
// ==========================================
// 1. Define Callback
type SinkCallback = extern "C" fn(
    *mut ffi::ArrowArray, 
    *mut ffi::ArrowSchema,
    *mut std::os::raw::c_char
) -> i32;

type CleanupCallback = extern "C" fn(*mut c_void);

// 2. Define Struct 
#[derive(Clone)]
struct CSharpSinkUdf {
    callback: SinkCallback,
    cleanup: CleanupCallback,
    user_data: *mut c_void, // GCHandle
}

// Send + Sync
unsafe impl Send for CSharpSinkUdf {}
unsafe impl Sync for CSharpSinkUdf {}

impl Drop for CSharpSinkUdf {
    fn drop(&mut self) {
        (self.cleanup)(self.user_data);
    }
}

impl CSharpSinkUdf {
    fn call(&self, mut df: DataFrame) -> PolarsResult<DataFrame> {
        // 👑 现代 API 核心：确保 DataFrame 的所有列都切分成了完全相同长度的 Chunk 块
        df.align_chunks();

        // Error Message Buffer (1KB)
        let mut error_msg_buf = [0u8; 1024]; 
        let error_ptr = error_msg_buf.as_mut_ptr() as *mut std::os::raw::c_char;

        // 遍历对齐后的每一个 Chunk (批次)
        for chunk_idx in 0..df.n_chunks() {
            let mut fields = Vec::with_capacity(df.width());
            let mut arrays = Vec::with_capacity(df.width());

            // 手动拉链：把每一列当前的 Chunk 抽出来
            for s in df.columns() {
                // 拿到最底层的物理 Arrow Array (Box<dyn Array>)
                let chunk_array = s.as_materialized_series().chunks()[chunk_idx].clone();

                // 👑 终极杀招：不信任 Polars 的逻辑 dtype，只信任底层物理数组的真实类型！
                let arrow_type = chunk_array.dtype().clone();
                
                let field = ArrowField::new(s.name().clone(), arrow_type, true);

                fields.push(field);
                arrays.push(chunk_array);
            }
            
            // 提取 Chunk 长度
            let chunk_len = arrays.first().map(|a| a.len()).unwrap_or(0);
            
            // 自己动手组装 StructArray (这就是标准的 Arrow RecordBatch)
            let struct_dtype = ArrowDataType::Struct(fields);
            let struct_array = StructArray::new(struct_dtype.clone(), chunk_len, arrays, None);

            // 转成 trait object，准备发射
            let array_ref: Box<dyn polars_arrow::array::Array> = Box::new(struct_array);
            let batch_field = ArrowField::new("batch".into(), struct_dtype, true);

            // 完美的 FFI 导出
            let c_array = ffi::export_array_to_c(array_ref);
            let c_schema = ffi::export_field_to_c(&batch_field);

            // Alloc array to heap
            let ptr_array = Box::into_raw(Box::new(c_array));
            let ptr_schema = Box::into_raw(Box::new(c_schema));

            // Call C# Callback
            let status = (self.callback)(ptr_array, ptr_schema, error_ptr);

            // 👑 拦截 C# 抛出的异常并转为 Rust Panic/Error
            if status != 0 {
                let msg = unsafe { std::ffi::CStr::from_ptr(error_ptr).to_string_lossy().into_owned() };
                return Err(PolarsError::ComputeError(format!("C# Sink Failed: {}", msg).into()));
            }
        }

        // Return empty DataFrame
        Ok(DataFrame::empty())
    }
}



#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_map_batches(
    lf_ptr: *mut LazyFrameContext,
    callback: SinkCallback,
    cleanup: CleanupCallback,
    user_data: *mut c_void
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        // Build UDF Object
        let udf = Arc::new(CSharpSinkUdf { 
            callback, 
            cleanup, 
            user_data 
        });

        let new_lf = lf_ctx.inner.map(
            // 1. function
            move |df| udf.call(df),
            
            // 2. optimizations
            AllowedOptimizations::default(), 

            // 3. schema
            None,

            // 4. name
            Some("csharp_sink"), 
        );

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_export_batches(
    df_ptr: *mut DataFrameContext,
    callback: SinkCallback,
    cleanup: CleanupCallback,
    user_data: *mut c_void
) {
    ffi_try_void!({
        let df_ctx = unsafe { &*df_ptr }; 
        // 👑 必须 clone 一份所有权，因为我们需要在原地对齐内存块
        let mut df = df_ctx.df.clone(); 

        let udf = CSharpSinkUdf { 
            callback, 
            cleanup, 
            user_data 
        };

        // 👑 现代 API 核心：确保 DataFrame 的所有列都切分成了完全相同长度的 Chunk 块
        // 这是安全组装 StructArray 的大前提！
        df.align_chunks();

        let mut error_msg_buf = [0u8; 1024]; 
        let error_ptr = error_msg_buf.as_mut_ptr() as *mut std::os::raw::c_char;

        // 遍历对齐后的每一个 Chunk (批次)
        for chunk_idx in 0..df.n_chunks() {
            let mut fields = Vec::with_capacity(df.width());
            let mut arrays = Vec::with_capacity(df.width());

            // 手动拉链：把每一列当前的 Chunk 抽出来
            for s in df.columns() {
                // 拿到最底层的物理 Arrow Array (Box<dyn Array>)
                let chunk_array = s.as_materialized_series().chunks()[chunk_idx].clone();

                // 👑 终极杀招：不信任 Polars 的逻辑 dtype，只信任底层物理数组的真实类型！
                // (注: 在 arrow2 中获取数组类型的标准方法是 .data_type()，如果你之前的 .dtype() 是宏或别名，请自行适配)
                let arrow_type = chunk_array.dtype().clone();
                
                let field = ArrowField::new(s.name().clone(), arrow_type, true);

                fields.push(field);
                arrays.push(chunk_array);
            }
            let chunk_len = arrays.first().map(|a| a.len()).unwrap_or(0);
            // 自己动手组装 StructArray (这就是标准的 Arrow RecordBatch)
            let struct_dtype = ArrowDataType::Struct(fields);
            let struct_array = StructArray::new(struct_dtype.clone(),chunk_len, arrays, None);

            // 转成 trait object，准备发射
            let array_ref: Box<dyn polars_arrow::array::Array> = Box::new(struct_array);
            let batch_field = ArrowField::new("batch".into(), struct_dtype, true);

            // 完美的 FFI 导出
            let c_array = ffi::export_array_to_c(array_ref);
            let c_schema = ffi::export_field_to_c(&batch_field);

            let ptr_array = Box::into_raw(Box::new(c_array));
            let ptr_schema = Box::into_raw(Box::new(c_schema));

            // Call C# Callback
            let status = (udf.callback)(ptr_array, ptr_schema, error_ptr);

            if status != 0 {
                return Ok(()); 
            }
        }
        
        Ok(())
    })
}