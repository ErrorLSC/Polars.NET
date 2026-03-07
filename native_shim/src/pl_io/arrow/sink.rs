use polars::prelude::*;
use polars_arrow::ffi::{self};
use polars_arrow::array::{StructArray};
use polars_arrow::datatypes::ArrowDataType;

use polars_core::utils::Container;
use std::ffi::{c_char, c_void};
use crate::types::{DataFrameContext, LazyFrameContext};

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
        df.align_chunks();

        // Error Message Buffer (1KB)
        let mut error_msg_buf = [0u8; 1024]; 
        let error_ptr = error_msg_buf.as_mut_ptr() as *mut c_char;

        // Iter Chunks 
        for chunk_idx in 0..df.n_chunks() {
            let mut fields = Vec::with_capacity(df.width());
            let mut arrays = Vec::with_capacity(df.width());

            // Get each Chunk
            for s in df.columns() {
                // Get Arrow Array (Box<dyn Array>)
                let chunk_array = s.as_materialized_series().chunks()[chunk_idx].clone();

                // Get dtype, notice: logic type lost here
                let arrow_type = chunk_array.dtype().clone();
                
                let field = ArrowField::new(s.name().clone(), arrow_type, true);

                fields.push(field);
                arrays.push(chunk_array);
            }
            
            // Get Chunk Length
            let chunk_len = arrays.first().map(|a| a.len()).unwrap_or(0);
            
            // Construct StructArray
            let struct_dtype = ArrowDataType::Struct(fields);
            let struct_array = StructArray::new(struct_dtype.clone(), chunk_len, arrays, None);

            // Convert to trait object
            let array_ref: Box<dyn polars_arrow::array::Array> = Box::new(struct_array);
            let batch_field = ArrowField::new("batch".into(), struct_dtype, true);

            // FFI Export
            let c_array = ffi::export_array_to_c(array_ref);
            let c_schema = ffi::export_field_to_c(&batch_field);

            // Alloc array to heap
            let ptr_array = Box::into_raw(Box::new(c_array));
            let ptr_schema = Box::into_raw(Box::new(c_schema));

            // Call C# Callback
            let status = (self.callback)(ptr_array, ptr_schema, error_ptr);

            // Handle Rust Panic/Error
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
        let df = df_ctx.df.clone(); 

        let udf = CSharpSinkUdf { 
            callback, 
            cleanup, 
            user_data 
        };

        udf.call(df)?;

        Ok(())
    })
}