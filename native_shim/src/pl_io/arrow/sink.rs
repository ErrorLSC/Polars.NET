use polars::prelude::*;
use polars_arrow::ffi::{self};
use polars_arrow::array::{StructArray};
use polars_arrow::datatypes::ArrowDataType;

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

        // Logical Arrow types (Decimal128, dictionaries, ...), not the engine's physical chunks.
        let fields: Vec<ArrowField> = df.schema().to_arrow(CompatLevel::newest()).iter_values().cloned().collect();
        let struct_dtype = ArrowDataType::Struct(fields);
        let batch_field = ArrowField::new("batch".into(), struct_dtype.clone(), true);

        for batch in df.iter_chunks(CompatLevel::newest(), true) {
            let chunk_len = batch.len();
            let struct_array = StructArray::new(struct_dtype.clone(), chunk_len, batch.into_arrays(), None);

            // FFI Export
            let ptr_array = Box::into_raw(Box::new(ffi::export_array_to_c(Box::new(struct_array))));
            let ptr_schema = Box::into_raw(Box::new(ffi::export_field_to_c(&batch_field)));

            // Call C# Callback
            let status = (self.callback)(ptr_array, ptr_schema, error_ptr);

            // C# moved the array out (its release is cleared); dropping releases whatever it left.
            unsafe {
                drop(Box::from_raw(ptr_array));
                drop(Box::from_raw(ptr_schema));
            }

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

        // A streaming callback sink: the engine hands each batch to C# as it is produced (in order), instead of
        // collecting the whole frame for one call.
        let new_lf = lf_ctx.inner.sink_batches(
            polars_plan::callback::PlanCallback::new(move |df| udf.call(df).map(|_| false)),
            true,
            None,
        )?;

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