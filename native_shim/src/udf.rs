use polars::prelude::*;
use polars_arrow::ffi;
use crate::types::{ExprContext,DataTypeContext};
use std::sync::Arc;
use polars_arrow::datatypes::Field as ArrowField;
use std::ffi::{CStr,c_void};

// Define Cleanup Callback
type CleanupCallback = extern "C" fn(*mut c_void);

impl Drop for CSharpUdf {
    fn drop(&mut self) {
        (self.cleanup)(self.user_data);
    }
}

type UdfCallback = extern "C" fn(
    *const ffi::ArrowArray, 
    *const ffi::ArrowSchema, 
    *mut ffi::ArrowArray, 
    *mut ffi::ArrowSchema,
    *mut std::os::raw::c_char
) -> i32;

#[derive(Clone)]
struct CSharpUdf {
    callback: UdfCallback,
    cleanup: CleanupCallback, 
    user_data: *mut c_void,   
}

unsafe impl Send for CSharpUdf {}
unsafe impl Sync for CSharpUdf {}

impl CSharpUdf {
    fn call(&self, s: Series) -> PolarsResult<Option<Series>> {
        let s = s.rechunk();
        let array = s.to_arrow(0, CompatLevel::newest());
        
        let field = ArrowField::new("".into(), array.dtype().clone(), true);
        
        let c_array_in = ffi::export_array_to_c(array);
        let c_schema_in = ffi::export_field_to_c(&field);

        let mut c_array_out = ffi::ArrowArray::empty();
        let mut c_schema_out = ffi::ArrowSchema::empty();

        let mut error_msg_buf = [0u8; 1024]; 
        let error_ptr = error_msg_buf.as_mut_ptr() as *mut std::os::raw::c_char;

        let status = (self.callback)(&c_array_in, &c_schema_in, &mut c_array_out, &mut c_schema_out, error_ptr);
        if status != 0 {
            let msg = unsafe { CStr::from_ptr(error_ptr).to_string_lossy().into_owned() };
            return Err(PolarsError::ComputeError(format!("C# UDF Failed: {}", msg).into()));
        }
        let out_field = unsafe { ffi::import_field_from_c(&c_schema_out).map_err(|e| PolarsError::ComputeError(e.to_string().into()))? };
        let out_array = unsafe { ffi::import_array_from_c(c_array_out, out_field.dtype.clone()).map_err(|e| PolarsError::ComputeError(e.to_string().into()))? };

        let out_series = Series::try_from((s.name().clone(), out_array))?;
        
        Ok(Some(out_series))
    }
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_map(
    expr_ptr: *mut ExprContext,
    callback: UdfCallback,
    output_type_ptr: *mut DataTypeContext,
    cleanup: CleanupCallback,
    user_data: *mut c_void 
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let udf = Arc::new(CSharpUdf { callback,cleanup,user_data });
        let target_dtype = unsafe { &(*output_type_ptr).dtype };
        let target_dtype_owned = target_dtype.clone();

        let output_map = move |_input_schema: &Schema, input_field: &Field| -> PolarsResult<Field> {
            match &target_dtype_owned {
                // Unknown means keep Identity
                DataType::Unknown(_) => Ok(input_field.clone()),
                
                known_dtype => Ok(Field::new(input_field.name().clone(), known_dtype.clone())),
            }
        };

        let new_expr = ctx.inner.map(
            move |c| {
                // Column -> Series
                let s = c.as_materialized_series().clone();
                
                // Call C# UDF
                let res_series_opt = udf.call(s)?;
                
                // Unwrap Option (C# UDF must return a Series)
                let out_s = res_series_opt.expect("C# UDF returned None");
                
                // Series -> Column
                Ok(out_s.into_column())
            }, 
            output_map
        );

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}