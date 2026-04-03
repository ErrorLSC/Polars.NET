use polars::prelude::*;
use polars_arrow::ffi;
use crate::{types::{DataFrameContext, DataTypeContext, ExprContext, LazyFrameContext}, utils::consume_exprs_array};
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

// ==========================================
// Expr Map Many (Multiple Inputs -> Single Output)
// ==========================================

//Define C# Callback for multi-column mapping
pub type MultiUdfCallback = extern "C" fn(
    num_args: usize,
    in_arrays: *const *const ffi::ArrowArray,
    in_schemas: *const *const ffi::ArrowSchema,
    out_array: *mut ffi::ArrowArray,
    out_schema: *mut ffi::ArrowSchema,
    user_data: *mut c_void,
) -> i32;

#[derive(Clone)]
struct CSharpMultiUdf {
    callback: MultiUdfCallback,
    cleanup: CleanupCallback,
    user_data: *mut c_void,
}

unsafe impl Send for CSharpMultiUdf {}
unsafe impl Sync for CSharpMultiUdf {}

impl Drop for CSharpMultiUdf {
    fn drop(&mut self) {
        if !self.user_data.is_null() {
            (self.cleanup)(self.user_data);
        }
    }
}

impl CSharpMultiUdf {
    fn call(&self, cols: &mut [Column]) -> PolarsResult<Column> {
        let num_args = cols.len();
        
        let mut array_ptrs = Vec::with_capacity(num_args);
        let mut schema_ptrs = Vec::with_capacity(num_args);

        // Convert all input Columns to Arrow C Data Pointers
        for col in cols.iter() {
            let s = col.as_materialized_series().rechunk();
            let array = s.to_arrow(0, CompatLevel::newest());
            let field = ArrowField::new("".into(), array.dtype().clone(), true);
            
            // Allocate C structs on the heap to get stable pointers
            let c_array = Box::into_raw(Box::new(ffi::export_array_to_c(array)));
            let c_schema = Box::into_raw(Box::new(ffi::export_field_to_c(&field)));
            
            array_ptrs.push(c_array as *const ffi::ArrowArray);
            schema_ptrs.push(c_schema as *const ffi::ArrowSchema);
        }

        // Prepare output structures
        let mut out_array = ffi::ArrowArray::empty();
        let mut out_schema = ffi::ArrowSchema::empty();

        // Call C#
        let res_code = (self.callback)(
            num_args,
            array_ptrs.as_ptr(),
            schema_ptrs.as_ptr(),
            &mut out_array as *mut ffi::ArrowArray,
            &mut out_schema as *mut ffi::ArrowSchema,
            self.user_data,
        );

        // Free the allocated pointers for inputs
        for (arr_ptr, schema_ptr) in array_ptrs.into_iter().zip(schema_ptrs.into_iter()) {
            unsafe {
                let _ = Box::from_raw(arr_ptr as *mut ffi::ArrowArray);
                let _ = Box::from_raw(schema_ptr as *mut ffi::ArrowSchema);
            }
        }

        // Handle Error
        if res_code != 0 {
            return Err(PolarsError::ComputeError(
                "C# Multi-Column UDF returned an error".into()
            ));
        }

        // Convert Output Arrow C Data back to Polars Column
        unsafe {
            let out_field = ffi::import_field_from_c(&out_schema)?;
            let out_arr = ffi::import_array_from_c(out_array, out_field.dtype.clone())?;
            
            let out_series = Series::try_from((&out_field, out_arr))?; 
            
            Ok(out_series.into_column())
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_map_many(
    base_expr_ptr: *mut ExprContext,
    args_ptr: *const *mut ExprContext,
    args_len: usize,
    callback: MultiUdfCallback,
    output_type_ptr: *mut DataTypeContext,
    cleanup: CleanupCallback,
    user_data: *mut c_void,
) -> *mut ExprContext {
    ffi_try!({
        // Consume base Expr
        let base_ctx = unsafe { Box::from_raw(base_expr_ptr) };
        // Consume additional arguments
        let args = unsafe { consume_exprs_array(args_ptr, args_len) };
        
        let udf = Arc::new(CSharpMultiUdf { callback, cleanup, user_data });
        
        // Clone output dtype for the schema closure
        let target_dtype_owned = unsafe { (*output_type_ptr).dtype.clone() };

        let new_expr = base_ctx.inner.map_many(
            move |cols: &mut [Column]| -> PolarsResult<Column> {
                udf.call(cols)
            },
            &args,
            move |_schema: &Schema, _fields: &[Field]| -> PolarsResult<Field> {
                // Return schema definition for optimizer
                match &target_dtype_owned {
                    DataType::Unknown(_) => Ok(Field::new("output".into(), DataType::Unknown(Default::default()))),
                    known => Ok(Field::new("output".into(), known.clone()))
                }
            }
        );

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Define UDF Callback for DataFrame -> DataFrame
pub type DfUdfCallback = extern "C" fn(
    *mut DataFrameContext, 
    *mut std::ffi::c_void
) -> *mut DataFrameContext;

struct CSharpDataFrameUdf {
    callback: DfUdfCallback,
    cleanup: CleanupCallback,
    user_data: *mut std::ffi::c_void,
}

// Polars requires the closure to be Send + Sync to run in parallel
unsafe impl Send for CSharpDataFrameUdf {}
unsafe impl Sync for CSharpDataFrameUdf {}

impl Drop for CSharpDataFrameUdf {
    fn drop(&mut self) {
        (self.cleanup)(self.user_data);
    }
}

// ==========================================
// LazyFrame Map (map_batches)
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_map(
    lf_ptr: *mut LazyFrameContext,
    callback: DfUdfCallback,
    cleanup: CleanupCallback,
    user_data: *mut std::ffi::c_void,
    predicate_pushdown: bool,
    projection_pushdown: bool,
    slice_pushdown: bool,
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let udf = Arc::new(CSharpDataFrameUdf { callback, cleanup, user_data });

        // Map python-like pushdown flags to Rust AllowedOptimizations
        let mut optimizations = AllowedOptimizations::default();

        optimizations.set(AllowedOptimizations::PREDICATE_PUSHDOWN, predicate_pushdown);
        optimizations.set(AllowedOptimizations::PROJECTION_PUSHDOWN, projection_pushdown);
        optimizations.set(AllowedOptimizations::SLICE_PUSHDOWN, slice_pushdown);

        let new_lf = lf_ctx.inner.map(
            move |df: DataFrame| -> PolarsResult<DataFrame> {
                // Box the incoming DataFrame and pass its pointer to C#
                let df_ptr = Box::into_raw(Box::new(DataFrameContext { df }));
                
                // Call the C# UDF
                let res_ptr = (udf.callback)(df_ptr, udf.user_data);
                
                // Check if C# returned an error/null
                if res_ptr.is_null() {
                    return Err(PolarsError::ComputeError(
                        "C# LazyFrame.Map (map_batches) UDF failed or returned null".into()
                    ));
                }
                
                // Take back ownership from the returned pointer
                let res_ctx = unsafe { Box::from_raw(res_ptr) };
                Ok(res_ctx.df)
            },
            optimizations,
            None, // Optional: schema inference isn't supported via pure pointer FFI yet
            Some("csharp_lazy_map"),
        );

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

