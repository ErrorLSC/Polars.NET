use polars::error::polars_bail;
use polars_arrow::ffi::{ArrowArray, ArrowSchema, export_array_to_c, export_field_to_c};
use polars_arrow::datatypes::Field;
use polars::error::PolarsResult;

pub mod scan;
pub mod sink;
pub mod toarrowstream;

pub struct ArrowArrayContext {
    pub array: Box<dyn polars_arrow::array::Array>, 
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_arrow_array_free(ptr: *mut ArrowArrayContext) {
    if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_arrow_array_export(
    ptr: *mut ArrowArrayContext,
    out_c_array: *mut ArrowArray 
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "ArrowArrayContext pointer is null");
        }
        if out_c_array.is_null() {
            polars_bail!(ComputeError: "Output ArrowArray pointer is null");
        }

        let ctx = unsafe { &*ptr };
        let array = ctx.array.clone(); 

        let rust_arrow_array = export_array_to_c(array);

        unsafe {
            std::ptr::write(out_c_array, rust_arrow_array);
        }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_arrow_schema_export(
    ptr: *mut ArrowArrayContext,
    out_c_schema: *mut ArrowSchema
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "ArrowArrayContext pointer is null");
        }
        if out_c_schema.is_null() {
            polars_bail!(ComputeError: "Output ArrowSchema pointer is null");
        }

        let ctx = unsafe { &*ptr };
        let dtype = ctx.array.dtype().clone();

        let field = Field::new("".into(), dtype, true);
        let rust_arrow_schema = export_field_to_c(&field);

        unsafe {
            std::ptr::write(out_c_schema, rust_arrow_schema);
        }
        
        Ok(())
    })
}