use std::ffi::c_char;

use polars::prelude::*;

use crate::{types::{DataTypeContext, DataTypeExprContext, ExprContext, SchemaContext, SelectorContext}, utils::ptr_to_str};

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_free(ptr: *mut DataTypeExprContext) {
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_clone(
    ptr: *mut DataTypeExprContext
) -> *mut DataTypeExprContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        
        let new_expr = ctx.inner.clone();
        
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}

// ==========================================
// DataType
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_dtype_of(
    expr_ptr: *mut ExprContext
) -> *mut DataTypeExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let new_expr = DataTypeExpr::OfExpr(Box::new(ctx.inner));
        
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_self_dtype() -> *mut DataTypeExprContext {
    ffi_try!({
        let new_expr = DataTypeExpr::SelfDtype; 
        
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_from_datatype(
    dtype_ptr: *mut DataTypeContext
) -> *mut DataTypeExprContext {
    ffi_try!({
        let ctx = unsafe { &*dtype_ptr };
        
        let new_expr = DataTypeExpr::Literal(ctx.dtype.clone());
        
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_into_datatype(
    ptr: *mut DataTypeExprContext,
    schema_ptr: *mut SchemaContext
) -> *mut DataTypeContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        
        if schema_ptr.is_null() {
            polars_bail!(ComputeError: "schema pointer cannot be null in pl_datatype_expr_into_datatype");
        }
        
        let schema_ctx = unsafe { &*schema_ptr };
        
        let dt = ctx.inner.into_datatype(&schema_ctx.schema)?;
        Ok(Box::into_raw(Box::new(DataTypeContext { dtype: dt })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_into_literal(
    ptr: *mut DataTypeExprContext
) -> *mut DataTypeContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        if let Some(dt) = ctx.inner.into_literal() {
            Ok(Box::into_raw(Box::new(DataTypeContext { dtype: dt })))
        } else {
            Ok(std::ptr::null_mut())
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_inner_dtype(
    ptr: *mut DataTypeExprContext
) -> *mut DataTypeExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let new_expr = ctx.inner.inner_dtype();
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_equals(
    left_ptr: *mut DataTypeExprContext,
    right_ptr: *mut DataTypeExprContext
) -> *mut ExprContext {
    ffi_try!({
        let left = unsafe { Box::from_raw(left_ptr) };
        let right = unsafe { Box::from_raw(right_ptr) };
        
        let expr = left.inner.equals(right.inner);
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_display(
    ptr: *mut DataTypeExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let expr = ctx.inner.display();
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_default_value(
    ptr: *mut DataTypeExprContext,
    n: usize,
    numeric_to_one: bool,
    num_list_values: usize
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let expr = ctx.inner.default_value(n, numeric_to_one, num_list_values);
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_matches(
    ptr: *mut DataTypeExprContext,
    sel_ptr: *mut SelectorContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let sel_ctx = unsafe { Box::from_raw(sel_ptr) };

        let dt_sel = match sel_ctx.inner {
            Selector::ByDType(dts) => dts,
            _ => polars_bail!(ComputeError: "Provided selector is not a DataTypeSelector. Please use dtype-based selectors (e.g., pl_selector_by_dtype, pl_selector_numeric)."),
        };
        
        let expr = ctx.inner.matches(dt_sel);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_wrap_in_list(
    ptr: *mut DataTypeExprContext
) -> *mut DataTypeExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let new_expr = ctx.inner.wrap_in_list();
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_wrap_in_array(
    ptr: *mut DataTypeExprContext,
    width: usize
) -> *mut DataTypeExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let new_expr = ctx.inner.wrap_in_array(width);
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}

// ==========================================
// Int Namespace (.int())
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_int_to_unsigned(
    ptr: *mut DataTypeExprContext
) -> *mut DataTypeExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let new_expr = ctx.inner.int().to_unsigned();
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_int_to_signed(
    ptr: *mut DataTypeExprContext
) -> *mut DataTypeExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let new_expr = ctx.inner.int().to_signed();
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}

// ==========================================
// List Namespace (.list())
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_list_inner_dtype(
    ptr: *mut DataTypeExprContext
) -> *mut DataTypeExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let new_expr = ctx.inner.list().inner_dtype();
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}

// ==========================================
// Array Namespace (.arr())
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_arr_inner_dtype(
    ptr: *mut DataTypeExprContext
) -> *mut DataTypeExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let new_expr = ctx.inner.arr().inner_dtype();
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_arr_width(
    ptr: *mut DataTypeExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let expr = ctx.inner.arr().width();
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_arr_shape(
    ptr: *mut DataTypeExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let expr = ctx.inner.arr().shape();
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

// ==========================================
// Struct Namespace (.struct_())
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_struct_field_dtype_by_index(
    ptr: *mut DataTypeExprContext,
    index: i64
) -> *mut DataTypeExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let new_expr = ctx.inner.struct_().field_dtype_by_index(index);
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_struct_field_dtype_by_name(
    ptr: *mut DataTypeExprContext,
    name_ptr: *const c_char
) -> *mut DataTypeExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let name = ptr_to_str(name_ptr).unwrap();
        
        let new_expr = ctx.inner.struct_().field_dtype_by_name(name);
        Ok(Box::into_raw(Box::new(DataTypeExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_datatype_expr_struct_field_names(
    ptr: *mut DataTypeExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(ptr) };
        let expr = ctx.inner.struct_().field_names();
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}