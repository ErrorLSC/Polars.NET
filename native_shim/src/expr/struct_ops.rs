use polars::prelude::*;
use std::os::raw::c_char;
use crate::types::ExprContext;
use crate::utils::{consume_exprs_array, ptr_to_vec_string};

// as_struct(exprs) -> Expr
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_as_struct(
    exprs_ptr: *const *mut ExprContext,
    len: usize
) -> *mut ExprContext {
    ffi_try!({
        let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };
        // polars::prelude::as_struct
        let new_expr = as_struct(exprs);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// struct.field_by_name(name)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_struct_field_by_names(
    expr_ptr: *mut ExprContext,
    names_ptr: *const *const c_char,
    names_len: usize,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let names = unsafe { ptr_to_vec_string(names_ptr, names_len) };
        
        let new_expr = ctx.inner.struct_().field_by_names(names);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_struct_field_by_index(
    expr: *mut Expr, 
    index: i64
) -> *mut Expr {
    let e = unsafe { Box::from_raw(expr)};

    let new_expr = e.struct_().field_by_index(index);
    Box::into_raw(Box::new(new_expr))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_struct_rename_fields(
    expr: *mut Expr,
    names_ptr: *mut *mut c_char,
    len: usize
) -> *mut Expr {
    let e = unsafe { Box::from_raw(expr) };
    
    // Convert C String vector to Vec<String>
    let names: Vec<String> = if names_ptr.is_null() || len == 0 {
        Vec::new()
    } else {
        let slice = unsafe { std::slice::from_raw_parts(names_ptr, len) };
        slice.iter()
            .map(|&p| unsafe { 
                std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned() 
            })
            .collect()
    };

    let new_expr = e.struct_().rename_fields(names);
    Box::into_raw(Box::new(new_expr))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_struct_drop(
    expr: *mut Expr,
    names_ptr: *mut *mut c_char,
    len: usize,
    strict: bool
) -> *mut Expr {
    let e = unsafe { Box::from_raw(expr) };
    
    // Convert C String vector to Vec<String>
    let names: Vec<String> = if names_ptr.is_null() || len == 0 {
        Vec::new()
    } else {
        let slice = unsafe { std::slice::from_raw_parts(names_ptr, len) };
        slice.iter()
            .map(|&p| unsafe { 
                std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned() 
            })
            .collect()
    };

    let new_expr = e.struct_().drop(names,strict);
    Box::into_raw(Box::new(new_expr))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_struct_json_encode(
    expr: *mut Expr
) -> *mut Expr {
    let e = unsafe { Box::from_raw(expr)};
    let new_expr = e.struct_().json_encode();
    Box::into_raw(Box::new(new_expr))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_struct_with_fields(
    expr_ptr: *mut ExprContext,
    fields_ptr: *const *mut ExprContext,
    fields_len: usize,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let fields = unsafe { consume_exprs_array(fields_ptr, fields_len) };
        
        let new_expr = ctx.inner.struct_().with_fields(fields);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}