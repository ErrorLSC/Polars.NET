use std::ffi::{CString, c_char, c_int};
use polars::prelude::*;
use polars_plan::utils::expr_to_leaf_column_names;
use crate::types::{ExprContext, SchemaContext, SelectorContext};

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_meta_eq(
    expr_ptr: *const ExprContext,
    other_ptr: *const ExprContext,
    out_val: *mut bool,
) -> c_int {
    ffi_eval_out_try!(out_val, {
        let e1 = unsafe { &*expr_ptr };
        let e2 = unsafe { &*other_ptr };
        
        Ok(e1.inner == e2.inner) 
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_get_output_name(
    expr_ptr: *const ExprContext,
    out_str: *mut *mut c_char
) -> c_int {
    ffi_eval_out_try!(out_str, {
        let ctx = unsafe { &*expr_ptr };
        
        let name = ctx.inner.clone().meta().output_name()?;
        
        let c_str = CString::new(name.as_str())
            .map_err(|e| PolarsError::ComputeError(
                format!("Failed to convert output name to CString: {}", e).into()
            ))?;
            
        Ok(c_str.into_raw())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_try_into_selector(
    expr_ptr: *mut ExprContext
) -> *mut SelectorContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let selector = ctx.inner.try_into_selector()?;
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: selector })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_meta_is_column(
    expr_ptr: *const ExprContext,
    out_val: *mut bool,
) -> c_int {
    ffi_eval_out_try!(out_val, {
        let ctx = unsafe { &*expr_ptr };
        Ok(ctx.inner.clone().meta().is_column())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_meta_is_column_selection(
    expr_ptr: *const ExprContext,
    allow_aliasing: bool,
    out_val: *mut bool,
) -> c_int {
    ffi_eval_out_try!(out_val, {
        let ctx = unsafe { &*expr_ptr };
        Ok(ctx.inner.clone().meta().is_column_selection(allow_aliasing))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_meta_is_literal(
    expr_ptr: *const ExprContext,
    allow_aliasing: bool,
    out_val: *mut bool,
) -> c_int {
    ffi_eval_out_try!(out_val, {
        let ctx = unsafe { &*expr_ptr };
        Ok(ctx.inner.clone().meta().is_literal(allow_aliasing))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_meta_is_regex_projection(
    expr_ptr: *const ExprContext,
    out_val: *mut bool,
) -> c_int {
    ffi_eval_out_try!(out_val, {
        let ctx = unsafe { &*expr_ptr };
        Ok(ctx.inner.clone().meta().is_regex_projection())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_meta_has_multiple_outputs(
    expr_ptr: *const ExprContext,
    out_val: *mut bool,
) -> c_int {
    ffi_eval_out_try!(out_val, {
        let ctx = unsafe { &*expr_ptr };
        Ok(ctx.inner.clone().meta().has_multiple_outputs())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_meta_undo_aliases(
    expr_ptr: *mut ExprContext,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr)};
        let new_expr = ctx.inner.meta().undo_aliases();

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_meta_root_names(
    expr_ptr: *const ExprContext,
    out_str: *mut *mut c_char,
) -> c_int {
    ffi_eval_out_try!(out_str, {
        let ctx = unsafe { &*expr_ptr };
        let names = expr_to_leaf_column_names(&ctx.inner);
        
        let joined: String = names
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join("\x1F");
            
        let c_str = CString::new(joined)
            .map_err(|e| PolarsError::ComputeError(format!("CString error: {}", e).into()))?;
            
        Ok(c_str.into_raw())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_meta_pop(
    expr_ptr: *mut ExprContext,
    out_ptrs: *mut *mut *mut ExprContext,
    out_len: *mut usize,
) -> c_int {
    ffi_try_c_int!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let mut out_exprs = Vec::new();
        ctx.inner.nodes_owned(&mut out_exprs);
        
        unsafe { *out_len = out_exprs.len() };
        
        let mut ptrs: Vec<*mut ExprContext> = out_exprs
            .into_iter()
            .map(|e| Box::into_raw(Box::new(ExprContext { inner: e })))
            .collect();
            
        unsafe { *out_ptrs = ptrs.as_mut_ptr() };

        std::mem::forget(ptrs);
        
        Ok(0) 
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_meta_into_tree_formatter(
    expr_ptr: *const ExprContext,
    display_as_dot: bool,
    schema_ptr: *const SchemaContext,
    out_str: *mut *mut c_char,
) -> c_int {
    ffi_eval_out_try!(out_str, {
        let ctx = unsafe { &*expr_ptr };
        
        let schema_opt = if schema_ptr.is_null() {
            None
        } else {
            let schema_ctx = unsafe { &*schema_ptr };
            Some(&schema_ctx.schema) 
        };
        
        let formatter = ctx.inner.clone().meta().into_tree_formatter(display_as_dot, schema_opt.map(|v| &**v))?;
        
        let formatted_str = format!("{}", formatter);
        
        let c_str = CString::new(formatted_str)
            .map_err(|e| PolarsError::ComputeError(format!("CString error: {}", e).into()))?;
            
        Ok(c_str.into_raw())
    })
}