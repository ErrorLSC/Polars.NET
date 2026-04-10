use polars::prelude::*;
use std::os::raw::c_char;
use crate::gen_namespace_unary;
use crate::types::ExprContext;
use crate::utils::ptr_to_str;

gen_namespace_unary!(pl_expr_list_first, list, first);
gen_namespace_unary!(pl_expr_list_sum, list, sum);
gen_namespace_unary!(pl_expr_list_min, list, min);
gen_namespace_unary!(pl_expr_list_max, list, max);
gen_namespace_unary!(pl_expr_list_mean, list, mean);

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_get(
    expr_ptr: *mut ExprContext, 
    index: i64
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.list().get(lit(index),true);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_sort(
    expr_ptr: *mut ExprContext,
    descending: bool,
    nulls_last: bool,      
    maintain_order: bool   
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let options = SortOptions {
            descending,
            nulls_last,
            multithreaded: true, 
            maintain_order,
            limit: None
        };
        
        let new_expr = ctx.inner.list().sort(options);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// 3. list.contains(item)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_contains(
    expr_ptr: *mut ExprContext,
    item_ptr: *mut ExprContext,
    nulls_equal: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let item = unsafe { Box::from_raw(item_ptr) };

        let new_expr = ctx.inner.list().contains(item.inner, nulls_equal);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_reverse(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.list().reverse();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_join(
    expr_ptr: *mut ExprContext,
    sep_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let sep = ptr_to_str(sep_ptr).unwrap();
        // list().join(sep, ignore_nulls=true)
        let new_expr = ctx.inner.list().join(lit(sep), true);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_len(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.list().len();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
