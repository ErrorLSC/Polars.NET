use polars::prelude::*;
use std::os::raw::c_char;
use crate::gen_namespace_unary;
use crate::types::ExprContext;
use crate::utils::ptr_to_str;

gen_namespace_unary!(pl_expr_array_max, arr, max);
gen_namespace_unary!(pl_expr_array_min, arr, min);
gen_namespace_unary!(pl_expr_array_sum, arr, sum);
gen_namespace_unary!(pl_expr_array_mean, arr, mean);
gen_namespace_unary!(pl_expr_array_median, arr, median);
gen_namespace_unary!(pl_expr_array_arg_max, arr, arg_max);
gen_namespace_unary!(pl_expr_array_arg_min, arr, arg_min);
gen_namespace_unary!(pl_expr_array_any, arr, any);
gen_namespace_unary!(pl_expr_array_all, arr, all);
gen_namespace_unary!(pl_expr_array_len, arr, len);
gen_namespace_unary!(pl_expr_array_n_unique, arr, n_unique);
gen_namespace_unary!(pl_expr_array_to_list, arr, to_list);
gen_namespace_unary!(pl_expr_array_reverse, arr, reverse);

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_unique(expr_ptr: *mut ExprContext, stable: bool) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = if stable {
            ctx.inner.arr().unique_stable()
        } else {
            ctx.inner.arr().unique()
        };
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_count_matches(expr_ptr: *mut ExprContext, item_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let item = unsafe { Box::from_raw(item_ptr) };
        let new_expr = ctx.inner.arr().count_matches(item.inner);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_agg(expr_ptr: *mut ExprContext, agg_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let agg = unsafe { Box::from_raw(agg_ptr) };
        let new_expr = ctx.inner.arr().agg(agg.inner);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_shift(expr_ptr: *mut ExprContext, shift_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let shift = unsafe { Box::from_raw(shift_ptr) };
        let new_expr = ctx.inner.arr().shift(shift.inner);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_head(expr_ptr: *mut ExprContext, head_ptr: *mut ExprContext, as_list :bool) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let head = unsafe { Box::from_raw(head_ptr) };
        let new_expr = ctx.inner.arr().head(head.inner,as_list)?;
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_tail(expr_ptr: *mut ExprContext, tail_ptr: *mut ExprContext, as_array :bool) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let tail = unsafe { Box::from_raw(tail_ptr) };
        let new_expr = ctx.inner.arr().tail(tail.inner,as_array)?;
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_eval(expr_ptr: *mut ExprContext,other_ptr: *mut ExprContext, as_list: bool) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let other = unsafe { Box::from_raw(other_ptr) };
        let new_expr = ctx.inner.arr().eval(other.inner, as_list);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_slice(expr_ptr: *mut ExprContext,offset_ptr: *mut ExprContext,length_ptr: *mut ExprContext, as_list: bool) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let offset = unsafe { Box::from_raw(offset_ptr) };
        let length = unsafe {Box::from_raw(length_ptr)};
        let new_expr = ctx.inner.arr().slice(offset.inner,length.inner, as_list)?;
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Join elements with a separator (requires Array<String>)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_join(
    expr_ptr: *mut ExprContext, 
    separator_ptr: *const c_char,
    ignore_nulls: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let sep = ptr_to_str(separator_ptr).unwrap();
        
        // array().join(separator, ignore_nulls)
        let new_expr = ctx.inner.arr().join(lit(sep), ignore_nulls);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Check if array contains value
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_contains(
    expr_ptr: *mut ExprContext,
    item_ptr: *mut ExprContext,
    nulls_equal: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let item = unsafe { Box::from_raw(item_ptr) };
        
        // array().contains(item)
        let new_expr = ctx.inner.arr().contains(item.inner,nulls_equal);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Array Statistics

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_std(expr_ptr: *mut ExprContext, ddof: u8) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().std(ddof);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_var(expr_ptr: *mut ExprContext, ddof: u8) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().var(ddof);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}


// Sort
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_sort(
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
            limit: None,
        };
        let new_expr = ctx.inner.arr().sort(options);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Transform
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_get(
    expr_ptr: *mut ExprContext, 
    index_ptr: *mut ExprContext,
    null_on_oob: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let idx = unsafe { Box::from_raw(index_ptr) };
        // arr().get(index, null_on_oob)
        let new_expr = ctx.inner.arr().get(idx.inner, null_on_oob);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_explode(
    expr_ptr: *mut ExprContext,
    empty_as_null: bool,
    keep_nulls: bool
) -> *mut ExprContext {
    ffi_try!({
        let options = ExplodeOptions {
            empty_as_null,
            keep_nulls,
        };
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().explode(options);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_to_struct(
    expr_ptr: *mut ExprContext,
    names_ptr: *const *const c_char,
    names_len: usize,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let name_generator = if names_ptr.is_null() || names_len == 0 {
            None
        } else {
            let mut names = Vec::with_capacity(names_len);
            unsafe {
                let slice = std::slice::from_raw_parts(names_ptr, names_len);
                for &ptr in slice {
                    let c_str = std::ffi::CStr::from_ptr(ptr);
                    names.push(c_str.to_string_lossy().into_owned());
                }
            }
            
        let cb = PlanCallback::new(move |i: usize| {
                if i < names.len() {
                    Ok(names[i].clone())
                } else {
                    Ok(format!("field_{i}"))
                }
            });
            
            Some(cb)
        };

        let new_expr = ctx.inner.arr().to_struct(name_generator);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}