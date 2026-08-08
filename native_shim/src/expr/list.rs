use polars::prelude::*;
use polars::series::ops::NullBehavior;
use std::os::raw::c_char;
use crate::{gen_namespace_unary, impl_expr_namespace_expr_arg};
use crate::types::ExprContext;
use crate::utils::{ptr_to_str, ptr_to_vec_pl_string_with_default};

gen_namespace_unary!(pl_expr_list_sum, list, sum);
gen_namespace_unary!(pl_expr_list_min, list, min);
gen_namespace_unary!(pl_expr_list_max, list, max);
gen_namespace_unary!(pl_expr_list_arg_max, list, arg_max);
gen_namespace_unary!(pl_expr_list_arg_min, list, arg_min);
gen_namespace_unary!(pl_expr_list_mean, list, mean);
gen_namespace_unary!(pl_expr_list_median, list, median);
gen_namespace_unary!(pl_expr_list_len, list, len);
gen_namespace_unary!(pl_expr_list_drop_nulls, list, drop_nulls);

impl_expr_namespace_expr_arg!(pl_expr_list_head, list, head);
impl_expr_namespace_expr_arg!(pl_expr_list_tail, list, tail);
impl_expr_namespace_expr_arg!(pl_expr_list_count_matches, list, count_matches);
impl_expr_namespace_expr_arg!(pl_expr_list_agg, list, agg);
impl_expr_namespace_expr_arg!(pl_expr_list_shift, list, shift);

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_get(
    expr_ptr: *mut ExprContext, 
    index_ptr: *mut ExprContext,
    null_on_oob: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let index = unsafe { Box::from_raw(index_ptr) };
        let new_expr = ctx.inner.list().get(index.inner,null_on_oob);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_gather(
    expr_ptr: *mut ExprContext, 
    index_ptr: *mut ExprContext,
    null_on_oob: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let index = unsafe { Box::from_raw(index_ptr) };
        let new_expr = ctx.inner.list().gather(index.inner,null_on_oob);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_gather_every(
    expr_ptr: *mut ExprContext, 
    n_ptr: *mut ExprContext,
    offset_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let n = unsafe { Box::from_raw(n_ptr) };
        let offset = unsafe { Box::from_raw(offset_ptr) };
        let new_expr = ctx.inner.list().gather_every(n.inner,offset.inner);
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
pub extern "C" fn pl_expr_list_join(
    expr_ptr: *mut ExprContext,
    sep_ptr: *const c_char,
    ignore_nulls: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let sep = ptr_to_str(sep_ptr).unwrap();
        // list().join(sep, ignore_nulls=true)
        let new_expr = ctx.inner.list().join(lit(sep), ignore_nulls);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_slice(expr_ptr: *mut ExprContext,offset_ptr: *mut ExprContext,length_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let offset = unsafe { Box::from_raw(offset_ptr) };
        let length = unsafe {Box::from_raw(length_ptr)};
        let new_expr = ctx.inner.list().slice(offset.inner,length.inner);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_std(expr_ptr: *mut ExprContext, ddof: u8) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.list().std(ddof);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_var(expr_ptr: *mut ExprContext, ddof: u8) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.list().var(ddof);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_to_array(
    expr_ptr: *mut ExprContext, 
    width: usize
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.list().to_array(width);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_to_struct(
    expr_ptr: *mut ExprContext,
    names_ptr: *const *const c_char,
    names_len: usize,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let names = unsafe { ptr_to_vec_pl_string_with_default(names_ptr, names_len,"") };

        let new_expr = ctx.inner.list().to_struct(names.into());
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_diff(
    expr_ptr: *mut ExprContext,
    n: i64,
    null_behavior_code:u8
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };

        let behavior = match null_behavior_code {
            0 => NullBehavior::Ignore,
            1 => NullBehavior::Drop,
            _ => NullBehavior::Ignore
        };
        let new_expr = ctx.inner.list().diff(n,behavior);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_sample(
    expr_ptr: *mut ExprContext,
    n_or_frac_ptr: *mut ExprContext,
    is_fraction: bool,
    with_replacement: bool,
    shuffle: *const bool,
    has_seed: bool,
    seed: u64,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let n_or_frac = unsafe { Box::from_raw(n_or_frac_ptr) };
        
        let seed_opt = if has_seed { Some(seed) } else { None };
        let shfl = if shuffle.is_null() { None } else { Some(unsafe { *shuffle }) };

        let new_expr = if is_fraction {
            ctx.inner
                .list()
                .sample_fraction(n_or_frac.inner, with_replacement, shfl, seed_opt)
        } else {
            ctx.inner
                .list()
                .sample_n(n_or_frac.inner, with_replacement, shfl, seed_opt)
        };

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_set_operation(
    expr_ptr: *mut ExprContext,
    other_ptr: *mut ExprContext,
    op: u8, 
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let other = unsafe { Box::from_raw(other_ptr) };

        let new_expr = match op {
            0 => ctx.inner.list().union(other.inner),
            1 => ctx.inner.list().set_difference(other.inner),
            2 => ctx.inner.list().set_intersection(other.inner),
            3 => ctx.inner.list().set_symmetric_difference(other.inner),
            _ => panic!("Invalid set operation code: {}. Expected 0 to 3.", op), 
        };

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_concat_list(
    exprs_ptr: *const *mut ExprContext,
    exprs_len: usize
) -> *mut ExprContext {
    ffi_try!({
        let mut exprs = Vec::with_capacity(exprs_len);
        let ptr_slice = unsafe { std::slice::from_raw_parts(exprs_ptr, exprs_len) };
        for &ptr in ptr_slice {
            let expr_ctx = unsafe { Box::from_raw(ptr) };
            exprs.push(expr_ctx.inner);
        }

        let new_expr = concat_list(exprs)?;
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_eval(expr_ptr: *mut ExprContext,other_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let other = unsafe { Box::from_raw(other_ptr) };
        let new_expr = ctx.inner.list().eval(other.inner);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}