use std::ffi::c_char;
use polars::prelude::*;

use crate::gen_namespace_unary;
use crate::types::{DataTypeExprContext, ExprContext};
use crate::utils::ptr_to_str;

/// The categories of a Categorical or Enum column, as Polars 0.55's `cat.get_categories()` gave them (every category
/// of its mapping); Polars 2.x dropped it.
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_cat_get_categories(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let function = |c: Column| {
            let mapping = c.dtype().cat_mapping()?;
            let categories = unsafe { StringChunked::from_chunks(c.name().clone(), vec![mapping.to_arrow(true)]) };
            Ok(categories.into_column())
        };
        let output_map = |_schema: &Schema, field: &Field| Ok(Field::new(field.name().clone(), DataType::String));
        // The whole column at once (its length changes), not batch by batch.
        Ok(Box::into_raw(Box::new(ExprContext { inner: ctx.inner.apply_with_fmt_str(function, output_map, "cat.get_categories") })))
    })
}
gen_namespace_unary!(pl_expr_cat_len_bytes,cat,len_bytes);
gen_namespace_unary!(pl_expr_cat_len_chars,cat,len_chars);
gen_namespace_unary!(pl_expr_cat_physical,cat,physical);

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_cat_starts_with(
    expr_ptr: *mut ExprContext,
    prefix_ptr: *const c_char,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let prefix = ptr_to_str(prefix_ptr).unwrap();
        // list().join(sep, ignore_nulls=true)
        let new_expr = ctx.inner.cat().starts_with(prefix.to_string());
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_cat_ends_with(
    expr_ptr: *mut ExprContext,
    suffix_ptr: *const c_char,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let suffix = ptr_to_str(suffix_ptr).unwrap();
        // list().join(sep, ignore_nulls=true)
        let new_expr = ctx.inner.cat().ends_with(suffix.to_string());
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_cat_slice(
    expr_ptr: *mut ExprContext,
    offset: i64,
    has_length: bool,
    length: usize,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let opt_length = if has_length {
            Some(length)
        } else {
            None
        };

        let new_expr = ctx.inner.cat().slice(offset, opt_length);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_cat_to(
    expr_ptr: *mut ExprContext,
    datatype_expr: *mut DataTypeExprContext,
    strict:bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let dtype = unsafe {Box::from_raw(datatype_expr)};

        let new_expr = ctx.inner.cat().to(dtype.inner,strict);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

