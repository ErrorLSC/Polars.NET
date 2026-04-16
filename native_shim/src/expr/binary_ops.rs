use polars::prelude::*;
use crate::{gen_namespace_unary, impl_expr_namespace_expr_arg};
use crate::types::{DataTypeExprContext, ExprContext};

gen_namespace_unary!(pl_expr_bin_size_bytes,binary,size_bytes);

impl_expr_namespace_expr_arg!(pl_expr_bin_contains,binary,contains_literal);
impl_expr_namespace_expr_arg!(pl_expr_bin_ends_with,binary,ends_with);
impl_expr_namespace_expr_arg!(pl_expr_bin_starts_with,binary,starts_with);
impl_expr_namespace_expr_arg!(pl_expr_bin_head,binary,head);
impl_expr_namespace_expr_arg!(pl_expr_bin_tail,binary,tail);

macro_rules! impl_expr_bin_bool_arg {
    ($ffi_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $ffi_name(
            expr_ptr: *mut ExprContext,
            flag: bool
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let new_expr = ctx.inner.binary().$method(flag);
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

impl_expr_bin_bool_arg!(pl_expr_bin_hex_decode,hex_decode);
impl_expr_bin_bool_arg!(pl_expr_bin_base64_decode,base64_decode);
gen_namespace_unary!(pl_expr_bin_hex_encode,binary,hex_encode);
gen_namespace_unary!(pl_expr_bin_base64_encode,binary,base64_encode);

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_bin_reinterpret(
    expr_ptr: *mut ExprContext,
    dtype_ptr: *mut DataTypeExprContext,
    is_little_endian:bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let dtype = unsafe { Box::from_raw(dtype_ptr) };

        let new_expr = ctx.inner.binary().reinterpret(dtype.inner,is_little_endian);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_bin_get(
    expr_ptr: *mut ExprContext,
    index_ptr: *mut ExprContext,
    null_on_oob:bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let index = unsafe { Box::from_raw(index_ptr) };

        let new_expr = ctx.inner.binary().get(index.inner,null_on_oob);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_bin_slice(
    expr_ptr: *mut ExprContext,
    offset_ptr: *mut ExprContext,
    length_ptr: *mut ExprContext,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let offset = unsafe { Box::from_raw(offset_ptr) };
        let length = unsafe { Box::from_raw(length_ptr) };
        let new_expr = ctx.inner.binary().slice(offset.inner,length.inner);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}