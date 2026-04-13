use polars::prelude::*;
use std::ffi::c_void;
use std::os::raw::c_char;
use crate::gen_namespace_unary;
use crate::types::ExprContext;
use crate::utils::{ptr_to_str};

gen_namespace_unary!(pl_expr_name_keep, name, keep);
gen_namespace_unary!(pl_expr_name_to_lowercase, name, to_lowercase);
gen_namespace_unary!(pl_expr_name_to_uppercase, name, to_uppercase);

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_prefix(
    expr_ptr: *mut ExprContext, 
    prefix_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let prefix = ptr_to_str(prefix_ptr).unwrap();
        let new_expr = ctx.inner.name().prefix(prefix);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_suffix(
    expr_ptr: *mut ExprContext, 
    suffix_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let suffix = ptr_to_str(suffix_ptr).unwrap();
        let new_expr = ctx.inner.name().suffix(suffix);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_name_replace(
    expr_ptr: *mut ExprContext, 
    pattern_ptr: *const c_char,
    value_ptr:*const c_char,
    literal: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let pattern = ptr_to_str(pattern_ptr).unwrap();
        let value = ptr_to_str(value_ptr).unwrap();
        let new_expr = ctx.inner.name().replace(pattern,value,literal);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_name_prefix_fields(
    expr_ptr: *mut ExprContext, 
    prefix_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let prefix = ptr_to_str(prefix_ptr).unwrap();
        let new_expr = ctx.inner.name().prefix_fields(prefix);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_name_suffix_fields(
    expr_ptr: *mut ExprContext, 
    suffix_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let suffix = ptr_to_str(suffix_ptr).unwrap();
        let new_expr = ctx.inner.name().suffix_fields(suffix);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

pub type MapStringCallback = extern "C" fn(*const c_char) -> *mut c_char;
pub type FreeStringCallback = extern "C" fn(*mut c_char);
pub type FreeHandleCallback = extern "C" fn(*mut c_void);

struct GcHandleGuard {
    handle_ptr: *mut c_void,
    free_cb: FreeHandleCallback,
}
unsafe impl Send for GcHandleGuard {}
unsafe impl Sync for GcHandleGuard {}
impl Drop for GcHandleGuard {
    fn drop(&mut self) {
        if !self.handle_ptr.is_null() {
            (self.free_cb)(self.handle_ptr);
        }
    }
}

fn build_string_map_callback(
    callback: MapStringCallback,
    free_string_cb: FreeStringCallback,
    gc_handle_ptr: *mut c_void,
    free_handle_cb: FreeHandleCallback,
) -> PlanCallback<PlSmallStr, PlSmallStr> {
    let handle_guard = GcHandleGuard {
        handle_ptr: gc_handle_ptr,
        free_cb: free_handle_cb,
    };

    PlanCallback::Rust(SpecialEq::new(Arc::new(
        move |name: PlSmallStr| -> PolarsResult<PlSmallStr> {
            let _keep_alive = &handle_guard; 
            let c_name = std::ffi::CString::new(name.as_str())
                .map_err(|_| PolarsError::ComputeError("Invalid UTF-8".into()))?;

            let result_ptr = callback(c_name.as_ptr());
            if result_ptr.is_null() {
                return Err(PolarsError::ComputeError("C# callback returned null".into()));
            }

            let result_str = {
                let result_cstr = unsafe { std::ffi::CStr::from_ptr(result_ptr) };
                result_cstr.to_string_lossy().into_owned()
            };

            free_string_cb(result_ptr); 
            Ok(PlSmallStr::from_string(result_str))
        }
    )))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_name_map(
    expr_ptr: *mut ExprContext,
    callback: MapStringCallback,
    free_string_cb: FreeStringCallback,
    gc_handle_ptr: *mut c_void,
    free_handle_cb: FreeHandleCallback,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let cb = build_string_map_callback(callback, free_string_cb, gc_handle_ptr, free_handle_cb);
        
        let new_expr = ctx.inner.name().map(cb);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_name_map_fields(
    expr_ptr: *mut ExprContext,
    callback: MapStringCallback,
    free_string_cb: FreeStringCallback,
    gc_handle_ptr: *mut c_void,
    free_handle_cb: FreeHandleCallback,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let cb = build_string_map_callback(callback, free_string_cb, gc_handle_ptr, free_handle_cb);
        
        let new_expr = ctx.inner.name().map_fields(cb);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}