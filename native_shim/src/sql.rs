use polars::prelude::*;
use polars::sql::SQLContext;
use std::{ffi::CString, os::raw::c_char};
use crate::{types::LazyFrameContext, utils::ptr_to_str};

// Define Context Container
pub struct SqlContextWrapper {
    pub inner: SQLContext,
}

// Create Context
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_new() -> *mut SqlContextWrapper {
    ffi_try!({
        let ctx = SQLContext::new();
        Ok(Box::into_raw(Box::new(SqlContextWrapper { inner: ctx })))
    })
}

// Release Context
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_free(ptr: *mut SqlContextWrapper) {
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}

// Register LazyFrame
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_register(
    ctx_ptr: *mut SqlContextWrapper,
    name_ptr: *const c_char,
    lf_ptr: *mut LazyFrameContext
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *ctx_ptr };
        let name = ptr_to_str(name_ptr).unwrap();
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        ctx.inner.register(name, lf_ctx.inner);
        Ok(())
    })
}

// Unregister LazyFrame
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_unregister(
    ctx_ptr: *mut SqlContextWrapper,
    name_ptr: *const c_char,
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *ctx_ptr };
        let name = ptr_to_str(name_ptr).unwrap();

        ctx.inner.unregister(name);
        Ok(())
    })
}

// Execute) -> Return LazyFrame
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_execute(
    ctx_ptr: *mut SqlContextWrapper,
    query_ptr: *const c_char
) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe { &mut *ctx_ptr };
        let query = ptr_to_str(query_ptr).unwrap();

        let lf = ctx.inner.execute(query)?;
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

// Get all registered tables in the SQLContext
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_get_tables(
    ctx_ptr: *mut SqlContextWrapper,
    out_len: *mut usize,
) -> *mut *mut c_char {
    ffi_try!({
        let ctx = unsafe { &*ctx_ptr };
        let tables = ctx.inner.get_tables();
        
        let mut c_strings: Vec<*mut c_char> = tables
            .into_iter()
            .filter_map(|s| CString::new(s).ok()) // Safely convert to CString
            .map(|c| c.into_raw())                // Relinquish memory ownership to raw pointer
            .collect();
            
        c_strings.shrink_to_fit();
        let ptr = c_strings.as_mut_ptr();
        
        // Write the length of the array to the out parameter
        unsafe {
            *out_len = c_strings.len();
        }
        
        // Prevent Rust from dropping the Vec so C# can safely read it
        std::mem::forget(c_strings);
        
        Ok(ptr)
    })
}

// Free the string array allocated by pl_sql_context_get_tables
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_free_tables_array(
    ptr: *mut *mut c_char,
    len: usize,
) {
    if !ptr.is_null() {
        unsafe {
            // Reconstruct the Vec from raw parts to correctly drop it
            let c_strings = Vec::from_raw_parts(ptr, len, len);
            for c_str in c_strings {
                if !c_str.is_null() {
                    // Reconstruct CString to correctly free the memory of each string
                    let _ = CString::from_raw(c_str);
                }
            }
        }
    }
}