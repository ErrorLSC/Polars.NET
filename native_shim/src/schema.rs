use crate::{datatypes::DataTypeContext, types::LazyFrameContext};
use std::{ffi::{CStr, CString}, os::raw::c_char};
use polars_core::prelude::*;

pub struct SchemaContext {
    pub schema: SchemaRef, // 使用 Arc<Schema>
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_lazy_frame_get_schema(lf_ptr: *mut LazyFrameContext) -> *mut SchemaContext {
    ffi_try!({
        if lf_ptr.is_null() {
            return Ok(std::ptr::null_mut());
        }
        
        // 获取可变引用，因为 collect_schema 会修改内部缓存
        let ctx = unsafe { &mut *lf_ptr };
        
        // 调用你提供的 collect_schema 逻辑
        // 这里的 inner 是你的 LazyFrame 包装结构
        let schema_ref = ctx.inner.collect_schema().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        Ok(Box::into_raw(Box::new(SchemaContext { schema: schema_ref })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_schema_len(ptr: *mut SchemaContext) -> usize {
    if ptr.is_null() { return 0; }
    let ctx = unsafe {&*ptr};
    ctx.schema.len()
}

// C# 传入 name_out 和 dtype_out 的指针的指针，Rust 负责赋值
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_schema_get_at_index(
    ptr: *mut SchemaContext,
    index: usize,
    name_out: *mut *mut c_char,
    dtype_out: *mut *mut DataType 
) {
    let ctx = unsafe{&*ptr};
    
    // Schema 是 IndexMap，可以通过索引高效访问
    if let Some((name, dtype)) = ctx.schema.get_at_index(index) {
        // A. 复制 Name (Rust String -> C String)
        // C# 端必须负责调用 pl_free_string 释放它
        unsafe {*name_out = CString::new(name.as_str()).unwrap().into_raw()};
        
        // B. 复制 DataType (Clone -> Box)
        // 返回一个新的 DataTypeHandle，C# 端 DataType 对象接管生命周期
        unsafe {*dtype_out = Box::into_raw(Box::new(dtype.clone()))};
    } else {
        // 越界保护
        unsafe {*name_out = std::ptr::null_mut()};
        unsafe {*dtype_out = std::ptr::null_mut()};
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_schema_new(
    names: *const *const c_char,
    dtypes: *const *mut DataTypeContext,
    len: usize,
) -> *mut SchemaContext {
    let mut schema = Schema::with_capacity(len);
    unsafe {
        for i in 0..len {
            let name_ptr = *names.add(i);
            let dtype_ptr = *dtypes.add(i);

            let name = CStr::from_ptr(name_ptr).to_string_lossy().into_owned();
            let dtype = &(*dtype_ptr).dtype;

            schema.insert(name.into(), dtype.clone());
        }
    }
    Box::into_raw(Box::new(SchemaContext { schema:schema.into() }))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_schema_free(ptr: *mut SchemaContext) {
    if !ptr.is_null() {
        let _ = unsafe {Box::from_raw(ptr)};
    }
}