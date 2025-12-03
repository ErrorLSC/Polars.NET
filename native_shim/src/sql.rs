use polars::prelude::*;
use polars::sql::SQLContext;
use std::os::raw::c_char;
use crate::types::{LazyFrameContext, ptr_to_str};

// 定义 Context 容器
pub struct SqlContextWrapper {
    pub inner: SQLContext,
}

// 1. 创建 Context
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_new() -> *mut SqlContextWrapper {
    ffi_try!({
        let ctx = SQLContext::new();
        Ok(Box::into_raw(Box::new(SqlContextWrapper { inner: ctx })))
    })
}

// 2. 释放 Context
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_free(ptr: *mut SqlContextWrapper) {
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}

// 3. 注册表 (Register LazyFrame)
// name: 表名
// lf: LazyFrame (注意：注册会消耗 LazyFrame 的所有权吗？SQLContext::register 接受 LazyFrame，所以是 Move)
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_register(
    ctx_ptr: *mut SqlContextWrapper,
    name_ptr: *const c_char,
    lf_ptr: *mut LazyFrameContext
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *ctx_ptr };
        let name = ptr_to_str(name_ptr).unwrap();
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) }; // 消费 LF

        ctx.inner.register(name, lf_ctx.inner);
        Ok(())
    })
}

// 4. 执行 SQL (Execute) -> 返回 LazyFrame
#[unsafe(no_mangle)]
pub extern "C" fn pl_sql_context_execute(
    ctx_ptr: *mut SqlContextWrapper,
    query_ptr: *const c_char
) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe { &mut *ctx_ptr };
        let query = ptr_to_str(query_ptr).unwrap();

        // execute 返回 PolarsResult<LazyFrame>
        let lf = ctx.inner.execute(query)?;
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}