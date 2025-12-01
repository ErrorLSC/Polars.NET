use polars::prelude::*;
use std::os::raw::c_char;
use crate::types::{ExprContext, ptr_to_str};
// 确保 lib.rs 里有 #[macro_use] mod error; 以便使用 ffi_try!

// 定义 Selector 容器
pub struct SelectorContext {
    pub inner: Selector,
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_free(ptr: *mut SelectorContext) {
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}
// 1. cs.all()
#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_all() -> *mut SelectorContext {
    ffi_try!({
        // polars::prelude::all() 在 0.50 中如果是 Selector 上下文，通常指 selectors::all()
        // 但这里我们需要显式使用 selector 的 all
        let s = all();
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

// 2. selector.exclude(["a", "b"])
#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_exclude(
    sel_ptr: *mut SelectorContext,
    names_ptr: *const *const c_char,
    len: usize
) -> *mut SelectorContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(sel_ptr) };
        
        // 构造排序列名列表 (Vec<str>)
        // Selector::exclude 接受 impl IntoVec<PlSmallStr>
        let mut exclusions = Vec::with_capacity(len);
        let slice = unsafe { std::slice::from_raw_parts(names_ptr, len) };
        for &p in slice {
            let s = ptr_to_str(p).unwrap();
            exclusions.push(PlSmallStr::from_str(s));
        }

        let new_sel = ctx.inner.exclude_cols(exclusions);
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: new_sel })))
    })
}

// 3. selector.as_expr() -> 桥接回 Expr 体系
// 最终 select() 还是接受 Expr，所以我们需要把 Selector 变成 Expr
#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_into_expr(
    sel_ptr: *mut SelectorContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(sel_ptr) };
        // Selector 实现了 Into<Expr>
        let expr: Expr = ctx.inner.into(); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_clone(
    sel_ptr: *mut SelectorContext
) -> *mut SelectorContext {
    ffi_try!({
        // 借用而不消耗
        let ctx = unsafe { &*sel_ptr };
        let new_sel = ctx.inner.clone();
        Ok(Box::into_raw(Box::new(SelectorContext { inner: new_sel })))
    })
}