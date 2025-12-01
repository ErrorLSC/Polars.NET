use polars::prelude::*;
use crate::types::*;

// ==========================================
// 2. 宏定义
// ==========================================

/// 模式 A: LazyFrame -> Vec<Expr> -> LazyFrame
/// 适用: select, with_columns
macro_rules! gen_lazy_vec_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            lf_ptr: *mut LazyFrameContext,
            exprs_ptr: *const *mut ExprContext,
            len: usize
        ) -> *mut LazyFrameContext {
            ffi_try!({
                // 1. 拿回 LazyFrame 所有权 (Consume)
                // 链式调用的核心：上一步的输出是这一步的输入，旧壳子丢弃
                let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
                
                // 2. 拿回 Exprs 所有权
                let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };

                // 3. 执行转换
                let new_lf = lf_ctx.inner.$method(exprs);

                // 4. 返回新壳子
                Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
            })
        }
    };
}

/// 模式 B: LazyFrame -> 单个 Expr -> LazyFrame
/// 适用: filter
macro_rules! gen_lazy_single_expr_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            lf_ptr: *mut LazyFrameContext, 
            expr_ptr: *mut ExprContext
        ) -> *mut LazyFrameContext {
            ffi_try!({
                let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
                let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
                
                let new_lf = lf_ctx.inner.$method(expr_ctx.inner);
                
                Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
            })
        }
    };
}

/// 模式 C: LazyFrame -> 标量参数 -> LazyFrame
/// 适用: limit (u32), head (u32, 只要类型匹配)
macro_rules! gen_lazy_scalar_op {
    ($func_name:ident, $method:ident, $arg_type:ty) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            lf_ptr: *mut LazyFrameContext, 
            val: $arg_type
        ) -> *mut LazyFrameContext {
            ffi_try!({
                let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
                let new_lf = lf_ctx.inner.$method(val); 
                Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
            })
        }
    };
}

// ==========================================
// 3. 宏应用 (标准 API)
// ==========================================

// --- Select / WithColumns ---
gen_lazy_vec_op!(pl_lazy_select, select);
gen_lazy_vec_op!(pl_lazy_with_columns, with_columns);

// --- Filter ---
gen_lazy_single_expr_op!(pl_lazy_filter, filter);

// --- Limit ---
// limit 在 Polars 中通常接受 IdxSize (u32)
gen_lazy_scalar_op!(pl_lazy_limit, limit, u32);
// 也可以加个 tail
gen_lazy_scalar_op!(pl_lazy_tail, tail, u32);

// ==========================================
// 4. 特殊函数 (Sort & Collect)
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sort(
    lf_ptr: *mut LazyFrameContext,
    expr_ptr: *mut ExprContext,
    descending: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
        
        // 构建排序选项
        let options = SortMultipleOptions::default()
            .with_order_descending(descending);

        // Polars 0.50: sort_by_exprs 接受 Vec<Expr>
        let new_lf = lf_ctx.inner.sort_by_exprs(
            vec![expr_ctx.inner], 
            options
        );
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
// ==========================================
// GroupBy
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_groupby_agg(
    lf_ptr: *mut LazyFrameContext,
    keys_ptr: *const *mut ExprContext, keys_len: usize,
    aggs_ptr: *const *mut ExprContext, aggs_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let keys = unsafe { consume_exprs_array(keys_ptr, keys_len) };
        let aggs = unsafe { consume_exprs_array(aggs_ptr, aggs_len) };

        // 链式调用
        let new_lf = lf_ctx.inner.group_by(keys).agg(aggs);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_explode(
    lf_ptr: *mut LazyFrameContext,
    exprs_ptr: *const *mut ExprContext,
    len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };

        if exprs.is_empty() {
            return Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf_ctx.inner })));
        }

        let mut iter = exprs.into_iter();
        
        // 1. 处理第一个
        let first_expr = iter.next().unwrap();
        // [修复] 处理 Option: 如果转换失败，抛出错误
        let mut final_selector = first_expr.into_selector()
            .ok_or_else(|| PolarsError::ComputeError("Expr cannot be converted to Selector".into()))?;

        // 2. 处理剩下的
        for e in iter {
            let s = e.into_selector()
                .ok_or_else(|| PolarsError::ComputeError("Expr cannot be converted to Selector".into()))?;
            
            final_selector = final_selector | s; // Union
        }

        let new_lf = lf_ctx.inner.explode(final_selector);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

// ==========================================
// Collect (出口：LazyFrame -> DataFrame)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_collect(lf_ptr: *mut LazyFrameContext) -> *mut DataFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        // 去掉了 println!，保持库函数的纯洁性。
        // 如果想看日志，可以在 F# 端调用 explain 或者 check schema。
        // 这里的 ? 会捕获 PolarsError 并转给 ffi_try
        let df = lf_ctx.inner.collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

// ==========================================
// 5. 实用功能
// ==========================================

// 不执行计算，只查看计划
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_explain(lf_ptr: *mut LazyFrameContext) -> *mut std::os::raw::c_char {
    // 注意：这里不能 consume (Box::from_raw)，因为调试不应该消耗掉 LazyFrame
    // 我们只是借用一下
    let ctx = unsafe { &*lf_ptr };
    
    // explain 通常返回 PolarsResult<String>
    match ctx.inner.explain(true) {
        Ok(plan_str) => std::ffi::CString::new(plan_str).unwrap().into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

// 释放字符串 (配合 pl_lazy_explain 使用)
#[unsafe(no_mangle)]
pub extern "C" fn pl_free_string(ptr: *mut std::os::raw::c_char) {
    if !ptr.is_null() {
        unsafe { let _ = std::ffi::CString::from_raw(ptr); }
    }
}

// 克隆逻辑计划
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_clone(lf_ptr: *mut LazyFrameContext) -> *mut LazyFrameContext {
    // 注意：这里用 &*lf_ptr 借用，而不是 Box::from_raw 消费
    let ctx = unsafe { &*lf_ptr };
    
    // LazyFrame 的 clone 只是复制查询计划，非常快
    let new_lf = ctx.inner.clone();
    
    Box::into_raw(Box::new(LazyFrameContext { inner: new_lf }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_frame_free(ptr: *mut LazyFrameContext) {
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}
