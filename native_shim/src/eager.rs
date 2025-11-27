use polars::prelude::*;
use std::os::raw::c_char;
use crate::types::*;

// ==========================================
// 1. 内部辅助函数 (消灭重复的 unsafe 循环)
// ==========================================

/// 将 C 传递过来的 Expr 指针数组转换为 Rust 的 Vec<Expr>
/// 注意：这会消耗掉 C 端传递过来的 Expr 所有权 (Box::from_raw)
unsafe fn consume_exprs_array(ptr: *const *mut ExprContext, len: usize) -> Vec<Expr> {
    let slice = unsafe {std::slice::from_raw_parts(ptr, len)};
    slice.iter()
        .map(|&p| unsafe { Box::from_raw(p).inner})
        .collect()
}
// ==========================================
// 2. 宏定义
// ==========================================

/// 模式 A: DataFrame -> 单个 Expr -> DataFrame
/// 适用: filter
macro_rules! gen_eager_op_single {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            df_ptr: *mut DataFrameContext, 
            expr_ptr: *mut ExprContext
        ) -> *mut DataFrameContext {
            ffi_try!({
                let ctx = unsafe { &mut *df_ptr };
                // 拿回 Expr 所有权
                let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
                
                // 执行操作: clone -> lazy -> op -> collect
                let res_df = ctx.df.clone().lazy()
                    .$method(expr_ctx.inner)
                    .collect()?;

                Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
            })
        }
    };
}

/// 模式 B: DataFrame -> Expr 数组 -> DataFrame
/// 适用: select, with_columns (如果有)
macro_rules! gen_eager_op_vec {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            df_ptr: *mut DataFrameContext, 
            exprs_ptr: *const *mut ExprContext, 
            len: usize
        ) -> *mut DataFrameContext {
            ffi_try!({
                let ctx = unsafe { &mut *df_ptr };
                // 使用辅助函数转换数组
                let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };
                
                let res_df = ctx.df.clone().lazy()
                    .$method(exprs)
                    .collect()?;

                Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
            })
        }
    };
}

// ==========================================
// 3. 宏应用 (标准 API)
// ==========================================

// 生成 pl_filter
gen_eager_op_single!(pl_filter, filter);

// 生成 pl_select
gen_eager_op_vec!(pl_select, select);

// pl_with_columns
gen_eager_op_vec!(pl_with_columns, with_columns);

// ==========================================
// GroupBy 核心逻辑
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_groupby_agg(
    df_ptr: *mut DataFrameContext,
    by_ptr: *const *mut ExprContext, by_len: usize,
    agg_ptr: *const *mut ExprContext, agg_len: usize
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &mut *df_ptr };
        
        // 利用辅助函数极大地简化代码
        let by_exprs = unsafe { consume_exprs_array(by_ptr, by_len) };
        let agg_exprs = unsafe { consume_exprs_array(agg_ptr, agg_len) };

        // 链式调用
        let res_df = ctx.df.clone().lazy()
            .group_by(by_exprs)
            .agg(agg_exprs)
            .collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}

// ==========================================
// Join (连接)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_join(
    left_ptr: *mut DataFrameContext,
    right_ptr: *mut DataFrameContext,
    left_on_ptr: *const *mut ExprContext, left_on_len: usize,
    right_on_ptr: *const *mut ExprContext, right_on_len: usize,
    how_ptr: *const c_char
) -> *mut DataFrameContext {
    ffi_try!({
        let left_ctx = unsafe { &*left_ptr };
        let right_ctx = unsafe { &*right_ptr };
        
        // 1. 修复问号报错：手动映射错误
        let how_str = ptr_to_str(how_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // 2. 匹配 JoinType (注意 Outer -> Full)
        let how = match how_str {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "outer" | "full" => JoinType::Full, // 0.50+ 使用 Full
            "cross" => JoinType::Cross,         // 需要 feature "cross_join"
            "semi" => JoinType::Semi,           // 需要 feature "semi_anti_join"
            "anti" => JoinType::Anti,           // 需要 feature "semi_anti_join"
            _ => return Err(PolarsError::ComputeError(format!("Unknown join type: {}", how_str).into())),
        };

        let left_on = unsafe { consume_exprs_array(left_on_ptr, left_on_len) };
        let right_on = unsafe { consume_exprs_array(right_on_ptr, right_on_len) };

        // 0.50 写法
        let args = JoinArgs::new(how);
        
        let res_df = left_ctx.df.clone().lazy()
            .join(right_ctx.df.clone().lazy(), left_on, right_on, args)
            .collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}

// ==========================================
// Head (取头)
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_height(df_ptr: *mut DataFrameContext) -> usize {
    let ctx = unsafe { &*df_ptr };
    ctx.df.height()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_width(ptr: *mut DataFrameContext) -> usize {
    if ptr.is_null() { return 0; }
    let ctx = unsafe { &*ptr };
    ctx.df.width()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_head(df_ptr: *mut DataFrameContext, n: usize) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        // head 只是切片，开销极小
        let res_df = ctx.df.head(Some(n));
        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}