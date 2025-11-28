use polars::prelude::*;
use std::{ffi::CString, os::raw::c_char};
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
// DataFrame Ops
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
pub extern "C" fn pl_dataframe_get_column_name(
    df_ptr: *mut DataFrameContext, 
    index: usize
) -> *mut c_char {
    let ctx = unsafe { &*df_ptr };
    let cols = ctx.df.get_column_names();
    
    if index >= cols.len() {
        return std::ptr::null_mut();
    }

    // 分配新内存返回给 C#，C# 必须负责释放
    CString::new(cols[index].as_str()).unwrap().into_raw()
}

// --- 标量获取 (Scalar Access) ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_i64(
    df_ptr: *mut DataFrameContext, 
    col_name_ptr: *const c_char, 
    row_index: usize,
    out_val: *mut i64 // <--- [修改] 这是一个输出参数
) -> bool { // <--- [修改] 返回值变为 bool: true=成功拿到值, false=失败/空/类型不对
    let ctx = unsafe { &*df_ptr };
    let col_name = ptr_to_str(col_name_ptr).unwrap_or("");
    
    // 如果列不存在，直接返回 false
    let col = match ctx.df.column(col_name) {
        Ok(c) => c,
        Err(_) => return false,
    };

    // 获取单元格值
    match col.get(row_index) {
        Ok(val) => match val {
            // 严格匹配整数类型
            AnyValue::Int64(v) => { unsafe { *out_val = v }; true },
            AnyValue::Int32(v) => { unsafe { *out_val = v as i64 }; true },
            AnyValue::Int16(v) => { unsafe { *out_val = v as i64 }; true },
            AnyValue::Int8(v) =>  { unsafe { *out_val = v as i64 }; true },
            AnyValue::UInt64(v) => { 
                // i64 装不下 u64 的最大值，这里看你业务需求
                // 严谨做法是再搞个 get_u64，或者这里由用户承担溢出风险
                unsafe { *out_val = v as i64 }; true 
            },
            // 如果是 Null 或者其他类型，都视为“无法获取 i64”
            _ => false, 
        },
        Err(_) => false // 索引越界
    }
}

// 同理，f64 也要改，防止 NaN 混淆
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_f64(
    df_ptr: *mut DataFrameContext, 
    col_name_ptr: *const c_char, 
    row_index: usize,
    out_val: *mut f64
) -> bool {
    let ctx = unsafe { &*df_ptr };
    let col_name = ptr_to_str(col_name_ptr).unwrap_or("");
    
    let col = match ctx.df.column(col_name) {
        Ok(c) => c,
        Err(_) => return false,
    };

    match col.get(row_index) {
        Ok(val) => match val {
            AnyValue::Float64(v) => { unsafe { *out_val = v }; true },
            AnyValue::Float32(v) => { unsafe { *out_val = v as f64 }; true },
            // 整数也可以转浮点
            AnyValue::Int64(v) => { unsafe { *out_val = v as f64 }; true },
            AnyValue::Int32(v) => { unsafe { *out_val = v as f64 }; true },
            _ => false, 
        },
        Err(_) => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_string(
    df_ptr: *mut DataFrameContext, 
    col_name_ptr: *const c_char, 
    row_index: usize
) -> *mut c_char {
    let ctx = unsafe { &*df_ptr };
    let col_name = ptr_to_str(col_name_ptr).unwrap_or("");
    
    match ctx.df.column(col_name) {
        Ok(col) => match col.get(row_index) {
            Ok(AnyValue::String(s)) => CString::new(s).unwrap().into_raw(),
            // 0.50+ StringView
            Ok(AnyValue::StringOwned(s)) => CString::new(s.as_str()).unwrap().into_raw(),
            _ => std::ptr::null_mut()
        },
        Err(_) => std::ptr::null_mut()
    }
}


// ==========================================
// Head (取头)
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_head(df_ptr: *mut DataFrameContext, n: usize) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        // head 只是切片，开销极小
        let res_df = ctx.df.head(Some(n));
        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}