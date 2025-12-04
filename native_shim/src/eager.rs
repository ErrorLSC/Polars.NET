use polars::prelude::*;
use std::{ffi::CString, os::raw::c_char};
use crate::types::*;
use polars::lazy::frame::pivot::pivot as pivot_impl; 
use polars::lazy::dsl::UnpivotArgsDSL;
// ==========================================
// 0. Memory Safety
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_free(ptr: *mut DataFrameContext) {
    ffi_try_void!({
        if !ptr.is_null() {
        // 拿回所有权，离开作用域时自动 Drop (释放内存)
        unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
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
    how_code: i32
) -> *mut DataFrameContext {
    ffi_try!({
        let left_ctx = unsafe { &*left_ptr };
        let right_ctx = unsafe { &*right_ptr };

        // 匹配 JoinType
        let how = map_jointype(how_code);

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
// Sort
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_sort(
    df_ptr: *mut DataFrameContext,
    expr_ptr: *mut ExprContext,
    descending: bool
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
        
        // 0.50+ Eager Sort 支持表达式
        let res_df = ctx.df.clone()
            .lazy()
            // LazyFrame::sort 接受 Expr 列表
            .sort_by_exprs(
                vec![expr_ctx.inner], 
                SortMultipleOptions::default().with_order_descending(descending)
            )
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

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_clone(ptr: *mut DataFrameContext) -> *mut DataFrameContext {
    ffi_try!({
        // 1. 借用 (&*ptr) 而不是消费 (Box::from_raw)
        let ctx = unsafe { &*ptr };
        
        // 2. Clone (Deep copy of the logical plan/structure, data is COW)
        let new_df = ctx.df.clone();
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
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
                // i64::MAX 是 9,223,372,036,854,775,807
                if v > (i64::MAX as u64) {
                    // 溢出！数值太大，无法用 i64 表示
                    // 返回 false，这在 C#/F# 端会变成 null/None
                    false 
                } else {
                    // 安全，可以转换
                    unsafe { *out_val = v as i64 }; 
                    true 
                }
            },// 潜在溢出风险
            AnyValue::UInt32(v) => { unsafe { *out_val = v as i64 }; true }, // <--- 关键修复
            AnyValue::UInt16(v) => { unsafe { *out_val = v as i64 }; true },
            AnyValue::UInt8(v) =>  { unsafe { *out_val = v as i64 }; true },
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
            // 显式处理 Null，返回空指针
            Ok(AnyValue::Null) => std::ptr::null_mut(),
            // 1. 本身就是字符串，直接返回
            Ok(AnyValue::String(s)) => CString::new(s).unwrap().into_raw(),
            Ok(AnyValue::StringOwned(s)) => CString::new(s.as_str()).unwrap().into_raw(),
            
            // 2. [关键修复] 其他类型 (如 Date, Int, Float)，调用 to_string()
            // Polars 的 AnyValue 实现了 Display，会自动格式化 Date 为 "2023-12-25" 格式
            Ok(v) => CString::new(v.to_string()).unwrap().into_raw(),
            
            // 3. 只有真正的获取失败才返回 null
            Err(_) => std::ptr::null_mut()
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
// ==========================================
// Explode
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_explode(
    df_ptr: *mut DataFrameContext,
    exprs_ptr: *const *mut ExprContext,
    len: usize
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };

        if exprs.is_empty() {
             let res_df = ctx.df.clone();
             return Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })));
        }

        let mut iter = exprs.into_iter();
        
        // 1. 处理第一个
        let first_expr = iter.next().unwrap();
        // [修复] 安全解包
        let mut final_selector = first_expr.into_selector()
            .ok_or_else(|| PolarsError::ComputeError("Expr cannot be converted to Selector".into()))?;

        // 2. 处理剩下的
        for e in iter {
            let s = e.into_selector()
                .ok_or_else(|| PolarsError::ComputeError("Expr cannot be converted to Selector".into()))?;
                
            final_selector = final_selector | s;
        }

        // 转 Lazy -> explode -> collect
        let res_df = ctx.df.clone()
            .lazy()
            .explode(final_selector)
            .collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}
// ==========================================
// Pivot & Unpivot
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_pivot(
    df_ptr: *mut DataFrameContext,
    values_ptr: *const *const c_char, values_len: usize,
    index_ptr: *const *const c_char, index_len: usize,
    columns_ptr: *const *const c_char, columns_len: usize,
    agg_code: i32
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        // 1. 转换字符串数组 (逻辑不变)
        let to_strs = |ptr, len| unsafe {
            let mut v = Vec::with_capacity(len);
            for &p in std::slice::from_raw_parts(ptr, len) {
                v.push(ptr_to_str(p).unwrap()); // 返回 &str
            }
            v
        };

        let values = to_strs(values_ptr, values_len);
        let index = to_strs(index_ptr, index_len);
        let columns = to_strs(columns_ptr, columns_len);
        
        // 我们构建一个针对 "element" 的表达式
        let el = col(""); 
        let agg_expr = match agg_code {
            1 => el.sum(),    // Sum
            2 => el.min(),    // Min
            3 => el.max(),    // Max
            4 => el.mean(),   // Mean
            5 => el.median(), // Median
            6 => len(),       // Count
            7 => len(),       // Len
            8 => el.last(),   // Last
            0 | _ => el.first(), // First (Default)
        };

        // 3. 调用 polars::lazy::frame::pivot::pivot
        // 这个函数就是你贴出的那个源代码，它接受 &DataFrame 和 Expr
        let res_df = pivot_impl(
            &ctx.df,
            columns,          // I0
            Some(index),  // Option<I1>
            Some(values),   // Option<I2>
            false,          // sort_columns
            Some(agg_expr), // Option<Expr>
            None            // separator
        )?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_unpivot(
    df_ptr: *mut DataFrameContext,
    id_vars_ptr: *const *const c_char, id_len: usize,
    val_vars_ptr: *const *const c_char, val_len: usize,
    variable_name_ptr: *const c_char,
    value_name_ptr: *const c_char
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        // 1. 辅助：C字符串数组 -> Vec<PlSmallStr>
        let to_pl_strs = |ptr, len| unsafe {
            let mut v = Vec::with_capacity(len);
            for &p in std::slice::from_raw_parts(ptr, len) {
                let s = ptr_to_str(p).unwrap();
                v.push(PlSmallStr::from_str(s));
            }
            v
        };

        let index_names = to_pl_strs(id_vars_ptr, id_len);
        let on_names = to_pl_strs(val_vars_ptr, val_len);

        // 2. 构造 Selector (复用 Lazy 的逻辑)
        let index_selector = cols(index_names.clone());

        // 默认行为：如果 value_vars 为空，选所有非 index 的列
        let on_selector = if on_names.is_empty() {
            all().exclude_cols(index_names)
        } else {
            cols(on_names)
        };

        // 3. 处理重命名
        let variable_name = if variable_name_ptr.is_null() { None } else { Some(PlSmallStr::from_str(ptr_to_str(variable_name_ptr).unwrap())) };
        let value_name = if value_name_ptr.is_null() { None } else { Some(PlSmallStr::from_str(ptr_to_str(value_name_ptr).unwrap())) };

        // 4. 构建参数
        let args = UnpivotArgsDSL {
            index: index_selector,
            on: on_selector,
            variable_name,
            value_name,
        };

        // 5. 执行: Eager -> Lazy -> Unpivot -> Collect
        let res_df = ctx.df.clone()
            .lazy()
            .unpivot(args)
            .collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}
// ==========================================
// Concat
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_concat_vertical(
    dfs_ptr: *const *mut DataFrameContext,
    len: usize
) -> *mut DataFrameContext {
    ffi_try!({
        let slice = unsafe { std::slice::from_raw_parts(dfs_ptr, len) };
        if len == 0 {
            return Ok(Box::into_raw(Box::new(DataFrameContext { df: DataFrame::default() })));
        }

        // 取出第一个作为 base
        let base_ctx = unsafe { Box::from_raw(slice[0]) };
        let mut base_df = base_ctx.df;

        // 依次 vstack 剩下的
        for &p in &slice[1..] {
            let other_ctx = unsafe { Box::from_raw(p) };
            // vstack 默认是做了垂直拼接
            base_df.vstack_mut(&other_ctx.df)?;
        }

        Ok(Box::into_raw(Box::new(DataFrameContext { df: base_df })))
    })
}