use polars::prelude::*;
use polars_core::utils::concat_df;
use polars_arrow::ffi::{ArrowArrayStreamReader,ArrowArrayStream};
use polars_arrow::array::Array;
use std::ffi::CStr;
use std::{ffi::CString, os::raw::c_char};
use crate::types::*;
use polars::lazy::frame::pivot::pivot as pivot_impl; 
use polars::lazy::dsl::UnpivotArgsDSL;
use polars::functions::{concat_df_horizontal,concat_df_diagonal};
use crate::series::SeriesContext;
use polars::prelude::{Field as PolarsField};
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
pub extern "C" fn pl_dataframe_schema(df_ptr: *mut DataFrameContext) -> *mut c_char {
    let ctx = unsafe { &*df_ptr };
    // 获取 Schema
    let schema = ctx.df.schema();
      
    // 简单粗暴方案：构建一个 Map<Name, DtypeStr>
    let map: std::collections::HashMap<String, String> = schema.iter_names_and_dtypes()
        .map(|(name, dtype)| (name.to_string(), dtype.to_string()))
        .collect();

    let json = serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string());
    
    CString::new(json).unwrap().into_raw()
}

// --- Convenience Ops ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_drop(df_ptr: *mut DataFrameContext, name: *const c_char) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let col_name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        
        // Clone + Drop (Immutable Semantics)
        let new_df = ctx.df.drop(&col_name)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_rename(df_ptr: *mut DataFrameContext, old: *const c_char, new: *const c_char) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let old_name = unsafe { CStr::from_ptr(old).to_string_lossy() };
        let new_name = unsafe { CStr::from_ptr(new).to_string_lossy() };

        // Clone + Rename
        let mut new_df = ctx.df.clone();
        new_df.rename(&old_name, PlSmallStr::from_str(&new_name))?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_drop_nulls(df_ptr: *mut DataFrameContext, subset: *const *const c_char, len: usize) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        let new_df = if subset.is_null() || len == 0 {
            // [修复] 显式告诉编译器：这里的 None 是 Option<&[String]> 类型的 None
            // 这样泛型 S 就被推断为 String，且 String 实现了 Into<PlSmallStr>
            ctx.df.drop_nulls::<String>(None)? 
        } else {
            let slice = unsafe { std::slice::from_raw_parts(subset, len) };
            let cols: Vec<String> = slice.iter()
                .map(|&p| unsafe { CStr::from_ptr(p).to_string_lossy().to_string() })
                .collect();
            // 这里 cols 是 Vec<String>，&cols 是 &[String]，S 推断为 String，没问题
            ctx.df.drop_nulls(Some(&cols))?
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_sample_n(
    df_ptr: *mut DataFrameContext, 
    n: usize, 
    replacement: bool, 
    shuffle: bool, 
    seed: *const u64
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let s = if seed.is_null() { None } else { Some(unsafe { *seed }) };
        
        // [修复] 调用 literal 版本
        let new_df = ctx.df.sample_n_literal(n, replacement, shuffle, s)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_sample_frac(
    df_ptr: *mut DataFrameContext, 
    frac: f64, 
    replacement: bool, 
    shuffle: bool, 
    seed: *const u64
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let s = if seed.is_null() { None } else { Some(unsafe { *seed }) };
        
        // [修复] 手动计算 n，因为 sample_frac_literal 不存在或未公开
        // 逻辑参考 Polars 源码: n = height * frac
        let height = ctx.df.height();
        let n = (height as f64 * frac) as usize;
        
        // 调用 sample_n_literal
        let new_df = ctx.df.sample_n_literal(n, replacement, shuffle, s)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
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
// Head/Tail
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_head(df_ptr: *mut DataFrameContext, n: usize) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let res_df = ctx.df.head(Some(n));
        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_tail(df_ptr: *mut DataFrameContext, n: usize) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let res_df = ctx.df.tail(Some(n));
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
pub extern "C" fn pl_concat(
    dfs_ptr: *const *mut DataFrameContext,
    len: usize,
    how: i32 // 0=Vertical, 1=Horizontal, 2=Diagonal
) -> *mut DataFrameContext {
    ffi_try!({
        if len == 0 {
            return Ok(Box::into_raw(Box::new(DataFrameContext { df: DataFrame::default() })));
        }

        let slice = unsafe { std::slice::from_raw_parts(dfs_ptr, len) };

        // 1. 将所有指针解包为 DataFrame 的 Vector
        // 注意：这里我们接管了所有输入 DataFrame 的所有权
        let mut dfs: Vec<DataFrame> = Vec::with_capacity(len);
        for &p in slice {
            let ctx = unsafe { Box::from_raw(p) };
            dfs.push(ctx.df);
        }

        // 2. 根据策略调用 Polars 内置的高性能拼接函数
        // 这些函数接受 &[DataFrame] 并返回一个新的 DataFrame
        let out_df = match how {
            // Vertical (vstack)
            0 => concat_df(&dfs)?,
            
            // Horizontal (hstack)
            1 => concat_df_horizontal(&dfs,true)?,
            
            // Diagonal (对角拼接：自动对齐列，缺失补 Null)
            2 => concat_df_diagonal(&dfs)?,
            
            _ => return Err(PolarsError::ComputeError("Invalid concat strategy".into())),
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df: out_df })))
    })
}
// ==========================================
// Unnest
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_unnest(
    df: *mut DataFrame, 
    cols: *const *const c_char, 
    len: usize
) -> *mut DataFrame {
    ffi_try!({
        let df = unsafe { &*df };
        
        // 1. 安全地将 C 字符串数组转换为 Rust 迭代器
        let cols_slice = unsafe { std::slice::from_raw_parts(cols, len) };
        
        // 这里使用 map 转换，如果字符串 utf8 编码不对会 panic，
        // 但由 C# 传来的 LPUTF8Str 通常是安全的。
        let names = cols_slice
            .iter()
            .map(|&ptr| unsafe { CStr::from_ptr(ptr).to_str().unwrap() });

        // 2. 调用 Polars 的 unnest
        let result_df = df.unnest(names)?;

        // 3. 返回指针
        Ok(Box::into_raw(Box::new(result_df)))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_column(
    ptr: *mut DataFrameContext, 
    name: *const c_char
) -> *mut SeriesContext {
    ffi_try!({  
        let ctx = unsafe { &*ptr };
        // [修正] 使用 ptr_to_str 辅助函数
        let name_str = ptr_to_str(name).unwrap_or("");
        
        match ctx.df.column(name_str) {
            Ok(column) => {

                let s = column.as_materialized_series().clone();
                
                Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
            },
            Err(_) => Ok(std::ptr::null_mut())
        }
    })
}

// 2. 按索引获取 Series (顺手加的，很常用)
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_column_at(
    ptr: *mut DataFrameContext, 
    index: usize
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        
        // select_at_idx 返回 Option<&Column>
        match ctx.df.select_at_idx(index) {
            Some(column) => {
                // [修正] 同样需要从 Column 提取 Series
                let s = column.as_materialized_series().clone();
                Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
            },
            None => Ok(std::ptr::null_mut())
        }
    })
}

// 3. 将 Series 转为 DataFrame (方便单列操作后的还原)
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_to_frame(ptr: *mut SeriesContext) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let s = ctx.series.clone();
        
        // [修正] DataFrame::new 接受 Vec<Column>
        // Series 实现了 Into<Column>
        let df = DataFrame::new(vec![s.into()]).unwrap_or_default();
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_new(
    columns_ptr: *const *mut SeriesContext, // 指向 SeriesContext 指针数组的指针
    len: usize,
) -> *mut DataFrameContext {
    ffi_try!({
        // 1. 校验输入
        if columns_ptr.is_null() || len == 0 {
            // 返回空 DataFrame
            return Ok(Box::into_raw(Box::new(DataFrameContext { df: DataFrame::default() })));
        }

        // 2. 将 C 数组转换为 Rust Vec<Series>
        let slice = unsafe { std::slice::from_raw_parts(columns_ptr, len) };
        let mut series_vec = Vec::with_capacity(len);

        for &ptr in slice {
            if !ptr.is_null() {
                let ctx = unsafe { &*ptr };
                // [关键] Clone Series。
                // Series 底层是 Arc 的，所以这里只是增加引用计数。
                // 这样 C# 那边的 SeriesHandle 依然有效，不会被这里消耗掉。
                series_vec.push(ctx.series.clone().into());
            }
        }

        // 3. 创建 DataFrame
        // Polars 会检查所有 Series 长度是否一致，名字是否重复等
        let df = DataFrame::new(series_vec)?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_dataframe_new_from_stream(
    stream_ptr: *mut ArrowArrayStream,
) -> *mut DataFrameContext {
    ffi_try!({
        // 1. 校验空指针
        if stream_ptr.is_null() {
            return Err(PolarsError::ComputeError("Stream pointer is null".into()));
        }
        // 2. 指针转换 (Raw Pointer -> Mutable Reference)
        // [关键] 我们使用引用 (&mut)，而不是 Box。
        // 因为 C# 端的 CArrowArrayStream 结构体生命周期由 C# 控制 (在 P/Invoke 期间有效)。
        // 如果用 Box::from_raw，Rust 会尝试释放这段内存，导致崩溃。
        let stream = unsafe { &mut *stream_ptr};

        // 3. 创建 Reader
        // try_new 接受 impl DerefMut<Target=ArrowArrayStream>
        // &mut ArrowArrayStream 完美满足这个条件
        let mut reader = unsafe {ArrowArrayStreamReader::try_new(stream)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to create Arrow Stream Reader: {}", e).into()))?};

        // 尝试获取第一个 Chunk
        // 注意：这里我们显式调用 next()，这是 Iterator trait 的方法。
        // 如果这里报错 "no method named next"，说明确实没引入 Iterator trait。
        // 但 ArrowArrayStreamReader 绝对实现了 Iterator。
        let first_chunk_result = unsafe {reader.next()};

        // 如果流是空的，直接返回空 DataFrame
        if first_chunk_result.is_none() {
            let df = DataFrame::default();
            return Ok(Box::into_raw(Box::new(DataFrameContext { df })));
        }

        let first_chunk = first_chunk_result.unwrap()
            .map_err(|e| PolarsError::ComputeError(format!("Error reading first batch: {}", e).into()))?;

        // 3. 从第一个 Chunk 反推 Schema (关键！)
        // Arrow Stream 传输的是 StructArray
        let struct_array = first_chunk
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| PolarsError::ComputeError("First batch is not a StructArray".into()))?;

        // 获取列的字段定义 (Fields)
        // struct_array.data_type() 返回 DataType::Struct(fields)
        let fields = match struct_array.dtype() {
            ArrowDataType::Struct(f) => f,
            _ => return Err(PolarsError::ComputeError("Stream data type is not Struct".into())),
        };

        let num_cols = fields.len();
        let mut columns_chunks: Vec<Vec<Box<dyn Array>>> = vec![Vec::new(); num_cols];

        // 4. 处理第一个 Chunk
        for (col_idx, column) in struct_array.values().iter().enumerate() {
            if col_idx < num_cols {
                columns_chunks[col_idx].push(column.clone());
            }
        }

        // 5. 循环处理剩余 Chunks
        // 继续调用 next() 直到返回 None
        while let Some(chunk_result) = unsafe {reader.next()} {
            let chunk = chunk_result
                .map_err(|e| PolarsError::ComputeError(format!("Error reading batch: {}", e).into()))?;

            let struct_array = chunk
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| PolarsError::ComputeError("Subsequent batch is not a StructArray".into()))?;

            for (col_idx, column) in struct_array.values().iter().enumerate() {
                if col_idx < num_cols {
                    columns_chunks[col_idx].push(column.clone());
                }
            }
        }

        // 6. 构建 Series
        let mut series_vec = Vec::with_capacity(num_cols);

        for (i, chunks) in columns_chunks.into_iter().enumerate() {
            let arrow_field = &fields[i]; // &ArrowField
            
            // [核心修复] 通过 Field 进行转换
            // PolarsField::from(&ArrowField) 是标准 API，它会自动推导 DataType
            let p_field = PolarsField::from(arrow_field); 
            let p_dtype = p_field.dtype;
            let name = p_field.name.as_str();

            // [核心修复] 使用 Unchecked 构造函数
            // 我们已经有了正确的 Polars DataType，直接组装 Chunks
            // 这绕过了 Series::try_from 的 trait bound 问题
            let s = unsafe {Series::from_chunks_and_dtype_unchecked(name.into(), chunks, &p_dtype)};
            
            series_vec.push(s.into());
        }

        // 7. 返回 DataFrame
        let df = DataFrame::new(series_vec)?;
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_lazy(df_ptr: *mut DataFrameContext) -> *mut LazyFrameContext {
    let ctx = unsafe { &*df_ptr };
    // [关键] 我们 Clone 一份 DataFrame 再转 Lazy。
    // 这样原来的 DataFrameHandle 在 C# 端依然有效，符合 .NET 的引用语义。
    // Polars 的 DF Clone 是浅拷贝 (Arc)，开销很小。
    let inner = ctx.df.clone().lazy();
    
    Box::into_raw(Box::new(LazyFrameContext { inner }))
}