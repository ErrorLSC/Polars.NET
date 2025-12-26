use polars::prelude::*;
use std::{ffi::CStr, os::raw::c_char};
use crate::types::{ExprContext, consume_exprs_array, ptr_to_str};
use std::ops::{Add, Sub, Mul, Div, Rem};
use crate::datatypes::DataTypeContext;

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_free(ptr: *mut ExprContext) {
    // 使用 ffi_try_void! 确保异常安全
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}
// ==========================================
// 1. 宏定义区域
// ==========================================

/// 模式 1: 基础类型字面量构造 (Literal Constructor)
/// 生成: fn pl_expr_lit_i32(val: i32) -> *mut ExprContext
macro_rules! gen_lit_ctor {
    ($func_name:ident, $input_type:ty) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(val: $input_type) -> *mut ExprContext {
            ffi_try!({
                let expr = lit(val);
                Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
            })
        }
    };
}

/// 模式 2: 字符串构造 (String Constructor)
/// 生成: fn pl_expr_col(ptr: *const c_char) -> *mut ExprContext
macro_rules! gen_str_ctor {
    ($func_name:ident, $polars_func:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(ptr: *const c_char) -> *mut ExprContext {
            ffi_try!({
                let s = ptr_to_str(ptr).unwrap();
                let expr = $polars_func(s); // 调用 col(s) 或 lit(s)
                Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
            })
        }
    };
}

/// 模式 3: 一元操作 (Unary Operator)
/// 生成: fn pl_expr_sum(ptr: *mut ExprContext) -> *mut ExprContext
macro_rules! gen_unary_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(ptr: *mut ExprContext) -> *mut ExprContext {
            ffi_try!({
                // 1. 拿回所有权
                let ctx = unsafe { Box::from_raw(ptr) };
                // 2. 调用方法 (如 ctx.inner.sum())
                let new_expr = ctx.inner.$method(); 
                // 3. 返回
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

/// 模式 4: 二元操作 (Binary Operator)
/// 生成: fn pl_expr_eq(left: *mut, right: *mut) -> *mut
macro_rules! gen_binary_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(left_ptr: *mut ExprContext, right_ptr: *mut ExprContext) -> *mut ExprContext {
            ffi_try!({
                let left = unsafe { Box::from_raw(left_ptr) };
                let right = unsafe { Box::from_raw(right_ptr) };
                
                // 调用 left.inner.eq(right.inner)
                let new_expr = left.inner.$method(right.inner);
                
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

/// 模式 5: 命名空间一元操作 (Namespace Unary)
/// 专门处理 .dt().year(), .str().to_uppercase() 这种
macro_rules! gen_namespace_unary {
    ($func_name:ident, $ns:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(ptr: *mut ExprContext) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(ptr) };
                // 例如: ctx.inner.dt().year()
                let new_expr = ctx.inner.$ns().$method();
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}
/// 模式 6: RollingWindow操作
fn parse_fixed_window_size(s: &str) -> PolarsResult<usize> {
    // 去掉可能的 "i" 后缀 (Polars 习惯 "3i" 代表 3 index/rows)
    let clean_s = s.trim().trim_end_matches('i');
    clean_s.parse::<usize>().map_err(|_| {
        PolarsError::ComputeError(format!("Invalid fixed window size: '{}'. For time-based windows (e.g. '3d'), use rolling_by.", s).into())
    })
}
macro_rules! gen_rolling_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            expr_ptr: *mut ExprContext,
            window_size_ptr: *const c_char,
            min_periods: usize,
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let window_size_str = ptr_to_str(window_size_ptr).unwrap();

                // 1. 解析大小
                let window_size = parse_fixed_window_size(window_size_str)?;

                // 2. 构建 Fixed Window Options
                let options = RollingOptionsFixedWindow {
                    window_size,
                    min_periods: min_periods, // 默认至少1个数据，防止全Null
                    weights: None,
                    center: false,
                    fn_params: None,
                };

                // 3. 调用 expr.rolling_mean(options)
                let new_expr = ctx.inner.$method(options);
                
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}
fn map_closed_window(s: &str) -> ClosedWindow {
    match s {
        "left" => ClosedWindow::Left,
        "right" => ClosedWindow::Right,
        "both" => ClosedWindow::Both,
        "none" => ClosedWindow::None,
        _ => ClosedWindow::Left, // 默认左闭右开 [ )
    }
}
macro_rules! gen_rolling_by_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            expr_ptr: *mut ExprContext,
            window_size_ptr: *const c_char,
            min_periods: usize,
            by_ptr: *mut ExprContext,       // 时间索引列
            closed_ptr: *const c_char       // "left", "right" ...
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let by = unsafe { Box::from_raw(by_ptr) }; 
                
                let window_size_str = ptr_to_str(window_size_ptr).unwrap();
                let closed_str = ptr_to_str(closed_ptr).unwrap_or("left");

                // 1. 解析 Duration
                // Duration::parse 会处理 "1d", "30m" 等格式
                let duration = Duration::parse(window_size_str);

                // 2. 构建 Options
                let options = RollingOptionsDynamicWindow {
                    window_size: duration,
                    min_periods: min_periods,
                    closed_window: map_closed_window(closed_str),
                    fn_params: None,
                };

                // 3. 调用 expr.rolling_xxx_by(by, options)
                // 注意：在 0.50 中，window_size 已经在 options 里了，所以函数参数变少了
                let new_expr = ctx.inner.$method(
                    by.inner, 
                    options
                );
                
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}
// ==========================================
// 2. 宏应用区域 (Boilerplate 消灭术)
// ==========================================

// --- Group 1: 构造函数 ---
gen_lit_ctor!(pl_expr_lit_i32, i32);
gen_lit_ctor!(pl_expr_lit_i64, i64);
gen_lit_ctor!(pl_expr_lit_bool, bool);
gen_lit_ctor!(pl_expr_lit_f32, f32);
gen_lit_ctor!(pl_expr_lit_f64, f64);

// --- Group 2: 字符串构造 ---
gen_str_ctor!(pl_expr_col, col);
gen_str_ctor!(pl_expr_lit_str, lit);

// --- Group 3: 一元操作 ---
gen_unary_op!(pl_expr_sum, sum);
gen_unary_op!(pl_expr_mean, mean);
gen_unary_op!(pl_expr_max, max);
gen_unary_op!(pl_expr_min, min);
gen_unary_op!(pl_expr_abs, abs);
// 逻辑非 (!)
gen_unary_op!(pl_expr_not, not);
// is_null()
gen_unary_op!(pl_expr_is_null, is_null);
gen_unary_op!(pl_expr_is_not_null, is_not_null);
// Math Ops
gen_unary_op!(pl_expr_sqrt,sqrt);
gen_unary_op!(pl_expr_exp,exp);

// --- Group 4: 二元操作 ---
gen_binary_op!(pl_expr_eq, eq); // ==
gen_binary_op!(pl_expr_neq, neq); // !=
gen_binary_op!(pl_expr_gt, gt); // >
gen_binary_op!(pl_expr_gt_eq, gt_eq); // >=
gen_binary_op!(pl_expr_lt, lt);       // <
gen_binary_op!(pl_expr_lt_eq, lt_eq); // <=
// 算术运算
gen_binary_op!(pl_expr_add, add); // +
gen_binary_op!(pl_expr_sub, sub); // -
gen_binary_op!(pl_expr_mul, mul); // *
gen_binary_op!(pl_expr_div, div); // /
gen_binary_op!(pl_expr_floor_div, floor_div); // //
gen_binary_op!(pl_expr_rem, rem); // % (取余)
// 逻辑运算
gen_binary_op!(pl_expr_and, and); // &
gen_binary_op!(pl_expr_or, or);   // |
gen_binary_op!(pl_expr_xor, xor); // xor
// Null Ops
gen_binary_op!(pl_expr_fill_null, fill_null);
// Math Ops
gen_binary_op!(pl_expr_pow,pow);

// --- Group 5: 命名空间操作 ---
// dt 命名空间
gen_namespace_unary!(pl_expr_dt_year, dt, year);
gen_namespace_unary!(pl_expr_dt_month, dt, month);
gen_namespace_unary!(pl_expr_dt_day, dt, day);
gen_namespace_unary!(pl_expr_dt_ordinal_day, dt, ordinal_day);
gen_namespace_unary!(pl_expr_dt_weekday, dt, weekday);
gen_namespace_unary!(pl_expr_dt_hour, dt, hour);
gen_namespace_unary!(pl_expr_dt_minute, dt, minute);
gen_namespace_unary!(pl_expr_dt_second, dt, second);
gen_namespace_unary!(pl_expr_dt_millisecond, dt, millisecond);
gen_namespace_unary!(pl_expr_dt_microsecond, dt, microsecond);
gen_namespace_unary!(pl_expr_dt_nanosecond, dt, nanosecond);

gen_namespace_unary!(pl_expr_dt_date, dt, date); // 转为 Date 类型
gen_namespace_unary!(pl_expr_dt_time, dt, time); // 转为 Time 类型
// String Namespace
gen_namespace_unary!(pl_expr_str_to_uppercase, str, to_uppercase);
gen_namespace_unary!(pl_expr_str_to_lowercase, str, to_lowercase);
gen_namespace_unary!(pl_expr_str_len_bytes, str, len_bytes);
// --- List Ops (list 命名空间) ---
gen_namespace_unary!(pl_expr_list_first, list, first);
gen_namespace_unary!(pl_expr_list_sum, list, sum);
gen_namespace_unary!(pl_expr_list_min, list, min);
gen_namespace_unary!(pl_expr_list_max, list, max);
gen_namespace_unary!(pl_expr_list_mean, list, mean);
// 
gen_rolling_op!(pl_expr_rolling_mean, rolling_mean);
gen_rolling_op!(pl_expr_rolling_sum, rolling_sum);
gen_rolling_op!(pl_expr_rolling_min, rolling_min);
gen_rolling_op!(pl_expr_rolling_max, rolling_max);

gen_rolling_by_op!(pl_expr_rolling_mean_by, rolling_mean_by);
gen_rolling_by_op!(pl_expr_rolling_sum_by, rolling_sum_by);
gen_rolling_by_op!(pl_expr_rolling_min_by, rolling_min_by);
gen_rolling_by_op!(pl_expr_rolling_max_by, rolling_max_by);

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_alias(expr_ptr: *mut ExprContext, name_ptr: *const c_char) -> *mut ExprContext {
    ffi_try!({
        let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
        let name = ptr_to_str(name_ptr).unwrap();
        // alias 逻辑
        let new_expr = expr_ctx.inner.alias(name);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_lit_null() -> *mut Expr {
    ffi_try!({
    let e = lit(Null {});
    Ok(Box::into_raw(Box::new(e)))
    })
}

// ==========================================
// String Operations (例如 Contains)
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_contains(
    expr_ptr: *mut ExprContext, 
    pat_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let pat = ptr_to_str(pat_ptr).unwrap();
        
        // str().contains() 比较特殊，有两个参数 (pattern, strict)
        // 这里的 false 是 hardcode 的 strict 参数，如果想暴露出去，需要修改 C 接口签名
        let new_expr = ctx.inner.str().contains(lit(pat), false);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// offset: 起始位置 (支持负数), length: 长度
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_slice(
    expr_ptr: *mut ExprContext, 
    offset: i64, 
    length: u64
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        // Polars API: str().slice(offset, length)
        let new_expr = ctx.inner.str().slice(offset.into(), length.into());
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// extract (正则提取)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_extract(
    expr_ptr: *mut ExprContext, 
    pat_ptr: *const c_char,
    group_index: usize
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let pat = ptr_to_str(pat_ptr).unwrap();
        
        // str.extract(pattern, group_index)
        let new_expr = ctx.inner.str().extract(lit(pat), group_index);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// 替换操作 (Replace All)
// pat: 匹配模式, val: 替换值
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_replace_all(
    expr_ptr: *mut ExprContext, 
    pat_ptr: *const c_char,
    val_ptr: *const c_char,
    use_regex: bool // [新增]
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let pat = ptr_to_str(pat_ptr).unwrap();
        let val = ptr_to_str(val_ptr).unwrap();

        // Polars 参数名是 literal。
        // 如果 use_regex = true，则 literal = false。
        let new_expr = ctx.inner.str().replace_all(lit(pat), lit(val), !use_regex);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_split(
    expr_ptr: *mut ExprContext, 
    pat_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let pat = ptr_to_str(pat_ptr).unwrap();
        // by_lengths=false (也就是 split by pattern)
        let new_expr = ctx.inner.str().split(lit(pat));
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Helper: 将 C 字符串转换为 Polars Literal Expr
// 如果 ptr 为 null，则返回 lit(Null) -> 表示去除空白符
unsafe fn str_or_null_lit(ptr: *const c_char) -> Expr {
    if ptr.is_null() {
        // 创建一个类型为 String 但值为 Null 的字面量
        lit(NULL) 
    } else {
        let s = unsafe { CStr::from_ptr(ptr).to_string_lossy() };
        lit(s.as_ref())
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_strip_chars(
    expr_ptr: *mut ExprContext, 
    matches: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let match_expr = unsafe { str_or_null_lit(matches) };
        
        // Clone 是为了支持不可变 API
        let new_expr = ctx.inner.str().strip_chars(match_expr);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// 2. Strip Chars Start (LTrim)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_strip_chars_start(
    expr_ptr: *mut ExprContext, 
    matches: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe {Box::from_raw(expr_ptr) };
        let match_expr = unsafe { str_or_null_lit(matches) };
        
        let new_expr = ctx.inner.str().strip_chars_start(match_expr);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// 3. Strip Chars End (RTrim)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_strip_chars_end(
    expr_ptr: *mut ExprContext, 
    matches: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr)};
        let match_expr = unsafe { str_or_null_lit(matches) };
        
        let new_expr = ctx.inner.str().strip_chars_end(match_expr);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// 4. Strip Prefix (去除固定前缀，不是字符集合)
// 比如 "http://google.com" strip_prefix "http://" -> "google.com"
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_strip_prefix(
    expr_ptr: *mut ExprContext, 
    prefix: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        // Prefix 必须有值，不能是 Null (业务逻辑上)
        let prefix_str = unsafe { CStr::from_ptr(prefix).to_string_lossy() };
        
        let new_expr = ctx.inner.str().strip_prefix(lit(prefix_str.as_ref()));
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// 5. Strip Suffix
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_strip_suffix(
    expr_ptr: *mut ExprContext, 
    suffix: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr)};
        let suffix_str = unsafe { CStr::from_ptr(suffix).to_string_lossy() };
        
        let new_expr = ctx.inner.str().strip_suffix(lit(suffix_str.as_ref()));
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// Anchors
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_starts_with(expr_ptr: *mut ExprContext, prefix: *const c_char) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(expr_ptr) };
    let p = unsafe { CStr::from_ptr(prefix).to_string_lossy() };
    
    // starts_with 接受 Expr，我们需要把 prefix 转为 Lit
    let new_expr = ctx.inner.str().starts_with(lit(p.as_ref()));
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_ends_with(expr_ptr: *mut ExprContext, suffix: *const c_char) -> *mut ExprContext {
    let ctx = unsafe {Box::from_raw(expr_ptr)};
    let s = unsafe { CStr::from_ptr(suffix).to_string_lossy() };
    
    let new_expr = ctx.inner.str().ends_with(lit(s.as_ref()));
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

// Parsing (String -> Date/Time)
// format: e.g. "%Y-%m-%d"
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_to_date(expr_ptr: *mut ExprContext, format: *const c_char) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(expr_ptr) };
    let fmt = unsafe { CStr::from_ptr(format).to_string_lossy() };
    
    // strptime(dtype, options)
    // 这里简化处理，直接用 StrptimeOptions::default()
    let options = StrptimeOptions {
        format: Some(fmt.into()),
        strict: false, // 转换失败返回 Null，不 panic
        exact: true,
        ..Default::default()
    };
    
    let new_expr = ctx.inner.str().to_date(options);
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_to_datetime(expr_ptr: *mut ExprContext, format: *const c_char) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(expr_ptr) };
    let fmt = unsafe { CStr::from_ptr(format).to_string_lossy() };
    
    let options = StrptimeOptions {
        format: Some(fmt.into()),
        strict: false,
        exact: true,
        ..Default::default()
    };
    
    // 默认转为 Microseconds, 无时区
    let new_expr = ctx.inner.str().to_datetime(Some(TimeUnit::Microseconds), None, options, lit("raise"));
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

// ==========================================
// 复用expr
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_clone(ptr: *mut ExprContext) -> *mut ExprContext {
    let ctx = unsafe { &*ptr };
    let new_expr = ctx.inner.clone();
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}
// ==========================================
// Temporal Ops
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_to_string(
    expr_ptr: *mut ExprContext,
    format_ptr: *const c_char // 必须传入格式字符串，如 "%Y-%m-%d"
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let format = ptr_to_str(format_ptr).unwrap();
        
        // Polars API: dt().to_string(format)
        let new_expr = ctx.inner.dt().to_string(format);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Truncate (Floor)
// every: e.g. "1h", "1d"
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_truncate(expr_ptr: *mut ExprContext, every: *const c_char) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) }; // Move
        let every_str = unsafe { CStr::from_ptr(every).to_string_lossy() };
        
        // dt().truncate(every)
        let new_expr = ctx.inner.dt().truncate(lit(every_str.as_ref()));
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Round
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_round(expr_ptr: *mut ExprContext, every: *const c_char) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let every_str = unsafe { CStr::from_ptr(every).to_string_lossy() };
        
        let new_expr = ctx.inner.dt().round(lit(every_str.as_ref()));
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Offset By (Add Duration)
// by: Duration Expr (e.g. lit("1d") or col("duration"))
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_offset_by(expr_ptr: *mut ExprContext, by_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let by_ctx = unsafe { Box::from_raw(by_ptr) };
        
        // dt().offset_by(expr)
        let new_expr = ctx.inner.dt().offset_by(by_ctx.inner);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Timestamp (to Int64)
// unit: 0=ns, 1=us, 2=ms
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_timestamp(expr_ptr: *mut ExprContext, unit_code: i32) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let unit = match unit_code {
            0 => TimeUnit::Nanoseconds,
            1 => TimeUnit::Microseconds,
            2 => TimeUnit::Milliseconds,
            _ => TimeUnit::Microseconds,
        };
        
        let new_expr = ctx.inner.dt().timestamp(unit);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// 辅助函数：String -> NonExistent Enum
fn parse_non_existent(s: &str) -> NonExistent {
    match s {
        "null" => NonExistent::Null,
        "raise" => NonExistent::Raise,
        // Polars 可能不支持其他策略用于 NonExistent，或者有 Time/RollForward 等
        // 根据 0.50 源码，通常是 raise 或 null。
        _ => NonExistent::Raise, // 默认报错
    }
}
// Convert Time Zone (Physical value changes, Wall time changes)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_convert_time_zone(
    expr_ptr: *mut ExprContext,
    tz_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let tz_str = unsafe { CStr::from_ptr(tz_ptr).to_string_lossy() };
        
        // [修复] 构造 TimeZone 结构体
        // Polars 0.50+ TimeZone::new(str)
        let tz = unsafe{ TimeZone::new_unchecked(tz_str.as_ref()) };
        
        let new_expr = ctx.inner.dt().convert_time_zone(tz);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Replace Time Zone (Physical value stays, Wall time changes or meta changes)
// tz_ptr: NULL means "unset" (make naive), otherwise "set" (make aware)
// ambiguous_ptr: "raise", "earliest", "latest", "null", etc.
// 2. Replace Time Zone (参数升级)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_replace_time_zone(
    expr_ptr: *mut ExprContext,
    tz_ptr: *const c_char,          // TimeZone (Option)
    ambiguous_ptr: *const c_char,   // Ambiguous (Expr string, e.g. "raise")
    non_existent_ptr: *const c_char // NonExistent (Enum string, e.g. "raise")
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        // A. 构造 Option<TimeZone>
        let tz = if tz_ptr.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(tz_ptr).to_string_lossy() };
            unsafe { Some(TimeZone::new_unchecked(s.as_ref())) }
        };

        // B. 构造 Ambiguous Expr
        // 这里的 ambiguous 是 Expr 类型，通常传 lit("raise") 或 lit("earliest")
        let amb_str = if ambiguous_ptr.is_null() {
            "raise"
        } else {
            unsafe { CStr::from_ptr(ambiguous_ptr).to_str().unwrap() }
        };
        let ambiguous_expr = lit(amb_str);

        // C. 构造 NonExistent Enum
        // 这里必须解析字符串为 Rust 枚举
        let ne_str = if non_existent_ptr.is_null() {
            "raise"
        } else {
            unsafe { CStr::from_ptr(non_existent_ptr).to_str().unwrap() }
        };
        let non_existent = parse_non_existent(ne_str);

        // D. 调用
        let new_expr = ctx.inner.dt().replace_time_zone(tz, ambiguous_expr, non_existent);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// ==========================================
// Intervals
// ==========================================
// --- IsBetween ---
// 这是一个三元操作: expr.is_between(lower, upper)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_is_between(
    expr_ptr: *mut ExprContext,
    lower_ptr: *mut ExprContext,
    upper_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let lower = unsafe { Box::from_raw(lower_ptr) };
        let upper = unsafe { Box::from_raw(upper_ptr) };

        // 默认 behavior 是 ClosedInterval::Both (闭区间 [])
        // 如果想暴露给 C#，可以传个 int 进来映射
        let new_expr = ctx.inner.is_between(lower.inner, upper.inner, ClosedInterval::Both);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// --- DateTime Literal ---
// 接收一个 i64 (微秒时间戳)，返回一个 Datetime 类型的 Expr
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_lit_datetime(
    micros: i64
) -> *mut ExprContext {
    ffi_try!({
        // 1. 先造一个 Int64 字面量
        let lit_expr = lit(micros);
        // 2. Cast 成 Datetime (Microseconds)
        let dt_expr = lit_expr.cast(DataType::Datetime(TimeUnit::Microseconds, None));
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: dt_expr })))
    })
}
// ==========================================
// List Ops
// ==========================================
// list.get(index)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_get(
    expr_ptr: *mut ExprContext, 
    index: i64
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.list().get(lit(index),true);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_sort(
    expr_ptr: *mut ExprContext,
    descending: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let options = SortOptions {
            descending,
            ..Default::default()
        };
        let new_expr = ctx.inner.list().sort(options);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// 3. list.contains(item)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_contains(
    expr_ptr: *mut ExprContext,
    item_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let item = unsafe { Box::from_raw(item_ptr) };

        let new_expr = item.inner.is_in(ctx.inner, true);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_cols(
    names_ptr: *const *const c_char,
    len: usize
) -> *mut ExprContext {
    ffi_try!({
        // 构造 Vec<String> (cols 接受 AsRef<str>)
        let mut names = Vec::with_capacity(len);
        let slice = unsafe { std::slice::from_raw_parts(names_ptr, len) };
        for &p in slice {
            let s = ptr_to_str(p).unwrap();
            names.push(s);
        }

        // polars::prelude::cols
        let selection = cols(names);
        let new_expr = selection.into();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_explode(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.explode();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_implode(expr: *mut Expr) -> *mut Expr {
    ffi_try!({
        let e = unsafe { Box::from_raw(expr) };
        // 调用 polars expr 的 implode
        let new_expr = e.implode();
        Ok(Box::into_raw(Box::new(new_expr)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_join(
    expr_ptr: *mut ExprContext,
    sep_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let sep = ptr_to_str(sep_ptr).unwrap();
        // list().join(sep, ignore_nulls=true)
        let new_expr = ctx.inner.list().join(lit(sep), true);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_len(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.list().len();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// Math
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_log(
    expr_ptr: *mut ExprContext, 
    base: f64 // <--- 这里是 f64，不是 *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        // Polars API: log(base: f64)
        let new_expr = ctx.inner.log(base); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_round(
    expr_ptr: *mut ExprContext, 
    decimals: u32
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        // round 默认行为
        let new_expr = ctx.inner.round(decimals, RoundMode::HalfAwayFromZero); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// Meta Data
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_len() -> *mut ExprContext {
    ffi_try!({
        // polars::prelude::len()
        let expr = len(); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

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

// --- Struct Ops (struct 命名空间) ---

// 1. as_struct(exprs) -> Expr (构造结构体)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_as_struct(
    exprs_ptr: *const *mut ExprContext,
    len: usize
) -> *mut ExprContext {
    ffi_try!({
        let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };
        // polars::prelude::as_struct
        let new_expr = as_struct(exprs);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// 2. struct.field_by_name(name)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_struct_field_by_name(
    expr_ptr: *mut ExprContext,
    name_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let name = ptr_to_str(name_ptr).unwrap();
        // struct_() 是进入 struct namespace 的入口
        let new_expr = ctx.inner.struct_().field_by_name(name);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// 1. 按索引取字段
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_struct_field_by_index(
    expr: *mut Expr, 
    index: i64
) -> *mut Expr {
    let e = unsafe { Box::from_raw(expr)};

    let new_expr = e.struct_().field_by_index(index);
    Box::into_raw(Box::new(new_expr))
}

// 2. 重命名字段
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_struct_rename_fields(
    expr: *mut Expr,
    names_ptr: *mut *mut c_char, // 字符串数组指针
    len: usize
) -> *mut Expr {
    let e = unsafe { Box::from_raw(expr) };
    
    // 将 C 字符串数组转换为 Vec<String>
    let names: Vec<String> = if names_ptr.is_null() || len == 0 {
        Vec::new()
    } else {
        let slice = unsafe { std::slice::from_raw_parts(names_ptr, len) };
        slice.iter()
            .map(|&p| unsafe { 
                std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned() 
            })
            .collect()
    };

    let new_expr = e.struct_().rename_fields(names);
    Box::into_raw(Box::new(new_expr))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_struct_json_encode(
    expr: *mut Expr
) -> *mut Expr {
    let e = unsafe { Box::from_raw(expr)};
    // Polars 原生支持 struct.json_encode()
    let new_expr = e.struct_().json_encode();
    Box::into_raw(Box::new(new_expr))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_over(
    expr_ptr: *mut ExprContext,
    partition_by_ptr: *const *mut ExprContext,
    len: usize
) -> *mut ExprContext {
    ffi_try!({
        // 1. 拿到主表达式 (例如 sum("salary"))
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        // 2. 拿到分组表达式列表 (例如 [col("department")])
        // 使用我们之前提取到 types.rs 的公共函数
        let partition_by = unsafe { consume_exprs_array(partition_by_ptr, len) };

        // 3. 调用 over
        let new_expr = ctx.inner.over(partition_by);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_cast(
    expr_ptr: *mut ExprContext, 
    dtype_ptr: *mut DataTypeContext,
    strict: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { &*expr_ptr };
        let target_dtype = unsafe { &(*dtype_ptr).dtype };

        let new_expr = if strict {
            ctx.inner.clone().strict_cast(target_dtype.clone())
        } else {
            ctx.inner.clone().cast(target_dtype.clone())
        };

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// --- Time Series: Shift / Diff ---
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_shift(
    expr_ptr: *mut ExprContext,
    n: i64
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        // shift(n)
        let new_expr = ctx.inner.shift(lit(n)); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// diff(n, null_behavior)
// null_behavior: "ignore" or "drop" (Polars 0.50 默认可能是 ignore)
// 这里简单起见，只暴露 n，使用默认行为
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_diff(
    expr_ptr: *mut ExprContext,
    n: i64
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        // diff(n, null_behavior)
        // NullBehavior::Ignore 是通用默认值
        let new_expr = ctx.inner.diff(n.into(), Default::default());
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// --- Time Series: Fill ---

// forward_fill -> fill_null_with_strategy(Forward)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_forward_fill(
    expr_ptr: *mut ExprContext,
    limit: u32 // 0 = None (Unlimited)
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        // 转换 limit: 0 -> None, 其他 -> Some
        let limit_opt = if limit == 0 { None } else { Some(limit as u32) };
        
        // 使用策略枚举
        let strategy = FillNullStrategy::Forward(limit_opt);
        let new_expr = ctx.inner.fill_null_with_strategy(strategy);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// backward_fill -> fill_null_with_strategy(Backward)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_backward_fill(
    expr_ptr: *mut ExprContext,
    limit: u32
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let limit_opt = if limit == 0 { None } else { Some(limit as u32) };
        
        let strategy = FillNullStrategy::Backward(limit_opt);
        let new_expr = ctx.inner.fill_null_with_strategy(strategy);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// 逻辑: when(predicate).then(truthy).otherwise(falsy)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_if_else(
    pred_ptr: *mut ExprContext,
    true_ptr: *mut ExprContext,
    false_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let pred = unsafe { Box::from_raw(pred_ptr) };
        let truthy = unsafe { Box::from_raw(true_ptr) };
        let falsy = unsafe { Box::from_raw(false_ptr) };

        // Polars DSL: when(...).then(...).otherwise(...)
        let new_expr = when(pred.inner)
            .then(truthy.inner)
            .otherwise(falsy.inner);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// --- Statistics ---
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_count(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(expr_ptr) };
    let new_expr = ctx.inner.count();
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_std(expr_ptr: *mut ExprContext, ddof: u8) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(expr_ptr)};
    // std(ddof) -> ddof usually 1 for sample std dev
    let new_expr = ctx.inner.std(ddof);
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_var(expr_ptr: *mut ExprContext, ddof: u8) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(expr_ptr)};
    let new_expr = ctx.inner.var(ddof);
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_median(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(expr_ptr)};
    let new_expr = ctx.inner.median();
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

// quantile(quantile, interpolation)
// interpolation: "nearest", "higher", "lower", "midpoint", "linear"
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_quantile(
    expr_ptr: *mut ExprContext, 
    quantile: f64, // e.g. 0.5
    interpol: *const c_char
) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(expr_ptr) };
    let method_str = unsafe { CStr::from_ptr(interpol).to_string_lossy() };
    
    // 解析 QuantileInterpolOptions
    let method = match method_str.as_ref() {
        "nearest" => QuantileMethod::Nearest,
        "higher" => QuantileMethod::Higher,
        "lower" => QuantileMethod::Lower,
        "midpoint" => QuantileMethod::Midpoint,
        _ => QuantileMethod::Linear, // 默认 Linear
    };

    let new_expr = ctx.inner.quantile(lit(quantile), method);
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_fill_nan(
    expr: *mut ExprContext,
    fill_value: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let e = unsafe { Box::from_raw(expr) };
        let v = unsafe { Box::from_raw(fill_value) };
        
        // fill_nan 接受一个 Expr
        let out = e.inner.fill_nan(v.inner);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: out })))
    })
}