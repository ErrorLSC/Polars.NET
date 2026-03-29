use polars::{prelude::*, sql::sql_expr};
use polars_plan::utils::expr_output_name;
use std::{ffi::{CStr, CString}, os::raw::c_char};
use crate::{datatypes::parse_timeunit, types::{DataTypeContext, ExprContext, SelectorContext, SeriesContext}};
use std::ops::{Add, Sub, Mul, Div, Rem};
use crate::utils::{consume_exprs_array, ptr_to_str};
use polars_arrow::array::PrimitiveArray;

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_free(ptr: *mut ExprContext) {
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}
// ==========================================
// Marcos
// ==========================================
/// Literal Constructor
/// like: fn pl_expr_lit_i32(val: i32) -> *mut ExprContext
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

/// String Constructor
/// like: fn pl_expr_col(ptr: *const c_char) -> *mut ExprContext
macro_rules! gen_str_ctor {
    ($func_name:ident, $polars_func:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(ptr: *const c_char) -> *mut ExprContext {
            ffi_try!({
                let s = ptr_to_str(ptr).unwrap();
                let expr = $polars_func(s); 
                Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
            })
        }
    };
}

/// Unary Operator
/// like: fn pl_expr_sum(ptr: *mut ExprContext) -> *mut ExprContext
macro_rules! gen_unary_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(ptr: *mut ExprContext) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(ptr) };
                let new_expr = ctx.inner.$method(); 
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

macro_rules! gen_unary_op_arg_bool {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(ptr: *mut ExprContext, param: bool) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(ptr) };
                let new_expr = ctx.inner.$method(param); 
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

macro_rules! gen_unary_op_u8 {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(ptr: *mut ExprContext, param: u8) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(ptr) };
                let new_expr = ctx.inner.$method(param); 
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

/// Binary Operator
/// like: fn pl_expr_eq(left: *mut, right: *mut) -> *mut
macro_rules! gen_binary_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(left_ptr: *mut ExprContext, right_ptr: *mut ExprContext) -> *mut ExprContext {
            ffi_try!({
                let left = unsafe { Box::from_raw(left_ptr) };
                let right = unsafe { Box::from_raw(right_ptr) };
                
                let new_expr = left.inner.$method(right.inner);
                
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

/// Namespace Unary
/// for .dt().year(), .str().to_uppercase(), etc.
macro_rules! gen_namespace_unary {
    ($func_name:ident, $ns:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(ptr: *mut ExprContext) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(ptr) };
                let new_expr = ctx.inner.$ns().$method();
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}
/// RollingWindow
fn parse_fixed_window_size(s: &str) -> PolarsResult<usize> {
    // remove "i" suffix
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
            weights_ptr: *const f64, 
            weights_len: usize,      
            center: bool             
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let window_size_str = ptr_to_str(window_size_ptr).unwrap();

                // Parse size
                let window_size = parse_fixed_window_size(window_size_str)?;

                // Parse weights (Handle null ptr)
                let weights = if !weights_ptr.is_null() && weights_len > 0 {
                    let slice = unsafe { std::slice::from_raw_parts(weights_ptr, weights_len) };
                    Some(slice.to_vec())
                } else {
                    None
                };

                // Build Fixed Window Options
                let options = RollingOptionsFixedWindow {
                    window_size,
                    min_periods: min_periods, 
                    weights,    
                    center,     
                    fn_params: None,
                };

                // Call expr.rolling_mean(options)
                let new_expr = ctx.inner.$method(options);
                
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}
fn parse_closed_window(val: u8) -> ClosedWindow {
    match val {
        0 => ClosedWindow::Left,
        1 => ClosedWindow::Right,
        2 => ClosedWindow::Both,
        3 => ClosedWindow::None,
        _ => ClosedWindow::Left,
    }
}
macro_rules! gen_rolling_by_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            expr_ptr: *mut ExprContext,
            window_size_ptr: *const c_char,
            min_periods: usize,
            by_ptr: *mut ExprContext,       
            closed: u8  
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let by = unsafe { Box::from_raw(by_ptr) }; 
                
                let window_size_str = ptr_to_str(window_size_ptr).unwrap();
                
                // Parse Duration
                let duration = Duration::parse(window_size_str);
                
                // Update: Use u8 helper directly
                let closed_window = parse_closed_window(closed);

                // Build Options
                let options = RollingOptionsDynamicWindow {
                    window_size: duration,
                    min_periods: min_periods,
                    closed_window: closed_window,
                    fn_params: None,
                };

                // Call expr.rolling_xxx_by(by, options)
                let new_expr = ctx.inner.$method(
                    by.inner, 
                    options
                );
                
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

/// EWM Ops Macro (Unified)
/// Signature: fn name(ptr, alpha, adjust, bias, min_periods, ignore_nulls)
macro_rules! gen_ewm_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            expr_ptr: *mut ExprContext,
            alpha: f64,
            adjust: bool,
            bias: bool,        // Unified: mean also takes bias now
            min_periods: usize,
            ignore_nulls: bool
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                
                let options = EWMOptions {
                    alpha: alpha,
                    adjust: adjust,
                    bias: bias,
                    min_periods: min_periods,
                    ignore_nulls: ignore_nulls,
                };

                let new_expr = ctx.inner.$method(options);
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

// ==========================================
// Boilerplate Killer
// ==========================================

// --- Group 1: lit funcs ---
gen_lit_ctor!(pl_expr_lit_i32, i32);
gen_lit_ctor!(pl_expr_lit_i64, i64);
gen_lit_ctor!(pl_expr_lit_bool, bool);
gen_lit_ctor!(pl_expr_lit_f16, pf16);
gen_lit_ctor!(pl_expr_lit_f32, f32);
gen_lit_ctor!(pl_expr_lit_f64, f64);
gen_lit_ctor!(pl_expr_lit_i8, i8);
gen_lit_ctor!(pl_expr_lit_u8, u8);
gen_lit_ctor!(pl_expr_lit_i16, i16);
gen_lit_ctor!(pl_expr_lit_u16, u16);
gen_lit_ctor!(pl_expr_lit_u32, u32);
gen_lit_ctor!(pl_expr_lit_u64, u64);

// --- Group 2: String col and lit ---
// gen_str_ctor!(pl_expr_col, col);
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_col(name: *const c_char) -> *mut ExprContext {
    ffi_try!({
        let name_str = if name.is_null() {
            ""
        } else {
            unsafe {
                CStr::from_ptr(name)
                    .to_str()
                    .map_err(|e| PolarsError::ComputeError(format!("Invalid UTF-8 in column name: {}", e).into()))?
            }
        };

        let expr = if name_str.is_empty() {
            polars::lazy::dsl::Expr::Element
        } else {
            col(name_str)
        };

        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}
gen_str_ctor!(pl_expr_lit_str, lit);

// --- Group 3: Unarp Ops ---
gen_unary_op!(pl_expr_rechunk, rechunk);
gen_unary_op!(pl_expr_approx_n_unique, approx_n_unique);
gen_unary_op!(pl_expr_sum, sum);
gen_unary_op!(pl_expr_mean, mean);

gen_unary_op!(pl_expr_max, max);
gen_unary_op!(pl_expr_min, min);
gen_unary_op!(pl_expr_abs, abs);
gen_unary_op!(pl_expr_null_count, null_count);
gen_unary_op!(pl_expr_n_unique,n_unique);
gen_unary_op!(pl_expr_product, product);
gen_unary_op!(pl_expr_first, first);
gen_unary_op!(pl_expr_first_non_null, first_non_null);
gen_unary_op!(pl_expr_last_non_null, last_non_null);
gen_unary_op!(pl_expr_last, last);
gen_unary_op!(pl_expr_reverse, reverse);
gen_unary_op_arg_bool!(pl_expr_any, any);
gen_unary_op_arg_bool!(pl_expr_all, all);
gen_unary_op_arg_bool!(pl_expr_item, item);
// Logic Not (!)
gen_unary_op!(pl_expr_not, not);
// is_null()
gen_unary_op!(pl_expr_is_null, is_null);
gen_unary_op!(pl_expr_is_not_null, is_not_null);
gen_unary_op!(pl_expr_drop_nulls, drop_nulls);
gen_unary_op!(pl_expr_drop_nans, drop_nans);
// dupilicated and unique
gen_unary_op!(pl_expr_unique, unique);
gen_unary_op!(pl_expr_unique_stable, unique_stable);
gen_unary_op!(pl_expr_is_duplicated, is_duplicated);
gen_unary_op!(pl_expr_is_unique, is_unique);
// Math Ops
gen_unary_op!(pl_expr_sqrt,sqrt);
gen_unary_op!(pl_expr_cbrt, cbrt);
gen_unary_op!(pl_expr_exp,exp);
// Arg Ops
gen_unary_op!(pl_expr_arg_unique,arg_unique);
gen_unary_op!(pl_expr_arg_min,arg_min);
gen_unary_op!(pl_expr_arg_max,arg_max);
// --- Trigonometry ---
gen_unary_op!(pl_expr_sin, sin);
gen_unary_op!(pl_expr_cos, cos);
gen_unary_op!(pl_expr_tan, tan);

gen_unary_op!(pl_expr_arcsin, arcsin);
gen_unary_op!(pl_expr_arccos, arccos);
gen_unary_op!(pl_expr_arctan, arctan);

gen_unary_op!(pl_expr_sinh, sinh);
gen_unary_op!(pl_expr_cosh, cosh);
gen_unary_op!(pl_expr_tanh, tanh);

gen_unary_op!(pl_expr_arcsinh, arcsinh);
gen_unary_op!(pl_expr_arccosh, arccosh);
gen_unary_op!(pl_expr_arctanh, arctanh);

gen_unary_op!(pl_expr_sign, sign); // Sign func (-1, 0, 1)
gen_unary_op!(pl_expr_ceil, ceil); // ceiling round
gen_unary_op!(pl_expr_floor, floor); // flooring round

// --- Group 4: Binary Ops ---
gen_binary_op!(pl_expr_eq, eq); // ==
gen_binary_op!(pl_expr_neq, neq); // !=
gen_binary_op!(pl_expr_gt, gt); // >
gen_binary_op!(pl_expr_gt_eq, gt_eq); // >=
gen_binary_op!(pl_expr_lt, lt);       // <
gen_binary_op!(pl_expr_lt_eq, lt_eq); // <=
gen_binary_op!(pl_expr_filter,filter);

// Arithmetic
gen_binary_op!(pl_expr_add, add); // +
gen_binary_op!(pl_expr_sub, sub); // -
gen_binary_op!(pl_expr_mul, mul); // *
gen_binary_op!(pl_expr_div, div); // /
gen_binary_op!(pl_expr_floor_div, floor_div); // //
gen_binary_op!(pl_expr_rem, rem); // % 
// Logic Ops
gen_binary_op!(pl_expr_and, and); // &
gen_binary_op!(pl_expr_or, or);   // |
gen_binary_op!(pl_expr_xor, xor); // xor
// Null Ops
gen_binary_op!(pl_expr_fill_null, fill_null);
gen_binary_op!(pl_expr_interpolate_by, interpolate_by);
// Math Ops
gen_binary_op!(pl_expr_pow,pow);
gen_binary_op!(pl_expr_dot,dot);
// Gather 
gen_binary_op!(pl_expr_gather, gather);
// --- Cumulative Functions ---
gen_unary_op_arg_bool!(pl_expr_cum_sum, cum_sum);
gen_unary_op_arg_bool!(pl_expr_cum_max, cum_max);
gen_unary_op_arg_bool!(pl_expr_cum_min, cum_min);
gen_unary_op_arg_bool!(pl_expr_cum_prod, cum_prod);
gen_unary_op_arg_bool!(pl_expr_cum_count, cum_count);
gen_unary_op_arg_bool!(pl_expr_mode, mode);

// --- EWM Functions ---
// Mean/Std/Var all share the same signature now
gen_ewm_op!(pl_expr_ewm_mean, ewm_mean);
gen_ewm_op!(pl_expr_ewm_std, ewm_std);
gen_ewm_op!(pl_expr_ewm_var, ewm_var);

// --- Group 5: Namespace Ops ---
// dt namespace
gen_namespace_unary!(pl_expr_dt_year, dt, year);
gen_namespace_unary!(pl_expr_dt_month, dt, month);
gen_namespace_unary!(pl_expr_dt_quarter, dt, quarter);
gen_namespace_unary!(pl_expr_dt_day, dt, day);
gen_namespace_unary!(pl_expr_dt_ordinal_day, dt, ordinal_day);
gen_namespace_unary!(pl_expr_dt_weekday, dt, weekday);
gen_namespace_unary!(pl_expr_dt_hour, dt, hour);
gen_namespace_unary!(pl_expr_dt_minute, dt, minute);
gen_namespace_unary!(pl_expr_dt_second, dt, second);
gen_namespace_unary!(pl_expr_dt_millisecond, dt, millisecond);
gen_namespace_unary!(pl_expr_dt_microsecond, dt, microsecond);
gen_namespace_unary!(pl_expr_dt_nanosecond, dt, nanosecond);

gen_namespace_unary!(pl_expr_dt_date, dt, date); // convert to Date
gen_namespace_unary!(pl_expr_dt_time, dt, time); // convert to Time 
// String Namespace
gen_namespace_unary!(pl_expr_str_to_uppercase, str, to_uppercase);
gen_namespace_unary!(pl_expr_str_to_lowercase, str, to_lowercase);
gen_namespace_unary!(pl_expr_str_len_bytes, str, len_bytes);
// --- List Ops ---
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
gen_rolling_op!(pl_expr_rolling_std, rolling_std);
gen_rolling_op!(pl_expr_rolling_median, rolling_median);

gen_rolling_by_op!(pl_expr_rolling_mean_by, rolling_mean_by);
gen_rolling_by_op!(pl_expr_rolling_sum_by, rolling_sum_by);
gen_rolling_by_op!(pl_expr_rolling_min_by, rolling_min_by);
gen_rolling_by_op!(pl_expr_rolling_max_by, rolling_max_by);
gen_rolling_by_op!(pl_expr_rolling_std_by, rolling_std_by);
gen_rolling_by_op!(pl_expr_rolling_median_by, rolling_median_by);

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_combine(
    expr_ptr: *mut ExprContext, 
    time_ptr: *mut ExprContext, 
    tu: u8
) -> *mut ExprContext {
    let expr = unsafe { Box::from_raw(expr_ptr)};
    let time = unsafe {Box::from_raw(time_ptr)};
    let time_unit = parse_timeunit(tu);

    let new_expr = expr.inner.dt().combine(time.inner, time_unit);

    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_alias(expr_ptr: *mut ExprContext, name_ptr: *const c_char) -> *mut ExprContext {
    ffi_try!({
        let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
        let name = ptr_to_str(name_ptr).unwrap();
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

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_head(
    expr_ptr: *mut ExprContext, 
    length: usize
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.head(Some(length));
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_tail(
    expr_ptr: *mut ExprContext, 
    length: usize
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.tail(Some(length));
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_is_in(
    expr_ptr: *mut ExprContext, 
    other_ptr: *mut ExprContext,
    nulls_equal : bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let other = unsafe { Box::from_raw(other_ptr) };

        let new_expr = ctx.inner.is_in(other.inner,nulls_equal);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// Gather(Take)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_get(
    expr_ptr: *mut ExprContext,
    idx_ptr: *mut ExprContext,
    null_on_oob: bool,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let idx = unsafe { Box::from_raw(idx_ptr) };
        
        let new_expr = ctx.inner.get(idx.inner, null_on_oob);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_gather_every(
    expr_ptr: *mut ExprContext,
    n: usize,
    offset: usize,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let new_expr = ctx.inner.gather_every(n, offset);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// Sort
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_sort(
    expr_ptr: *mut ExprContext,
    descending: bool,
    nulls_last: bool,
    multithreaded: bool,
    maintain_order: bool,
    limit_ptr: *const IdxSize, 
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let limit = if limit_ptr.is_null() {
            None
        } else {
            Some(unsafe { *limit_ptr })
        };

        let options = SortOptions {
            descending,
            nulls_last,
            multithreaded,
            maintain_order,
            limit,
        };
        
        let new_expr = ctx.inner.sort(options);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// Arg Ops
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_arg_sort(
    expr_ptr: *mut ExprContext,
    descending: bool,
    nulls_last: bool,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let new_expr = ctx.inner.arg_sort(descending, nulls_last);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// Index Of
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_index_of(
    expr_ptr: *mut ExprContext,
    element_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let element = unsafe { Box::from_raw(element_ptr) };
        
        let new_expr = ctx.inner.index_of(element.inner);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// SearchSorted
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_search_sorted(
    expr_ptr: *mut ExprContext,
    element_ptr: *mut ExprContext,
    side: u8, 
    descending: bool,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let element = unsafe { Box::from_raw(element_ptr) };
        
        let search_side = match side {
            1 => SearchSortedSide::Left,
            2 => SearchSortedSide::Right,
            _ => SearchSortedSide::Any, 
        };

        let new_expr = ctx.inner.search_sorted(element.inner, search_side, descending);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// EWM By (Time-based EWM)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_ewm_mean_by(
    expr_ptr: *mut ExprContext,
    by_ptr: *mut ExprContext,       // The 'times' expression
    half_life_ptr: *const c_char    // Duration string, e.g. "1d", "12h"
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let by = unsafe { Box::from_raw(by_ptr) };
        
        let half_life_str = ptr_to_str(half_life_ptr).unwrap();
        
        let half_life = Duration::parse(half_life_str);

        let new_expr = ctx.inner.ewm_mean_by(by.inner, half_life);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// ==========================================
// Bitwise Operations (Shift)
// ==========================================

// Macro for shift op
macro_rules! impl_shift_op {
    ($s:ident, $n:ident, $op:tt) => {{
        match $s.dtype() {
            // Signed Integers
            DataType::Int8 => {
                let ca = $s.i8()?;
                Ok(ca.apply_values(|v| v $op $n).into_series())
            },
            DataType::Int16 => Ok($s.i16()?.apply_values(|v| v $op $n).into_series()),
            DataType::Int32 => Ok($s.i32()?.apply_values(|v| v $op $n).into_series()),
            DataType::Int64 => Ok($s.i64()?.apply_values(|v| v $op $n).into_series()),
            
            // Unsigned Integers
            DataType::UInt8 => Ok($s.u8()?.apply_values(|v| v $op $n).into_series()),
            DataType::UInt16 => Ok($s.u16()?.apply_values(|v| v $op $n).into_series()),
            DataType::UInt32 => Ok($s.u32()?.apply_values(|v| v $op $n).into_series()),
            DataType::UInt64 => Ok($s.u64()?.apply_values(|v| v $op $n).into_series()),
            
            // Other dtype not supported
            dt => polars_bail!(ComputeError: "Bitwise shift not supported for dtype: {}", dt),
        }
    }}
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_bit_shl(expr_ptr: *mut ExprContext, n: i32) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let function = move |c: Column| {
            let s = c.as_materialized_series();
            
            let op_result: PolarsResult<Series> = impl_shift_op!(s, n, <<);
            let res_series = op_result?;
            
            Ok(Column::from(res_series))
        };

        let output_map = |_input_schema: &Schema, input_field: &Field| Ok(input_field.clone());

        let new_expr = ctx.inner.map(function, output_map);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_bit_shr(expr_ptr: *mut ExprContext, n: i32) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let function = move |c: Column| {
            let s = c.as_materialized_series();
            let op_result: PolarsResult<Series> = impl_shift_op!(s, n, >>);
            let res_series = op_result?;

            Ok(Column::from(res_series))
        };

        let output_map = |_input_schema: &Schema, input_field: &Field| Ok(input_field.clone());

        let new_expr = ctx.inner.map(function, output_map);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// String Operations 
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_format_str(
    format_ptr: *const c_char,
    exprs_ptr: *const *mut ExprContext,
    exprs_len: usize
) -> *mut ExprContext {
    ffi_try!({
        let format_template = if format_ptr.is_null() {
            ""
        } else {
            unsafe { std::ffi::CStr::from_ptr(format_ptr) }
                .to_str()
                .map_err(|e| PolarsError::ComputeError(format!("Invalid UTF-8 in format string: {}", e).into()))?
        };

        let mut exprs = Vec::with_capacity(exprs_len);
        let ptr_slice = unsafe { std::slice::from_raw_parts(exprs_ptr, exprs_len) };
        for &ptr in ptr_slice {
            let expr_ctx = unsafe { Box::from_raw(ptr) };
            exprs.push(expr_ctx.inner);
        }

        let new_expr = format_str(format_template, exprs)?;
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_contains(
    expr_ptr: *mut ExprContext, 
    pat_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let pat = ptr_to_str(pat_ptr).unwrap();
        
        let new_expr = ctx.inner.str().contains(lit(pat), false);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// offset: start position , length: offset length
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
// extract (Regex)
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
// Replace All
// pat: matching pattern, val: replace value
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_replace_all(
    expr_ptr: *mut ExprContext, 
    pat_ptr: *const c_char,
    val_ptr: *const c_char,
    use_regex: bool 
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let pat = ptr_to_str(pat_ptr).unwrap();
        let val = ptr_to_str(val_ptr).unwrap();

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
        let new_expr = ctx.inner.str().split(lit(pat));
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Helper: Convert C String into Polars Literal Expr
// if ptr is null，return lit(Null)
unsafe fn str_or_null_lit(ptr: *const c_char) -> Expr {
    if ptr.is_null() {
        lit(NULL) 
    } else {
        let s = unsafe { CStr::from_ptr(ptr).to_string_lossy() };
        lit(s.as_ref() as &str)
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
        
        let new_expr = ctx.inner.str().strip_chars(match_expr);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Strip Chars Start (LTrim)
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

// Strip Chars End (RTrim)
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

// Strip Prefix
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_strip_prefix(
    expr_ptr: *mut ExprContext, 
    prefix: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let prefix_str = unsafe { CStr::from_ptr(prefix).to_string_lossy() };
        
        let new_expr = ctx.inner.str().strip_prefix(lit(prefix_str.as_ref() as &str));
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Strip Suffix
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_strip_suffix(
    expr_ptr: *mut ExprContext, 
    suffix: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr)};
        let suffix_str = unsafe { CStr::from_ptr(suffix).to_string_lossy() };
        
        let new_expr = ctx.inner.str().strip_suffix(lit(suffix_str.as_ref()as &str));
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// Anchors
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_starts_with(expr_ptr: *mut ExprContext, prefix: *const c_char) -> *mut ExprContext {
    let ctx = unsafe { Box::from_raw(expr_ptr) };
    let p = unsafe { CStr::from_ptr(prefix).to_string_lossy() };
    
    let new_expr = ctx.inner.str().starts_with(lit(p.as_ref()as &str));
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_ends_with(expr_ptr: *mut ExprContext, suffix: *const c_char) -> *mut ExprContext {
    let ctx = unsafe {Box::from_raw(expr_ptr)};
    let s = unsafe { CStr::from_ptr(suffix).to_string_lossy() };
    
    let new_expr = ctx.inner.str().ends_with(lit(s.as_ref()as &str));
    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

// Parsing (String -> Date/Time)
// format: e.g. "%Y-%m-%d"
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_to_date(
    expr_ptr: *mut ExprContext,
    format: *const c_char,
    strict: bool,
    exact: bool,
    cache: bool,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };

        let format_opt = if format.is_null() {
            None
        } else {
            let fmt_str = unsafe { CStr::from_ptr(format).to_string_lossy() };
            Some(fmt_str.into())
        };

        let options = StrptimeOptions {
            format: format_opt,
            strict,
            exact,
            cache,
        };

        let new_expr = ctx.inner.str().to_date(options);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_to_datetime(
    expr_ptr: *mut ExprContext,
    time_unit: u8, // 0: Nano, 1: Micro, 2: Milli, -1: None
    time_zone: *const c_char,
    format: *const c_char,
    strict: bool,
    exact: bool,
    cache: bool,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let tu_opt = match time_unit {
            0 => Some(TimeUnit::Nanoseconds),
            1 => Some(TimeUnit::Microseconds),
            2 => Some(TimeUnit::Milliseconds),
            _ => None, 
        };

        let tz_opt = if time_zone.is_null() {
            None
        } else {
            let tz_str = unsafe { CStr::from_ptr(time_zone).to_string_lossy() };
            TimeZone::opt_try_new(Some(tz_str.as_ref()))?
        };

        let format_opt = if format.is_null() {
            None
        } else {
            let fmt_str = unsafe { CStr::from_ptr(format).to_string_lossy() };
            Some(fmt_str.into())
        };

        let options = StrptimeOptions {
            format: format_opt,
            strict,
            exact,
            cache,
        };

        let new_expr = ctx.inner.str().to_datetime(tu_opt, tz_opt, options, lit("raise"));
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// clone expr
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
    format_ptr: *const c_char
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
        let ctx = unsafe { Box::from_raw(expr_ptr) }; 
        let every_str = unsafe { CStr::from_ptr(every).to_string_lossy() };
        
        // dt().truncate(every)
        let new_expr = ctx.inner.dt().truncate(lit(every_str.as_ref()as &str));
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Round
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_round(expr_ptr: *mut ExprContext, every: *const c_char) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let every_str = unsafe { CStr::from_ptr(every).to_string_lossy() };
        
        let new_expr = ctx.inner.dt().round(lit(every_str.as_ref()as &str));
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
pub extern "C" fn pl_expr_dt_timestamp(expr_ptr: *mut ExprContext, unit_code: u8) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let unit = parse_timeunit(unit_code);
        let new_expr = ctx.inner.dt().timestamp(unit);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Helper：String -> NonExistent Enum
fn parse_non_existent(s: &str) -> NonExistent {
    match s {
        "null" => NonExistent::Null,
        "raise" => NonExistent::Raise,
        _ => NonExistent::Raise, 
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
        
        // Polars 0.50+ TimeZone::new(str)
        let tz = unsafe{ TimeZone::new_unchecked(tz_str.as_ref()as &str) };
        
        let new_expr = ctx.inner.dt().convert_time_zone(tz);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Replace Time Zone (Physical value stays, Wall time changes or meta changes)
// tz_ptr: NULL means "unset" (make naive), otherwise "set" (make aware)
// ambiguous_ptr: "raise", "earliest", "latest", "null", etc.
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_replace_time_zone(
    expr_ptr: *mut ExprContext,
    tz_ptr: *const c_char,          // TimeZone (Option)
    ambiguous_ptr: *const c_char,   // Ambiguous (Expr string, e.g. "raise")
    non_existent_ptr: *const c_char // NonExistent (Enum string, e.g. "raise")
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        // Build Option<TimeZone>
        let tz = if tz_ptr.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(tz_ptr).to_string_lossy() };
            unsafe { Some(TimeZone::new_unchecked(s.as_ref()as &str)) }
        };

        // Build Ambiguous Expr
        let amb_str = if ambiguous_ptr.is_null() {
            "raise"
        } else {
            unsafe { CStr::from_ptr(ambiguous_ptr).to_str().unwrap() }
        };
        let ambiguous_expr = lit(amb_str);

        // Build NonExistent Enum
        let ne_str = if non_existent_ptr.is_null() {
            "raise"
        } else {
            unsafe { CStr::from_ptr(non_existent_ptr).to_str().unwrap() }
        };
        let non_existent = parse_non_existent(ne_str);

        // Call Polars
        let new_expr = ctx.inner.dt().replace_time_zone(tz, ambiguous_expr, non_existent);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_add_business_days(
    expr_ptr: *mut ExprContext,
    n_ptr: *mut ExprContext,
    week_mask_ptr: *const u8, 
    holidays_ptr: *const i32, 
    holidays_len: usize,
    roll_strategy: u8          
) -> *mut ExprContext {
    ffi_try!({
        let e = unsafe { Box::from_raw(expr_ptr) };
        let n = unsafe { Box::from_raw(n_ptr) };
        
        // Build Week Mask [bool; 7]
        // Order: Mon, Tue, Wed, Thu, Fri, Sat, Sun
        let week_mask = unsafe {
        let slice = std::slice::from_raw_parts(week_mask_ptr, 7);
        let mut arr = [false; 7];
            for i in 0..7 {
                arr[i] = slice[i] != 0; 
            }
            arr
        };

        // Build Holidays Vec<i32>
        let holidays = unsafe {
            std::slice::from_raw_parts(holidays_ptr, holidays_len).to_vec()
        };

        // Build Roll Strategy
        let roll = match roll_strategy {
            1 => Roll::Forward,
            2 => Roll::Backward,
            _ => Roll::Raise,
        };

        // Call Polars DSL
        let new_expr = e.inner.dt().add_business_days(
            n.inner,
            week_mask,
            holidays,
            roll
        );

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_is_business_day(
    expr_ptr: *mut ExprContext,
    week_mask_ptr: *const u8,
    holidays_ptr: *const i32,
    holidays_len: usize
) -> *mut ExprContext {
    ffi_try!({
        let e = unsafe { Box::from_raw(expr_ptr) };

        let week_mask = unsafe {
        let slice = std::slice::from_raw_parts(week_mask_ptr, 7);
        let mut arr = [false; 7];
            for i in 0..7 {
                arr[i] = slice[i] != 0; 
            }
            arr
        };

        let holidays = unsafe {
            std::slice::from_raw_parts(holidays_ptr, holidays_len).to_vec()
        };

        let new_expr = e.inner.dt().is_business_day(
            week_mask,
            holidays
        );

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// Intervals
// ==========================================
// --- IsBetween ---
// expr.is_between(lower, upper)
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

        let new_expr = ctx.inner.is_between(lower.inner, upper.inner, ClosedInterval::Both);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// --- DateTime Literal ---
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_lit_datetime(
    micros: i64
) -> *mut ExprContext {
    ffi_try!({
        let lit_expr = lit(micros);
        let dt_expr = lit_expr.cast(DataType::Datetime(TimeUnit::Microseconds, None));
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: dt_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_lit_date(
    days: i32
) -> *mut ExprContext {
    ffi_try!({
        let lit_expr = lit(days);
        let dt_expr = lit_expr.cast(DataType::Date);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: dt_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_lit_time(
    nanoseconds: i64
) -> *mut ExprContext {
    ffi_try!({
        let lit_expr = lit(nanoseconds);
        let dt_expr = lit_expr.cast(DataType::Time);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: dt_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_lit_duration(
    microseconds: i64
) -> *mut ExprContext {
    ffi_try!({
        let lit_expr = lit(microseconds);
        
        let dur_expr = lit_expr.cast(DataType::Duration(TimeUnit::Microseconds));
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: dur_expr })))
    })
}

// lit Decimal and Int128
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_lit_decimal(
    low: u64,  // C# split low part
    high: i64, // C# split high part
    scale: u32,
) -> *mut ExprContext {
    ffi_try!({
        // Rebuild i128 (Unscaled Value)
        let v = ((high as i128) << 64) | (low as i128);

        let data_type = ArrowDataType::Decimal(38, scale as usize);

        let arrow_array = PrimitiveArray::new(
            data_type,
            vec![v].into(), 
            None
        );

        let series = Series::from_arrow(
            "literal".into(), 
            Box::new(arrow_array)
        ).unwrap();

        let expr = lit(series);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_lit_i128(
    low: u64,  // C# split low part
    high: i64 // C# split high part
) -> *mut ExprContext {
    ffi_try!({
        // Rebuild i128 (Unscaled Value)
        let v = ((high as i128) << 64) | (low as i128);

        let data_type = ArrowDataType::Int128;

        let arrow_array = PrimitiveArray::new(
            data_type,
            vec![v].into(), 
            None
        );

        let series = Series::from_arrow(
            "literal".into(), 
            Box::new(arrow_array)
        ).unwrap();

        let expr = lit(series);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_lit_series(
    series_ptr: *mut SeriesContext
) -> *mut ExprContext {
    ffi_try!({
        let s_ctx = unsafe { Box::from_raw(series_ptr) };
        let s = s_ctx.series;

        let expr = lit(s);

        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
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
    descending: bool,
    nulls_last: bool,      
    maintain_order: bool   
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let options = SortOptions {
            descending,
            nulls_last,
            multithreaded: true, 
            maintain_order,
            limit: None
        };
        
        let new_expr = ctx.inner.list().sort(options);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// 3. list.contains(item)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_list_contains(
    expr_ptr: *mut ExprContext,
    item_ptr: *mut ExprContext,
    nulls_equal: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let item = unsafe { Box::from_raw(item_ptr) };

        let new_expr = ctx.inner.list().contains(item.inner, nulls_equal);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_cols(
    names_ptr: *const *const c_char,
    len: usize
) -> *mut ExprContext {
    ffi_try!({
        // Build Vec<String>
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
pub extern "C" fn pl_expr_list_reverse(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.list().reverse();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
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
// Concat Ops 
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_concat_array(
    exprs_ptr: *const *mut ExprContext,
    exprs_len: usize
) -> *mut ExprContext {
    ffi_try!({
        let mut exprs = Vec::with_capacity(exprs_len);
        let ptr_slice = unsafe { std::slice::from_raw_parts(exprs_ptr, exprs_len) };
        for &ptr in ptr_slice {
            let expr_ctx = unsafe { Box::from_raw(ptr) };
            exprs.push(expr_ctx.inner);
        }

        let new_expr = concat_arr(exprs)?;
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_concat_list(
    exprs_ptr: *const *mut ExprContext,
    exprs_len: usize
) -> *mut ExprContext {
    ffi_try!({
        let mut exprs = Vec::with_capacity(exprs_len);
        let ptr_slice = unsafe { std::slice::from_raw_parts(exprs_ptr, exprs_len) };
        for &ptr in ptr_slice {
            let expr_ctx = unsafe { Box::from_raw(ptr) };
            exprs.push(expr_ctx.inner);
        }

        let new_expr = concat_list(exprs)?;
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_concat_str(
    exprs_ptr: *const *mut ExprContext,
    exprs_len: usize,
    separator_ptr: *const c_char,
    ignore_nulls: bool
) -> *mut ExprContext {
    ffi_try!({
        let separator = if separator_ptr.is_null() {
            ""
        } else {
            unsafe { CStr::from_ptr(separator_ptr) }
                .to_str()
                .map_err(|e| PolarsError::ComputeError(format!("Invalid UTF-8 in separator: {}", e).into()))?
        };

        let mut exprs = Vec::with_capacity(exprs_len);
        let ptr_slice = unsafe { std::slice::from_raw_parts(exprs_ptr, exprs_len) };
        for &ptr in ptr_slice {
            let expr_ctx = unsafe { Box::from_raw(ptr) };
            exprs.push(expr_ctx.inner);
        }

        let new_expr = concat_str(exprs, separator, ignore_nulls);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_concat_expr(
    exprs_ptr: *const *mut ExprContext,
    exprs_len: usize,
    rechunk: bool
) -> *mut ExprContext {
    ffi_try!({
        let mut exprs = Vec::with_capacity(exprs_len);
        let ptr_slice = unsafe { std::slice::from_raw_parts(exprs_ptr, exprs_len) };
        for &ptr in ptr_slice {
            let expr_ctx = unsafe { Box::from_raw(ptr) };
            exprs.push(expr_ctx.inner);
        }

        let new_expr = concat_expr(exprs, rechunk)?;
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// Array Ops 
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_max(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        // array().max()
        let new_expr = ctx.inner.arr().max(); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_min(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().min();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_sum(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().sum();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_unique(expr_ptr: *mut ExprContext, stable: bool) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = if stable {
            ctx.inner.arr().unique_stable()
        } else {
            ctx.inner.arr().unique()
        };
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Join elements with a separator (requires Array<String>)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_join(
    expr_ptr: *mut ExprContext, 
    separator_ptr: *const c_char,
    ignore_nulls: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let sep = ptr_to_str(separator_ptr).unwrap();
        
        // array().join(separator, ignore_nulls)
        let new_expr = ctx.inner.arr().join(lit(sep), ignore_nulls);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Convert Array (Fixed) to List (Variable)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_to_list(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let new_expr = ctx.inner.cast(DataType::List(Box::new(DataType::Null)));
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Check if array contains value
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_contains(
    expr_ptr: *mut ExprContext,
    item_ptr: *mut ExprContext,
    nulls_equal: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let item = unsafe { Box::from_raw(item_ptr) };
        
        // array().contains(item)
        let new_expr = ctx.inner.arr().contains(item.inner,nulls_equal);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Array Statistics
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_mean(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().mean();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_median(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().median();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_std(expr_ptr: *mut ExprContext, ddof: u8) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().std(ddof);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_var(expr_ptr: *mut ExprContext, ddof: u8) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().var(ddof);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Boolean logic
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_any(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().any();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_all(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().all();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Sort
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_sort(
    expr_ptr: *mut ExprContext, 
    descending: bool, 
    nulls_last: bool,
    maintain_order: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let options = SortOptions {
            descending,
            nulls_last,
            multithreaded: true,
            maintain_order,
            limit: None,
        };
        let new_expr = ctx.inner.arr().sort(options);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_reverse(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().reverse();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_arg_min(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().arg_min();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_arg_max(expr_ptr: *mut ExprContext) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().arg_max();
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Transform
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_get(
    expr_ptr: *mut ExprContext, 
    index_ptr: *mut ExprContext,
    null_on_oob: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let idx = unsafe { Box::from_raw(index_ptr) };
        // arr().get(index, null_on_oob)
        let new_expr = ctx.inner.arr().get(idx.inner, null_on_oob);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_explode(
    expr_ptr: *mut ExprContext,
    empty_as_null: bool,
    keep_nulls: bool
) -> *mut ExprContext {
    ffi_try!({
        let options = ExplodeOptions {
            empty_as_null,
            keep_nulls,
        };
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().explode(options);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_array_to_struct(
    expr_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.arr().to_struct(None);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// ==========================================
// Math
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_log(
    expr_ptr: *mut ExprContext, 
    base: f64 
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        // Polars API: log(base: f64)
        let new_expr = ctx.inner.log(base.into()); 
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
        // default round
        let new_expr = ctx.inner.round(decimals, RoundMode::HalfAwayFromZero); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// ==========================================
// Meta Data
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_len() -> *mut ExprContext {
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

// --- Struct Ops ---

// as_struct(exprs) -> Expr
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

// struct.field_by_name(name)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_struct_field_by_name(
    expr_ptr: *mut ExprContext,
    name_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let name = ptr_to_str(name_ptr).unwrap();
        // struct_() is the entry of struct namespace
        let new_expr = ctx.inner.struct_().field_by_name(name);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_struct_field_by_index(
    expr: *mut Expr, 
    index: i64
) -> *mut Expr {
    let e = unsafe { Box::from_raw(expr)};

    let new_expr = e.struct_().field_by_index(index);
    Box::into_raw(Box::new(new_expr))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_struct_rename_fields(
    expr: *mut Expr,
    names_ptr: *mut *mut c_char,
    len: usize
) -> *mut Expr {
    let e = unsafe { Box::from_raw(expr) };
    
    // Convert C String vector to Vec<String>
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
    let new_expr = e.struct_().json_encode();
    Box::into_raw(Box::new(new_expr))
}

// --- Rolling Var ---
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_rolling_var(
    expr_ptr: *mut ExprContext,
    window_size_ptr: *const c_char,
    min_periods: usize,
    weights_ptr: *const f64, 
    weights_len: usize,      
    center: bool,  
    ddof: u8
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let window_size_str = ptr_to_str(window_size_ptr).unwrap();
        let window_size = parse_fixed_window_size(window_size_str)?;

        let params = RollingFnParams::Var(RollingVarParams { ddof });

        let weights = if !weights_ptr.is_null() && weights_len > 0 {
                    let slice = unsafe { std::slice::from_raw_parts(weights_ptr, weights_len) };
                    Some(slice.to_vec())
                } else {
                    None
                };

        let options = RollingOptionsFixedWindow {
            window_size,
            min_periods,
            weights: weights,
            center: center,
            fn_params: Some(params),
        };

        let new_expr = ctx.inner.rolling_var(options);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_rolling_var_by(
    expr_ptr: *mut ExprContext,
    window_size_ptr: *const c_char,
    min_periods: usize,
    by_ptr: *mut ExprContext,
    closed: u8,
    ddof: u8 
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let by = unsafe { Box::from_raw(by_ptr) };
        
        let window_size_str = ptr_to_str(window_size_ptr).unwrap();
        let duration = Duration::parse(window_size_str);
        let closed_window = parse_closed_window(closed);

        let params = RollingFnParams::Var(RollingVarParams { ddof });

        let options = RollingOptionsDynamicWindow {
            window_size: duration,
            min_periods,
            closed_window,
            fn_params: Some(params),
        };

        let new_expr = ctx.inner.rolling_var_by(by.inner, options);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// --- Skews ---
gen_unary_op_arg_bool!(pl_expr_skew, skew);

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_rolling_skew(
    expr_ptr: *mut ExprContext,
    window_size_ptr: *const c_char,
    min_periods: usize,
    weights_ptr: *const f64, 
    weights_len: usize,      
    center: bool,  
    bias: bool 
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let window_size_str = ptr_to_str(window_size_ptr).unwrap();
        let window_size = parse_fixed_window_size(window_size_str)?;
        let weights = if !weights_ptr.is_null() && weights_len > 0 {
                    let slice = unsafe { std::slice::from_raw_parts(weights_ptr, weights_len) };
                    Some(slice.to_vec())
                } else {
                    None
                };
        let params = RollingFnParams::Skew { bias };

        let options = RollingOptionsFixedWindow {
            window_size,
            min_periods,
            weights: weights,
            center: center,
            fn_params: Some(params),
        };

        let new_expr = ctx.inner.rolling_skew(options);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// --- Kurtosis
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_kurtosis(
    expr_ptr: *mut ExprContext,
    fisher: bool,
    bias: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.kurtosis(fisher, bias);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_rolling_kurtosis(
    expr_ptr: *mut ExprContext,
    window_size_ptr: *const c_char,
    min_periods: usize,
    weights_ptr: *const f64, 
    weights_len: usize,      
    center: bool,  
    fisher: bool, 
    bias: bool    
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let window_size_str = ptr_to_str(window_size_ptr).unwrap();
        let window_size = parse_fixed_window_size(window_size_str)?;

        let weights = if !weights_ptr.is_null() && weights_len > 0 {
                    let slice = unsafe { std::slice::from_raw_parts(weights_ptr, weights_len) };
                    Some(slice.to_vec())
                } else {
                    None
                };

        let params = RollingFnParams::Kurtosis { fisher, bias };

        let options = RollingOptionsFixedWindow {
            window_size,
            min_periods,
            weights: weights,
            center: center,
            fn_params: Some(params),
        };

        let new_expr = ctx.inner.rolling_kurtosis(options);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// --- Ranks ---

// Helper: Map u8 to RankMethod
// C# Enum mapping:
// 0 = Average (Default), 1 = Min, 2 = Max, 3 = Dense, 4 = Ordinal, 5 = Random
fn parse_rank_method(val: u8) -> RankMethod {
    match val {
        1 => RankMethod::Min,
        2 => RankMethod::Max,
        3 => RankMethod::Dense,
        4 => RankMethod::Ordinal,
        5 => RankMethod::Random,
        _ => RankMethod::Average,
    }
}
fn parse_rolling_rank_method(val: u8) -> RollingRankMethod {
    match val {
        1 => RollingRankMethod::Min,
        2 => RollingRankMethod::Max,
        3 => RollingRankMethod::Dense,
        4 => RollingRankMethod::Random,
        _ => RollingRankMethod::Average,
    }
}
// rank(method, descending, seed)
// method: "average", "min", "max", "dense", "ordinal", "random"
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_rank(
    expr_ptr: *mut ExprContext,
    method: u8, // Changed from *const c_char, removed _ptr suffix
    descending: bool,
    seed_ptr: *const u64
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let rank_method = parse_rank_method(method);

        let options = RankOptions {
            method: rank_method,
            descending,
        };

        let seed = if seed_ptr.is_null() {
            None
        } else {
            Some(unsafe { *seed_ptr })
        };

        let new_expr = ctx.inner.rank(options, seed);

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_rolling_rank(
    expr_ptr: *mut ExprContext,
    window_size_ptr: *const c_char,
    min_periods: usize,
    method: u8,                            
    seed_ptr: *const u64,  
    weights_ptr: *const f64,
    weights_len: usize,
    center: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let window_size_str = ptr_to_str(window_size_ptr).unwrap();
        let window_size = parse_fixed_window_size(window_size_str)?;
        
        let rank_method = parse_rolling_rank_method(method); 
        let seed = if seed_ptr.is_null() { None } else { Some(unsafe { *seed_ptr }) };

        let rank_params = RollingFnParams::Rank {
            method: rank_method,
            seed: seed
        };

        let weights = if !weights_ptr.is_null() && weights_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(weights_ptr, weights_len) };
            Some(slice.to_vec())
        } else {
            None
        };

        let options = RollingOptionsFixedWindow {
            window_size,
            min_periods,
            weights,
            center: center,
            fn_params: Some(rank_params) 
        };

        let new_expr = ctx.inner.rolling_rank(options);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_rolling_rank_by(
    expr_ptr: *mut ExprContext,
    method: u8,                     
    seed_ptr: *const u64,           
    window_size_ptr: *const c_char,
    min_periods: usize,
    by_ptr: *mut ExprContext,       
    closed: u8
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let by = unsafe { Box::from_raw(by_ptr) }; 
        
        let window_size_str = ptr_to_str(window_size_ptr).unwrap();
        let duration = Duration::parse(window_size_str);
        let closed_window = parse_closed_window(closed);

        let rank_method = parse_rolling_rank_method(method);

        let seed = if seed_ptr.is_null() {
            None
        } else {
            Some(unsafe { *seed_ptr })
        };

        let rank_params = RollingFnParams::Rank {
            method: rank_method,
            seed: seed
        };

        let options = RollingOptionsDynamicWindow {
            window_size: duration,
            min_periods,
            closed_window,
            fn_params: Some(rank_params) 
        };

        let new_expr = ctx.inner.rolling_rank_by(by.inner, options);

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// --- Differences ---
// pct_change(n) -> (val - lag(n)) / lag(n)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_pct_change(
    expr_ptr: *mut ExprContext,
    n: i64
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let new_expr = ctx.inner.pct_change(lit(n));
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_over(
    expr_ptr: *mut ExprContext,
    partition_by_ptr: *const *mut ExprContext,
    len: usize
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let partition_by = unsafe { consume_exprs_array(partition_by_ptr, len) };

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
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_diff(
    expr_ptr: *mut ExprContext,
    n: i64
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        // diff(n, null_behavior)
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
        
        let limit_opt = if limit == 0 { None } else { Some(limit as u32) };
        
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
// ==========================================
// Interpolate
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_interpolate(
    expr_ptr: *mut ExprContext,
    method: u8 // 0=Linear, 1=Nearest
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let interp_method = match method {
            1 => InterpolationMethod::Nearest,
            _ => InterpolationMethod::Linear,
        };

        let new_expr = ctx.inner.interpolate(interp_method);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Logic: when(predicate).then(truthy).otherwise(falsy)
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
gen_unary_op!(pl_expr_count, count);
gen_unary_op!(pl_expr_len, len);
gen_unary_op!(pl_expr_median, median);
gen_unary_op_u8!(pl_expr_std, std);
gen_unary_op_u8!(pl_expr_var, var);

// quantile(quantile, interpolation)
// interpolation: "nearest", "higher", "lower", "midpoint", "linear"

// Helper: Map u8 to QuantileMethod
// C# Enum mapping:
// 0 = Nearest, 1 = Higher, 2 = Lower, 3 = Midpoint, 4 = Linear (Default)
fn parse_quantile_method(val: u8) -> QuantileMethod {
    match val {
        0 => QuantileMethod::Nearest,
        1 => QuantileMethod::Higher,
        2 => QuantileMethod::Lower,
        3 => QuantileMethod::Midpoint,
        _ => QuantileMethod::Linear,
    }

}
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_quantile(
    expr_ptr: *mut ExprContext, 
    quantile: f64, 
    interpol: u8  // Changed from *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let method = parse_quantile_method(interpol);

        let new_expr = ctx.inner.quantile(lit(quantile), method);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_rolling_quantile(
    expr_ptr: *mut ExprContext,
    quantile: f64,
    interpolation: u8, // Changed from *const c_char, removed _ptr suffix
    window_size_ptr: *const c_char,
    min_periods: usize,
    weights_ptr: *const f64,
    weights_len: usize,
    center: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let window_size_str = ptr_to_str(window_size_ptr).unwrap();
        
        let window_size = parse_fixed_window_size(window_size_str)?;
        
        let method = parse_quantile_method(interpolation);

        // Parse Weights
        let weights = if !weights_ptr.is_null() && weights_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(weights_ptr, weights_len) };
            Some(slice.to_vec())
        } else {
            None
        };

        let options = RollingOptionsFixedWindow {
            window_size,
            min_periods,
            weights,
            center: center,
            fn_params: None
        };

        let new_expr = ctx.inner.rolling_quantile(method, quantile, options);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_rolling_quantile_by(
    expr_ptr: *mut ExprContext,
    quantile: f64,                  
    interpolation: u8,              // Interpolation Enum
    window_size_ptr: *const c_char, 
    min_periods: usize,
    by_ptr: *mut ExprContext,       
    closed: u8                      // Changed from *const c_char to u8
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let by = unsafe { Box::from_raw(by_ptr) };
        
        let window_size_str = ptr_to_str(window_size_ptr).unwrap();
        
        let duration = Duration::parse(window_size_str);
        
        let closed_window = parse_closed_window(closed);
        let method = parse_quantile_method(interpolation);

        let options = RollingOptionsDynamicWindow {
            window_size: duration,
            min_periods,
            closed_window,
            fn_params: None, 
        };

        let new_expr = ctx.inner.rolling_quantile_by(
            by.inner,
            method,
            quantile,
            options
        );
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_fill_nan(
    expr: *mut ExprContext,
    fill_value: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let e = unsafe { Box::from_raw(expr) };
        let v = unsafe { Box::from_raw(fill_value) };
        
        let out = e.inner.fill_nan(v.inner);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: out })))
    })
}

// --- TopK / BottomK ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_top_k(expr_ptr: *mut ExprContext, k: u32) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        // Polars: self.top_k(lit(k))
        let new_expr = ctx.inner.top_k(lit(k));
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_bottom_k(expr_ptr: *mut ExprContext, k: u32) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        // Polars: self.bottom_k(lit(k))
        let new_expr = ctx.inner.bottom_k(lit(k));
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_top_k_by(
    expr_ptr: *mut ExprContext, 
    k: u32, 
    by_ptrs: *const *mut ExprContext, 
    by_len: usize,
    descending_ptr: *const bool,      
    desc_len: usize
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let mut by_exprs = Vec::with_capacity(by_len);
        if by_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(by_ptrs, by_len) };
            for &p in slice {
                let e = unsafe { Box::from_raw(p) };
                by_exprs.push(e.inner);
            }
        }

        let mut descending = Vec::with_capacity(desc_len);
        if desc_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(descending_ptr, desc_len) };
            descending.extend_from_slice(slice);
        }

        let new_expr = ctx.inner.top_k_by(lit(k), by_exprs, descending);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_bottom_k_by(
    expr_ptr: *mut ExprContext, 
    k: u32, 
    by_ptrs: *const *mut ExprContext, 
    by_len: usize,
    descending_ptr: *const bool, 
    desc_len: usize
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let mut by_exprs = Vec::with_capacity(by_len);
        if by_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(by_ptrs, by_len) };
            for &p in slice {
                let e = unsafe { Box::from_raw(p) };
                by_exprs.push(e.inner);
            }
        }

        let mut descending = Vec::with_capacity(desc_len);
        if desc_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(descending_ptr, desc_len) };
            descending.extend_from_slice(slice);
        }

        let new_expr = ctx.inner.bottom_k_by(lit(k), by_exprs, descending);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_explode(
    expr_ptr: *mut ExprContext,
    empty_as_null: bool,
    keep_nulls: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let options = ExplodeOptions {
            empty_as_null,
            keep_nulls,
        };
        let new_expr = ctx.inner.explode(options);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_implode(expr: *mut Expr) -> *mut Expr {
    ffi_try!({
        let e = unsafe { Box::from_raw(expr) };
        let new_expr = e.implode();
        Ok(Box::into_raw(Box::new(new_expr)))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_sql(
    sql_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let sql_str = ptr_to_str(sql_ptr).unwrap();
        
        let expr = sql_expr(sql_str)?;
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

// Meta

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_get_output_name(
    expr_ptr: *mut ExprContext,
    out_str: *mut *mut c_char
) -> bool {
    let ctx = unsafe { &*expr_ptr };
    
    match expr_output_name(&ctx.inner) {
        Ok(name) => {
            let c_str = CString::new(name.as_str()).unwrap();
            unsafe { *out_str = c_str.into_raw() };
            true
        },
        Err(_) => false 
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_try_into_selector(
    expr_ptr: *mut ExprContext
) -> *mut SelectorContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let selector = ctx.inner.try_into_selector()?;
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: selector })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_to_string(ptr: *mut ExprContext) -> *mut c_char {
    ffi_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "Expr pointer is null");
        }
        
        let ctx = unsafe { &*ptr };
        
        let mut s = ctx.inner.to_string();
        
        if s.contains('\0') {
            s = s.replace('\0', "␀"); 
        }
        
        let c_str = CString::new(s).expect("String sanitization failed");
        Ok(c_str.into_raw())
    })
}