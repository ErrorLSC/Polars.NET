use polars::{chunked_array::cast::CastOptions, prelude::*, series::ops::NullBehavior, sql::sql_expr};
use std::{ffi::{CStr, CString}, os::raw::c_char, slice::from_raw_parts};
use crate::{types::{DataTypeExprContext, ExprContext, SeriesContext}, utils::{parse_closed_window, ptr_to_opt_pl_str_vec}};
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
#[macro_export]
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

#[macro_export]
macro_rules! impl_expr_namespace_expr_arg {
    ($ffi_name:ident, $namespace:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $ffi_name(
            expr_ptr: *mut ExprContext, 
            arg_ptr: *mut ExprContext
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let arg = unsafe { Box::from_raw(arg_ptr) };
                
                let new_expr = ctx.inner.$namespace().$method(arg.inner);
                
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
gen_unary_op!(pl_expr_nan_max, nan_max);
gen_unary_op!(pl_expr_nan_min, nan_min);
gen_unary_op!(pl_expr_min, min);
gen_unary_op!(pl_expr_abs, abs);
gen_unary_op!(pl_expr_null_count, null_count);
gen_unary_op!(pl_expr_n_unique,n_unique);
gen_unary_op!(pl_expr_product, product);
gen_unary_op!(pl_expr_rle, rle);
gen_unary_op!(pl_expr_rle_id, rle_id);
gen_unary_op!(pl_expr_peak_max, peak_max);
gen_unary_op!(pl_expr_peak_min,peak_min);
gen_unary_op!(pl_expr_to_physical,to_physical);

gen_unary_op!(pl_expr_bitwise_and, bitwise_and);
gen_unary_op!(pl_expr_bitwise_or, bitwise_or);
gen_unary_op!(pl_expr_bitwise_xor, bitwise_xor);
gen_unary_op!(pl_expr_bitwise_count_ones, bitwise_count_ones);
gen_unary_op!(pl_expr_bitwise_count_zeros, bitwise_count_zeros);
gen_unary_op!(pl_expr_bitwise_leading_ones, bitwise_leading_ones);
gen_unary_op!(pl_expr_bitwise_leading_zeros, bitwise_leading_zeros);
gen_unary_op!(pl_expr_bitwise_trailing_ones, bitwise_trailing_ones);
gen_unary_op!(pl_expr_bitwise_trailing_zeros, bitwise_trailing_zeros);

gen_unary_op!(pl_expr_first, first);
gen_unary_op!(pl_expr_first_non_null, first_non_null);
gen_unary_op!(pl_expr_last_non_null, last_non_null);
gen_unary_op!(pl_expr_last, last);
gen_unary_op!(pl_expr_reverse, reverse);
gen_unary_op!(pl_expr_upper_bound, upper_bound);
gen_unary_op!(pl_expr_lower_bound, lower_bound);
gen_unary_op_arg_bool!(pl_expr_any, any);
gen_unary_op_arg_bool!(pl_expr_all, all);
gen_unary_op_arg_bool!(pl_expr_item, item);
// Logic Not (!)
gen_unary_op!(pl_expr_not, not);
// is_null()
gen_unary_op!(pl_expr_is_null, is_null);
gen_unary_op!(pl_expr_is_not_null, is_not_null);
gen_unary_op!(pl_expr_is_nan, is_nan);
gen_unary_op!(pl_expr_is_not_nan, is_not_nan);
gen_unary_op!(pl_expr_is_infinite, is_infinite);
gen_unary_op!(pl_expr_is_finite, is_finite);
gen_unary_op!(pl_expr_is_first_distinct, is_first_distinct);
gen_unary_op!(pl_expr_is_last_distinct, is_last_distinct);
gen_unary_op!(pl_expr_drop_nulls, drop_nulls);
gen_unary_op!(pl_expr_drop_nans, drop_nans);
// dupilicated and unique
gen_unary_op!(pl_expr_unique, unique);
gen_unary_op!(pl_expr_unique_counts, unique_counts);
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
gen_unary_op!(pl_expr_cot, cot);

gen_unary_op!(pl_expr_arcsin, arcsin);
gen_unary_op!(pl_expr_arccos, arccos);
gen_unary_op!(pl_expr_arctan, arctan);

gen_unary_op!(pl_expr_sinh, sinh);
gen_unary_op!(pl_expr_cosh, cosh);
gen_unary_op!(pl_expr_tanh, tanh);

gen_unary_op!(pl_expr_arcsinh, arcsinh);
gen_unary_op!(pl_expr_arccosh, arccosh);
gen_unary_op!(pl_expr_arctanh, arctanh);

gen_unary_op!(pl_expr_degrees, degrees);
gen_unary_op!(pl_expr_radians, radians);

gen_unary_op!(pl_expr_sign, sign); // Sign func (-1, 0, 1)
gen_unary_op!(pl_expr_ceil, ceil); // ceiling round
gen_unary_op!(pl_expr_floor, floor); // flooring round

// --- Group 4: Binary Ops ---
gen_binary_op!(pl_expr_eq, eq); // ==
gen_binary_op!(pl_expr_eq_missing, eq_missing); // ==
gen_binary_op!(pl_expr_neq, neq); // !=
gen_binary_op!(pl_expr_neq_missing, neq_missing); // !=
gen_binary_op!(pl_expr_gt, gt); // >
gen_binary_op!(pl_expr_gt_eq, gt_eq); // >=
gen_binary_op!(pl_expr_lt, lt);       // <
gen_binary_op!(pl_expr_lt_eq, lt_eq); // <=
gen_binary_op!(pl_expr_filter,filter);

gen_binary_op!(pl_expr_max_by, max_by); 
gen_binary_op!(pl_expr_min_by, min_by); 

gen_binary_op!(pl_expr_clip_max, clip_max); 
gen_binary_op!(pl_expr_clip_min, clip_min); 

gen_binary_op!(pl_expr_repeat_by, repeat_by); 

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
gen_binary_op!(pl_expr_fill_nan, fill_nan);
gen_binary_op!(pl_expr_interpolate_by, interpolate_by);
// Math Ops
gen_binary_op!(pl_expr_pow,pow);
gen_unary_op!(pl_expr_log1p, log1p);
gen_binary_op!(pl_expr_log,log);
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
gen_unary_op_arg_bool!(pl_expr_reinterpret, reinterpret);
// --- EWM Functions ---
// Mean/Std/Var all share the same signature now
gen_ewm_op!(pl_expr_ewm_mean, ewm_mean);
gen_ewm_op!(pl_expr_ewm_std, ewm_std);
gen_ewm_op!(pl_expr_ewm_var, ewm_var);

// --- Group 5: Namespace Ops ---

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
pub extern "C" fn pl_expr_reshape(
    expr_ptr: *mut ExprContext,
    dims_ptr: *const i64,
    dims_len: usize,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let dims = unsafe { from_raw_parts(dims_ptr, dims_len) };
        
        let new_expr = ctx.inner.reshape(dims);
        
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


#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_is_close(
    expr_ptr: *mut ExprContext, 
    other_ptr: *mut ExprContext,
    abs_tol: f64,
    rel_tol : f64,
    nans_equal: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let other = unsafe { Box::from_raw(other_ptr) };

        let new_expr = ctx.inner.is_close(other.inner,abs_tol,rel_tol,nans_equal);
        
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

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_arg_sort_by(
    by_exprs: *const *mut ExprContext,
    num_exprs: usize,
    descending: *const bool,
    nulls_last: *const bool,
    multithreaded: bool,
    maintain_order: bool,
) -> *mut ExprContext {
    ffi_try!({
        let by_vec = unsafe { consume_exprs_array(by_exprs, num_exprs) };

        let desc_vec = if descending.is_null() {
            vec![false; num_exprs]
        } else {
            unsafe { std::slice::from_raw_parts(descending, num_exprs) }.to_vec()
        };

        let nulls_vec = if nulls_last.is_null() {
            vec![false; num_exprs]
        } else {
            unsafe { std::slice::from_raw_parts(nulls_last, num_exprs) }.to_vec()
        };

        let sort_options = SortMultipleOptions {
            descending: desc_vec,
            nulls_last: nulls_vec,
            multithreaded,
            maintain_order,
            limit:None
        };

        let new_expr = arg_sort_by(by_vec, sort_options);

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_arg_where(
    condition_ptr: *mut ExprContext,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(condition_ptr) };
        
        let expr = arg_where(ctx.inner);

        Ok(Box::into_raw(Box::new(ExprContext { inner:expr })))
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
        let v = ((high as i128) << 64) | (low as i128);
        let expr = lit(v); 
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
// Concat Ops 
// ==========================================

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
// Math
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_round(
    expr_ptr: *mut ExprContext, 
    decimals: u32,
    mode_code:u8
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        // default round
        let mode = match mode_code {
            0 => RoundMode::HalfAwayFromZero,
            1 => RoundMode::HalfToEven,
            _ => RoundMode::HalfAwayFromZero
        };
        let new_expr = ctx.inner.round(decimals, mode); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_round_sig_figs(
    expr_ptr: *mut ExprContext, 
    digits: i32
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };

        let new_expr = ctx.inner.round_sig_figs(digits); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}


// #[unsafe(no_mangle)]
// pub extern "C" fn pl_expr_truncate(
//     expr_ptr: *mut ExprContext, 
//     digits: i32
// ) -> *mut ExprContext {
//     ffi_try!({
//         let ctx = unsafe { Box::from_raw(expr_ptr) };

//         let new_expr = ctx.inner.truncate(digits); 
//         Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
//     })
// }

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_shuffle(
    expr_ptr: *mut ExprContext,
    has_seed: bool,
    seed: u64,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let seed_opt = if has_seed { Some(seed) } else { None };
        
        let new_expr = ctx.inner.shuffle(seed_opt);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_sample_n(
    expr_ptr: *mut ExprContext,
    n_ptr: *mut ExprContext,
    with_replacement: bool,
    shuffle: bool,
    has_seed: bool,
    seed: u64,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let n_ctx = unsafe { Box::from_raw(n_ptr) };
        let seed_opt = if has_seed { Some(seed) } else { None };
        
        let new_expr = ctx.inner.sample_n(
            n_ctx.inner,
            with_replacement,
            shuffle,
            seed_opt,
        );
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_sample_frac(
    expr_ptr: *mut ExprContext,
    frac_ptr: *mut ExprContext,
    with_replacement: bool,
    shuffle: bool,
    has_seed: bool,
    seed: u64,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let frac_ctx = unsafe { Box::from_raw(frac_ptr) };
        let seed_opt = if has_seed { Some(seed) } else { None };
        
        let new_expr = ctx.inner.sample_frac(
            frac_ctx.inner,
            with_replacement,
            shuffle,
            seed_opt,
        );
        
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
    dexpr_ptr: *mut DataTypeExprContext, 
    strict: bool,
    wrap_numerical: bool
) -> *mut ExprContext {
    ffi_try!({
        let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
        let dexpr_ctx = unsafe { Box::from_raw(dexpr_ptr) };

        let options = match (strict, wrap_numerical) {
            (true, _) => CastOptions::Strict,
            (false, true) => CastOptions::Overflowing,
            (false, false) => CastOptions::NonStrict,
        };

        let new_expr = expr_ctx.inner.cast_with_options(dexpr_ctx.inner, options);

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// --- Time Series: Shift / Diff ---
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_shift(
    expr_ptr: *mut ExprContext,
    n_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let n = unsafe { Box::from_raw(n_ptr) };
        // shift(n)
        let new_expr = ctx.inner.shift(n.inner); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// diff(n, null_behavior)
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_diff(
    expr_ptr: *mut ExprContext,
    n_ptr: *mut ExprContext,
    null_behavior_code:u8
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let n = unsafe { Box::from_raw(n_ptr) };
        let behavior = match null_behavior_code {
            0 => NullBehavior::Ignore,
            1 => NullBehavior::Drop,
            _ => NullBehavior::Ignore
        };
        let new_expr = ctx.inner.diff(n.inner,behavior);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
// --- Time Series: Fill ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_fill_null_with_strategy(
    expr_ptr: *mut ExprContext,
    strategy_code: u8,
    limit: u32 // 0 = None (Unlimited)
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let limit_opt = if limit == 0 { None } else { Some(limit as u32) };
        
        let strategy = match strategy_code {
            0 => FillNullStrategy::Forward(limit_opt),
            1 => FillNullStrategy::Backward(limit_opt),
            2 => FillNullStrategy::Max,
            3 => FillNullStrategy::Min,
            4 => FillNullStrategy::Mean,
            5 => FillNullStrategy::Zero,
            6 => FillNullStrategy::One,
            _ => FillNullStrategy::Zero,
        };
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
pub extern "C" fn pl_expr_ternary(
    pred_ptr: *mut ExprContext,
    true_ptr: *mut ExprContext,
    false_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let pred = unsafe { Box::from_raw(pred_ptr) };
        let truthy = unsafe { Box::from_raw(true_ptr) };
        let falsy = unsafe { Box::from_raw(false_ptr) };

        let new_expr = ternary_expr(pred.inner, truthy.inner, falsy.inner);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_append(
    self_ptr: *mut ExprContext,
    other_ptr: *mut ExprContext,
    upcast: bool
) -> *mut ExprContext {
    ffi_try!({
        let self_expr = unsafe { Box::from_raw(self_ptr) };
        let other = unsafe { Box::from_raw(other_ptr) };

        let new_expr = self_expr.inner.append(other.inner, upcast);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_extend_constant(
    self_ptr: *mut ExprContext,
    value_ptr: *mut ExprContext,
    n_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let self_expr = unsafe { Box::from_raw(self_ptr) };
        let value = unsafe { Box::from_raw(value_ptr) };
        let n = unsafe { Box::from_raw(n_ptr) };

        let new_expr = self_expr.inner.extend_constant(value.inner, n.inner);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_value_counts(
    self_ptr: *mut ExprContext,
    sort: bool,
    parallel : bool,
    name_ptr: *const c_char,
    normalize:bool
) -> *mut ExprContext {
    ffi_try!({
        let self_expr = unsafe { Box::from_raw(self_ptr) };
        let name = ptr_to_str(name_ptr).unwrap();

        let new_expr = self_expr.inner.value_counts(sort,parallel,name,normalize);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}



#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_entropy(
    self_ptr: *mut ExprContext,
    base: f64,
    normalize:bool
) -> *mut ExprContext {
    ffi_try!({
        let self_expr = unsafe { Box::from_raw(self_ptr) };

        let new_expr = self_expr.inner.entropy(base,normalize);
        
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

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_expr_coalesce(
    expr_ptrs: *const *mut ExprContext,
    len: usize,
) -> *mut ExprContext {
    ffi_try!({
        if expr_ptrs.is_null() || len == 0 {
            return Err(PolarsError::ComputeError("coalesce requires at least one expression".into()));
        }

        let exprs = unsafe {consume_exprs_array(expr_ptrs, len)};

        let result_expr = polars::lazy::dsl::coalesce(&exprs);

        Ok(Box::into_raw(Box::new(ExprContext { inner: result_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_set_sorted_flag(
    expr_ptr: *mut ExprContext,
    descending: bool,
    _nulls_last: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe {Box::from_raw(expr_ptr) };

        let sorted_flag = if descending {
            polars::series::IsSorted::Descending
        } else {
            polars::series::IsSorted::Ascending
        };

        let res_expr = ctx.inner.set_sorted_flag(sorted_flag);

        Ok(Box::into_raw(Box::new(ExprContext { inner: res_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_hash(
    expr_ptr: *mut ExprContext,
    k0: u64,
    k1: u64,
    k2: u64,
    k3: u64,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe {Box::from_raw(expr_ptr) };

        let res_expr = ctx.inner.hash(k0,k1,k2,k3);

        Ok(Box::into_raw(Box::new(ExprContext { inner: res_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_replace(
    self_ptr: *mut ExprContext,
    old_ptr: *mut ExprContext,
    new_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let self_expr = unsafe { Box::from_raw(self_ptr) };
        let old = unsafe { Box::from_raw(old_ptr) };
        let new = unsafe { Box::from_raw(new_ptr) };

        let new_expr = self_expr.inner.replace(old.inner, new.inner);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_replace_strict(
    self_ptr: *mut ExprContext,
    old_ptr: *mut ExprContext,
    new_ptr: *mut ExprContext,
    default_ptr: *mut ExprContext,
    return_dtype_ptr: *mut DataTypeExprContext,
) -> *mut ExprContext {
    ffi_try!({
        let self_expr = unsafe { Box::from_raw(self_ptr) };
        let old = unsafe { Box::from_raw(old_ptr) };
        let new = unsafe { Box::from_raw(new_ptr) };

        let default_opt = if default_ptr.is_null() {
            None
        } else {
            Some(unsafe { Box::from_raw(default_ptr) }.inner)
        };

        let return_dtype_opt = if return_dtype_ptr.is_null() {
            None
        } else {
            Some(unsafe { Box::from_raw(return_dtype_ptr) }.inner)
        };

        let out_expr = self_expr.inner.replace_strict(
            old.inner,
            new.inner,
            default_opt,
            return_dtype_opt,
        );

        Ok(Box::into_raw(Box::new(ExprContext { inner: out_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_cut(
    expr_ptr: *mut ExprContext,
    breaks_ptr: *const f64,
    breaks_len: usize,
    labels_ptr: *const *const c_char,
    labels_len: usize,
    left_closed: bool,
    include_breaks: bool,
) -> *mut ExprContext {
    ffi_try!({
        let expr = unsafe { Box::from_raw(expr_ptr) }.inner;

        let breaks = if breaks_ptr.is_null() || breaks_len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(breaks_ptr, breaks_len) }.to_vec()
        };

        let labels = unsafe { ptr_to_opt_pl_str_vec(labels_ptr, labels_len) };

        let out_expr = expr.cut(breaks, labels, left_closed, include_breaks);

        Ok(Box::into_raw(Box::new(ExprContext { inner: out_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_qcut(
    expr_ptr: *mut ExprContext,
    probs_ptr: *const f64,
    probs_len: usize,
    labels_ptr: *const *const c_char,
    labels_len: usize,
    left_closed: bool,
    allow_duplicates: bool,
    include_breaks: bool,
) -> *mut ExprContext {
    ffi_try!({
        let expr = unsafe { Box::from_raw(expr_ptr) }.inner;

        let probs = if probs_ptr.is_null() || probs_len == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(probs_ptr, probs_len) }.to_vec()
        };

        let labels = unsafe { ptr_to_opt_pl_str_vec(labels_ptr, labels_len) };

        let out_expr = expr.qcut(probs, labels, left_closed, allow_duplicates, include_breaks);

        Ok(Box::into_raw(Box::new(ExprContext { inner: out_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_qcut_uniform(
    expr_ptr: *mut ExprContext,
    n_bins: usize,
    labels_ptr: *const *const c_char,
    labels_len: usize,
    left_closed: bool,
    allow_duplicates: bool,
    include_breaks: bool,
) -> *mut ExprContext {
    ffi_try!({
        let expr = unsafe { Box::from_raw(expr_ptr) }.inner;

        let labels = unsafe { ptr_to_opt_pl_str_vec(labels_ptr, labels_len) };

        let out_expr = expr.qcut_uniform(n_bins, labels, left_closed, allow_duplicates, include_breaks);

        Ok(Box::into_raw(Box::new(ExprContext { inner: out_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_cumulative_eval(
    expr_ptr: *mut ExprContext,
    eval_ptr: *mut ExprContext,
    min_samples: usize
) -> *mut ExprContext {
    ffi_try!({
        let expr = unsafe { Box::from_raw(expr_ptr) }.inner;
        let eval = unsafe { Box::from_raw(eval_ptr) }.inner;

        let out_expr = expr.cumulative_eval(eval,min_samples);

        Ok(Box::into_raw(Box::new(ExprContext { inner: out_expr })))
    })
}



#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_ext_to(
    expr_ptr: *mut ExprContext,
    dtype_ptr: *mut DataTypeExprContext,
) -> *mut ExprContext {
    ffi_try!({
        let expr = unsafe { Box::from_raw(expr_ptr) }.inner;
        let dtype = unsafe { Box::from_raw(dtype_ptr) };        
        let out_expr = expr.ext().to(dtype.inner);
        Ok(Box::into_raw(Box::new(ExprContext { inner: out_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_ext_storage(
    expr_ptr: *mut ExprContext,
) -> *mut ExprContext {
    ffi_try!({
        let expr = unsafe { Box::from_raw(expr_ptr) }.inner;
        
        let out_expr = expr.ext().storage();
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: out_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_hist(
    expr_ptr: *mut ExprContext,
    bins_ptr: *mut ExprContext,
    has_bin_count: bool,
    bin_count: usize,
    include_category: bool,
    include_breakpoint: bool,
) -> *mut ExprContext {
    ffi_try!({
        let expr = unsafe { Box::from_raw(expr_ptr) }.inner;

        let bins_opt = if bins_ptr.is_null() {
            None
        } else {
            Some(unsafe { Box::from_raw(bins_ptr) }.inner)
        };

        let bin_count_opt = if has_bin_count {
            Some(bin_count)
        } else {
            None
        };

        let out_expr = expr.hist(
            bins_opt,
            bin_count_opt,
            include_category,
            include_breakpoint,
        );

        Ok(Box::into_raw(Box::new(ExprContext { inner: out_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_clip(
    expr_ptr: *mut ExprContext,
    min_ptr: *mut ExprContext,
    max_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let expr = unsafe { Box::from_raw(expr_ptr) }.inner;
        let max = unsafe { Box::from_raw(max_ptr) }.inner;
        let min = unsafe { Box::from_raw(min_ptr) }.inner;

        let out_expr = expr.clip(min,max);

        Ok(Box::into_raw(Box::new(ExprContext { inner: out_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_inspect(
    ptr: *mut ExprContext,
    fmt_ptr: *const c_char,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        
        let fmt = if fmt_ptr.is_null() { 
            "{}".to_string() 
        } else { 
            unsafe { CStr::from_ptr(fmt_ptr).to_string_lossy().into_owned() } 
        };

        let output_type = |_: &Schema, f: &Field| Ok(f.clone());

        let new_expr = ctx.inner.clone().map_with_fmt_str(
            move |col: Column| {
                let s_str = format!("{}", col.as_materialized_series());
                
                let out_msg = if fmt.contains("{}") {
                    fmt.replace("{}", &s_str)
                } else {
                    format!("{} {}", fmt, s_str)
                };

                println!("{}", out_msg);
                
                Ok(col)
            },
            output_type,
            "inspect", 
        );

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}