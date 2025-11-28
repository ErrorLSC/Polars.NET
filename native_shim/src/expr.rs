use polars::prelude::*;
use polars::prelude::ClosedInterval;
use std::os::raw::c_char;
use crate::types::{ExprContext, ptr_to_str};
use std::ops::{Add, Sub, Mul, Div, Rem};
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

// ==========================================
// 2. 宏应用区域 (Boilerplate 消灭术)
// ==========================================

// --- Group 1: 构造函数 ---
gen_lit_ctor!(pl_expr_lit_i32, i32);
gen_lit_ctor!(pl_expr_lit_f64, f64);

// --- Group 2: 字符串构造 ---
gen_str_ctor!(pl_expr_col, col);
gen_str_ctor!(pl_expr_lit_str, lit);

// --- Group 3: 一元操作 ---
gen_unary_op!(pl_expr_sum, sum);
gen_unary_op!(pl_expr_mean, mean);
gen_unary_op!(pl_expr_max, max);
gen_unary_op!(pl_expr_min, min);
// gen_unary_op!(pl_expr_abs, abs);
// 逻辑非 (!)
gen_unary_op!(pl_expr_not, not);
// is_null()
gen_unary_op!(pl_expr_is_null, is_null);
gen_unary_op!(pl_expr_is_not_null, is_not_null);

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
gen_binary_op!(pl_expr_rem, rem); // % (取余)
// 逻辑运算
gen_binary_op!(pl_expr_and, and); // &
gen_binary_op!(pl_expr_or, or);   // |
gen_binary_op!(pl_expr_xor, xor);
// Null Ops
gen_binary_op!(pl_expr_fill_null, fill_null);



// --- Group 5: 命名空间操作 ---
// dt 命名空间
gen_namespace_unary!(pl_expr_dt_year, dt, year);
gen_namespace_unary!(pl_expr_dt_month, dt, month);
// String Namespace
gen_namespace_unary!(pl_expr_str_to_uppercase, str, to_uppercase);
gen_namespace_unary!(pl_expr_str_to_lowercase, str, to_lowercase);
gen_namespace_unary!(pl_expr_str_len_bytes, str, len_bytes);

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

// 替换操作 (Replace All)
// pat: 匹配模式, val: 替换值
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_replace_all(
    expr_ptr: *mut ExprContext, 
    pat_ptr: *const c_char,
    val_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let pat = ptr_to_str(pat_ptr).unwrap();
        let val = ptr_to_str(val_ptr).unwrap();

        // literal=true 表示纯文本替换（非正则），通常是用户默认想要的
        let new_expr = ctx.inner.str().replace_all(lit(pat), lit(val), true);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
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