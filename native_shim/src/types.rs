use polars::prelude::*;
use std::ffi::CStr;
use std::os::raw::c_char;
// ==========================================
// 1. 定义“不透明”容器
// ==========================================
// 这是一个壳，专门用来在 C# 和 Rust 之间传递 DataFrame 的所有权
pub struct DataFrameContext {
    pub df: DataFrame,
}
// 定义 Expr 的壳子
pub struct ExprContext {
    pub inner: Expr,
}
// 定义 LazyFrame 壳子
pub struct LazyFrameContext {
    pub inner: LazyFrame,
}
// 辅助函数
pub fn ptr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, std::str::Utf8Error> {
    if ptr.is_null() { 
        // 这里 panic 会被 ffi_try 捕获，虽然不建议 panic，但作为底层守卫可以接受
        panic!("Null pointer passed to ptr_to_str"); 
    }
    unsafe { CStr::from_ptr(ptr).to_str() }
}
/// 将 C 传递过来的 Expr 指针数组转换为 Rust 的 Vec<Expr>
/// 注意：这会消耗掉 C 端传递过来的 Expr 所有权 (Box::from_raw)
pub(crate) unsafe fn consume_exprs_array(
    ptr: *const *mut ExprContext, 
    len: usize
) -> Vec<Expr> {
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    slice.iter()
        .map(|&p| unsafe { Box::from_raw(p).inner }) // 拿走所有权
        .collect()
}

pub(crate) fn map_jointype(code: i32) -> JoinType {
    match code {
        0 => JoinType::Inner,
        1 => JoinType::Left,
        2 => JoinType::Full, // 对应 C# Outer
        3 => JoinType::Cross,
        4 => JoinType::Semi,
        5 => JoinType::Anti,
        _ => JoinType::Inner, // 默认
    }
}

pub(crate) fn map_datatype(code: i32) -> DataType {
    match code {
        1 => DataType::Boolean,
        2 => DataType::Int8,
        3 => DataType::Int16,
        4 => DataType::Int32,
        5 => DataType::Int64,
        6 => DataType::UInt8,
        7 => DataType::UInt16,
        8 => DataType::UInt32,
        9 => DataType::UInt64,
        10 => DataType::Float32,
        11 => DataType::Float64,
        12 => DataType::String,
        13 => DataType::Date,
        14 => DataType::Datetime(TimeUnit::Microseconds, None), // 默认无时区微秒
        15 => DataType::Time,
        16 => DataType::Duration(TimeUnit::Microseconds),
        17 => DataType::Binary,
        _ => DataType::Unknown(UnknownKind::Any), // 0 或未知
    }
}