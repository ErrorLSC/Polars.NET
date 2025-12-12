use polars::prelude::*;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use crate::utils::*;
use crate::datatypes::DataTypeContext;

// 包装结构体
pub struct SeriesContext {
    pub series: Series,
}

// ==========================================
// Constructors 
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_i32(
    name: *const c_char, 
    ptr: *const i32, 
    validity: *const bool, 
    len: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
        
        let series = if validity.is_null() {
            Series::new(name.into(), slice)
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, len) };
            let opts: Vec<Option<i32>> = slice.iter().zip(v_slice.iter())
                .map(|(&v, &valid)| if valid { Some(v) } else { None })
                .collect();
            Series::new(name.into(), &opts)
        };

        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_i64(
    name: *const c_char, 
    ptr: *const i64, 
    validity: *const bool, 
    len: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };

        let series = if validity.is_null() {
            Series::new(name.into(), slice)
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, len) };
            let opts: Vec<Option<i64>> = slice.iter().zip(v_slice.iter())
                .map(|(&v, &valid)| if valid { Some(v) } else { None })
                .collect();
            Series::new(name.into(), &opts)
        };

        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_f64(
    name: *const c_char, 
    ptr: *const f64, 
    validity: *const bool, 
    len: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };

        let series = if validity.is_null() {
            Series::new(name.into(), slice)
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, len) };
            let opts: Vec<Option<f64>> = slice.iter().zip(v_slice.iter())
                .map(|(&v, &valid)| if valid { Some(v) } else { None })
                .collect();
            Series::new(name.into(), &opts)
        };

        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_bool(
    name: *const c_char, 
    ptr: *const bool, 
    validity: *const bool, 
    len: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };

        let series = if validity.is_null() {
            Series::new(name.into(), slice)
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, len) };
            let opts: Vec<Option<bool>> = slice.iter().zip(v_slice.iter())
                .map(|(&v, &valid)| if valid { Some(v) } else { None })
                .collect();
            Series::new(name.into(), &opts)
        };

        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_str(
    name: *const c_char, 
    strs: *const *const c_char, 
    len: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let slice = unsafe { std::slice::from_raw_parts(strs, len) };
        
        let vec_opts: Vec<Option<&str>> = slice.iter()
            .map(|&p| {
                if p.is_null() {
                    None 
                } else {
                    unsafe { Some(CStr::from_ptr(p).to_str().unwrap_or("")) }
                }
            })
            .collect();

        let series = Series::new(name.into(), &vec_opts);
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_decimal(
    name: *const c_char,
    ptr: *const i128,
    validity: *const bool,
    len: usize,
    scale: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
        let series = if validity.is_null() {
            Series::new(name.clone().into(), slice)
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, len) };
            let opts: Vec<Option<i128>> = slice.iter().zip(v_slice.iter())
                .map(|(&v, &valid)| if valid { Some(v) } else { None })
                .collect();
            Series::new(name.clone().into(), &opts)
        };

        // 处理 Result
        let decimal_series = series
            .i128()
            .map_err(|_| PolarsError::ComputeError("Failed to cast to i128 for decimal creation".into()))?
            .clone()
            .into_decimal(None, scale)
            .map_err(|e| PolarsError::ComputeError(format!("Decimal creation failed: {}", e).into()))?
            .into_series();

        Ok(Box::into_raw(Box::new(SeriesContext { series: decimal_series })))
    })
}

// ==========================================
// Methods
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_free(ptr: *mut SeriesContext) {
    if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
    }
}

// len 和 name 通常不会 panic，不包也可以，包了更安全
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_len(ptr: *mut SeriesContext) -> usize {
    let ctx = unsafe { &*ptr };
    ctx.series.len()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_name(ptr: *mut SeriesContext) -> *mut c_char {
    let ctx = unsafe { &*ptr };
    CString::new(ctx.series.name().as_str()).unwrap().into_raw()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_rename(ptr: *mut SeriesContext, name: *const c_char) {
    let ctx = unsafe { &mut *ptr };
    let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
    ctx.series.rename(name_str.into());
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_dtype_str(s_ptr: *mut SeriesContext) -> *mut c_char {
    let ctx = unsafe { &*s_ptr };
    let dtype_str = ctx.series.dtype().to_string();
    CString::new(dtype_str).unwrap().into_raw()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_to_arrow(ptr: *mut SeriesContext) -> *mut ArrowArrayContext {
    // 这里涉及 rechunk 和 to_arrow，建议包裹
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let contiguous_series = ctx.series.rechunk();
        let arr = contiguous_series.to_arrow(0, CompatLevel::newest());
        Ok(Box::into_raw(Box::new(ArrowArrayContext { array: arr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_cast(
    ptr: *mut SeriesContext, 
    dtype_ptr: *mut DataTypeContext
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let target_dtype = unsafe { &(*dtype_ptr).dtype };
        
        let s = ctx.series.cast(target_dtype)?;
        Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_null(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let series = ctx.series.is_null().into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_not_null(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let series = ctx.series.is_not_null().into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_null_at(s_ptr: *mut SeriesContext, idx: usize) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { 
        return false; // 越界不算 Null，算无效
    }
    match ctx.series.get(idx) {
        Ok(AnyValue::Null) => true,
        _ => false // 有值（包括错误，但 get 已经通过 len 检查了）
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_null_count(s_ptr: *mut SeriesContext) -> usize {
    let ctx = unsafe { &*s_ptr };
    ctx.series.null_count()
}
// --- Scalar Access ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_i64(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut i64) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }

    // 使用 get(i) 获取 AnyValue
    match ctx.series.get(idx) {
        Ok(AnyValue::Int64(v)) => { unsafe { *out_val = v }; true }
        Ok(AnyValue::Int32(v)) => { unsafe { *out_val = v as i64 }; true }
        Ok(AnyValue::Int16(v)) => { unsafe { *out_val = v as i64 }; true }
        Ok(AnyValue::Int8(v)) => { unsafe { *out_val = v as i64 }; true }
        Ok(AnyValue::UInt64(v)) => { unsafe { *out_val = v as i64 }; true } // 注意溢出风险，但通常 ok
        Ok(AnyValue::UInt32(v)) => { unsafe { *out_val = v as i64 }; true }
        _ => false // Null or type mismatch
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_f64(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut f64) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }

    match ctx.series.get(idx) {
        Ok(AnyValue::Float64(v)) => { unsafe { *out_val = v }; true }
        Ok(AnyValue::Float32(v)) => { unsafe { *out_val = v as f64 }; true }
        _ => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_bool(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut bool) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }

    match ctx.series.get(idx) {
        Ok(AnyValue::Boolean(v)) => { unsafe { *out_val = v }; true }
        _ => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_str(s_ptr: *mut SeriesContext, idx: usize) -> *mut c_char {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return std::ptr::null_mut(); }

    match ctx.series.get(idx) {
        // String / StringView 统一处理
        Ok(AnyValue::String(s)) => CString::new(s).unwrap().into_raw(),
        _ => std::ptr::null_mut()
    }
}

// [新增] Decimal 支持
// 返回值：true=成功, false=null/fail
// out_val: 写入 i128 值
// out_scale: 写入 scale (因为 AnyValue 包含 scale)
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_decimal(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut i128, out_scale: *mut usize) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }

    match ctx.series.get(idx) {
        Ok(AnyValue::Decimal(v, scale)) => { 
            unsafe { 
                *out_val = v; 
                *out_scale = scale;
            } 
            true 
        }
        _ => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_date(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut i32) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }
    match ctx.series.get(idx) {
        Ok(AnyValue::Date(v)) => { unsafe { *out_val = v }; true }
        _ => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_time(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut i64) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }
    match ctx.series.get(idx) {
        Ok(AnyValue::Time(v)) => { unsafe { *out_val = v }; true } // Nanoseconds
        _ => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_datetime(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut i64) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }
    match ctx.series.get(idx) {
        // Datetime(val, unit, timezone)
        // 我们这里只取 val。通常 Polars 默认是 Microseconds (us)。
        // 严谨的做法应该转换单位，但这里为了性能直接返回物理值，C# 端按 Microseconds 处理。
        Ok(AnyValue::Datetime(v, _, _)) => { unsafe { *out_val = v }; true }
        _ => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_duration(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut i64) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }
    match ctx.series.get(idx) {
        Ok(AnyValue::Duration(v, _)) => { unsafe { *out_val = v }; true }
        _ => false
    }
}

// ==========================================
// Arithmetic Ops (High Risk Area!)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_add(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        // 运算符重载可能会 panic (例如形状极度不匹配且无法广播)
        let res = s1 + s2; 
        Ok(Box::into_raw(Box::new(SeriesContext { series: res? })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_sub(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1 - s2;
        Ok(Box::into_raw(Box::new(SeriesContext { series: res? })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_mul(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1 * s2;
        Ok(Box::into_raw(Box::new(SeriesContext { series: res? })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_div(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1 / s2;
        Ok(Box::into_raw(Box::new(SeriesContext { series: res? })))
    })
}

// ==========================================
// Comparison Ops (High Risk: Removed unwrap)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_eq(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        // [修复] 去掉 unwrap(), 使用 ? 传播错误
        let res = s1.equal(s2).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_neq(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1.not_equal(s2).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_gt(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1.gt(s2).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_gt_eq(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1.gt_eq(s2).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_lt(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1.lt(s2).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_lt_eq(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1.lt_eq(s2).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}
// ==========================================
// Aggregations
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_sum(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &(*s_ptr).series };
        // [修复] 使用 sum_reduce() 获取 Scalar，再转回 Series
        let res = s.sum_reduce()?.into_series(s.name().clone());
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_mean(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &(*s_ptr).series };
        let mean_val = s.mean();
        let res = Series::new(s.name().clone(), &[mean_val]);
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_min(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &(*s_ptr).series };
        
        // 1. 使用 min_reduce 获取 Scalar 对象 (处理 Result)
        let scalar = s.min_reduce()?;
        
        // 2. 将 Scalar 转回 Series (需要传入列名)
        let res = scalar.into_series(s.name().clone());
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_max(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &(*s_ptr).series };
        
        // 1. 使用 max_reduce 获取 Scalar
        let scalar = s.max_reduce()?;
        
        // 2. 将 Scalar 转回 Series
        let res = scalar.into_series(s.name().clone());
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_nan(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        // is_nan() 返回 Result<BooleanChunked> -> ? 解包 -> into_series()
        let res = ctx.series.is_nan()?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_not_nan(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let res = ctx.series.is_not_nan()?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_finite(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let res = ctx.series.is_finite()?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_infinite(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let res = ctx.series.is_infinite()?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}