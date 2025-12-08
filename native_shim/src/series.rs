use polars::prelude::*;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use crate::utils::*;

// 包装结构体
pub struct SeriesContext {
    pub series: Series,
}

// ==========================================
// Constructors (支持 Null，无泛型魔法版)
// ==========================================

// --- Int32 ---
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_i32(
    name: *const c_char, 
    ptr: *const i32, 
    validity: *const bool, 
    len: usize
) -> *mut SeriesContext {
    let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    
    let series = if validity.is_null() {
        Series::new(name.into(), slice)
    } else {
        let v_slice = unsafe { std::slice::from_raw_parts(validity, len) };
        // 直接 zip 生成 Option<i32>，无需 ToPrimitive
        let opts: Vec<Option<i32>> = slice.iter().zip(v_slice.iter())
            .map(|(&v, &valid)| if valid { Some(v) } else { None })
            .collect();
        Series::new(name.into(), &opts)
    };

    Box::into_raw(Box::new(SeriesContext { series }))
}

// --- Int64 ---
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_i64(
    name: *const c_char, 
    ptr: *const i64, 
    validity: *const bool, 
    len: usize
) -> *mut SeriesContext {
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

    Box::into_raw(Box::new(SeriesContext { series }))
}

// --- Float64 ---
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_f64(
    name: *const c_char, 
    ptr: *const f64, 
    validity: *const bool, 
    len: usize
) -> *mut SeriesContext {
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

    Box::into_raw(Box::new(SeriesContext { series }))
}

// --- Boolean ---
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_bool(
    name: *const c_char, 
    ptr: *const bool, 
    validity: *const bool, 
    len: usize
) -> *mut SeriesContext {
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

    Box::into_raw(Box::new(SeriesContext { series }))
}

// --- String ---
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_str(
    name: *const c_char, 
    strs: *const *const c_char, 
    len: usize
) -> *mut SeriesContext {
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
    Box::into_raw(Box::new(SeriesContext { series }))
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

// [Series 转 Arrow]
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_to_arrow(ptr: *mut SeriesContext) -> *mut ArrowArrayContext {
    let ctx = unsafe { &*ptr };
    
    // 1. Rechunk: 保证物理上只有一块内存
    let contiguous_series = ctx.series.rechunk();

    // 2. 取出 chunks (ArrayRef = Box<dyn Array>)
    // Polars 的 to_arrow(0) 实际就是取第0个 chunk
    let arr = contiguous_series.to_arrow(0, CompatLevel::newest());
    
    Box::into_raw(Box::new(ArrowArrayContext { array: arr }))
}