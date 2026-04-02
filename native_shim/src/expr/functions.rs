use std::ffi::{CStr, c_char};
use polars::lazy::dsl;
use polars::prelude::*;

use crate::types::ExprContext;
use crate::utils::{parse_closed_interval, parse_closed_window, parse_time_unit};

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_int_range(
    start_ptr: *mut ExprContext,
    end_ptr: *mut ExprContext,
    step: i64,
    dtype_ptr: *mut DataType, 
) -> *mut ExprContext {
    ffi_try!({
        let start = unsafe { Box::from_raw(start_ptr) };
        let end = unsafe { Box::from_raw(end_ptr) };
        
        let dtype = unsafe { &*dtype_ptr }.clone();

        let new_expr = int_range(start.inner, end.inner, step, dtype);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_int_ranges(
    start_ptr: *mut ExprContext,
    end_ptr: *mut ExprContext,
    step_ptr: *mut ExprContext,
    dtype_ptr: *mut DataType, 
) -> *mut ExprContext {
    ffi_try!({
        let start = unsafe { Box::from_raw(start_ptr) };
        let end = unsafe { Box::from_raw(end_ptr) };
        let step = unsafe { Box::from_raw(step_ptr) };
        
        let dtype = unsafe { &*dtype_ptr }.clone();

        let new_expr = int_ranges(start.inner, end.inner, step.inner, dtype);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_date_range(
    start_ptr: *mut ExprContext,
    end_ptr: *mut ExprContext,
    interval_ptr: *const c_char,
    num_samples_ptr: *mut ExprContext,
    closed_val: u8,
) -> *mut ExprContext {
    ffi_try!({
        let start = if start_ptr.is_null() { None } else { Some(unsafe { Box::from_raw(start_ptr) }.inner) };
        let end = if end_ptr.is_null() { None } else { Some(unsafe { Box::from_raw(end_ptr) }.inner) };
        let num_samples = if num_samples_ptr.is_null() { None } else { Some(unsafe { Box::from_raw(num_samples_ptr) }.inner) };

        let interval = if interval_ptr.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(interval_ptr) }
                .to_str()
                .map_err(|_| polars_err!(ComputeError: "Invalid UTF-8 string passed for interval"))?;
            
            Some(polars::time::Duration::parse(s))
        };

        let closed = parse_closed_window(closed_val);

        let new_expr = dsl::date_range(start, end, interval, num_samples, closed)?;
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_date_ranges(
    start_ptr: *mut ExprContext,
    end_ptr: *mut ExprContext,
    interval_ptr: *const c_char,
    num_samples_ptr: *mut ExprContext,
    closed_val: u8,
) -> *mut ExprContext {
    ffi_try!({
        let start = if start_ptr.is_null() { None } else { Some(unsafe { Box::from_raw(start_ptr) }.inner) };
        let end = if end_ptr.is_null() { None } else { Some(unsafe { Box::from_raw(end_ptr) }.inner) };
        let num_samples = if num_samples_ptr.is_null() { None } else { Some(unsafe { Box::from_raw(num_samples_ptr) }.inner) };

        let interval = if interval_ptr.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(interval_ptr) }
                .to_str()
                .map_err(|_| polars_err!(ComputeError: "Invalid UTF-8 string passed for interval"))?;
            Some(polars::time::Duration::parse(s))
        };

        let closed = parse_closed_window(closed_val);

        let new_expr = dsl::date_ranges(start, end, interval, num_samples, closed)?;
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_datetime_range(
    start_ptr: *mut ExprContext,
    end_ptr: *mut ExprContext,
    interval_ptr: *const c_char,
    num_samples_ptr: *mut ExprContext,
    closed_val: u8,
    time_unit_val: u8,
    time_zone_ptr: *const c_char,
) -> *mut ExprContext {
    ffi_try!({
        let start = if start_ptr.is_null() { None } else { Some(unsafe { Box::from_raw(start_ptr) }.inner) };
        let end = if end_ptr.is_null() { None } else { Some(unsafe { Box::from_raw(end_ptr) }.inner) };
        let num_samples = if num_samples_ptr.is_null() { None } else { Some(unsafe { Box::from_raw(num_samples_ptr) }.inner) };

        let interval = if interval_ptr.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(interval_ptr) }
                .to_str()
                .map_err(|_| polars_err!(ComputeError: "Invalid UTF-8 string passed for interval"))?;
            Some(polars::time::Duration::parse(s))
        };

        let tz_str = if time_zone_ptr.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(time_zone_ptr) }
                .to_str()
                .map_err(|_| polars_err!(ComputeError: "Invalid UTF-8 string passed for time_zone"))?;
            Some(s)
        };
        let time_zone = TimeZone::opt_try_new(tz_str)?;

        let closed = parse_closed_window(closed_val);
        let time_unit = parse_time_unit(time_unit_val);

        let new_expr = dsl::datetime_range(
            start, 
            end, 
            interval, 
            num_samples, 
            closed, 
            time_unit, 
            time_zone
        )?;
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_datetime_ranges(
    start_ptr: *mut ExprContext,
    end_ptr: *mut ExprContext,
    interval_ptr: *const c_char,
    num_samples_ptr: *mut ExprContext,
    closed_val: u8,
    time_unit_val: u8,
    time_zone_ptr: *const c_char,
) -> *mut ExprContext {
    ffi_try!({
        let start = if start_ptr.is_null() { None } else { Some(unsafe { Box::from_raw(start_ptr) }.inner) };
        let end = if end_ptr.is_null() { None } else { Some(unsafe { Box::from_raw(end_ptr) }.inner) };
        let num_samples = if num_samples_ptr.is_null() { None } else { Some(unsafe { Box::from_raw(num_samples_ptr) }.inner) };

        let interval = if interval_ptr.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(interval_ptr) }
                .to_str()
                .map_err(|_| polars_err!(ComputeError: "Invalid UTF-8 string passed for interval"))?;
            Some(polars::time::Duration::parse(s))
        };

        let tz_str = if time_zone_ptr.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(time_zone_ptr) }
                .to_str()
                .map_err(|_| polars_err!(ComputeError: "Invalid UTF-8 string passed for time_zone"))?;
            Some(s)
        };
        let time_zone = TimeZone::opt_try_new(tz_str)?;

        let closed = parse_closed_window(closed_val);
        let time_unit = parse_time_unit(time_unit_val);

        let new_expr = dsl::datetime_ranges(
            start, 
            end, 
            interval, 
            num_samples, 
            closed, 
            time_unit, 
            time_zone
        )?;
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_time_range(
    start_ptr: *mut ExprContext,
    end_ptr: *mut ExprContext,
    interval_ptr: *const c_char,
    closed_val: u8,
) -> *mut ExprContext {
    ffi_try!({
        let start = unsafe { Box::from_raw(start_ptr) }.inner;
        let end = unsafe { Box::from_raw(end_ptr) }.inner;
        
        let s = unsafe { CStr::from_ptr(interval_ptr) }
            .to_str()
            .map_err(|_| polars_err!(ComputeError: "Invalid UTF-8 string passed for interval"))?;
        let interval = polars::time::Duration::parse(s);

        let closed = parse_closed_window(closed_val);

        let new_expr = dsl::time_range(start, end, interval, closed);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_time_ranges(
    start_ptr: *mut ExprContext,
    end_ptr: *mut ExprContext,
    interval_ptr: *const c_char,
    closed_val: u8,
) -> *mut ExprContext {
    ffi_try!({
        let start = unsafe { Box::from_raw(start_ptr) }.inner;
        let end = unsafe { Box::from_raw(end_ptr) }.inner;
        
        let s = unsafe { CStr::from_ptr(interval_ptr) }
            .to_str()
            .map_err(|_| polars_err!(ComputeError: "Invalid UTF-8 string passed for interval"))?;
        let interval = polars::time::Duration::parse(s);

        let closed = parse_closed_window(closed_val);

        let new_expr = dsl::time_ranges(start, end, interval, closed);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// =========================================================
// Linear Space
// =========================================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_linear_space(
    start_ptr: *mut ExprContext,
    end_ptr: *mut ExprContext,
    num_samples_ptr: *mut ExprContext,
    closed_val: u8,
) -> *mut ExprContext {
    ffi_try!({
        let start = unsafe { Box::from_raw(start_ptr) }.inner;
        let end = unsafe { Box::from_raw(end_ptr) }.inner;
        let num_samples = unsafe { Box::from_raw(num_samples_ptr) }.inner;
        
        let closed = parse_closed_interval(closed_val);

        let new_expr = dsl::linear_space(start, end, num_samples, closed);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_linear_spaces(
    start_ptr: *mut ExprContext,
    end_ptr: *mut ExprContext,
    num_samples_ptr: *mut ExprContext,
    closed_val: u8,
    as_array: bool,
) -> *mut ExprContext {
    ffi_try!({
        let start = unsafe { Box::from_raw(start_ptr) }.inner;
        let end = unsafe { Box::from_raw(end_ptr) }.inner;
        let num_samples = unsafe { Box::from_raw(num_samples_ptr) }.inner;
        
        let closed = parse_closed_interval(closed_val);

        let new_expr = dsl::linear_spaces(start, end, num_samples, closed, as_array)?;
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}