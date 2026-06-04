use polars::prelude::*;
use std::ffi::{CStr};
use std::os::raw::c_char;
use crate::datatypes::parse_timeunit;
use crate::{gen_namespace_unary, impl_expr_namespace_expr_arg};
use crate::types::ExprContext;
use crate::utils::{ptr_to_str};

gen_namespace_unary!(pl_expr_dt_millennium, dt, millennium);
gen_namespace_unary!(pl_expr_dt_century, dt, century);
gen_namespace_unary!(pl_expr_dt_iso_year, dt, iso_year);
gen_namespace_unary!(pl_expr_dt_year, dt, year);
gen_namespace_unary!(pl_expr_dt_month, dt, month);
gen_namespace_unary!(pl_expr_dt_quarter, dt, quarter);
gen_namespace_unary!(pl_expr_dt_day, dt, day);
gen_namespace_unary!(pl_expr_dt_days_in_month, dt, days_in_month);
gen_namespace_unary!(pl_expr_dt_ordinal_day, dt, ordinal_day);
gen_namespace_unary!(pl_expr_dt_weekday, dt, weekday);
gen_namespace_unary!(pl_expr_dt_hour, dt, hour);
gen_namespace_unary!(pl_expr_dt_minute, dt, minute);
gen_namespace_unary!(pl_expr_dt_second, dt, second);
gen_namespace_unary!(pl_expr_dt_millisecond, dt, millisecond);
gen_namespace_unary!(pl_expr_dt_microsecond, dt, microsecond);
gen_namespace_unary!(pl_expr_dt_nanosecond, dt, nanosecond);
gen_namespace_unary!(pl_expr_dt_is_leap_year, dt, is_leap_year);
gen_namespace_unary!(pl_expr_dt_month_start, dt, month_start);
gen_namespace_unary!(pl_expr_dt_month_end, dt, month_end);
gen_namespace_unary!(pl_expr_dt_base_utc_offset, dt, base_utc_offset);
gen_namespace_unary!(pl_expr_dt_dst_offset, dt, dst_offset);
gen_namespace_unary!(pl_expr_dt_datetime, dt, datetime);
gen_namespace_unary!(pl_expr_dt_date, dt, date); // convert to Date
gen_namespace_unary!(pl_expr_dt_time, dt, time); // convert to Time 

impl_expr_namespace_expr_arg!(pl_expr_dt_truncate,dt,truncate);
impl_expr_namespace_expr_arg!(pl_expr_dt_round,dt,round);
impl_expr_namespace_expr_arg!(pl_expr_dt_offset_by,dt,offset_by);

macro_rules! impl_expr_dt_bool_arg {
    ($ffi_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $ffi_name(
            expr_ptr: *mut ExprContext,
            flag: bool
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let new_expr = ctx.inner.dt().$method(flag);
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

impl_expr_dt_bool_arg!(pl_expr_dt_total_days,total_days);
impl_expr_dt_bool_arg!(pl_expr_dt_total_hours,total_hours);
impl_expr_dt_bool_arg!(pl_expr_dt_total_minutes,total_minutes);
impl_expr_dt_bool_arg!(pl_expr_dt_total_seconds,total_seconds);
impl_expr_dt_bool_arg!(pl_expr_dt_total_milliseconds,total_milliseconds);
impl_expr_dt_bool_arg!(pl_expr_dt_total_microseconds,total_microseconds);
impl_expr_dt_bool_arg!(pl_expr_dt_total_nanoseconds,total_nanoseconds);


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
pub extern "C" fn pl_expr_dt_cast_time_unit(
    expr_ptr: *mut ExprContext, 
    tu: u8
) -> *mut ExprContext {
    let expr = unsafe { Box::from_raw(expr_ptr)};
    let time_unit = parse_timeunit(tu);

    let new_expr = expr.inner.dt().cast_time_unit(time_unit);

    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_with_time_unit(
    expr_ptr: *mut ExprContext, 
    tu: u8
) -> *mut ExprContext {
    let expr = unsafe { Box::from_raw(expr_ptr)};
    let time_unit = parse_timeunit(tu);

    let new_expr = expr.inner.dt().with_time_unit(time_unit);

    Box::into_raw(Box::new(ExprContext { inner: new_expr }))
}


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
    ambiguous_ptr: *mut ExprContext,   // Ambiguous (Expr string, e.g. "raise")
    non_existent_code: u8 // NonExistent (Enum string, e.g. "raise")
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let ambiguous = unsafe { Box::from_raw(ambiguous_ptr) };
        // Build Option<TimeZone>
        let tz = if tz_ptr.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(tz_ptr).to_string_lossy() };
            unsafe { Some(TimeZone::new_unchecked(s.as_ref()as &str)) }
        };

        let non_existent = match non_existent_code {
            0 => NonExistent::Raise,
            1 => NonExistent::Null,
            _ => NonExistent::Raise
        };

        let new_expr = ctx.inner.dt().replace_time_zone(tz, ambiguous.inner, non_existent);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_add_business_days(
    expr_ptr: *mut ExprContext,
    n_ptr: *mut ExprContext,
    week_mask_ptr: *const u8, 
    holidays_ptr: *mut ExprContext, 
    roll_strategy: u8          
) -> *mut ExprContext {
    ffi_try!({
        let e = unsafe { Box::from_raw(expr_ptr) };
        let n = unsafe { Box::from_raw(n_ptr) };
        let h = unsafe { Box::from_raw(holidays_ptr) };
        
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
            h.inner,
            roll
        );

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_is_business_day(
    expr_ptr: *mut ExprContext,
    week_mask_ptr: *const u8,
    holidays_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let e = unsafe { Box::from_raw(expr_ptr) };
        let h = unsafe { Box::from_raw(holidays_ptr) };
        let week_mask = unsafe {
        let slice = std::slice::from_raw_parts(week_mask_ptr, 7);
        let mut arr = [false; 7];
            for i in 0..7 {
                arr[i] = slice[i] != 0; 
            }
            arr
        };

        let new_expr = e.inner.dt().is_business_day(
            week_mask,
            h.inner
        );

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dt_replace(
    expr_ptr: *mut ExprContext,
    year_ptr: *mut ExprContext,
    month_ptr: *mut ExprContext,
    day_ptr: *mut ExprContext,
    hour_ptr: *mut ExprContext,
    minute_ptr: *mut ExprContext,
    second_ptr: *mut ExprContext,
    microsecond_ptr: *mut ExprContext,
    ambiguous_ptr: *mut ExprContext,
) -> *mut ExprContext {
    ffi_try!({
        let expr = unsafe { Box::from_raw(expr_ptr) };
        let year = unsafe { Box::from_raw(year_ptr) };
        let month = unsafe { Box::from_raw(month_ptr) };
        let day = unsafe { Box::from_raw(day_ptr) };
        let hour = unsafe { Box::from_raw(hour_ptr) };
        let minute = unsafe { Box::from_raw(minute_ptr) };
        let second = unsafe { Box::from_raw(second_ptr) };
        let microsecond = unsafe { Box::from_raw(microsecond_ptr) };
        let ambiguous = unsafe { Box::from_raw(ambiguous_ptr) };

        let new_expr = expr.inner.dt().replace(
            year.inner,
            month.inner,
            day.inner,
            hour.inner,
            minute.inner,
            second.inner,
            microsecond.inner,
            ambiguous.inner,
        );

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_business_day_count(
    start_ptr: *mut ExprContext,
    end_ptr:*mut ExprContext,
    week_mask_ptr: *const u8,
    holidays_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        unsafe {
        let start =  Box::from_raw(start_ptr);
        let end =  Box::from_raw(end_ptr);
        let week_mask = {
            let slice = std::slice::from_raw_parts(week_mask_ptr, 7);
            let mut arr = [false; 7];
                for i in 0..7 {
                    arr[i] = slice[i] != 0; 
                }
                arr
            };

        let h = Box::from_raw(holidays_ptr) ;

        let new_expr = polars_plan::dsl::functions::business_day_count(start.inner, end.inner, week_mask, h.inner);

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))}
    })
}