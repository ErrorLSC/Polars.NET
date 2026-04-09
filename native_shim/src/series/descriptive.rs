use std::ffi::{CStr, c_char, c_int};
use polars::prelude::*;


use crate::types::{DataFrameContext, SeriesContext};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_series_chunk_lengths(
    ptr: *mut SeriesContext, 
    out_ptr: *mut usize
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*ptr };
        let lengths = ctx.series.chunk_lengths();
        
        for (i, len) in lengths.enumerate() {
            unsafe {
                *out_ptr.add(i) = len;
            }
        }
        
        Ok(())
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_series_chunk_count(ptr: *mut SeriesContext,out_count: *mut u32) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*ptr };

        let count = ctx.series.n_chunks();

        unsafe { *out_count = count as u32 };

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_estimated_size(ptr: *mut SeriesContext,out_size: *mut usize) -> bool {
    ffi_bool_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "DataFrame pointer is null");
        }
        let ctx = unsafe { &*ptr };
        
        unsafe { *out_size = ctx.series.estimated_size() };
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_series_has_nulls(
    ptr: *mut SeriesContext, 
    out_has_nulls: *mut bool
) -> c_int {
    ffi_try_c_int!({
        let ctx = unsafe { &*ptr };
        
        unsafe {
            *out_has_nulls = ctx.series.has_nulls();
        }
        
        Ok(0)
    })
}

macro_rules! impl_series_ops_bool {
    ($ffi_name:ident, $op_name:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $ffi_name(ptr: *mut SeriesContext) -> *mut SeriesContext {
            ffi_try!({
                let ctx = unsafe { &*ptr };
                
                let res = polars_ops::series::$op_name(&ctx.series)?.into_series();
                
                Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
            })
        }
    };
}

impl_series_ops_bool!(pl_series_is_first_distinct, is_first_distinct);
impl_series_ops_bool!(pl_series_is_last_distinct, is_last_distinct);
impl_series_ops_bool!(pl_series_is_duplicated, is_duplicated);
impl_series_ops_bool!(pl_series_is_unique, is_unique);

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
pub extern "C" fn pl_series_null_count(s_ptr: *mut SeriesContext,out_count: *mut usize) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*s_ptr };

        let count = ctx.series.null_count();
        
        unsafe { *out_count = count };
            
        Ok(())
    })
}

/// Check if the Series is actually sorted according to the given rules.
/// Returns 0 on success, 1 on panic/error.
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_sorted(
    series_ptr: *mut SeriesContext,
    descending: bool,
    nulls_last: bool, // for polars 0.54
    out_result: *mut bool,
) -> c_int {
    ffi_try_c_int!({
        let ctx = unsafe { &*series_ptr };
        
        let opts = polars::prelude::SortOptions {
            descending,
            nulls_last,
            ..Default::default()
        };

        let is_sorted = ctx.series.is_sorted(opts)?;

        if !out_result.is_null() {
            unsafe { *out_result = is_sorted };
        }
        
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_value_counts(
    s_ptr: *mut SeriesContext,
    sort: bool,
    parallel: bool,
    name: *const c_char,
    normalize: bool,
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        
        let c_str = unsafe { CStr::from_ptr(name) };
        let name_str = c_str.to_str().map_err(|e| polars_err!(ComputeError: "Series name contains null byte: {}", e))?;
        let pl_name = PlSmallStr::from_str(name_str); 

        let df = ctx.series.value_counts(sort, parallel, pl_name, normalize)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_len(ptr: *mut SeriesContext,out_count: *mut u32) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &*ptr };

        let count = ctx.series.len();

        unsafe { *out_count = count as u32 };
        
        Ok(())
    })
}


macro_rules! impl_series_unary_bool_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
            ffi_try!({
                let s = unsafe { &(*s_ptr).series }; 
                let res = s.$method()?.into_series();
                Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
            })
        }
    };
}

impl_series_unary_bool_op!(pl_series_is_nan, is_nan);
impl_series_unary_bool_op!(pl_series_is_not_nan, is_not_nan);
impl_series_unary_bool_op!(pl_series_is_finite, is_finite);
impl_series_unary_bool_op!(pl_series_is_infinite, is_infinite);

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_in(
    ptr: *mut SeriesContext,
    other_ptr: *mut SeriesContext,
    nulls_equal: bool,
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let other_ctx = unsafe { &*other_ptr };
        
        let res = polars::prelude::is_in(&ctx.series, &other_ctx.series, nulls_equal)?.into_series();
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

