use polars::prelude::*;
use crate::types::SeriesContext;

macro_rules! impl_series_reduce_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
            ffi_try!({
                let s = unsafe { &(*s_ptr).series };
                let res = s.$method()?.into_series(s.name().clone());
                Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
            })
        }
    };
}

impl_series_reduce_op!(pl_series_sum, sum_reduce);
impl_series_reduce_op!(pl_series_min, min_reduce);
impl_series_reduce_op!(pl_series_max, max_reduce);
impl_series_reduce_op!(pl_series_mean, mean_reduce);

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_implode(ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let res = ctx.series.implode()?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}