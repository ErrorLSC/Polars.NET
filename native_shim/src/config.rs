use std::ffi::{CString, c_int};
use std::os::raw::c_char;
use polars_config::config;
use polars_core::fmt::*;
use polars::prelude::*;
use crate::utils::{ptr_to_str_unchecked};

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_set_env_var(key_ptr: *const c_char, value_ptr: *const c_char) {
    ffi_try_void!({
        if key_ptr.is_null() {
            return Ok(());
        }
        let key = unsafe { ptr_to_str_unchecked(key_ptr) }
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        if value_ptr.is_null() {
            unsafe { std::env::remove_var(key); }
        } else {
            let value = unsafe { ptr_to_str_unchecked(value_ptr) }
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                
            unsafe { std::env::set_var(key, value); }
        }

        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_config_reload_var(key_ptr: *const c_char) {
    ffi_try_void!({
        if key_ptr.is_null() { 
            return Ok(()); 
        }
        
        let key = unsafe { ptr_to_str_unchecked(key_ptr) }
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            
        config().reload_env_var(key);
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_reload_all() {
    ffi_try_void!({
        config().reload_env_vars();
        Ok(())
    });
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_max_threads(out_threads: *mut u64) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_threads = config().max_threads() as u64;}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_verbose(out_verbose:*mut bool) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_verbose = config().verbose();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_warn_unstable(out_verbose:*mut bool) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_verbose = config().warn_unstable();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_ideal_morsel_size(out_size:*mut u64) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_size = config().ideal_morsel_size();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_engine_affinity(out_engine: *mut u8) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_engine = config().engine_affinity() as u8;}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_parquet_binary_statistics_truncate_length(out_size:*mut u64) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_size = config().parquet_binary_statistics_truncate_length();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_prune_parquet_metadata(out_active:*mut bool) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_active = config().prune_parquet_metadata();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_allow_nested_cspe(out_active:*mut bool) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_active = config().allow_nested_cspe();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_verbose_sensitive(out_active:*mut bool) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_active = config().verbose_sensitive();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_force_async(out_active:*mut bool) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_active = config().force_async();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_import_interval_as_struct(out_active:*mut bool) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_active = config().import_interval_as_struct();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_ooc_drift_threshold(out_threshold:*mut u64) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_threshold = config().ooc_drift_threshold();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_resolve_metadata_level(out_level: *mut u8) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_level = config().resolve_metadata_level() as u8;}
        Ok(0)
    })
}

// #[unsafe(no_mangle)]
// pub extern "C" fn pl_config_get_ooc_spill_policy(out_policy: *mut u8) -> c_int {
//     ffi_try_c_int!({
//         unsafe{ *out_policy = config().ooc_spill_policy() as u8;}
//         Ok(0)
//     })
// }

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_ooc_spill_format(out_format: *mut u8) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_format = config().ooc_spill_format() as u8;}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_ooc_memory_budget_fraction(out_fraction:*mut f64) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_fraction = config().ooc_memory_budget_fraction();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_ooc_spill_min_bytes(out_threshold:*mut u64) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_threshold = config().ooc_spill_min_bytes();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_config_get_ooc_spill_dir() -> *mut c_char {
    ffi_try!({
        let path = config().ooc_spill_dir();
        
        let path_str = path.to_string_lossy().into_owned();
        
        let c_str = CString::new(path_str).unwrap();
        Ok(c_str.into_raw())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_join_sample_limit(out_threshold:*mut u64) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_threshold = config().join_sample_limit();}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_projection_pushdown_prune_strict_hconcat_inputs(out_active:*mut bool) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_active = config().projection_pushdown_prune_strict_hconcat_inputs();}
        Ok(0)
    })
}


// 1. Float Format (0 = Mixed, 1 = Full)
#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_float_fmt(out_fmt:*mut u8) -> c_int {
    ffi_try_c_int!({
        unsafe{ *out_fmt = get_float_fmt() as u8;}
        Ok(0)
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_config_set_float_fmt(fmt: u8) {
    ffi_try_void!({
        let mode = match fmt {
            1 => FloatFmt::Full,
            _ => FloatFmt::Mixed,
        };
        set_float_fmt(mode);
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_get_float_precision(out_precision:*mut i64) -> c_int {
    ffi_try_c_int!({
        let result = match get_float_precision() {
            Some(p) => p as i64,
            None => -1,
        };
        unsafe {*out_precision = result;}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_set_float_precision(precision: i64) {
    ffi_try_void!({
        if precision < 0 {
            set_float_precision(None);
        } else {
            set_float_precision(Some(precision as usize));
        }
        Ok(())
    })
}

// 3. Separators
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_config_get_decimal_separator() -> *mut c_char {
    ffi_try!({
        let dec_char = get_decimal_separator(); 
        let dec_str = dec_char.to_string(); 
        
        let c_str = CString::new(dec_str).unwrap();
        Ok(c_str.into_raw())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_config_set_decimal_separator(dec_ptr: *const c_char) {
    ffi_try_void!({
        if dec_ptr.is_null() {
            set_decimal_separator(None);
            return Ok(());
        }
        let dec_str = unsafe { ptr_to_str_unchecked(dec_ptr) }
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        if let Some(dec_char) = dec_str.chars().next() {
            set_decimal_separator(Some(dec_char));
        } else {
            set_decimal_separator(None);
        }

        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_config_get_thousands_separator() -> *mut c_char {
    ffi_try!({
        let s = get_thousands_separator(); 
        
        let c_str = CString::new(s).unwrap();
        Ok(c_str.into_raw())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_config_set_thousands_separator(sep_ptr: *const c_char) {
    ffi_try_void!({
        if sep_ptr.is_null() {
            set_thousands_separator(None);
            return Ok(());
        }

        let sep_str = unsafe { ptr_to_str_unchecked(sep_ptr) }
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        if let Some(sep_char) = sep_str.chars().next() {
            set_thousands_separator(Some(sep_char));
        } else {
            set_thousands_separator(None);
        }

        Ok(())
    });
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_config_get_trim_decimal_zeros(out_trim:*mut bool) -> c_int {
    ffi_try_c_int!({
        let result = get_trim_decimal_zeros(); 
        unsafe {*out_trim=result;}
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_config_set_trim_decimal_zeros(trim_value: bool, has_value: bool) {
    ffi_try_void!({
        if has_value {
            set_trim_decimal_zeros(Some(trim_value));
        } else {
            set_trim_decimal_zeros(None);
        }
        Ok(())
    });
}

// #[unsafe(no_mangle)]
// pub extern "C" fn pl_fmt_duration_string(v: i64, time_unit_code: u8) -> *mut c_char {
//     ffi_try!({
//         let unit = parse_time_unit(time_unit_code)
//             .ok_or_else(|| PolarsError::ComputeError(format!("Invalid TimeUnit code: {}", time_unit_code).into()))?;
        
//         let mut buf = String::new();
//         fmt_duration_string(&mut buf, v, unit)
//             .map_err(|e| PolarsError::ComputeError(format!("Duration formatting failed: {}", e).into()))?;

//         let c_str = CString::new(buf)
//             .map_err(|e| PolarsError::ComputeError(format!("CString creation failed: {}", e).into()))?;
            
//         Ok(c_str.into_raw())
//     })
// }

// #[unsafe(no_mangle)]
// pub extern "C" fn pl_iso_duration_string(v: i64, time_unit_code: u8) -> *mut c_char {
//     ffi_try!({
//         let unit = parse_time_unit(time_unit_code)
//             .ok_or_else(|| PolarsError::ComputeError(format!("Invalid TimeUnit code: {}", time_unit_code).into()))?;
        
//         let mut buf = String::new();
//         iso_duration_string(&mut buf, v, unit);

//         let c_str = CString::new(buf)
//             .map_err(|e| PolarsError::ComputeError(format!("CString creation failed: {}", e).into()))?;
            
//         Ok(c_str.into_raw())
//     })
// }