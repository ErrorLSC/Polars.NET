use std::ffi::{CStr};
use std::os::raw::c_char;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_set_env_var(key: *const c_char, value: *const c_char) {
    if key.is_null() || value.is_null() {
        return;
    }

    if let (Ok(k), Ok(v)) = (unsafe { CStr::from_ptr(key).to_str() }, unsafe {CStr::from_ptr(value).to_str()}) {
        unsafe {
            std::env::set_var(k, v);
        }
    }
}

// // ==========================================
// // Float Precision
// // ==========================================

// #[unsafe(no_mangle)]
// pub extern "C" fn pl_set_float_precision(precision: isize) {
//     let opt_prec = if precision < 0 {
//         None
//     } else {
//         Some(precision as usize)
//     };
//     polars_core::fmt::set_float_precision(opt_prec);
// }

// #[unsafe(no_mangle)]
// pub extern "C" fn pl_get_float_precision() -> isize {
//     match polars_core::fmt::get_float_precision() {
//         Some(p) => p as isize,
//         None => -1,
//     }
// }

// // ==========================================
// // Thousands Separator
// // ==========================================

// #[unsafe(no_mangle)]
// pub unsafe extern "C" fn pl_set_thousands_separator(sep: *const c_char) {
//     let sep_char = if sep.is_null() {
//         None
//     } else {
//         let s = unsafe { CStr::from_ptr(sep).to_str().unwrap_or("") };
//         if s.is_empty() {
//             None
//         } else {
//             Some(s.chars().next().unwrap())
//         }
//     };
    
//     polars_core::fmt::set_thousands_separator(sep_char);
// }

// #[unsafe(no_mangle)]
// pub extern "C" fn pl_get_thousands_separator() -> *mut c_char {
//     let sep_string = polars_core::fmt::get_thousands_separator();
    
//     match CString::new(sep_string) {
//         Ok(c_string) => c_string.into_raw(),
//         Err(_) => std::ptr::null_mut(),
//     }
// }

// // ==========================================
// // Decimal Separator
// // ==========================================

// #[unsafe(no_mangle)]
// pub unsafe extern "C" fn pl_set_decimal_separator(sep: *const c_char) {
//     let sep_char = if sep.is_null() {
//         None
//     } else {
//         let s = unsafe { CStr::from_ptr(sep).to_str().unwrap_or(".") };
//         if s.is_empty() {
//             None
//         } else {
//             Some(s.chars().next().unwrap())
//         }
//     };
    
//     polars_core::fmt::set_decimal_separator(sep_char);
// }

// #[unsafe(no_mangle)]
// pub extern "C" fn pl_get_decimal_separator() -> *mut c_char {
//     let sep_char = polars_core::fmt::get_decimal_separator();
    
//     let sep_string = sep_char.to_string();
    
//     match CString::new(sep_string) {
//         Ok(c_string) => c_string.into_raw(),
//         Err(_) => std::ptr::null_mut(),
//     }
// }