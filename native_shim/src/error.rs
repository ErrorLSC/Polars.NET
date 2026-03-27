use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;

// ==========================================
// 0. Infra for Error Handling
// ==========================================

// Save Error Message in local thread
thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = RefCell::new(None);
}

pub fn set_error(msg: String) {
    LAST_ERROR.with(|e| *e.borrow_mut() = Some(msg));
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_get_last_error() -> *mut c_char {
    let msg = LAST_ERROR.with(|e| e.borrow_mut().take()); 
    
    match msg {
        Some(s) => {
            match CString::new(s) {
                Ok(c_str) => c_str.into_raw(),
                Err(_) => {
                    let safe_msg = CString::new("Error message contained null byte").unwrap();
                    safe_msg.into_raw()
                }
            }
        },
        None => std::ptr::null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_free_error_msg(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe { let _ = CString::from_raw(ptr); }
    }
}

// --- Marco ---

#[macro_export]
macro_rules! ffi_try {
    ($body:expr) => {{
        use std::panic::{catch_unwind, AssertUnwindSafe};
        use crate::error::set_error;
        
        let closure = || -> PolarsResult<_> { $body };
        let result = catch_unwind(AssertUnwindSafe(closure));

        match result {
            Ok(inner_result) => match inner_result {
                Ok(val) => val,
                Err(e) => {
                    set_error(e.to_string());
                    std::ptr::null_mut()
                }
            },
            Err(e) => {
                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown Rust Panic".to_string()
                };
                set_error(msg);
                std::ptr::null_mut()
            }
        }
    }};
}

#[macro_export]
macro_rules! ffi_try_void {
    ($body:expr) => {{
        use std::panic::{catch_unwind, AssertUnwindSafe};
        use crate::error::set_error;

        let closure = || -> PolarsResult<()> { $body };
        let result = catch_unwind(AssertUnwindSafe(closure));

        match result {
            Ok(inner_result) => match inner_result {
                Ok(()) => (),
                Err(e) => {
                    set_error(e.to_string());
                }
            },
            Err(e) => {
                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown Rust Panic".to_string()
                };
                set_error(msg);
            }
        }
    }};
}

#[macro_export]
macro_rules! ffi_try_c_int {
    ($body:expr) => {{
        use std::panic::{catch_unwind, AssertUnwindSafe};
        use crate::error::set_error;
        
        let closure = || -> PolarsResult<_> { $body };
        let result = catch_unwind(AssertUnwindSafe(closure));

        match result {
            Ok(inner_result) => match inner_result {
                Ok(val) => val, 
                Err(e) => {
                    set_error(e.to_string());
                    1 
                }
            },
            Err(e) => {
                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown Rust Panic".to_string()
                };
                set_error(msg);
                1 
            }
        }
    }};
}

#[macro_export]
macro_rules! ffi_bool_try {
    ($body:expr) => {{
        use std::panic::{catch_unwind, AssertUnwindSafe};
        use crate::error::set_error;
        
        let closure = || -> PolarsResult<_> { $body };
        let result = catch_unwind(AssertUnwindSafe(closure));

        match result {
            Ok(inner_result) => match inner_result {
                Ok(_) => true, 
                Err(e) => {
                    set_error(e.to_string());
                    false 
                }
            },
            Err(e) => {
                let msg = if let Some(s) = e.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = e.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Unknown Rust Panic".to_string()
                };
                set_error(msg);
                false 
            }
        }
    }};
}