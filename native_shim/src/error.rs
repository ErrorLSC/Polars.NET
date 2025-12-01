use std::cell::RefCell;
use std::ffi::CString;
use std::os::raw::c_char;

// ==========================================
// 0. 错误处理基础设施
// ==========================================

// 线程局部存储错误信息
thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = RefCell::new(None);
}

// 辅助函数：设置错误信息 (pub 使得其他模块可见)
pub fn set_error(msg: String) {
    LAST_ERROR.with(|e| *e.borrow_mut() = Some(msg));
}

// 供外部调用：获取错误
#[unsafe(no_mangle)]
pub extern "C" fn pl_get_last_error() -> *mut c_char {
    // take() 是个好习惯，读完即毁，防止 stale error
    let msg = LAST_ERROR.with(|e| e.borrow_mut().take()); 
    
    match msg {
        Some(s) => {
            // 【修复】处理包含 \0 的边缘情况
            match CString::new(s) {
                Ok(c_str) => c_str.into_raw(),
                Err(_) => {
                    // 如果原字符串有 \0，返回一个通用的错误提示，保证不崩
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

// --- 宏定义 ---
// 注意：宏要用 macro_export 导出，或者在 lib.rs 里用 #[macro_use]
// 这里的技巧是：在宏内部引用 crate::error::set_error，这样在任何文件调用宏都能找到 set_error

#[macro_export]
macro_rules! ffi_try {
    ($body:expr) => {{
        use std::panic::{catch_unwind, AssertUnwindSafe};
        // 引用 crate::error::set_error 确保路径正确
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