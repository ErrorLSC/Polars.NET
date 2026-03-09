use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[macro_use]
mod error;
mod utils;
mod types;
mod expr;
mod pl_io;
mod delta;
mod catalog;
mod eager;
mod lazy;
mod udf;
mod selectors;
mod sql;
mod series;
mod datatypes;
mod schema;
mod excel_reader;
mod excel_writer;

use std::ffi::CStr;
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


