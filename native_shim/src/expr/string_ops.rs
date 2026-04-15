use polars::prelude::*;
use std::ffi::{CStr};
use std::os::raw::c_char;
use crate::{gen_namespace_unary, impl_expr_namespace_expr_arg};
use crate::types::{DataTypeContext, DataTypeExprContext, ExprContext};
use crate::utils::{ptr_to_str};

gen_namespace_unary!(pl_expr_str_to_uppercase, str, to_uppercase);
gen_namespace_unary!(pl_expr_str_to_lowercase, str, to_lowercase);
gen_namespace_unary!(pl_expr_str_len_bytes, str, len_bytes);
gen_namespace_unary!(pl_expr_str_len_chars, str, len_chars);
gen_namespace_unary!(pl_expr_str_escape_regex, str, escape_regex);
gen_namespace_unary!(pl_expr_str_to_titlecase, str, to_titlecase);
gen_namespace_unary!(pl_expr_str_reverse, str, reverse);

impl_expr_namespace_expr_arg!(pl_expr_str_split, str,split);
impl_expr_namespace_expr_arg!(pl_expr_str_split_inclusive, str,split_inclusive);
impl_expr_namespace_expr_arg!(pl_expr_str_strip_chars,str, strip_chars);
impl_expr_namespace_expr_arg!(pl_expr_str_strip_chars_start,str, strip_chars_start);
impl_expr_namespace_expr_arg!(pl_expr_str_strip_chars_end,str, strip_chars_end);
impl_expr_namespace_expr_arg!(pl_expr_str_strip_prefix, str,strip_prefix);
impl_expr_namespace_expr_arg!(pl_expr_str_strip_suffix, str,strip_suffix);
impl_expr_namespace_expr_arg!(pl_expr_str_starts_with, str,starts_with);
impl_expr_namespace_expr_arg!(pl_expr_str_ends_with, str,ends_with);
impl_expr_namespace_expr_arg!(pl_expr_str_head, str,head);
impl_expr_namespace_expr_arg!(pl_expr_str_tail, str,tail);
impl_expr_namespace_expr_arg!(pl_expr_str_json_path_match, str,json_path_match);

macro_rules! impl_expr_str_expr_usize_arg {
    ($ffi_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $ffi_name(
            expr_ptr: *mut ExprContext,
            arg_ptr: *mut ExprContext,
            n: usize
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let arg = unsafe { Box::from_raw(arg_ptr) };
                let new_expr = ctx.inner.str().$method(arg.inner, n);
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}
impl_expr_str_expr_usize_arg!(pl_expr_str_split_exact, split_exact);
impl_expr_str_expr_usize_arg!(pl_expr_str_split_exact_inclusive, split_exact_inclusive);
impl_expr_str_expr_usize_arg!(pl_expr_str_splitn, splitn);
impl_expr_str_expr_usize_arg!(pl_expr_str_extract, extract);

macro_rules! impl_expr_str_expr_bool_arg {
    ($ffi_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $ffi_name(
            expr_ptr: *mut ExprContext,
            arg_ptr: *mut ExprContext,
            flag: bool
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let arg = unsafe { Box::from_raw(arg_ptr) };
                let new_expr = ctx.inner.str().$method(arg.inner, flag);
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

impl_expr_str_expr_bool_arg!(pl_expr_str_split_regex, split_regex);
impl_expr_str_expr_bool_arg!(pl_expr_str_split_regex_inclusive, split_regex_inclusive);
impl_expr_str_expr_bool_arg!(pl_expr_str_contains, contains);
impl_expr_str_expr_bool_arg!(pl_expr_str_contains_any, contains_any);
impl_expr_str_expr_bool_arg!(pl_expr_str_find, find);
impl_expr_str_expr_bool_arg!(pl_expr_str_count_matches, count_matches);

macro_rules! impl_expr_str_bool_arg {
    ($ffi_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $ffi_name(
            expr_ptr: *mut ExprContext,
            flag: bool
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let new_expr = ctx.inner.str().$method(flag);
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

macro_rules! impl_expr_str_many_search {
    ($ffi_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $ffi_name(
            expr_ptr: *mut ExprContext, 
            pat_ptr: *mut ExprContext,
            ascii_case_insensitive: bool,
            overlapping: bool,
            left_most: bool
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let pat = unsafe { Box::from_raw(pat_ptr) };
                let new_expr = ctx.inner.str().$method(pat.inner, ascii_case_insensitive, overlapping, left_most);
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

impl_expr_str_many_search!(pl_expr_str_extract_many, extract_many);
impl_expr_str_many_search!(pl_expr_str_find_many, find_many);

macro_rules! impl_expr_str_pad {
    ($ffi_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $ffi_name(
            expr_ptr: *mut ExprContext,
            length_ptr: *mut ExprContext,
            fill_char_ptr: *const c_char
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let length = unsafe { Box::from_raw(length_ptr) };
                let fill_str = ptr_to_str(fill_char_ptr).unwrap_or(" ");
                let fill_char = fill_str.chars().next().unwrap_or(' ');
                
                let new_expr = ctx.inner.str().$method(length.inner, fill_char);
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

impl_expr_str_pad!(pl_expr_str_pad_start, pad_start);
impl_expr_str_pad!(pl_expr_str_pad_end, pad_end);

macro_rules! impl_expr_str_to_simple_time {
    ($ffi_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $ffi_name(
            expr_ptr: *mut ExprContext,
            format: *const c_char,
            strict: bool,
            exact: bool,
            cache: bool
        ) -> *mut ExprContext {
            ffi_try!({
                let ctx = unsafe { Box::from_raw(expr_ptr) };
                let format_opt = if format.is_null() {
                    None
                } else {
                    let fmt_str = unsafe { std::ffi::CStr::from_ptr(format).to_string_lossy() };
                    Some(fmt_str.into())
                };

                let options = StrptimeOptions {
                    format: format_opt, strict, exact, cache,
                };
                let new_expr = ctx.inner.str().$method(options);
                Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
            })
        }
    };
}

impl_expr_str_to_simple_time!(pl_expr_str_to_date, to_date);
impl_expr_str_to_simple_time!(pl_expr_str_to_time, to_time);

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_json_decode(
    expr_ptr: *mut ExprContext,
    dtype_ptr: *mut DataTypeExprContext,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let dtype = unsafe { Box::from_raw(dtype_ptr) };

        let new_expr = ctx.inner.str().json_decode(dtype.inner);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_join(
    expr_ptr: *mut ExprContext,
    sep_ptr: *const c_char,
    ignore_nulls: bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let sep = ptr_to_str(sep_ptr).unwrap();
        // list().join(sep, ignore_nulls=true)
        let new_expr = ctx.inner.str().join(sep, ignore_nulls);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_replace_n(
    expr_ptr: *mut ExprContext, 
    pat_ptr: *mut ExprContext,
    value_ptr: *mut ExprContext,
    literal:bool,
    n:i64
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let pat = unsafe { Box::from_raw(pat_ptr) };
        let value = unsafe { Box::from_raw(value_ptr) };
        
        let new_expr = ctx.inner.str().replace_n(pat.inner,value.inner ,literal,n);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_replace_many(
    expr_ptr: *mut ExprContext, 
    pat_ptr: *mut ExprContext,
    replace_ptr: *mut ExprContext,
    ascii_case_insensitive:bool,
    left_most : bool
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let pat = unsafe { Box::from_raw(pat_ptr) };
        let replace = unsafe { Box::from_raw(replace_ptr) };
        
        let new_expr = ctx.inner.str().replace_many(pat.inner,replace.inner ,ascii_case_insensitive,left_most);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

// Replace All
// pat: matching pattern, val: replace value
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_replace_all(
    expr_ptr: *mut ExprContext, 
    pat_ptr: *mut ExprContext,
    val_ptr: *mut ExprContext,
    literal: bool 
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let pat = unsafe { Box::from_raw(pat_ptr) };
        let val = unsafe { Box::from_raw(val_ptr) };

        let new_expr = ctx.inner.str().replace_all(pat.inner, val.inner, literal);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}


impl_expr_namespace_expr_arg!(pl_expr_str_extract_all, str, extract_all);

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_extract_groups(
    expr_ptr: *mut ExprContext,
    sep_ptr: *const c_char
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let sep = ptr_to_str(sep_ptr).unwrap();
        let new_expr = ctx.inner.str().extract_groups(sep)?;
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}
impl_expr_namespace_expr_arg!(pl_expr_str_find_literal, str, find_literal);

impl_expr_str_bool_arg!(pl_expr_str_hex_decode, hex_decode);
impl_expr_str_bool_arg!(pl_expr_str_base64_decode, base64_decode);
gen_namespace_unary!(pl_expr_str_hex_encode, str, hex_encode);
gen_namespace_unary!(pl_expr_str_base64_encode, str, base64_encode);

impl_expr_namespace_expr_arg!(pl_expr_str_zfill, str, zfill);

// offset: start position , length: offset length
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_slice(
    expr_ptr: *mut ExprContext, offset_ptr: *mut ExprContext,length_ptr: *mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let offset = unsafe { Box::from_raw(offset_ptr) };
        let length = unsafe {Box::from_raw(length_ptr)};
        // Polars API: str().slice(offset, length)
        let new_expr = ctx.inner.str().slice(offset.inner, length.inner);
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_strptime(
    expr_ptr: *mut ExprContext,
    dtype_ptr: *mut DataTypeExprContext,
    format: *const c_char,
    strict: bool,
    exact: bool,
    cache: bool,
    ambiguous_ptr:*mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let dtype = unsafe { Box::from_raw(dtype_ptr) };
        let ambiguous = unsafe { Box::from_raw(ambiguous_ptr) };
        let format_opt = if format.is_null() {
            None
        } else {
            let fmt_str = unsafe { CStr::from_ptr(format).to_string_lossy() };
            Some(fmt_str.into())
        };

        let options = StrptimeOptions {
            format: format_opt,
            strict,
            exact,
            cache,
        };

        let new_expr = ctx.inner.str().strptime(dtype.inner,options,ambiguous.inner);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_to_datetime(
    expr_ptr: *mut ExprContext,
    time_unit: u8, // 0: Nano, 1: Micro, 2: Milli, -1: None
    time_zone: *const c_char,
    format: *const c_char,
    strict: bool,
    exact: bool,
    cache: bool,
    ambiguous_ptr:*mut ExprContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let ambiguous = unsafe { Box::from_raw(ambiguous_ptr) };
        let tu_opt = match time_unit {
            0 => Some(TimeUnit::Nanoseconds),
            1 => Some(TimeUnit::Microseconds),
            2 => Some(TimeUnit::Milliseconds),
            _ => None, 
        };

        let tz_opt = if time_zone.is_null() {
            None
        } else {
            let tz_str = unsafe { CStr::from_ptr(time_zone).to_string_lossy() };
            TimeZone::opt_try_new(Some(tz_str.as_ref()))?
        };

        let format_opt = if format.is_null() {
            None
        } else {
            let fmt_str = unsafe { CStr::from_ptr(format).to_string_lossy() };
            Some(fmt_str.into())
        };

        let options = StrptimeOptions {
            format: format_opt,
            strict,
            exact,
            cache,
        };

        let new_expr = ctx.inner.str().to_datetime(tu_opt, tz_opt, options, ambiguous.inner);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_to_decimal(
    expr_ptr: *mut ExprContext, 
    scale: usize
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        
        let new_expr = ctx.inner.str().to_decimal(scale);
        
        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_to_integer(
    expr_ptr: *mut ExprContext,
    base_ptr: *mut ExprContext,
    dtype_ptr: *mut DataTypeContext,
    strict: bool,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let base = unsafe { Box::from_raw(base_ptr) };

        let dtype_opt = if dtype_ptr.is_null() {
            None
        } else {
            let dtype_ctx = unsafe { &*dtype_ptr };
            Some(dtype_ctx.dtype.clone()) 
        };

        let new_expr = ctx.inner.str().to_integer(base.inner, dtype_opt, strict);

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_str_normalize(
    expr_ptr: *mut ExprContext,
    code: i32,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };

        let form = match code {
            1 => UnicodeForm::NFC,
            5 => UnicodeForm::NFKC,
            2 => UnicodeForm::NFD, 
            6 => UnicodeForm::NFKD, 
            _ => UnicodeForm::NFC
        };

        let new_expr = ctx.inner.str().normalize(form);

        Ok(Box::into_raw(Box::new(ExprContext { inner: new_expr })))
    })
}



