use std::ffi::{CString, c_char, c_int};
use polars::prelude::*;
use polars::datatypes::CategoricalPhysical;

use crate::{types::{CategoriesContext, FrozenCategoriesContext, SeriesContext}, utils::ptr_to_str};

pub fn parse_categorical_physical(unit: u8) -> CategoricalPhysical {
    match unit {
        0 => CategoricalPhysical::U32,
        1 => CategoricalPhysical::U16,
        2 => CategoricalPhysical::U8,
        _ => CategoricalPhysical::U32, 
    }
}

pub fn physical_to_u8(physical: CategoricalPhysical) -> u8 {
    match physical {
        CategoricalPhysical::U32 => 0,
        CategoricalPhysical::U16 => 1,
        CategoricalPhysical::U8 => 2 
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_categories_new(
    name: *const c_char,
    namespace: *const c_char,
    physical_code: u8,
) -> *mut CategoriesContext {
    ffi_try!({
        let name_str = if name.is_null() { "" } else {  ptr_to_str(name).unwrap_or("")  };
        let namespace_str = if namespace.is_null() { "" } else { ptr_to_str(namespace).unwrap_or("")  };
        
        let physical = parse_categorical_physical(physical_code);
        
        let cats = Categories::new(
            name_str.into(),
            namespace_str.into(),
            physical,
        );
        
        Ok(Box::into_raw(Box::new(CategoriesContext { inner: cats })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_categories_global() -> *mut CategoriesContext {
    ffi_try!({
        let cats = Categories::global();
        Ok(Box::into_raw(Box::new(CategoriesContext { inner: cats })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_categories_is_global(
    ptr: *mut CategoriesContext,
    out_is_global: *mut bool,
) -> c_int {
    ffi_try_c_int!({
        if ptr.is_null() {
            return Err(PolarsError::ComputeError("CategoriesContext pointer is null".into()));
        }
        if out_is_global.is_null() {
            return Err(PolarsError::ComputeError("Output pointer for is_global is null".into()));
        }
        
        unsafe { 
            let ctx =  &*ptr;
            *out_is_global = ctx.inner.is_global();
        }
        
        Ok(0)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_categories_get_physical(
    ptr: *mut CategoriesContext,
    out_physical: *mut u8,
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() || out_physical.is_null() {
            return Err(PolarsError::ComputeError("CategoriesContext or output pointer is null".into()));
        }
        unsafe {
            let ctx = &*ptr;
            let physical_enum = ctx.inner.physical();
            
            *out_physical = physical_to_u8(physical_enum);
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_categories_random(
    namespace: *const c_char,
    physical_code: u8, 
) -> *mut CategoriesContext {
    ffi_try!({
        let namespace_str = if namespace.is_null() { "" } else { ptr_to_str(namespace).unwrap_or("")  };
        let physical = parse_categorical_physical(physical_code);
        
        let cats = Categories::random(
            namespace_str.into(),
            physical,
        );
        
        Ok(Box::into_raw(Box::new(CategoriesContext { inner: cats })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_categories_free(ptr: *mut CategoriesContext) {
    if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_categories_get_name(
    ptr: *mut CategoriesContext,
    out_str: *mut *mut c_char,
) -> c_int {
    ffi_try_c_int!({
        if ptr.is_null() {
            return Err(PolarsError::ComputeError("CategoriesContext pointer is null".into()));
        }
        let ctx = unsafe {&*ptr};
        let c_str = CString::new(ctx.inner.name().as_str())
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        unsafe {*out_str = c_str.into_raw()};
        Ok(0) 
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_categories_get_namespace(
    ptr: *mut CategoriesContext,
    out_str: *mut *mut c_char,
) -> c_int {
    ffi_try_c_int!({
        if ptr.is_null() {
            return Err(PolarsError::ComputeError("CategoriesContext pointer is null".into()));
        }
        let ctx = unsafe {&*ptr};
        let c_str = CString::new(ctx.inner.namespace().as_str())
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        unsafe {*out_str = c_str.into_raw()};
        Ok(0) 
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_categories_hash(
    ptr: *mut CategoriesContext,
    out_hash: *mut u64,
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() || out_hash.is_null() {
            return Err(PolarsError::ComputeError("Pointer is null".into()));
        }
        unsafe {
            let ctx = &*ptr;
            *out_hash = ctx.inner.hash()
        };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_categories_freeze(
    ptr: *mut CategoriesContext
) -> *mut FrozenCategoriesContext {
    ffi_try!({
        if ptr.is_null() {
            return Err(PolarsError::ComputeError("CategoriesContext pointer is null".into())); 
        }
        let ctx = unsafe { &*ptr };
        
        let frozen = ctx.inner.freeze();
        
        Ok(Box::into_raw(Box::new(FrozenCategoriesContext { inner: frozen })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_frozencategories_free(ptr: *mut FrozenCategoriesContext) {
    if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_frozencategories_new(
    strings: *const *const c_char,
    len: usize,
) -> *mut FrozenCategoriesContext {
    ffi_try!({
        if strings.is_null() {
            return Err(PolarsError::ComputeError("String array pointer is null".into()));
        }

        let mut rust_strings = Vec::with_capacity(len);
        let slice = unsafe { std::slice::from_raw_parts(strings, len) };

        for &ptr in slice {
            if ptr.is_null() {
                return Err(PolarsError::ComputeError("Enum categories cannot contain null strings".into()));
            }
            let s = ptr_to_str(ptr).unwrap_or("");
            rust_strings.push(s);
        }

        let frozen = FrozenCategories::new(rust_strings)?;
        Ok(Box::into_raw(Box::new(FrozenCategoriesContext { inner: frozen })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_frozencategories_hash(
    ptr: *mut FrozenCategoriesContext,
    out_hash: *mut u64,
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() || out_hash.is_null() {
            return Err(PolarsError::ComputeError("Pointer is null".into()));
        }
        unsafe {
            let ctx = &*ptr;
            *out_hash = ctx.inner.hash()
        };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_frozencategories_get_categories(
    ptr: *mut FrozenCategoriesContext
) -> *mut SeriesContext {
    ffi_try!({
        if ptr.is_null() {
            return Err(PolarsError::ComputeError("FrozenCategoriesContext pointer is null".into()));
        }
        
        let ctx = unsafe { &*ptr };
        let arr = ctx.inner.categories();

        let chunked = StringChunked::with_chunk("categories".into(), arr.clone());
        let series = chunked.into_series();
        
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_frozencategories_get_physical(
    ptr: *mut FrozenCategoriesContext,
    out_physical: *mut u8,
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() || out_physical.is_null() {
            return Err(PolarsError::ComputeError("CategoriesContext or output pointer is null".into()));
        }
        unsafe {
            let ctx = &*ptr;
            let physical_enum = ctx.inner.physical();
            
            *out_physical = physical_to_u8(physical_enum);
        }
        Ok(())
    })
}
