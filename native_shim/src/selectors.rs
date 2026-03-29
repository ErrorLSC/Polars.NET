use polars::prelude::*;
use std::{ffi::CStr, os::raw::c_char};
use crate::{types::{ExprContext, SelectorContext}, utils::ptr_to_str};

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_free(ptr: *mut SelectorContext) {
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}

// =================================================================
// Basic Selectors
// =================================================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_all() -> *mut SelectorContext {
    ffi_try!({
        let s = all();
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

// selector.exclude(["a", "b"])
#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_exclude(
    sel_ptr: *mut SelectorContext,
    names_ptr: *const *const c_char,
    len: usize
) -> *mut SelectorContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(sel_ptr) };
        
        let mut exclusions = Vec::with_capacity(len);
        let slice = unsafe { std::slice::from_raw_parts(names_ptr, len) };
        for &p in slice {
            let s = ptr_to_str(p).unwrap();
            exclusions.push(PlSmallStr::from_str(s));
        }

        let new_sel = ctx.inner.exclude_cols(exclusions);
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: new_sel })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_cols(
    names_ptr: *const *const c_char,
    len: usize
) -> *mut SelectorContext {
    ffi_try!({
        // Build Vec<PlSmallStr>
        let mut names_vec = Vec::with_capacity(len);
        if len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(names_ptr, len) };
            for &p in slice {
                let s = ptr_to_str(p).unwrap();
                names_vec.push(PlSmallStr::from_string(s.to_string()));
            }
        }

        // Build Selector::ByName
        // Convert Vec to Arc<[T]>
        let names_arc: Arc<[PlSmallStr]> = names_vec.into();

        let s = Selector::ByName {
            names: names_arc,
            strict: true, 
        };
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}
// =================================================================
// String Matchers (StartsWith, EndsWith, Contains, Regex)
// =================================================================

fn to_small_str(ptr: *const c_char) -> PolarsResult<PlSmallStr> {
    let s = ptr_to_str(ptr).unwrap();
    Ok(PlSmallStr::from_str(s))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_starts_with(
    pattern: *const c_char
) -> *mut SelectorContext {
    ffi_try!({
        let p = ptr_to_str(pattern).unwrap();
        let regex = format!("^{}", p);
        let s = Selector::Matches(PlSmallStr::from_str(&regex));
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_ends_with(
    pattern: *const c_char
) -> *mut SelectorContext {
    ffi_try!({
        let p = ptr_to_str(pattern).unwrap();
        let regex = format!("{}$", p);
        let s = Selector::Matches(PlSmallStr::from_str(&regex));
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_contains(
    pattern: *const c_char
) -> *mut SelectorContext {
    ffi_try!({
        let p = ptr_to_str(pattern).unwrap();
        let s = Selector::Matches(PlSmallStr::from_str(p));
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_match(
    pattern: *const c_char
) -> *mut SelectorContext {
    ffi_try!({
        let p = to_small_str(pattern).unwrap();
        let s = Selector::Matches(p);
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

// =================================================================
// Type Selectors (DataTypeSelector)
// =================================================================

// Helper：Wrap DataType to DataTypeSelector::AnyOf
#[inline]
fn dt_selector_single(dt: DataType) -> DataTypeSelector {
    DataTypeSelector::AnyOf(Arc::new([dt]))
}

// C# PlDataType (i32) -> Rust DataTypeSelector
fn map_i32_to_dtype_selector(kind: i32) -> DataTypeSelector {
    match kind {
        // 0: Unknown / SameAsInput
        0 => DataTypeSelector::Empty,

        // 1: Boolean
        1 => dt_selector_single(DataType::Boolean),

        // Integers
        2 => dt_selector_single(DataType::Int8),
        3 => dt_selector_single(DataType::Int16),
        4 => dt_selector_single(DataType::Int32),
        5 => dt_selector_single(DataType::Int64),

        // Unsigned Integers
        6 => dt_selector_single(DataType::UInt8),
        7 => dt_selector_single(DataType::UInt16),
        8 => dt_selector_single(DataType::UInt32),
        9 => dt_selector_single(DataType::UInt64),

        // Floats
        10 => dt_selector_single(DataType::Float32),
        11 => dt_selector_single(DataType::Float64),

        // String
        12 => dt_selector_single(DataType::String),

        // Temporal - Date
        13 => dt_selector_single(DataType::Date),

        // Temporal - Datetime
        14 => DataTypeSelector::Datetime(TimeUnitSet::all(), TimeZoneSet::Any),

        // Temporal - Time
        15 => dt_selector_single(DataType::Time),

        // Temporal - Duration
        16 => DataTypeSelector::Duration(TimeUnitSet::all()),

        // Binary
        17 => dt_selector_single(DataType::Binary),

        // Null 
        18 => dt_selector_single(DataType::Null),

        // Struct
        19 => DataTypeSelector::Struct,

        20 => DataTypeSelector::List(None),

        21 => DataTypeSelector::Categorical,
        22 => DataTypeSelector::Decimal,
        23 => DataTypeSelector::Array(None, None),
        24 => dt_selector_single(DataType::Int128),
        25 => dt_selector_single(DataType::UInt128),
        26 => dt_selector_single(DataType::Float16),
        // Empty
        _ => DataTypeSelector::Empty,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_by_dtype(kind: i32) -> *mut SelectorContext {
    ffi_try!({
        let dts = map_i32_to_dtype_selector(kind);
        // Selector::ByDType(DataTypeSelector)
        let s = Selector::ByDType(dts);
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

macro_rules! impl_simple_dtype_selector {
    ($($fn_name:ident => $variant:ident),+ $(,)?) => {
        $(
            #[unsafe(no_mangle)]
            pub extern "C" fn $fn_name() -> *mut SelectorContext {
                ffi_try!({
                    let s = Selector::ByDType(DataTypeSelector::$variant);
                    Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
                })
            }
        )+
    };
}

impl_simple_dtype_selector! {
    // pl_selector_wildcard => Wildcard,
    pl_selector_empty => Empty,
    
    pl_selector_integer => Integer,
    pl_selector_unsigned_integer => UnsignedInteger,
    pl_selector_signed_integer => SignedInteger,
    pl_selector_float => Float,
    pl_selector_numeric => Numeric,
    pl_selector_decimal => Decimal,
    
    pl_selector_enum_type => Enum,
    pl_selector_categorical => Categorical,
    
    pl_selector_nested => Nested,
    pl_selector_struct => Struct,
    
    pl_selector_temporal => Temporal,
    // pl_selector_object => Object,
}

// ==========================================
// List Selector
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_list(inner_ptr: *mut SelectorContext) -> *mut SelectorContext {
    ffi_try!({
        let inner_dt = if inner_ptr.is_null() {
            None
        } else {
            let ctx = unsafe { Box::from_raw(inner_ptr) };
            match &ctx.inner {
                Selector::ByDType(dt) => Some(Arc::new(dt.clone())),
                _ => polars_bail!(ComputeError: "Inner selector for List must be a DataType selector (e.g. Cs.Numeric())"),
            }
        };

        let s = Selector::ByDType(DataTypeSelector::List(inner_dt));
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

// ==========================================
// Array Selector
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_array(
    inner_ptr: *mut SelectorContext,
    width: usize,
) -> *mut SelectorContext {
    ffi_try!({
        let inner_dt = if inner_ptr.is_null() {
            None
        } else {
            let ctx = unsafe { Box::from_raw(inner_ptr) };
            match &ctx.inner {
                Selector::ByDType(dt) => Some(Arc::new(dt.clone())),
                _ => polars_bail!(ComputeError: "Inner selector for Array must be a DataType selector"),
            }
        };

        let opt_width = if width == 0 { None } else { Some(width) };
        
        let s = Selector::ByDType(DataTypeSelector::Array(inner_dt, opt_width));
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

// ==========================================
// Datetime & Duration Selector
// ==========================================
fn map_time_unit(tu: u8) -> TimeUnitSet {
    match tu {
        0 => TimeUnitSet::NANO_SECONDS,
        1 => TimeUnitSet::MICRO_SECONDS,
        2 => TimeUnitSet::MILLI_SECONDS,
        _ => TimeUnitSet::all(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_datetime(
    time_unit: u8, 
    time_zone: *const c_char,
) -> *mut SelectorContext {
    ffi_try!({
        let tu_set = map_time_unit(time_unit);

        let tz_set = if time_zone.is_null() {
            TimeZoneSet::Any
        } else {
            let tz_str = unsafe { CStr::from_ptr(time_zone).to_string_lossy() };
            
            match tz_str.as_ref() {
                "" => TimeZoneSet::Unset,  
                "*" => TimeZoneSet::AnySet, 
                _ => {
                    let tz = TimeZone::opt_try_new(Some(tz_str.as_ref()))?
                        .expect("opt_try_new should return Some when valid");
                        
                    TimeZoneSet::AnyOf(Arc::new([tz]))
                }
            }
        };

        let s = Selector::ByDType(DataTypeSelector::Datetime(tu_set, tz_set));
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_duration(time_unit: u8) -> *mut SelectorContext {
    ffi_try!({
        let tu_set = map_time_unit(time_unit);
        let s = Selector::ByDType(DataTypeSelector::Duration(tu_set));
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}
// =================================================================
// Set Operations
// =================================================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_and(
    left: *mut SelectorContext,
    right: *mut SelectorContext
) -> *mut SelectorContext {
    ffi_try!({
        let l = unsafe { Box::from_raw(left) };
        let r = unsafe { Box::from_raw(right) };
        
        // Selector::Intersect(lhs, rhs)
        let res = Selector::Intersect(Arc::new(l.inner), Arc::new(r.inner));
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_or(
    left: *mut SelectorContext,
    right: *mut SelectorContext
) -> *mut SelectorContext {
    ffi_try!({
        let l = unsafe { Box::from_raw(left) };
        let r = unsafe { Box::from_raw(right) };
        
        // Selector::Union(lhs, rhs)
        let res = Selector::Union(Arc::new(l.inner), Arc::new(r.inner));
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_not(
    ptr: *mut SelectorContext
) -> *mut SelectorContext {
    ffi_try!({
        let l = unsafe { Box::from_raw(ptr) };
        
        let res = Selector::Difference(
            Arc::new(Selector::Wildcard),
            Arc::new(l.inner)
        );
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_sub(
    left: *mut SelectorContext,
    right: *mut SelectorContext
) -> *mut SelectorContext {
    ffi_try!({
        let l = unsafe { Box::from_raw(left) };
        let r = unsafe { Box::from_raw(right) };
        
        // A - B
        let res = Selector::Difference(Arc::new(l.inner), Arc::new(r.inner));
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_xor(
    left: *mut SelectorContext,
    right: *mut SelectorContext
) -> *mut SelectorContext {
    ffi_try!({
        let l = unsafe { Box::from_raw(left) };
        let r = unsafe { Box::from_raw(right) };
        
        // A ^ B
        let res = Selector::ExclusiveOr(Arc::new(l.inner), Arc::new(r.inner));
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: res })))
    })
}

// =================================================================
// Bridges
// =================================================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_into_expr(
    sel_ptr: *mut SelectorContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(sel_ptr) };
        let expr: Expr = ctx.inner.into(); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_clone(
    sel_ptr: *mut SelectorContext
) -> *mut SelectorContext {
    ffi_try!({
        let ctx = unsafe { &*sel_ptr };
        let new_sel = ctx.inner.clone();
        Ok(Box::into_raw(Box::new(SelectorContext { inner: new_sel })))
    })
}