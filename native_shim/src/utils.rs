use std::ffi::{CStr, c_char, c_void};
use polars::frame::UniqueKeepStrategy;
use polars::prelude::{AsofStrategy, ClosedInterval, Expr, JoinBuildSide, JoinCoalesce, JoinType, JoinValidation, MaintainOrderJoin, ParallelStrategy, PlSmallStr, SchemaRef, TimeUnit};
use polars::time::ClosedWindow;
use polars_io::ExternalCompression;
use polars_io::prelude::JsonFormat;
use polars_io::utils::sync_on_close::SyncOnCloseType;

use crate::types::{ExprContext,SchemaContext};

#[unsafe(no_mangle)]
pub extern "C" fn pl_free_string(ptr: *mut std::os::raw::c_char) {
    if !ptr.is_null() {
        unsafe { let _ = std::ffi::CString::from_raw(ptr); }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_free_ptr_array(ptr: *mut *mut c_void, len: usize) {
    if !ptr.is_null() {
        unsafe {
            let _ = Vec::from_raw_parts(ptr, len, len);
        }
    }
}

pub unsafe fn ptr_to_vec_string(ptr: *const *const c_char, len: usize) -> Vec<String> {
    if ptr.is_null() || len == 0 {
        return Vec::new();
    }
    let mut res = Vec::with_capacity(len);
    for i in 0..len {
        let c_str = unsafe { *ptr.add(i)};
        if !c_str.is_null() {
            if let Ok(s) = unsafe {CStr::from_ptr(c_str).to_str()}{
                res.push(s.to_string());
            }
        }
    }
    res
}

pub fn ptr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, std::str::Utf8Error> {
    if ptr.is_null() { 
        panic!("Null pointer passed to ptr_to_str"); 
    }
    unsafe { CStr::from_ptr(ptr).to_str() }
}

pub(crate) unsafe fn consume_exprs_array(
    ptr: *const *mut ExprContext, 
    len: usize
) -> Vec<Expr> {
    let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
    slice.iter()
        .map(|&p| unsafe { Box::from_raw(p).inner })
        .collect()
}

pub(crate) unsafe fn ptr_to_schema_ref(ptr: *mut SchemaContext) -> Option<SchemaRef> {
    if ptr.is_null() {
        None
    } else {
        let ctx = unsafe { &*ptr };
        Some(ctx.schema.clone())
    }
}

pub(crate) unsafe fn ptr_to_opt_pl_str(ptr: *const c_char) -> Option<PlSmallStr> {
    if ptr.is_null() {
        None
    } else {
        ptr_to_str(ptr).ok().map(PlSmallStr::from_str)
    }
}

pub(crate) unsafe fn ptr_to_pl_str(ptr: *const c_char, default: &str) -> PlSmallStr {
    if ptr.is_null() {
        PlSmallStr::from_str(default)
    } else {
        ptr_to_str(ptr).ok().map(PlSmallStr::from_str).unwrap_or_else(|| PlSmallStr::from_str(default))
    }
}

pub unsafe fn ptr_to_vec_pl_string_with_default(
    ptr: *const *const c_char, 
    len: usize, 
    default: &str
) -> Vec<PlSmallStr> {
    if ptr.is_null() || len == 0 {
        return Vec::new();
    }
    
    let mut res = Vec::with_capacity(len);
    for i in 0..len {
        let c_str =unsafe{ *ptr.add(i)};
        res.push(unsafe{ptr_to_pl_str(c_str, default)});
    }
    res
}

pub(crate) fn map_jointype(code: u8) -> JoinType {
    match code {
        0 => JoinType::Inner,
        1 => JoinType::Left,
        2 => JoinType::Full, 
        3 => JoinType::Cross,
        4 => JoinType::Semi,
        5 => JoinType::Anti,
        6 => JoinType::IEJoin,
        _ => JoinType::Inner, // Default
    }
}

pub fn map_validation(code: u8) -> JoinValidation {
    match code {
        0 => JoinValidation::ManyToMany,
        1 => JoinValidation::ManyToOne,
        2 => JoinValidation::OneToMany,
        3 => JoinValidation::OneToOne,
        _ => JoinValidation::ManyToMany, // Default
    }
}

pub fn map_coalesce(code: u8) -> JoinCoalesce {
    match code {
        0 => JoinCoalesce::JoinSpecific,
        1 => JoinCoalesce::CoalesceColumns,
        2 => JoinCoalesce::KeepColumns,
        _ => JoinCoalesce::JoinSpecific, // Default
    }
}

pub fn map_maintain_order(code: u8) -> MaintainOrderJoin {
    match code {
        0 => MaintainOrderJoin::None,
        1 => MaintainOrderJoin::Left,
        2 => MaintainOrderJoin::Right,
        3 => MaintainOrderJoin::LeftRight,
        4 => MaintainOrderJoin::RightLeft,
        _ => MaintainOrderJoin::None, // Default
    }
}

#[inline]
pub fn map_join_side(code: u8) -> JoinBuildSide {
    match code {
        1 => JoinBuildSide::PreferLeft,
        2 => JoinBuildSide::ForceLeft,
        3 => JoinBuildSide::PreferRight,
        4 => JoinBuildSide::ForceRight,
        _ => JoinBuildSide::PreferLeft, 
    }
}

pub(crate) fn map_asof_strategy(code: u8) -> AsofStrategy {
    match code {
        0 => AsofStrategy::Backward,
        1 => AsofStrategy::Forward,
        2 => AsofStrategy::Nearest,
        _ => AsofStrategy::Backward,
    }
}

// helper function: u8 -> UniqueKeepStrategy
// 0: First, 1: Last, 2: Any, 3: None
#[inline]
pub(crate) fn parse_keep_strategy(val: u8) -> UniqueKeepStrategy {
    match val {
        0 => UniqueKeepStrategy::First,
        1 => UniqueKeepStrategy::Last,
        2 => UniqueKeepStrategy::Any,
        3 => UniqueKeepStrategy::None,
        _ => UniqueKeepStrategy::First, // Default fallback
    }
}

pub(crate) fn map_parallel_strategy(code: u8) -> ParallelStrategy {
    match code {
        1 => ParallelStrategy::Columns,
        2 => ParallelStrategy::RowGroups,
        3 => ParallelStrategy::None,
        0 | _ => ParallelStrategy::Auto,
    }
}

#[inline]
pub(crate) fn map_json_format(code: u8) -> JsonFormat {
    match code {
        1 => JsonFormat::JsonLines, // .jsonl / .ndjson
        0 | _ => JsonFormat::Json,  // standard .json array
    }
}

#[inline]
pub(crate) fn map_sync_on_close(val: u8) -> SyncOnCloseType {
    match val {
        1 => SyncOnCloseType::Data,
        2 => SyncOnCloseType::All,
        _ => SyncOnCloseType::None,
    }
}

#[inline]
pub(crate) fn parse_closed_window(val: u8) -> ClosedWindow {
    match val {
        0 => ClosedWindow::Left,
        1 => ClosedWindow::Right,
        2 => ClosedWindow::Both,
        3 => ClosedWindow::None,
        _ => ClosedWindow::Left,
    }
}

#[inline]
pub(crate) fn parse_time_unit(val: u8) -> Option<TimeUnit> {
    match val {
        0 => Some(TimeUnit::Nanoseconds),
        1 => Some(TimeUnit::Microseconds),
        2 => Some(TimeUnit::Milliseconds),
        _ => None,
    }
}

#[inline]
pub(crate) fn parse_closed_interval(val: u8) -> ClosedInterval {
    match val {
        0 => ClosedInterval::Left,
        1 => ClosedInterval::Right,
        2 => ClosedInterval::Both,
        3 => ClosedInterval::None,
        _ => ClosedInterval::Both, 
    }
}

#[inline]
pub(crate) fn map_external_compression(
    compression_code: u8, 
    compression_level: i32
) -> ExternalCompression {

    let get_level = || -> Option<u32> {
        if compression_level >= 0 {
            Some(compression_level as u32)
        } else {
            None
        }
    };

    match compression_code {
        1 => ExternalCompression::Gzip {
            level: get_level(),
        },
        2 => ExternalCompression::Zstd {
            level: get_level(),
        },
        _ => ExternalCompression::Uncompressed,
    }
}

