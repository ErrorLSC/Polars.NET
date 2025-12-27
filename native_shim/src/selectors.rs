use polars::prelude::*;
use std::os::raw::c_char;
use crate::{types::{ExprContext, SelectorContext}, utils::ptr_to_str};

// 定义 Selector 容器

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
// 1. Basic Selectors
// =================================================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_all() -> *mut SelectorContext {
    ffi_try!({
        // polars::prelude::all() 在 0.50 中如果是 Selector 上下文，通常指 selectors::all()
        // 但这里我们需要显式使用 selector 的 all
        let s = all();
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

// 2. selector.exclude(["a", "b"])
#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_exclude(
    sel_ptr: *mut SelectorContext,
    names_ptr: *const *const c_char,
    len: usize
) -> *mut SelectorContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(sel_ptr) };
        
        // 构造排序列名列表 (Vec<str>)
        // Selector::exclude 接受 impl IntoVec<PlSmallStr>
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

// =================================================================
// 2. String Matchers (StartsWith, EndsWith, Contains, Regex)
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
        // Selector 没有直接的 StartsWith 变体，它是通过 Matches (Regex) 实现的
        // ^pattern
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
        // pattern$
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
        // Regex 默认就是包含匹配，除非加了 ^$
        let s = Selector::Matches(PlSmallStr::from_str(p));
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_match(
    pattern: *const c_char
) -> *mut SelectorContext {
    ffi_try!({
        // 直接传入 Regex 字符串
        let p = to_small_str(pattern).unwrap();
        let s = Selector::Matches(p);
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

// =================================================================
// 3. Type Selectors (DataTypeSelector)
// =================================================================

// 辅助：将单一 DataType 包装为 DataTypeSelector::AnyOf
#[inline]
fn dt_selector_single(dt: DataType) -> DataTypeSelector {
    DataTypeSelector::AnyOf(Arc::new([dt]))
}

// 核心映射逻辑：C# PlDataType (i32) -> Rust DataTypeSelector
fn map_i32_to_dtype_selector(kind: i32) -> DataTypeSelector {
    match kind {
        // 0: Unknown / SameAsInput
        // 在 Selector 上下文中，Unknown 意味着"啥也不是"，映射为 Empty 最安全
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
        // 注意：C# 的 Datetime 通常不带具体的 Unit/Timezone
        // 所以我们使用 Selector 的模糊匹配能力：匹配任意精度 (ns/us/ms) 和任意时区 (Any)
        14 => DataTypeSelector::Datetime(TimeUnitSet::all(), TimeZoneSet::Any),

        // Temporal - Time
        15 => dt_selector_single(DataType::Time),

        // Temporal - Duration
        // 同样匹配任意精度
        16 => DataTypeSelector::Duration(TimeUnitSet::all()),

        // Binary
        17 => dt_selector_single(DataType::Binary),

        // Null (用于选中全是 null 的列，或者 Null 类型的列)
        18 => dt_selector_single(DataType::Null),

        // Struct
        // 使用 DataTypeSelector::Struct 变体，它比 AnyOf([DataType::Struct(...)]) 更强
        // 因为它不关心 Struct 内部的字段结构，只要是 Struct 就选中
        19 => DataTypeSelector::Struct,

        // Float16 (Polars 较新的特性)
        // 20 => dt_selector_single(DataType::Float16),

        // 兜底
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

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_numeric() -> *mut SelectorContext {
    ffi_try!({
        // DataTypeSelector::Numeric
        let s = Selector::ByDType(DataTypeSelector::Numeric);
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

// =================================================================
// 4. Set Operations
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
        
        // Selector 的 Not 实现通常是 Wildcard - Self
        // 参考 impl Not for Selector: Self::Wildcard - self
        // 这里我们手动构建 Difference 或者直接利用 Rust 的 Not trait (如果实现了)
        // 源码显示: impl Not for Selector { fn not(self) -> Self { Self::Wildcard - self } }
        // 也就是 Difference(Wildcard, self)
        
        let res = Selector::Difference(
            Arc::new(Selector::Wildcard),
            Arc::new(l.inner)
        );
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: res })))
    })
}

// =================================================================
// 5. Bridges
// =================================================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_into_expr(
    sel_ptr: *mut SelectorContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(sel_ptr) };
        // Selector 实现了 Into<Expr>
        let expr: Expr = ctx.inner.into(); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_clone(
    sel_ptr: *mut SelectorContext
) -> *mut SelectorContext {
    ffi_try!({
        // 借用而不消耗
        let ctx = unsafe { &*sel_ptr };
        let new_sel = ctx.inner.clone();
        Ok(Box::into_raw(Box::new(SelectorContext { inner: new_sel })))
    })
}