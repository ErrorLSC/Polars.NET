use std::ffi::{CStr, c_char};
use polars::prelude::*;
use crate::{types::*, utils::parse_closed_window};
use polars::lazy::dsl::UnpivotArgsDSL;
use crate::utils::{consume_exprs_array, map_asof_strategy, map_coalesce, map_join_side, map_jointype, map_maintain_order, map_validation, parse_keep_strategy, ptr_to_str, ptr_to_vec_string};

// ==========================================
// Macro Definition
// ==========================================

/// LazyFrame -> Vec<Expr> -> LazyFrame
/// for select, with_columns
macro_rules! gen_lazy_vec_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            lf_ptr: *mut LazyFrameContext,
            exprs_ptr: *const *mut ExprContext,
            len: usize
        ) -> *mut LazyFrameContext {
            ffi_try!({
                // Consume LazyFrame handle
                let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
                
                // Take back the ownership of Exprs
                let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };

                // Transform
                let new_lf = lf_ctx.inner.$method(exprs);

                // Return new context
                Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
            })
        }
    };
}

/// LazyFrame -> Single Expr -> LazyFrame
/// for: filter
macro_rules! gen_lazy_single_expr_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            lf_ptr: *mut LazyFrameContext, 
            expr_ptr: *mut ExprContext
        ) -> *mut LazyFrameContext {
            ffi_try!({
                let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
                let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
                
                let new_lf = lf_ctx.inner.$method(expr_ctx.inner);
                
                Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
            })
        }
    };
}

// ==========================================
// Macro (Standard API)
// ==========================================

// --- Select / WithColumns ---
gen_lazy_vec_op!(pl_lazy_select, select);
gen_lazy_vec_op!(pl_lazy_with_columns, with_columns);

// --- Filter ---
gen_lazy_single_expr_op!(pl_lazy_filter, filter);

// ==========================================
// Rename
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_rename(
    lf_ptr: *mut LazyFrameContext,
    existing_ptr: *const *const c_char,
    existing_len: usize,
    new_ptr: *const *const c_char,
    new_len: usize,
    strict: bool,
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        let existing = unsafe { ptr_to_vec_string(existing_ptr, existing_len) };
        let new_cols = unsafe { ptr_to_vec_string(new_ptr, new_len) };

        let df = lf_ctx.inner.rename(existing, new_cols, strict);

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: df })))
    })
}

// ==========================================
// Slice
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_slice(
    lf_ptr: *mut LazyFrameContext,
    offset: i64,
    len: IdxSize, 
) -> *mut LazyFrame {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(lf_ptr) };
        
        let new_lf = ctx.inner.slice(offset, len);

        Ok(Box::into_raw(Box::new(new_lf)))
    })
}

// ==========================================
// Sort
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sort(
    lf_ptr: *mut LazyFrameContext,
    expr_ptrs: *const *mut ExprContext,
    expr_len: usize,
    descending_ptr: *const bool,
    descending_len: usize,
    nulls_last_ptr: *const bool,   
    nulls_last_len: usize,         
    maintain_order: bool           
) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(lf_ptr) };
        
        let mut exprs = Vec::with_capacity(expr_len);
        let ptr_slice = unsafe { std::slice::from_raw_parts(expr_ptrs, expr_len) };
        for &ptr in ptr_slice {
            let expr_ctx = unsafe { Box::from_raw(ptr) };
            exprs.push(expr_ctx.inner);
        }

        let desc_slice = unsafe { std::slice::from_raw_parts(descending_ptr, descending_len) };
        let descending = if descending_len == 1 && expr_len > 1 {
            vec![desc_slice[0]; expr_len]
        } else {
            desc_slice.to_vec()
        };

        let nulls_slice = unsafe { std::slice::from_raw_parts(nulls_last_ptr, nulls_last_len) };
        let nulls_last = if nulls_last_len == 1 && expr_len > 1 {
            vec![nulls_slice[0]; expr_len]
        } else {
            nulls_slice.to_vec()
        };

        let options = SortMultipleOptions::default()
            .with_order_descending_multi(descending)
            .with_nulls_last_multi(nulls_last)
            .with_maintain_order(maintain_order);

        let res_lf = ctx.inner.sort_by_exprs(exprs, options);

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: res_lf })))
    })
}

fn build_sort_options(descending: Vec<bool>) -> SortMultipleOptions {
    let len = descending.len();
    SortMultipleOptions {
        descending,
        nulls_last: vec![true; len], 
        multithreaded: true,
        maintain_order: false,
        limit:None
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_top_k(
    lf_ptr: *mut LazyFrameContext,
    k: u32,
    by_ptrs: *const *mut ExprContext,
    by_len: usize,
    reverse_ptr: *const bool, 
    reverse_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(lf_ptr) };
        
        let mut by_exprs = Vec::with_capacity(by_len);
        if by_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(by_ptrs, by_len) };
            for &p in slice {
                let e = unsafe { Box::from_raw(p) };
                by_exprs.push(e.inner);
            }
        }

        let mut reverse = Vec::with_capacity(reverse_len);
        if reverse_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(reverse_ptr, reverse_len) };
            reverse.extend_from_slice(slice);
        }

        let options = build_sort_options(reverse);

        let new_lf = ctx.inner.top_k(k, by_exprs, options);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_bottom_k(
    lf_ptr: *mut LazyFrameContext,
    k: u32,
    by_ptrs: *const *mut ExprContext,
    by_len: usize,
    reverse_ptr: *const bool,
    reverse_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(lf_ptr) };
        
        let mut by_exprs = Vec::with_capacity(by_len);
        if by_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(by_ptrs, by_len) };
            for &p in slice {
                let e = unsafe { Box::from_raw(p) };
                by_exprs.push(e.inner);
            }
        }

        let mut reverse = Vec::with_capacity(reverse_len);
        if reverse_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(reverse_ptr, reverse_len) };
            reverse.extend_from_slice(slice);
        }

        let options = build_sort_options(reverse);

        let new_lf = ctx.inner.bottom_k(k, by_exprs, options);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
// ==========================================
// GroupBy
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_groupby_agg(
    lf_ptr: *mut LazyFrameContext,
    keys_ptr: *const *mut ExprContext, keys_len: usize,
    aggs_ptr: *const *mut ExprContext, aggs_len: usize,
    having_ptr: *mut ExprContext,
    maintain_order: bool 
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let keys = unsafe { consume_exprs_array(keys_ptr, keys_len) };
        let aggs = unsafe { consume_exprs_array(aggs_ptr, aggs_len) };

        let mut group_by = if maintain_order {
            lf_ctx.inner.group_by_stable(keys)
        } else {
            lf_ctx.inner.group_by(keys)
        };

        if !having_ptr.is_null() {
            let having_expr = unsafe { Box::from_raw(having_ptr).inner };
            group_by = group_by.having(having_expr);
        }
        
        let res_lf = group_by.agg(aggs);

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner:res_lf })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_lazy_group_by_dynamic(
    lf_ptr: *mut LazyFrameContext,
    index_col: *const c_char,
    every: *const c_char,
    period: *const c_char,
    offset: *const c_char,
    label_idx: u8,         
    include_boundaries: bool,
    closed_window_idx: u8, 
    start_by_idx: u8,      
    // --- Keys & Aggs ---
    keys_ptr: *const *mut ExprContext, keys_len: usize,
    aggs_ptr: *const *mut ExprContext, aggs_len: usize,
    having_ptr: *mut ExprContext
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        let index_col_str = unsafe { CStr::from_ptr(index_col).to_str().unwrap() };
        let every_str = unsafe { CStr::from_ptr(every).to_str().unwrap() };
        let period_str = unsafe { CStr::from_ptr(period).to_str().unwrap() };
        let offset_str = unsafe { CStr::from_ptr(offset).to_str().unwrap() };

        let closed_window = parse_closed_window(closed_window_idx);

        let label = match label_idx {
            0 => Label::Left,
            1 => Label::Right,
            2 => Label::DataPoint,
            _ => Label::Left,
        };

        let start_by = match start_by_idx {
            0 => StartBy::WindowBound,
            1 => StartBy::DataPoint,
            2 => StartBy::Monday,
            3 => StartBy::Tuesday,
            4 => StartBy::Wednesday,
            5 => StartBy::Thursday,
            6 => StartBy::Friday,
            7 => StartBy::Saturday,
            8 => StartBy::Sunday,
            _ => StartBy::WindowBound,
        };

        let options = DynamicGroupOptions {
            index_column: PlSmallStr::from_str(index_col_str), 
            every: Duration::parse(every_str),
            period: Duration::parse(period_str),
            offset: Duration::parse(offset_str),
            label,    
            include_boundaries,
            closed_window,
            start_by,
        };

        let keys = unsafe { consume_exprs_array(keys_ptr, keys_len) };
        let aggs = unsafe { consume_exprs_array(aggs_ptr, aggs_len) };

        let mut group_by = lf_ctx.inner
            .group_by_dynamic(
                col(index_col_str), 
                keys, 
                options
            );

        if !having_ptr.is_null() {
            let having_expr = unsafe { Box::from_raw(having_ptr).inner };
            group_by = group_by.having(having_expr);
        }
        
        let new_lf = group_by.agg(aggs);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_group_by_rolling(
    lf_ptr: *mut LazyFrameContext,
    index_column_ptr: *const c_char, 
    period_ptr: *const c_char,
    offset_ptr: *const c_char,
    closed_code: u8,
    by_ptr: *const *mut ExprContext, 
    by_len: usize,
    aggs_ptr: *const *mut ExprContext, 
    aggs_len: usize,
    having_ptr: *mut ExprContext
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let by_exprs = unsafe { consume_exprs_array(by_ptr, by_len) };
        let aggs_exprs = unsafe { consume_exprs_array(aggs_ptr, aggs_len) };

        let index_col_str = ptr_to_str(index_column_ptr).unwrap_or("");
        let period_str = ptr_to_str(period_ptr).unwrap_or("");
        let offset_str = ptr_to_str(offset_ptr).unwrap_or("");

        let defaults = RollingGroupOptions::default();

        let period = if period_str.is_empty() {
            defaults.period
        } else {
            Duration::parse(period_str)
        };

        let offset = if offset_str.is_empty() {
            defaults.offset
        } else {
            Duration::parse(offset_str)
        };

        let closed_window = parse_closed_window(closed_code);

        let options = RollingGroupOptions {
            index_column: index_col_str.into(),
            period,
            offset,
            closed_window,
        };

        let index_expr = col(index_col_str);
 
        let mut group_by = lf_ctx.inner.rolling(index_expr, by_exprs, options);

        if !having_ptr.is_null() {
            let having_expr = unsafe { Box::from_raw(having_ptr).inner };
            group_by = group_by.having(having_expr);
        }
        
        let res_lf = group_by.agg(aggs_exprs);

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: res_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_explode(
    lf_ptr: *mut LazyFrameContext,
    selector_ptr: *mut SelectorContext,
    empty_as_null: bool,
    keep_nulls: bool,
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let sel_ctx = unsafe { Box::from_raw(selector_ptr) };

        let options = ExplodeOptions {
            empty_as_null,
            keep_nulls,
        };

        let new_lf = lf_ctx.inner.explode(sel_ctx.inner, options);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_unnest(
    lf_ptr: *mut LazyFrameContext,
    sel_ptr: *mut SelectorContext,
    separator_ptr: *const c_char 
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let sel_ctx = unsafe {  Box::from_raw(sel_ptr) }; 

        let sep = if separator_ptr.is_null() {
            None
        } else {
            let s = ptr_to_str(separator_ptr).unwrap();
            Some(PlSmallStr::from_str(s))
        };

        let new_lf = lf_ctx.inner.unnest(sel_ctx.inner, sep);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

// ==========================================
// Collect (LazyFrame -> DataFrame)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_collect(
    lf_ptr: *mut LazyFrameContext, 
    use_streaming: bool
) -> *mut DataFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let lf = lf_ctx.inner;

        let df = if use_streaming {
            lf.with_new_streaming(true).collect_with_engine(Engine::Streaming)?
        } else {
            lf.collect_with_engine(Engine::Auto)?
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_collect_streaming(lf_ptr: *mut LazyFrameContext) -> *mut DataFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        // Polars 0.50+ API: with_streaming(true).collect()
        let df = lf_ctx.inner
            .with_new_streaming(true)
            .collect_with_engine(Engine::Streaming)?;
            
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
// ==========================================
// Unpivot
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_unpivot(
    lf_ptr: *mut LazyFrameContext,
    index_ptr: *mut SelectorContext,
    on_ptr: *mut SelectorContext,
    variable_name_ptr: *const c_char,
    value_name_ptr: *const c_char
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let index_ctx = unsafe { Box::from_raw(index_ptr) };
        let on = if on_ptr.is_null() {
            None
        } else {
            let on_ctx = unsafe { Box::from_raw(on_ptr) };
            Some(on_ctx.inner)
        };

        let variable_name = if variable_name_ptr.is_null() { 
            None 
        } else { 
            Some(PlSmallStr::from_str(ptr_to_str(variable_name_ptr).unwrap())) 
        };
        
        let value_name = if value_name_ptr.is_null() { 
            None 
        } else { 
            Some(PlSmallStr::from_str(ptr_to_str(value_name_ptr).unwrap())) 
        };

        let args = UnpivotArgsDSL {
            index: index_ctx.inner, 
            on,       
            variable_name,
            value_name,
        };

        let new_lf = lf_ctx.inner.unpivot(args);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_pivot(
    lf_ptr: *mut LazyFrameContext,
    on_ptr: *mut SelectorContext,
    on_columns_ptr: *mut DataFrameContext,
    index_ptr: *mut SelectorContext,
    values_ptr: *mut SelectorContext,
    agg_expr_ptr: *mut ExprContext,
    agg_code: u8, 
    maintain_order: bool,
    separator_ptr: *const c_char,
) -> *mut LazyFrameContext {
    ffi_try!({
        // 1. Unbox Contexts
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let on_ctx = unsafe { Box::from_raw(on_ptr) };
        let index_ctx = unsafe { Box::from_raw(index_ptr) };
        let values_ctx = unsafe { Box::from_raw(values_ptr) };
        
        let on_columns_ctx = unsafe {&*on_columns_ptr};
        let on_columns = Arc::new(on_columns_ctx.df.clone());

        // 2. Handle Agg Expr
        let agg_expr = if !agg_expr_ptr.is_null() {
            let e_ctx = unsafe { Box::from_raw(agg_expr_ptr) };
            e_ctx.inner
        } else {
            let el = Expr::Element;
            match agg_code {
                1 => el.sum(),
                2 => el.min(),
                3 => el.max(),
                4 => el.mean(),
                5 => el.median(),
                6 => polars::prelude::len(), // Count
                7 => polars::prelude::len(), // Count
                8 => el.last(),
                _ => el.first(),
            }
        };

        // 3. Handle Separator String
        let separator = if separator_ptr.is_null() {
            PlSmallStr::EMPTY
        } else {
            PlSmallStr::from_str(ptr_to_str(separator_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?)
        };

        // 4. Call LazyFrame::pivot
        let new_lf = lf_ctx.inner.pivot(
            on_ctx.inner,
            on_columns,
            index_ctx.inner,
            values_ctx.inner,
            agg_expr,
            maintain_order,
            separator,
        );

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
// ==========================================
// Concat
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_concat(
    lfs_ptr: *const *mut LazyFrameContext, 
    len: usize,
    how: i32,                    // 0=Vertical, 1=Horizontal, 2=Diagonal
    // Common / UnionArgs
    parallel: bool,
    rechunk: bool,
    to_supertypes: bool,         
    maintain_order: bool,        
    // Horizontal / Strict
    strict: bool,                
    broadcast_unit_length: bool  
) -> *mut LazyFrameContext {
    ffi_try!({
        let mut lfs = Vec::with_capacity(len);
        let slice = unsafe { std::slice::from_raw_parts(lfs_ptr, len) };
        
        for &p in slice {
            let lf_ctx = unsafe { Box::from_raw(p) };
            lfs.push(lf_ctx.inner);
        }

        if lfs.is_empty() {
             return Err(PolarsError::ComputeError("Cannot concat empty list of LazyFrames".into()));
        }

        let new_lf = match how {
            // Vertical (Union)
            0 => {
                let args = UnionArgs {
                    parallel,
                    rechunk,
                    to_supertypes,
                    maintain_order,
                    strict, 
                    diagonal: false,
                    from_partitioned_ds: false, 
                };
                polars::prelude::concat(lfs, args)?
            },
            
            // Horizontal
            1 => {
                let options = HConcatOptions {
                    parallel,
                    strict, 
                    broadcast_unit_length,
                };
                polars::prelude::concat_lf_horizontal(lfs, options)?
            },

            // Diagonal (Union with diagonal=true)
            2 => {
                let args = UnionArgs {
                    parallel,
                    rechunk,
                    to_supertypes,
                    maintain_order,
                    strict,
                    diagonal: true, 
                    from_partitioned_ds: false,
                };
                polars::prelude::concat(lfs, args)?
            },

            _ => return Err(PolarsError::ComputeError("Invalid lazy concat strategy".into())),
        };
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

// ==========================================
// Join & Join As of
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_join(
    left_ptr: *mut LazyFrameContext,
    right_ptr: *mut LazyFrameContext,
    left_on_ptr: *const *mut ExprContext, left_on_len: usize,
    right_on_ptr: *const *mut ExprContext, right_on_len: usize,
    how_code: u8,
    suffix_ptr: *const c_char,
    validation_code: u8,
    coalesce_code: u8,
    maintain_order_code: u8,
    join_build_side_code: u8,
    nulls_equal: bool,
    slice_offset_ptr: *const i64, // Nullable pointer for Slice Offset
    slice_len: usize              // Slice Length
) -> *mut LazyFrameContext {
    ffi_try!({
        let left_ctx = unsafe { Box::from_raw(left_ptr) };
        let right_ctx = unsafe { Box::from_raw(right_ptr)};

        let how = map_jointype(how_code);

        let left_on = unsafe { consume_exprs_array(left_on_ptr, left_on_len) };
        let right_on = unsafe { consume_exprs_array(right_on_ptr, right_on_len) };

        // 1. Map Suffix
        let suffix = if suffix_ptr.is_null() {
            None
        } else {
            let s_str = ptr_to_str(suffix_ptr)
                .map_err(|e| PolarsError::ComputeError(format!("Invalid suffix string: {}", e).into()))?;
            Some(PlSmallStr::from_str(s_str))
        };

        // 2. Map Enums
        let validation = map_validation(validation_code);
        let coalesce = map_coalesce(coalesce_code);
        let maintain_order = map_maintain_order(maintain_order_code);
        let build_side = if join_build_side_code == 0 {
            None
        } else {
            Some(map_join_side(join_build_side_code))
        };

        // 3. Map Slice (Option<(i64, usize)>)
        let slice = if slice_offset_ptr.is_null() {
            None
        } else {
            Some((unsafe { *slice_offset_ptr }, slice_len))
        };

        // 4. Construct JoinArgs
        let args = JoinArgs {
            how,
            validation,
            suffix,
            slice,
            nulls_equal,
            coalesce,
            maintain_order,
            build_side
        };
        
        let res_lf = left_ctx.inner.join(right_ctx.inner, left_on, right_on, args);

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: res_lf })))
    })
}
fn exprs_to_names(exprs: &[Expr]) -> PolarsResult<Vec<PlSmallStr>> {
    let mut names = Vec::new();
    for e in exprs {
        // e.meta().root_names() returns Vec<PlSmallStr>
        let roots = e.clone().meta().root_names();
        
        names.extend_from_slice(&roots);
    }
    Ok(names)
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_join_asof(
    left_ptr: *mut LazyFrameContext,
    right_ptr: *mut LazyFrameContext,
    left_on_ptr: *const *mut ExprContext, left_on_len: usize,
    right_on_ptr: *const *mut ExprContext, right_on_len: usize,
    
    // --- AsOf Args ---
    left_by_ptr: *const *mut ExprContext, left_by_len: usize,
    right_by_ptr: *const *mut ExprContext, right_by_len: usize,
    strategy_code: u8,
    
    // --- Tolerance ---
    tolerance_str_ptr: *const c_char, // Duration String (e.g. "2h")
    tolerance_i64_ptr: *const i64,    // Int Scalar (e.g. Timestamp, Int)
    tolerance_f64_ptr: *const f64,    // Float Scalar (e.g. Float)
    
    allow_eq: bool,
    check_sorted: bool,
    
    // --- JoinArgs ---
    suffix_ptr: *const c_char,
    validation_code: u8,
    coalesce_code: u8,
    maintain_order_code: u8,
    join_build_side_code: u8,
    nulls_equal: bool,
    slice_offset_ptr: *const i64,     // Slice Offset
    slice_len: usize                  // Slice Len
    
) -> *mut LazyFrameContext {
    ffi_try!({
        let left_ctx = unsafe { Box::from_raw(left_ptr) };
        let right_ctx = unsafe { Box::from_raw(right_ptr) };

        let left_on = unsafe { consume_exprs_array(left_on_ptr, left_on_len) };
        let right_on = unsafe { consume_exprs_array(right_on_ptr, right_on_len) };
        
        let by_left_exprs = unsafe { consume_exprs_array(left_by_ptr, left_by_len) };
        let by_right_exprs = unsafe { consume_exprs_array(right_by_ptr, right_by_len) };

        let left_by_names = if by_left_exprs.is_empty() { None } else { Some(exprs_to_names(&by_left_exprs)?) };
        let right_by_names = if by_right_exprs.is_empty() { None } else { Some(exprs_to_names(&by_right_exprs)?) };

        // 1. Strategy
        let strategy = map_asof_strategy(strategy_code);

        // 2. Tolerance Logic (Priority: Str > I64 > F64)
        let mut tolerance: Option<Scalar> = None;
        let mut tolerance_str: Option<PlSmallStr> = None;

        if !tolerance_str_ptr.is_null() {
            let s = ptr_to_str(tolerance_str_ptr).unwrap();
            tolerance_str = Some(PlSmallStr::from_str(s));
        } else if !tolerance_i64_ptr.is_null() {
             let v = unsafe { *tolerance_i64_ptr };
             tolerance = Some(Scalar::new(DataType::Int64, AnyValue::Int64(v)));
        } else if !tolerance_f64_ptr.is_null() {
             let v = unsafe { *tolerance_f64_ptr };
             tolerance = Some(Scalar::new(DataType::Float64, AnyValue::Float64(v)));
        }

        let asof_options = AsOfOptions {
            strategy,
            tolerance,
            tolerance_str,
            left_by: left_by_names,
            right_by: right_by_names,
            allow_eq,
            check_sortedness: check_sorted,
        };

        // 3. JoinArgs Common
        let suffix = if suffix_ptr.is_null() {
            None
        } else {
            let s = ptr_to_str(suffix_ptr).unwrap();
            Some(PlSmallStr::from_str(s))
        };
        
        let validation = map_validation(validation_code);
        let coalesce = map_coalesce(coalesce_code);
        let maintain_order = map_maintain_order(maintain_order_code);
        let build_side = if join_build_side_code == 0 {
            None
        } else {
            Some(map_join_side(join_build_side_code))
        };

        let slice = if slice_offset_ptr.is_null() {
            None
        } else {
            Some((unsafe { *slice_offset_ptr }, slice_len))
        };

        let args = JoinArgs {
            how: JoinType::AsOf(Box::new(asof_options)),
            validation,
            suffix,
            slice,
            nulls_equal,
            coalesce,
            maintain_order,
            build_side
        };

        let new_lf = left_ctx.inner.join(right_ctx.inner, left_on, right_on, args);

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

// ==========================================
// Join Where (Inequi-Join / Predicate Join)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_join_where(
    left_ptr: *mut LazyFrameContext,
    right_ptr: *mut LazyFrameContext,
    predicates_ptr: *const *mut ExprContext, predicates_len: usize,
    how_code: u8,
    suffix_ptr: *const c_char,
    validation_code: u8,
    coalesce_code: u8,
    maintain_order_code: u8,
    nulls_equal: bool,
) -> *mut LazyFrameContext {
    ffi_try!({
        // Consume contexts and take ownership
        let left_ctx = unsafe { Box::from_raw(left_ptr) };
        let right_ctx = unsafe { Box::from_raw(right_ptr) };
        
        // Consume predicates array
        let predicates = unsafe { consume_exprs_array(predicates_ptr, predicates_len) };

        // Map Enums (using existing utilities from utils.rs)
        let how = map_jointype(how_code);
        let validation = map_validation(validation_code);
        let coalesce = map_coalesce(coalesce_code);
        let maintain_order = map_maintain_order(maintain_order_code);

        // Construct the JoinBuilder
        let mut builder = JoinBuilder::new(left_ctx.inner)
            .with(right_ctx.inner)
            .how(how)
            .validate(validation)
            .join_nulls(nulls_equal)
            .coalesce(coalesce)
            .maintain_order(maintain_order);

        // Apply suffix if provided
        if !suffix_ptr.is_null() {
            let s_str = ptr_to_str(suffix_ptr)
                .map_err(|e| PolarsError::ComputeError(format!("Invalid suffix string: {}", e).into()))?;
            builder = builder.suffix(PlSmallStr::from_str(s_str));
        }

        // 6. Finish with join predicates
        let res_lf = builder.join_where(predicates);

        // 7. Return the new LazyFrameContext
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: res_lf })))
    })
}

// ==========================================
// Merge Sorted
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_merge_sorted(
    lf_ptr: *mut LazyFrameContext,
    other_ptr: *mut LazyFrameContext,
    key_ptr: *const std::os::raw::c_char,
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let other_ctx = unsafe { Box::from_raw(other_ptr) };

        let key_str = ptr_to_str(key_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let new_lf = lf_ctx.inner.merge_sorted(
            other_ctx.inner, 
            key_str
        )?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
// ==========================================
// Ops
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_drop(lf_ptr: *mut LazyFrameContext, sel_ptr: *mut SelectorContext,) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(lf_ptr) };
        let sel_ctx = unsafe {  Box::from_raw(sel_ptr) }; 
        
        let new_lf = ctx.inner.drop(sel_ctx.inner);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_drop_nulls(
    lf_ptr: *mut LazyFrameContext,
    sel_ptr: *mut SelectorContext, 
) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(lf_ptr)};
        
        let subset = if sel_ptr.is_null() {
            None
        } else {
            let sel_ctx = unsafe { Box::from_raw(sel_ptr) };
            Some(sel_ctx.inner)
        };
        
        let new_lf = ctx.inner.drop_nulls(subset);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_drop_nans(
    lf_ptr: *mut LazyFrameContext,
    sel_ptr: *mut SelectorContext, 
) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(lf_ptr)};
        
        let subset = if sel_ptr.is_null() {
            None
        } else {
            let sel_ctx = unsafe { Box::from_raw(sel_ptr) };
            Some(sel_ctx.inner)
        };
        
        let new_lf = ctx.inner.drop_nans(subset);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_unique(
    lf_ptr: *mut LazyFrameContext,       
    selector: *mut SelectorContext, 
    keep_strategy: u8,
    maintain_order: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf = unsafe { Box::from_raw(lf_ptr).inner };

        let subset = if selector.is_null() {
            None
        } else {
            let sel_ctx = unsafe { Box::from_raw(selector) };
            Some(sel_ctx.inner) 
        };
    
        let keep = parse_keep_strategy(keep_strategy);

        let new_lf = if maintain_order {
            lf.unique_stable(subset, keep)
        } else {
            lf.unique(subset, keep)
        };

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_lazyframe_get_schema(lf_ptr: *mut LazyFrameContext) -> *mut SchemaContext {
    ffi_try!({
        if lf_ptr.is_null() {
            return Ok(std::ptr::null_mut());
        }
        
        let ctx = unsafe { &mut *lf_ptr };
        
        let schema_ref = ctx.inner.collect_schema().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        Ok(Box::into_raw(Box::new(SchemaContext { schema: schema_ref })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_explain(lf_ptr: *mut LazyFrameContext, optimized: bool) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &*lf_ptr };
        
        let plan_str = ctx.inner.explain(optimized)?;
        
        Ok(std::ffi::CString::new(plan_str).unwrap().into_raw())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_clone(lf_ptr: *mut LazyFrameContext) -> *mut LazyFrameContext {
    let ctx = unsafe { &*lf_ptr };
    
    let new_lf = ctx.inner.clone();
    
    Box::into_raw(Box::new(LazyFrameContext { inner: new_lf }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_frame_free(ptr: *mut LazyFrameContext) {
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}
