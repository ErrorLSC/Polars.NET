use polars::prelude::*;
use polars_core::utils::concat_df;
use std::ffi::CStr;
use std::{ffi::CString, os::raw::c_char};
use crate::types::*;
use polars::functions::{concat_df_horizontal,concat_df_diagonal};
use crate::utils::{parse_keep_strategy, ptr_to_str};

// ==========================================
// Memory Safety
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_free(ptr: *mut DataFrameContext) {
    ffi_try_void!({
        if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}
// ==========================================
// Slice
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_slice(
    df: *mut DataFrame,
    offset: i64,
    length: usize,
) -> *mut DataFrame {
    ffi_try!({
        let df = unsafe { &*df };
        let result_df = df.slice(offset, length);

        Ok(Box::into_raw(Box::new(result_df)))
    })
}
// ==========================================
// DataFrame Ops
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_height(ptr: *mut DataFrameContext) -> usize {
    if ptr.is_null() { return 0; }
    let ctx = unsafe { &*ptr };
    ctx.df.height()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_width(ptr: *mut DataFrameContext) -> usize {
    if ptr.is_null() { return 0; }
    let ctx = unsafe { &*ptr };
    ctx.df.width()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_column_name(
    df_ptr: *mut DataFrameContext, 
    index: usize
) -> *mut c_char {
    let ctx = unsafe { &*df_ptr };
    let cols = ctx.df.get_column_names();
    
    if index >= cols.len() {
        return std::ptr::null_mut();
    }

    CString::new(cols[index].as_str()).unwrap().into_raw()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_schema(
    df_ptr: *mut DataFrameContext,
) -> *mut SchemaContext {
    let _df = unsafe { &*df_ptr };

    let schema = unsafe { &*df_ptr }.df.schema();
    
    Box::into_raw(Box::new(SchemaContext { 
        schema: schema.to_owned() 
    }))
}

// --- Convenience Ops ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_drop(df_ptr: *mut DataFrameContext, name: *const c_char) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let col_name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        
        let new_df = ctx.df.drop(&col_name)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_rename(df_ptr: *mut DataFrameContext, old: *const c_char, new: *const c_char) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let old_name = unsafe { CStr::from_ptr(old).to_string_lossy() };
        let new_name = unsafe { CStr::from_ptr(new).to_string_lossy() };

        // Clone + Rename
        let mut new_df = ctx.df.clone();
        new_df.rename(&old_name, PlSmallStr::from_str(&new_name))?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_rename_many(
    df_ptr: *mut DataFrameContext,
    old_names_ptr: *const *const c_char,
    new_names_ptr: *const *const c_char,
    count: usize,
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };

        let old_ptrs = unsafe { std::slice::from_raw_parts(old_names_ptr, count) };
        let new_ptrs = unsafe { std::slice::from_raw_parts(new_names_ptr, count) };

        let mut renames = Vec::with_capacity(count);
        for i in 0..count {
            let old_str = unsafe { CStr::from_ptr(old_ptrs[i]).to_string_lossy().into_owned() };
            let new_str = unsafe { CStr::from_ptr(new_ptrs[i]).to_string_lossy().into_owned() };
            
            renames.push((old_str, PlSmallStr::from_str(&new_str)));
        }

        let mut new_df = ctx.df.clone();

        new_df.rename_many(
            renames.iter().map(|(old, new)| (old.as_str(), new.clone()))
        )?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_drop_nulls(df_ptr: *mut DataFrameContext, subset: *const *const c_char, len: usize) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        let new_df = if subset.is_null() || len == 0 {
            ctx.df.drop_nulls::<String>(None)? 
        } else {
            let slice = unsafe { std::slice::from_raw_parts(subset, len) };
            let cols: Vec<String> = slice.iter()
                .map(|&p| unsafe { CStr::from_ptr(p).to_string_lossy().to_string() })
                .collect();
            ctx.df.drop_nulls(Some(&cols))?
        };
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}



#[unsafe(no_mangle)]
pub extern "C" fn pl_df_unique_stable(
    df: *mut DataFrame,
    subset: *const *const c_char, // String array ptr
    subset_len: usize,            // Array length
    keep_strategy: u8, // "first", "last", "any", "none"
    slice_offset: i64,
    slice_len: usize,
    slice_valid: u8,              // 1 = use slice, 0 = ignore slice
) -> *mut DataFrame {
    let df = unsafe { &*df };

    // 1. Parse Subset (Option<&[String]>)
    let subset_vec: Option<Vec<String>> = if subset.is_null() || subset_len == 0 {
        None
    } else {
        let slice = unsafe { std::slice::from_raw_parts(subset, subset_len) };
        let vec = slice
            .iter()
            .map(|&p| unsafe { CStr::from_ptr(p).to_string_lossy().to_string() })
            .collect();
        Some(vec)
    };

    // 2. Parse Keep Strategy
    let keep = parse_keep_strategy(keep_strategy);

    // 3. Parse Slice
    let slice = if slice_valid != 0 {
        Some((slice_offset, slice_len))
    } else {
        None
    };

    // 4. Call Polars
    let res = df.unique_stable(
        subset_vec.as_deref(),
        keep, 
        slice
    );

    match res {
        Ok(d) => Box::into_raw(Box::new(d)),
        Err(e) => {
            // Error handling machinery (assuming you have one, or just panic for now)
            eprintln!("Polars Error: {}", e);
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_sample_n(
    df_ptr: *mut DataFrameContext, 
    n: usize, 
    replacement: bool, 
    shuffle: bool, 
    seed: *const u64
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let s = if seed.is_null() { None } else { Some(unsafe { *seed }) };
        
        let new_df = ctx.df.sample_n_literal(n, replacement, shuffle, s)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_sample_frac(
    df_ptr: *mut DataFrameContext, 
    frac: f64, 
    replacement: bool, 
    shuffle: bool, 
    seed: *const u64
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let s = if seed.is_null() { None } else { Some(unsafe { *seed }) };
        
        let height = ctx.df.height();
        let n = (height as f64 * frac) as usize;
        
        let new_df = ctx.df.sample_n_literal(n, replacement, shuffle, s)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_clone(ptr: *mut DataFrameContext) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        
        let new_df = ctx.df.clone();
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}
// --- Scalar Access ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_i64(
    df_ptr: *mut DataFrameContext, 
    col_name_ptr: *const c_char, 
    row_index: usize,
    out_val: *mut i64 
) -> bool { 
    let ctx = unsafe { &*df_ptr };
    let col_name = ptr_to_str(col_name_ptr).unwrap_or("");
    
    let col = match ctx.df.column(col_name) {
        Ok(c) => c,
        Err(_) => return false,
    };

    match col.get(row_index) {
        Ok(val) => match val {
            AnyValue::Int64(v) => { unsafe { *out_val = v }; true },
            AnyValue::Int32(v) => { unsafe { *out_val = v as i64 }; true },
            AnyValue::Int16(v) => { unsafe { *out_val = v as i64 }; true },
            AnyValue::Int8(v) =>  { unsafe { *out_val = v as i64 }; true },
            AnyValue::UInt64(v) => { 
                // i64::MAX 9,223,372,036,854,775,807
                if v > (i64::MAX as u64) {
                    // Overflow
                    // return false
                    false 
                } else {
                    unsafe { *out_val = v as i64 }; 
                    true 
                }
            },
            AnyValue::UInt32(v) => { unsafe { *out_val = v as i64 }; true }, 
            AnyValue::UInt16(v) => { unsafe { *out_val = v as i64 }; true },
            AnyValue::UInt8(v) =>  { unsafe { *out_val = v as i64 }; true },
            _ => false, 
        },
        Err(_) => false 
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_f64(
    df_ptr: *mut DataFrameContext, 
    col_name_ptr: *const c_char, 
    row_index: usize,
    out_val: *mut f64
) -> bool {
    let ctx = unsafe { &*df_ptr };
    let col_name = ptr_to_str(col_name_ptr).unwrap_or("");
    
    let col = match ctx.df.column(col_name) {
        Ok(c) => c,
        Err(_) => return false,
    };

    match col.get(row_index) {
        Ok(val) => match val {
            AnyValue::Float64(v) => { unsafe { *out_val = v }; true },
            AnyValue::Float32(v) => { unsafe { *out_val = v as f64 }; true },
            
            AnyValue::Int64(v) => { unsafe { *out_val = v as f64 }; true },
            AnyValue::Int32(v) => { unsafe { *out_val = v as f64 }; true },
            _ => false, 
        },
        Err(_) => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_string(
    df_ptr: *mut DataFrameContext, 
    col_name_ptr: *const c_char, 
    row_index: usize
) -> *mut c_char {
    let ctx = unsafe { &*df_ptr };
    let col_name = ptr_to_str(col_name_ptr).unwrap_or("");
    
    match ctx.df.column(col_name) {
        Ok(col) => match col.get(row_index) {
            Ok(AnyValue::Null) => std::ptr::null_mut(),

            Ok(AnyValue::String(s)) => CString::new(s).unwrap().into_raw(),
            Ok(AnyValue::StringOwned(s)) => CString::new(s.as_str()).unwrap().into_raw(),
            
            Ok(v) => CString::new(v.to_string()).unwrap().into_raw(),
            
            Err(_) => std::ptr::null_mut()
        },
        Err(_) => std::ptr::null_mut()
    }
}

// ==========================================
// Head/Tail
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_head(df_ptr: *mut DataFrameContext, n: usize) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let res_df = ctx.df.head(Some(n));
        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_tail(df_ptr: *mut DataFrameContext, n: usize) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let res_df = ctx.df.tail(Some(n));
        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}
// ==========================================
// Pivot & Unpivot
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_pivot(
    df_ptr: *mut DataFrameContext,
    on_ptr: *mut SelectorContext,
    index_ptr: *mut SelectorContext,
    values_ptr: *mut SelectorContext,
    agg_expr_ptr: *mut ExprContext,
    agg_code: u8,        
    maintain_order: bool, 
    sort_columns: bool,   
    separator_ptr: *const c_char,
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        // 1. Unpack Selectors
        let on_ctx = unsafe { Box::from_raw(on_ptr) };
        let index_ctx = unsafe { Box::from_raw(index_ptr) };
        let values_ctx = unsafe { Box::from_raw(values_ptr) };
        
        // 2. Values Check
        let schema = ctx.df.schema();
        let ignored = PlHashSet::new();
        let values_names_set = values_ctx.inner.into_columns(&schema, &ignored)?;
        let values_names: Vec<&str> = values_names_set.iter().map(|s| s.as_str()).collect();

        // Ensure we have values to pivot on
        if agg_expr_ptr.is_null() && values_names.is_empty() {
             return Err(PolarsError::ComputeError("Pivot requires at least one value column.".into()));
        }

        // 3. Build Agg Expr
        let agg_expr = if !agg_expr_ptr.is_null() {
            // Case A: Custom Expr from C#
            // IMPORTANT: The C# side MUST use Col("").Agg() or similar context-free expr
            // if values are specified. Your test uses Col("").First(), which is perfect.
            let e_ctx = unsafe { Box::from_raw(agg_expr_ptr) };
            e_ctx.inner
        } else {
            // Case B: Enum Mode (Standard Pivot)
            // [FIX]: Use col("") (empty string) as the column selector.
            // This tells Polars "use the column defined in the 'values' argument"
            // without triggering the "explicit column reference" error.
            let el = Expr::Element;

            match agg_code {
                1 => el.sum(),
                2 => el.min(),
                3 => el.max(),
                4 => el.mean(),
                5 => el.median(),
                6 => polars::prelude::len(), // Count (*)
                7 => polars::prelude::len(), // Count
                8 => el.last(),
                _ => el.first(),
            }
        };

        // 4. Separator
        let separator = if separator_ptr.is_null() {
            PlSmallStr::EMPTY 
        } else {
            PlSmallStr::from_str(ptr_to_str(separator_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?)
        };

        // 5. Prepare Headers (on_columns)
        let on_names_set = on_ctx.inner.into_columns(&schema, &ignored)?;
        let on_names: Vec<&str> = on_names_set.iter().map(|s| s.as_str()).collect();
        let on_df = ctx.df.select(&on_names)?;
        let mut on_columns = on_df.unique_stable(None, UniqueKeepStrategy::Any, None)?;
        
        if sort_columns {
            on_columns = on_columns.sort(on_names, SortMultipleOptions::default())?;
        }

        // 6. Execute Pivot
        // [FIX]: We pass the REAL values selector (not empty).
        // Since agg_expr uses col("") (implicit ref), Polars will accept both.
        let new_df = ctx.df.clone()
            .lazy()
            .pivot(
                on_ctx.inner,
                Arc::new(on_columns),
                index_ctx.inner,
                values_ctx.inner, // Pass the actual values selector
                agg_expr,
                maintain_order,
                separator,
            )
            .collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}
// ==========================================
// Concat
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_concat(
    dfs_ptr: *const *mut DataFrameContext,
    len: usize,
    how: u8, // 0=Vertical, 1=Horizontal, 2=Diagonal
    check_duplicates: bool,
    strict: bool,           // Horizontal : if true，different height will return exception
    unit_length_as_scalar: bool // Horizontal : choose whether broadcast length 1 scalar column 
) -> *mut DataFrameContext {
    ffi_try!({
        if len == 0 {
            return Ok(Box::into_raw(Box::new(DataFrameContext { df: DataFrame::default() })));
        }

        let slice = unsafe { std::slice::from_raw_parts(dfs_ptr, len) };

        let mut dfs: Vec<DataFrame> = Vec::with_capacity(len);
        for &p in slice {
            let ctx = unsafe { Box::from_raw(p) };
            dfs.push(ctx.df);
        }

        let out_df = match how {
            0 => concat_df(&dfs)?,
            
            1 => concat_df_horizontal(&dfs, 
                check_duplicates, 
                strict, 
                unit_length_as_scalar)?,
            
            2 => concat_df_diagonal(&dfs)?,
            
            _ => return Err(PolarsError::ComputeError("Invalid concat strategy".into())),
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df: out_df })))
    })
}

// ==========================================
// Vstack & Hstack
// ==========================================

/// Horizontal stack: Appends columns to the DataFrame.
/// Returns a new DataFrame.
#[unsafe(no_mangle)]
pub extern "C" fn pl_hstack(
    df_ptr: *mut DataFrameContext,
    cols_ptr: *const *mut SeriesContext,
    len: usize,
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        let slice = unsafe { std::slice::from_raw_parts(cols_ptr, len) };
        let mut columns = Vec::with_capacity(len);

        for &p in slice {
            if !p.is_null() {
                let s_ctx = unsafe { &*p };
                // Clone the series to ensure we don't steal ownership from the caller
                columns.push(s_ctx.series.clone().into());
            }
        }

        let res_df = ctx.df.hstack(&columns)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}

/// Vertical stack: Appends rows from another DataFrame to this one.
/// Returns a new DataFrame (polars::vstack clones internally).
#[unsafe(no_mangle)]
pub extern "C" fn pl_vstack(
    df_ptr: *mut DataFrameContext,
    other_ptr: *mut DataFrameContext,
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let other_ctx = unsafe { &*other_ptr };

        // vstack in polars-core returns a PolarsResult<DataFrame> (it clones self internally)
        let res_df = ctx.df.vstack(&other_ctx.df)?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}

// ==========================================
// Unnest
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_unnest(
    df: *mut DataFrame, 
    cols: *const *const c_char, 
    len: usize,
    separator: *const c_char
) -> *mut DataFrame {
    ffi_try!({
        let df = unsafe { &*df };
        
        let cols_slice = unsafe { std::slice::from_raw_parts(cols, len) };
        
        let names = cols_slice
            .iter()
            .map(|&ptr| unsafe { CStr::from_ptr(ptr).to_str().unwrap() });

        let sep_opt = if separator.is_null() {
            None
        } else {
            unsafe { Some(CStr::from_ptr(separator).to_str().unwrap()) }
        };

        let result_df = df.unnest(names, sep_opt)?;

        Ok(Box::into_raw(Box::new(result_df)))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_column(
    ptr: *mut DataFrameContext, 
    name: *const c_char
) -> *mut SeriesContext {
    ffi_try!({  
        let ctx = unsafe { &*ptr };
        let name_str = ptr_to_str(name).unwrap_or("");
        
        match ctx.df.column(name_str) {
            Ok(column) => {

                let s = column.as_materialized_series().clone();
                
                Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
            },
            Err(_) => Ok(std::ptr::null_mut())
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_column_at(
    ptr: *mut DataFrameContext, 
    index: usize
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        
        match ctx.df.select_at_idx(index) {
            Some(column) => {
                let s = column.as_materialized_series().clone();
                Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
            },
            None => Ok(std::ptr::null_mut())
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_to_frame(ptr: *mut SeriesContext) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let s = ctx.series.clone();
        let height = s.len();
        let df = DataFrame::new(height,vec![s.into()]).unwrap_or_default();
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

// Build DataFrame from Series Array
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_new(
    columns_ptr: *const *mut SeriesContext,
    len: usize,
) -> *mut DataFrameContext {
    ffi_try!({
        if columns_ptr.is_null() || len == 0 {
            return Ok(Box::into_raw(Box::new(DataFrameContext { df: DataFrame::default() })));
        }

        let slice = unsafe { std::slice::from_raw_parts(columns_ptr, len) };
        let mut series_vec = Vec::with_capacity(len);

        for &ptr in slice {
            if !ptr.is_null() {
                let ctx = unsafe { &*ptr };
                series_vec.push(ctx.series.clone().into());
            }
        }

        let height = series_vec.first().map(|s:&Column| s.len()).unwrap_or(0);

        let df = DataFrame::new(height, series_vec)?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_lazy(df_ptr: *mut DataFrameContext) -> *mut LazyFrameContext {
    let ctx = unsafe { &*df_ptr };
    let inner = ctx.df.clone().lazy();
    
    Box::into_raw(Box::new(LazyFrameContext { inner }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_to_string(df_ptr: *mut DataFrameContext) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &mut *df_ptr };
        let mut s = ctx.df.to_string();
        
        if s.contains('\0') {
            s = s.replace('\0', "␀"); 
        }
        
        let c_str = CString::new(s).expect("String sanitization failed");
        Ok(c_str.into_raw())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_hash_rows(
    df_ptr: *mut DataFrameContext,
    seed: u64,
    has_seed: bool,
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &mut *df_ptr };
        
        let hasher_builder = if has_seed {
            Some(PlSeedableRandomStateQuality::seed_from_u64(seed))
        } else {
            None
        };
        
        let chunked_array = ctx.df.hash_rows(hasher_builder)?;
        
        let mut series = chunked_array.into_series();
        series.rename("".into());
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: series })))
    })
}