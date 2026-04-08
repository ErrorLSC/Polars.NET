use polars::prelude::*;
use polars_arrow::Either;
use polars_core::utils::concat_df;
use std::ffi::{CStr, c_int};
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

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_from_schema(
    schema_ptr: *mut SchemaContext,
    length: usize
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*schema_ptr };
        
        let df = DataFrame::full_null(&ctx.schema, length);
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
// ==========================================
// DataFrame Ops
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_shrink_to_fit(df_ptr: *mut DataFrameContext) {
    ffi_try_void!({
        if !df_ptr.is_null() {
            let ctx = unsafe { &mut *df_ptr };
            // Shrink the memory footprint of the DataFrame in-place
            ctx.df.shrink_to_fit();
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_height(
    ptr: *mut DataFrameContext,
    out_height: *mut usize 
) -> bool {
    ffi_bool_try!({ 
        if ptr.is_null() { 
            polars_bail!(ComputeError: "DataFrame pointer is null"); 
        }
        let ctx = unsafe { &*ptr };
        unsafe { *out_height = ctx.df.height() };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_width(
    ptr: *mut DataFrameContext,
    out_width: *mut usize 
) -> bool {
    ffi_bool_try!({ 
        if ptr.is_null() { 
            polars_bail!(ComputeError: "DataFrame pointer is null"); 
        }
        let ctx = unsafe { &*ptr };
        unsafe { *out_width = ctx.df.width() };
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_estimated_size(
    ptr: *mut DataFrameContext,
    out_size: *mut usize
) -> bool {
    ffi_bool_try!({
        if ptr.is_null() {
            polars_bail!(ComputeError: "DataFrame pointer is null");
        }
        let ctx = unsafe { &*ptr };
        
        unsafe { *out_size = ctx.df.estimated_size() };
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_column_name(
    df_ptr: *mut DataFrameContext, 
    index: usize
) -> *mut c_char {
    ffi_try!({
        if df_ptr.is_null() {
            polars_bail!(ComputeError: "DataFrame pointer is null");
        }
        let ctx = unsafe { &*df_ptr };
        let cols = ctx.df.get_column_names();
    
        if index >= cols.len() {
            polars_bail!(OutOfBounds: "Column index {} is out of bounds for DataFrame of width {}", index, cols.len());
        }

        let name = cols[index].as_str();
        let c_str = CString::new(name)
            .map_err(|e| polars_err!(ComputeError: "Column name contains null byte: {}", e))?;

        Ok(c_str.into_raw())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_schema(
    df_ptr: *mut DataFrameContext,
) -> *mut SchemaContext {
    ffi_try!({
        if df_ptr.is_null() {
            polars_bail!(ComputeError: "DataFrame pointer is null");
        }
        
        let ctx = unsafe { &*df_ptr };
        let schema = ctx.df.schema();
        
        let schema_ctx = Box::new(SchemaContext { 
            schema: schema.to_owned()
        });

        Ok(Box::into_raw(schema_ctx))
    })
}
// --- Convenience Ops ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_drop_many(
    df_ptr: *mut DataFrameContext,
    names_ptr: *const *const c_char,
    names_len: usize,
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };

        let names_slice = unsafe { std::slice::from_raw_parts(names_ptr, names_len) };

        let mut names_set = PlHashSet::with_capacity(names_len);
        for &n_ptr in names_slice {
            let col_name = unsafe { CStr::from_ptr(n_ptr).to_string_lossy() };
            names_set.insert(PlSmallStr::from_str(&col_name)); 
        }

        let new_df = ctx.df.drop_many_amortized(&names_set);
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_drop_in_place(
    df_ptr: *mut DataFrameContext,
    name: *const c_char,
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &mut *df_ptr }; 
        let col_name = unsafe { CStr::from_ptr(name).to_string_lossy() };

        let col = ctx.df.drop_in_place(&col_name)?;

        let series = col.as_series()
            .cloned()
            .expect("Failed to cast Column to Series");

        Ok(Box::into_raw(Box::new(SeriesContext { series })))
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
pub extern "C" fn pl_df_unique(
    df: *mut DataFrame,
    subset: *const *const c_char, // String array ptr
    subset_len: usize,            // Array length
    keep_strategy: u8,
    maintain_order: bool,            
    slice_offset: i64,
    slice_len: usize,
    slice_valid: u8,              // 1 = use slice, 0 = ignore slice
) -> *mut DataFrame {
    let df = unsafe { &*df };

    // 1. Parse Subset (Option<Vec<String>>)
    let subset_vec: Option<Vec<String>> = if subset.is_null() || subset_len == 0 {
        None
    } else {
        let slice = unsafe { std::slice::from_raw_parts(subset, subset_len) };
        let vec = slice
            .iter()
            .map(|&p| unsafe { CStr::from_ptr(p).to_string_lossy().into_owned() })
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

    // 4. Call Polars based on maintain_order flag
    let res = if maintain_order {
        df.unique_stable(subset_vec.as_deref(), keep, slice)
    } else {
        df.unique::<Vec<String>, String>(subset_vec.as_deref(), keep, slice)
    };

    // 5. Handle Result
    match res {
        Ok(d) => Box::into_raw(Box::new(d)),
        Err(e) => {
            // Error handling machinery
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

        let res_df = ctx.df.vstack(&other_ctx.df)?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_extend(
    df_ptr: *mut DataFrameContext,
    other_ptr: *mut DataFrameContext,
) -> bool {
    ffi_bool_try!({
        if df_ptr.is_null() {
            polars_bail!(ComputeError: "Target DataFrame pointer is null");
        }
        if other_ptr.is_null() {
            polars_bail!(ComputeError: "DataFrame to extend pointer is null");
        }

        let target_ctx = unsafe { &mut *df_ptr };
        let other_ctx = unsafe { &*other_ptr };
        
        target_ctx.df.extend(&other_ctx.df)?;

        Ok(())
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
pub extern "C" fn pl_dataframe_explode(
    df: *mut DataFrame,
    cols: *const *const c_char,
    len: usize,
    empty_as_null: bool,
    keep_nulls: bool,
) -> *mut DataFrame {
    ffi_try!({
        let df = unsafe { &*df };
        
        let cols_slice = unsafe { std::slice::from_raw_parts(cols, len) };
        
        let names = cols_slice
            .iter()
            .map(|&ptr| unsafe { CStr::from_ptr(ptr).to_str().unwrap() });

        let options = ExplodeOptions {
            empty_as_null,
            keep_nulls
        };

        let result_df = df.explode(names, options)?;

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

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_is_duplicated(df_ptr: *mut DataFrameContext) -> *mut SeriesContext {
    ffi_try!({
        let df = unsafe { &(*df_ptr).df };
        let res = df.is_duplicated()?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_is_unique(df_ptr: *mut DataFrameContext) -> *mut SeriesContext {
    ffi_try!({
        let df = unsafe { &(*df_ptr).df };
        let res = df.is_unique()?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_rechunk(df_ptr: *mut DataFrameContext) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        let mut new_df = ctx.df.clone();
        
        new_df.rechunk_mut_par();

        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_align_chunks(df_ptr: *mut DataFrameContext) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        let mut new_df = ctx.df.clone();
        
        new_df.align_chunks_par();

        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_partition_by(
    df_ptr: *mut DataFrameContext,
    cols_ptr: *const *const c_char,
    cols_len: usize,
    maintain_order: bool,
    include_key: bool,
    out_len: *mut usize,
) -> *mut *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        let cols_slice = unsafe { std::slice::from_raw_parts(cols_ptr, cols_len) };
        let cols: Vec<String> = cols_slice
            .iter()
            .map(|&p| unsafe { CStr::from_ptr(p).to_string_lossy().into_owned() })
            .collect();

        let partitions = if maintain_order {
            ctx.df.partition_by_stable(cols, include_key)?
        } else {
            ctx.df.partition_by(cols, include_key)?
        };

        unsafe { *out_len = partitions.len() };

        let mut out_ptrs: Vec<*mut DataFrameContext> = partitions
            .into_iter()
            .map(|df| Box::into_raw(Box::new(DataFrameContext { df })))
            .collect();

        let ptr = out_ptrs.as_mut_ptr();
        std::mem::forget(out_ptrs);
        
        Ok(ptr)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_equals(
    df_ptr: *mut DataFrameContext,
    other_ptr: *mut DataFrameContext,
    null_equal: bool,
    out_result: *mut bool,
) -> c_int {
    ffi_eval_out_try!(out_result, {
        let ctx = unsafe { &*df_ptr };
        let other_ctx = unsafe { &*other_ptr };
        
        let is_eq = if null_equal {
            ctx.df.equals_missing(&other_ctx.df)
        } else {
            ctx.df.equals(&other_ctx.df)
        };
        
        Ok(is_eq)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_replace_column_at(
    df_ptr: *mut DataFrameContext,
    index: usize,
    series_ptr: *mut SeriesContext,
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &mut *df_ptr };
        let s_ctx = unsafe { &*series_ptr };
        
        let new_col = s_ctx.series.clone().into();
        ctx.df.replace_column(index, new_col)?;
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_replace(
    df_ptr: *mut DataFrameContext,
    name_ptr: *const std::os::raw::c_char,
    series_ptr: *mut SeriesContext,
) -> bool {
    ffi_bool_try!({
        let ctx = unsafe { &mut *df_ptr };
        let s_ctx = unsafe { &*series_ptr };
        
        let col_name = unsafe { CStr::from_ptr(name_ptr).to_string_lossy() };
        let new_col = s_ctx.series.clone().into();
        
        ctx.df.replace(&col_name, new_col)?;
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_dataframe_with_row_index(
    df_ptr: *mut DataFrameContext,
    name: *const c_char,
    offset_val: i64, 
) -> *mut DataFrameContext {
    ffi_try!({
       
        let ctx = unsafe { &mut *df_ptr  };
        
        let name_str = if name.is_null() {
            "index".to_string()
        } else {
            unsafe { CStr::from_ptr(name) }.to_string_lossy().into_owned()
        };
        
        let offset = if offset_val < 0 {
            None
        } else {
            Some(offset_val as u32)
        };
        
        let new_df = ctx.df.with_row_index(name_str.into(), offset)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_transpose(
    df_ptr: *mut DataFrameContext,
    keep_names_as_ptr: *const c_char,
    
    name_col_ptr: *const c_char,
    
    custom_names_ptr: *const *const c_char,
    custom_names_len: usize,
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &mut *df_ptr };

        // keep_names_as
        let keep_names_as = if keep_names_as_ptr.is_null() {
            None
        } else {
            Some(ptr_to_str(keep_names_as_ptr).unwrap_or(""))
        };

        // Either<String, Vec<String>>
        let new_col_names = if !custom_names_ptr.is_null() && custom_names_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(custom_names_ptr, custom_names_len) };
            let vec_names: Vec<String> = slice
                .iter()
                .map(|&p| ptr_to_str(p).unwrap_or("").to_string())
                .collect();
                
            Some(Either::Right(vec_names))
            
        } else if !name_col_ptr.is_null() {
            let col_name = ptr_to_str(name_col_ptr).unwrap_or("").to_string();
            
            Some(Either::Left(col_name))
            
        } else {
            None
        };

        let res_df = ctx.df.transpose(keep_names_as, new_col_names)?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_upsample(
    df_ptr: *mut DataFrameContext,
    time_column_ptr: *const c_char,
    every_ptr: *const c_char,
    group_by_ptr: *const *const c_char,
    group_by_len: usize,
    maintain_order: bool,
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };

        let time_column = ptr_to_str(time_column_ptr).unwrap_or("");

        let every_str = ptr_to_str(every_ptr).unwrap_or("1d");
        let every = polars::time::Duration::parse(every_str);

        let by: Vec<PlSmallStr> = if group_by_ptr.is_null() || group_by_len == 0 {
            Vec::new()
        } else {
            let slice = unsafe { std::slice::from_raw_parts(group_by_ptr, group_by_len) };
            slice
                .iter()
                .filter_map(|&p| ptr_to_str(p).ok().map(|s| PlSmallStr::from_str(s)))
                .collect()
        };

        let res_df = if maintain_order {
            ctx.df.upsample_stable(by, time_column, every)?
        } else {
            ctx.df.upsample(by, time_column, every)?
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_to_dummies(
    df_ptr: *mut DataFrameContext,
    columns_ptr: *const *const c_char,
    columns_len: usize,
    separator_ptr: *const c_char,
    drop_first: bool,
    drop_nulls: bool,
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };

        let separator = if separator_ptr.is_null() {
            None
        } else {
            Some(ptr_to_str(separator_ptr).unwrap_or("_"))
        };

        let res_df = if columns_ptr.is_null() || columns_len == 0 {
            ctx.df.to_dummies(separator, drop_first, drop_nulls)?
        } else {
            let slice = unsafe { std::slice::from_raw_parts(columns_ptr, columns_len) };
            
            let mut cols: Vec<&str> = Vec::with_capacity(columns_len);
            for &p in slice {
                if let Ok(s) = ptr_to_str(p) {
                    cols.push(s);
                }
            }
            
            ctx.df.columns_to_dummies(cols, separator, drop_first, drop_nulls)?
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}