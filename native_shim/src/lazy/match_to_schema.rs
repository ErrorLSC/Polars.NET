use polars::lazy::dsl::{ExtraColumnsPolicy, MissingColumnsPolicy, MatchToSchemaPerColumn};
use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::c_char;
use polars::prelude::*;

use crate::types::{ExprContext, LazyFrameContext, SchemaContext};

fn parse_missing(policy_type: u8, expr_ptr: *mut ExprContext) -> MissingColumnsPolicyOrExpr {
    match policy_type {
        0 => MissingColumnsPolicyOrExpr::Insert,
        1 => MissingColumnsPolicyOrExpr::Raise,
        2 => {
            if !expr_ptr.is_null() {
                let expr = unsafe { Box::from_raw(expr_ptr) };
                MissingColumnsPolicyOrExpr::InsertWith(expr.inner)
            } else {
                MissingColumnsPolicyOrExpr::Raise
            }
        }
        _ => MissingColumnsPolicyOrExpr::Raise,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_match_to_schema(
    lf_ptr: *mut LazyFrameContext,
    schema_ptr: *mut SchemaContext,
    extra_columns_code: u8,
    
    def_missing_type: u8,
    def_missing_expr: *mut ExprContext,
    def_missing_struct: u8,
    def_extra_struct: u8,
    def_int_cast: u8,
    def_float_cast: u8,
    
    ov_names: *const *const c_char,
    ov_missing_type: *const u8,
    ov_missing_expr: *const *mut ExprContext,
    ov_missing_struct: *const u8,
    ov_extra_struct: *const u8,
    ov_int_cast: *const u8,
    ov_float_cast: *const u8,
    ov_len: usize,
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let schema_ctx = unsafe { &*schema_ptr };

        let extra_columns = match extra_columns_code {
            1 => ExtraColumnsPolicy::Ignore,
            _ => ExtraColumnsPolicy::Raise,
        };

        let default_config = MatchToSchemaPerColumn {
            missing_columns: parse_missing(def_missing_type, def_missing_expr),
            missing_struct_fields: if def_missing_struct == 1 { MissingColumnsPolicy::Insert } else { MissingColumnsPolicy::Raise },
            extra_struct_fields: if def_extra_struct == 1 { ExtraColumnsPolicy::Ignore } else { ExtraColumnsPolicy::Raise },
            integer_cast: if def_int_cast == 1 { UpcastOrForbid::Upcast } else { UpcastOrForbid::Forbid },
            float_cast: if def_float_cast == 1 { UpcastOrForbid::Upcast } else { UpcastOrForbid::Forbid },
        };

        let mut overrides_map = HashMap::with_capacity(ov_len);
        if ov_len > 0 && !ov_names.is_null() {
            unsafe {
                let names = std::slice::from_raw_parts(ov_names, ov_len);
                let missing_types = std::slice::from_raw_parts(ov_missing_type, ov_len);
                let missing_exprs = std::slice::from_raw_parts(ov_missing_expr, ov_len);
                let missing_structs = std::slice::from_raw_parts(ov_missing_struct, ov_len);
                let extra_structs = std::slice::from_raw_parts(ov_extra_struct, ov_len);
                let int_casts = std::slice::from_raw_parts(ov_int_cast, ov_len);
                let float_casts = std::slice::from_raw_parts(ov_float_cast, ov_len);

                for i in 0..ov_len {
                    let name = CStr::from_ptr(names[i]).to_string_lossy().to_string();
                    overrides_map.insert(name, MatchToSchemaPerColumn {
                        missing_columns: parse_missing(missing_types[i], missing_exprs[i]),
                        missing_struct_fields: if missing_structs[i] == 1 { MissingColumnsPolicy::Insert } else { MissingColumnsPolicy::Raise },
                        extra_struct_fields: if extra_structs[i] == 1 { ExtraColumnsPolicy::Ignore } else { ExtraColumnsPolicy::Raise },
                        integer_cast: if int_casts[i] == 1 { UpcastOrForbid::Upcast } else { UpcastOrForbid::Forbid },
                        float_cast: if float_casts[i] == 1 { UpcastOrForbid::Upcast } else { UpcastOrForbid::Forbid },
                    });
                }
            }
        }

        let mut per_column_vec = Vec::with_capacity(schema_ctx.schema.len());
        for (name, _) in schema_ctx.schema.iter() {
            if let Some(cfg) = overrides_map.get(name.as_str()) {
                per_column_vec.push(cfg.clone());
            } else {
                per_column_vec.push(default_config.clone());
            }
        }

        let new_lf = lf_ctx.inner.match_to_schema(schema_ctx.schema.clone(), per_column_vec.into(), extra_columns);
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}