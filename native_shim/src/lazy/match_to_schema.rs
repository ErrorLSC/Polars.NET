use polars::lazy::dsl::{ExtraColumnsPolicy, MissingColumnsPolicy, MatchToSchemaPerColumn};
use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::c_char;
use polars::prelude::*;

use crate::types::{ExprContext, LazyFrameContext, SchemaContext};
// ==========================================
// Schema Alignment C-Structs
// ==========================================

#[repr(C)]
pub struct MissingColumnsPolicyOrExprC {
    pub policy_type: u8, // 0 = Insert, 1 = Raise, 2 = InsertWith
    pub expr_ptr: *mut ExprContext,
}

#[repr(C)]
pub struct MatchToSchemaPerColumnC {
    pub missing_columns: MissingColumnsPolicyOrExprC,
    pub missing_struct_fields: u8, // 0 = Raise, 1 = Insert
    pub extra_struct_fields: u8,   // 0 = Raise, 1 = Ignore
    pub integer_cast: u8,          // 0 = Forbid, 1 = Upcast
    pub float_cast: u8,            // 0 = Forbid, 1 = Upcast
}

#[repr(C)]
pub struct SchemaColumnOverrideC {
    pub col_name: *const c_char,
    pub config: MatchToSchemaPerColumnC,
}

fn convert_c_config(c_pol: &MatchToSchemaPerColumnC) -> MatchToSchemaPerColumn {
    let missing_columns = match c_pol.missing_columns.policy_type {
        0 => MissingColumnsPolicyOrExpr::Insert,
        1 => MissingColumnsPolicyOrExpr::Raise,
        2 => {
            if !c_pol.missing_columns.expr_ptr.is_null() {
                let expr_ctx = unsafe { Box::from_raw(c_pol.missing_columns.expr_ptr) };
                MissingColumnsPolicyOrExpr::InsertWith(expr_ctx.inner)
            } else {
                MissingColumnsPolicyOrExpr::Raise
            }
        },
        _ => MissingColumnsPolicyOrExpr::Raise,
    };

    let missing_struct_fields = match c_pol.missing_struct_fields {
        1 => MissingColumnsPolicy::Insert,
        _ => MissingColumnsPolicy::Raise,
    };

    let extra_struct_fields = match c_pol.extra_struct_fields {
        1 => ExtraColumnsPolicy::Ignore,
        _ => ExtraColumnsPolicy::Raise,
    };

    let integer_cast = match c_pol.integer_cast {
        1 => UpcastOrForbid::Upcast,
        _ => UpcastOrForbid::Forbid,
    };

    let float_cast = match c_pol.float_cast {
        1 => UpcastOrForbid::Upcast,
        _ => UpcastOrForbid::Forbid,
    };

    MatchToSchemaPerColumn {
        missing_columns,
        missing_struct_fields,
        extra_struct_fields,
        integer_cast,
        float_cast,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_match_to_schema(
    lf_ptr: *mut LazyFrameContext,
    schema_ptr: *mut SchemaContext, 
    extra_columns_code: u8,
    default_config: MatchToSchemaPerColumnC,
    overrides_ptr: *const SchemaColumnOverrideC,
    overrides_len: usize,
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let schema_ctx = unsafe { &*schema_ptr };

        let extra_columns = match extra_columns_code {
            1 => ExtraColumnsPolicy::Ignore,
            _ => ExtraColumnsPolicy::Raise,
        };

        let mut overrides_map = HashMap::with_capacity(overrides_len);
        if !overrides_ptr.is_null() && overrides_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(overrides_ptr, overrides_len) };
            for ov in slice {
                let name = unsafe { CStr::from_ptr(ov.col_name) }.to_string_lossy().to_string();
                overrides_map.insert(name, convert_c_config(&ov.config));
            }
        }

        let default_rust_config = convert_c_config(&default_config);

        let mut per_column_vec = Vec::with_capacity(schema_ctx.schema.len());
        for (name, _) in schema_ctx.schema.iter() {
            if let Some(specific_config) = overrides_map.get(name.as_str()) {
                per_column_vec.push(specific_config.clone());
            } else {
                per_column_vec.push(default_rust_config.clone());
            }
        }

        let new_lf = lf_ctx.inner.match_to_schema(
            schema_ctx.schema.clone(),
            per_column_vec.into(),
            extra_columns,
        );

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}