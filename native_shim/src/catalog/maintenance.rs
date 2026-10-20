use std::ffi::c_char;
use futures::TryStreamExt;
use polars::error::{PolarsError, PolarsResult};

use crate::catalog::ffi::CatalogContext;
use crate::catalog::utils::{load_catalog_table_sync, parse_catalog_table_args};
use crate::delta::utils::{build_delta_storage_options_map, get_runtime};
use crate::utils::ptr_to_str;

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_delta_vacuum(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    retention_hours: i32,
    enforce_retention: bool,
    dry_run: bool,
    vacuum_mode_full: bool,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize,
    out_files_deleted: *mut usize,
) {
    ffi_try_void!({
        let (ctx, catalog, schema, table_name) = parse_catalog_table_args(
            ctx_ptr, catalog_name_ptr, schema_name_ptr, table_name_ptr,
        )?;
        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let (table, _, _) = load_catalog_table_sync(ctx, catalog, schema, table_name, true, base_options)?;

        let mut vacuum_builder = table.vacuum();
        if retention_hours >= 0 {
            let sec = (retention_hours as i64).checked_mul(3600).unwrap();
            vacuum_builder = vacuum_builder.with_retention_period(chrono::Duration::seconds(sec));
        }
        vacuum_builder = vacuum_builder
            .with_enforce_retention_duration(enforce_retention)
            .with_dry_run(dry_run)
            .with_mode(if vacuum_mode_full {
                deltalake::operations::vacuum::VacuumMode::Full
            } else {
                deltalake::operations::vacuum::VacuumMode::Lite
            });

        let (_, metrics) = get_runtime().block_on(async { vacuum_builder.await })
            .map_err(|e| PolarsError::ComputeError(format!("Vacuum failed: {}", e).into()))?;

        if !out_files_deleted.is_null() {
            unsafe { *out_files_deleted = metrics.files_deleted.len() };
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_delta_restore(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    target_version: u64,
    target_timestamp_ms: i64,
    ignore_missing_files: bool,
    protocol_downgrade_allowed: bool,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize,
    out_new_version: *mut u64,
) {
    ffi_try_void!({
        let (ctx, catalog, schema, table_name) = parse_catalog_table_args(
            ctx_ptr, catalog_name_ptr, schema_name_ptr, table_name_ptr,
        )?;
        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let (table, _, _) = load_catalog_table_sync(ctx, catalog, schema, table_name, true, base_options)?;

        let mut cmd = table.restore();
        if target_timestamp_ms >= 0 {
            use chrono::{TimeZone, Utc};
            let dt = Utc.timestamp_millis_opt(target_timestamp_ms)
                .single()
                .ok_or_else(|| PolarsError::ComputeError("Invalid timestamp for restore".into()))?;
            cmd = cmd.with_datetime_to_restore(dt);
        } else {
            cmd = cmd.with_version_to_restore(target_version);
        }

        cmd = cmd.with_ignore_missing_files(ignore_missing_files)
                 .with_protocol_downgrade_allowed(protocol_downgrade_allowed);

        let (new_table, _) = get_runtime().block_on(async { cmd.await })
            .map_err(|e| PolarsError::ComputeError(format!("Catalog table restore failed: {}", e).into()))?;

        if !out_new_version.is_null() {
            unsafe { *out_new_version = new_table.version().unwrap_or(0) };
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_delta_history(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    limit: usize,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize,
    out_json_ptr: *mut *mut c_char,
) {
    ffi_try_void!({
        let (ctx, catalog, schema, table_name) = parse_catalog_table_args(
            ctx_ptr, catalog_name_ptr, schema_name_ptr, table_name_ptr,
        )?;
        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let (table, _, _) = load_catalog_table_sync(ctx, catalog, schema, table_name, false, base_options)?;

        let limit_opt = if limit == 0 { None } else { Some(limit) };
        let history_stream = table.history(limit_opt);

        let commits: Vec<_> = get_runtime().block_on(history_stream.try_collect())
            .map_err(|e| PolarsError::ComputeError(format!("Failed to get history: {}", e).into()))?;

        let json_string = serde_json::to_string(&commits)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize history: {}", e).into()))?;

        let c_str = std::ffi::CString::new(json_string).unwrap();
        unsafe {
            *out_json_ptr = c_str.into_raw();
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_delta_add_feature(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    feature_name_ptr: *const c_char,
    allow_protocol_increase: bool,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize,
) {
    ffi_try_void!({
        let (ctx, catalog, schema, table_name) = parse_catalog_table_args(
            ctx_ptr, catalog_name_ptr, schema_name_ptr, table_name_ptr,
        )?;
        let feature_str = ptr_to_str(feature_name_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let feature = <deltalake::kernel::TableFeatures as std::str::FromStr>::from_str(feature_str)
            .map_err(|_| PolarsError::ComputeError(format!("Unknown table feature: {}", feature_str).into()))?;

        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let (table, _, _) = load_catalog_table_sync(ctx, catalog, schema, table_name, true, base_options)?;

        let builder = table.add_feature()
            .with_feature(feature)
            .with_allow_protocol_versions_increase(allow_protocol_increase);

        get_runtime().block_on(async { builder.await })
            .map_err(|e| PolarsError::ComputeError(format!("Failed to add feature: {}", e).into()))?;

        Ok(())
    })
}