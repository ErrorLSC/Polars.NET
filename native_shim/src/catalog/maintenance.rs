use std::ffi::c_char;
use polars::error::PolarsError;
use polars::error::PolarsResult;

use crate::catalog::utils::load_catalog_table;
use crate::catalog::ffi::CatalogContext;
use crate::delta::utils::build_delta_storage_options_map;
use crate::delta::utils::get_runtime;
use crate::utils::ptr_to_str;

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_delta_vacuum(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char,
    schema_name_ptr: *const c_char,
    table_name_ptr: *const c_char,
    
    retention_hours: i32, enforce_retention: bool, dry_run: bool, vacuum_mode_full: bool,
    
    cloud_keys: *const *const c_char, cloud_values: *const *const c_char, cloud_len: usize,
    out_files_deleted: *mut usize, 
) {
    ffi_try_void!({
        let ctx = unsafe { &*ctx_ptr };
        let catalog_name = ptr_to_str(catalog_name_ptr).unwrap().to_string();
        let schema_name = ptr_to_str(schema_name_ptr).unwrap().to_string();
        let table_name = ptr_to_str(table_name_ptr).unwrap().to_string();
        
        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let rt = get_runtime();

        let deleted_count = rt.block_on(async {
            let (table, _,_) = load_catalog_table(ctx, &catalog_name, &schema_name, &table_name, true, base_options).await?;

            let mut vacuum_builder = table.vacuum();
            if retention_hours >= 0 {
                let sec = (retention_hours as i64).checked_mul(3600).unwrap();
                vacuum_builder = vacuum_builder.with_retention_period(chrono::Duration::seconds(sec));
            }
            vacuum_builder = vacuum_builder.with_enforce_retention_duration(enforce_retention).with_dry_run(dry_run);
            vacuum_builder = vacuum_builder.with_mode(if vacuum_mode_full { deltalake::operations::vacuum::VacuumMode::Full } else { deltalake::operations::vacuum::VacuumMode::Lite });

            let (_, metrics) = vacuum_builder.await.map_err(|e| PolarsError::ComputeError(format!("Vacuum failed: {}", e).into()))?;
            Ok::<usize, PolarsError>(metrics.files_deleted.len())
        })?;

        if !out_files_deleted.is_null() { unsafe { *out_files_deleted = deleted_count }; }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_delta_restore(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char, 
    schema_name_ptr: *const c_char, 
    table_name_ptr: *const c_char,

    // --- Restore Target  ---
    target_version: u64, 
    target_timestamp_ms: i64, 

    // --- Options ---
    ignore_missing_files: bool, 
    protocol_downgrade_allowed: bool, 

    // --- Cloud Auth ---
    cloud_keys: *const *const c_char, 
    cloud_values: *const *const c_char, 
    cloud_len: usize,

    // --- Output ---
    out_new_version: *mut u64,
) {
    ffi_try_void!({
        // Safely dereference context
        let ctx = unsafe { 
            if ctx_ptr.is_null() {
                return Err(PolarsError::ComputeError("Catalog context pointer is null".into()));
            }
            &*ctx_ptr 
        };

        // Parse strings safely instead of unwrap()
        let catalog_name = ptr_to_str(catalog_name_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        let schema_name = ptr_to_str(schema_name_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        let table_name = ptr_to_str(table_name_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();
        
        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let rt = get_runtime();

        let new_version = rt.block_on(async {
            // Load table through catalog
            let (table, _, _) = load_catalog_table(
                ctx, 
                &catalog_name, 
                &schema_name, 
                &table_name, 
                true, // Check schema mismatch? (Assuming true based on original code)
                base_options
            ).await?;

            let mut cmd = table.restore();

            // Handle Target: Sentinel approach (-1ms means use version)
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

            // Execute restore
            let (new_table, _) = cmd.await
                .map_err(|e| PolarsError::ComputeError(format!("Catalog table restore failed: {}", e).into()))?;
                
            Ok::<u64, PolarsError>(new_table.version().unwrap_or(0))
        })?;

        // Write output
        if !out_new_version.is_null() { 
            unsafe { *out_new_version = new_version }; 
        }
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_catalog_delta_history(
    ctx_ptr: *mut CatalogContext,
    catalog_name_ptr: *const c_char, schema_name_ptr: *const c_char, table_name_ptr: *const c_char,
    limit: usize, 
    cloud_keys: *const *const c_char, cloud_values: *const *const c_char, cloud_len: usize,
    out_json_ptr: *mut *mut c_char, 
) {
    ffi_try_void!({
        let ctx = unsafe { &*ctx_ptr };
        let catalog_name = ptr_to_str(catalog_name_ptr).unwrap().to_string();
        let schema_name = ptr_to_str(schema_name_ptr).unwrap().to_string();
        let table_name = ptr_to_str(table_name_ptr).unwrap().to_string();
        
        let base_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let rt = get_runtime();

        let json_string = rt.block_on(async {
            let (table, _,_) = load_catalog_table(ctx, &catalog_name, &schema_name, &table_name, false, base_options).await?;

            let limit_opt = if limit == 0 { None } else { Some(limit) };
            let history_iter = table.history(limit_opt).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get history: {}", e).into()))?;

            let commits: Vec<_> = history_iter.collect();
            serde_json::to_string(&commits)
                .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize history: {}", e).into()))
        })?;

        let c_str = std::ffi::CString::new(json_string).unwrap();
        unsafe { *out_json_ptr = c_str.into_raw(); }
        Ok(())
    })
}