use chrono::TimeZone;
use chrono::Utc;
use deltalake::DeltaTable;
use deltalake::kernel::TableFeatures;
use deltalake::operations::vacuum::VacuumMode;
use std::collections::HashMap;
use std::ffi::CString;
use std::ffi::c_char;
use polars::error::PolarsError;
use polars::error::PolarsResult;

use crate::delta::utils::build_delta_storage_options_map;
use crate::delta::utils::get_runtime;
use crate::delta::utils::parse_table_url;
use crate::utils::ptr_to_str;

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_vacuum(
    table_path_ptr: *const c_char,
    
    // --- Vacuum Options ---
    retention_hours: i32,      
    enforce_retention: bool,   
    dry_run: bool,             
    vacuum_mode_full: bool,
    // --- Cloud Args ---
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize,

    // --- Return Metric ---
    out_files_deleted: *mut usize, 
) {
    ffi_try_void!({
        // Parse Path and Cloud Options
        let path_str = ptr_to_str(table_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = parse_table_url(path_str)?;
        let storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        
        let rt = get_runtime();

        // Vacuum 
        let deleted_count = rt.block_on(async {
            // A. Load Table
            let table = DeltaTable::try_from_url_with_storage_options(table_url, storage_options)
                .await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to load table: {}", e).into()))?;

            let mut vacuum_builder = table.vacuum();
            
            // Retention
            if retention_hours >= 0 {
                let seconds = (retention_hours as i64).checked_mul(3600)
                    .ok_or(PolarsError::ComputeError("Retention hours overflow".into()))?;
                    
                vacuum_builder = vacuum_builder.with_retention_period(
                    chrono::Duration::seconds(seconds)
                );
            }

            // Safety Check
            vacuum_builder = vacuum_builder.with_enforce_retention_duration(enforce_retention);

            // Dry Run
            vacuum_builder = vacuum_builder.with_dry_run(dry_run);

            // Mode Selection (Lite vs Full)
            let mode = if vacuum_mode_full {
                VacuumMode::Full 
            } else {
                VacuumMode::Lite
            };
            vacuum_builder = vacuum_builder.with_mode(mode);

            // Execute
            let (_table_state, metrics) = vacuum_builder.await
                .map_err(|e| PolarsError::ComputeError(format!("Vacuum failed: {}", e).into()))?;

            // Result
            Ok::<usize, PolarsError>(metrics.files_deleted.len())
        })?;

        if !out_files_deleted.is_null() {
            unsafe { *out_files_deleted = deleted_count };
        }

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_restore(
    table_path_ptr: *const c_char,

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
        let path_str = ptr_to_str(table_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = parse_table_url(path_str)?;
        let storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        
        let rt = get_runtime();

        let new_version = rt.block_on(async {
            // 1. Load Table
            let table = DeltaTable::try_from_url_with_storage_options(table_url, storage_options)
                .await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to load table: {}", e).into()))?;

            // 2. Build Restore Command
            let mut cmd = table.restore();

            // 3. Set Target (Version vs Timestamp)
            if target_timestamp_ms >= 0 {
                // If timestamp is provided (>=0), use it
                let dt = Utc.timestamp_millis_opt(target_timestamp_ms)
                    .single()
                    .ok_or_else(|| PolarsError::ComputeError("Invalid timestamp".into()))?;
                cmd = cmd.with_datetime_to_restore(dt);
            } else {
                // If timestamp is < 0 (e.g., -1), fallback to using version
                cmd = cmd.with_version_to_restore(target_version);
            }

            // 4. Set Options
            cmd = cmd.with_ignore_missing_files(ignore_missing_files);
            cmd = cmd.with_protocol_downgrade_allowed(protocol_downgrade_allowed);

            // 5. Execute
            let (new_table, _metrics) = cmd.await
                .map_err(|e| PolarsError::ComputeError(format!("Restore failed: {}", e).into()))?;

            Ok::<u64, PolarsError>(new_table.version().unwrap())
        })?;

        if !out_new_version.is_null() {
            unsafe { *out_new_version = new_version };
        }

        Ok(())
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_history(
    table_path_ptr: *const c_char,
    limit: usize, // 0 = All, >0 = Limit
    
    // Cloud Options
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize,

    // Output: JSON String Pointer
    out_json_ptr: *mut *mut c_char, 
) {
    ffi_try_void!({
        let path_str = ptr_to_str(table_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = parse_table_url(path_str)?;
        let storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        
        let rt = get_runtime();

        let json_string = rt.block_on(async {
            let table = DeltaTable::try_from_url_with_storage_options(table_url, storage_options)
                .await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to load table: {}", e).into()))?;

            // limit: 0 => None, >0 => Some(limit)
            let limit_opt = if limit == 0 { None } else { Some(limit) };

            let history_iter = table.history(limit_opt).await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get history: {}", e).into()))?;

            // Gather CommitInfo
            let commits: Vec<_> = history_iter.collect();

            // Serilize JSON 
            serde_json::to_string(&commits)
                .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize history: {}", e).into()))
        })?;

        // Convert to CString 
        let c_str = CString::new(json_string).unwrap();
        unsafe { 
            *out_json_ptr = c_str.into_raw(); 
        }

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_add_feature(
    path_ptr: *const c_char,
    feature_name_ptr: *const c_char, // e.g., "deletionVectors", "appendOnly"
    allow_protocol_increase: bool,
    // --- Cloud Args ---
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) {
    ffi_try_void!({
        // Parse Params
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let feature_str = ptr_to_str(feature_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = parse_table_url(path_str)?;
        let storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        // Convert string to Delta TableFeatures Enum
        let feature = <TableFeatures as std::str::FromStr>::from_str(feature_str)
            .map_err(|_| PolarsError::ComputeError(format!("Unknown table feature: {}", feature_str).into()))?;

        let rt = get_runtime();

        rt.block_on(async {
            // Load Table
            let table = DeltaTable::try_from_url_with_storage_options(table_url, storage_options)
                .await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to load table: {}", e).into()))?;

            // Execuate Add Feature
            let builder = table.add_feature()
                .with_feature(feature)
                .with_allow_protocol_versions_increase(allow_protocol_increase);

            let _ = builder.await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to add feature: {}", e).into()))?;
            
            Ok::<(), PolarsError>(())
        })?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_set_table_properties(
    path_ptr: *const c_char,
    
    // --- Properties to Set ---
    props_keys: *const *const c_char,
    props_values: *const *const c_char,
    props_len: usize,
    
    // --- Options ---
    raise_if_not_exists: bool, 
    
    // --- Cloud Args ---
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) {
    ffi_try_void!({
        // Parse Params
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = parse_table_url(path_str)?;
        let storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        // Build Properties HashMap
        let mut properties = HashMap::with_capacity(props_len);
        for i in 0..props_len {
            unsafe {
                let k_ptr = *props_keys.add(i);
                let v_ptr = *props_values.add(i);
                if !k_ptr.is_null() && !v_ptr.is_null() {
                    let k = ptr_to_str(k_ptr).unwrap().to_string();
                    let v = ptr_to_str(v_ptr).unwrap().to_string();
                    properties.insert(k, v);
                }
            }
        }

        if properties.is_empty() {
            return Ok(());
        }

        let rt = get_runtime();
        rt.block_on(async {
            let table = DeltaTable::try_from_url_with_storage_options(table_url, storage_options)
                .await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to load table: {}", e).into()))?;

            // Set Properties
            let builder = table.set_tbl_properties()
                .with_properties(properties)
                .with_raise_if_not_exists(raise_if_not_exists);

            let _ = builder.await
                .map_err(|e| PolarsError::ComputeError(format!("Failed to set properties: {}", e).into()))?;
            
            Ok::<(), PolarsError>(())
        })?;

        Ok(())
    })
}