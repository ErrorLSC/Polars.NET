use std::{collections::HashMap, ffi::c_char, time::{SystemTime, UNIX_EPOCH}};
use deltalake::{DeltaTable, Path, kernel::{Action, Remove, transaction}, protocol::DeltaOperation};
use futures::StreamExt;
use serde_json::Value;
use url::Url;
use uuid::Uuid;
use polars::{error::{PolarsError, PolarsResult}, prelude::*};
use deltalake::protocol::SaveMode;

use crate::pl_io::{io_utils::build_unified_sink_args, parquet::parquet_utils::build_parquet_write_options};
use crate::types::{LazyFrameContext,SelectorContext};
use crate::utils::ptr_to_str;
use crate::delta::utils::*;
use crate::delta::merge::phase_process_staging;

#[unsafe(no_mangle)]
pub extern "C" fn pl_sink_delta(
    lf_ptr: *mut LazyFrameContext,
    base_path_ptr: *const c_char,
    mode: u8,
    can_evolve: bool,
    // --- Partition Params ---
    partition_by_ptr: *mut SelectorContext,
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,

    // --- Parquet Options ---
    compression: u8,        
    compression_level: i32, 
    statistics: bool,       
    row_group_size: usize,  
    data_page_size: usize,
    compat_level: i32,
    
    // --- Unified Options ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
    
    // --- Cloud Params ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, 
    cloud_retry_max_backoff_ms: u64,  
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) {
    ffi_try_void!({
        // =========================================================
        // Phase 0: FFI Data Cleaning & Parameter Preparation
        // =========================================================
        let mut lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let base_path_str = ptr_to_str(base_path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.to_string();

        let schema = lf_ctx.inner.collect_schema()
            .map_err(|e| PolarsError::ComputeError(format!("Failed to collect schema: {}", e).into()))?;

        let partition_cols = if !partition_by_ptr.is_null() {
            let selector_ctx = unsafe { &*partition_by_ptr };
            let ignored = PlHashSet::new();
            selector_ctx.inner.into_columns(&schema, &ignored)?
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
        } else {
            Vec::new()
        };

        let save_mode = map_savemode(mode);
        let delta_opts = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        
        let write_options_arc = build_parquet_write_options(
            compression, compression_level, statistics, row_group_size, data_page_size, compat_level
        )?;
        let file_format = FileWriteFormat::Parquet(write_options_arc);
        
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir, maintain_order, sync_on_close,
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                cloud_keys, cloud_values, cloud_len
            )
        };

        sink_delta_internal(
            lf_ctx.inner,
            schema,
            base_path_str,
            delta_opts,
            save_mode,
            can_evolve,
            partition_cols,
            include_keys,
            keys_pre_grouped,
            max_rows_per_file,
            approx_bytes_per_file,
            file_format,
            unified_args,
            mkdir
        )?;
        Ok(())
    })
}

pub(crate) fn sink_delta_internal(
    lf: LazyFrame,
    schema: SchemaRef,
    base_path_str: String,
    delta_opts: std::collections::HashMap<String, String>,
    save_mode: SaveMode,
    can_evolve: bool,
    partition_cols: Vec<String>,
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,
    file_format: FileWriteFormat,
    unified_args: UnifiedSinkArgs,
    mkdir: bool,
) -> PolarsResult<()> {
    
    let rt = get_runtime();
    let write_id = Uuid::new_v4(); 

    let staging_dir_name = format!(".tmp_write_{}", write_id);
    let staging_uri = if base_path_str.contains("://") {
        format!("{}/{}", base_path_str.trim_end_matches('/'), staging_dir_name)
    } else {
        std::path::Path::new(&base_path_str)
            .join(&staging_dir_name)
            .to_string_lossy()
            .to_string()
    };

    if mkdir {
        let is_local = !base_path_str.contains("://") || base_path_str.starts_with("file://");
        if is_local {
            let local_path = base_path_str.strip_prefix("file://").unwrap_or(&base_path_str);
            std::fs::create_dir_all(local_path)
                .map_err(|e| PolarsError::ComputeError(format!("Failed to create directory {}: {}", local_path, e).into()))?;
        }
    }

    let table_url = parse_table_url(&base_path_str)?;

    // =========================================================
    // Phase 1: Physical Table Load, Mode Check & Partition Alignment
    // =========================================================
    let (table, should_skip, final_partition_cols) = rt.block_on(
        phase_init_and_validate_sink(table_url, delta_opts, save_mode, partition_cols, &schema)
    )?;

    if should_skip {
        return Ok(());
    }

    // =========================================================
    // Phase 2: Execute Polars Sink (Write Parquet to Staging)
    // =========================================================
    phase_execute_polars_sink(
        lf, &staging_uri, &final_partition_cols, include_keys, keys_pre_grouped, 
        max_rows_per_file, approx_bytes_per_file, file_format, unified_args
    )?;

    // =========================================================
    // Phase 3: Transaction Commit & Cleanup (Async)
    // =========================================================
    rt.block_on(
        phase_commit_and_cleanup(table, schema, staging_dir_name, final_partition_cols, save_mode, can_evolve, write_id)
    )?;

    Ok(())
}

pub(crate) async fn phase_init_and_validate_sink(
    table_url: Url,
    delta_opts: HashMap<String, String>,
    save_mode: SaveMode,
    partition_cols: Vec<String>,
    schema: &SchemaRef,
) -> PolarsResult<(DeltaTable, bool, Vec<String>)> {
    
    let mut table = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_opts)
        .await
        .map_err(|e| PolarsError::ComputeError(format!("Delta init error: {}", e).into()))?;

    match table.load().await {
        Ok(_) => {},
        Err(deltalake::errors::DeltaTableError::NotATable(_)) => {},
        Err(e) => {
            return Err(PolarsError::ComputeError(format!("Failed to load Delta state: {}", e).into()));
        }
    }

    let mut skip_write = false;
    let mut final_partition_cols = partition_cols.clone();

    if table.version() >= Some(0) {
        
        match save_mode {
            SaveMode::ErrorIfExists => {
                return Err(PolarsError::ComputeError(
                    format!("Table already exists at {}", table_url).into(),
                ));
            }
            SaveMode::Ignore => {
                skip_write = true;
                return Ok((table, skip_write, final_partition_cols));
            }
            _ => {} 
        }

        let snapshot = table.snapshot()
            .map_err(|e| PolarsError::ComputeError(format!("Snapshot: {}", e).into()))?;
        let existing_part_cols = snapshot.metadata().partition_columns().clone();

        if !existing_part_cols.is_empty() {
            if partition_cols.is_empty() {
                for col in &existing_part_cols {
                    if schema.get_field(col).is_none() {
                        return Err(PolarsError::ComputeError(
                            format!("DataFrame missing partition column: {}", col).into(),
                        ));
                    }
                }
                final_partition_cols = existing_part_cols;
            } else if partition_cols != existing_part_cols {
                return Err(PolarsError::ComputeError(
                    format!(
                        "Partition mismatch. Table expects: {:?}, Input provided: {:?}",
                        existing_part_cols, partition_cols
                    )
                    .into(),
                ));
            }
        }
    }

    Ok((table, skip_write, final_partition_cols))
}

pub(crate) fn phase_execute_polars_sink(
    lf: LazyFrame,
    staging_uri: &str,
    final_partition_cols: &[String],
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,
    file_format: FileWriteFormat,
    unified_args: UnifiedSinkArgs,
) -> PolarsResult<()> {
    
    let partition_strategy = if final_partition_cols.is_empty() {
        PartitionStrategy::FileSize
    } else {
        let keys: Vec<Expr> = final_partition_cols.iter().map(|n| col(n)).collect();
        PartitionStrategy::Keyed {
            keys,
            include_keys, 
            keys_pre_grouped,
        }
    };

    let hive_provider = file_provider::HivePathProvider {
        extension: PlSmallStr::from_str(".parquet"),
    };
    
    let destination = SinkDestination::Partitioned {
        base_path: PlRefPath::new(staging_uri), 
        file_path_provider: Some(file_provider::FileProviderType::Hive(hive_provider)),
        partition_strategy,
        max_rows_per_file: if max_rows_per_file == 0 { u32::MAX } else { max_rows_per_file as u32 },
        approximate_bytes_per_file: if approx_bytes_per_file == 0 { usize::MAX as u64 } else { approx_bytes_per_file },
    };

    lf.sink(destination, file_format, unified_args)?
        .collect_with_engine(Engine::Streaming)?;
        
    Ok(())
}

pub(crate) async fn phase_commit_and_cleanup(
    mut table: DeltaTable,
    schema: SchemaRef,
    staging_dir_name: String,
    final_partition_cols: Vec<String>,
    save_mode: SaveMode,
    can_evolve: bool,
    write_id: Uuid,
) -> PolarsResult<()> {
    
    let _ = table.update_state().await;
    
    if table.version() < Some(0) {
        let delta_schema = convert_to_delta_schema(&schema)?;
        table = table.create()
            .with_columns(delta_schema.fields().cloned())
            .with_partition_columns(final_partition_cols.clone()) 
            .await
            .map_err(|e| PolarsError::ComputeError(format!("Create table error: {}", e).into()))?;
    }

    let object_store = table.object_store();
    let mut actions = Vec::new();

    // ==========================================
    // 2. Schema Evolution Check 
    // ==========================================
    let current_snapshot = table.snapshot()
        .map_err(|e| PolarsError::ComputeError(format!("Failed to get snapshot: {}", e).into()))?;
    let current_delta_schema = current_snapshot.schema();
    
    let new_delta_schema = convert_to_delta_schema(&schema)?;

    if current_delta_schema.as_ref() != &new_delta_schema {
        if !can_evolve {
            return Err(PolarsError::ComputeError(
                "Schema mismatch detected. If you want to evolve the schema, enable 'can_evolve'.".into()
            ));
        }

        let current_metadata = current_snapshot.metadata();
        let mut meta_json = serde_json::to_value(current_metadata)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize metadata: {}", e).into()))?;

        let new_schema_string = serde_json::to_string(&new_delta_schema)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize new schema: {}", e).into()))?;

        if let Some(obj) = meta_json.as_object_mut() {
            obj.insert("schemaString".to_string(), Value::String(new_schema_string));
        } else {
            return Err(PolarsError::ComputeError("Metadata is not a JSON object".into()));
        }

        let new_metadata_action: deltalake::kernel::Metadata = serde_json::from_value(meta_json)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to recreate metadata: {}", e).into()))?;

        actions.insert(0, Action::Metadata(new_metadata_action));
    }

    // ==========================================
    // Staging
    // ==========================================
    let add_actions = phase_process_staging(&table, &staging_dir_name, &final_partition_cols, write_id).await?;
    
    for res in add_actions {
        actions.push(res);
    }

    // ==========================================
    // Overwrite
    // ==========================================
    if let SaveMode::Overwrite = save_mode {
        let mut stream = table.get_active_add_actions_by_partitions(&[]);
        
        while let Some(view_res) = stream.next().await {
            let view = view_res.map_err(|e| PolarsError::ComputeError(format!("List files error: {}", e).into()))?;
            let add_action = view_to_add_action(&view);
            
            let remove = Remove {
                path: add_action.path.clone(),
                deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                data_change: true,
                extended_file_metadata: Some(true),
                partition_values: Some(add_action.partition_values),
                size: Some(add_action.size),
                deletion_vector: add_action.deletion_vector,
                tags: add_action.tags,
                base_row_id: add_action.base_row_id,
                default_row_commit_version: add_action.default_row_commit_version,
            };
            actions.push(Action::Remove(remove));
        }
    }

    if actions.is_empty() {
        return Ok(());
    }

    // ==========================================
    // Commit
    // ==========================================
    let operation = DeltaOperation::Write {
        mode: save_mode,
        partition_by: if !final_partition_cols.is_empty() { Some(final_partition_cols) } else { None },
        predicate: None,
    };

    let _ver = transaction::CommitBuilder::default()
        .with_actions(actions)
        .build(
            table.state.as_ref().map(|s| s as &dyn transaction::TableReference), 
            table.log_store().clone(), 
            operation
        )
        .await
        .map_err(|e| PolarsError::ComputeError(format!("Commit failed: {}", e).into()))?;
        
    // ==========================================
    // Cleanup
    // ==========================================
    let _ = object_store.delete(&Path::from(staging_dir_name)).await;
    
    Ok(())
}