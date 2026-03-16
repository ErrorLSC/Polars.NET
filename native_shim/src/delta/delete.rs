use std::{cmp::Ordering, collections::HashMap, ffi::c_char, time::{SystemTime, UNIX_EPOCH}};

use chrono::{NaiveDate, TimeDelta, Utc};
use deltalake::{DeltaTable, DeltaTableError, ObjectStore, kernel::{DeletionVectorDescriptor, StorageType}, table::state::DeltaTableState}; 
use deltalake::kernel::{Action, Add, Remove};
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use deltalake::protocol::DeltaOperation;
use futures::StreamExt;
use polars::{error::{PolarsError, PolarsResult}, frame::DataFrame, prelude::{DataType, LazyFrame, PlRefPath, ScanArgsParquet}, series::Series};
use polars::prelude::*;
use rand::RngExt;
use roaring::RoaringBitmap;
use serde_json::Value;
use uuid::Uuid;
use crate::{delta::{deletion_vector::read_deletion_vector, utils::{RawCloudArgs, build_delta_storage_options_map, check_file_overlap_optimized, extract_delta_stats, get_polars_schema_from_delta, get_runtime, parse_hive_partitions, parse_table_url, view_to_add_action}}, pl_io::parquet::parquet_utils::build_parquet_write_options};
use crate::pl_io::io_utils::{build_cloud_options, build_unified_sink_args};
use crate::types::ExprContext;
use crate::utils::ptr_to_str;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeleteStrategy {
    /// Copy-on-Write (CoW): Rewrite the entire Parquet file excluding deleted rows.
    /// Used when:
    /// 1. Table does not support deletion vectors.
    /// 2. User explicitly requests it (e.g., for compaction).
    CopyOnWrite,

    /// Merge-on-Read (MoR): Write a Deletion Vector (Bitmap) file.
    /// Used when:
    /// 1. Table supports deletion vectors (Reader v3, Writer v7).
    /// 2. 'deletionVectors' feature is enabled.
    MergeOnRead,
}

impl DeleteStrategy {
    pub fn determine(state: &DeltaTableState, force_cow: bool) -> Self {
        if force_cow {
            return Self::CopyOnWrite;
        }

        let protocol = state.protocol();
        let min_writer_version = protocol.min_writer_version();

        // Serilize protocol to JSON Value 
        let has_deletion_vectors = serde_json::to_value(protocol)
            .ok()
            .and_then(|json| {
                json.get("readerFeatures")
                    .and_then(|features| features.as_array())
                    // Check "deletionVectors"
                    .map(|arr| arr.iter().any(|v| v.as_str() == Some("deletionVectors")))
            })
            .unwrap_or(false);

        if has_deletion_vectors && min_writer_version >= 7 {
            Self::MergeOnRead
        } else {
            Self::CopyOnWrite
        }
    }
}

/// DeleteContext holds all the necessary state for a delete operation.
/// It decouples the execution logic from the FFI interface.
pub struct DeleteContext {
    // --- Identity & Config ---
    pub table_root: String,
    pub strategy: DeleteStrategy,
    
    // --- Logic ---
    pub predicate: Expr, // The WHERE clause
    pub key_bounds: HashMap<String, (Option<serde_json::Value>, Option<serde_json::Value>)>,
    
    // --- Schema Information ---
    pub polars_schema: Schema,
    pub partition_cols: Vec<String>,
    
    // --- Infrastructure ---
    pub object_store: Arc<dyn ObjectStore>,
    pub cloud_args: RawCloudArgs, // For Polars Scan/Sink (FFI compat)
    
}

impl DeleteContext {
    pub fn new(
        table: &DeltaTable,
        predicate: Expr,
        polars_schema: Schema,
        partition_cols: Vec<String>,
        strategy: DeleteStrategy,
        cloud_args: RawCloudArgs,
    ) -> Self {
        // Extract Key Bounds immediately upon creation
        let mut key_bounds = HashMap::new();
        extract_bounds_from_expr(&predicate, &mut key_bounds);

        Self {
            table_root: table.table_url().to_string(),
            strategy,
            predicate,
            key_bounds,
            polars_schema,
            partition_cols,
            object_store: table.object_store(),
            cloud_args,
        }
    }
}

/// Represents the decision made after analyzing a file (Pruning Phase).
pub enum FileActionDecision {
    /// 1. Skip: The file does not contain any data matching the predicate.
    /// (Based on Partition Pruning or Stats Pruning)
    Skip,

    /// 2. Full Drop: Every row in this file matches the predicate.
    /// We can simply emit a `Remove` action for this file.
    /// (Optimization: No need to rewrite or write DV)
    FullDrop(Action), // Action::Remove

    /// 3. Process: The file *might* contain matching rows.
    /// We need to Scan it and apply the Delete Strategy (CoW or MoR).
    Process {
        file_view: Add, // The original file info
    },
}
// ==========================================
// Delete Implementation
// ==========================================
pub type PruningMap = HashMap<String, (Option<Value>, Option<Value>)>;

#[derive(Debug, Clone, Copy)]
enum BoundType {
    Min,
    Max,
    Exact, // Min == Max
}

pub(crate) fn extract_bounds_from_expr(expr: &Expr, map: &mut PruningMap) {
    match expr {
        // Handle AND: Recurse both sides
        // (A > 5) & (A < 10)
        Expr::BinaryExpr { left, op: Operator::And, right } => {
            extract_bounds_from_expr(left, map);
            extract_bounds_from_expr(right, map);
        },

        // Handle Comparison Operators
        Expr::BinaryExpr { left, op, right } => {
            // Helper to clean up the nesting
            let (col_name, lit_val, effective_op) = match (left.as_ref(), right.as_ref()) {
                // Case 1: Col op Lit (e.g., A > 10) -> Keep Op
                (Expr::Column(name), Expr::Literal(lit)) => (name.as_str(), lit, *op),
                
                // Case 2: Lit op Col (e.g., 10 < A) -> Flip Op (A > 10)
                (Expr::Literal(lit), Expr::Column(name)) => {
                    let flipped_op = match op {
                        Operator::Eq => Operator::Eq,
                        Operator::Gt => Operator::Lt,   // 10 > A  => A < 10
                        Operator::Lt => Operator::Gt,   // 10 < A  => A > 10
                        Operator::GtEq => Operator::LtEq,
                        Operator::LtEq => Operator::GtEq,
                        _ => return, // Unsupported op
                    };
                    (name.as_str(), lit, flipped_op)
                },
                _ => return, // Not a simple Col/Lit comparison
            };

            // Apply Bound based on Operator
            match effective_op {
                Operator::Eq => update_map(map, col_name, lit_val, BoundType::Exact),
                Operator::Gt | Operator::GtEq => update_map(map, col_name, lit_val, BoundType::Min),
                Operator::Lt | Operator::LtEq => update_map(map, col_name, lit_val, BoundType::Max),
                _ => {}
            }
        },

        // Handle Alias (pass-through)
        Expr::Alias(inner, _) => {
            extract_bounds_from_expr(inner, map);
        },

        _ => {}
    }
}

fn compare_json_values(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
            if let (Some(i1), Some(i2)) = (n1.as_i64(), n2.as_i64()) {
                Some(i1.cmp(&i2))
            } else if let (Some(u1), Some(u2)) = (n1.as_u64(), n2.as_u64()) {
                Some(u1.cmp(&u2))
            } else if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) {
                f1.partial_cmp(&f2)
            } else {
                None
            }
        }
        (Value::String(s1), Value::String(s2)) => Some(s1.cmp(s2)),
        (Value::Bool(b1), Value::Bool(b2)) => Some(b1.cmp(b2)),
        _ => None,
    }
}

fn update_map(map: &mut PruningMap, col_name: &str, lit: &LiteralValue, b_type: BoundType) {
    if let Some(json_val) = polars_literal_to_json(lit) {
        let entry = map.entry(col_name.to_string()).or_insert((None, None));

        match b_type {
            BoundType::Exact => {
                // A == 10 means Min=10, Max=10
                entry.0 = Some(json_val.clone());
                entry.1 = Some(json_val);
            },
            BoundType::Min => {
                // A > 10 (lower bound):  MAX(current_min, new_min)
                if let Some(ref current_min) = entry.0 {
                    if let Some(Ordering::Less) = compare_json_values(current_min, &json_val) {
                        entry.0 = Some(json_val);
                    }
                } else {
                    entry.0 = Some(json_val);
                }
            },
            BoundType::Max => {
                // A < 10 (upper bound): MIN(current_max, new_max)
                if let Some(ref current_max) = entry.1 {
                    if let Some(Ordering::Greater) = compare_json_values(current_max, &json_val) {
                        entry.1 = Some(json_val);
                    }
                } else {
                    entry.1 = Some(json_val);
                }
            }
        }
    }
}

fn polars_literal_to_json(lit: &LiteralValue) -> Option<serde_json::Value> {
    match lit {
        // Unpack LiteralValue::Scalar
        LiteralValue::Scalar(scalar_struct) => {
            let any_value = &scalar_struct.value(); 

            match any_value {
                // Primitives
                AnyValue::Boolean(b) => Some(serde_json::json!(b)),
                AnyValue::Int8(v)    => Some(serde_json::json!(v)),
                AnyValue::Int16(v)   => Some(serde_json::json!(v)),
                AnyValue::Int32(v)   => Some(serde_json::json!(v)),
                AnyValue::Int64(v)   => Some(serde_json::json!(v)),
                AnyValue::UInt8(v)   => Some(serde_json::json!(v)),
                AnyValue::UInt16(v)  => Some(serde_json::json!(v)),
                AnyValue::UInt32(v)  => Some(serde_json::json!(v)),
                AnyValue::UInt64(v)  => Some(serde_json::json!(v)),
                AnyValue::Float32(v) => Some(serde_json::json!(v)),
                AnyValue::Float64(v) => Some(serde_json::json!(v)),

                // String / Utf8
                AnyValue::String(s)  => Some(serde_json::json!(s)),
                AnyValue::StringOwned(s) => Some(serde_json::json!(s)), 
                
                // Date
                AnyValue::Date(days) => {
                    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                    let date = TimeDelta::try_days(*days as i64)
                        .and_then(|delta| epoch.checked_add_signed(delta));
                    date.map(|d| serde_json::json!(d.format("%Y-%m-%d").to_string()))
                },

                // Timestamp
                // TimeZone is hard to convert
                AnyValue::Datetime(_val, _time_unit, _tz) => {
                    None 
                },

                // Others
                _ => None,
            }
        },
        
        _ => None
    }
}


/// Serilize RoaringBitmap then write to storage
pub(crate) async fn write_dv_file(
    object_store: Arc<dyn ObjectStore>,
    table_root: &str,
    bitmap: &RoaringBitmap,
) -> PolarsResult<(String, i64)> {
    // Serilize Bitmap (Portable Format)
    let mut buf = Vec::new();
    bitmap.serialize_into(&mut buf)
        .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize bitmap: {}", e).into()))?;

    // Generate file name
    let uuid = Uuid::new_v4();
    let file_name = format!("deletion_vector_{}.bin", uuid);
    
    // Build path
    let path = deltalake::Path::from(format!("{}/{}", table_root.trim_end_matches('/'), file_name));

    let buf_len = buf.len() as i64;

    // Write to storage
    object_store.put(&path, buf.into()).await
        .map_err(|e| PolarsError::ComputeError(format!("Failed to write DV file: {}", e).into()))?;

    Ok((file_name, buf_len))
}


///  Delta Protocol DV Descriptor
pub(crate) fn create_dv_descriptor(
    file_name: &str, // deletion_vector_{uuid}.bin
    dv_size: i64,
    cardinality: i64,
) -> PolarsResult<DeletionVectorDescriptor> {
    
    // Extract UUID from filename
    // file_name: "deletion_vector_xxxxxxxx-xxxx-....bin"
    let uuid_str = file_name
        .trim_start_matches("deletion_vector_")
        .trim_end_matches(".bin");
    
    let uuid = Uuid::parse_str(uuid_str)
        .map_err(|e| PolarsError::ComputeError(format!("Invalid UUID in DV filename: {}", e).into()))?;

    // Generate Random Prefix (1 byte) + Base85(UUID)
    // pathOrInlineDv = <randomPrefix> + <base85(uuid)>
    
    // Generate UUID (16 bytes -> 20 chars Z85)
    let uuid_bytes = uuid.as_bytes();
    let uuid_z85 = z85::encode(uuid_bytes);

    // Generate prefix (1 char)
    let prefix = "p"; 
    
    // Concat
    let path_encoded = format!("{}{}", prefix, uuid_z85);

    Ok(DeletionVectorDescriptor {
        storage_type: StorageType::UuidRelativePath, // 'u'
        path_or_inline_dv: path_encoded,
        offset: Some(0), 
        size_in_bytes: dv_size as i32,
        cardinality,
    })
}

/// Version of prune_file that works on Owned Add struct
pub fn prune_file_from_add(ctx: &DeleteContext, add: &Add) -> PolarsResult<FileActionDecision> {
    
    // 1. Partition Pruning
    if !ctx.partition_cols.is_empty() {
        // Build Mini-DataFrame from Add.partition_values (Map<String, Option<String>>)
        let mut columns = Vec::with_capacity(ctx.partition_cols.len());
        
        for target_col in &ctx.partition_cols {
            let val_opt = add.partition_values.get(target_col).and_then(|v| v.as_ref());
            
            let dtype = ctx.polars_schema.get_field(target_col)
                .map(|f| f.dtype.clone())
                .unwrap_or(DataType::String);
            
            let s = match val_opt {
                Some(v) => Series::new(target_col.into(), &[v.as_str()]).cast(&dtype)?,
                None => Series::new_null(target_col.into(), 1).cast(&dtype)?
            };
            columns.push(s.into());
        }

        if let Ok(mini_df) = DataFrame::new(1,columns) {
            let eval_result = mini_df.lazy()
                .select([ctx.predicate.clone().alias("result")])
                .collect_with_engine(Engine::Streaming);

            if let Ok(res) = eval_result {
                if let Ok(bool_s) = res.column("result") {
                    if bool_s.bool().ok().map(|b| b.get(0) == Some(true)).unwrap_or(false) {
                        // Match -> Full Drop
                        let remove = Remove {
                            path: add.path.clone(),
                            deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                            data_change: true,
                            extended_file_metadata: Some(true),
                            partition_values: Some(add.partition_values.clone()),
                            size: Some(add.size),
                            deletion_vector: add.deletion_vector.clone(),
                            tags: None,
                            base_row_id: None,
                            default_row_commit_version: None,
                        };
                        return Ok(FileActionDecision::FullDrop(Action::Remove(remove)));
                    } else {
                        return Ok(FileActionDecision::Skip);
                    }
                }
            }
        }
    }

    // 2. Stats Pruning
    if !ctx.key_bounds.is_empty() {
        // Add.stats is Option<String> JSON
        let stats_struct = add.stats.as_ref()
            .map(|s| serde_json::from_str(s).unwrap_or(serde_json::Value::Null));

        let mut file_overlaps = true;
        for (key, (src_min, src_max)) in &ctx.key_bounds {
            if !check_file_overlap_optimized(&stats_struct, key, src_min.as_ref(), src_max.as_ref()) {
                file_overlaps = false;
                break;
            }
        }
        
        if !file_overlaps {
            return Ok(FileActionDecision::Skip);
        }
    }

    // 3. Process
    // Clone is cheap enough here compared to IO
    Ok(FileActionDecision::Process { file_view: add.clone() })
}


/// Copy-on-Write 
pub fn execute_copy_on_write(
    ctx: &DeleteContext,
    file_action: &Add,
) -> PolarsResult<Vec<Action>> {
    
    let file_path_str = &file_action.path;
    let full_scan_path = format!("{}/{}", ctx.table_root.trim_end_matches('/'), file_path_str);

    // Build Scan Args
    let scan_cloud_options = unsafe {
        build_cloud_options(
            ctx.cloud_args.provider, 
            ctx.cloud_args.retries, 
            ctx.cloud_args.retry_timeout_ms,
            ctx.cloud_args.retry_init_backoff_ms, 
            ctx.cloud_args.retry_max_backoff_ms, 
            ctx.cloud_args.cache_ttl,
            ctx.cloud_args.keys, 
            ctx.cloud_args.values, 
            ctx.cloud_args.len
        )
    };
    
    // Scan Parquet
    let mut lf = LazyFrame::scan_parquet(
        PlRefPath::new(&full_scan_path), 
        ScanArgsParquet { cloud_options: scan_cloud_options, ..Default::default() }
    )?;

    // Inject Partition Columns
    for target_col in &ctx.partition_cols {
        // Get partition value from action 
        let val_opt = file_action.partition_values.get(target_col).and_then(|v| v.as_ref());

        // Build Literal Expr
        let lit_expr = match val_opt {
            Some(v) => lit(v.as_str()),
            None => lit(NULL)
        };

        // Cast to correct type
        let final_expr = if let Some(dtype) = ctx.polars_schema.get_field(target_col).map(|f| f.dtype.clone()) {
            lit_expr.cast(dtype)
        } else {
            lit_expr
        };
        
        lf = lf.with_column(final_expr.alias(target_col));
    }

    // Optimization
    let has_match_df = lf.clone()
        .filter(ctx.predicate.clone())
        .limit(1)
        .collect_with_engine(Engine::Streaming)?;
    
    if has_match_df.height() == 0 {
        // No Predicate -> Keep File
        return Ok(Vec::new());
    }

    // Rewrite
    // Keep rows unmatch Predicate
    let keep_expr = ctx.predicate.clone().not();
    
    // Drop Partition Columns
    let drop_names: Vec<PlSmallStr> = ctx.partition_cols.iter()
        .map(|s| PlSmallStr::from_string(s.clone()))
        .collect();
    let drop_selector = Selector::ByName { names: drop_names.into(), strict: false }; 

    let new_lf = lf.filter(keep_expr).drop(drop_selector);

    // Generate new name
    // Format：part-timestamp-uuid.parquet
    let new_name = format!("part-{}-{}.parquet", Utc::now().timestamp_millis(), Uuid::new_v4());
    
    // Keep original dir structure
    let parent_dir = std::path::Path::new(file_path_str) 
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "".to_string());
        
    let relative_new_path = if parent_dir.is_empty() { 
        new_name.clone() 
    } else { 
        format!("{}/{}", parent_dir, new_name) 
    };
    
    let full_write_path = format!("{}/{}", ctx.table_root.trim_end_matches('/'), relative_new_path);

    // Sink
    let write_opts = build_parquet_write_options(1, -1, true, 0, 0, -1)
        .map_err(|e| PolarsError::ComputeError(format!("Options error: {}", e).into()))?;
    
    let target = SinkTarget::Path(PlRefPath::new(&full_write_path));
    
    // Rebuild Unified Sink Args
    let unified_args = unsafe {
        build_unified_sink_args(
            false, false, 0, 
            ctx.cloud_args.provider, ctx.cloud_args.retries, ctx.cloud_args.retry_timeout_ms,
            ctx.cloud_args.retry_init_backoff_ms, ctx.cloud_args.retry_max_backoff_ms, ctx.cloud_args.cache_ttl,
            ctx.cloud_args.keys, ctx.cloud_args.values, ctx.cloud_args.len 
        )
    };

    new_lf.sink(SinkDestination::File { target }, FileWriteFormat::Parquet(write_opts), unified_args)?
        .collect_with_engine(Engine::Streaming)?;

    // Read Stats (Metadata)
    let rt = get_runtime();
    let (file_size, stats_json) = rt.block_on(async {
        let path = deltalake::Path::from(relative_new_path.clone());
        let meta = ctx.object_store.head(&path).await
                .map_err(|e| PolarsError::ComputeError(format!("Head error: {}", e).into()))?;
        
        let mut reader = ParquetObjectReader::new(ctx.object_store.clone(), path.clone())
                .with_file_size(meta.size as u64);
        let footer = reader.get_metadata(None).await
                .map_err(|e| PolarsError::ComputeError(format!("Footer error: {}", e).into()))?;
        
        let (_, json) = extract_delta_stats(&footer)?;
        Ok::<_, PolarsError>((meta.size as i64, json))
    })?;

    // Build Actions
    let mut actions = Vec::new();

    // Remove Old File
    let remove = Remove {
        path: file_path_str.clone(),
        deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
        data_change: true,
        extended_file_metadata: Some(true),
        partition_values: Some(file_action.partition_values.clone()),
        size: Some(file_action.size),
        deletion_vector: file_action.deletion_vector.clone(),
        tags: None,
        base_row_id: None,
        default_row_commit_version: None,
    };
    actions.push(Action::Remove(remove));

    // Add New File
    let add = Add {
        path: relative_new_path.clone(),
        size: file_size,
        partition_values: parse_hive_partitions(&relative_new_path, &ctx.partition_cols), // 重新解析最稳
        modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
        data_change: true,
        stats: Some(stats_json),
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    };
    actions.push(Action::Add(add));

    Ok(actions)
}

pub fn execute_merge_on_read(
    ctx: &DeleteContext,
    file_action: &Add,
) -> PolarsResult<Vec<Action>> {
    let file_path_str = &file_action.path;
    let full_scan_path = format!("{}/{}", ctx.table_root.trim_end_matches('/'), file_path_str);

    // Scan Parquet to Get deletion row index
    let scan_cloud_options = unsafe {
        build_cloud_options(
            ctx.cloud_args.provider, 
            ctx.cloud_args.retries, 
            ctx.cloud_args.retry_timeout_ms,
            ctx.cloud_args.retry_init_backoff_ms, 
            ctx.cloud_args.retry_max_backoff_ms, 
            ctx.cloud_args.cache_ttl,
            ctx.cloud_args.keys, 
            ctx.cloud_args.values, 
            ctx.cloud_args.len
        )
    };

    let mut lf = LazyFrame::scan_parquet(
        PlRefPath::new(&full_scan_path), 
        ScanArgsParquet { cloud_options: scan_cloud_options, ..Default::default() }
    )?;

    // Inject Partition cols
    for target_col in &ctx.partition_cols {
        let val_opt = file_action.partition_values.get(target_col).and_then(|v| v.as_ref());
        let lit_expr = match val_opt {
            Some(v) => lit(v.as_str()),
            None => lit(NULL)
        };
        let final_expr = if let Some(dtype) = ctx.polars_schema.get_field(target_col).map(|f| f.dtype.clone()) {
            lit_expr.cast(dtype)
        } else {
            lit_expr
        };
        lf = lf.with_column(final_expr.alias(target_col));
    }

    // Filter deletion rows
    // filter(predicate) -> select(row_nr)
    let deleted_indices_df = lf
        .with_row_index("row_nr", None)
        .filter(ctx.predicate.clone()) 
        .select([col("row_nr")])
        .collect_with_engine(Engine::Streaming)?;

    if deleted_indices_df.height() == 0 {
        return Ok(Vec::new()); 
    }

    // Collect new deleted indices
    let new_deleted_indices: Vec<u32> = deleted_indices_df
        .column("row_nr")?
        .u32()?
        .into_no_null_iter()
        .collect();

    // Handle old DV (Merge Logic)
    let rt = get_runtime(); 
    let table_root_path = deltalake::Path::from(ctx.table_root.trim_end_matches('/'));

    let mut bitmap = if let Some(dv_desc) = &file_action.deletion_vector {
        rt.block_on(async {
            read_deletion_vector(ctx.object_store.clone(), dv_desc, &table_root_path).await
        })?
    } else {
        RoaringBitmap::new()
    };

    // Concat：Old | New
    bitmap.extend(new_deleted_indices);

    // Write new DV file
    let (dv_file_name, dv_size) = rt.block_on(async {
        write_dv_file(ctx.object_store.clone(), &ctx.table_root, &bitmap).await
    })?;

    // Build Actions
    let mut actions = Vec::new();

    // Remove Old File State
    let remove = Remove {
        path: file_path_str.clone(),
        deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
        data_change: true,
        extended_file_metadata: Some(true),
        partition_values: Some(file_action.partition_values.clone()),
        size: Some(file_action.size),
        deletion_vector: file_action.deletion_vector.clone(), 
        tags: None,
        base_row_id: None,
        default_row_commit_version: None,
    };
    actions.push(Action::Remove(remove));

    // Add New File State (Same Path, New DV)
    let new_dv_descriptor = create_dv_descriptor(
        &dv_file_name, 
        dv_size, 
        bitmap.len() as i64 
    )?;

    let add = Add {
        path: file_path_str.clone(), 
        size: file_action.size,   
        partition_values: file_action.partition_values.clone(),
        modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
        data_change: true,
        stats: file_action.stats.clone(), 
        tags: None,
        deletion_vector: Some(new_dv_descriptor), 
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    };
    actions.push(Action::Add(add));

    Ok(actions)
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_delete(
    table_path_ptr: *const c_char,
    predicate_ptr: *mut ExprContext, 
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
        let path_str = ptr_to_str(table_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = parse_table_url(path_str)?;
        let predicate_ctx = unsafe { *Box::from_raw(predicate_ptr) };
        let predicate_expr = predicate_ctx.inner;

        let cloud_args = RawCloudArgs {
            provider: cloud_provider, retries: cloud_retries, retry_timeout_ms: cloud_retry_timeout_ms,
            retry_init_backoff_ms: cloud_retry_init_backoff_ms, retry_max_backoff_ms: cloud_retry_max_backoff_ms,
            cache_ttl: cloud_cache_ttl, keys: cloud_keys, values: cloud_values, len: cloud_len,
        };
        
        let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        delete_delta_internal(table_url, predicate_expr, delta_storage_options, cloud_args)?;
        Ok(())
    })

}

pub(crate) fn delete_delta_internal(
    table_url: url::Url,
    predicate_expr: Expr,
    delta_storage_options: HashMap<String, String>,
    cloud_args: RawCloudArgs,
) -> PolarsResult<()> {
    let rt = get_runtime();
    let max_attempts = std::env::var("POLARS_DELTA_MAX_RETRIES")
        .unwrap_or_else(|_| "5".to_string())
        .parse::<u32>()
        .unwrap_or(5);

    for attempt in 1..=max_attempts {
        // ==========================================
        // Phase 1: Load 
        // ==========================================
        let (t, schema, part_cols, all_files) = rt.block_on(async {
            let t = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options.clone())
                .await.map_err(|e| PolarsError::ComputeError(format!("Delta load error: {}", e).into()))?;
            
            let schema = get_polars_schema_from_delta(&t)?;
            let snapshot = t.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot error: {}", e).into()))?;
            let part_cols = snapshot.metadata().partition_columns().clone();

            let binding = t.clone();
            let mut stream = binding.get_active_add_actions_by_partitions(&[]);
            let mut all_files = Vec::new();
            while let Some(item) = stream.next().await {
                let view = item.map_err(|e| PolarsError::ComputeError(format!("Stream error: {}", e).into()))?;
                all_files.push(view_to_add_action(&view));
            }
            Ok::<_, PolarsError>((t, schema, part_cols, all_files))
        })?;

        // ==========================================
        // Phase 2: Execution 
        // ==========================================
        let snapshot = t.snapshot().map_err(|e| PolarsError::ComputeError(format!("{}", e).into()))?;
        let strategy = DeleteStrategy::determine(snapshot, false);
        let ctx = DeleteContext::new(
            &t, predicate_expr.clone(), schema.clone(), part_cols.clone(), strategy, cloud_args.clone()
        );

        let mut actions_to_commit = Vec::new();
        for file in all_files {
            let decision = prune_file_from_add(&ctx, &file)?; 
            match decision {
                FileActionDecision::Skip => continue,
                FileActionDecision::FullDrop(action) => actions_to_commit.push(action),
                FileActionDecision::Process { file_view } => {
                    let new_actions = match ctx.strategy {
                        DeleteStrategy::CopyOnWrite => execute_copy_on_write(&ctx, &file_view)?,
                        DeleteStrategy::MergeOnRead => execute_merge_on_read(&ctx, &file_view)?,
                    };
                    actions_to_commit.extend(new_actions);
                }
            }
        }

        if actions_to_commit.is_empty() {
            return Ok(());
        }

        // ==========================================
        // Phase 3: Commit 
        // ==========================================
        let commit_res = rt.block_on(async {
            let operation = DeltaOperation::Delete { predicate: None };
            
            CommitBuilder::default()
                .with_actions(actions_to_commit)
                .build(
                    Some(t.snapshot().map_err(|e| DeltaTableError::Generic(e.to_string()))?),
                    t.log_store().clone(),
                    operation
                ).await
        });

        match commit_res {
            Ok(_) => return Ok(()), 
            Err(deltalake::errors::DeltaTableError::CommitValidation { .. }) 
            | Err(deltalake::errors::DeltaTableError::VersionAlreadyExists(_)) 
            | Err(deltalake::errors::DeltaTableError::Transaction { .. }) => {
                
                if attempt < max_attempts {
                    let base_sleep = 50_u64.saturating_mul(2_u64.pow(attempt - 1));
                    let capped_sleep = std::cmp::min(base_sleep, 1000);
                    
                    let mut rng = rand::rng();
                    let jitter_millis = rng.random_range((capped_sleep / 2)..=(capped_sleep * 3 / 2));

                    println!("[Delta-RS] OCC Conflict! Backoff for {}ms (Attempt {}/{})", 
                             jitter_millis, attempt, max_attempts);

                    std::thread::sleep(std::time::Duration::from_millis(jitter_millis));
                    continue; 
                } else {
                    return Err(PolarsError::ComputeError(
                        format!("Max retry attempts ({}) reached for Delete OCC conflict", max_attempts).into()
                    ));
                }
            },
            
            Err(e) => return Err(PolarsError::ComputeError(format!("Commit failed: {}", e).into())),
        }
    }
    
    Ok(())
}