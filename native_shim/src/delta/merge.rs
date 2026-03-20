use std::{collections::{HashMap, HashSet}, ffi::c_char, time::{SystemTime, UNIX_EPOCH}};

use chrono::{DateTime, NaiveDate};
use futures::StreamExt;
use polars::prelude::{file_provider::HivePathProvider, *};
use polars_buffer::Buffer;
use rand::RngExt;
use roaring::RoaringBitmap;
use url::Url;
use deltalake::{DeltaTable, Path, table::state::DeltaTableState};
use deltalake::kernel::{Action, Add, Remove, scalars::ScalarExt};
use deltalake::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use deltalake::protocol::{DeltaOperation};
use uuid::Uuid;

use crate::{delta::{delete::{create_dv_descriptor, write_dv_file}, deletion_vector::{read_deletion_vector}, utils::*}, pl_io::parquet::parquet_utils::build_parquet_write_options, types::{ExprContext, LazyFrameContext}};
use crate::pl_io::io_utils::{build_cloud_options, build_unified_sink_args};

const MAX_PRUNING_CANDIDATES: usize = 100_000;

// -------------------------------------------------------------------------
// Merge / Update Helpers
// -------------------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MergeStrategy {
    /// Copy-on-Write: Rewrite entire files for any match. (Classic)
    CopyOnWrite,
    /// Merge-on-Read: Write new files for Inserts/Updates, write DVs for Deletes/Updates.
    MergeOnRead,
}

impl MergeStrategy {
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
                // Read "readerFeatures" or "writerFeatures" array in JSON
                json.get("readerFeatures")
                    .and_then(|features| features.as_array())
                    .map(|arr| arr.iter().any(|v| v.as_str() == Some("deletionVectors")))
            })
            .unwrap_or(false);

        // If DeletionVectors is on and Writer Version >= 7
        if has_deletion_vectors && min_writer_version >= 7 {
            Self::MergeOnRead
        } else {
            Self::CopyOnWrite
        }
    }
}

pub(crate) struct MergeContext {
    pub merge_keys: Vec<String>,
    pub table_url: Url,
    pub can_evolve: bool,
    pub strategy: MergeStrategy,
    // Conditions (Owned Exprs)
    pub cond_update: Option<Expr>,     // Default: true
    pub cond_delete: Option<Expr>,     // Default: false
    pub cond_insert: Option<Expr>,     // Default: true
    pub cond_src_delete: Option<Expr>, // Default: false
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub(crate) enum PruningBound {
    Integer(i128),
    Float(f64),
    Lexical(String), 
}
pub(crate) fn bound_to_json(bound: &PruningBound) -> serde_json::Value {
    match bound {
        // A. Integer (i128)
        PruningBound::Integer(i) => {
            if let Ok(val) = i64::try_from(*i) {
                serde_json::json!(val)
            } else {
                serde_json::json!(*i as f64)
            }
        },
        
        // B. Float (f64) -> JSON Number
        PruningBound::Float(f) => serde_json::json!(f),
        
        // C. Lexical (String) -> JSON String
        // Contains Date, String
        PruningBound::Lexical(s) => serde_json::json!(s),
    }
}
/// Parse Source LazyFrame to extract partition values
pub(crate) fn get_source_partition_values(
    source_lf: &LazyFrame,
    partition_cols: &[String]
) -> Option<HashSet<Vec<String>>> {
    if partition_cols.is_empty() {
        return None;
    }
    let cols: Vec<Expr> = partition_cols.iter().map(|c| col(c)).collect();
    let distinct_df = source_lf.clone().select(cols).unique(None, UniqueKeepStrategy::Any).collect().ok()?;

    let height = distinct_df.height();
    if height == 0 { return Some(HashSet::new()); }

    let mut affected_partitions = HashSet::with_capacity(height);
    for row_idx in 0..height {
        let mut row_values = Vec::with_capacity(partition_cols.len());
        for col_name in partition_cols {
            let s = distinct_df.column(col_name).ok()?;
            let val = s.get(row_idx).ok()?;
            row_values.push(any_value_to_string(&val));
        }
        affected_partitions.insert(row_values);
    }
    Some(affected_partitions)
}
pub(crate) fn any_value_to_string(val: &AnyValue) -> String {
    match val {
        AnyValue::Null => "__HIVE_DEFAULT_PARTITION__".to_string(),
        AnyValue::String(s) => s.to_string(),
        AnyValue::StringOwned(s) => s.to_string(),
        AnyValue::Binary(b) => String::from_utf8_lossy(b).to_string(),
        AnyValue::BinaryOwned(b) => String::from_utf8_lossy(&b).to_string(),
        v => v.to_string(),
    }
}

pub(crate) fn any_value_to_bound(val: &AnyValue, _dtype: &DataType) -> Option<PruningBound> {
    match val {
        AnyValue::Int8(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::Int16(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::Int32(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::Int64(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::UInt8(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::UInt16(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::UInt32(v) => Some(PruningBound::Integer(*v as i128)),
        AnyValue::UInt64(v) => Some(PruningBound::Integer(*v as i128)),
        
        AnyValue::Float32(v) => Some(PruningBound::Float(*v as f64)),
        AnyValue::Float64(v) => Some(PruningBound::Float(*v)),
        
        AnyValue::String(s) => Some(PruningBound::Lexical(s.to_string())),
        AnyValue::StringOwned(s) => Some(PruningBound::Lexical(s.to_string())),
        AnyValue::Date(days) => {
            let date = NaiveDate::from_num_days_from_ce_opt(*days + 719163)?;
            Some(PruningBound::Lexical(date.format("%Y-%m-%d").to_string()))
        },
        AnyValue::Datetime(v, unit, _) => {
            let (seconds, nanos) = match unit {
                TimeUnit::Nanoseconds => (v.div_euclid(1_000_000_000), (v.rem_euclid(1_000_000_000)) as u32),
                TimeUnit::Microseconds => (v.div_euclid(1_000_000), (v.rem_euclid(1_000_000) * 1_000) as u32),
                TimeUnit::Milliseconds => (v.div_euclid(1_000), (v.rem_euclid(1_000) * 1_000_000) as u32),
            };
            let dt_utc = DateTime::from_timestamp(seconds, nanos)?;
            Some(PruningBound::Lexical(dt_utc.to_rfc3339()))
        },
        _ => None 
    }
}

// Get Source Min/Max
pub(crate) fn get_source_key_bounds(lf: &LazyFrame, key: &str, dtype: &DataType) -> Option<(PruningBound, PruningBound)> {
    if !dtype.is_numeric() && !dtype.is_temporal() && !matches!(dtype, DataType::String) {
        return None;
    }
    let df = lf.clone()
        .select([col(key).min().alias("min_v"), col(key).max().alias("max_v")])
        .collect().ok()?;

    let min_s = df.column("min_v").ok()?;
    let max_s = df.column("max_v").ok()?;
    
    let min_val = any_value_to_bound(&min_s.get(0).ok()?, dtype)?;
    let max_val = any_value_to_bound(&max_s.get(0).ok()?, dtype)?;

    Some((min_val, max_val))
}

#[derive(Debug, Clone)]
pub(crate) enum PruningCandidates {
    Integers(Vec<i64>), 
    Strings(Vec<String>), 
}

/// Phase 1: Pruning
/// Partition Pruning, Z-Order Bounds Pruning, Discrete Candidates Pruning
pub(crate) async fn phase_1_load_table(
    table_url: Url,
    storage_options: HashMap<String, String>,
) -> PolarsResult<(DeltaTable, Vec<String>, SchemaRef)> {
    
    let table = DeltaTable::try_from_url_with_storage_options(
        table_url,
        storage_options
    ).await.map_err(|e| PolarsError::ComputeError(format!("Delta load error: {}", e).into()))?;

    let polars_schema = get_polars_schema_from_delta(&table)?;
    let snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot: {}", e).into()))?;
    let part_cols = snapshot.metadata().partition_columns().clone();

    Ok((table, part_cols, polars_schema.into()))
}
// =========================================================
// Step 2: Analyze Source (Sync - Pure CPU/Polars)
// =========================================================
pub(crate) fn phase_1_analyze_source(
    merge_keys: &[String],
    source_lf: &LazyFrame,
    polars_schema: &Schema,
    part_cols: &[String],
) -> PolarsResult<(Option<HashSet<Vec<String>>>, HashMap<String, (serde_json::Value, serde_json::Value)>, Option<PruningCandidates>)> {
    
    // A. Partition Values
    let source_partitions = get_source_partition_values(source_lf, part_cols);

    // B. Key Bounds
    let mut key_bounds_json = HashMap::new(); 
    for key in merge_keys {
        let dtype = polars_schema.get_field(key).map(|f| f.dtype.clone()).unwrap_or(DataType::Null);
        if let Some((min_lit, max_lit)) = get_source_key_bounds(source_lf, key, &dtype) {
            let min_json = bound_to_json(&min_lit);
            let max_json = bound_to_json(&max_lit);
            key_bounds_json.insert(key.clone(), (min_json, max_json));
        }
    }

    // C. Discrete Candidates
    let mut pruning_candidates: Option<PruningCandidates> = None;
    if merge_keys.len() == 1 {
        let key_col = &merge_keys[0];
        let distinct_lf = source_lf.clone()
            .select([col(key_col)])
            .unique(None, UniqueKeepStrategy::Any)
            .limit((MAX_PRUNING_CANDIDATES + 1) as u32);

        if let Ok(df) = distinct_lf.collect_with_engine(Engine::Streaming) {
            if df.height() <= MAX_PRUNING_CANDIDATES {
                let s = df.column(&key_col).unwrap();
                let dtype = s.dtype();
                if dtype.is_integer() {
                    if let Ok(s_cast) = s.cast(&DataType::Int64) {
                        if let Ok(ca) = s_cast.i64() {
                            let mut keys: Vec<i64> = ca.into_iter().flatten().collect();
                            keys.sort_unstable();
                            if !keys.is_empty() { pruning_candidates = Some(PruningCandidates::Integers(keys)); }
                        }
                    }
                } else if dtype == &DataType::String {
                     if let Ok(ca) = s.str() {
                         let mut keys: Vec<String> = ca.into_iter().flatten().map(|v| v.to_string()).collect();
                         keys.sort_unstable();
                         if !keys.is_empty() { pruning_candidates = Some(PruningCandidates::Strings(keys)); }
                     }
                }
            }
        }
    }

    Ok((source_partitions, key_bounds_json, pruning_candidates))
}
pub(crate) async fn phase_1_scan_and_prune(
    table: &DeltaTable,
    part_cols: &[String],
    source_partitions: Option<HashSet<Vec<String>>>,
    key_bounds_json: HashMap<String, (serde_json::Value, serde_json::Value)>,
    pruning_candidates: Option<PruningCandidates>,
    merge_keys_0: Option<&String>,
) -> PolarsResult<Vec<Add>> {
    
    let mut candidate_files = Vec::new();
    let t_scan = table.clone(); 
    let mut stream = t_scan.get_active_add_actions_by_partitions(&[]);

    while let Some(action_res) = stream.next().await {
        let view = action_res.map_err(|e| PolarsError::ComputeError(format!("Delta stream error: {}", e).into()))?;
        
        // --- Filter 1: Partition Pruning ---
        if let Some(affected_set) = &source_partitions {
            let maybe_key = view.partition_values().map(|struct_data| {
                 let fields = struct_data.fields();
                 let values = struct_data.values();
                 let mut key = Vec::with_capacity(part_cols.len());
                 for target_col_name in part_cols {
                    let val_str = fields.iter().zip(values.iter())
                        .find(|(f, _)| f.name() == target_col_name)
                        .map(|(_, v)| if v.is_null() { "__HIVE_DEFAULT_PARTITION__".to_string() } else { v.serialize() })
                        .unwrap_or_else(|| "__HIVE_DEFAULT_PARTITION__".to_string());
                    key.push(val_str);
                 }
                 key
            });

            let file_key = match maybe_key {
                Some(k) => k,
                None => {
                    let path_str = view.path();
                    let file_parts_map = parse_hive_partitions(path_str.as_ref(), part_cols);
                    part_cols.iter().map(|c| file_parts_map.get(c).cloned().flatten().unwrap_or_else(|| "__HIVE_DEFAULT_PARTITION__".to_string())).collect()
                }
            };
            
            if !affected_set.contains(&file_key) { continue; }
        }

        // --- Filter 2: Stats Pruning ---
        let mut file_overlaps = true;
        let stats_json_str = view.stats();
        let stats_struct: Option<serde_json::Value> = stats_json_str
            .map(|s| serde_json::from_str(&s).unwrap_or(serde_json::Value::Null));

        if !key_bounds_json.is_empty() {
            for (key, (src_min, src_max)) in &key_bounds_json {
                if !check_file_overlap_optimized(&stats_struct, key, Some(src_min), Some(src_max)) {
                     file_overlaps = false; break;
                }
            }
        }

        // --- Filter 3: Discrete Candidates ---
        if file_overlaps && pruning_candidates.is_some() {
            if let Some(key_col) = merge_keys_0 {
                let candidates = pruning_candidates.as_ref().unwrap();
                let f_min_opt = extract_stats_value(&stats_struct, "minValues", key_col);
                let f_max_opt = extract_stats_value(&stats_struct, "maxValues", key_col);

                if let (Some(f_min_val), Some(f_max_val)) = (f_min_opt, f_max_opt) {
                    let hit = match candidates {
                        PruningCandidates::Integers(sorted_keys) => {
                            let f_min_i = as_i64_safe(f_min_val);
                            let f_max_i = as_i64_safe(f_max_val);
                            if let (Some(mn), Some(mx)) = (f_min_i, f_max_i) {
                                let idx = sorted_keys.partition_point(|&x| x < mn);
                                idx < sorted_keys.len() && sorted_keys[idx] <= mx
                            } else { true }
                        },
                        PruningCandidates::Strings(sorted_keys) => {
                             let f_min_s = f_min_val.as_str();
                             let f_max_s = f_max_val.as_str();
                             if let (Some(mn), Some(mx)) = (f_min_s, f_max_s) {
                                 let idx = sorted_keys.partition_point(|x| x.as_str() < mn);
                                 idx < sorted_keys.len() && sorted_keys[idx].as_str() <= mx
                             } else { true }
                        }
                    };
                    if !hit { file_overlaps = false; }
                }
            }
        }

        if file_overlaps {
            candidate_files.push(crate::delta::utils::view_to_add_action(&view));
        }
    }
    Ok(candidate_files)
}

pub(crate) fn phase_validation(
    merge_keys: &[String],     
    can_evolve: bool,          
    source_lf: &mut LazyFrame,
    target_schema: &Schema,
) -> PolarsResult<()> {
    
    // =========================================================
    // Schema Compatibility Check (Metadata Level)
    // =========================================================
    let src_schema = source_lf.collect_schema()
        .map_err(|e| PolarsError::ComputeError(format!("Failed to get source schema: {}", e).into()))?;

    // A. Merge Key Checks
    for key in merge_keys {
        // 1. Check Source Existence
        let src_field = src_schema.get(key)
            .ok_or_else(|| PolarsError::ComputeError(format!(
                "Merge Key '{}' not found in SOURCE table schema.", key
            ).into()))?;

        // 2. Check Target Existence
        let tgt_field = target_schema.get_field(key)
            .ok_or_else(|| PolarsError::ComputeError(format!(
                "Merge Key '{}' not found in TARGET table schema.", key
            ).into()))?;

        // 3. Strict Type Check
        if src_field != &tgt_field.dtype {
             return Err(PolarsError::SchemaMismatch(format!(
                "Merge Key Type Mismatch for column '{}'! \n\
                 Source: {:?} \n\
                 Target: {:?} \n\
                 Merge Keys must have identical types.", 
                key, src_field, tgt_field.dtype
            ).into()));
        }
    }

    // Extra Column Check (Schema Evolution Guard)
    if !can_evolve {
        for name in src_schema.iter_names() {
            if target_schema.get_field(name).is_none() {
                return Err(PolarsError::SchemaMismatch(
                    format!("Schema mismatch: Source column '{}' is not present in target. Pass 'can_evolve=true' to allow.", name).into()
                ));
            }
        }
    }

    // =========================================================
    // 2. Data Quality Check (Execution Level)
    // =========================================================
    let partition_exprs: Vec<Expr> = merge_keys.iter().map(|k| col(k)).collect();
    
    let has_null_expr = polars::prelude::any_horizontal(
         merge_keys.iter().map(|k| col(k).is_null()).collect::<Vec<_>>()
    )?.alias("has_null_key");

    let check_lf = source_lf.clone()
        .select([
            len().over(partition_exprs.clone()).alias("group_count"),
            has_null_expr
        ])
        .filter(
            col("group_count").gt(lit(1)) // Duplicated
            .or(col("has_null_key"))      // Null Key Found
        )
        .limit(1); 

    let check_df = check_lf.collect_with_engine(Engine::Streaming)?;

    // =========================================================
    // Error Reporting
    // =========================================================
    if check_df.height() > 0 {
        // Null or Duplicate
        let is_null_error = check_df.column("has_null_key")?.bool()?.get(0).unwrap_or(false);

        if is_null_error {
             let msg = format!(
                "CRITICAL ERROR: Null values detected in Merge Keys! \n\
                 Merge Keys: {:?}", 
                merge_keys
            );
            return Err(PolarsError::ComputeError(msg.into()));
        } else {
            let example_dupes = source_lf.clone()
                .group_by(partition_exprs) 
                .agg([len().alias("duplicate_count")]) 
                .filter(col("duplicate_count").gt(lit(1))) 
                .sort(["duplicate_count"], SortMultipleOptions { 
                    descending: vec![true], 
                    ..Default::default() 
                }) 
                .limit(5) 
                .collect()?; 

            let msg = format!(
                "CRITICAL ERROR: Duplicate keys detected in SOURCE table!\n\
                 Merge expects unique source keys per checking round.\n\
                 [Merge Keys]: {:?}\n\
                 --- Duplicate Key Examples (Top 5) ---\n\
                 {}", 
                merge_keys, 
                example_dupes
            );
            
            return Err(PolarsError::Duplicate(msg.into()));
        }
    }

    Ok(())
}

pub(crate) fn construct_target_lf(
    table_url: &url::Url,       
    strategy: crate::delta::merge::MergeStrategy,
    table: &deltalake::DeltaTable, 
    remove_actions: &[deltalake::kernel::Add], 
    target_schema: &Schema,
    cloud_args: &crate::delta::utils::RawCloudArgs,
) -> PolarsResult<LazyFrame> {
    
    let is_mor = strategy == crate::delta::merge::MergeStrategy::MergeOnRead;

    if remove_actions.is_empty() {
        if is_mor {
            let mut final_schema = target_schema.clone();
            final_schema.with_column(PlSmallStr::from_static("__file_path"), DataType::String.into());
            final_schema.with_column(PlSmallStr::from_static("__row_index"), DataType::UInt32.into());
            return Ok(DataFrame::empty_with_schema(&final_schema).lazy());
        } else {
            return Ok(DataFrame::empty_with_schema(target_schema).lazy());
        }
    }

    let root_trimmed = table_url.as_str().trim_end_matches('/');
    
    let make_scan_args = || unsafe {
        let mut args = ScanArgsParquet::default();
        args.hive_options = HiveOptions {
            enabled: Some(true), hive_start_idx: 0, schema: None, try_parse_dates: true,
        };
        args.cloud_options = build_cloud_options(
            cloud_args.provider, cloud_args.retries, cloud_args.retry_timeout_ms,
            cloud_args.retry_init_backoff_ms, cloud_args.retry_max_backoff_ms, cloud_args.cache_ttl,
            cloud_args.keys, cloud_args.values, cloud_args.len,
        );

        if is_mor {
            args.row_index = Some(RowIndex { 
                name: "__row_index".into(), 
                offset: 0 
            });
            args.include_file_paths = Some(PlSmallStr::from_static("__file_path"));
        }
        args
    };

    // =========================================================
    // CoW (Fast Path): Bulk Scan
    // =========================================================
    if !is_mor {
        let full_paths: Vec<PlRefPath> = remove_actions.iter()
            .map(|r| PlRefPath::new(format!("{}/{}", root_trimmed, r.path)))
            .collect();
        
        let paths_buffer = Buffer::from(full_paths);
        return LazyFrame::scan_parquet_files(paths_buffer, make_scan_args());
    }

    // =========================================================
    // MoR Mode: MUST scan iteratively to guarantee row_index = 0 per file
    // =========================================================
    let rt = crate::delta::utils::get_runtime();
    let object_store = table.object_store();
    let table_root = deltalake::Path::from(root_trimmed);

    let lfs = rt.block_on(async {
        let mut processed = Vec::with_capacity(remove_actions.len());
        
        for r in remove_actions {
            let full_path = format!("{}/{}", root_trimmed, r.path);
            
            // Scan single files one by one
            let mut lf = LazyFrame::scan_parquet(
                PlRefPath::new(&full_path), 
                make_scan_args()
            )?;

            // Read and Apply DV if exists
            if let Some(dv) = &r.deletion_vector {
                let bitmap = crate::delta::deletion_vector::read_deletion_vector(object_store.clone(), dv, &table_root).await?;
                
                let indices: Vec<u32> = bitmap.into_iter().collect();
                if !indices.is_empty() {
                    let mask = col("__row_index").is_in(lit(Series::new(PlSmallStr::from_static(""), indices)).implode(),false).not();
                    lf = lf.filter(mask);
                }
            }
            processed.push(lf);
        }
        Ok::<_, PolarsError>(processed)
    })?;

    // Concat Everything
    let args = UnionArgs {
        parallel: true,
        rechunk: false,
        to_supertypes: true,
        diagonal: true, 
        ..Default::default()
    };
    
    let final_lf = polars::prelude::concat(lfs, args)
        .map_err(|e| PolarsError::ComputeError(format!("Merge Target Concat failed: {}", e).into()))?;

    Ok(final_lf)
}

/// Phase 3: Planning
pub(crate) fn phase_planning(
    ctx: &MergeContext,
    target_lf: LazyFrame,
    mut source_lf: LazyFrame,
    target_schema: &Schema, 
) -> PolarsResult<LazyFrame> {

    // =========================================================
    // 1. Rename Source Columns (Avoid Collision)
    // =========================================================
    let src_schema = source_lf.collect_schema()
        .map_err(|e| PolarsError::ComputeError(format!("Source schema error: {}", e).into()))?;
    
    let mut rename_old = Vec::new();
    let mut rename_new = Vec::new();
    let mut source_cols_set = HashSet::new();

    for name in src_schema.iter_names() {
        rename_old.push(name.as_str());
        rename_new.push(format!("{}_src_tmp", name)); 
        source_cols_set.insert(name.to_string());
    }

    let source_renamed = source_lf.rename(rename_old, rename_new, true);

    // =========================================================
    // Execute Full Outer Join
    // =========================================================
    let left_on: Vec<Expr> = ctx.merge_keys.iter().map(|k| col(k)).collect();
    let right_on: Vec<Expr> = ctx.merge_keys.iter().map(|k| col(&format!("{}_src_tmp", k))).collect();

    let join_args = JoinArgs {
        how: JoinType::Full,
        validation: JoinValidation::ManyToMany, 
        coalesce: JoinCoalesce::KeepColumns,    
        maintain_order: MaintainOrderJoin::None,
        ..Default::default()
    };

    let joined_lf = target_lf.join(source_renamed, left_on, right_on, join_args);

    // =========================================================
    // 3. Define Logic Constants & State
    // =========================================================
    // Action Constants
    let act_keep   = lit(0); // Keep Target
    let act_insert = lit(1); // Insert new value in Source 
    let act_update = lit(2); // Update Target with Source 
    let act_delete = lit(3); // Marked as delete
    let act_ignore = lit(4); // Ignore

    // Row State 
    let first_key = &ctx.merge_keys[0];
    let src_key_col = format!("{}_src_tmp", first_key);
    
    let src_exists = col(&src_key_col).is_not_null();
    let tgt_exists = col(first_key).is_not_null();

    // Three Status
    let is_matched     = src_exists.clone().and(tgt_exists.clone());
    let is_source_only = src_exists.clone().and(tgt_exists.clone().not());
    let is_target_only = src_exists.not().and(tgt_exists);
    let expr_update = ctx.cond_update.clone().unwrap_or(lit(true));
    let expr_delete = ctx.cond_delete.clone().unwrap_or(lit(false));
    let expr_insert = ctx.cond_insert.clone().unwrap_or(lit(true));
    let expr_src_del = ctx.cond_src_delete.clone().unwrap_or(lit(false));
    // =========================================================
    // Calculate Action Column
    // =========================================================
    // Delete > Update > Insert > Ignore/Keep
    let action_expr = when(is_matched.clone().and(expr_delete))
        .then(act_delete.clone())
        .when(is_matched.and(expr_update))
        .then(act_update.clone())
        .when(is_source_only.clone().and(expr_insert))
        .then(act_insert.clone())
        .when(is_source_only)
        .then(act_ignore.clone()) 
        .when(is_target_only.and(expr_src_del))
        .then(act_delete.clone()) 
        .otherwise(act_keep)      
        .alias("_merge_action");

    // =========================================================
    // 5. Generate Column Expressions (Projection & Evolution)
    // =========================================================
    let mut final_exprs = Vec::new();

    // Target Schema + New Source Columns
    let mut output_cols = Vec::new();
    let mut seen = HashSet::new();

    // Add Target Columns
    for name in target_schema.iter_names() {
        output_cols.push(name.to_string());
        seen.insert(name.to_string());
    }
    // Add Source New Column (Schema Evolution)
    for name in src_schema.iter_names() {
        if !seen.contains(name.as_str()) {
            output_cols.push(name.to_string());
        }
    }

    for col_name in output_cols {
        let src_col_alias = format!("{}_src_tmp", col_name);
        let has_source = source_cols_set.contains(&col_name);
        let has_target = target_schema.get_field(&col_name).is_some();

        let src_val = if has_source { col(&src_col_alias) } else { lit(NULL) };
        let tgt_val = if has_target { col(&col_name) } else { lit(NULL) };

        let col_expr = if has_source {
            // Case 1: Source has this column (Full Update / New Data)
            // If Insert or Update, use Source；or Target
            when(col("_merge_action").eq(act_insert.clone())
                .or(col("_merge_action").eq(act_update.clone())))
            .then(src_val)
            .otherwise(tgt_val)
        } else {
            // Case 2: Source does not have this column (Partial Update / Evolution Old Rows)
            // Logic: 
            // - Insert (new row): Null
            // - Update (old row): Keep target
            // - Keep: Target
            when(col("_merge_action").eq(act_insert.clone()))
            .then(lit(NULL))
            .otherwise(tgt_val)
        };

        final_exprs.push(col_expr.alias(&col_name));
    }

    // =========================================================
    // 6. Final Assemble
    // =========================================================
    let processed_lf = joined_lf
        .with_column(action_expr)
        .filter(
            col("_merge_action").neq(act_delete.clone()) 
            .and(col("_merge_action").neq(act_ignore))   
        )
        .select(final_exprs);

    Ok(processed_lf)
}

/// Phase 3 (MoR): Planning
/// Returns: 
/// 1. new_data_lf: Rows to be written to new Parquet files (Updates' New Values + Inserts)
/// 2. tombstones_lf: Rows to be soft-deleted (Updates Old Rows + Deletes) -> (Path, RowIndex)
pub(crate) fn phase_planning_mor(
    ctx: &MergeContext,
    target_lf: LazyFrame, 
    mut source_lf: LazyFrame,
    target_schema: &Schema, 
) -> PolarsResult<(LazyFrame, LazyFrame)> {

    // =========================================================
    // 1. Rename Source Columns (Standard Procedure)
    // =========================================================
    let src_schema = source_lf.collect_schema()
        .map_err(|e| PolarsError::ComputeError(format!("Source schema error: {}", e).into()))?;
    
    let mut rename_old = Vec::new();
    let mut rename_new = Vec::new();
    let mut source_cols_set = HashSet::new();

    for name in src_schema.iter_names() {
        rename_old.push(name.as_str());
        rename_new.push(format!("{}_src_tmp", name)); 
        source_cols_set.insert(name.to_string());
    }

    let source_renamed = source_lf.rename(rename_old, rename_new, true);

    // =========================================================
    // 2. Join (Full Outer)
    // =========================================================
    let left_on: Vec<Expr> = ctx.merge_keys.iter().map(|k| col(k)).collect();
    let right_on: Vec<Expr> = ctx.merge_keys.iter().map(|k| col(&format!("{}_src_tmp", k))).collect();

    let join_args = JoinArgs {
        how: JoinType::Full,
        validation: JoinValidation::ManyToMany, 
        coalesce: JoinCoalesce::KeepColumns,    
        maintain_order: MaintainOrderJoin::None,
        ..Default::default()
    };

    let joined_lf = target_lf.join(source_renamed, left_on, right_on, join_args);

    // =========================================================
    // 3. Define Logic Actions
    // =========================================================
    // Action Constants
    let act_keep   = lit(0);
    let act_insert = lit(1);
    let act_update = lit(2);
    let act_delete = lit(3);
    let act_ignore = lit(4);

    // Row State 
    let first_key = &ctx.merge_keys[0];
    let src_key_col = format!("{}_src_tmp", first_key);
    
    let src_exists = col(&src_key_col).is_not_null();
    let tgt_exists = col(first_key).is_not_null();

    // Three Status
    let is_matched     = src_exists.clone().and(tgt_exists.clone());
    let is_source_only = src_exists.clone().and(tgt_exists.clone().not());
    let is_target_only = src_exists.not().and(tgt_exists);
    
    // Conditions
    let expr_update = ctx.cond_update.clone().unwrap_or(lit(true));
    let expr_delete = ctx.cond_delete.clone().unwrap_or(lit(false));
    let expr_insert = ctx.cond_insert.clone().unwrap_or(lit(true));
    let expr_src_del = ctx.cond_src_delete.clone().unwrap_or(lit(false));

    // Calculate Action Column
    let action_expr = when(is_matched.clone().and(expr_delete))
        .then(act_delete.clone())
        .when(is_matched.and(expr_update))
        .then(act_update.clone())
        .when(is_source_only.clone().and(expr_insert))
        .then(act_insert.clone())
        .when(is_source_only)
        .then(act_ignore.clone()) 
        .when(is_target_only.and(expr_src_del))
        .then(act_delete.clone()) 
        .otherwise(act_keep)      
        .alias("_merge_action");

    let lf_with_action = joined_lf.with_column(action_expr);

    // =========================================================
    // 4. BRANCH A: New Data Generation (Inserts + Updates)
    // =========================================================
    
    let mut new_data_exprs = Vec::new();
    let mut output_cols = Vec::new();
    let mut seen = HashSet::new();

    // Target Columns
    for name in target_schema.iter_names() {
        // Skip metadata cols
        if name == "__row_index" || name == "__file_path" { continue; }
        output_cols.push(name.to_string());
        seen.insert(name.to_string());
    }
    // New Source Columns (Evolution)
    for name in src_schema.iter_names() {
        if !seen.contains(name.as_str()) {
            output_cols.push(name.to_string());
        }
    }

    for col_name in output_cols {
        let src_col_alias = format!("{}_src_tmp", col_name);
        let has_source = source_cols_set.contains(&col_name);
        let has_target = target_schema.get_field(&col_name).is_some();

        let src_val = if has_source { col(&src_col_alias) } else { lit(NULL) };
        let tgt_val = if has_target { col(&col_name) } else { lit(NULL) };

        // Projection Logic:
        // - Insert: Source Value (or Null)
        // - Update: Source Value (or Target Value if partial update)
        let col_expr = if has_source {
            when(col("_merge_action").eq(act_insert.clone())
                .or(col("_merge_action").eq(act_update.clone())))
            .then(src_val)
            .otherwise(tgt_val) // Should be filtered out anyway, but safe fallback
        } else {
            // Source missing this col
            // Insert -> Null
            // Update -> Keep Target
            when(col("_merge_action").eq(act_insert.clone()))
            .then(lit(NULL))
            .otherwise(tgt_val)
        };

        new_data_exprs.push(col_expr.alias(&col_name));
    }

    let new_data_lf = lf_with_action.clone()
        .filter(
            col("_merge_action").eq(act_insert.clone())
            .or(col("_merge_action").eq(act_update.clone()))
        )
        .select(new_data_exprs);

    // =========================================================
    // 5. BRANCH B: Tombstones (Deletes + Updates' Old Rows)
    // =========================================================
    // Update = Soft Delete Old + Insert New。
    
    let tombstones_lf = lf_with_action
        .filter(
            col("_merge_action").eq(act_delete.clone())
            .or(col("_merge_action").eq(act_update.clone()))
        )
        .select([
            col("__file_path"),
            col("__row_index"), 
        ]);

    Ok((new_data_lf, tombstones_lf))
}

// Phase 4: Execution 
pub(crate) fn phase_execution(
    table_url: &url::Url,
    processed_lf: LazyFrame,
    partition_cols: &[String],
    cloud_args: &RawCloudArgs,
) -> PolarsResult<(String, Uuid)> {
    
    // 1. Generate Identity
    let write_id = Uuid::new_v4();
    let staging_dir_name = format!(".merge_staging_{}", write_id);
    
    // table_url: s3://bucket/table -> trim -> s3://bucket/table
    let root_trimmed = table_url.as_str().trim_end_matches('/');
    let staging_uri = format!("{}/{}", root_trimmed, staging_dir_name);

    // 2. Configure Partition Strategy
    let partition_strategy = if partition_cols.is_empty() {
        PartitionStrategy::FileSize
    } else {
        let keys: Vec<Expr> = partition_cols.iter().map(|n| col(n)).collect();
        PartitionStrategy::Keyed {
            keys,
            include_keys: false,
            keys_pre_grouped: false,
        }
    };

    // 3. Configure Sink Destination (Hive Style)
    let hive_provider = HivePathProvider {
        extension: PlSmallStr::from_str(".parquet"),
    };
    
    let destination = SinkDestination::Partitioned {
        base_path: PlRefPath::new(&staging_uri), 
        file_path_provider: Some(file_provider::FileProviderType::Hive(hive_provider)),
        partition_strategy,
        max_rows_per_file: u32::MAX, 
        approximate_bytes_per_file: usize::MAX as u64,
    };

    // 4. Configure Parquet Options
    let write_opts = build_parquet_write_options(
        1,  // compression: Snappy
        -1, // compression level: Default
        true, // statistics: true 
        0,  // row_group_size: Default
        0,  // data_page_size: Default
        -1  // compat_level: Default
    ).map_err(|e| PolarsError::ComputeError(format!("Options error: {}", e).into()))?;

    // 5. Re-build Cloud Args (Unsafe FFI)
    let unified_args = unsafe {
        build_unified_sink_args(
            false, // mkdir
            false, // maintain_order: false
            0,     // sync_on_close: Default
            cloud_args.provider,
            cloud_args.retries,
            cloud_args.retry_timeout_ms,
            cloud_args.retry_init_backoff_ms,
            cloud_args.retry_max_backoff_ms,
            cloud_args.cache_ttl,
            cloud_args.keys,
            cloud_args.values,
            cloud_args.len,
        )
    };

    // 6. Execute
    processed_lf
        .sink(destination, FileWriteFormat::Parquet(write_opts), unified_args)?
        .collect_with_engine(Engine::Streaming)?;

    Ok((staging_dir_name, write_id))
}

/// Phase 4-B (MoR Only): Process Tombstones into Deletion Vectors
pub(crate) async fn phase_execution_dv(
    table_url: &url::Url,
    table: &DeltaTable,
    // tombstones_lf: LazyFrame,
    df: DataFrame,
    candidate_files: &[Add], 
) -> PolarsResult<Vec<Action>> {
    
    if df.height() == 0 {
        return Ok(Vec::new()); // No rows deleted
    }

    // Build Lookup Table for Original Files
    let root_trimmed = table_url.as_str().trim_end_matches('/');
    let mut file_lookup: HashMap<String, &Add> = HashMap::new();
    for f in candidate_files {
        file_lookup.insert(f.path.clone(), f);
    }

    let mut final_actions = Vec::new();
    let object_store = table.object_store();
    let table_root = Path::from(root_trimmed);

    let path_series = df.column("__file_path")?.str()?;
    let row_idx_series = df.column("__row_index")?.list()?;

    // Iterate over affected files
    for i in 0..df.height() {
        let full_path = path_series.get(i).unwrap();
        
        let rel_path = full_path.strip_prefix(&format!("{}/", root_trimmed))
            .unwrap_or(full_path)
            .to_string();

        let row_indices_series = row_idx_series.get_as_series(i).unwrap();
        let row_indices_ca = row_indices_series.u32()?;
        let new_deleted_indices: Vec<u32> = row_indices_ca.into_no_null_iter().collect();

        // Get original Add Action for DV and stats
        let original_add = file_lookup.get(&rel_path).ok_or_else(|| {
            PolarsError::ComputeError(format!("Cannot find original action for {}", rel_path).into())
        })?;

        // Read old DV
        let mut bitmap = if let Some(old_dv) = &original_add.deletion_vector {
            read_deletion_vector(object_store.clone(), old_dv, &table_root).await?
        } else {
            RoaringBitmap::new()
        };

        // Concat new deletion rows
        bitmap.extend(new_deleted_indices);

        // Write new DV 
        let (dv_file_name, dv_size) = write_dv_file(object_store.clone(), &root_trimmed, &bitmap).await?;
        let new_dv_descriptor = create_dv_descriptor(&dv_file_name, dv_size, bitmap.len() as i64)?;

        // Build Remove Action
        let remove = Remove {
            path: original_add.path.clone(),
            deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(original_add.partition_values.clone()),
            size: Some(original_add.size),
            deletion_vector: original_add.deletion_vector.clone(), 
            tags: original_add.tags.clone(),
            base_row_id: original_add.base_row_id,
            default_row_commit_version: original_add.default_row_commit_version,
        };
        final_actions.push(Action::Remove(remove));

        // Build Add Action
        let mut new_add = (*original_add).clone();
        new_add.deletion_vector = Some(new_dv_descriptor);
        new_add.modification_time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        final_actions.push(Action::Add(new_add));
    }

    Ok(final_actions)
}


/// Phase 5-A: Process Staging Files
/// Scan Staging, read Stats, move file, generate Add Actions
pub async fn phase_process_staging(
    table: &DeltaTable,
    staging_dir_name: &str,
    partition_cols: &[String],
    write_id: Uuid,
) -> PolarsResult<Vec<Action>> {
    
    let object_store = table.object_store();
    let staging_path = Path::from(staging_dir_name);

    // 1. List Files
    let file_metas: Vec<_> = object_store.list(Some(&staging_path))
        .filter_map(|res| async { res.ok() }) 
        .filter(|meta| futures::future::ready(meta.location.to_string().ends_with(".parquet")))
        .collect()
        .await;

    if file_metas.is_empty() {
        return Ok(Vec::new());
    }

    // 2. Parallel Processing (Read Stats + Rename)
    const CONCURRENCY: usize = 64; 

    let stream = futures::stream::iter(file_metas)
        .map(|meta| {
            let object_store = object_store.clone();
            let staging_dir_name = staging_dir_name.to_string();
            let partition_cols = partition_cols.to_vec();
            let write_id = write_id;

            async move {
                let src_path_str = meta.location.to_string();
                let file_size = meta.size as i64;

                // ---------------------------------------------------------
                // A. Read Parquet Footer (IO - Small Read)
                // ---------------------------------------------------------
                let mut reader = ParquetObjectReader::new(object_store.clone(), meta.location.clone())
                    .with_file_size(meta.size as u64);
                
                let footer = reader.get_metadata(None).await
                    .map_err(|e| PolarsError::ComputeError(format!("Read parquet footer error: {}", e).into()))?;
                
                // Extract Stats 
                let (_, stats_json) = extract_delta_stats(&footer)?;

                // ---------------------------------------------------------
                // B. Rename & Promote (IO - Metadata Operation)
                // ---------------------------------------------------------
                // Old Path: .merge_staging_xxx/region=A/part-001.parquet
                // Target Path: region=A/part-001-<uuid>.parquet
                
                // 1. Remove Staging Prefix
                let rel_path = src_path_str.trim_start_matches(&format!("{}/", staging_dir_name));
                // 2. Inject Write ID
                let dest_path_str = rel_path.replace(".parquet", &format!("-{}.parquet", write_id));
                
                let src_path = Path::from(src_path_str);
                let dest_path = Path::from(dest_path_str.clone());

                // Rename (S3: Copy + Delete，Azure/HDFS: Rename)
                object_store.rename(&src_path, &dest_path).await
                    .map_err(|e| PolarsError::ComputeError(format!("Rename failed: {}", e).into()))?;

                // ---------------------------------------------------------
                // C. Build Add Action (CPU)
                // ---------------------------------------------------------
                // Parse hive partitions
                let partition_values = parse_hive_partitions(&dest_path_str, &partition_cols);

                Ok::<Action, PolarsError>(Action::Add(Add {
                    path: dest_path_str,
                    size: file_size,
                    partition_values,
                    modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
                    data_change: true, 
                    stats: Some(stats_json),
                    tags: None,
                    deletion_vector: None,
                    base_row_id: None,
                    default_row_commit_version: None,
                    clustering_provider: None,
                }))
            }
        })
        .buffer_unordered(CONCURRENCY); 

    // 3. Collect Results
    let results: Vec<Result<Action, PolarsError>> = stream.collect().await;

    // Unwrap results
    let mut add_actions = Vec::with_capacity(results.len());
    for res in results {
        add_actions.push(res?);
    }

    Ok(add_actions)
}

pub(crate) async fn phase_commit(
    table: &mut DeltaTable,
    mut actions: Vec<Action>, 
    source_schema: &Schema,   
    target_schema: &Schema,
    merge_keys: &[String],
    can_evolve: bool,
    matched_preds: Vec<deltalake::protocol::MergePredicate>,
    not_matched_preds: Vec<deltalake::protocol::MergePredicate>,
    not_matched_by_source_preds: Vec<deltalake::protocol::MergePredicate>,
) -> Result<(), deltalake::errors::DeltaTableError> {

    // =========================================================
    // 1. Schema Evolution (Metadata Action)
    // =========================================================
    
    // Build New Polars Schema (Target + Source New Cols)
    let mut new_pl_schema = target_schema.clone();
    let mut has_schema_change = false;
    
    for (name, dtype) in source_schema.iter() {
        if new_pl_schema.get_field(name).is_none() {
            new_pl_schema.with_column(name.to_string().into(), dtype.clone());
            has_schema_change = true;
        }
    }

    if has_schema_change {
        // Convert to Delta Schema and PolarsError to DeltaTableError::Generic
        let new_delta_schema = crate::delta::utils::convert_to_delta_schema(&new_pl_schema)
            .map_err(|e| deltalake::errors::DeltaTableError::Generic(e.to_string()))?;
        
        let current_snapshot = table.snapshot()?;
        let current_delta_schema = current_snapshot.schema();

        if current_delta_schema.as_ref() != &new_delta_schema {
            if !can_evolve {
                return Err(deltalake::errors::DeltaTableError::SchemaMismatch {
                    msg: "Schema mismatch detected. Pass 'can_evolve=true'.".into()
                });
            }
            
            // Current Metadata => JSON Value
            let current_metadata = current_snapshot.metadata();
            let mut meta_json = serde_json::to_value(current_metadata)
                .map_err(|e| deltalake::errors::DeltaTableError::Generic(format!("Failed to serialize metadata: {}", e)))?;

            // New Schema => String
            let new_schema_string = serde_json::to_string(&new_delta_schema)
                .map_err(|e| deltalake::errors::DeltaTableError::Generic(format!("Failed to serialize new schema: {}", e)))?;

            // Replace "schemaString" field in JSON
            if let Some(obj) = meta_json.as_object_mut() {
                obj.insert("schemaString".to_string(), serde_json::Value::String(new_schema_string));
            } else {
                return Err(deltalake::errors::DeltaTableError::Generic("Metadata is not a JSON object".into()));
            }

            // Convert to Metadata Action
            let new_metadata_action: deltalake::kernel::Metadata = serde_json::from_value(meta_json)
                .map_err(|e| deltalake::errors::DeltaTableError::Generic(format!("Failed to deserialize modified metadata: {}", e)))?;

            // Inject Actions List
            actions.insert(0, Action::Metadata(new_metadata_action));
        }
    }

    // =========================================================
    // 2. Commit Transaction
    // =========================================================
    
    // Build SQL Style Predicate (e.g. "source.id = target.id")
    let join_predicate = merge_keys.iter()
        .map(|k| format!("source.{} = target.{}", k, k))
        .collect::<Vec<_>>()
        .join(" AND ");

    let operation = DeltaOperation::Merge {
        predicate: Some(join_predicate.clone()),
        merge_predicate: Some(join_predicate), 
        matched_predicates: matched_preds,
        not_matched_predicates: not_matched_preds,
        not_matched_by_source_predicates: not_matched_by_source_preds,
    };

    // =========================================================
    // Strict Commit 
    // =========================================================
    
    let _commit_res = deltalake::kernel::transaction::CommitBuilder::default()
        .with_actions(actions)
        .with_app_metadata(crate::delta::utils::get_polars_net_metadata())
        .build(
            Some(table.snapshot()?), 
            table.log_store().clone(),
            operation
        )
        .await?;

    Ok(())
}

pub(crate) fn merge_delta_internal_custom(
    mut source_lf: LazyFrame,
    table_url: url::Url,
    merge_keys: Vec<String>,
    cond_update: Option<Expr>,
    cond_delete: Option<Expr>,
    cond_insert: Option<Expr>,
    cond_src_delete: Option<Expr>,
    can_evolve: bool,
    delta_storage_options: std::collections::HashMap<String, String>,
    cloud_args: RawCloudArgs,
) -> PolarsResult<()> {
    let rt = crate::delta::utils::get_runtime();

    let max_attempts = std::env::var("POLARS_DELTA_MAX_RETRIES")
        .unwrap_or_else(|_| "5".to_string())
        .parse::<u32>()
        .unwrap_or(5);

    let (mut table, partition_cols, mut target_schema) = rt.block_on(
        crate::delta::merge::phase_1_load_table(table_url.clone(), delta_storage_options)
    )?;
    
    let snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot error: {}", e).into()))?;
    let strategy = crate::delta::merge::MergeStrategy::determine(snapshot, false);

    let ctx = crate::delta::merge::MergeContext {
        table_url, merge_keys, can_evolve, strategy,
        cond_update, cond_delete, cond_insert, cond_src_delete,
    };

    let mut source_lf_clone = source_lf.clone();
    crate::delta::merge::phase_validation(&ctx.merge_keys, ctx.can_evolve, &mut source_lf_clone, &target_schema)?;

    for attempt in 1..=max_attempts {
        if attempt > 1 {
            rt.block_on(async {
                table.update_state().await.map_err(|e| PolarsError::ComputeError(e.to_string().into()))
            })?;
            let polars_schema = crate::delta::utils::get_polars_schema_from_delta(&table)?;
            target_schema = polars_schema.into();
        }

        let (src_parts, key_bounds, cands) = crate::delta::merge::phase_1_analyze_source(
            &ctx.merge_keys, &source_lf, &target_schema, &partition_cols
        )?;
        let candidate_files = rt.block_on(
            crate::delta::merge::phase_1_scan_and_prune(&table, &partition_cols, src_parts, key_bounds, cands, ctx.merge_keys.first())
        )?;

        let target_lf = crate::delta::merge::construct_target_lf(
            &ctx.table_url, ctx.strategy, &table, &candidate_files, &target_schema, &cloud_args
        )?;

        let (new_data_lf, tombstones_lf_opt) = match ctx.strategy {
            crate::delta::merge::MergeStrategy::CopyOnWrite => {
                let processed = phase_planning(&ctx, target_lf, source_lf.clone(), &target_schema)?;
                (processed, None)
            },
            crate::delta::merge::MergeStrategy::MergeOnRead => {
                let (new_data, tombstones) = phase_planning_mor(&ctx, target_lf, source_lf.clone(), &target_schema)?;
                (new_data, Some(tombstones))
            }
        };

        let (staging_dir, write_id) = crate::delta::merge::phase_execution(&ctx.table_url, new_data_lf, &partition_cols, &cloud_args)?;
        let staging_dir_for_commit = staging_dir.clone();

        let tombstones_df_opt = match tombstones_lf_opt {
            Some(lf) => {
                let df = lf.group_by([col("__file_path")]).agg([col("__row_index")]).collect_with_engine(Engine::Streaming)?;
                if df.height() > 0 { Some(df) } else { None }
            },
            None => None,
        };

        let commit_result = rt.block_on(async {
            let mut final_actions = crate::delta::merge::phase_process_staging(&table, &staging_dir, &partition_cols, write_id).await
                .map_err(|e| deltalake::errors::DeltaTableError::Generic(e.to_string()))?;

            match ctx.strategy {
                crate::delta::merge::MergeStrategy::CopyOnWrite => {
                    for add in &candidate_files {
                        let remove = deltalake::kernel::Remove {
                            path: add.path.clone(),
                            deletion_timestamp: Some(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64),
                            data_change: true,
                            extended_file_metadata: Some(true),
                            partition_values: Some(add.partition_values.clone()),
                            size: Some(add.size),
                            deletion_vector: add.deletion_vector.clone(),
                            ..Default::default()
                        };
                        final_actions.push(deltalake::kernel::Action::Remove(remove));
                    }
                },
                crate::delta::merge::MergeStrategy::MergeOnRead => {
                    if let Some(t_df) = tombstones_df_opt {
                        let dv_actions = crate::delta::merge::phase_execution_dv(&ctx.table_url, &table, t_df, &candidate_files).await
                            .map_err(|e| deltalake::errors::DeltaTableError::Generic(e.to_string()))?;
                        final_actions.extend(dv_actions);
                    }
                }
            }

            if !final_actions.is_empty() {
                let src_schema = source_lf.collect_schema().unwrap();
                let mut matched_preds = Vec::new();
                let mut not_matched_preds = Vec::new();           
                let mut not_matched_by_source_preds = Vec::new(); 
                
                if ctx.cond_delete.is_some() { matched_preds.push(deltalake::protocol::MergePredicate { predicate: Some("matched_delete_condition".into()), action_type: "DELETE".into() }); }
                if ctx.cond_update.is_some() { matched_preds.push(deltalake::protocol::MergePredicate { predicate: Some("custom_update_condition".into()), action_type: "UPDATE".into() }); } 
                else { matched_preds.push(deltalake::protocol::MergePredicate { predicate: None, action_type: "UPDATE".into() }); }
                if ctx.cond_insert.is_some() { not_matched_preds.push(deltalake::protocol::MergePredicate { predicate: Some("not_matched_insert_condition".into()), action_type: "INSERT".into() }); } 
                else { not_matched_preds.push(deltalake::protocol::MergePredicate { predicate: None, action_type: "INSERT".into() }); }
                if ctx.cond_src_delete.is_some() { not_matched_by_source_preds.push(deltalake::protocol::MergePredicate { predicate: Some("not_matched_by_source_delete_condition".into()), action_type: "DELETE".into() }); }

                crate::delta::merge::phase_commit(
                    &mut table, final_actions, &src_schema, &target_schema,
                    &ctx.merge_keys, ctx.can_evolve,
                    matched_preds, not_matched_preds, not_matched_by_source_preds
                ).await?;
            }
            Ok::<(), deltalake::errors::DeltaTableError>(())
        });

        match commit_result {
            Ok(_) => return Ok(()),
            Err(deltalake::errors::DeltaTableError::CommitValidation { .. }) 
            | Err(deltalake::errors::DeltaTableError::VersionAlreadyExists(_)) 
            | Err(deltalake::errors::DeltaTableError::Transaction { .. }) => {
                if attempt < max_attempts {
                    rt.block_on(async {
                        let object_store = table.object_store();
                        let _ = object_store.delete(&deltalake::Path::from(staging_dir_for_commit.as_str())).await;
                    });
                    
                    let base_sleep = 50_u64.saturating_mul(2_u64.pow(attempt - 1));
                    let capped_sleep = std::cmp::min(base_sleep, 1000);
                    
                    let mut rng = rand::rng();
                    let jitter_millis = rng.random_range((capped_sleep / 2)..=(capped_sleep * 3 / 2));

                    println!("[Delta-RS] Merge OCC Conflict! Backoff for {}ms (Attempt {}/{})", jitter_millis, attempt, max_attempts);

                    std::thread::sleep(std::time::Duration::from_millis(jitter_millis));
                    continue; 
                } else {
                    return Err(PolarsError::ComputeError(format!("Max retry attempts ({}) reached for Merge OCC conflict", max_attempts).into()));
                }
            },
            Err(e) => return Err(PolarsError::ComputeError(format!("Merge Commit failed: {}", e).into())),
        }
    } 

    Ok(())
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_merge(
    source_lf_ptr: *mut LazyFrameContext, 
    target_path_ptr: *const c_char,
    merge_keys_ptr: *const *const c_char,
    merge_keys_len: usize,
    
    matched_update_cond: *mut ExprContext,        
    matched_delete_cond: *mut ExprContext,        
    not_matched_insert_cond: *mut ExprContext,    
    not_matched_by_source_delete_cond: *mut ExprContext, 

    can_evolve: bool, 
    cloud_provider: u8, cloud_retries: usize, cloud_retry_timeout_ms: u64, cloud_retry_init_backoff_ms: u64, cloud_retry_max_backoff_ms: u64, cloud_cache_ttl: u64, cloud_keys: *const *const c_char, cloud_values: *const *const c_char, cloud_len: usize
) {
    ffi_try_void!({
        let path_str = crate::utils::ptr_to_str(target_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = crate::delta::utils::parse_table_url(path_str)?;
        let merge_keys = unsafe { crate::utils::ptr_to_vec_string(merge_keys_ptr, merge_keys_len) };
        let source_lf_ctx = unsafe { Box::from_raw(source_lf_ptr) };

        let cond_update = if !matched_update_cond.is_null() { Some(unsafe { *Box::from_raw(matched_update_cond) }.inner) } else { None };
        let cond_delete = if !matched_delete_cond.is_null() { Some(unsafe { *Box::from_raw(matched_delete_cond) }.inner) } else { None };
        let cond_insert = if !not_matched_insert_cond.is_null() { Some(unsafe { *Box::from_raw(not_matched_insert_cond) }.inner) } else { None };
        let cond_src_delete = if !not_matched_by_source_delete_cond.is_null() { Some(unsafe { *Box::from_raw(not_matched_by_source_delete_cond) }.inner) } else { None };

        let delta_storage_options = crate::delta::utils::build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let cloud_args = RawCloudArgs { provider: cloud_provider, retries: cloud_retries, retry_timeout_ms: cloud_retry_timeout_ms, retry_init_backoff_ms: cloud_retry_init_backoff_ms, retry_max_backoff_ms: cloud_retry_max_backoff_ms, cache_ttl: cloud_cache_ttl, keys: cloud_keys, values: cloud_values, len: cloud_len };

        merge_delta_internal_custom(
            source_lf_ctx.inner, table_url, merge_keys, 
            cond_update, cond_delete, cond_insert, cond_src_delete, 
            can_evolve, delta_storage_options, cloud_args
        )?;
        
        Ok(()) 
    })
}