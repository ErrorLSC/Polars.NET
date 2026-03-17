// -------------------------------------------------------------------------
// Helper Functions
// -------------------------------------------------------------------------

use deltalake::DeltaTable;
use deltalake::kernel::engine::arrow_conversion::{TryIntoArrow, TryIntoKernel};
use deltalake::parquet::data_type::ByteArray;
use deltalake::parquet::file::metadata::ParquetMetaData;
use deltalake::parquet::file::statistics::Statistics as ParquetStatistics;
use deltalake::arrow::datatypes::Schema as DeltaArrowSchema;
use deltalake::arrow::ffi::FFI_ArrowSchema as DeltaFFISchema;
use deltalake::kernel::{Add, LogicalFileView, StructType};
use deltalake::protocol::SaveMode;
use polars::{error::PolarsError, prelude::*};
use polars_arrow::ffi::{import_field_from_c,ArrowSchema as PolarsFFISchema};
use tokio::runtime::Runtime;
use std::os::raw::c_char;
use std::collections::HashMap;
use std::sync::OnceLock;
use url::Url;
use serde_json::{json, Value};

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

// Get global tokio runtime
pub(crate) fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create global Tokio runtime")
    })
}

/// Extract Parquet Stat then convert to Delta JSON format
/// Return: (num_records, stats_json_string)
pub(crate) fn extract_delta_stats(metadata: &ParquetMetaData) -> Result<(i64, String), PolarsError> {
    let file_metadata = metadata.file_metadata();
    let num_rows = file_metadata.num_rows();
    
    // Delta Stats Struct
    // {
    //   "numRecords": 100,
    //   "minValues": { "col1": 1, "col2": "a" },
    //   "maxValues": { "col1": 10, "col2": "z" },
    //   "nullCount": { "col1": 0, "col2": 5 }
    // }

    let mut min_values = serde_json::Map::new();
    let mut max_values = serde_json::Map::new();
    let mut null_count = serde_json::Map::new();

    let schema_descr = file_metadata.schema_descr();

    // Iter all columns
    for (col_idx, column_descr) in schema_descr.columns().iter().enumerate() {
        let col_name = column_descr.name();

        // Agg RowGroups Stats
        let mut global_min: Option<Value> = None;
        let mut global_max: Option<Value> = None;
        let mut global_nulls: i64 = 0;
        let mut has_stats = false;

        for rg in metadata.row_groups() {
            if let Some(col_chunk) = rg.column(col_idx).statistics() {
                has_stats = true;
                global_nulls += col_chunk.null_count_opt().unwrap_or(0) as i64;

                // Convert Min/Max
                let (min_val, max_val) = parquet_stats_to_json(col_chunk);
                
                // Aggregation: Min
                if let Some(v) = min_val {
                    match global_min {
                        None => global_min = Some(v),
                        Some(ref current) => {
                            if is_smaller(&v, current) {
                                global_min = Some(v);
                            }
                        }
                    }
                }

                // Aggregation: Max
                if let Some(v) = max_val {
                    match global_max {
                        None => global_max = Some(v),
                        Some(ref current) => {
                            if is_larger(&v, current) {
                                global_max = Some(v);
                            }
                        }
                    }
                }
            }
        }

        if has_stats {
            null_count.insert(col_name.to_string(), json!(global_nulls));
            if let Some(min) = global_min { min_values.insert(col_name.to_string(), min); }
            if let Some(max) = global_max { max_values.insert(col_name.to_string(), max); }
        }
    }

    let stats_json = json!({
        "numRecords": num_rows,
        "minValues": min_values,
        "maxValues": max_values,
        "nullCount": null_count
    });

    Ok((num_rows, stats_json.to_string()))
}

pub(crate) fn extract_stats_value<'a>(stats: &'a Option<serde_json::Value>, key: &str, col: &str) -> Option<&'a serde_json::Value> {
    stats.as_ref()?
        .get(key)? // "minValues" or "maxValues"
        .get(col)
}

// Convert JSON Value to i64
pub(crate) fn as_i64_safe(v: &serde_json::Value) -> Option<i64> {
    v.as_i64()
     .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
}

// Convert JSON Value to f64
pub(crate) fn as_f64_safe(v: &serde_json::Value) -> Option<f64> {
    v.as_f64()
     .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
}

// One-sided Range Pruning
/// - FileMax < QueryMin -> Prune (Skip)
/// - FileMin > QueryMax -> Prune (Skip)
/// - OR -> Keep
pub(crate) fn check_file_overlap_optimized(
    stats: &Option<Value>, 
    col: &str, 
    src_min_opt: Option<&Value>, // 改为 Option
    src_max_opt: Option<&Value>  // 改为 Option
) -> bool {
    let f_min_val = extract_stats_value(stats, "minValues", col);
    let f_max_val = extract_stats_value(stats, "maxValues", col);

    // -----------------------------------------------------------
    // 1. Check Lower Bound: QueryMin
    // -----------------------------------------------------------
    if let Some(q_min) = src_min_opt {
        if let Some(f_max) = f_max_val {
            match q_min {
                Value::Number(q_num) => {
                    // Try Integer Comparison
                    let f_i = as_i64_safe(f_max);
                    let q_i = q_num.as_i64();
                    
                    if let (Some(f), Some(q)) = (f_i, q_i) {
                        if f < q { return false; } // Pruned
                    } else {
                        // Fallback to Float
                        let f_f = as_f64_safe(f_max);
                        let q_f = q_num.as_f64();
                        if let (Some(f), Some(q)) = (f_f, q_f) {
                            if f < q { return false; } // Pruned
                        }
                    }
                },
                Value::String(q_str) => {
                    if let Some(f_str) = f_max.as_str() {
                        if *f_str < **q_str { return false; } // Pruned
                    }
                },
                _ => {}
            }
        }
    }

    // -----------------------------------------------------------
    // 2. Check Upper Bound: QueryMax
    // -----------------------------------------------------------
    if let Some(q_max) = src_max_opt {
        if let Some(f_min) = f_min_val {
             match q_max {
                Value::Number(q_num) => {
                    // Try Integer
                    let f_i = as_i64_safe(f_min);
                    let q_i = q_num.as_i64();
                    
                    if let (Some(f), Some(q)) = (f_i, q_i) {
                        if f > q { return false; } // Pruned
                    } else {
                        // Fallback to Float
                        let f_f = as_f64_safe(f_min);
                        let q_f = q_num.as_f64();
                        if let (Some(f), Some(q)) = (f_f, q_f) {
                            if f > q { return false; } // Pruned
                        }
                    }
                },
                Value::String(q_str) => {
                    if let Some(f_str) = f_min.as_str() {
                        if *f_str > **q_str { return false; } // Pruned
                    }
                },
                _ => {}
            }
        }
    }

    true
}

// Compare JSON Value
fn is_smaller(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
             if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) { f1 < f2 } 
             else if let (Some(i1), Some(i2)) = (n1.as_i64(), n2.as_i64()) { i1 < i2 }
             else { false }
        },
        (Value::String(s1), Value::String(s2)) => s1 < s2,
        _ => false 
    }
}

fn is_larger(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
             if let (Some(f1), Some(f2)) = (n1.as_f64(), n2.as_f64()) { f1 > f2 } 
             else if let (Some(i1), Some(i2)) = (n1.as_i64(), n2.as_i64()) { i1 > i2 }
             else { false }
        },
        (Value::String(s1), Value::String(s2)) => s1 > s2,
        _ => false 
    }
}

// Convert Parquet raw Stats to JSON Value
fn parquet_stats_to_json(stats: &ParquetStatistics) -> (Option<Value>, Option<Value>) {
    match stats {
        // Boolean
        ParquetStatistics::Boolean(s) => (
            s.min_opt().map(|&v| json!(v)), 
            s.max_opt().map(|&v| json!(v))
        ),
        // Int32
        ParquetStatistics::Int32(s) => (
            s.min_opt().map(|&v| json!(v)), 
            s.max_opt().map(|&v| json!(v))
        ),
        // Int64
        ParquetStatistics::Int64(s) => (
            s.min_opt().map(|&v| json!(v)), 
            s.max_opt().map(|&v| json!(v))
        ),
        // Float
        ParquetStatistics::Float(s) => (
            s.min_opt().map(|&v| json!(v)), 
            s.max_opt().map(|&v| json!(v))
        ),
        // Double
        ParquetStatistics::Double(s) => (
            s.min_opt().map(|&v| json!(v)), 
            s.max_opt().map(|&v| json!(v))
        ),
        // ByteArray (String / Binary)
        ParquetStatistics::ByteArray(s) => {
            let convert = |bytes: &ByteArray| -> Option<Value> {
                std::str::from_utf8(bytes.data()).ok().map(|s| json!(s))
            };
            (
                s.min_opt().and_then(convert),
                s.max_opt().and_then(convert)
            )
        },
        // Int96 (Legacy Timestamp)
        ParquetStatistics::Int96(_) => (None, None),
        
        // FixedLenByteArray (usually Decimal)
        ParquetStatistics::FixedLenByteArray(_) => (None, None),
    }
}

pub(crate) fn map_savemode(mode: u8) -> SaveMode {
    match mode {
        0 => SaveMode::Append,
        1 => SaveMode::Overwrite,
        2 => SaveMode::ErrorIfExists,
        3 => SaveMode::Ignore,
        _ => return SaveMode::Append
    }
}

pub(crate) fn get_polars_schema_from_delta(table: &DeltaTable) -> PolarsResult<Schema> {
    let snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Failed to get snapshot: {}", e).into()))?;
    let kernel_schema = snapshot.schema(); 
    
    // 1. Convert to Arrow Schema
    let arrow_schema: DeltaArrowSchema = TryIntoArrow::<DeltaArrowSchema>::try_into_arrow(kernel_schema.as_ref())
        .map_err(|e| PolarsError::ComputeError(format!("Schema conversion error: {}", e).into()))?;

    // 2. FFI Export -> Import to Polars Schema
    let delta_ffi = DeltaFFISchema::try_from(&arrow_schema)
        .map_err(|e| PolarsError::ComputeError(format!("FFI Export Error: {}", e).into()))?;

    let delta_ffi_ptr = &delta_ffi as *const DeltaFFISchema;
    let polars_ffi_ptr = delta_ffi_ptr as *const PolarsFFISchema;

    let imported_field = unsafe { import_field_from_c(&*polars_ffi_ptr) }
        .map_err(|e| PolarsError::ComputeError(format!("FFI Import error: {}", e).into()))?;

    match imported_field.dtype {
        ArrowDataType::Struct(fields) => {
            let polars_fields = fields.iter().map(|arrow_field| {
                Field::from(arrow_field)
            });
            Ok(Schema::from_iter(polars_fields))
        },
        _ => Err(PolarsError::ComputeError("Imported Delta Schema is not a Struct".into())),
    }
}

/// path: "date=2023-01-01/region=US/part-xxx.parquet"
/// cols: ["date", "region"]
pub(crate) fn parse_hive_partitions(path: &str, cols: &[String]) -> HashMap<String, Option<String>> {
    let mut map = HashMap::new();
    if cols.is_empty() { return map; }
    
    let parts: Vec<&str> = path.split('/').collect();
    for part in parts {
        if let Some((k, v)) = part.split_once('=') {
            if cols.contains(&k.to_string()) {
                let decoded_v = urlencoding::decode(v).map(|c| c.into_owned()).unwrap_or(v.to_string());
                map.insert(k.to_string(), Some(decoded_v));
            }
        }
    }
    for col in cols {
        map.entry(col.clone()).or_insert(None);
    }
    map
}

pub(crate) fn build_delta_storage_options_map(
    keys: *const *const c_char,
    values: *const *const c_char,
    len: usize
) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if !keys.is_null() && !values.is_null() && len > 0 {
        let keys_slice = unsafe { std::slice::from_raw_parts(keys, len) };
        let vals_slice = unsafe { std::slice::from_raw_parts(values, len) };
        for i in 0..len {
            if !keys_slice[i].is_null() && !vals_slice[i].is_null() {
                let k = unsafe { std::ffi::CStr::from_ptr(keys_slice[i]).to_string_lossy().into_owned() };
                let v = unsafe { std::ffi::CStr::from_ptr(vals_slice[i]).to_string_lossy().into_owned() };
                map.insert(k, v);
            }
        }
    }
    map
}

pub(crate) fn parse_table_url(path_str: &str) -> PolarsResult<Url> {
    // Case A: Normal Url (如 s3://, abfss://, gs://)
    if let Ok(u) = Url::parse(path_str) {
        if u.has_host() || u.scheme() == "file" {
            return Ok(u);
        }
    }

    // Case B: Local Disk (./data/test or /tmp/test or C:\data\test)
    let abs_path = std::fs::canonicalize(path_str)
        .or_else(|_| {
            // If path is absent, try to join to current folder
            std::env::current_dir().map(|cwd| cwd.join(path_str))
        })
        .map_err(|e| PolarsError::ComputeError(format!("Invalid local path '{}': {}", path_str, e).into()))?;

    // Convert to file:// URL
    Url::from_directory_path(abs_path)
        .map_err(|_| PolarsError::ComputeError(format!("Could not convert path '{}' to file URL", path_str).into()))
}

pub(crate) fn convert_to_delta_schema(pl_schema: &Schema) -> PolarsResult<StructType> {
    // 1. Polars Schema -> Polars Arrow Schema
    let pl_arrow_schema = pl_schema.to_arrow(CompatLevel::newest());

    // 2. FFI Convert cycle: Polars Field -> Delta Field
    let delta_arrow_fields = pl_arrow_schema.iter_values().map(|pl_field| {
        // A. Export: Polars Field -> Polars FFI Struct
        let pl_ffi = polars_arrow::ffi::export_field_to_c(pl_field);
        
        // B. Cast: Polars FFI Ptr -> Delta FFI Ptr
        // Arrow C Data Interface is ABI compatiable
        let delta_ffi_ptr = &pl_ffi as *const _ as *const deltalake::arrow::ffi::FFI_ArrowSchema;

        // C. Import: Delta FFI -> Delta Arrow Field
        unsafe { deltalake::arrow::datatypes::Field::try_from(&*delta_ffi_ptr) }
            .map_err(|e| PolarsError::ComputeError(format!("FFI Import Error: {}", e).into()))
    }).collect::<Result<Vec<deltalake::arrow::datatypes::Field>, PolarsError>>()?;

    // 3. Build Delta (Standard) Arrow Schema
    let delta_arrow_schema = deltalake::arrow::datatypes::Schema::new(delta_arrow_fields);

    // 4. Convert to Delta Kernel Schema (StructType)
    let delta_schema: StructType = TryIntoKernel::<StructType>::try_into_kernel(&delta_arrow_schema)
         .map_err(|e| PolarsError::ComputeError(format!("Failed to convert to Delta Kernel Schema: {}", e).into()))?;

    Ok(delta_schema)
}


#[derive(Clone, Copy)]
pub struct RawCloudArgs {
    pub provider: u8,
    pub retries: usize,
    pub retry_timeout_ms: u64,      
    pub retry_init_backoff_ms: u64, 
    pub retry_max_backoff_ms: u64, 
    pub cache_ttl: u64,
    pub keys: *const *const c_char,
    pub values: *const *const c_char,
    pub len: usize,
}

pub(crate) fn view_to_add_action(view: &LogicalFileView) -> Add {
    
    let partition_values = view.partition_values()
        .map(|data| {
            data.fields()
                .iter()
                .zip(data.values().iter())
                .map(|(k, v)| {
                    (
                        k.name().to_string(),
                        if v.is_null() {
                            None
                        } else {
                            Some(deltalake::kernel::scalars::ScalarExt::serialize(v))
                        }
                    )
                })
                .collect::<HashMap<String, Option<String>>>()
        })
        .unwrap_or_default();

    Add {
        path: view.path().to_string(),
        size: view.size(),
        partition_values, 
        modification_time: view.modification_time(),
        data_change: true, 
        stats: view.stats(),
        tags: None,
        deletion_vector: view.deletion_vector_descriptor(),
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    }
}

pub(crate) fn get_polars_net_metadata() -> HashMap<String, Value> {
    let mut meta = HashMap::new();
    meta.insert("engine".to_string(), json!("Polars.NET"));
    meta.insert("engineMaker".to_string(), json!("ErrorLSC"));
    meta
}